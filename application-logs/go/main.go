package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	scopedb "github.com/scopedb/goscopedb"
)

const (
	schemaName = "guide_logs"
	tableName  = "requests"
)

var runIDPattern = regexp.MustCompile(`^[a-zA-Z0-9-]{1,100}$`)

type logRow struct {
	Time    string            `json:"time"`
	Service string            `json:"service"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Var     map[string]string `json:"var"`
}

func main() {
	cycles := flag.Int("cycles", 0, "number of checkout cycles; zero runs until interrupted")
	flag.Parse()
	if *cycles < 0 {
		fmt.Fprintln(os.Stderr, "-cycles must be a nonnegative integer")
		os.Exit(2)
	}

	endpoint, apiKey := os.Getenv("SCOPEDB_ENDPOINT"), os.Getenv("SCOPEDB_API_KEY")
	if endpoint == "" || apiKey == "" {
		fmt.Fprintln(os.Stderr, "Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY before running")
		os.Exit(2)
	}
	runID := os.Getenv("SCOPEDB_DEMO_RUN")
	if runID == "" {
		runID = uuid.NewString()
	}
	if !runIDPattern.MatchString(runID) {
		fmt.Fprintln(os.Stderr, "SCOPEDB_DEMO_RUN must contain 1-100 letters, digits, or hyphens")
		os.Exit(2)
	}

	client, err := scopedb.NewClient(scopedb.Config{Endpoint: endpoint, APIKey: apiKey})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, client, runID, *cycles, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, client *scopedb.Client, runID string, cycles int, out io.Writer) error {
	if err := prepare(ctx, client, runID); err != nil {
		return err
	}

	table := client.Table(tableName)
	table.Schema = schemaName
	stream, err := table.AppendStream(scopedb.AppendStreamOptions{FlushInterval: time.Second})
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(out, "Streaming checkout logs. Run ID: %s. Keep this terminal open; Ctrl+C flushes and stops.\n", runID); err != nil {
		return err
	}

	requestNumber, sentRows, completedCycles := 0, 0, 0
	var loopErr error

loop:
	for cycles == 0 || completedCycles < cycles {
		if ctx.Err() != nil {
			break
		}
		for _, release := range []string{"1.8", "1.9"} {
			for _, gateway := range []string{"credit_card", "bank_transfer"} {
				for attempt := 0; attempt < 3; attempt++ {
					row := checkout(runID, &requestNumber, release, gateway, attempt)
					payload, err := json.Marshal(row)
					if err != nil {
						loopErr = err
						break loop
					}
					if _, err := fmt.Fprintln(out, string(payload)); err != nil {
						loopErr = err
						break loop
					}
					if err := stream.Send(ctx, row); err != nil {
						if ctx.Err() == nil {
							loopErr = err
						}
						break loop
					}
					sentRows++
				}
			}
		}
		completedCycles++
		if cycles > 0 && completedCycles >= cycles {
			break
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			break loop
		case <-timer.C:
		}
	}

	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	report, flushErr := stream.Shutdown(settleCtx)
	if flushErr != nil {
		return errors.Join(loopErr, fmt.Errorf("flush logs: %w", flushErr))
	}
	if report.CommittedRows != uint64(sentRows) {
		return errors.Join(loopErr, fmt.Errorf(
			"the SDK confirmed %d of %d logs; inspect this run before retrying",
			report.CommittedRows, sentRows,
		))
	}
	if loopErr != nil {
		return loopErr
	}
	_, err = fmt.Fprintf(out, "Committed %d logs. Run ID: %s\n", sentRows, runID)
	return err
}

func prepare(ctx context.Context, client *scopedb.Client, runID string) error {
	if _, err := client.Catalog().Schema("scopedb", schemaName).Apply(ctx, &scopedb.CatalogSchema{
		Name: schemaName,
		Tables: []scopedb.CatalogTable{{
			Name: tableName,
			Columns: []scopedb.CatalogColumn{
				{Name: "time", DataType: scopedb.TimestampDataType},
				{Name: "service", DataType: scopedb.StringDataType},
				{Name: "level", DataType: scopedb.StringDataType},
				{Name: "message", DataType: scopedb.StringDataType},
				{Name: "var", DataType: scopedb.ObjectDataType},
			},
		}},
	}); err != nil {
		return fmt.Errorf("apply catalog schema: %w", err)
	}

	result, err := client.Query(ctx, fmt.Sprintf(
		"FROM %s.%s WHERE var['run_id']::string = '%s' AGGREGATE count() AS requests",
		schemaName, tableName, runID,
	))
	if err != nil {
		return fmt.Errorf("check existing logs: %w", err)
	}
	rows, err := result.RawRows()
	if err != nil {
		return fmt.Errorf("read existing log count: %w", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		return errors.New("could not read the existing log count")
	}
	previous, err := strconv.ParseUint(*rows[0][0], 10, 64)
	if err != nil {
		return fmt.Errorf("parse existing log count: %w", err)
	}
	if previous > 0 {
		return errors.New("this run ID already has logs; choose a new SCOPEDB_DEMO_RUN")
	}
	return nil
}

func checkout(runID string, requestNumber *int, release, gateway string, attempt int) logRow {
	*requestNumber++
	requestID := fmt.Sprintf("req-%02d", *requestNumber)
	level, message := "info", "Payment accepted"
	if release == "1.9" && gateway == "credit_card" && attempt > 0 {
		level, message = "error", "Credit card gateway timed out"
	}
	return logRow{
		Time: time.Now().UTC().Format(time.RFC3339Nano), Service: "checkout", Level: level, Message: message,
		Var: map[string]string{
			"run_id": runID, "request_id": requestID, "release": release, "gateway": gateway,
		},
	}
}
