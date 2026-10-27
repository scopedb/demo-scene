package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
	scopedb "github.com/scopedb/goscopedb"
)

const (
	postCollection = "app.bsky.feed.post"
	maxFrameBytes  = 1 << 20
	maxBatchBytes  = 2 << 20
	commitType     = "network.bsky.jetstream.subscribeEvents#commit"
)

var collections = []string{postCollection, "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.graph.follow"}

// RawMessage preserves unknown fields and integer precision through capture and replay.
type capturedEvent struct {
	Source string          `json:"source"`
	Raw    json.RawMessage `json:"raw"`
}

type eventInfo struct {
	Type        string `json:"$type"`
	Seq         int64  `json:"seq"`
	Time        string `json:"time"`
	WitnessedAt string `json:"witnessedAt"`
	Collection  string `json:"collection"`
	Operation   string `json:"operation"`
	DID         string `json:"did"`
	RKey        string `json:"rkey"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	command := "stream"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	defaultID := "live-first"
	if command == "load" {
		defaultID = ""
	}
	captureID := flags.String("capture-id", defaultID, "observation label; use the same label to replay its recovery file")
	source := flags.String("source", "wss://jetstream.us-west.bsky.network", "Jetstream origin")
	input := flags.String("file", "", "saved event file, for the load command")
	limit := flags.Int("limit", 0, "stop after this many live deliveries; zero streams until Ctrl+C")
	batchSize := flags.Int("batch-size", 500, "maximum rows per SDK append")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*captureID) == "" || *limit < 0 || *batchSize < 1 || *batchSize > 1000 {
		return errors.New("use a nonempty capture ID, nonnegative limit, and batch size from 1 to 1000")
	}
	if command != "stream" && command != "load" {
		return errors.New("use stream (the default) or load -file <saved-file> -capture-id <original-label>")
	}
	if command == "load" && *input == "" {
		return errors.New("load requires -file")
	}
	if command == "stream" && *captureID == "first" {
		return errors.New("capture ID first is reserved for the constructed sample")
	}
	endpoint, key := os.Getenv("SCOPEDB_ENDPOINT"), os.Getenv("SCOPEDB_API_KEY")
	if endpoint == "" || key == "" {
		return errors.New("set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY using Console > Connect")
	}
	httpClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer httpClient.CloseIdleConnections()
	client, err := scopedb.NewClient(scopedb.Config{Endpoint: endpoint, APIKey: key, HTTPClient: httpClient})
	if err != nil {
		return err
	}
	defer client.Close()
	table := client.Table("events_raw")
	table.Schema = "guide_bluesky"
	if command == "load" {
		return load(ctx, *input, *captureID, *batchSize, table)
	}
	if err := ensureSample(ctx, client, table); err != nil {
		return fmt.Errorf("prepare constructed sample: %w", err)
	}
	return stream(ctx, *source, *captureID, *limit, *batchSize, table)
}

func ensureSample(ctx context.Context, client *scopedb.Client, table *scopedb.Table) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := client.Query(ctx, "FROM guide_bluesky.events WHERE capture_id = 'first' AGGREGATE count() AS events")
	if err != nil {
		return fmt.Errorf("run the three setup statements before starting: %w", err)
	}
	rows, err := result.RawRows()
	if err != nil || len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		return errors.New("could not read the constructed sample count")
	}
	if *rows[0][0] == "16" {
		fmt.Println("Constructed sample ready: capture first has 16 unique events")
		return nil
	}
	return load(ctx, "sample.ndjson", "first", 500, table)
}

func inspect(event capturedEvent) (eventInfo, error) {
	var info eventInfo
	if err := json.Unmarshal(event.Raw, &info); err != nil {
		return info, fmt.Errorf("invalid event payload: %w", err)
	}
	if event.Source == "" || info.Type != commitType || info.Seq <= 0 || info.Collection == "" || info.DID == "" || info.RKey == "" {
		return info, errors.New("expected a Jetstream commit with source, seq, collection, did, and rkey")
	}
	if info.Operation != "create" && info.Operation != "update" && info.Operation != "delete" {
		return info, fmt.Errorf("unexpected operation %q", info.Operation)
	}
	if info.WitnessedAt != "" {
		info.Time = info.WitnessedAt
	}
	if _, err := time.Parse(time.RFC3339Nano, info.Time); err != nil {
		return info, fmt.Errorf("invalid event time: %w", err)
	}
	return info, nil
}

type batchWriter struct {
	table        *scopedb.Table
	captureID    string
	batchSize    int
	rows         bytes.Buffer
	pending      []capturedEvent
	pendingSeq   int64
	committedSeq int64
	committed    int64
	recoveryDir  string
}

func (w *batchWriter) add(ctx context.Context, event capturedEvent, info eventInfo) error {
	row, err := json.Marshal(map[string]any{
		"capture_id": w.captureID, "event_id": event.Source + "/" + strconv.FormatInt(info.Seq, 10),
		"event_time": info.Time, "ingested_at": time.Now().UTC().Format(time.RFC3339Nano),
		"collection": info.Collection, "operation": info.Operation, "raw": event.Raw,
	})
	if err != nil {
		return err
	}
	if w.rows.Len()+len(row)+1 > maxBatchBytes {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	w.rows.Write(row)
	w.rows.WriteByte('\n')
	w.pending = append(w.pending, event)
	w.pendingSeq = info.Seq
	if len(w.pending) >= w.batchSize {
		return w.flush(ctx)
	}
	return nil
}

func (w *batchWriter) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	// Stop reading on Ctrl+C, but let an admitted batch settle before exiting.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	result, err := w.table.AppendNDJSON(writeCtx, w.rows.Bytes())
	if err != nil {
		file, saveErr := os.CreateTemp(w.recoveryDir, "scopedb-bluesky-recovery-*.ndjson")
		if saveErr != nil {
			return fmt.Errorf("SDK append failed: %w; could not save the unconfirmed batch: %v", err, saveErr)
		}
		for _, event := range w.pending {
			if saveErr = json.NewEncoder(file).Encode(event); saveErr != nil {
				break
			}
		}
		saveErr = errors.Join(saveErr, file.Close())
		if saveErr != nil {
			return fmt.Errorf("SDK append failed: %w; saving recovery file %s failed: %v", err, file.Name(), saveErr)
		}
		return fmt.Errorf("SDK append failed after %d confirmed rows: %w; unconfirmed events saved in %s; replay with load -file <that-file> -capture-id %q", w.committed, err, file.Name(), w.captureID)
	}
	w.committed += result.NumRowsInserted
	w.committedSeq = w.pendingSeq
	fmt.Printf("Committed %d events for capture %s\n", w.committed, w.captureID)
	w.rows.Reset()
	w.pending = nil
	return nil
}

func load(ctx context.Context, input, captureID string, batchSize int, table *scopedb.Table) error {
	file, err := os.Open(input)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), maxFrameBytes+1024)
	writer := batchWriter{table: table, captureID: captureID, batchSize: batchSize}
	for line := 1; scanner.Scan(); line++ {
		if ctx.Err() != nil {
			return writer.flush(ctx)
		}
		var event capturedEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		info, err := inspect(event)
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if err := writer.add(ctx, event, info); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return writer.flush(ctx)
}

type liveMessage struct {
	event capturedEvent
	err   error
	fatal bool
}

func readLive(ctx context.Context, conn *websocket.Conn, source string, messages chan<- liveMessage) {
	defer close(messages)
	for {
		_, frame, err := conn.Read(ctx)
		fatal := false
		var envelope struct {
			Type    string          `json:"$type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err == nil {
			if err = json.Unmarshal(frame, &envelope); err == nil && envelope.Type != "message" {
				err = errors.New("unexpected Jetstream frame")
			}
			fatal = err != nil
		}
		message := liveMessage{event: capturedEvent{Source: source, Raw: envelope.Payload}, err: err, fatal: fatal}
		select {
		case messages <- message:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func stream(ctx context.Context, source, captureID string, limit, batchSize int, table *scopedb.Table) error {
	u, err := url.Parse(source)
	if err != nil || u.Host == "" || (u.Scheme != "wss" && u.Scheme != "ws") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("source must be a Jetstream origin, such as wss://jetstream.us-west.bsky.network")
	}
	source = u.Scheme + "://" + u.Host
	u.Path = "/xrpc/network.bsky.jetstream.subscribeEvents"
	query := u.Query()
	query.Set("kinds", "commit")
	for _, collection := range collections {
		query.Add("collections", collection)
	}
	writer := batchWriter{table: table, captureID: captureID, batchSize: batchSize}
	fmt.Printf("Streaming into capture %s using the Go SDK. Keep this terminal open; Ctrl+C flushes and stops.\n", captureID)
	received := 0
	for ctx.Err() == nil {
		if writer.committedSeq > 0 {
			query.Set("cursor", strconv.FormatInt(writer.committedSeq, 10))
		}
		u.RawQuery = query.Encode()
		dialCtx, cancelDial := context.WithTimeout(ctx, 20*time.Second)
		conn, _, dialErr := websocket.Dial(dialCtx, u.String(), &websocket.DialOptions{Subprotocols: []string{"xrpc.v1.json"}})
		cancelDial()
		if dialErr == nil {
			conn.SetReadLimit(maxFrameBytes)
			var sessionErr error
			received, sessionErr = consume(ctx, conn, source, &writer, received, limit)
			if sessionErr != nil {
				return sessionErr
			}
			if limit > 0 && received >= limit {
				return nil
			}
		} else if ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "Jetstream connection unavailable: %v\n", dialErr)
		}
		if ctx.Err() != nil {
			break
		}
		fmt.Fprintln(os.Stderr, "Reconnecting to the same source in two seconds from the last confirmed sequence")
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
	return nil
}

func consume(ctx context.Context, conn *websocket.Conn, source string, writer *batchWriter, received, limit int) (int, error) {
	readCtx, cancelRead := context.WithCancel(ctx)
	messages := make(chan liveMessage, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		readLive(readCtx, conn, source, messages)
	}()
	defer func() { cancelRead(); conn.CloseNow(); <-done }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	finish := func() (int, error) {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return received, writer.flush(flushCtx)
	}
	for {
		select {
		case <-ctx.Done():
			return finish()
		case <-ticker.C:
			if err := writer.flush(ctx); err != nil {
				return received, err
			}
		case message, ok := <-messages:
			if !ok || message.err != nil {
				n, flushErr := finish()
				if message.fatal {
					return n, errors.Join(message.err, flushErr)
				}
				return n, flushErr
			}
			var tag struct {
				Type string `json:"$type"`
			}
			if err := json.Unmarshal(message.event.Raw, &tag); err != nil {
				n, flushErr := finish()
				return n, errors.Join(err, flushErr)
			}
			if tag.Type == "network.bsky.jetstream.subscribeEvents#info" {
				continue
			}
			info, err := inspect(message.event)
			if err != nil {
				n, flushErr := finish()
				return n, errors.Join(err, flushErr)
			}
			if err := writer.add(ctx, message.event, info); err != nil {
				return received, err
			}
			received++
			if limit > 0 && received >= limit {
				return finish()
			}
		}
	}
}
