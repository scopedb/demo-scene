package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const (
	postCollection = "app.bsky.feed.post"
	maxFrameBytes  = 1 << 20
	maxFileBytes   = 50 << 20
	commitType     = "network.bsky.jetstream.subscribeEvents#commit"
)

var collections = []string{postCollection, "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.graph.follow"}

// Keep the full payload, including fields this demo does not know about.
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
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: go run . capture -out <file> | load -file <file> -stage posts|social|all")
	}
	switch args[0] {
	case "capture":
		flags := flag.NewFlagSet("capture", flag.ContinueOnError)
		output := flags.String("out", "", "new capture file (will not overwrite an existing file)")
		source := flags.String("source", "wss://jetstream.us-west.bsky.network", "Jetstream v2 origin")
		duration := flags.Duration("duration", 30*time.Second, "capture duration after connecting")
		limit := flags.Int("limit", 5000, "maximum number of events")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *output == "" || *duration <= 0 || *limit <= 0 || flags.NArg() != 0 {
			return errors.New("capture requires -out, a positive -duration and -limit, and no positional arguments")
		}
		return capture(*source, *output, *duration, *limit)
	case "load":
		flags := flag.NewFlagSet("load", flag.ContinueOnError)
		input := flags.String("file", "", "capture file to load")
		stage := flags.String("stage", "posts", "posts, social (likes/reposts/follows), or all")
		runID := flags.String("run-id", "demo-001", "identifier used to isolate this walkthrough")
		batchSize := flags.Int("batch-size", 500, "events per HTTP request, from 1 to 1000")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *input == "" || *runID == "" || *batchSize < 1 || *batchSize > 1000 || flags.NArg() != 0 {
			return errors.New("load requires -file, a nonempty -run-id, and -batch-size between 1 and 1000")
		}
		if *stage != "posts" && *stage != "social" && *stage != "all" {
			return errors.New("-stage must be posts, social, or all")
		}
		endpoint := strings.TrimRight(os.Getenv("SCOPEDB_ENDPOINT"), "/")
		key := os.Getenv("SCOPEDB_API_KEY")
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("set SCOPEDB_ENDPOINT to the ScopeDB API address from Console > Connect")
		}
		if key == "" {
			return errors.New("set SCOPEDB_API_KEY to an API key for that workspace")
		}
		client := &http.Client{Timeout: 60 * time.Second}
		return load(*input, *stage, *runID, *batchSize, client, endpoint, key)
	default:
		return fmt.Errorf("unknown command %q; use capture or load", args[0])
	}
}

func capture(source, output string, duration time.Duration, limit int) error {
	u, err := url.Parse(source)
	if err != nil || u.Host == "" || (u.Scheme != "wss" && u.Scheme != "ws") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("-source must be a Jetstream v2 origin, such as wss://jetstream.us-west.bsky.network")
	}
	source = u.Scheme + "://" + u.Host
	u.Path = "/xrpc/network.bsky.jetstream.subscribeEvents"
	query := u.Query()
	query.Set("kinds", "commit")
	for _, collection := range collections {
		query.Add("collections", collection)
	}
	u.RawQuery = query.Encode()
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelDial()
	conn, _, err := websocket.Dial(dialCtx, u.String(), &websocket.DialOptions{Subprotocols: []string{"xrpc.v1.json"}})
	if err != nil {
		return fmt.Errorf("connect to Jetstream: %w; retry with a new -out file", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxFrameBytes)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	fmt.Printf("Capturing from %s for up to %s or %d events (50 MiB maximum)...\n", source, duration, limit)
	counts := make(map[string]int)
	total, size := 0, 0
	for total < limit {
		_, frame, err := conn.Read(ctx)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				break
			}
			return fmt.Errorf("capture stopped after %d events: %w; complete lines in %s can still be loaded", total, err, output)
		}
		var envelope struct {
			Type    string          `json:"$type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(frame, &envelope); err != nil || envelope.Type != "message" {
			return errors.New("unexpected Jetstream frame; expected a v2 JSON message")
		}
		event := capturedEvent{Source: source, Raw: envelope.Payload}
		info, err := inspect(event)
		if err != nil {
			return err
		}
		line, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if size+len(line)+1 > maxFileBytes {
			break
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			return err
		}
		total++
		size += len(line) + 1
		counts[info.Collection]++
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Printf("Saved %d events (%d bytes) to %s\n", total, size, output)
	for _, collection := range collections {
		fmt.Printf("  %s: %d\n", collection, counts[collection])
	}
	if total == 0 {
		return errors.New("no events captured; try the other public endpoint with a new -out file")
	}
	return nil
}

func inspect(event capturedEvent) (eventInfo, error) {
	var info eventInfo
	if err := json.Unmarshal(event.Raw, &info); err != nil {
		return info, fmt.Errorf("invalid event payload: %w", err)
	}
	if event.Source == "" || info.Type != commitType || info.Seq <= 0 || info.Collection == "" {
		return info, errors.New("expected a Jetstream v2 commit with source, seq, and collection")
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

const ingestStatement = `SELECT
    $0['run_id']::string AS run_id,
    $0['event_id']::string AS event_id,
    $0['event_time']::timestamp AS event_time,
    NOW() AS ingested_at,
    $0['collection']::string AS collection,
    $0['operation']::string AS operation,
    $0['raw']::object AS raw
INSERT INTO bluesky_demo.events_raw`

func load(input, stage, runID string, batchSize int, client *http.Client, endpoint, key string) error {
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), maxFrameBytes+1024)
	var rows bytes.Buffer
	batchCount, total, lineNumber := 0, 0, 0
	flush := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := ingest(client, endpoint, key, rows.String(), batchCount); err != nil {
			return fmt.Errorf("%d rows confirmed before this batch: %w; replay the same file with the same run ID after fixing the cause (the events view removes repeated events)", total, err)
		}
		total += batchCount
		fmt.Printf("Loaded %d events for run %s (%s)\n", total, runID, stage)
		rows.Reset()
		batchCount = 0
		return nil
	}
	for scanner.Scan() {
		lineNumber++
		var event capturedEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("line %d: %w", lineNumber, err)
		}
		info, err := inspect(event)
		if err != nil {
			return fmt.Errorf("line %d: %w", lineNumber, err)
		}
		selected := stage == "all" || (stage == "posts" && info.Collection == postCollection)
		if stage == "social" {
			for _, collection := range collections[1:] {
				selected = selected || info.Collection == collection
			}
		}
		if !selected {
			continue
		}
		row, err := json.Marshal(map[string]any{
			"run_id": runID, "event_id": event.Source + "/" + strconv.FormatInt(info.Seq, 10),
			"event_time": info.Time, "collection": info.Collection, "operation": info.Operation, "raw": event.Raw,
		})
		if err != nil {
			return err
		}
		// Bound requests by bytes as well as event count; individual records vary in size.
		if rows.Len()+len(row)+1 > 2<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
		rows.Write(row)
		rows.WriteByte('\n')
		batchCount++
		if batchCount == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read capture near line %d: %w", lineNumber+1, err)
	}
	if err := flush(); err != nil {
		return err
	}
	if total == 0 {
		return fmt.Errorf("no events matched stage %s; capture a longer sample", stage)
	}
	return nil
}

func ingest(client *http.Client, endpoint, key, rows string, expected int) error {
	body, err := json.Marshal(map[string]any{
		"data":      map[string]string{"format": "json", "rows": rows},
		"statement": ingestStatement,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+"/v1/ingest", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	// A timeout can leave an unknown write outcome, so let the user replay explicitly.
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ingest outcome unknown: %w", err)
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("read ingest response (outcome unknown): %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingest HTTP %d (request ID %s): %s", resp.StatusCode, resp.Header.Get("X-Request-ID"), response)
	}
	var result struct {
		Inserted *int `json:"num_rows_inserted"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return fmt.Errorf("invalid ingest response (outcome unknown): %w", err)
	}
	if result.Inserted == nil || *result.Inserted != expected {
		return fmt.Errorf("ingest response did not confirm all %d rows: %s", expected, response)
	}
	return nil
}
