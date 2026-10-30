package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/coder/websocket"
	scopedb "github.com/scopedb/goscopedb"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func payload(seq int64, operation string) string {
	return fmt.Sprintf(`{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"did":"did:example:author","rkey":"post-1","time":"2026-10-10T00:00:00Z","witnessedAt":"2026-10-10T00:00:01Z","collection":"app.bsky.feed.post","operation":%q,"record":{"text":"Constructed post","extension":{"number":9007199254740993}}}`, seq, operation)
}

type appendRecorder struct {
	mu       sync.Mutex
	rows     []map[string]json.RawMessage
	requests int
	fail     bool
	onAppend func()
}

func (r *appendRecorder) handler(t *testing.T, w http.ResponseWriter, request *http.Request) {
	t.Helper()
	if request.URL.Path != "/v1/catalog/databases/scopedb/schemas/guide_bluesky/tables/events_raw/ingest" || request.Header.Get("Authorization") != "Bearer test-key" {
		t.Errorf("incorrect SDK table or credentials: %s", request.URL.Path)
	}
	body, err := gzip.NewReader(request.Body)
	if err != nil {
		t.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Error(err)
		return
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	r.mu.Lock()
	r.requests++
	for _, line := range lines {
		var row map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Error(err)
		}
		delete(row, "ingested_at")
		r.rows = append(r.rows, row)
	}
	fail, onAppend := r.fail, r.onAppend
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"code":"unavailable","message":"response lost","details":{"status":"unknown"}}`)
	} else {
		fmt.Fprintf(w, `{"status":"committed","num_rows_inserted":%d}`, len(lines))
	}
	if onAppend != nil {
		onAppend()
	}
}

func sdkTable(t *testing.T, recorder *appendRecorder) *scopedb.Table {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { recorder.handler(t, w, r) }))
	t.Cleanup(server.Close)
	client, err := scopedb.NewClient(scopedb.Config{Endpoint: server.URL, APIKey: "test-key", Compression: scopedb.CompressionGzip, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	table := client.Table("events_raw")
	table.Schema = "guide_bluesky"
	return table
}

func sourceServer(t *testing.T, produce func(context.Context, *websocket.Conn, *http.Request)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/network.bsky.jetstream.subscribeEvents" || r.URL.Query().Get("kinds") != "commit" {
			t.Errorf("unexpected subscription: %s", r.URL)
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"xrpc.v1.json"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		produce(ctx, conn, r)
	}))
	t.Cleanup(server.Close)
	return server
}

func send(t *testing.T, ctx context.Context, conn *websocket.Conn, raw string) {
	t.Helper()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"$type":"message","payload":`+raw+`}`)); err != nil {
		t.Error(err)
	}
}

func TestStreamAndReplayPreserveMutations(t *testing.T) {
	frames := []string{payload(9007199254740993, "create"), payload(9007199254740994, "update"), payload(9007199254740995, "delete")}
	server := sourceServer(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		send(t, ctx, conn, `{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"FutureCursor"}`)
		for _, frame := range frames {
			send(t, ctx, conn, frame)
		}
		_, _, _ = conn.Read(ctx)
	})
	recorder := &appendRecorder{}
	table := sdkTable(t, recorder)
	source := "ws" + strings.TrimPrefix(server.URL, "http")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := stream(ctx, source, "test-run", 3, 2, table); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "replay.ndjson")
	var saved strings.Builder
	for _, raw := range frames {
		line, _ := json.Marshal(capturedEvent{Source: source, Raw: json.RawMessage(raw)})
		saved.Write(line)
		saved.WriteByte('\n')
	}
	if err := os.WriteFile(file, []byte(saved.String()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := load(ctx, file, "test-run", 2, table); err != nil {
		t.Fatal(err)
	}
	if len(recorder.rows) != 6 || !reflect.DeepEqual(recorder.rows[:3], recorder.rows[3:]) {
		t.Fatal("replay changed mutation identity or preserved payloads")
	}
	if string(recorder.rows[0]["raw"]) != frames[0] || !strings.Contains(string(recorder.rows[0]["event_id"]), "9007199254740993") || string(recorder.rows[0]["event_time"]) != `"2026-10-10T00:00:01Z"` {
		t.Fatal("large integers, raw fields, or source observation time changed")
	}
}

func TestUnknownAppendStopsAndSavesRecovery(t *testing.T) {
	recorder := &appendRecorder{fail: true}
	table := sdkTable(t, recorder)
	writer := batchWriter{table: table, captureID: "recover", batchSize: 1, recoveryDir: t.TempDir()}
	event := capturedEvent{Source: "wss://example.test", Raw: json.RawMessage(payload(1, "create"))}
	info, _ := inspect(event)
	if err := writer.add(t.Context(), event, info); err == nil {
		t.Fatal("unknown write was reported as committed")
	}
	if recorder.requests != 1 || writer.committed != 0 {
		t.Fatal("an unknown append was retried or counted as acknowledged")
	}
	files, err := filepath.Glob(filepath.Join(writer.recoveryDir, "*.ndjson"))
	if err != nil || len(files) != 1 {
		t.Fatalf("unconfirmed batch not saved: %v", err)
	}
	recorder.fail = false
	if err := load(t.Context(), files[0], "recover", 1, table); err != nil {
		t.Fatal(err)
	}
	if len(recorder.rows) != 2 || !reflect.DeepEqual(recorder.rows[0], recorder.rows[1]) {
		t.Fatal("recovery did not retain the original identity after an ambiguous commit")
	}
}

func TestIdleSourceFlushesBeforeAnotherEventArrives(t *testing.T) {
	firstAppend := make(chan struct{})
	var once sync.Once
	recorder := &appendRecorder{onAppend: func() { once.Do(func() { close(firstAppend) }) }}
	table := sdkTable(t, recorder)
	server := sourceServer(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		send(t, ctx, conn, payload(1, "create"))
		select {
		case <-firstAppend:
		case <-ctx.Done():
			t.Error("one buffered event was never delivered while the source was idle")
			return
		}
		send(t, ctx, conn, payload(2, "create"))
		_, _, _ = conn.Read(ctx)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := stream(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), "idle", 2, 500, table); err != nil {
		t.Fatal(err)
	}
	if len(recorder.rows) != 2 || recorder.requests != 2 {
		t.Fatal("idle flush or final partial-batch flush lost an event")
	}
}

func TestReconnectUsesLastCommittedSequence(t *testing.T) {
	var connections atomic.Int32
	server := sourceServer(t, func(ctx context.Context, conn *websocket.Conn, r *http.Request) {
		if connections.Add(1) == 1 {
			send(t, ctx, conn, payload(1, "create"))
			conn.Close(websocket.StatusGoingAway, "restart")
			return
		}
		if r.URL.Query().Get("cursor") != "1" {
			t.Errorf("resumed without committed cursor: %s", r.URL)
		}
		// The source cursor is inclusive; the view can deduplicate this delivery.
		send(t, ctx, conn, payload(1, "create"))
		send(t, ctx, conn, payload(2, "update"))
		_, _, _ = conn.Read(ctx)
	})
	recorder := &appendRecorder{}
	table := sdkTable(t, recorder)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := stream(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), "resume", 3, 500, table); err != nil {
		t.Fatal(err)
	}
	if connections.Load() != 2 || len(recorder.rows) != 3 || !reflect.DeepEqual(recorder.rows[0], recorder.rows[1]) {
		t.Fatal("reconnect lost the cursor or changed an inclusive replay's identity")
	}
}

func TestInterruptLetsInFlightAppendSettle(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sourceClosed := make(chan struct{})
	source := sourceServer(t, func(sourceCtx context.Context, conn *websocket.Conn, _ *http.Request) {
		send(t, sourceCtx, conn, payload(1, "create"))
		_, _, _ = conn.Read(sourceCtx)
		close(sourceClosed)
	})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		cancel()
		select {
		case <-sourceClosed:
		case <-r.Context().Done():
			t.Error("interrupt cancelled an in-flight append instead of settling it")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"status":"committed","num_rows_inserted":1}`)
	}))
	defer api.Close()
	client, err := scopedb.NewClient(scopedb.Config{Endpoint: api.URL, APIKey: "test-key", HTTPClient: api.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	table := client.Table("events_raw")
	table.Schema = "guide_bluesky"
	if err := stream(ctx, "ws"+strings.TrimPrefix(source.URL, "http"), "interrupt", 0, 1, table); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureSampleAppliesCatalogAndLoadsSample(t *testing.T) {
	var catalogApplied bool
	recorder := &appendRecorder{}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/catalog/databases/scopedb/schemas/guide_bluesky":
			if r.Method != http.MethodPut {
				t.Errorf("unexpected catalog method: %s", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var schema scopedb.CatalogSchema
			if err := json.Unmarshal(data, &schema); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if schema.Name != "guide_bluesky" || len(schema.Tables) != 1 || schema.Tables[0].Name != "events_raw" || len(schema.Views) != 1 || schema.Views[0].Name != "events" {
				t.Errorf("unexpected catalog schema: %+v", schema)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			catalogApplied = true
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("ETag", `"1"`)
			fmt.Fprintf(w, `{"before":null,"after":%s,"destructive":false}`, string(data))
		case "/v1/statements":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"status":"finished","statement_id":"01989a4e-4ee2-7e63-87a5-65ac3b5161dc","created_at":"2026-10-27T00:00:00Z","progress":{"compute_seconds":0,"total_percentage":100,"nanos_from_submitted":0,"nanos_from_started":0,"total_stages":0,"total_partitions":0,"total_rows":0,"total_compressed_bytes":0,"total_uncompressed_bytes":0,"scanned_stages":0,"scanned_partitions":0,"scanned_rows":0,"scanned_compressed_bytes":0,"scanned_uncompressed_bytes":0,"skipped_partitions":0,"skipped_rows":0,"skipped_compressed_bytes":0,"skipped_uncompressed_bytes":0},"result_set":{"format":"json","metadata":{"fields":[{"name":"events","data_type":"int"}],"num_rows":1},"rows":[["0"]]}}`)
		default:
			recorder.handler(t, w, r)
		}
	}))
	defer api.Close()
	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: api.URL, APIKey: "test-key", Compression: scopedb.CompressionGzip, HTTPClient: api.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	table := client.Table("events_raw")
	table.Schema = "guide_bluesky"
	if err := ensureSample(t.Context(), client, table); err != nil {
		t.Fatal(err)
	}
	if !catalogApplied || len(recorder.rows) != 16 {
		t.Fatalf("expected catalog apply and 16 sample rows, got catalogApplied=%v rows=%d", catalogApplied, len(recorder.rows))
	}
}
