package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func payload(seq int64, collection, operation, record string) string {
	return fmt.Sprintf(`{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"did":"did:example:alice","rkey":"example-record","time":"2026-10-10T00:00:00Z","witnessedAt":"2026-10-10T00:00:01Z","collection":%q,"operation":%q%s}`, seq, collection, operation, record)
}

func TestCaptureAndReplay(t *testing.T) {
	frames := []string{
		payload(9007199254740993, postCollection, "create", `,"record":{"text":"test","extension":{"number":9007199254740993}}`),
		payload(9007199254740994, "app.bsky.feed.like", "create", `,"record":{"subject":{"uri":"at://example/post/1","cid":"test"}}`),
		payload(9007199254740995, "app.bsky.graph.follow", "create", `,"record":{"subject":"did:plc:example"}`),
		payload(9007199254740996, postCollection, "delete", ""),
	}
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/network.bsky.jetstream.subscribeEvents" || r.URL.Query().Get("kinds") != "commit" {
			t.Errorf("unexpected subscription: %s", r.URL)
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"xrpc.v1.json"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, raw := range frames {
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"$type":"message","payload":`+raw+`}`)); err != nil {
				t.Error(err)
				return
			}
		}
		// Wait for the collector to close at its event limit, without a timing-dependent sleep.
		_, _, _ = conn.Read(ctx)
	}))
	defer stream.Close()
	path := filepath.Join(t.TempDir(), "events.ndjson")
	source := "ws" + strings.TrimPrefix(stream.URL, "http")
	if err := capture(source, path, time.Minute, len(frames)); err != nil {
		t.Fatal(err)
	}
	if err := capture(source, path, time.Minute, len(frames)); err == nil {
		t.Fatal("capture overwrote an existing file")
	}
	var inserted []map[string]json.RawMessage
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/databases/scopedb/schemas/guide_bluesky/tables/events_raw/rows" || r.ContentLength <= 0 || r.Header.Get("Content-Type") != "application/x-ndjson" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("invalid append request: method=%s path=%s length=%d", r.Method, r.URL.Path, r.ContentLength)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		for _, line := range lines {
			var row map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Error(err)
			}
			inserted = append(inserted, row)
		}
		fmt.Fprintf(w, `{"append_state":"committed","num_rows_inserted":%d}`, len(lines))
	}))
	defer api.Close()
	for _, stage := range []string{"posts", "social", "posts"} {
		if err := load(path, stage, "test-run", 1, api.Client(), api.URL, "test-key"); err != nil {
			t.Fatal(err)
		}
	}
	if len(inserted) != 6 {
		t.Fatalf("expected two posts, two social events, and two replayed posts; got %d", len(inserted))
	}
	for _, row := range inserted {
		var imported string
		if err := json.Unmarshal(row["ingested_at"], &imported); err != nil {
			t.Fatal(err)
		}
		if _, err := time.Parse(time.RFC3339Nano, imported); err != nil {
			t.Fatalf("invalid import timestamp: %v", err)
		}
		delete(row, "ingested_at")
	}
	if !reflect.DeepEqual(inserted[0], inserted[4]) || !reflect.DeepEqual(inserted[1], inserted[5]) {
		t.Fatal("replay changed event IDs or payloads")
	}
	if string(inserted[0]["raw"]) != frames[0] || !strings.Contains(string(inserted[0]["event_id"]), "9007199254740993") {
		t.Fatal("unknown fields or large integer precision were lost")
	}
	if string(inserted[0]["event_time"]) != `"2026-10-10T00:00:01Z"` {
		t.Fatal("event_time did not use witnessedAt")
	}
	if string(inserted[1]["operation"]) != `"delete"` || strings.Contains(string(inserted[1]["raw"]), `"record"`) {
		t.Fatal("delete without a record was not preserved")
	}
}

func TestAppendFailureStopsWithoutRetry(t *testing.T) {
	for _, response := range []struct {
		name   string
		status int
		body   string
	}{
		{"unknown write outcome", 503, `{"error":{"message":"unavailable"}}`},
		{"missing acknowledgement", 200, `{}`},
		{"partial acknowledgement", 200, `{"append_state":"committed","num_rows_inserted":0}`},
		{"missing commit state", 200, `{"num_rows_inserted":1}`},
		{"unknown commit state", 200, `{"append_state":"unknown","num_rows_inserted":1}`},
		{"rejected rows", 422, `{"append_state":"rejected","row_errors":[{"row_index":0,"column":"raw","message":"invalid object"}]}`},
	} {
		t.Run(response.name, func(t *testing.T) {
			requests := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.WriteHeader(response.status)
				io.WriteString(w, response.body)
			}))
			defer api.Close()
			path := filepath.Join(t.TempDir(), "events.ndjson")
			event := capturedEvent{Source: "wss://example.test", Raw: json.RawMessage(payload(1, postCollection, "delete", ""))}
			line, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(line, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			err = load(path, "posts", "test-run", 1, api.Client(), api.URL, "test-key")
			if err == nil {
				t.Fatal("an unconfirmed write was reported as successful")
			}
			if response.name == "rejected rows" && !strings.Contains(err.Error(), "append rejected") {
				t.Fatalf("structured rejection was not preserved: %v", err)
			}
			if requests != 1 {
				t.Fatalf("automatically retried an uncertain write: %d requests", requests)
			}
		})
	}
}

func TestReplayAfterPartiallyAcknowledgedImport(t *testing.T) {
	// Simulate an upstream commit whose acknowledgement never reaches the loader.
	var stored []string
	requests := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var row map[string]json.RawMessage
		if err := json.Unmarshal(body, &row); err != nil {
			t.Error(err)
			return
		}
		delete(row, "ingested_at")
		stable, _ := json.Marshal(row)
		stored = append(stored, string(stable))
		if requests == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, `{"append_state":"committed","num_rows_inserted":1}`)
	}))
	defer api.Close()
	path := filepath.Join(t.TempDir(), "events.ndjson")
	var lines []string
	for i := int64(1); i <= 3; i++ {
		line, err := json.Marshal(capturedEvent{Source: "wss://example.test", Raw: json.RawMessage(payload(i, postCollection, "delete", ""))})
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(line))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err := load(path, "all", "recovery", 1, api.Client(), api.URL, "test-key")
	if err == nil || !strings.Contains(err.Error(), "1 rows confirmed before this batch") || requests != 2 {
		t.Fatalf("expected one acknowledged row and a stopped second batch, got %v (%d requests)", err, requests)
	}
	if err := load(path, "all", "recovery", 1, api.Client(), api.URL, "test-key"); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 5 || stored[0] != stored[2] || stored[1] != stored[3] {
		t.Fatal("recovery did not preserve identifiers and raw payloads across an ambiguous write")
	}
}

func TestCaptureSkipsAdvisoriesAndRetainsUpdates(t *testing.T) {
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"xrpc.v1.json"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		frames := []string{
			`{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"FutureCursor"}`,
			payload(1, postCollection, "create", `,"record":{"text":"Constructed original"}`),
			payload(2, postCollection, "update", `,"record":{"text":"Constructed edit"}`),
		}
		for _, raw := range frames {
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"$type":"message","payload":`+raw+`}`)); err != nil {
				t.Error(err)
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	}))
	defer stream.Close()
	path := filepath.Join(t.TempDir(), "events.ndjson")
	if err := capture("ws"+strings.TrimPrefix(stream.URL, "http"), path, time.Minute, 2); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], `"operation":"update"`) || strings.Contains(string(data), "FutureCursor") {
		t.Fatal("advisory consumed the event limit or update was discarded")
	}
}
