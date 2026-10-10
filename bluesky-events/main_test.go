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
	return fmt.Sprintf(`{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"time":"2026-10-10T00:00:00Z","witnessedAt":"2026-10-10T00:00:01Z","collection":%q,"operation":%q%s}`, seq, collection, operation, record)
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
		if r.Method != http.MethodPost || r.URL.Path != "/v1/ingest" || r.ContentLength <= 0 || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("invalid ingest request: method=%s path=%s length=%d", r.Method, r.URL.Path, r.ContentLength)
		}
		var body struct {
			Data struct {
				Rows string `json:"rows"`
			} `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		lines := strings.Split(strings.TrimSpace(body.Data.Rows), "\n")
		for _, line := range lines {
			var row map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Error(err)
			}
			inserted = append(inserted, row)
		}
		fmt.Fprintf(w, `{"num_rows_inserted":%d}`, len(lines))
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

func TestIngestFailureStopsWithoutRetry(t *testing.T) {
	for _, response := range []struct {
		name   string
		status int
		body   string
	}{
		{"unknown write outcome", 503, `{"error":{"message":"unavailable"}}`},
		{"missing acknowledgement", 200, `{}`},
		{"partial acknowledgement", 200, `{"num_rows_inserted":0}`},
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
			if err := load(path, "posts", "test-run", 1, api.Client(), api.URL, "test-key"); err == nil {
				t.Fatal("an unconfirmed write was reported as successful")
			}
			if requests != 1 {
				t.Fatalf("automatically retried an uncertain write: %d requests", requests)
			}
		})
	}
}
