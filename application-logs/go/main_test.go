package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	scopedb "github.com/scopedb/goscopedb"
)

type appendRecorder struct {
	mu       sync.Mutex
	rows     []logRow
	onAppend func()
}

func (r *appendRecorder) handler(t *testing.T, w http.ResponseWriter, request *http.Request) {
	t.Helper()
	if request.Header.Get("Authorization") != "Bearer test-key" {
		t.Errorf("incorrect credentials: %q", request.Header.Get("Authorization"))
	}
	body := io.Reader(request.Body)
	if request.Header.Get("Content-Encoding") == "gzip" {
		reader, err := gzip.NewReader(request.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer reader.Close()
		body = reader
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Error(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/v1/catalog/databases/scopedb/schemas/guide_logs":
		if request.Method != http.MethodPut {
			t.Errorf("unexpected catalog method: %s", request.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var schema scopedb.CatalogSchema
		if err := json.Unmarshal(data, &schema); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if schema.Name != schemaName || len(schema.Tables) != 1 || schema.Tables[0].Name != tableName {
			t.Errorf("unexpected catalog schema: %+v", schema)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("ETag", `"1"`)
		fmt.Fprintf(w, `{"before":null,"after":%s,"destructive":false}`, string(data))
		return
	case "/v1/statements":
		var input struct {
			Statement string `json:"statement"`
		}
		if err := json.Unmarshal(data, &input); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !strings.Contains(input.Statement, "AGGREGATE count()") {
			t.Errorf("unexpected statement: %s", input.Statement)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fields, rows, numRows := `[{"name":"requests","data_type":"int"}]`, `[["0"]]`, 1
		fmt.Fprintf(w, `{"status":"finished","statement_id":"01989a4e-4ee2-7e63-87a5-65ac3b5161dc",`+
			`"created_at":"2026-10-27T00:00:00Z","progress":{"compute_seconds":0,"total_percentage":100,`+
			`"nanos_from_submitted":0,"nanos_from_started":0,"total_stages":0,"total_partitions":0,`+
			`"total_rows":0,"total_compressed_bytes":0,"total_uncompressed_bytes":0,"scanned_stages":0,`+
			`"scanned_partitions":0,"scanned_rows":0,"scanned_compressed_bytes":0,`+
			`"scanned_uncompressed_bytes":0,"skipped_partitions":0,"skipped_rows":0,`+
			`"skipped_compressed_bytes":0,"skipped_uncompressed_bytes":0},`+
			`"result_set":{"format":"json","metadata":{"fields":%s,"num_rows":%d},"rows":%s}}`,
			fields, numRows, rows)
		return
	case "/v1/catalog/databases/scopedb/schemas/guide_logs/tables/requests/ingest":
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		r.mu.Lock()
		for _, line := range lines {
			var row logRow
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				r.mu.Unlock()
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.rows = append(r.rows, row)
		}
		onAppend := r.onAppend
		r.mu.Unlock()
		fmt.Fprintf(w, `{"status":"committed","num_rows_inserted":%d}`, len(lines))
		if onAppend != nil {
			onAppend()
		}
		return
	default:
		http.NotFound(w, request)
	}
}

func testClient(t *testing.T, recorder *appendRecorder) *scopedb.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.handler(t, w, r)
	}))
	t.Cleanup(server.Close)
	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: server.URL, APIKey: "test-key", Compression: scopedb.CompressionGzip, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestRunProducesTwelveLogsPerCycle(t *testing.T) {
	recorder := &appendRecorder{}
	client := testClient(t, recorder)
	var out bytes.Buffer
	if err := run(t.Context(), client, "test-live", 2, &out); err != nil {
		t.Fatal(err)
	}

	recorder.mu.Lock()
	rows := append([]logRow(nil), recorder.rows...)
	recorder.mu.Unlock()
	if len(rows) != 24 {
		t.Fatalf("expected 24 logs, got %d", len(rows))
	}
	errors := 0
	ids := map[string]bool{}
	for _, row := range rows {
		if row.Level == "error" {
			errors++
			if row.Var["release"] != "1.9" || row.Var["gateway"] != "credit_card" {
				t.Fatalf("unexpected failed request: %+v", row)
			}
		}
		ids[row.Var["request_id"]] = true
	}
	if errors != 4 {
		t.Fatalf("expected 4 errors, got %d", errors)
	}
	if len(ids) != len(rows) {
		t.Fatalf("request IDs were not unique: %d IDs for %d rows", len(ids), len(rows))
	}
	if !strings.Contains(out.String(), "Committed 24 logs") {
		t.Fatalf("missing confirmation: %s", out.String())
	}
}

func TestRunFlushesWhenStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var once sync.Once
	recorder := &appendRecorder{onAppend: func() { once.Do(cancel) }}
	client := testClient(t, recorder)
	var out bytes.Buffer
	if err := run(ctx, client, "test-stop", 0, &out); err != nil {
		t.Fatal(err)
	}

	recorder.mu.Lock()
	rows := len(recorder.rows)
	recorder.mu.Unlock()
	if rows == 0 || rows%12 != 0 {
		t.Fatalf("expected complete cycles on interruption, got %d logs", rows)
	}
	if !strings.Contains(out.String(), fmt.Sprintf("Committed %d logs", rows)) {
		t.Fatalf("missing confirmation: %s", out.String())
	}
}
