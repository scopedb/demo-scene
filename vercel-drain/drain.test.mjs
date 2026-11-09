import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import { test } from "node:test";
import { gunzipSync } from "node:zlib";

async function exercise(t, args, stopAfterAppend = false) {
  const rows = [];
  let child;
  let signalled = false;
  const server = http.createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    const text = (request.headers["content-encoding"] === "gzip" ? gunzipSync(body) : body).toString();
    response.setHeader("Content-Type", "application/json");
    if (request.method === "PUT" && request.url === "/v1/catalog/databases/scopedb/schemas/guide_vercel") {
      const schema = JSON.parse(text);
      assert.equal(schema.name, "guide_vercel");
      assert.equal(schema.tables?.length, 1);
      assert.equal(schema.tables[0].name, "vercel_events");
      assert.equal(schema.views?.length, 4);
      response.setHeader("ETag", '"1"');
      response.end(JSON.stringify({ before: null, after: schema, destructive: false }));
      return;
    }
    if (request.url === "/v1/statements") {
      const input = JSON.parse(text);
      assert.ok(input.statement.includes("AGGREGATE count()"), `unexpected statement: ${input.statement}`);
      response.end(JSON.stringify({
        status: "finished", statement_id: input.statement_id ?? randomUUID(),
        created_at: "2026-11-09T00:00:00Z",
        progress: Object.fromEntries(["compute_seconds", "total_percentage", "nanos_from_submitted", "nanos_from_started", "total_stages", "total_partitions", "total_rows", "total_compressed_bytes", "total_uncompressed_bytes", "scanned_stages", "scanned_partitions", "scanned_rows", "scanned_compressed_bytes", "scanned_uncompressed_bytes", "skipped_partitions", "skipped_rows", "skipped_compressed_bytes", "skipped_uncompressed_bytes"].map(field => [field, 0])),
        result_set: { format: "json", metadata: {
          fields: [{ name: "events", data_type: "int" }],
          num_rows: 1,
        }, rows: [["0"]] },
      }));
      return;
    }
    if (request.url === "/v1/catalog/databases/scopedb/schemas/guide_vercel/tables/vercel_events/ingest") {
      const batch = text.trim().split("\n").map(JSON.parse);
      rows.push(...batch);
      response.end(JSON.stringify({ status: "committed", num_rows_inserted: batch.length }));
      if (stopAfterAppend && !signalled) {
        signalled = true;
        child.kill("SIGINT");
      }
      return;
    }
    response.writeHead(404).end();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise(resolve => server.close(resolve)));
  child = spawn(process.execPath, ["drain.mjs", ...args], {
    cwd: new URL(".", import.meta.url),
    env: { ...process.env, SCOPEDB_ENDPOINT: `http://127.0.0.1:${server.address().port}`, SCOPEDB_API_KEY: "test-key", SCOPEDB_DEMO_RUN: "test-vercel" },
  });
  t.after(() => child.kill("SIGKILL"));
  let output = "";
  let errors = "";
  child.stdout.on("data", chunk => { output += chunk; });
  child.stderr.on("data", chunk => { errors += chunk; });
  const [code, signal] = await once(child, "exit", { signal: AbortSignal.timeout(15000) });
  assert.equal(code, 0, errors);
  assert.equal(signal, null, errors);
  const committed = output.match(/Committed (\d+) events/);
  assert.ok(committed, output);
  assert.equal(Number(committed[1]), rows.length);
  return rows;
}

test("multiple cycles normalize signed Vercel Drain batches and Next.js after() events", async t => {
  const rows = await exercise(t, ["--cycles", "2"]);
  assert.equal(rows.length, 24);
  assert.equal(rows.filter(row => row.event_type === "vercel.request").length, 16);
  assert.equal(rows.filter(row => row.event_type === "vercel.log").length, 2);
  assert.equal(rows.filter(row => row.event_type === "vercel.pageview").length, 2);
  assert.equal(rows.filter(row => row.event_type === "vercel.web_vital").length, 2);
  assert.equal(rows.filter(row => row.event_type === "vercel.custom_event").length, 2);
  const errors = rows.filter(row => row.level === "error");
  assert.equal(errors.length, 4);
  assert.ok(errors.every(row => row.path === "/api/chat" && row.attributes.trace_id === "4bf92f3577b34da6a3ce929d0e0e4736"));
});

test("the default producer keeps running and flushes on Ctrl+C", async t => {
  const rows = await exercise(t, [], true);
  assert.ok(rows.length >= 12);
  assert.equal(rows.length % 12, 0);
});
