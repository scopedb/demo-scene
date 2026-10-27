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
    if (request.url === "/v1/statements") {
      const input = JSON.parse(text);
      const count = input.statement.includes("AGGREGATE count()");
      response.end(JSON.stringify({
        status: "finished", statement_id: input.statement_id ?? randomUUID(),
        created_at: "2026-10-27T00:00:00Z",
        progress: Object.fromEntries(["compute_seconds", "total_percentage", "nanos_from_submitted", "nanos_from_started", "total_stages", "total_partitions", "total_rows", "total_compressed_bytes", "total_uncompressed_bytes", "scanned_stages", "scanned_partitions", "scanned_rows", "scanned_compressed_bytes", "scanned_uncompressed_bytes", "skipped_partitions", "skipped_rows", "skipped_compressed_bytes", "skipped_uncompressed_bytes"].map(field => [field, 0])),
        result_set: { format: "json", metadata: {
          fields: count ? [{ name: "requests", data_type: "int" }] : [],
          num_rows: count ? 1 : 0,
        }, rows: count ? [["0"]] : [] },
      }));
      return;
    }
    if (request.url === "/v1/catalog/databases/scopedb/schemas/guide_logs/tables/requests/ingest") {
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
  child = spawn(process.execPath, ["checkout.mjs", ...args], {
    cwd: new URL(".", import.meta.url),
    env: { ...process.env, SCOPEDB_ENDPOINT: `http://127.0.0.1:${server.address().port}`, SCOPEDB_API_KEY: "test-key", SCOPEDB_DEMO_RUN: "test-live" },
  });
  t.after(() => child.kill("SIGKILL"));
  let output = "";
  let errors = "";
  child.stdout.on("data", chunk => { output += chunk; });
  child.stderr.on("data", chunk => { errors += chunk; });
  const [code, signal] = await once(child, "exit", { signal: AbortSignal.timeout(15000) });
  assert.equal(code, 0, errors);
  assert.equal(signal, null, errors);
  const committed = output.match(/Committed (\d+) logs/);
  assert.ok(committed, output);
  assert.equal(Number(committed[1]), rows.length);
  assert.equal(new Set(rows.map(row => row.var.request_id)).size, rows.length);
  return rows;
}

test("multiple cycles keep sending successful and failed requests", async t => {
  const rows = await exercise(t, ["--cycles", "2"]);
  assert.equal(rows.length, 24);
  assert.equal(rows.filter(row => row.level === "error").length, 4);
  assert.ok(rows.filter(row => row.level === "error").every(row => row.var.release === "1.9" && row.var.gateway === "credit_card"));
});

test("the default producer keeps running and flushes on Ctrl+C", async t => {
  const rows = await exercise(t, [], true);
  assert.ok(rows.length >= 12);
  assert.equal(rows.length % 12, 0);
});
