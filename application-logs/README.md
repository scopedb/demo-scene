# Investigate application errors

Keep a checkout simulator running, send its logs with an official ScopeDB SDK, and investigate a release-specific payment failure in Console. The scene has JavaScript, Go, and Rust implementations that produce the same events, share the same table, and answer the same questions. Each cycle creates 12 request logs — ten successes and two credit-card timeouts in release 1.9 — and repeats once per second until Ctrl+C.

Allow about 15 minutes after workspace setup. Payments are simulated and contain no customer data. The [illustrated tutorial](https://www.scopedb.io/docs/guides/investigate-application-errors) follows the complete investigation.

## Choose an SDK

| SDK        | Version | Runtime             | Implementation                       |
| ---------- | ------- | ------------------- | ------------------------------------ |
| JavaScript | 0.3.0   | Node.js 20 or later | [js/checkout.mjs](js/checkout.mjs)   |
| Go         | 0.7.0   | Go 1.26 or later    | [go/main.go](go/main.go)             |
| Rust       | 0.4.2   | Rust 1.91 or later  | [rust/src/main.rs](rust/src/main.rs) |

All three create the `guide_logs` schema and `requests` table when missing, write the same columns (`time timestamp`, `service string`, `level string`, `message string`, `var object`), and use `SCOPEDB_DEMO_RUN` as the run label. The [queries](queries) and expected results are shared. A new application should manage table definitions ahead of deployment rather than create them inside request handling.

## Connect and run

You need a ready ScopeDB Cloud workspace and one of the runtimes above. In Console, open **Connect**, copy **ScopeDB API**, and create a workspace API key. Keep the secret outside source control.

From the repository root, set the connection details:

```bash
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
export SCOPEDB_DEMO_RUN=checkout-first
```

Then start one implementation:

```bash
# JavaScript
cd application-logs/js
npm ci
npm start
```

```bash
# Go
cd application-logs/go
go mod download
go run .
```

```bash
# Rust
cd application-logs/rust
cargo run
```

The explicit run label is optional; without it each program generates a random UUID and prints it at startup. Use a new run ID when restarting so observations stay separate, including when switching languages. The program rejects a run ID that already has logs.

Keep the terminal running while you use Console. The SDK dispatches buffered logs within about one second. `send()` admits a row to a buffer; it does not acknowledge a commit. Ctrl+C stops production, flushes the stream, and reports confirmed rows. On failure, inspect the run before retrying; a lost response can follow a committed write.

## Investigate arriving requests

Open **Query** in the same workspace and run the files in [queries](queries):

1. `02-request-counts.scopeql` checks the complete request population and error count. After a complete cycle, expect at least 12 requests and two errors. Run again after a few seconds to include new arrivals.
2. `03-gateway-comparison.scopeql` compares releases and gateways. Each completed cycle gives each group three requests. Only release 1.9 with `credit_card` has errors: two per cycle, about 66.7%. A query during a partial delivery can show uneven counts.
3. `04-failed-requests.scopeql` returns up to ten recent failures. Double-click a `var` cell to inspect its request ID, release, and gateway.

`01-table-definition.scopeql` records the structure the programs create automatically. You do not need to run it separately. The queries filter `checkout-first`; change that filter when using another run ID.

In **Explore**, choose `guide_logs.requests`, include the producer's timestamps in the time range, and filter `var['run_id']::string = 'checkout-first' AND level = 'error'`. Select **Run** again to see new committed errors. Click a record for its details.

The result identifies where to investigate the simulated regression. Counts alone do not establish the cause of a real payment incident.

## Verify

Run the language's automated checks:

```bash
(cd application-logs/js && npm test)
(cd application-logs/go && go vet ./... && go test -race ./...)
(cd application-logs/rust && cargo clippy --all-targets -- -D warnings && cargo test)
```

Run one bounded cycle. Each run needs its own label because the run is written once:

```bash
(cd application-logs/js && SCOPEDB_DEMO_RUN=checkout-check-js npm start -- --cycles 1)
(cd application-logs/go && SCOPEDB_DEMO_RUN=checkout-check-go go run . -cycles 1)
(cd application-logs/rust && SCOPEDB_DEMO_RUN=checkout-check-rust cargo run -- --cycles 1)
```

Each commits 12 logs, including two errors, then exits. The default program runs continuously. Tests exercise repeated cycles and graceful interruption through the published SDK against a local protocol fixture. Checked with Node.js 24, Go 1.26.5, and Rust 1.97.1 with the pinned SDKs on 2026-10-27.
