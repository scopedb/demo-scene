# Investigate application errors

Keep a checkout simulator running, send its logs with the JavaScript SDK, and investigate a release-specific payment failure in Console. Each cycle creates 12 request logs: ten successes and two credit-card timeouts in release 1.9. The producer repeats once per second until Ctrl+C.

Allow about 15 minutes after workspace setup. Payments are simulated and contain no customer data. The [illustrated tutorial](https://www.scopedb.io/docs/guides/investigate-application-errors) follows the complete investigation.

## Connect and run

You need a ready ScopeDB Cloud workspace, Node.js 20 or later, and Git. In Console, open **Connect**, copy **ScopeDB API**, and create a workspace API key. Keep the secret outside source control.

From the repository root:

```bash
cd application-logs
npm ci
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
SCOPEDB_DEMO_RUN=checkout-first npm start
```

The program creates the `guide_logs` schema and `requests` table if they do not exist. Its columns are `time timestamp`, `service string`, `level string`, `message string`, and `var object`. A new application should manage table definitions ahead of deployment rather than create them inside request handling.

Keep the terminal running while you use Console. The SDK dispatches buffered logs within about one second. `send()` admits a row to a buffer; it does not acknowledge a commit. Ctrl+C stops production, flushes the stream, and reports confirmed rows. On failure, inspect the run before retrying; a lost response can follow a committed write. Use a new run ID when restarting so observations remain separate.

## Investigate arriving requests

Open **Query** in the same workspace and run the files in [queries](queries):

1. `02-request-counts.scopeql` checks the complete request population and error count. After a complete cycle, expect at least 12 requests and two errors. Run again after a few seconds to include new arrivals.
2. `03-gateway-comparison.scopeql` compares releases and gateways. Each completed cycle gives each group three requests. Only release 1.9 with `credit_card` has errors: two per cycle, about 66.7%. A query during a partial delivery can show uneven counts.
3. `04-failed-requests.scopeql` returns up to ten recent failures. Double-click a `var` cell to inspect its request ID, release, and gateway.

`01-table-definition.scopeql` records the structure the program creates automatically. You do not need to run it separately. The queries filter `checkout-first`; change that filter when using another run ID.

In **Explore**, choose `guide_logs.requests`, include the producer's timestamps in the time range, and filter `var['run_id']::string = 'checkout-first' AND level = 'error'`. Select **Run** again to see new committed errors. Click a record for its details.

The result identifies where to investigate the simulated regression. Counts alone do not establish the cause of a real payment incident.

## Verify

```bash
npm test
SCOPEDB_DEMO_RUN=checkout-check npm start -- --cycles 1
```

The explicit one-cycle check commits 12 logs, including two errors, then exits. The default program runs continuously. Tests exercise repeated cycles and graceful interruption through the published SDK against a local protocol fixture. Checked with Node.js 20 and the pinned SDK on 2026-10-27.
