# Trace agent runs

Keep an agent simulator running, stream execution spans with the Rust SDK, and investigate slow runs in Console. Each cycle creates three traces and 13 spans: two clean four-span traces and one five-span trace whose search times out and succeeds on retry. The producer repeats once per second until Ctrl+C.

Allow about 15 minutes after workspace setup. Model and tool calls are simulated; no model account, search provider, or user prompts are needed. The [illustrated tutorial](https://www.scopedb.io/docs/guides/trace-agent-runs) follows the complete investigation.

## Connect and run

You need a ready ScopeDB Cloud workspace, Rust 1.91.0 or later, and Git, on macOS, Linux, or WSL. In Console, open **Connect**, copy **ScopeDB API**, and create a workspace API key. Keep the secret outside source control.

From the repository root:

```bash
cd agent-runs
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
SCOPEDB_DEMO_RUN=agent-first cargo run
```

Cargo installs the locked dependencies, including `scopedb-client` 0.4.2. The program creates the `guide_agents` schema and `spans` table if they do not exist, in the default `scopedb` database. Its columns are `time timestamp`, `trace_id string`, `span_id string`, `step string`, `status string`, `duration_ms int`, and `var object`. A new application should manage table definitions ahead of deployment rather than create them inside request handling.

Keep the terminal running while you use Console. The SDK dispatches buffered spans within about one second. `send()` admits a row to a buffer; it does not acknowledge a commit. Ctrl+C or SIGTERM stops production after the current cycle, flushes the stream, and reports confirmed rows as `Committed <N> spans. Run ID: <runId>`. On failure, inspect the run before retrying; a lost response can follow a committed write. Use a new run ID when restarting so observations remain separate. Run IDs contain 1–100 ASCII letters, digits, or hyphens; omitting `SCOPEDB_DEMO_RUN` generates a UUID. The startup check rejects IDs with existing rows; use a different ID for every concurrent producer as well.

## Investigate arriving spans

Open **Query** in the same workspace and run the files in [queries](queries):

1. `02-span-counts.scopeql` checks the complete population. Each completed cycle contributes 13 spans, three distinct traces, and one failed search span. Run again after a few seconds to include new arrivals.
2. `03-step-performance.scopeql` compares steps and tools. Errors and retries are isolated to `search` with `web_search`; the query tool finishes normally.
3. `04-trace-timeline.scopeql` returns all five spans in `trace-03`, the first trace requiring a retry, with the latest span first. Later affected traces are `trace-06`, `trace-09`, and so on. Trace IDs increase across cycles; span IDs are unique within a run. Always filter by run ID as well. Use `ORDER BY time, span_id` to read the timeline from the beginning.

After one complete cycle, the step comparison is:

| step   | tool          | spans | errors | retries | max_ms |
| ------ | ------------- | ----- | ------ | ------- | ------ |
| search | web_search    | 4     | 1      | 1       | 1200   |
| answer | NULL          | 3     | 0      | 0       | 300    |
| query  | scopedb_query | 3     | 0      | 0       | 250    |
| plan   | NULL          | 3     | 0      | 0       | 60     |

`01-table-definition.scopeql` records the structure the program creates automatically. You do not need to run it separately. The queries filter `agent-first`; change that filter when using another run ID. Counts grow while streaming, and a query during partial delivery may show an incomplete cycle.

`time` is the simulated span start, and `duration_ms` is its simulated elapsed time. Search and query start together after planning; the third trace's search takes 1200 ms to fail, retries for 180 ms, then joins the completed query before the answer begins. Timestamps place completed traces in the recent past, so overlapping cycles are possible. These durations illustrate execution order, rather than measure SDK or database performance. `var` retains the run, model, attempt, tool, and error details.

In **Explore**, choose `guide_agents.spans`, include the producer's timestamps in the time range, and filter `var['run_id']::string = 'agent-first' AND status = 'error'`. Select **Run** again to see new committed failures. Click a record for its details.

## Verify

```bash
cargo test --locked
SCOPEDB_DEMO_RUN=agent-check cargo run -- --cycles 1
```

The explicit one-cycle check commits 13 spans across three traces, including one failed search and its successful retry, then exits. `--cycles 0` and the default mode run continuously. Tests exercise repeated cycles, buffered shutdown on SIGINT and SIGTERM, run-ID validation, and unconfirmed delivery through the published SDK against a local HTTP fixture. Checked with Rust 1.91.0 and the pinned SDK on 2026-10-27; the local fixture does not verify a live workspace.
