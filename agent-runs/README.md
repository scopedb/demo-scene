# Trace Codex runs

Run a real Codex task and inspect its execution in ScopeDB Console. Compare model sampling attempts and tool results, then follow the work behind the answer. Normal execution is the starting point; an optional exercise investigates a tool failure.

Allow about 15 minutes after workspace and Codex setup. Model calls use the provider's normal allowance. The [illustrated tutorial](https://www.scopedb.io/docs/guides/trace-agent-runs) follows the investigation.

## Connect and run

You need a ready ScopeDB Cloud workspace, Codex CLI 0.160.0 or later with access to a model, Python 3.11 or later, and Git on macOS arm64 or Linux amd64/arm64, including WSL. Configure Codex authentication with `codex login` if needed. In Console, open **Connect**, copy **ScopeDB API**, and create a workspace API key.

From the repository root:

```bash
cd agent-runs
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
SCOPEDB_DEMO_RUN=agent-first python3 run.py
```

The launcher downloads the official Telescope v0.3.0 archive on first use, checks its pinned SHA-256, and caches the executable outside this checkout. It creates `guide_agents.codex_spans` and `guide_agents.codex_events` in the default `scopedb` database, starts Telescope on available loopback ports, and asks Codex:

> Compare how bluesky-events and application-logs handle graceful shutdown and unconfirmed writes. Cite the exact files. Do not modify files.

Codex runs once with the read-only sandbox. Its native OpenTelemetry exporters send traces and structured events to Telescope. The launcher waits for confirmed delivery, stops the receiver, and checks both stored counts before printing `Committed <N> spans and <M> events. Run ID: agent-first`. Ctrl+C leaves the receiver available while Codex shuts down and exports its final telemetry.

The command uses `--ignore-user-config` and `--ephemeral`: authentication stays with Codex, user configuration is not loaded, and the task does not persist a Codex session. The default model is selected by the CLI. To choose another model available to your account, add `--model <model-id>`. No authentication file is read or copied by the launcher, and no Codex settings file is edited.

Use a new run ID for each invocation and concurrent producer. IDs contain 1–100 ASCII letters, digits, or hyphens; omitting the variable generates an ID printed by the launcher. Startup rejects an ID already present in either table. Keep ScopeDB credentials in environment variables; the launcher removes those variables from Codex's environment. `--prompt "Explain where this repository defines its demo checks. Cite files."` runs another read-only task.

## Investigate your run

Open **Query** in the same workspace and run the files in [queries](queries). Change `agent-first` to the run ID printed by your invocation.

1. `03-span-counts.scopeql` checks what arrived. There is no fixed row count: Codex emits startup, filesystem, transport, model, and tool spans, and each task takes a different path.
2. `04-model-performance.scopeql` groups `try_run_sampling_request` spans by model. Each row describes sampling attempts, including stream processing, rather than an isolated network latency measurement.
3. `05-tool-performance.scopeql` compares `codex.tool_result` events by tool, using their native duration and success fields. Code mode can report both an outer `exec` and the `exec_command` calls inside it. These are different layers; do not add their counts or durations as independent work.
4. `06-recent-turns.scopeql` shows `run_turn` elapsed time. `07-run-timeline.scopeql` keeps the model and execution spans visible while excluding most startup detail. It retains all trace IDs in this run because a tool in another process can have its own trace.

The trace table keeps original IDs, parent IDs, start/end times, names, status, duration, model, tool, and call ID. The event table keeps event time, trace/span IDs, event name, model, tool, call ID, duration, and success. Missing metadata stays `NULL`. A call ID can help relate a tool result to its execution span; compare it with the trace IDs and timestamps. Parent relationships apply within their native trace, and time order alone does not establish causality.

Use a turn's `elapsed_ms` for wall-clock time. Model attempts and nested tools can overlap, so adding every span duration counts some time more than once. Native `unset` status means no explicit status was set; it is not proof of success. A tool result with `success = FALSE` reports a failed tool invocation. `success = TRUE` means the harness returned a result, and does not guarantee that a shell command inside that result exited with code zero.

In **Explore**, select either table, include the task's timestamps in the time range, and filter `run_id = 'agent-first'`. Select **Run**, then click a record for its fields. Native span names and event coverage can change between Codex versions; this mapping and the queries were checked with CLI 0.160.0.

## Optional: investigate a tool failure

```bash
SCOPEDB_DEMO_RUN=agent-failure python3 run.py --failure-demo
```

The prompt asks Codex to run `pwd` using the obsolete `bluesky-events/legacy` working directory before finding the current entry point. If it attempts that directory, process creation fails before the command can run. Codex decides what to do next; the launcher does not insert error records or prescribe a retry count.

Run `08-tool-failures.scopeql`, then `09-tool-results.scopeql` to see failed and successful tool results together. Change the run filter in `07-run-timeline.scopeql` to `agent-failure` to inspect the surrounding sampling attempts and execution spans. A failure can appear in both an outer `exec` result and its inner `exec_command` result; these can be two reports of the same failure. If no failed event appears, inspect the results and timeline to see which calls Codex made.

Do not rely on `status = 'error'` alone for this exercise. Codex can leave spans `unset` even when a native tool result reports failure.

## Telemetry fields and reuse

[telescope.yaml](telescope.yaml) maps an explicit list of fields. It does not forward raw attribute objects or log bodies to ScopeDB. Account identifiers, email addresses, local paths, prompts, tool arguments, and tool output are outside this mapping. The local receiver still processes the original OTLP in memory; disable or filter additional source telemetry before using the pattern for other projects. Raw prompt logging and metrics export are disabled for this invocation.

The configuration uses loopback listeners, a one-second batch timeout, and an in-memory queue. The launcher writes available ports and a fresh run label into a temporary copy. For a maintained deployment, run Telescope directly, choose durable queue storage and retention, and manage table definitions ahead of time. Use `--telescope /path/to/telescope` to supply an installed v0.3.0 executable, or `--codex /path/to/codex` to select a CLI installation.

## Verify

```bash
python3.14 -m unittest discover -s tests -v
```

Tests require Python 3.14 for standard-library zstd decoding; the launcher supports Python 3.11 or later. They use the released Telescope binary, a local ScopeDB protocol fixture, and synthetic CLI telemetry to check field selection, native IDs, typed tool results, credential isolation, run labels, delivery failures, and shutdown. They need network access for the first binary download and no model account.
