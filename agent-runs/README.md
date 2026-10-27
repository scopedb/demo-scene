# Trace Claude Code runs

Run a real Claude Code task and inspect its execution in ScopeDB Console. Compare model requests and tool calls, then follow the timeline behind the answer. A successful run is enough to start; checking a failed tool call is an optional exercise.

Allow about 15 minutes after workspace and Claude Code setup. Claude uses your existing account and model configuration, with its normal usage charges. The [illustrated tutorial](https://www.scopedb.io/docs/guides/trace-agent-runs) follows the investigation.

## Connect and run

You need a ready ScopeDB Cloud workspace, a signed-in Claude Code CLI, Python 3.11 or later, and Git on macOS arm64 or Linux amd64/arm64 (including WSL). Claude Code tracing is currently beta; this integration was checked with CLI 2.1.123. In Console, open **Connect**, copy **ScopeDB API**, and create a workspace API key.

From the repository root:

```bash
cd agent-runs
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
SCOPEDB_DEMO_RUN=agent-first python3 run.py
```

The launcher downloads the official Telescope v0.3.0 archive on first use, checks its pinned SHA-256, and caches the executable outside this checkout. It creates `guide_agents.spans` in the default `scopedb` database, starts Telescope on available loopback ports, and asks Claude:

> Compare how bluesky-events and application-logs handle graceful shutdown and unconfirmed writes. Cite the exact files. Do not modify files.

Only `Read`, `Glob`, and `Grep` are available. Claude produces the answer and the original OpenTelemetry spans; Telescope maps those spans into ScopeDB. The launcher does not edit Claude settings, infer model steps, or rewrite span IDs and timing. It waits for confirmed delivery and checks the stored count after shutdown before printing `Committed <N> spans. Run ID: agent-first`.

Use a new run ID for each invocation and concurrent producer. IDs contain 1–100 ASCII letters, digits, or hyphens; omitting the variable generates an ID printed by the launcher. Existing rows cause startup to fail. Keep credentials in local environment variables. ScopeDB connection secrets are passed to Telescope and removed from the Claude subprocess environment.

For more questions in the same traced session:

```bash
SCOPEDB_DEMO_RUN=agent-session python3 run.py --interactive
```

Use `/exit` when finished. Ctrl+C can interrupt Claude's current turn while the receiver stays available. The launcher then waits for Claude's final export and settles delivery before stopping Telescope. To change the read-only task, use `--prompt "Explain where this repository defines its demo checks. Cite files."`.

## Investigate your run

Open **Query** in the same workspace and run the files in [queries](queries). Change `agent-first` to the ID printed by your invocation.

1. `02-span-counts.scopeql` checks the population and errors. There is no fixed expected row count: model choices and CLI versions affect the calls. A complete successful task may have zero error spans.
2. `03-call-performance.scopeql` compares model requests with tool calls by model and tool name. It uses the outer `claude_code.tool` span once per tool call, alongside `claude_code.llm_request`. `cumulative_ms` adds span durations within a group; parallel calls overlap, so it is not a division of wall-clock time.
3. `04-recent-traces.scopeql` lists interaction trace IDs and elapsed times. Copy one ID into `05-trace-timeline.scopeql` to inspect its spans, including successful steps.

The timeline retains native `trace_id`, `span_id`, `parent_span_id`, start/end times, names, and status. `duration_ns` preserves the original duration; the saved queries display milliseconds. Native attributes remain in `var`, including model names, token counts, and tool names when present. Several `Read` spans mean several calls; with tool details disabled, they do not establish that the same file was read again.

Claude can emit an interaction span, model requests, and tool spans with nested permission and execution spans. Read parent IDs together with timestamps. Do not add the root and all its children, or a tool wrapper and its execution child, to calculate total run time. Use the interaction's elapsed time for that question. `unset` is the native absence of an explicit status, rather than a failed call; inspect `error` on child spans even when their parent is `unset`.

In **Explore**, select `guide_agents.spans`, include the run's timestamps in the time range, and filter `run_id = 'agent-first'`. Select **Run**, then click a record for its attributes. Prompt text, tool arguments, and tool content are disabled at the source by the launcher; attribute availability varies with Claude Code versions.

## Optional: inspect a failed tool call

```bash
SCOPEDB_DEMO_RUN=agent-failure python3 run.py --failure-demo
```

This asks Claude to check the obsolete path `bluesky-events/collector.go` with `Read` before finding the current entry point. If it tries that path, the file read really fails. Claude may locate another file, try another tool, or stop; the launcher records what happened. It does not guarantee an error count, retry, or recovery sequence.

Run `06-errors.scopeql` to find failed spans, then use their trace ID in the full timeline query, changing its run filter to `agent-failure`. Look at the model requests and tool calls after the failure. A tool error can live on `claude_code.tool.execution` while the tool wrapper and interaction remain `unset`. If no error appears, run `04-recent-traces.scopeql` and `05-trace-timeline.scopeql` with `run_id = 'agent-failure'`: Claude may have located the current file without trying the old path.

## Reuse the telemetry path

[telescope.yaml](telescope.yaml) is a complete mapping for Telescope v0.3.0, with a one-second batch timeout, loopback listeners, and an in-memory queue. The launcher substitutes available ports in a temporary copy. For a maintained deployment, run Telescope directly, choose durable queue storage and retention, and point your application's OTLP exporter at it. Table creation is a demo setup convenience; manage definitions ahead of deployment for an application.

Use `--telescope /path/to/telescope` to supply an installed v0.3.0 executable instead of downloading it. `--claude /path/to/claude` selects a CLI installation. `--replay /path/to/capture.otlp.json` sends your own OTLP JSON capture through the same receiver using a fresh run label. It preserves the capture's IDs and timestamps, so include those timestamps when exploring a replay. No fabricated trace is loaded by default.

## Verify

```bash
python3.14 -m unittest discover -s tests -v
```

Tests require Python 3.14 to decode Telescope's zstd appends with the standard library; the launcher supports Python 3.11 or later. They use the released Telescope binary, a local ScopeDB protocol fixture, and a CLI fixture to exercise mapping, error children, run labels, credential isolation, final exports, and graceful interruption. They need network access for the first binary download and no model account.

Checked on 2026-10-27 with Telescope v0.3.0 and Claude Code 2.1.123. A separate integration check ran the real Claude CLI and its file tools against a controlled local model endpoint and an isolated ScopeDB v0.6.0 instance, including a missing-file failure and subsequent successful read. This verifies execution and delivery, not live model behavior, latency, or a Cloud workspace.
