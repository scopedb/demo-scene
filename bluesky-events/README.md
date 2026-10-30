# Analyze Bluesky activity

Keep a public event subscription running and append incoming mutations with the Go SDK. Investigate new posts, replies, likes, and reposts in Console while the producer continues. A 16-event constructed sample makes the same queries reproducible before interpreting live observations.

Allow about 20 minutes after workspace setup. The [illustrated tutorial](https://www.scopedb.io/docs/guides/analyze-bluesky-activity) follows the complete workflow.

## Prepare the workspace

You need a ready ScopeDB Cloud workspace, Go 1.26 or later, and Git. In Console, open **Home**, copy **ScopeDB API**, and create a workspace API key.

[catalog.json](catalog.json) defines the `guide_bluesky` schema, the append-only `events_raw` table, and the deduplicated `events` view (`FROM guide_bluesky.events_raw DISTINCT ON capture_id, event_id BY ingested_at DESC`). The collector applies this schema automatically on startup through the Go SDK Catalog API, or you can apply it ahead of time with `scope catalog apply --schema scopedb.guide_bluesky -f catalog.json --yes`. Query `guide_bluesky.events` for analysis so retries and inclusive source cursors do not inflate counts.

From the repository root:

```bash
cd bluesky-events
go mod download
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
go run .
```

The program uses `goscopedb` 0.7.0 for catalog management, queries, and writes. On startup it ensures `guide_bluesky.events_raw` and `guide_bluesky.events` exist and that capture `first` contains the 16 events in `sample.ndjson`; a complete sample is reused. Then it continuously subscribes to public Jetstream events and writes them under `live-first`, in batches of up to 500 rows or once per second while buffered data exists. There is no repeated capture/load step.

Keep the terminal running while you query. Output such as `Committed 500 events for capture live-first` reports acknowledged writes. Ctrl+C flushes the remaining batch and closes the connection. The public live subscription needs no Bluesky account or key. See [Jetstream's documentation](https://bsky.network/docs/jetstream/) for its source contract.

## Investigate

The saved queries match the website walkthrough:

- `01-sample-window.scopeql` checks the 16-event sample and its fixed UTC boundaries.
- `02-activity-mix.scopeql` counts creates: six posts, three likes, two reposts, and two follows in the sample.
- `03-replies-and-formats.scopeql` distinguishes the two replies from other posts and preserves the null embed group.
- `04-format-chart.scopeql` returns image = 2, external link = 1, and video = 1 for a Console bar chart.
- `05-reaction-targets.scopeql` matches two of three likes and one of two reposts to captured post creations. This is not an engagement rate.
- `06-raw-events.scopeql` inspects the original post payloads. Click a `raw` cell to inspect nested fields in **Row details**.
- `07-live-event-mix.scopeql` breaks down live `live-first` deliveries by shortened collection and operation for a Console bar chart.
- `08-live-top-actors.scopeql` groups live events directly by `raw['did']::string` to profile high-frequency actors and automation signatures (`follows`, `likes`, `posts`, `unfollows`).

The fixed sample is fictional. Select **Run** again on the live queries after a few seconds to include new commits. Live counts vary; interpret them alongside the actual time coverage.

To start a second observation, stop with Ctrl+C and run:

```bash
go run . -capture-id live-second
```

Earlier data is retained and the fixed sample is reused. Run `09-compare-live-windows.scopeql` to compare post counts and reply shares. The active observation continues growing; larger counts alone do not establish a higher activity rate when durations differ.

## Check replay

Run `10-stored-rows.scopeql` and `11-unique-events.scopeql`, then explicitly replay the sample:

```bash
go run . load -file sample.ndjson -capture-id first
```

With one initial sample import, raw rows increase from 16 to 32 and unique events remain at 16. The view deduplicates reads; it does not remove stored rows or make appends exactly once.

## Failure and observation boundaries

A lost source connection is retried against the same source from the last confirmed sequence. Its inclusive cursor can repeat a mutation; the view removes that duplicate from per-observation analysis. A process restart starts at the live tip, because the demo does not persist a restart checkpoint. It does not recover events beyond the source's retention or reconstruct current record state from history.

A failed SDK append stops the producer and saves its unconfirmed batch to a temporary recovery file. Keep that file, fix the cause, and replay it with the original capture ID:

```bash
go run . load -file <recovery-file> -capture-id live-first
```

Direct SDK appends make one attempt. Unknown outcomes are not automatically replayed; a failed response may follow a committed write. Recovery reuses source event identities, so the analysis view can remove duplicate deliveries. Events arriving while the producer is stopped are outside its observation.

`event_id` is the source instance plus sequence number; a record create, update, and delete are different events. `capture_id` labels an observation or saved input. Do not sum overlapping labels and assume global deduplication. Source timestamps use `witnessedAt`, falling back to `time`, rather than the author's `record.createdAt`. `ingested_at` is the loader's clock, not a commit timestamp. Original fields and integer precision remain in `raw`.

Keep live payloads and credentials outside source control. A delete mutation does not erase an earlier stored payload; choose an appropriate retention policy for your workspace.

## Verify

```bash
go test -race ./...
go vet ./...
go run . -capture-id checked-live -limit 1000
```

`-limit` is an explicit bounded verification mode; the default has no event limit. Tests cover live writes and replay, large integers, idle flushing, cursor-based reconnection, and recovery after an unknown append. Checked with Go 1.26, the pinned SDK, and live Jetstream events on 2026-10-27.
