# Analyze Vercel traffic and function errors

Keep a Vercel Drain and Next.js `after()` simulator running, verify `x-vercel-signature` (`HMAC-SHA1`) across gzip-compressed NDJSON batches, and separate human browser pageviews from AI crawlers and failing serverless routes in Console. Each cycle emits 12 rows into `guide_vercel.vercel_events` — four human browser pageviews, two static/API requests, two AI agent fetches (`/llms.txt` and `/docs/quickstart.md`), one `504` serverless timeout with its correlated runtime `error` log on `/api/chat`, one Speed Insights Web Vital (`LCP`), and one Next.js `after()` custom event — and repeats once per second until Ctrl+C.

Allow about 15 minutes after workspace setup. All events are simulated and contain no customer data. See [`scopedb/vercel-drain`](https://github.com/scopedb/vercel-drain) for the production Next.js Route Handler and the [Vercel traffic guide](https://www.scopedb.io/docs/guides/analyze-vercel-traffic) for the complete walkthrough.

## Connect and run

You need a ready ScopeDB Cloud workspace and Node.js 20 or later. In Console, open **Connect**, copy **ScopeDB API**, and create a workspace API key. Keep the secret outside source control.

From the repository root, set the connection details and start the producer:

```bash
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
export SCOPEDB_DEMO_RUN=vercel-first
cd vercel-drain
npm ci
npm start
```

The explicit run label is optional; without it the program generates a random UUID and prints it at startup. Use a new run ID when restarting so observations stay separate. The program rejects a run ID that already has events.

On startup, `drain.mjs` applies [catalog.json](catalog.json) through the Catalog API to ensure the `guide_vercel` schema, `vercel_events` table, and four analytical views (`v_vercel_requests`, `v_vercel_pageviews`, `v_vercel_errors`, `v_vercel_ai_bots`) exist. Keep the terminal running while you use Console; Ctrl+C stops production, flushes the stream, and reports confirmed rows.

## Investigate arriving traffic

Open **Query** in the same workspace and run the files in [queries](queries):

1. `01-event-breakdown.scopeql` counts rows by `event_type` and error count. After one completed cycle, expect 12 total rows: eight `vercel.request` rows (one error), one `vercel.log` row (one error), one `vercel.pageview` row, one `vercel.web_vital` row, and one `vercel.custom_event` row.
2. `02-clean-pageviews.scopeql` queries `guide_vercel.v_vercel_pageviews` to rank human browser pageviews (`/`, `/docs/quickstart`, `/pricing`, `/blog/launch`) alongside normalized external referrers (`github.com`, `t.co`) and `utm_source` attribution, while excluding `/_next/static/*`, `/api/*`, and AI crawlers.
3. `03-ai-bot-readership.scopeql` queries `guide_vercel.v_vercel_ai_bots` to inspect `ClaudeBot/1.0` fetching `/llms.txt` and `Cursor/1.0` fetching `/docs/quickstart.md`.
4. `04-correlate-route-errors.scopeql` queries `guide_vercel.v_vercel_errors` to correlate the `504` HTTP request on `/api/chat` with its runtime `Unhandled rejection: upstream model timeout` log line via `request_id` and W3C `trace_id` (`4bf92f3577b34da6a3ce929d0e0e4736`).

[catalog.json](catalog.json) records the declarative `guide_vercel` schema, table, and views that `drain.mjs` applies automatically (or that you can apply ahead of time with `scope catalog apply --schema scopedb.guide_vercel -f catalog.json --yes`). The queries filter `vercel-first`; change that filter when using another run ID.

## Verify

Run the automated checks and one bounded verification cycle:

```bash
(cd vercel-drain && npm test)
(cd vercel-drain && SCOPEDB_DEMO_RUN=vercel-check-js npm start -- --cycles 1)
```

One cycle commits 12 events, including two correlated error rows (`vercel.request` `504` and `vercel.log` `error`), then exits. The default program runs continuously. Tests exercise repeated cycles, `x-vercel-signature` verification, Catalog schema application, and graceful interruption against a local protocol fixture. Checked with Node.js 24 and `scopedb` 0.3.0 on 2026-11-09.
