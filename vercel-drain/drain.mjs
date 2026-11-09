import { createHmac, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { parseArgs } from "node:util";
import { gunzipSync, gzipSync } from "node:zlib";
import { Client } from "scopedb";

const { values } = parseArgs({ options: { cycles: { type: "string", default: "0" } } });
const cycles = Number(values.cycles);
if (!Number.isSafeInteger(cycles) || cycles < 0) {
  throw new Error("--cycles must be a nonnegative integer; zero runs until Ctrl+C");
}

const endpoint = process.env.SCOPEDB_ENDPOINT;
const apiKey = process.env.SCOPEDB_API_KEY;
const runId = process.env.SCOPEDB_DEMO_RUN ?? randomUUID();
const drainSecret = process.env.VERCEL_DRAIN_SECRET ?? "demo-drain-signing-secret";
if (!endpoint || !apiKey) {
  throw new Error("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY before running");
}
if (!/^[a-zA-Z0-9-]{1,100}$/.test(runId)) {
  throw new Error("SCOPEDB_DEMO_RUN must contain 1–100 letters, digits, or hyphens");
}

const catalog = JSON.parse(await readFile(new URL("./catalog.json", import.meta.url), "utf8"));
const client = new Client(endpoint, { apiKey });
await client.catalog().schema("scopedb", "guide_vercel").apply(catalog);

const previous = await client.query(`FROM guide_vercel.vercel_events
  WHERE attributes['run_id']::string = '${runId}' AGGREGATE count() AS events`);
if (previous.toObjects()[0].events > 0) {
  throw new Error("This run ID already has events; choose a new SCOPEDB_DEMO_RUN");
}

const stream = client
  .table("vercel_events", { schema: "guide_vercel" })
  .appendStream()
  .flushIntervalMs(1000)
  .build();
let sequence = 0;
let sentRows = 0;

function extractUrlMetadata(rawUrl) {
  if (typeof rawUrl !== "string" || rawUrl.length === 0) {
    return { host: null, path: null, query: {}, attribution: {} };
  }
  const parsed = new URL(rawUrl);
  return {
    host: parsed.hostname || null,
    path: parsed.pathname || null,
    query: Object.fromEntries(parsed.searchParams),
    attribution: {
      utm_source: parsed.searchParams.get("utm_source"),
      utm_medium: parsed.searchParams.get("utm_medium"),
      utm_campaign: parsed.searchParams.get("utm_campaign"),
      utm_term: parsed.searchParams.get("utm_term"),
      utm_content: parsed.searchParams.get("utm_content"),
      ref: parsed.searchParams.get("ref"),
    },
  };
}

function normalizeDrainItem(item) {
  const occurredAt = new Date(item.timestamp ?? Date.now()).toISOString();
  const service = item.projectName ?? "acme-docs";
  if (item.schema === "vercel.analytics.v2") {
    const target = extractUrlMetadata(`${item.origin}${item.path}${item.queryParams ? `?${item.queryParams}` : ""}`);
    const referer = extractUrlMetadata(item.referrer);
    return [
      {
        event_id: randomUUID(),
        occurred_at: occurredAt,
        service,
        event_type: "vercel.pageview",
        outcome: "ok",
        level: "info",
        method: "GET",
        host: target.host,
        path: target.path,
        status: 200,
        request_id: String(item.sessionId),
        message: `pageview ${target.path}`,
        attributes: {
          run_id: runId,
          schema: item.schema,
          project_id: item.projectId,
          deployment_id: item.deployment,
          environment: item.vercelEnvironment,
          route: item.route,
          country: item.country,
          region: item.region,
          city: item.city,
          device_type: item.deviceType,
          os_name: item.osName,
          client_name: item.clientName,
          referer_host: referer.host,
          referer_path: referer.path,
          query: target.query,
          ...target.attribution,
        },
      },
    ];
  }
  if (item.schema === "vercel.speed_insights.v1") {
    const target = extractUrlMetadata(`${item.origin}${item.path}`);
    return [
      {
        event_id: randomUUID(),
        occurred_at: occurredAt,
        service,
        event_type: "vercel.web_vital",
        outcome: item.rating === "poor" ? "client_error" : "ok",
        level: item.rating === "poor" ? "warn" : "info",
        method: "GET",
        host: target.host,
        path: target.path,
        status: null,
        request_id: item.id,
        message: `${item.metricType} ${item.value} (${item.rating})`,
        attributes: {
          run_id: runId,
          schema: item.schema,
          project_id: item.projectId,
          deployment_id: item.deployment,
          environment: item.vercelEnvironment,
          route: item.route,
          metric_type: item.metricType,
          metric_value: item.value,
          rating: item.rating,
          country: item.country,
          device_type: item.deviceType,
        },
      },
    ];
  }
  const proxy = item.proxy ?? {};
  const target = extractUrlMetadata(`https://${proxy.host}${proxy.path}`);
  const referer = extractUrlMetadata(proxy.referer);
  const status = proxy.statusCode;
  const traceMatch = /^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/i.exec(item.traceparent ?? "");
  const baseAttributes = {
    run_id: runId,
    project_id: item.projectId,
    deployment_id: item.deploymentId,
    environment: item.environment,
    branch: item.branch,
    source: item.source,
    entrypoint: item.entrypoint,
    route: item.route ?? item.entrypoint,
    region: proxy.region,
    execution_region: item.executionRegion,
    vercel_cache: proxy.vercelCache,
    duration_ms: item.durationMs ?? null,
    trace_id: traceMatch?.[1] ?? null,
    span_id: traceMatch?.[2] ?? null,
    user_agent: Array.isArray(proxy.userAgent) ? proxy.userAgent.join(" ") : proxy.userAgent,
    referer_host: referer.host,
    referer_path: referer.path,
    query: target.query,
    ...target.attribution,
  };
  const rows = [
    {
      event_id: item.id,
      occurred_at: occurredAt,
      service,
      event_type: "vercel.request",
      outcome: status >= 500 || item.level === "error" ? "error" : "ok",
      level: status >= 500 || item.level === "error" ? "error" : "info",
      method: proxy.method,
      host: target.host,
      path: target.path,
      status,
      request_id: item.requestId,
      message: `${proxy.method} ${target.path} -> ${status}`,
      attributes: baseAttributes,
    },
  ];
  if (item.message && item.source !== "static") {
    rows.push({
      event_id: randomUUID(),
      occurred_at: occurredAt,
      service,
      event_type: "vercel.log",
      outcome: item.level === "error" ? "error" : "ok",
      level: item.level ?? "info",
      method: proxy.method,
      host: target.host,
      path: target.path,
      status,
      request_id: item.requestId,
      message: item.message,
      attributes: { ...baseAttributes, raw_level: item.level },
    });
  }
  return rows;
}

async function receiveSignedDrainBatch(compressedBody, signatureHeader) {
  const expected = createHmac("sha1", drainSecret).update(compressedBody).digest("hex");
  if (signatureHeader !== expected) {
    throw new Error("Invalid x-vercel-signature");
  }
  const ndjson = gunzipSync(compressedBody).toString("utf8");
  for (const line of ndjson.trim().split("\n")) {
    const item = JSON.parse(line);
    for (const row of normalizeDrainItem(item)) {
      console.log(JSON.stringify(row));
      await stream.send(row);
      sentRows++;
    }
  }
}

async function emitCycle(cycleIndex) {
  const now = Date.now();
  const nextReqId = () => `iad1::sfo1::${runId}-${String(++sequence).padStart(3, "0")}`;
  const browserUa = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36"];

  const drainBatch = [
    // 3 human browser HTTP GET 200 pageviews
    {
      id: randomUUID(), timestamp: now, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "edge",
      entrypoint: "/", requestId: nextReqId(), durationMs: 14.2,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/?utm_source=github&utm_medium=readme&ref=oss",
        statusCode: 200, region: "sfo1", vercelCache: "HIT",
        referer: "https://github.com/scopedb/vercel-drain", userAgent: browserUa,
      },
    },
    {
      id: randomUUID(), timestamp: now + 5, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "edge",
      entrypoint: "/docs/quickstart", requestId: nextReqId(), durationMs: 18.6,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/docs/quickstart?tenant=acme",
        statusCode: 200, region: "sfo1", vercelCache: "HIT",
        referer: "https://t.co/launch", userAgent: browserUa,
      },
    },
    {
      id: randomUUID(), timestamp: now + 10, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "edge",
      entrypoint: "/pricing", requestId: nextReqId(), durationMs: 11.4,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/pricing",
        statusCode: 200, region: "iad1", vercelCache: "HIT",
        referer: "https://docs.acme.dev/docs/quickstart", userAgent: browserUa,
      },
    },
    // 1 Web Analytics Drain browser pageview
    {
      schema: "vercel.analytics.v2", eventType: "pageview", timestamp: now + 15,
      projectId: "prj_acme", projectName: "acme-docs", deployment: "dpl_prod_1",
      vercelEnvironment: "production", sessionId: 1000 + cycleIndex,
      origin: "https://docs.acme.dev", path: "/blog/launch", route: "/blog/[slug]",
      queryParams: "utm_source=x&utm_campaign=launch&tenant=acme",
      referrer: "https://t.co/launch", country: "US", region: "CA", city: "San Francisco",
      deviceType: "desktop", osName: "Mac OS", clientName: "Chrome",
    },
    // 2 background static/API requests (included in v_vercel_requests, filtered out of v_vercel_pageviews)
    {
      id: randomUUID(), timestamp: now + 20, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "static",
      entrypoint: "/_next/static/chunks/main.js", requestId: nextReqId(), durationMs: 2.1,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/_next/static/chunks/main.js",
        statusCode: 200, region: "sfo1", vercelCache: "HIT", userAgent: browserUa,
      },
    },
    {
      id: randomUUID(), timestamp: now + 25, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "lambda",
      entrypoint: "/api/health", requestId: nextReqId(), durationMs: 4.8,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/api/health",
        statusCode: 200, region: "iad1", vercelCache: "MISS", userAgent: ["kube-probe/1.30"],
      },
    },
    // 2 AI agent / crawler fetches (captured by v_vercel_ai_bots, excluded from v_vercel_pageviews)
    {
      id: randomUUID(), timestamp: now + 30, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "edge",
      entrypoint: "/llms.txt", requestId: nextReqId(), durationMs: 7.3,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/llms.txt",
        statusCode: 200, region: "sfo1", vercelCache: "HIT",
        userAgent: ["ClaudeBot/1.0 (+https://www.anthropic.com)"],
      },
    },
    {
      id: randomUUID(), timestamp: now + 35, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "edge",
      entrypoint: "/docs/quickstart.md", requestId: nextReqId(), durationMs: 9.1,
      proxy: {
        method: "GET", host: "docs.acme.dev", path: "/docs/quickstart.md",
        statusCode: 200, region: "iad1", vercelCache: "HIT",
        userAgent: ["Cursor/1.0"],
      },
    },
    // 1 failing serverless route (emits 1 vercel.request 504 row + 1 vercel.log error row)
    {
      id: randomUUID(), timestamp: now + 40, projectId: "prj_acme", projectName: "acme-docs",
      deploymentId: "dpl_prod_1", environment: "production", branch: "main", source: "lambda",
      level: "error", message: "Unhandled rejection: upstream model timeout",
      entrypoint: "/api/chat", executionRegion: "iad1", requestId: nextReqId(), durationMs: 10004.5,
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      proxy: {
        method: "POST", host: "docs.acme.dev", path: "/api/chat?tenant=acme",
        statusCode: 504, region: "sfo1", vercelCache: "MISS", userAgent: browserUa,
      },
    },
    // 1 Speed Insights Web Vital
    {
      schema: "vercel.speed_insights.v1", id: nextReqId(), timestamp: now + 45,
      projectId: "prj_acme", projectName: "acme-docs", deployment: "dpl_prod_1",
      vercelEnvironment: "production", origin: "https://docs.acme.dev",
      path: "/docs/quickstart", route: "/docs/[slug]",
      metricType: "LCP", value: 1840.5, rating: "good", country: "US", deviceType: "desktop",
    },
  ];

  const ndjson = drainBatch.map(item => JSON.stringify(item)).join("\n");
  const compressed = gzipSync(Buffer.from(ndjson, "utf8"));
  const signature = createHmac("sha1", drainSecret).update(compressed).digest("hex");
  await receiveSignedDrainBatch(compressed, signature);

  // 1 Next.js after() custom application event
  const afterEvent = {
    event_id: randomUUID(),
    occurred_at: new Date(now + 50).toISOString(),
    service: "acme-docs",
    event_type: "vercel.custom_event",
    outcome: "ok",
    level: "info",
    method: "POST",
    host: "docs.acme.dev",
    path: "/api/search",
    status: 200,
    request_id: nextReqId(),
    message: "ai.completion.finished",
    attributes: {
      run_id: runId,
      environment: "production",
      model: "claude-sonnet-4-5",
      tokens: 842,
    },
  };
  console.log(JSON.stringify(afterEvent));
  await stream.send(afterEvent);
  sentRows++;
}

const stopping = new AbortController();
const stop = () => stopping.abort();
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
console.log(`Streaming Vercel Drain and Next.js after() events. Run ID: ${runId}. Keep this terminal open; Ctrl+C flushes and stops.`);
let completedCycles = 0;
try {
  while (!stopping.signal.aborted && (cycles === 0 || completedCycles < cycles)) {
    await emitCycle(completedCycles);
    completedCycles++;
    if (cycles > 0 && completedCycles >= cycles) break;
    try {
      await delay(1000, undefined, { signal: stopping.signal });
    } catch (error) {
      if (error.name !== "AbortError") throw error;
    }
  }
} finally {
  const result = await stream.shutdown();
  if (result?.num_rows_inserted !== sentRows) {
    throw new Error("The SDK did not confirm all generated events; inspect this run before retrying");
  }
  console.log(`Committed ${sentRows} events. Run ID: ${runId}`);
  process.removeListener("SIGINT", stop);
  process.removeListener("SIGTERM", stop);
}
