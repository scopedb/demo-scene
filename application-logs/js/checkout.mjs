import { randomUUID } from "node:crypto";
import { Client } from "scopedb";
import { setTimeout as delay } from "node:timers/promises";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { cycles: { type: "string", default: "0" } } });
const cycles = Number(values.cycles);
if (!Number.isSafeInteger(cycles) || cycles < 0) {
  throw new Error("--cycles must be a nonnegative integer; zero runs until Ctrl+C");
}

const endpoint = process.env.SCOPEDB_ENDPOINT;
const apiKey = process.env.SCOPEDB_API_KEY;
const runId = process.env.SCOPEDB_DEMO_RUN ?? randomUUID();
if (!endpoint || !apiKey) {
  throw new Error("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY before running");
}
if (!/^[a-zA-Z0-9-]{1,100}$/.test(runId)) {
  throw new Error("SCOPEDB_DEMO_RUN must contain 1–100 letters, digits, or hyphens");
}
const client = new Client(endpoint, { apiKey });
await client.catalog().schema("scopedb", "guide_logs").apply({
  name: "guide_logs",
  tables: [
    {
      name: "requests",
      columns: [
        { name: "time", data_type: "timestamp" },
        { name: "service", data_type: "string" },
        { name: "level", data_type: "string" },
        { name: "message", data_type: "string" },
        { name: "var", data_type: "object" },
      ],
    },
  ],
});
const previous = await client.query(`FROM guide_logs.requests
  WHERE var['run_id']::string = '${runId}' AGGREGATE count() AS requests`);
if (previous.toObjects()[0].requests > 0) {
  throw new Error("This run ID already has logs; choose a new SCOPEDB_DEMO_RUN");
}

const stream = client.table("requests", { schema: "guide_logs" }).appendStream().flushIntervalMs(1000).build();
let requestNumber = 0;
let sentRows = 0;

// Simulate a gateway regression without making real payments.
async function pay(release, gateway, attempt) {
  if (release === "1.9" && gateway === "credit_card" && attempt > 0) {
    throw new Error("Credit card gateway timed out");
  }
}

async function checkout(release, gateway, attempt) {
  const requestId = `req-${String(++requestNumber).padStart(2, "0")}`;
  let level = "info";
  let message = "Payment accepted";
  try {
    await pay(release, gateway, attempt);
  } catch (error) {
    level = "error";
    message = error.message;
  }
  const log = {
    time: new Date().toISOString(), service: "checkout", level, message,
    var: { run_id: runId, request_id: requestId, release, gateway },
  };
  console.log(JSON.stringify(log));
  await stream.send(log);
  sentRows++;
}

const stopping = new AbortController();
const stop = () => stopping.abort();
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
console.log(`Streaming checkout logs. Run ID: ${runId}. Keep this terminal open; Ctrl+C flushes and stops.`);
let completedCycles = 0;
try {
  while (!stopping.signal.aborted && (cycles === 0 || completedCycles < cycles)) {
    for (const release of ["1.8", "1.9"]) {
      for (const gateway of ["credit_card", "bank_transfer"]) {
        for (let attempt = 0; attempt < 3; attempt++) {
          await checkout(release, gateway, attempt);
        }
      }
    }
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
    throw new Error("The SDK did not confirm all generated logs; inspect this run before retrying");
  }
  console.log(`Committed ${sentRows} logs. Run ID: ${runId}`);
  process.removeListener("SIGINT", stop);
  process.removeListener("SIGTERM", stop);
}
