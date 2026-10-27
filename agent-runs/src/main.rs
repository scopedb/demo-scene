use std::error::Error;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use scopedb_client::{Client, Value};
use serde_json::json;
use tokio::signal::unix::{SignalKind, signal};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let cycles = match args.next().as_deref() {
        None => 0,
        Some("--cycles") => args
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or("--cycles must be a nonnegative integer; zero runs until Ctrl+C")?,
        _ => return Err("Usage: agent-runs [--cycles <N>]".into()),
    };
    if args.next().is_some() {
        return Err("Usage: agent-runs [--cycles <N>]".into());
    }
    let endpoint = std::env::var("SCOPEDB_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY before running")?;
    let api_key = std::env::var("SCOPEDB_API_KEY")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY before running")?;
    let run_id = match std::env::var("SCOPEDB_DEMO_RUN") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => uuid::Uuid::new_v4().to_string(),
        Err(error) => return Err(error.into()),
    };
    if !(1..=100).contains(&run_id.len())
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("SCOPEDB_DEMO_RUN must contain 1–100 ASCII letters, digits, or hyphens".into());
    }

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let client = Client::builder(endpoint).api_key(api_key).build()?;
    client
        .query("CREATE SCHEMA IF NOT EXISTS guide_agents")
        .await?;
    client
        .query(
            "CREATE TABLE IF NOT EXISTS guide_agents.spans (
            time timestamp, trace_id string, span_id string, step string,
            status string, duration_ms int, var object
        )",
        )
        .await?;
    let previous = client
        .query(format!(
            "FROM guide_agents.spans
            WHERE var['run_id']::string = '{run_id}' AGGREGATE count() AS spans"
        ))
        .await?
        .into_objects()?;
    match previous.first().and_then(|row| row.get("spans")) {
        Some(Value::Int(0) | Value::UInt(0)) => {}
        Some(Value::Int(n)) if *n > 0 => {
            return Err("This run ID already has spans; choose a new SCOPEDB_DEMO_RUN".into());
        }
        Some(Value::UInt(_)) => {
            return Err("This run ID already has spans; choose a new SCOPEDB_DEMO_RUN".into());
        }
        _ => return Err("Could not verify whether this run ID already has spans".into()),
    }

    let stream = client
        .table("spans")
        .with_schema("guide_agents")
        .append_stream()
        .flush_interval(Duration::from_secs(1))
        .build()?;
    println!(
        "Streaming agent spans. Run ID: {run_id}. Keep this terminal open; Ctrl+C flushes and stops."
    );
    let mut sent_rows = 0;
    let mut trace_number = 0;
    let production: Result<()> = async {
        let mut completed_cycles = 0;
        loop {
            for trace_in_cycle in 0..3 {
                trace_number += 1;
                for span in trace_spans(&run_id, trace_number, trace_in_cycle == 2)? {
                    stream.send(&span).await?;
                    sent_rows += 1;
                }
            }
            completed_cycles += 1;
            println!("Queued cycle {completed_cycles}: {sent_rows} spans. Run ID: {run_id}");
            if cycles > 0 && completed_cycles >= cycles {
                break;
            }
            // Finish admitting the cycle before handling shutdown so traces stay complete.
            tokio::select! {
                _ = interrupt.recv() => break,
                _ = terminate.recv() => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
        Ok(())
    }
    .await;
    let delivery = stream.shutdown().await?;
    production?;
    if delivery.committed_rows != sent_rows {
        return Err(
            "The SDK did not confirm all generated spans; inspect this run before retrying".into(),
        );
    }
    println!("Committed {sent_rows} spans. Run ID: {run_id}");
    Ok(())
}

fn trace_spans(run_id: &str, number: u64, retry: bool) -> Result<Vec<serde_json::Value>> {
    let trace_id = format!("trace-{number:02}");
    // Synthetic start times preserve parallel tool calls without real model or tool requests.
    // Anchor the whole trace in the past so even its final span has already finished.
    let start = Timestamp::now().checked_sub(SignedDuration::from_millis(1740))?;
    let mut steps = vec![
        ("plan", "ok", 0, 60, None, 1),
        (
            "search",
            if retry { "error" } else { "ok" },
            60,
            if retry { 1200 } else { 180 },
            Some("web_search"),
            1,
        ),
        ("query", "ok", 60, 250, Some("scopedb_query"), 1),
    ];
    if retry {
        steps.push(("search", "ok", 1260, 180, Some("web_search"), 2));
    }
    steps.push(("answer", "ok", if retry { 1440 } else { 310 }, 300, None, 1));
    steps
        .into_iter()
        .enumerate()
        .map(
            |(index, (step, status, offset, duration_ms, tool, attempt))| {
                let mut metadata =
                    json!({"run_id": run_id, "model": "claude-sonnet", "attempt": attempt});
                if let Some(tool) = tool {
                    metadata["tool"] = json!(tool);
                }
                if status == "error" {
                    metadata["error"] = json!("Search provider timed out");
                }
                Ok(json!({
                    "time": start.checked_add(SignedDuration::from_millis(offset))?.to_string(),
                    "trace_id": trace_id,
                    "span_id": format!("{trace_id}-{:02}", index + 1),
                    "step": step,
                    "status": status,
                    "duration_ms": duration_ms,
                    "var": metadata,
                }))
            },
        )
        .collect()
}
