#![cfg(unix)]

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use scopedb_client::StatementEstimatedProgress;
use serde_json::{Value, json};
use tiny_http::{Header, Response, Server};

struct Fixture {
    endpoint: String,
    rows: Arc<Mutex<Vec<Value>>>,
    stop: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Fixture {
    fn new(previous: u64, confirm_rows: bool) -> Self {
        let server = Server::http("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", server.server_addr());
        let rows = Arc::new(Mutex::new(Vec::new()));
        let collected = rows.clone();
        let (stop, stopped) = mpsc::channel();
        let worker = thread::spawn(move || {
            while stopped.try_recv().is_err() {
                let Some(mut request) = server.recv_timeout(Duration::from_millis(50)).unwrap()
                else {
                    continue;
                };
                assert!(
                    request
                        .headers()
                        .iter()
                        .any(|header| header.field.equiv("Authorization")
                            && header.value.as_str() == "Bearer test-key")
                );
                let compressed = request.headers().iter().any(|header| {
                    header.field.equiv("Content-Encoding") && header.value.as_str() == "zstd"
                });
                let mut bytes = Vec::new();
                request.as_reader().read_to_end(&mut bytes).unwrap();
                if compressed {
                    bytes = zstd::stream::decode_all(bytes.as_slice()).unwrap();
                }
                let body = String::from_utf8(bytes).unwrap();
                let result = match request.url() {
                    "/v1/statements" => {
                        let input: Value = serde_json::from_str(&body).unwrap();
                        let statement = input["statement"].as_str().unwrap();
                        let count = statement.contains("AGGREGATE count()");
                        assert!(
                            statement.starts_with("CREATE SCHEMA IF NOT EXISTS guide_agents")
                                || statement
                                    .starts_with("CREATE TABLE IF NOT EXISTS guide_agents.spans")
                                || (count
                                    && statement.contains("var['run_id']::string = 'test-live'"))
                        );
                        json!({
                            "status": "finished", "statement_id": uuid::Uuid::new_v4(),
                            "created_at": "2026-10-27T00:00:00Z",
                            "progress": StatementEstimatedProgress::default(),
                            "result_set": {"format": "json", "metadata": {
                                "fields": if count { json!([{"name": "spans", "data_type": "int"}]) } else { json!([]) },
                                "num_rows": if count { 1 } else { 0 },
                            }, "rows": if count { json!([[previous.to_string()]]) } else { json!([]) }},
                        })
                    }
                    "/v1/catalog/databases/scopedb/schemas/guide_agents/tables/spans/ingest" => {
                        let batch: Vec<Value> = body
                            .lines()
                            .map(|line| serde_json::from_str(line).unwrap())
                            .collect();
                        let count = batch.len();
                        collected.lock().unwrap().extend(batch);
                        json!({"status": "committed", "num_rows_inserted": if confirm_rows { count } else { 0 }})
                    }
                    url => panic!("Unexpected request: {url}"),
                };
                request
                    .respond(Response::from_string(result.to_string()).with_header(
                        Header::from_bytes("Content-Type", "application/json").unwrap(),
                    ))
                    .unwrap();
            }
        });
        Self {
            endpoint,
            rows,
            stop,
            worker: Some(worker),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-runs"));
        command
            .env("SCOPEDB_ENDPOINT", &self.endpoint)
            .env("SCOPEDB_API_KEY", "test-key")
            .env("SCOPEDB_DEMO_RUN", "test-live");
        command
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        self.worker.take().unwrap().join().unwrap();
    }
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn exercise(args: &[&str], stop_signal: Option<&str>) -> Vec<Value> {
    let fixture = Fixture::new(0, true);
    let mut child = Process(
        fixture
            .command()
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (lines, received) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            lines.send(line.unwrap()).unwrap();
        }
    });
    let mut output = String::new();
    let mut signalled = false;
    loop {
        match received.recv_timeout(Duration::from_secs(15)) {
            Ok(line) => {
                if line.starts_with("Queued cycle 1:")
                    && !signalled
                    && let Some(signal) = stop_signal
                {
                    // The producer has admitted a complete cycle; interrupt before its timer flush.
                    assert!(
                        Command::new("kill")
                            .args([signal, &child.0.id().to_string()])
                            .status()
                            .unwrap()
                            .success()
                    );
                    signalled = true;
                }
                output.push_str(&line);
                output.push('\n');
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(error) => panic!("Producer did not exit: {error}; output: {output}"),
        }
    }
    reader.join().unwrap();
    let status = child.0.wait().unwrap();
    let mut errors = String::new();
    std::io::Read::read_to_string(&mut child.0.stderr.take().unwrap(), &mut errors).unwrap();
    assert!(status.success(), "{status}: {errors}\n{output}");
    assert_eq!(signalled, stop_signal.is_some());
    let rows = fixture.rows.lock().unwrap().clone();
    assert!(
        output.contains(&format!(
            "Committed {} spans. Run ID: test-live",
            rows.len()
        )),
        "{output}"
    );
    rows
}

#[test]
fn two_cycles_commit_complete_traces_with_parallel_tools_and_search_retries() {
    let rows = exercise(&["--cycles", "2"], None);
    assert_eq!(rows.len(), 26);
    let traces: BTreeSet<_> = rows
        .iter()
        .map(|row| row["trace_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        traces,
        BTreeSet::from([
            "trace-01", "trace-02", "trace-03", "trace-04", "trace-05", "trace-06"
        ])
    );
    assert_eq!(
        rows.iter()
            .map(|row| row["span_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>()
            .len(),
        rows.len()
    );
    assert_eq!(
        rows.iter().filter(|row| row["status"] == "error").count(),
        2
    );
    assert_eq!(
        rows.iter().filter(|row| row["var"]["attempt"] == 2).count(),
        2
    );
    for trace in traces {
        let spans: Vec<_> = rows.iter().filter(|row| row["trace_id"] == trace).collect();
        let retry = matches!(trace, "trace-03" | "trace-06");
        let expected = if retry {
            vec!["plan", "search", "query", "search", "answer"]
        } else {
            vec!["plan", "search", "query", "answer"]
        };
        assert_eq!(
            spans
                .iter()
                .map(|span| span["step"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(spans[1]["time"], spans[2]["time"]);
        for span in &spans {
            assert_eq!(span["var"]["run_id"], "test-live");
            assert_eq!(span["var"]["model"], "claude-sonnet");
            let time: jiff::Timestamp = span["time"].as_str().unwrap().parse().unwrap();
            assert!(time <= jiff::Timestamp::now());
        }
        assert_eq!(spans[2]["status"], "ok");
        assert_eq!(spans[2]["var"]["tool"], "scopedb_query");
        if retry {
            assert_eq!(spans[1]["status"], "error");
            assert_eq!(spans[1]["duration_ms"], 1200);
            assert_eq!(spans[1]["var"]["attempt"], 1);
            assert_eq!(spans[1]["var"]["error"], "Search provider timed out");
            assert_eq!(spans[3]["status"], "ok");
            assert_eq!(spans[3]["duration_ms"], 180);
            assert_eq!(spans[3]["var"]["tool"], "web_search");
            assert_eq!(spans[3]["var"]["attempt"], 2);
            assert!(spans[3]["var"].get("error").is_none());
        } else {
            assert!(spans.iter().all(|span| span["status"] == "ok"));
        }
    }
}

#[test]
fn default_producer_flushes_buffered_spans_on_sigint() {
    assert_eq!(exercise(&[], Some("-INT")).len(), 13);
}

#[test]
fn default_producer_flushes_buffered_spans_on_sigterm() {
    assert_eq!(exercise(&[], Some("-TERM")).len(), 13);
}

#[test]
fn reused_run_id_is_rejected_before_appending() {
    let fixture = Fixture::new(13, true);
    let output = fixture.command().args(["--cycles", "1"]).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("choose a new SCOPEDB_DEMO_RUN"));
    assert!(fixture.rows.lock().unwrap().is_empty());
}

#[test]
fn unconfirmed_rows_do_not_print_a_commit_confirmation() {
    let fixture = Fixture::new(0, false);
    let output = fixture.command().args(["--cycles", "1"]).output().unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("Committed "));
}

#[test]
fn invalid_run_ids_and_cycle_limits_fail_before_connecting() {
    for run_id in ["", "not_safe", "quote'", "非ASCII", &"a".repeat(101)] {
        let output = Command::new(env!("CARGO_BIN_EXE_agent-runs"))
            .env("SCOPEDB_ENDPOINT", "http://127.0.0.1:1")
            .env("SCOPEDB_API_KEY", "test-key")
            .env("SCOPEDB_DEMO_RUN", run_id)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("SCOPEDB_DEMO_RUN must contain"));
    }
    for args in [
        vec!["--cycles", "-1"],
        vec!["--cycles", "1.5"],
        vec!["--cycles"],
        vec!["--unknown"],
    ] {
        assert!(
            !Command::new(env!("CARGO_BIN_EXE_agent-runs"))
                .args(args)
                .status()
                .unwrap()
                .success()
        );
    }
}
