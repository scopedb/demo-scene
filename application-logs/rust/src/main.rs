use std::env;
use std::error::Error;
use std::future::Future;
use std::io::Write;
use std::time::Duration;

use scopedb_client::Client;
use serde_json::json;
use tokio::time::sleep;

const SCHEMA: &str = "guide_logs";
const TABLE: &str = "requests";

#[tokio::main]
async fn main() {
    if let Err(error) = execute().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn execute() -> Result<(), Box<dyn Error>> {
    let cycles = parse_cycles(env::args().skip(1))?;
    let endpoint =
        env::var("SCOPEDB_ENDPOINT").map_err(|_| "Set SCOPEDB_ENDPOINT before running")?;
    let api_key = env::var("SCOPEDB_API_KEY").map_err(|_| "Set SCOPEDB_API_KEY before running")?;
    let run_id = env::var("SCOPEDB_DEMO_RUN").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    validate_run_id(&run_id)?;

    let client = Client::builder(endpoint).api_key(api_key).build()?;
    let mut out = std::io::stdout().lock();
    run(&client, &run_id, cycles, shutdown_signal(), &mut out).await?;
    Ok(())
}

async fn run<F, W>(
    client: &Client,
    run_id: &str,
    cycles: u64,
    shutdown: F,
    out: &mut W,
) -> Result<u64, Box<dyn Error>>
where
    F: Future<Output = ()>,
    W: Write,
{
    prepare(client, run_id).await?;

    let stream = client
        .table(TABLE)
        .with_schema(SCHEMA)
        .append_stream()
        .flush_interval(Duration::from_secs(1))
        .build()?;

    writeln!(
        out,
        "Streaming checkout logs. Run ID: {run_id}. Keep this terminal open; Ctrl+C flushes and stops."
    )?;

    let mut request_number = 0_u64;
    let mut sent_rows = 0_u64;
    let mut completed_cycles = 0_u64;
    let mut loop_error: Option<Box<dyn Error>> = None;
    tokio::pin!(shutdown);

    'outer: while cycles == 0 || completed_cycles < cycles {
        for release in ["1.8", "1.9"] {
            for gateway in ["credit_card", "bank_transfer"] {
                for attempt in 0..3 {
                    let row = checkout(run_id, &mut request_number, release, gateway, attempt);
                    serde_json::to_writer(&mut *out, &row)?;
                    writeln!(out)?;
                    if let Err(error) = stream.send(&row).await {
                        loop_error = Some(Box::new(error));
                        break 'outer;
                    }
                    sent_rows += 1;
                }
            }
        }
        completed_cycles += 1;
        if cycles > 0 && completed_cycles >= cycles {
            break;
        }
        tokio::select! {
            _ = &mut shutdown => break 'outer,
            _ = sleep(Duration::from_secs(1)) => {}
        }
    }

    let report = match stream.shutdown().await {
        Ok(report) => report,
        Err(error) => {
            return Err(match loop_error {
                Some(loop_error) => format!("{loop_error}; flush failed: {error}").into(),
                None => error.into(),
            });
        }
    };
    if report.committed_rows != sent_rows {
        return Err(format!(
            "the SDK confirmed {} of {sent_rows} logs; inspect this run before retrying",
            report.committed_rows
        )
        .into());
    }
    if let Some(error) = loop_error {
        return Err(error);
    }
    writeln!(out, "Committed {sent_rows} logs. Run ID: {run_id}")?;
    Ok(sent_rows)
}

async fn prepare(client: &Client, run_id: &str) -> Result<(), Box<dyn Error>> {
    client
        .query(format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        .await?;
    client
        .query(format!(
            "CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
  time timestamp, service string, level string, message string, var object
)"
        ))
        .await?;

    let result = client
        .query(format!(
            "FROM {SCHEMA}.{TABLE} WHERE var['run_id']::string = '{run_id}' \
             AGGREGATE count() AS requests"
        ))
        .await?;
    let rows = result.raw_rows();
    let previous = rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| value.as_deref())
        .ok_or("could not read the existing log count")?
        .parse::<u64>()?;
    if previous > 0 {
        return Err("this run ID already has logs; choose a new SCOPEDB_DEMO_RUN".into());
    }
    Ok(())
}

fn checkout(
    run_id: &str,
    request_number: &mut u64,
    release: &str,
    gateway: &str,
    attempt: u64,
) -> serde_json::Value {
    *request_number += 1;
    let request_id = format!("req-{request_number:02}");
    let (level, message) = if release == "1.9" && gateway == "credit_card" && attempt > 0 {
        ("error", "Credit card gateway timed out")
    } else {
        ("info", "Payment accepted")
    };
    json!({
        "time": jiff::Timestamp::now().to_string(),
        "service": "checkout",
        "level": level,
        "message": message,
        "var": {
            "run_id": run_id,
            "request_id": request_id,
            "release": release,
            "gateway": gateway,
        },
    })
}

fn parse_cycles(mut args: impl Iterator<Item = String>) -> Result<u64, Box<dyn Error>> {
    let mut cycles = 0;
    while let Some(argument) = args.next() {
        let value = if argument == "--cycles" {
            args.next().ok_or("--cycles needs a value")?
        } else if let Some(value) = argument.strip_prefix("--cycles=") {
            value.to_string()
        } else {
            return Err(format!("unrecognized argument: {argument}").into());
        };
        cycles = value
            .parse()
            .map_err(|_| format!("--cycles must be a nonnegative integer, got {value}"))?;
    }
    Ok(cycles)
}

fn validate_run_id(run_id: &str) -> Result<(), Box<dyn Error>> {
    if run_id.is_empty()
        || run_id.len() > 100
        || !run_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err("SCOPEDB_DEMO_RUN must contain 1-100 letters, digits, or hyphens".into());
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};

    use serde_json::Value;
    use serde_json::json;

    use super::*;

    struct MockServer {
        url: String,
        rows: Arc<Mutex<Vec<Value>>>,
        stop: Arc<AtomicBool>,
        handle: Option<JoinHandle<()>>,
    }

    impl MockServer {
        fn start() -> Result<Self, Box<dyn Error>> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            let url = format!("http://{}", listener.local_addr()?);
            let rows = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let thread_rows = Arc::clone(&rows);
            let thread_stop = Arc::clone(&stop);
            let handle = thread::spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if let Err(error) = handle_connection(&mut stream, &thread_rows) {
                                eprintln!("mock server: {error}");
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            });
            Ok(Self {
                url,
                rows,
                stop,
                handle: Some(handle),
            })
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn handle_connection(
        stream: &mut TcpStream,
        rows: &Arc<Mutex<Vec<Value>>>,
    ) -> Result<(), Box<dyn Error>> {
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let (path, compressed) = read_request(stream)?;
        let body = zstd::stream::decode_all(compressed.as_slice())?;
        let response = if path == "/v1/statements" {
            statement_response(&body)?
        } else if path == "/v1/catalog/databases/scopedb/schemas/guide_logs/tables/requests/ingest"
        {
            append_response(&body, rows)?
        } else {
            return Err(format!("unexpected path: {path}").into());
        };
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        )?;
        stream.flush()?;
        Ok(())
    }

    fn read_request(stream: &mut TcpStream) -> Result<(String, Vec<u8>), Box<dyn Error>> {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 8192];
        let header_end = loop {
            if let Some(position) = buffer.windows(4).position(|part| part == b"\r\n\r\n") {
                break position;
            }
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                return Err("connection closed before request headers".into());
            }
            buffer.extend_from_slice(&chunk[..read]);
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        let mut body = buffer[header_end + 4..].to_vec();
        while body.len() < content_length {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                return Err("connection closed before request body".into());
            }
            body.extend_from_slice(&chunk[..read]);
        }
        body.truncate(content_length);
        let path = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();
        Ok((path, body))
    }

    fn statement_response(body: &[u8]) -> Result<String, Box<dyn Error>> {
        let input: Value = serde_json::from_slice(body)?;
        let statement = input["statement"].as_str().unwrap_or_default();
        let count = statement.contains("AGGREGATE count()");
        let response = json!({
            "status": "finished",
            "statement_id": "01989a4e-4ee2-7e63-87a5-65ac3b5161dc",
            "created_at": "2026-10-27T00:00:00Z",
            "progress": {
                "compute_seconds": 0,
                "total_percentage": 100,
                "nanos_from_submitted": 0,
                "nanos_from_started": 0,
                "total_stages": 0,
                "total_partitions": 0,
                "total_rows": 0,
                "total_compressed_bytes": 0,
                "total_uncompressed_bytes": 0,
                "scanned_stages": 0,
                "scanned_partitions": 0,
                "scanned_rows": 0,
                "scanned_compressed_bytes": 0,
                "scanned_uncompressed_bytes": 0,
                "skipped_partitions": 0,
                "skipped_rows": 0,
                "skipped_compressed_bytes": 0,
                "skipped_uncompressed_bytes": 0
            },
            "result_set": {
                "format": "json",
                "metadata": {
                    "fields": if count { json!([{"name": "requests", "data_type": "int"}]) } else { json!([]) },
                    "num_rows": if count { 1 } else { 0 }
                },
                "rows": if count { json!([["0"]]) } else { json!([]) }
            }
        });
        Ok(response.to_string())
    }

    fn append_response(
        body: &[u8],
        rows: &Arc<Mutex<Vec<Value>>>,
    ) -> Result<String, Box<dyn Error>> {
        let text = String::from_utf8(body.to_vec())?;
        let mut count = 0;
        {
            let mut rows = rows.lock().unwrap();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                rows.push(serde_json::from_str(line)?);
                count += 1;
            }
        }
        Ok(json!({"status": "committed", "num_rows_inserted": count}).to_string())
    }

    fn test_client(server: &MockServer) -> Result<Client, Box<dyn Error>> {
        let http = scopedb_client::reqwest::Client::builder()
            .redirect(scopedb_client::reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .build()?;
        Ok(Client::builder(server.url.as_str())
            .api_key("test-key")
            .http_client(http)
            .build()?)
    }

    #[tokio::test]
    async fn produces_twelve_logs_per_cycle() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start()?;
        let client = test_client(&server)?;
        let mut out = Vec::new();
        let committed = run(&client, "test-live", 2, std::future::pending(), &mut out).await?;
        assert_eq!(committed, 24);

        let rows = server.rows.lock().unwrap();
        assert_eq!(rows.len(), 24);
        let errors: Vec<_> = rows.iter().filter(|row| row["level"] == "error").collect();
        assert_eq!(errors.len(), 4);
        assert!(errors.iter().all(|row| {
            row["var"]["release"] == "1.9" && row["var"]["gateway"] == "credit_card"
        }));
        let ids: HashSet<_> = rows
            .iter()
            .map(|row| row["var"]["request_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids.len(), rows.len());
        assert!(String::from_utf8(out)?.contains("Committed 24 logs"));
        Ok(())
    }

    #[tokio::test]
    async fn flushes_when_stopped() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start()?;
        let client = test_client(&server)?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        sender.send(()).unwrap();
        let mut out = Vec::new();
        let committed = run(
            &client,
            "test-stop",
            0,
            async move {
                let _ = receiver.await;
            },
            &mut out,
        )
        .await?;
        assert!(committed > 0 && committed % 12 == 0);
        assert_eq!(server.rows.lock().unwrap().len() as u64, committed);
        assert!(String::from_utf8(out)?.contains(&format!("Committed {committed} logs")));
        Ok(())
    }
}
