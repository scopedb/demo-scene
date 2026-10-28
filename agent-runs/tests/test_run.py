import gzip
import http.server
import importlib.util
import json
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if sys.version_info < (3, 14):
    raise SystemExit(
        "Tests require Python 3.14 for stdlib zstd decoding; the launcher supports Python 3.11+"
    )
spec = importlib.util.spec_from_file_location("agent_run", ROOT / "run.py")
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


def attribute(key, value):
    field = (
        "boolValue"
        if isinstance(value, bool)
        else "intValue"
        if isinstance(value, int)
        else "stringValue"
    )
    return {"key": key, "value": {field: str(value) if field == "intValue" else value}}


def captures(failure=False):
    trace = "0123456789abcdef0123456789abcdef"
    spans = []
    for name, span_id, parent, offset, duration, attrs in (
        ("run_turn", "0000000000000001", "", 0, 9000, {}),
        (
            "try_run_sampling_request",
            "0000000000000002",
            "0000000000000001",
            0,
            3000,
            {"model": "test-model"},
        ),
        (
            "exec_command",
            "0000000000000003",
            "0000000000000001",
            3000,
            500,
            {"tool_name": "exec_command", "call_id": "test-call"},
        ),
        (
            "unified_exec.exec_command",
            "0000000000000004",
            "0000000000000003",
            3100,
            300,
            {},
        ),
    ):
        spans.append(
            {
                "traceId": trace,
                "spanId": span_id,
                "parentSpanId": parent,
                "name": name,
                "startTimeUnixNano": str(1793100000000000000 + offset * 1000000),
                "endTimeUnixNano": str(
                    1793100000000000000 + (offset + duration) * 1000000
                ),
                "status": {"code": 0},
                "attributes": [
                    attribute(k, v)
                    for k, v in {
                        **attrs,
                        "cwd": "excluded-path",
                        "arguments": "excluded-arguments",
                        "output": "excluded-output",
                    }.items()
                ],
            }
        )
    resource = {
        "attributes": [
            attribute("service.name", "codex"),
            attribute("user.email", "excluded-identity"),
        ]
    }
    logs = [
        {
            "timeUnixNano": "1793100003500000000",
            "traceId": "abcdef0123456789abcdef0123456789",
            "spanId": "0000000000000003",
            "body": {"stringValue": "excluded-body"},
            "attributes": [
                attribute(k, v)
                for k, v in {
                    "event.name": "codex.tool_result",
                    "tool_name": "exec_command",
                    "call_id": "test-call",
                    "model": "test-model",
                    "duration_ms": "500",
                    "success": "false" if failure else "true",
                    "arguments": "excluded-arguments",
                    "output": "excluded-output",
                    "user.account_id": "excluded-account",
                    "user.email": "excluded-identity",
                }.items()
            ],
        }
    ]
    return {
        "resourceSpans": [
            {
                "resource": resource,
                "scopeSpans": [{"scope": {"name": "fixture"}, "spans": spans}],
            }
        ],
    }, {
        "resourceLogs": [
            {
                "resource": resource,
                "scopeLogs": [{"scope": {"name": "fixture"}, "logRecords": logs}],
            }
        ],
    }


class Destination:
    def __init__(self, previous_table=None, fail_append=False):
        self.rows = {"codex_spans": [], "codex_events": []}
        self.previous_table = previous_table
        self.fail_append = fail_append
        self.requests = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                owner.requests.append(self.path)
                data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.headers.get("Content-Encoding") == "gzip":
                    data = gzip.decompress(data)
                if self.headers.get("Content-Encoding") == "zstd":
                    from compression import zstd

                    data = zstd.decompress(data)
                if self.path == "/v1/statements":
                    statement = json.loads(data)["statement"]
                    table = (
                        "codex_events" if "codex_events" in statement else "codex_spans"
                    )
                    records = len(owner.rows[table]) + int(
                        owner.previous_table == table
                    )
                    count = "AGGREGATE count()" in statement
                    body = {
                        "status": "finished",
                        "statement_id": "00000000-0000-4000-8000-000000000001",
                        "created_at": "2026-10-27T00:00:00Z",
                        "progress": {},
                        "result_set": {
                            "format": "json",
                            "metadata": {
                                "fields": [{"name": "records", "data_type": "int"}]
                                if count
                                else [],
                                "num_rows": 1 if count else 0,
                            },
                            "rows": [[str(records)]] if count else [],
                        },
                    }
                elif self.path in (
                    "/v1/catalog/databases/scopedb/schemas/guide_agents/tables/codex_spans/ingest",
                    "/v1/catalog/databases/scopedb/schemas/guide_agents/tables/codex_events/ingest",
                ):
                    if owner.fail_append:
                        self.send_response(403)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(
                            json.dumps(
                                {"code": "forbidden", "message": "fixture rejection"}
                            ).encode()
                        )
                        return
                    batch = [json.loads(line) for line in data.splitlines()]
                    table = self.path.split("/")[-2]
                    owner.rows[table].extend(batch)
                    body = {"status": "committed", "num_rows_inserted": len(batch)}
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join()


class LauncherTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.telescope = demo.telescope_binary(os.environ.get("TELESCOPE_BINARY"))

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="agent-tests-")
        self.addCleanup(self.directory.cleanup)
        self.directory = Path(self.directory.name)
        self.destination = Destination()
        self.addCleanup(self.destination.close)
        self.environment = {
            **os.environ,
            "SCOPEDB_ENDPOINT": self.destination.endpoint,
            "SCOPEDB_API_KEY": "test-key",
            "SCOPEDB_DEMO_RUN": "test-live",
            "TELESCOPE_BINARY": self.telescope,
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://excluded.invalid",
        }

    def invoke(self, *args):
        return subprocess.run(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--codex",
                self.codex_fixture(),
                *args,
            ],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=50,
            check=False,
        )

    def codex_fixture(self, logged_in=True, version="0.160.0"):
        path = self.directory / "codex"
        path.write_text(f"""#!{sys.executable}
import json,os,signal,sys,threading,urllib.request
assert not any(key.startswith(('SCOPEDB_', 'TELESCOPE_', 'OTEL_')) for key in os.environ)
if sys.argv[1:] == ['--version']:
    print('codex-cli {version}'); sys.exit(0)
if sys.argv[1:] == ['login','status']:
    print('excluded-authentication-details',file=sys.stderr); sys.exit({0 if logged_in else 1})
assert sys.argv[1] == 'exec'
assert '--ignore-user-config' in sys.argv
assert '--ephemeral' in sys.argv
assert sys.argv[sys.argv.index('--sandbox')+1] == 'read-only'
assert 'otel.log_user_prompt=false' in sys.argv
assert 'otel.metrics_exporter="none"' in sys.argv
if 'no-spans' in sys.argv: sys.exit(0)
traces,logs={captures()!r}
if 'obsolete' in sys.argv[-1]:
    logs['resourceLogs'][0]['scopeLogs'][0]['logRecords'][0]['attributes']=[
        dict(item,value={{'stringValue':'false'}}) if item['key']=='success' else item
        for item in logs['resourceLogs'][0]['scopeLogs'][0]['logRecords'][0]['attributes']]
root=traces['resourceSpans'][0]['scopeSpans'][0]['spans'].pop(0)
def endpoint(setting):
    override=next(arg for arg in sys.argv if arg.startswith('otel.'+setting+'={{'))
    return override.split('endpoint="',1)[1].split('"',1)[0]
def send(target,payload):
    request=urllib.request.Request(target,json.dumps(payload).encode(),{{'Content-Type':'application/json'}})
    with urllib.request.urlopen(request,timeout=10) as response: response.read()
stopping=threading.Event()
signal.signal(signal.SIGINT,lambda *_:stopping.set())
signal.signal(signal.SIGTERM,lambda *_:stopping.set())
send(endpoint('trace_exporter'),traces)
if 'wait-for-signal' in sys.argv:
    print('Fixture Codex waiting',flush=True)
    assert stopping.wait(15)
traces['resourceSpans'][0]['scopeSpans'][0]['spans']=[root]
send(endpoint('trace_exporter'),traces)
if 'no-events' not in sys.argv: send(endpoint('exporter'),logs)
sys.exit(3 if 'fail-exit' in sys.argv else 0)
""")
        path.chmod(0o700)
        return str(path)

    def test_native_projection_preserves_ids_and_excludes_private_fields(self):
        output = self.invoke()
        self.assertEqual(output.returncode, 0, output.stderr + output.stdout)
        self.assertIn("Committed 4 spans and 1 events", output.stdout)
        spans = self.destination.rows["codex_spans"]
        events = self.destination.rows["codex_events"]
        self.assertEqual(len(spans), 4)
        child = next(row for row in spans if row["name"] == "unified_exec.exec_command")
        self.assertEqual(child["parent_span_id"], "0000000000000003")
        self.assertEqual(child["duration_ns"], 300000000)
        self.assertNotEqual(events[0]["trace_id"], spans[0]["trace_id"])
        self.assertEqual(events[0]["call_id"], "test-call")
        self.assertIs(events[0]["success"], True)
        self.assertEqual(events[0]["duration_ms"], 500)
        self.assertTrue(all(row["run_id"] == "test-live" for row in spans + events))
        self.assertNotIn("excluded-", json.dumps(self.destination.rows))

    def test_failed_tool_event_is_visible_with_unset_spans(self):
        output = self.invoke("--failure-demo")
        self.assertEqual(output.returncode, 0, output.stderr + output.stdout)
        self.assertIs(self.destination.rows["codex_events"][0]["success"], False)
        self.assertTrue(
            all(
                row["status"] == "unset" for row in self.destination.rows["codex_spans"]
            )
        )

    def test_run_labels_are_strings_even_for_yaml_literals(self):
        for run_id in ("123", "true", "null"):
            with self.subTest(run_id=run_id):
                self.environment["SCOPEDB_DEMO_RUN"] = run_id
                self.destination.rows = {"codex_spans": [], "codex_events": []}
                output = self.invoke()
                self.assertEqual(output.returncode, 0, output.stderr + output.stdout)
                for rows in self.destination.rows.values():
                    self.assertTrue(rows)
                    self.assertTrue(all(row["run_id"] == run_id for row in rows))

    def test_missing_events_never_reports_confirmed_delivery(self):
        output = self.invoke("--prompt", "no-events")
        self.assertNotEqual(output.returncode, 0)
        self.assertIn("exported no events", output.stderr)
        self.assertNotIn("Committed ", output.stdout)

    def test_reused_run_in_either_table_is_rejected(self):
        for table in ("codex_spans", "codex_events"):
            with self.subTest(table=table):
                self.destination.previous_table = table
                output = self.invoke()
                self.assertNotEqual(output.returncode, 0)
                self.assertIn("choose a new SCOPEDB_DEMO_RUN", output.stderr)
                self.assertFalse(any(self.destination.rows.values()))

    def test_rejected_append_never_reports_confirmed_delivery(self):
        self.destination.fail_append = True
        output = self.invoke()
        self.assertNotEqual(output.returncode, 0)
        self.assertNotIn("Committed ", output.stdout)

    def test_codex_exit_status_is_preserved_after_delivery(self):
        output = self.invoke("--prompt", "fail-exit")
        self.assertEqual(output.returncode, 3, output.stderr + output.stdout)
        self.assertIn("Committed 4 spans and 1 events", output.stdout)

    def test_authentication_is_checked_before_creating_tables(self):
        output = subprocess.run(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--codex",
                self.codex_fixture(logged_in=False),
            ],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertNotEqual(output.returncode, 0)
        self.assertIn("codex login", output.stderr)
        self.assertNotIn(
            "excluded-authentication-details", output.stderr + output.stdout
        )
        self.assertEqual(self.destination.requests, [])

    def test_unsupported_cli_fails_before_creating_tables(self):
        output = subprocess.run(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--codex",
                self.codex_fixture(version="0.100.0"),
            ],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertNotEqual(output.returncode, 0)
        self.assertIn("0.160.0 or later", output.stderr)
        self.assertEqual(self.destination.requests, [])

    def test_no_spans_is_an_error(self):
        output = self.invoke("--prompt", "no-spans")
        self.assertNotEqual(output.returncode, 0)
        self.assertIn("exported no spans", output.stderr)
        self.assertNotIn("Committed ", output.stdout)

    def test_sigint_can_cancel_delivery_wait_after_codex_exits(self):
        with subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--codex",
                self.codex_fixture(),
                "--prompt",
                "no-spans",
            ],
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as process:
            output = ""
            while "Waiting for ScopeDB delivery" not in output:
                line = process.stdout.readline()
                self.assertTrue(line, "Launcher exited before delivery waiting began")
                output += line
            process.send_signal(signal.SIGINT)
            remaining, errors = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 130, errors + output + remaining)
            self.assertNotIn("Committed ", output + remaining)

    def test_sigint_keeps_receiver_available_for_final_exports(self):
        process = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--codex",
                self.codex_fixture(),
                "--prompt",
                "wait-for-signal",
            ],
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        lines = queue.Queue()

        def read_lines():
            for line in process.stdout:
                lines.put(line)
            lines.put(None)

        reader = threading.Thread(target=read_lines, daemon=True)
        reader.start()
        output = ""
        while "Fixture Codex waiting" not in output:
            line = lines.get(timeout=20)
            self.assertIsNotNone(line)
            output += line
        os.killpg(process.pid, signal.SIGINT)
        while True:
            line = lines.get(timeout=20)
            if line is None:
                break
            output += line
        self.assertEqual(process.wait(timeout=20), 0, process.stderr.read() + output)
        reader.join()
        process.stdout.close()
        process.stderr.close()
        self.assertIn("Committed 4 spans and 1 events", output)
        self.assertEqual(len(self.destination.rows["codex_spans"]), 4)
        self.assertEqual(len(self.destination.rows["codex_events"]), 1)


if __name__ == "__main__":
    unittest.main()
