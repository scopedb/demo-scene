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


def capture(failure=False):
    trace = "0123456789abcdef0123456789abcdef"
    definitions = [
        ("claude_code.interaction", "0000000000000001", "", 0, 9000, {}),
        (
            "claude_code.llm_request",
            "0000000000000002",
            "0000000000000001",
            0,
            3000,
            {"model": "test-model", "gen_ai.usage.input_tokens": 120},
        ),
        (
            "claude_code.tool",
            "0000000000000003",
            "0000000000000001",
            3000,
            500,
            {"tool_name": "Read"},
        ),
        (
            "claude_code.tool.execution",
            "0000000000000004",
            "0000000000000003",
            3100,
            300,
            {"tool_name": "Read"},
        ),
        (
            "claude_code.llm_request",
            "0000000000000005",
            "0000000000000001",
            3500,
            2000,
            {"model": "test-model"},
        ),
        (
            "claude_code.tool",
            "0000000000000006",
            "0000000000000001",
            5500,
            400,
            {"tool_name": "Grep"},
        ),
        (
            "claude_code.tool.execution",
            "0000000000000007",
            "0000000000000006",
            5600,
            200,
            {"tool_name": "Grep"},
        ),
    ]
    spans = []
    for name, span_id, parent, offset, duration, attributes in definitions:
        error = failure and span_id == "0000000000000004"
        spans.append(
            {
                "traceId": trace,
                "spanId": span_id,
                "parentSpanId": parent,
                "name": name,
                "kind": 1,
                "startTimeUnixNano": str(1793100000000000000 + offset * 1000000),
                "endTimeUnixNano": str(
                    1793100000000000000 + (offset + duration) * 1000000
                ),
                "status": {"code": 2 if error else 0},
                "attributes": [
                    {
                        "key": key,
                        "value": {"stringValue": value}
                        if isinstance(value, str)
                        else {"intValue": str(value)},
                    }
                    for key, value in attributes.items()
                ],
            }
        )
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "claude-code"},
                        },
                    ]
                },
                "scopeSpans": [{"scope": {"name": "test-provider"}, "spans": spans}],
            }
        ]
    }


class Destination:
    def __init__(self, previous=0, fail_append=False):
        self.rows = []
        self.previous = previous
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
                    count = "AGGREGATE count()" in statement
                    body = {
                        "status": "finished",
                        "statement_id": "00000000-0000-4000-8000-000000000001",
                        "created_at": "2026-10-27T00:00:00Z",
                        "progress": {},
                        "result_set": {
                            "format": "json",
                            "metadata": {
                                "fields": [{"name": "spans", "data_type": "int"}]
                                if count
                                else [],
                                "num_rows": 1 if count else 0,
                            },
                            "rows": [[str(owner.previous + len(owner.rows))]]
                            if count
                            else [],
                        },
                    }
                elif (
                    self.path
                    == "/v1/catalog/databases/scopedb/schemas/guide_agents/tables/spans/ingest"
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
                    owner.rows.extend(batch)
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
        }

    def invoke(self, *args):
        return subprocess.run(
            [sys.executable, str(ROOT / "run.py"), *args],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=50,
            check=False,
        )

    def replay(self, failure=False):
        path = self.directory / "capture.json"
        path.write_text(json.dumps(capture(failure)))
        return self.invoke("--replay", str(path))

    def claude_fixture(self, logged_in=True):
        path = self.directory / "claude"
        path.write_text(f"""#!{sys.executable}
import json,os,signal,sys,threading,urllib.request
assert 'SCOPEDB_API_KEY' not in os.environ
assert 'SCOPEDB_ENDPOINT' not in os.environ
if sys.argv[1:] == ['auth','status']:
    print(json.dumps({{'loggedIn': {logged_in!r}}})); sys.exit(0)
assert os.environ['OTEL_LOG_TOOL_DETAILS'] == '0'
assert os.environ['OTEL_EXPORTER_OTLP_TRACES_PROTOCOL'] == 'http/json'
assert sys.argv[sys.argv.index('--tools')+1] == 'Read,Glob,Grep'
if 'no-spans' in sys.argv:
    sys.exit(0)
payload = {capture()!r}
resource=payload['resourceSpans'][0]
run_id=os.environ['OTEL_RESOURCE_ATTRIBUTES'].split('=',1)[1]
resource['resource']['attributes'].append({{'key':'scopedb.demo.run_id','value':{{'stringValue':run_id}}}})
spans=resource['scopeSpans'][0]['spans']
root=spans.pop(0)
def send(batch):
    resource['scopeSpans'][0]['spans']=batch
    request=urllib.request.Request(os.environ['OTEL_EXPORTER_OTLP_TRACES_ENDPOINT'],json.dumps(payload).encode(),{{'Content-Type':'application/json'}})
    with urllib.request.urlopen(request,timeout=10) as response: response.read()
stopping=threading.Event()
signal.signal(signal.SIGINT,lambda *_:stopping.set())
signal.signal(signal.SIGTERM,lambda *_:stopping.set())
send(spans)
if '-p' not in sys.argv:
    print('Fixture Claude waiting',flush=True)
    assert stopping.wait(15)
send([root])
sys.exit(3 if 'fail-exit' in sys.argv else 0)
""")
        path.chmod(0o700)
        return str(path)

    def test_native_mapping_preserves_ids_parentage_metadata_and_unset_status(self):
        output = self.replay()
        self.assertEqual(output.returncode, 0, output.stderr + output.stdout)
        self.assertIn("Committed 7 spans. Run ID: test-live", output.stdout)
        self.assertEqual(len(self.destination.rows), 7)
        rows = {row["span_id"]: row for row in self.destination.rows}
        self.assertEqual(rows["0000000000000004"]["parent_span_id"], "0000000000000003")
        self.assertEqual(rows["0000000000000003"]["parent_span_id"], "0000000000000001")
        self.assertEqual(
            rows["0000000000000002"]["var"]["gen_ai.usage.input_tokens"], 120
        )
        self.assertEqual(rows["0000000000000004"]["duration_ns"], 300000000)
        self.assertTrue(all(row["status"] == "unset" for row in rows.values()))
        self.assertTrue(all(row["run_id"] == "test-live" for row in rows.values()))

    def test_failed_child_is_preserved_even_when_parent_is_unset(self):
        output = self.replay(failure=True)
        self.assertEqual(output.returncode, 0, output.stderr + output.stdout)
        errors = [row for row in self.destination.rows if row["status"] == "error"]
        self.assertEqual(
            [row["name"] for row in errors], ["claude_code.tool.execution"]
        )
        root = next(
            row
            for row in self.destination.rows
            if row["name"] == "claude_code.interaction"
        )
        self.assertEqual(root["status"], "unset")

    def test_reused_run_is_rejected_before_receiving_data(self):
        self.destination.previous = 1
        output = self.replay()
        self.assertNotEqual(output.returncode, 0)
        self.assertIn("choose a new SCOPEDB_DEMO_RUN", output.stderr)
        self.assertEqual(self.destination.rows, [])

    def test_rejected_append_never_reports_confirmed_delivery(self):
        self.destination.fail_append = True
        output = self.replay()
        self.assertNotEqual(output.returncode, 0)
        self.assertNotIn("Committed ", output.stdout)

    def test_launcher_waits_for_claude_final_export_and_keeps_credentials_out(self):
        output = self.invoke("--claude", self.claude_fixture())
        self.assertEqual(output.returncode, 0, output.stderr + output.stdout)
        self.assertIn("Committed 7 spans", output.stdout)
        self.assertEqual(len(self.destination.rows), 7)

    def test_claude_failure_keeps_its_exit_status_after_delivery(self):
        output = self.invoke("--claude", self.claude_fixture(), "--prompt", "fail-exit")
        self.assertEqual(output.returncode, 3, output.stderr + output.stdout)
        self.assertIn("Committed 7 spans", output.stdout)

    def test_authentication_is_checked_before_creating_tables(self):
        output = self.invoke("--claude", self.claude_fixture(logged_in=False))
        self.assertNotEqual(output.returncode, 0)
        self.assertIn("claude auth login", output.stderr)
        self.assertEqual(self.destination.requests, [])

    def test_sigint_can_cancel_delivery_wait_after_claude_exits(self):
        with subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--claude",
                self.claude_fixture(),
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
            output += remaining
            self.assertEqual(process.returncode, 130, errors + output)
            self.assertNotIn("Committed ", output)

    def test_interactive_sigint_keeps_receiver_alive_for_final_spans(self):
        process = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "run.py"),
                "--interactive",
                "--claude",
                self.claude_fixture(),
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
        while "Fixture Claude waiting" not in output:
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
        self.assertIn("Committed 7 spans", output)
        self.assertEqual(len(self.destination.rows), 7)


if __name__ == "__main__":
    unittest.main()
