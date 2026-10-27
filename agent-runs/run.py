"""Run Claude Code with a local Telescope receiver and confirmed ScopeDB delivery."""

import argparse
import hashlib
import io
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VERSION = "v0.3.0"
ARCHIVES = {
    ("Darwin", "arm64"): (
        "darwin_arm64",
        "79da5590e270afa44c5b07b43b48b78f6da7ed6aa4864d570f88b3e6f160bdb2",
    ),
    ("Linux", "x86_64"): (
        "linux_amd64",
        "a02b180e1e0968330d514fe50a8631e5eccb33678952bf7673b7c458fa2e3f2f",
    ),
    ("Linux", "aarch64"): (
        "linux_arm64",
        "837fcd57fd5d776bd826e48785d867747d29c2ab685e92db3c4df8a4d4e9065f",
    ),
}
PROMPT = (
    "Compare how bluesky-events and application-logs handle graceful shutdown "
    "and unconfirmed writes. Cite the exact files. Do not modify files."
)
FAILURE_PROMPT = (
    "Use Read to check bluesky-events/collector.go first; this may be an old filename. "
    "Then locate the current entry point and explain how it handles shutdown. "
    "Do not modify files."
)


def telescope_binary(override=None):
    if override:
        binary = Path(override).resolve()
    else:
        target = ARCHIVES.get((platform.system(), platform.machine()))
        if target is None:
            raise RuntimeError(
                "Use --telescope with a Telescope v0.3.0 binary for this platform"
            )
        name, digest = target
        cache = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
        binary = cache / "scopedb-demos" / f"telescope-{VERSION}-{name}" / "telescope"
        if not binary.exists():
            binary.parent.mkdir(parents=True, exist_ok=True)
            url = f"https://github.com/scopedb/telescope/releases/download/{VERSION}/telescope_{VERSION}_{name}.tar.gz"
            print(f"Downloading Telescope {VERSION} ({name})...", flush=True)
            with urllib.request.urlopen(url, timeout=60) as response:
                archive = response.read(32 * 1024 * 1024 + 1)
            if hashlib.sha256(archive).hexdigest() != digest:
                raise RuntimeError("Telescope archive checksum mismatch")
            # Extract only the expected executable, never archive-provided paths.
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
                member = bundle.getmember(f"telescope_{VERSION}_{name}/telescope")
                if not member.isfile():
                    raise RuntimeError(
                        "Telescope archive does not contain a regular executable"
                    )
                with (
                    bundle.extractfile(member) as source,
                    tempfile.NamedTemporaryFile(
                        dir=binary.parent, delete=False
                    ) as output,
                ):
                    temporary = Path(output.name)
                    shutil.copyfileobj(source, output)
                temporary.chmod(0o700)
                temporary.replace(binary)
    version = subprocess.check_output([str(binary), "version"], text=True).strip()
    if version != VERSION:
        raise RuntimeError(f"Expected Telescope {VERSION}, found {version}")
    return str(binary)


def read_json(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def wait_for(collector, operation, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if collector.poll() is not None:
            raise RuntimeError(f"Telescope exited while {description}")
        try:
            result = operation()
            if result is not None:
                return result
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Timed out while {description}")


def free_ports():
    # Hold all reservations together so the OS cannot return the same port twice.
    sockets = [socket.socket() for _ in range(4)]
    try:
        for reservation in sockets:
            reservation.bind(("127.0.0.1", 0))
        return [reservation.getsockname()[1] for reservation in sockets]
    finally:
        for reservation in sockets:
            reservation.close()


def query(binary, environment, statement):
    result = subprocess.run(
        [binary, "query", "--format", "json", statement],
        env=environment,
        capture_output=True,
        text=True,
        timeout=40,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "ScopeDB query failed")
    return json.loads(result.stdout)


def run(args):
    run_id = os.environ.get("SCOPEDB_DEMO_RUN", str(uuid.uuid4()))
    if not re.fullmatch(r"[A-Za-z0-9-]{1,100}", run_id):
        raise RuntimeError(
            "SCOPEDB_DEMO_RUN must contain 1–100 ASCII letters, digits, or hyphens"
        )
    for name in ("SCOPEDB_ENDPOINT", "SCOPEDB_API_KEY"):
        if not os.environ.get(name):
            raise RuntimeError(f"Set {name} before running")
    if not args.replay:
        auth_environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("SCOPEDB_", "TELESCOPE_", "OTEL_"))
        }
        auth = subprocess.run(
            [args.claude, "auth", "status"],
            env=auth_environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if auth.returncode or not json.loads(auth.stdout).get("loggedIn"):
            raise RuntimeError(
                "Sign in to Claude Code with 'claude auth login', then run this demo again"
            )
    binary = telescope_binary(args.telescope)
    collector_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("TELESCOPE_")
    }
    query(binary, collector_env, "CREATE SCHEMA IF NOT EXISTS guide_agents")
    query(
        binary,
        collector_env,
        (ROOT / "queries/01-table-definition.scopeql").read_text(),
    )
    previous = query(
        binary,
        collector_env,
        f"FROM guide_agents.spans WHERE run_id = '{run_id}' AGGREGATE count() AS spans",
    )
    if previous[0]["spans"]:
        raise RuntimeError(
            "This run ID already has spans; choose a new SCOPEDB_DEMO_RUN"
        )

    with tempfile.TemporaryDirectory(prefix="scopedb-agent-") as directory:
        directory = Path(directory)
        grpc_port, otlp_port, http_port, metrics_port = free_ports()
        config = (ROOT / "telescope.yaml").read_text()
        for original, selected in zip(
            (4317, 4318, 8080, 8888), (grpc_port, otlp_port, http_port, metrics_port)
        ):
            config = config.replace(f"127.0.0.1:{original}", f"127.0.0.1:{selected}")
        config_path = directory / "telescope.yaml"
        config_path.write_text(config)
        collector_env["TELESCOPE_INTERNAL_METRICS_URL"] = (
            f"http://127.0.0.1:{metrics_port}/metrics"
        )
        endpoint = f"http://127.0.0.1:{otlp_port}/v1/traces"
        operations = f"http://127.0.0.1:{http_port}"
        child = None
        collector = None
        old_handlers = {}

        def interrupted(number, _frame):
            if child is None or child.poll() is not None:
                raise KeyboardInterrupt
            # Terminal Ctrl+C already reaches Claude in the foreground process group.
            # Keep the receiver alive while Claude interrupts a turn or flushes on exit.
            if number == signal.SIGTERM and child.poll() is None:
                child.terminate()

        for number in (signal.SIGINT, signal.SIGTERM):
            old_handlers[number] = signal.signal(number, interrupted)
        try:
            with (directory / "telescope.log").open("w+") as log:
                collector = subprocess.Popen(
                    [binary, "run", str(config_path)],
                    env=collector_env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                wait_for(
                    collector,
                    lambda: read_json(operations + "/readyz"),
                    15,
                    "starting the receiver",
                )
                print(f"Tracing Claude Code. Run ID: {run_id}", flush=True)
                print(f"Local receiver: {endpoint}", flush=True)
                if args.replay:
                    payload = json.loads(args.replay.read_text())
                    for resource in payload.get("resourceSpans", []):
                        attributes = resource.setdefault("resource", {}).setdefault(
                            "attributes", []
                        )
                        attributes[:] = [
                            item
                            for item in attributes
                            if item["key"] != "scopedb.demo.run_id"
                        ]
                        attributes.append(
                            {
                                "key": "scopedb.demo.run_id",
                                "value": {"stringValue": run_id},
                            }
                        )
                    request = urllib.request.Request(
                        endpoint,
                        json.dumps(payload).encode(),
                        {"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(request, timeout=15) as response:
                        response.read()
                    code = 0
                else:
                    environment = {
                        key: value
                        for key, value in os.environ.items()
                        if not key.startswith(("SCOPEDB_", "TELESCOPE_", "OTEL_"))
                        and key not in ("TRACEPARENT", "TRACESTATE")
                    }
                    environment.update(
                        {
                            "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
                            "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA": "1",
                            "OTEL_TRACES_EXPORTER": "otlp",
                            "OTEL_METRICS_EXPORTER": "none",
                            "OTEL_LOGS_EXPORTER": "none",
                            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/json",
                            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": endpoint,
                            "OTEL_TRACES_EXPORT_INTERVAL": "1000",
                            "OTEL_RESOURCE_ATTRIBUTES": f"scopedb.demo.run_id={run_id}",
                            "OTEL_LOG_USER_PROMPTS": "0",
                            "OTEL_LOG_TOOL_DETAILS": "0",
                            "OTEL_LOG_TOOL_CONTENT": "0",
                        }
                    )
                    command = [
                        args.claude,
                        "--tools",
                        "Read,Glob,Grep",
                        "--allowedTools",
                        "Read,Glob,Grep",
                    ]
                    if not args.interactive:
                        command.append("-p")
                    command.extend(
                        [
                            "--",
                            args.prompt
                            or (FAILURE_PROMPT if args.failure_demo else PROMPT),
                        ]
                    )
                    child = subprocess.Popen(command, cwd=ROOT.parent, env=environment)
                    code = child.wait()

                print("Waiting for ScopeDB delivery...", flush=True)
                no_spans_deadline = time.monotonic() + 2

                def settled():
                    status = read_json(operations + "/v1/ingestion/status")
                    traces = next(
                        item for item in status["signals"] if item["signal"] == "traces"
                    )
                    if traces["unknown"] or traces["dropped"]:
                        raise RuntimeError(
                            "Some spans were dropped or have an unknown delivery outcome; inspect this run before retrying"
                        )
                    if not traces["received"] and time.monotonic() >= no_spans_deadline:
                        raise RuntimeError(
                            "Claude Code exported no spans; check tracing settings or run another task"
                        )
                    if traces["received"] and traces["written"] == traces["received"]:
                        return traces["written"]
                    return None

                written = wait_for(
                    collector,
                    settled,
                    30,
                    "confirming span delivery (check Claude telemetry if no spans arrive)",
                )
        except Exception:
            log_path = directory / "telescope.log"
            if log_path.exists():
                print(
                    "\n".join(log_path.read_text().splitlines()[-12:]),
                    file=sys.stderr,
                    flush=True,
                )
            raise
        finally:
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            if collector is not None and collector.poll() is None:
                collector.terminate()
                try:
                    collector.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    collector.kill()
                    collector.wait()
                    raise RuntimeError("Telescope did not finish graceful shutdown")
                if collector.returncode:
                    raise RuntimeError(
                        f"Telescope shutdown failed with exit code {collector.returncode}"
                    )
            for number, handler in old_handlers.items():
                signal.signal(number, handler)
        stored = query(
            binary,
            collector_env,
            f"FROM guide_agents.spans WHERE run_id = '{run_id}' AGGREGATE count() AS spans",
        )[0]["spans"]
        if stored != written:
            raise RuntimeError(
                f"Stored span count ({stored}) differs from confirmed delivery ({written}); inspect this run"
            )
        print(f"Committed {stored} spans. Run ID: {run_id}", flush=True)
        return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="keep Claude open for more read-only prompts; use /exit to finish",
    )
    parser.add_argument(
        "--failure-demo",
        action="store_true",
        help="ask Claude to check an obsolete filename before locating the current entry point",
    )
    parser.add_argument(
        "--prompt", help="use another read-only task in this repository"
    )
    parser.add_argument(
        "--replay",
        type=Path,
        help="send an existing OTLP JSON capture instead of starting Claude",
    )
    parser.add_argument(
        "--telescope",
        default=os.environ.get("TELESCOPE_BINARY"),
        help="use an installed Telescope v0.3.0 instead of downloading it",
    )
    parser.add_argument("--claude", default="claude", help="Claude Code executable")
    args = parser.parse_args()
    if args.replay and (args.interactive or args.failure_demo or args.prompt):
        parser.error("--replay cannot be combined with Claude task options")
    try:
        return run(args)
    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
