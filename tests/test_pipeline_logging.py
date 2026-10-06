from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import run_pipeline
from progress_logging import Progress


def events(output: str) -> list[dict]:
    return [json.loads(line[len("[progress] "):]) for line in output.splitlines()
            if line.startswith("[progress] ")]


class PipelineLoggingTest(unittest.TestCase):
    def make_server(self, directory: str):
        server = run_pipeline.MySQLServer(
            run_pipeline.MySQLTools("unused", "unused", "unused", "unused", "8.4.0"),
            Path(directory) / "mysql", "128M",
        )
        server.progress_seconds = 0.03
        self.addCleanup(server.shutdown, None)
        return server

    def test_marker_parser_rejects_payloads_and_oversized_lines(self):
        output = io.StringIO()
        rows = (
            b"private XML and credentials\n"
            b"OJS_PROGRESS_V1\tstage\tsecret-value\n"
            b"OJS_PROGRESS_V1\tdedup_pass\t2\t-4\n"
            b"OJS_PROGRESS_V1\tdedup_pass\t65\t1\n"
            b"OJS_PROGRESS_V1\tstage\tsource_index\textra\n"
            + b"x" * 10000 + b"OJS_PROGRESS_V1\tstage\tschema\n"
            b"OJS_PROGRESS_V1\tstage\tnoisy_keys\n"
            b"OJS_PROGRESS_V1\tdedup_pass\t2\t4\n"
            b"OJS_PROGRESS_V1\tmetadata_rows\t17\n"
        )
        with Progress("fixture", interval=10, stream=output) as progress:
            for line in run_pipeline.bounded_lines(io.BytesIO(rows)):
                run_pipeline.consume_sql_progress(line, progress)
        logged = events(output.getvalue())
        self.assertEqual([row["event"] for row in logged],
                         ["started", "sql-stage", "dedup-pass", "metadata-rows", "completed"])
        self.assertEqual(logged[2]["changed_rows"], 4)
        self.assertEqual(logged[3]["extracted_rows"], 17)
        self.assertNotIn("private", output.getvalue())
        self.assertNotIn("secret-value", output.getvalue())

    def test_sql_progress_streams_before_statement_finishes_and_heartbeats(self):
        with TemporaryDirectory() as directory:
            server = self.make_server(directory)
            output = io.StringIO()
            script = (
                "import sys,time; sys.stdin.read(); "
                "print('OJS_PROGRESS_V1\\tstage\\tdeduplication',flush=True); "
                "print('OJS_PROGRESS_V1\\tdedup_pass\\t1\\t7',flush=True); "
                "print('private-record-'+'x'*20000,flush=True); time.sleep(.2); "
                "print('OJS_PROGRESS_V1\\tdedup_pass\\t2\\t0',flush=True)"
            )
            with mock.patch.object(server, "_client_args", return_value=[sys.executable, "-u", "-c", script]):
                with contextlib.redirect_stderr(output):
                    server.run_sql_text("private SQL", label="fixture", database="fixture", password="secret")
            logged = events(output.getvalue())
            milestones = [row for row in logged if row["event"] == "dedup-pass"]
            self.assertEqual([row["changed_rows"] for row in milestones], [7, 0])
            heartbeat = [row for row in logged if row["event"] == "heartbeat"]
            self.assertTrue(heartbeat)
            self.assertEqual(heartbeat[0]["changed_rows"], 7)
            self.assertEqual(logged[-1]["event"], "completed")
            self.assertNotIn("private", output.getvalue())
            self.assertNotIn("secret", output.getvalue())
            self.assertIn("--unbuffered", server._client_args())
            self.assertFalse(server._clients)
            self.assertFalse(any(thread.name == "ojs-mysql-output" for thread in threading.enumerate()))

    def test_import_heartbeat_continues_during_blocked_pipe_and_final_wait(self):
        with TemporaryDirectory() as directory:
            server = self.make_server(directory)
            output = io.StringIO()
            script = (
                "import sys,time; time.sleep(.15); sys.stdin.buffer.read(); "
                "print('OJS_PROGRESS_V1\\tstage\\tarticle_merges',flush=True); "
                "time.sleep(.15)"
            )
            payload = b"x" * (2 * 1024 * 1024)
            with mock.patch.object(server, "_client_args", return_value=[sys.executable, "-u", "-c", script]):
                with contextlib.redirect_stderr(output):
                    server._stream_into_mysql(
                        io.BytesIO(payload), label="fixture", total=len(payload),
                        database="fixture", password=None, progress_seconds=.03,
                        calculate_sha256=True,
                    )
            heartbeat = [row for row in events(output.getvalue()) if row["event"] == "heartbeat"]
            self.assertTrue(any(row["processed_bytes"] == 0 for row in heartbeat))
            self.assertTrue(any(row["step"] == "waiting-for-mysql" for row in heartbeat))
            self.assertNotIn("sql-stage", output.getvalue())
            self.assertEqual(events(output.getvalue())[-1]["processed_bytes"], len(payload))

    def test_failure_has_safe_code_and_no_sql_or_password(self):
        with TemporaryDirectory() as directory:
            server = self.make_server(directory)
            output = io.StringIO()
            script = (
                "import sys; sys.stdin.read(); "
                "sys.stderr.write('ERROR 1064 (42000) at line 12: private SQL password-value\\n'); "
                "sys.exit(1)"
            )
            with mock.patch.object(server, "_client_args", return_value=[sys.executable, "-u", "-c", script]):
                with contextlib.redirect_stderr(output):
                    with self.assertRaisesRegex(run_pipeline.PipelineError, "mysql_error=1064 sql_line=12") as raised:
                        server.run_sql_text("private SQL", label="fixture", database="fixture", password="password-value")
            self.assertNotIn("private", str(raised.exception) + output.getvalue())
            self.assertNotIn("password-value", str(raised.exception) + output.getvalue())
            self.assertEqual(events(output.getvalue())[-1]["event"], "failed")

    def test_control_command_failure_never_emits_completed(self):
        with TemporaryDirectory() as directory:
            server = self.make_server(directory)
            server.server_log.write_text(
                "[ERROR] [MY-010270] [Server] private diagnostic\n", encoding="ascii"
            )
            for allow_failure in (False, True):
                with self.subTest(allow_failure=allow_failure):
                    output = io.StringIO()
                    with contextlib.redirect_stderr(output):
                        args = [sys.executable, "-c", "raise SystemExit(7)"]
                        if allow_failure:
                            result = server._run_control(args, label="fixture", allow_failure=True)
                            self.assertEqual(result.returncode, 7)
                        else:
                            with self.assertRaisesRegex(run_pipeline.PipelineError, "daemon_errors=MY-010270") as raised:
                                server._run_control(args, label="fixture")
                            self.assertNotIn("private", str(raised.exception))
                    self.assertEqual(events(output.getvalue())[-1]["event"], "failed")
                    self.assertNotIn('"completed"', output.getvalue())

    def test_large_checksum_uses_heartbeat_interval_and_preserves_digest(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "export.sql.gz"
            payload = b"fixture export"
            path.write_bytes(payload)
            output = io.StringIO()
            with mock.patch.object(run_pipeline, "BUFFER_SIZE", 4):
                with mock.patch.dict(os.environ, {"OJS_PROGRESS_SECONDS": "2"}):
                    with mock.patch.object(run_pipeline, "sha256_file", wraps=run_pipeline.sha256_file) as observed:
                        with contextlib.redirect_stderr(output):
                            actual = run_pipeline.sha256_small_file(path)
                        observed.assert_called_once_with(path, 2.0)
            self.assertEqual(actual, hashlib.sha256(payload).hexdigest())
            self.assertEqual(events(output.getvalue())[-1]["processed_bytes"], len(payload))

    def test_invalid_interval_does_not_echo_value(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            with self.assertRaises(SystemExit):
                run_pipeline.parser().parse_args(["--progress-seconds", "private-value"])
        self.assertNotIn("private-value", output.getvalue())

    def test_sigint_and_sigterm_cancel_worker_clients_without_hanging(self):
        project_root = Path(__file__).parents[1]
        script = """
import signal, sys, tempfile
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path.cwd() / 'src'))
import run_pipeline
with tempfile.TemporaryDirectory() as directory:
    server = run_pipeline.MySQLServer(run_pipeline.MySQLTools('x','x','x','x','8.4.0'), Path(directory)/'mysql', '128M')
    server.progress_seconds = .03
    server._client_args = lambda database=None: [sys.executable, '-u', '-c', 'import time; time.sleep(30)']
    server.execute = lambda *args, **kwargs: '1\\t20'
    try:
        with run_pipeline.interruptible_pipeline():
            run_pipeline.extract_metadata_in_parallel(server, metadata_sql='SELECT 1;', database='fixture', password=None, workers=2, progress_seconds=.03)
    except run_pipeline.PipelineInterrupted as exc:
        sys.exit(128 + exc.signum)
    finally:
        server.shutdown(None)
"""
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signum=signum):
                process = subprocess.Popen(
                    [sys.executable, "-u", "-c", script], cwd=project_root,
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                )
                prefix = []
                try:
                    for _ in range(30):
                        line = process.stderr.readline()
                        prefix.append(line)
                        if '"heartbeat"' in line:
                            break
                    else:
                        self.fail("worker heartbeat did not arrive")
                    process.send_signal(signum)
                    _, tail = process.communicate(timeout=10)
                    self.assertEqual(process.returncode, 128 + signum)
                    logged = events("".join(prefix) + tail)
                    pids = {row["process_pid"] for row in logged if "process_pid" in row}
                    self.assertTrue(pids)
                    for pid in pids:
                        with self.assertRaises(ProcessLookupError):
                            os.kill(pid, 0)
                    self.assertTrue(any(row["event"] == "failed" for row in logged))
                finally:
                    run_pipeline.stop_process(process)
                    process.stderr.close()


if __name__ == "__main__":
    unittest.main()
