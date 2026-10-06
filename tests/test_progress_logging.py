from __future__ import annotations

import io
import json
import os
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from progress_logging import Progress, _process_metrics, progress_interval


def events(stream):
    return [json.loads(line.removeprefix("[progress] "))
            for line in stream.getvalue().splitlines()]


class ProgressLoggingTests(unittest.TestCase):
    def test_heartbeat_runs_while_work_is_blocked_and_stops_on_exit(self):
        output = io.StringIO()
        heartbeat = threading.Event()

        class Observe(io.StringIO):
            def write(self, value):
                result = output.write(value)
                if '"event": "heartbeat"' in value:
                    heartbeat.set()
                return result

        with Progress("long-query", interval=0.02, stream=Observe(), processed_rows=0) as progress:
            self.assertTrue(heartbeat.wait(2), "no heartbeat during blocked work")
            progress.event("dedup-pass", pass_number=1, changed_rows=18)
            progress.update(processed_rows=18)
        recorded = events(output)
        self.assertEqual(recorded[0]["event"], "started")
        self.assertIn("heartbeat", [event["event"] for event in recorded])
        self.assertEqual(recorded[-1]["event"], "completed")
        self.assertEqual(recorded[-1]["changed_rows"], 18)
        self.assertFalse(progress._thread.is_alive())
        count = len(recorded)
        time.sleep(0.05)
        self.assertEqual(len(events(output)), count)

    def test_failure_is_logged_without_exception_text_and_threads_are_stopped(self):
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, "sensitive"):
            with Progress("test-failure", interval=0.01, stream=output) as progress:
                raise RuntimeError("sensitive-password-and-sql-payload")
        self.assertEqual(events(output)[-1]["event"], "failed")
        self.assertNotIn("sensitive", output.getvalue())
        self.assertFalse(progress._thread.is_alive())

    def test_interrupt_is_logged_and_not_swallowed(self):
        output = io.StringIO()
        with self.assertRaises(KeyboardInterrupt):
            with Progress("interrupted", stream=output) as progress:
                raise KeyboardInterrupt
        self.assertEqual(events(output)[-1]["event"], "failed")
        self.assertFalse(progress._thread.is_alive())

    def test_unchanged_counter_does_not_fake_advancement(self):
        output = io.StringIO()
        with Progress("unchanged", interval=100, stream=output, processed_rows=0) as progress:
            old = progress._advanced
            progress.update(processed_rows=0)
            self.assertEqual(progress._advanced, old)
            progress.update(processed_rows=1)
            self.assertGreaterEqual(progress._advanced, old)

    def test_unsafe_fields_are_rejected_without_echoing_values(self):
        for fields in ({"password": "do-not-log"}, {"query": "SELECT private"},
                       {"metadata_xml": "private"}, {"step": "line\nbreak"},
                       {"event": "override"}, {"rows": float("nan")},
                       {"values": [1, 2]}, {"rows": 10 ** 1000}):
            with self.subTest(keys=list(fields)):
                with self.assertRaises(ValueError) as raised:
                    Progress("safe", **fields)
                self.assertNotIn("do-not-log", str(raised.exception))
                self.assertNotIn("SELECT private", str(raised.exception))

    def test_log_sink_failure_does_not_fail_work(self):
        class Closed:
            def write(self, value):
                raise OSError("log unavailable")
        with Progress("closed-sink", stream=Closed()):
            pass

    def test_default_output_is_stderr_not_machine_readable_stdout(self):
        output = io.StringIO()
        stdout = io.StringIO()
        with mock.patch("sys.stderr", output), mock.patch("sys.stdout", stdout):
            with Progress("validation"):
                pass
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(len(events(output)), 2)

    def test_resource_metrics_are_content_free_and_optional(self):
        metrics = _process_metrics(os.getpid())
        if Path("/proc/self/stat").exists():
            self.assertGreater(metrics["process_rss_bytes"], 0)
            self.assertGreaterEqual(metrics["process_cpu_seconds"], 0)
        self.assertEqual(_process_metrics(999999999), {})
        output = io.StringIO()
        with Progress("resources", stream=output, process_pid=os.getpid(), disk_path=Path("/tmp")):
            pass
        self.assertIn("disk_free_bytes", events(output)[-1])
        self.assertNotIn("command", output.getvalue())

    def test_configuration_is_validated(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(progress_interval(), 30)
        with mock.patch.dict(os.environ, {"OJS_PROGRESS_SECONDS": "60"}):
            self.assertEqual(progress_interval(), 60)
        for value in ("invalid", "nan", "inf", "0", "-1", "3601"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                progress_interval(value)


if __name__ == "__main__":
    unittest.main()
