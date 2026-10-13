"""Per-recipient delivery intervals with one shared reporting/budget lock."""
from __future__ import annotations

from contextlib import ExitStack, redirect_stdout, redirect_stderr
from datetime import timedelta
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from test_email_progress import NOW, snapshot
import email_progress as report


class RecipientTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.primary = self.directory / "last-sent.json"
        self.subscriber = self.directory / "subscriber-colleague.json"
        self.current = snapshot(remaining_rows=100)

    def run_report(self, state=None, *args, failure=None):
        state = state or self.subscriber
        output = io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, {
                "OJS_PROGRESS_EMAIL_FROM": "sender@example.org",
                "OJS_PROGRESS_EMAIL_TO": "colleague@example.org",
                "OJS_PROGRESS_LLM_ENABLED": "1", "CREDENTIALS_DIRECTORY": "/credentials",
            }, clear=True))
            stack.enter_context(mock.patch.object(report, "collect_snapshot", return_value=self.current))
            stack.enter_context(mock.patch.object(report, "utc_now", return_value=NOW))
            password = stack.enter_context(mock.patch.object(report, "read_password", return_value="private"))
            send = stack.enter_context(mock.patch.object(report, "send_email", side_effect=failure))
            generate = stack.enter_context(mock.patch.object(report.progress_llm, "generate_summary",
                return_value="The recovery job is building article matching keys. No row count is available."))
            stack.enter_context(redirect_stdout(output))
            stack.enter_context(redirect_stderr(output))
            code = report.main(["--state-file", str(state), *args])
        return code, output.getvalue(), password, send, generate

    def save(self, state, age):
        report.write_state(state, report.comparable_state(snapshot(age=500, remaining_rows=150), NOW - age))

    def test_subscriber_uses_own_baseline_and_leaves_primary_state_unchanged(self):
        self.save(self.primary, timedelta(hours=1))
        self.save(self.subscriber, timedelta(hours=73))
        primary_before = self.primary.read_bytes()
        code, _, _, send, generate = self.run_report(None, "--min-send-interval-hours=72")
        self.assertEqual(code, 0)
        self.assertEqual(send.call_args.args[:2], ("sender@example.org", "colleague@example.org"))
        self.assertEqual(self.primary.read_bytes(), primary_before)
        self.assertEqual(report.read_state(self.subscriber)["sent_at"], report.stamp(NOW))
        self.assertEqual(generate.call_args.kwargs["budget_path"], self.directory / "llm-budget.json")
        self.assertEqual(generate.call_args.args[0]["comparison"]["counter_changes"]["remaining_rows"], -50)

    def test_three_day_guard_boundaries_and_future_state(self):
        for age, expected in ((timedelta(hours=72, seconds=-1), False),
                              (timedelta(hours=72), True),
                              (timedelta(hours=73), True),
                              (timedelta(seconds=-1), False)):
            with self.subTest(age=age):
                self.save(self.subscriber, age)
                before = self.subscriber.read_bytes()
                code, _, password, send, generate = self.run_report(None, "--min-send-interval-hours=72")
                self.assertEqual(code, 0)
                self.assertEqual(send.called, expected)
                self.assertEqual(generate.called, expected)
                self.assertEqual(password.called, expected)
                if not expected:
                    self.assertEqual(before, self.subscriber.read_bytes())

    def test_primary_default_guard_remains_eleven_hours(self):
        for hours, expected in ((10, False), (11, True)):
            self.save(self.primary, timedelta(hours=hours))
            code, _, _, send, _ = self.run_report(self.primary)
            self.assertEqual(code, 0)
            self.assertEqual(send.called, expected)

    def test_failed_send_does_not_change_either_recipient_state(self):
        self.save(self.primary, timedelta(hours=1))
        self.save(self.subscriber, timedelta(hours=73))
        before = [path.read_bytes() for path in (self.primary, self.subscriber)]
        code, output, _, send, _ = self.run_report(None, "--min-send-interval-hours=72", failure=OSError("private"))
        self.assertEqual(code, 1)
        self.assertNotIn("private", output)
        self.assertEqual(before, [path.read_bytes() for path in (self.primary, self.subscriber)])

    def test_subscribers_and_preview_share_primary_lock_before_api_or_secrets(self):
        with report.report_lock(self.primary) as primary_acquired:
            self.assertTrue(primary_acquired)
            with report.report_lock(self.subscriber) as acquired:
                self.assertFalse(acquired)
            for arguments in (("--min-send-interval-hours=72",), ("--preview-llm",)):
                code, _, password, send, generate = self.run_report(None, *arguments)
                password.assert_not_called()
                send.assert_not_called()
                generate.assert_not_called()
        self.assertEqual({item.name for item in self.directory.iterdir()}, {"last-sent.json.lock"})

    def test_bounded_lock_wait_can_acquire_after_contention(self):
        with mock.patch.object(report.fcntl, "flock", side_effect=[BlockingIOError(), None]) as flock, \
                mock.patch.object(report.time, "sleep") as sleep, \
                mock.patch.object(report.time, "monotonic", side_effect=[100, 100.1]):
            with report.report_lock(self.subscriber, wait_seconds=60) as acquired:
                self.assertTrue(acquired)
        self.assertEqual(flock.call_count, 2)
        sleep.assert_called_once_with(0.25)

    def test_lock_wait_timeout_is_failure_not_silent_three_day_skip(self):
        with report.report_lock(self.primary):
            with mock.patch.object(report.time, "monotonic", side_effect=[100, 160.1]):
                code, output, password, send, generate = self.run_report(None, "--lock-wait-seconds=60")
        self.assertEqual(code, 1)
        self.assertIn("wait expired", output)
        password.assert_not_called()
        send.assert_not_called()
        generate.assert_not_called()

    def test_option_bounds_fail_before_diagnostics_or_delivery(self):
        for option in ("--min-send-interval-hours=0", "--min-send-interval-hours=8761",
                       "--lock-wait-seconds=-1", "--lock-wait-seconds=61"):
            with self.subTest(option=option), mock.patch.object(report, "collect_snapshot") as collect:
                with self.assertRaises(SystemExit) as error, redirect_stderr(io.StringIO()):
                    report.main(["--dry-run", option])
                self.assertEqual(error.exception.code, 2)
                collect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
