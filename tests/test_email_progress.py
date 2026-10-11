from __future__ import annotations

from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import smtplib
import ssl
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import email_progress as report


NOW = datetime(2026, 10, 11, 12, tzinfo=timezone.utc)
INVOCATION = "a" * 32
OLD_INVOCATION = "b" * 32
SECRET = "never-echo-this-secret-or-raw-sql"


def journal(stage="recovery-rollback", *, invocation=INVOCATION, age=10,
            event="heartbeat", **fields):
    value = {"stage": stage, "event": event, "timestamp": report.stamp(NOW - timedelta(seconds=age)),
             **fields}
    return json.dumps({"_SYSTEMD_INVOCATION_ID": invocation,
                       "__REALTIME_TIMESTAMP": str(int((NOW - timedelta(seconds=age)).timestamp() * 1_000_000)),
                       "MESSAGE": "[progress] " + json.dumps(value)})


def service_text(invocation=INVOCATION, active="activating", result="success"):
    return (f"ActiveState={active}\nSubState=start\nResult={result}\n"
            f"InvocationID={invocation}\nMainPID=1234\nMemoryCurrent=104857600\n"
            "ExecMainCode=0\nExecMainStatus=0\n")


def snapshot(*, stage="recovery-rollback", event="heartbeat", age=10, **fields):
    return {"observed_at": report.stamp(NOW), "service": report.parse_service(service_text()),
            "event": report.parse_events(journal(stage, event=event, age=age, **fields), INVOCATION)[0],
            "root_free": 20 * 1024 ** 3, "data_free": 19 * 1024 ** 3,
            "memory": {"MemAvailable": 1024 ** 3, "MemTotal": 2 * 1024 ** 3}, "warnings": []}


class SnapshotTests(unittest.TestCase):
    def test_properties_are_allowlisted_and_invalid_values_omitted(self):
        service = report.parse_service(service_text() + "ExecStart=" + SECRET + "\n"
                                       "MemoryCurrent=18446744073709551615\n"
                                       "Result=" + SECRET + "\n")
        self.assertEqual(service["MainPID"], 1234)
        self.assertEqual(service["InvocationID"], INVOCATION)
        self.assertNotIn(SECRET, json.dumps(service))
        unavailable = report.parse_service("MemoryCurrent=[not set]\nMainPID=-1\nInvocationID=" + "0" * 32)
        self.assertEqual(unavailable, {})

    def test_filters_old_invocations_and_internal_query_labels(self):
        lines = [journal(event="failed", invocation=OLD_INVOCATION),
                 journal(remaining_rows=29001002, pending_transactions=1),
                 journal("recovery-transactions", age=0),
                 journal("update-recover-exact-keys", age=0, step="supervise-child")]
        events = report.parse_events("\n".join(lines), INVOCATION)
        chosen = report.select_event(events)
        self.assertEqual(chosen["stage"], "recovery-rollback")
        self.assertEqual(chosen["event"], "heartbeat")
        self.assertEqual(chosen["remaining_rows"], 29001002)
        self.assertEqual(report.parse_events("\n".join(lines), ""), [])

    def test_finalization_is_preferred_to_supervisor_even_when_stale(self):
        events = report.parse_events("\n".join([
            journal("temporal finalization phase", step="exact_keys", age=600),
            journal("update-recover-exact-keys", step="supervise-child", age=0),
        ]), INVOCATION)
        selected = report.select_event(events)
        self.assertEqual(selected["stage"], "temporal finalization phase")
        current = snapshot()
        current["event"] = selected
        body = report.render_report(current)[1]
        self.assertIn("STALE stage observation", body)
        self.assertIn("current stage is unconfirmed", body)

    def test_current_new_stage_supersedes_rollback_completion(self):
        events = report.parse_events("\n".join([
            journal(event="completed", age=50, remaining_rows=0),
            journal("recovery-metadata-audit", age=5, checked_rows=123),
        ]), INVOCATION)
        self.assertEqual(report.select_event(events)["stage"], "recovery-metadata-audit")

    def test_arbitrary_strings_raw_logs_and_non_numeric_counters_never_escape(self):
        lines = [SECRET, "not JSON", "[]", journal(SECRET),
                 journal(event=SECRET), journal(step=SECRET, password=SECRET,
                         sql=SECRET, remaining_rows=SECRET, checked_rows=True,
                         total_ranges=-1, total_bytes=10 ** 100,
                         covered_source_rows=float("nan"), pending_transactions=1)]
        chosen = report.select_event(report.parse_events("\n".join(lines), INVOCATION))
        self.assertEqual(chosen["pending_transactions"], 1)
        for field in ("password", "sql", "step", "remaining_rows", "checked_rows", "total_ranges",
                      "total_bytes", "covered_source_rows"):
            self.assertNotIn(field, chosen)
        self.assertNotIn(SECRET, json.dumps(chosen))

    def test_malformed_payload_shapes_and_oversized_messages_are_ignored(self):
        valid = json.loads(journal())
        cases = []
        for message in (None, ["[progress] "], "prefix [progress] {}", "[progress] []",
                        "[progress] " + json.dumps({"stage": [], "event": "heartbeat"}),
                        "[progress] " + "a" * 4096):
            cases.append(json.dumps({**valid, "MESSAGE": message}))
        cases.append(json.dumps({**valid, "__REALTIME_TIMESTAMP": "bad"}))
        cases.append(json.dumps({**valid, "__REALTIME_TIMESTAMP": "9" * 20}))
        self.assertEqual(report.parse_events("\n".join(cases), INVOCATION), [])

    def test_collects_bounded_current_invocation_tail_and_two_known_filesystems(self):
        with mock.patch.object(report, "diagnostic", side_effect=[
                service_text(), journal(remaining_rows=500), service_text()]) as diagnostic, \
                mock.patch.object(report, "free_bytes", return_value=10) as disk, \
                mock.patch.object(report, "host_memory", return_value={}), \
                mock.patch.object(report, "utc_now", return_value=NOW):
            current = report.collect_snapshot(Path("/project"))
        self.assertEqual(current["event"]["remaining_rows"], 500)
        commands = [call.args[0] for call in diagnostic.call_args_list]
        self.assertEqual(commands[0][0], "systemctl")
        self.assertIn("--lines=300", commands[1])
        self.assertIn("_SYSTEMD_INVOCATION_ID=" + INVOCATION, commands[1])
        self.assertNotIn("--all", commands[1])
        self.assertEqual(commands[0], commands[2])
        self.assertEqual(disk.call_args_list, [mock.call(Path("/")), mock.call(Path("/project/data"))])

    def test_restart_during_collection_excludes_previous_run(self):
        with mock.patch.object(report, "diagnostic", side_effect=[
                service_text(), journal(event="failed"), service_text(OLD_INVOCATION)]), \
                mock.patch.object(report, "free_bytes", return_value=10), \
                mock.patch.object(report, "host_memory", return_value={}):
            current = report.collect_snapshot(Path("/project"))
        self.assertIsNone(current["event"])
        self.assertEqual(current["service"]["InvocationID"], OLD_INVOCATION)
        self.assertIn("changed during collection", " ".join(current["warnings"]))

    def test_no_invocation_does_not_read_historical_journal(self):
        with mock.patch.object(report, "diagnostic", return_value="ActiveState=inactive\n") as diagnostic, \
                mock.patch.object(report, "free_bytes", return_value=10), \
                mock.patch.object(report, "host_memory", return_value={}):
            current = report.collect_snapshot(Path("/project"))
        diagnostic.assert_called_once()
        self.assertIsNone(current["event"])

    def test_unavailable_diagnostics_do_not_echo_errors(self):
        with mock.patch.object(report, "diagnostic", side_effect=OSError(SECRET)), \
                mock.patch.object(report, "free_bytes", return_value=None), \
                mock.patch.object(report, "host_memory", return_value={}):
            current = report.collect_snapshot(Path("/project"))
        subject, body = report.render_report(current)
        self.assertIn("Unknown", subject)
        self.assertIn("unavailable", body)
        self.assertNotIn(SECRET, body)

    def test_diagnostic_capture_and_timeout_are_bounded(self):
        self.assertEqual(report.diagnostic([sys.executable, "-c", "print('ok')"]), "ok\n")
        with self.assertRaises(report.ReportError):
            report.diagnostic([sys.executable, "-c", "print('x' * 10000)"], limit=100)
        with self.assertRaises(report.ReportError):
            report.diagnostic([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.05)


class ReportTests(unittest.TestCase):
    def test_fresh_heartbeat_reports_exact_counters_not_an_estimate(self):
        subject, body = report.render_report(snapshot(remaining_rows=29001002, idle_seconds=43200))
        self.assertIn("Running", subject)
        self.assertIn("remaining_rows=29,001,002", body)
        self.assertIn("Recent stage observation", body)
        self.assertIn("43,200 seconds since a counter change or milestone", body)
        self.assertIn("not that SQL is advancing", body)
        self.assertIn("No percentage or ETA is inferred", body)
        self.assertIn("Current service memory: 0.10 GiB", body)
        self.assertNotIn("%", body)

    def test_completed_stage_does_not_claim_completed_recovery(self):
        subject, body = report.render_report(snapshot(event="completed", remaining_rows=0))
        self.assertIn("Running", subject)
        self.assertIn("does not establish completion of the whole recovery", body)

    def test_idle_failed_and_successful_exit_have_distinct_meanings(self):
        cases = [({"ActiveState": "inactive", "Result": "success"}, "Idle"),
                 ({"ActiveState": "failed", "Result": "exit-code"}, "Failed"),
                 ({"ActiveState": "inactive", "Result": "exit-code"}, "Failed"),
                 ({"ActiveState": "inactive", "Result": "success", "ExecMainCode": 1,
                   "ExecMainStatus": 0}, "Finished"),
                 ({"ActiveState": "deactivating", "Result": "success"}, "Stopping")]
        for properties, expected in cases:
            with self.subTest(expected=expected):
                current = snapshot()
                current["service"].update(properties)
                subject, body = report.render_report(current)
                self.assertIn(expected, subject)
                if expected == "Finished":
                    self.assertIn("recovery/data completion is unverified", body)

    def test_unknown_and_future_timestamps_are_not_fresh(self):
        current = snapshot(age=-30)
        self.assertIn("Freshness unknown", report.render_report(current)[1])
        current["event"] = None
        self.assertIn("No recognized progress event", report.render_report(current)[1])

    def test_comparison_is_only_same_invocation_stage_and_step(self):
        before = snapshot(age=100, remaining_rows=29001002, pending_transactions=1)
        previous = report.comparable_state(before, NOW - timedelta(seconds=90))
        current = snapshot(age=10, remaining_rows=28000000, pending_transactions=1)
        rendered = "\n".join(report.comparisons(current, previous))
        self.assertIn("remaining_rows: -1,001,002", rendered)
        self.assertIn("pending_transactions: +0", rendered)
        self.assertIn("not a completion estimate", rendered)
        for key, value in (("invocation", OLD_INVOCATION), ("stage", "clean-export"),
                           ("step", "exact_keys")):
            with self.subTest(key=key):
                altered = {**previous, key: value}
                rendered = "\n".join(report.comparisons(current, altered))
                self.assertIn("not compared", rendered)
                self.assertNotIn("-1,001,002", rendered)

    def test_same_old_observation_cannot_imply_progress(self):
        current = snapshot(age=900, remaining_rows=5)
        previous = report.comparable_state(current, NOW - timedelta(seconds=10))
        self.assertIn("No newer stage observation", report.comparisons(current, previous)[0])


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.state = self.root / "report" / "last-sent.json"
        self.current = snapshot(remaining_rows=100)
        self.environment = {"OJS_PROGRESS_EMAIL_FROM": "sender@gmail.com",
                            "OJS_PROGRESS_EMAIL_TO": "recipient@example.org"}

    def cli(self, *arguments, send_error=None):
        output, errors = io.StringIO(), io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, self.environment, clear=True))
            stack.enter_context(mock.patch.object(report, "collect_snapshot", return_value=self.current))
            stack.enter_context(mock.patch.object(report, "utc_now", return_value=NOW))
            password = stack.enter_context(mock.patch.object(report, "read_password", return_value=SECRET))
            send = stack.enter_context(mock.patch.object(report, "send_email", side_effect=send_error))
            stack.enter_context(redirect_stdout(output))
            stack.enter_context(redirect_stderr(errors))
            code = report.main(["--state-file", str(self.state), *arguments])
        return code, output.getvalue(), errors.getvalue(), password, send

    def test_dry_run_needs_no_settings_secret_lock_or_state_write(self):
        self.environment = {}
        code, output, errors, password, send = self.cli("--dry-run")
        self.assertEqual(code, 0)
        self.assertIn("remaining_rows=100", output)
        self.assertEqual(errors, "")
        password.assert_not_called()
        send.assert_not_called()
        self.assertFalse(self.state.parent.exists())
        self.assertNotIn(SECRET, output)

    def test_smtp_acceptance_precedes_atomic_state_commit(self):
        actions = []
        original = report.write_state

        def write(path, state):
            actions.append("write")
            original(path, state)

        with mock.patch.object(report, "write_state", side_effect=write):
            code, output, errors, password, send = self.cli(send_error=lambda *args: actions.append("accept"))
        self.assertEqual(code, 0)
        self.assertEqual(actions, ["accept", "write"])
        self.assertEqual(errors, "")
        self.assertIn("accepted by SMTP", output)
        self.assertEqual(report.read_state(self.state)["counters"]["remaining_rows"], 100)
        self.assertEqual({path.name for path in self.state.parent.iterdir()}, {"last-sent.json", "last-sent.json.lock"})
        self.assertEqual(self.state.stat().st_mode & 0o777, 0o600)
        self.assertNotIn(SECRET, self.state.read_text())

    def test_smtp_failure_preserves_last_successful_comparison_state(self):
        self.state.parent.mkdir()
        report.write_state(self.state, report.comparable_state(snapshot(age=100, remaining_rows=200), NOW - timedelta(hours=12)))
        before = self.state.read_bytes()
        code, output, errors, password, send = self.cli(send_error=smtplib.SMTPAuthenticationError(535, SECRET.encode()))
        self.assertEqual(code, 1)
        self.assertEqual(self.state.read_bytes(), before)
        self.assertIn("was not sent", errors)
        self.assertNotIn(SECRET, output + errors)

    def test_failure_to_save_after_acceptance_is_reported_honestly(self):
        with mock.patch.object(report, "write_state", side_effect=OSError(SECRET)):
            code, output, errors, password, send = self.cli()
        self.assertEqual(code, 1)
        self.assertIn("accepted by SMTP, but comparison state could not be saved", errors)
        self.assertNotIn(SECRET, output + errors)

    def test_lock_prevents_duplicate_send_and_credential_read(self):
        with report.report_lock(self.state) as acquired:
            self.assertTrue(acquired)
            code, output, errors, password, send = self.cli()
        self.assertEqual(code, 0)
        self.assertIn("another report is in progress", output)
        password.assert_not_called()
        send.assert_not_called()
        self.assertFalse(self.state.exists())

    def test_recent_send_skips_without_password_smtp_or_state_update(self):
        self.state.parent.mkdir()
        report.write_state(self.state, report.comparable_state(self.current, NOW - timedelta(minutes=1)))
        before = self.state.read_bytes()
        code, output, errors, password, send = self.cli()
        self.assertEqual(code, 0)
        self.assertIn("already sent; skipped", output)
        password.assert_not_called()
        send.assert_not_called()
        self.assertEqual(self.state.read_bytes(), before)

    def test_recent_unknown_or_idle_state_also_deduplicates(self):
        self.state.parent.mkdir()
        current = {**self.current, "service": {}, "event": None}
        report.write_state(self.state, report.comparable_state(current, NOW - timedelta(minutes=1)))
        code, output, errors, password, send = self.cli()
        self.assertEqual(code, 0)
        self.assertIn("already sent; skipped", output)
        password.assert_not_called()
        send.assert_not_called()

    def test_report_older_than_eleven_hours_is_sent(self):
        self.state.parent.mkdir()
        report.write_state(self.state, report.comparable_state(self.current, NOW - timedelta(hours=11, seconds=1)))
        code, output, errors, password, send = self.cli()
        self.assertEqual(code, 0)
        send.assert_called_once()
        self.assertEqual(report.read_state(self.state)["sent_at"], report.stamp(NOW))

    def test_force_sends_even_after_recent_report(self):
        self.state.parent.mkdir()
        report.write_state(self.state, report.comparable_state(self.current, NOW - timedelta(minutes=1)))
        code, output, errors, password, send = self.cli("--force")
        self.assertEqual(code, 0)
        send.assert_called_once()

    def test_tls_verified_before_login_and_send(self):
        smtp = mock.Mock()
        smtp.send_message.return_value = {}
        context = ssl.create_default_context()
        with mock.patch.object(report.smtplib, "SMTP", return_value=smtp) as constructor, \
                mock.patch.object(report.ssl, "create_default_context", return_value=context):
            report.send_email("sender@gmail.com", "recipient@example.org", SECRET, "Subject", "Body")
        constructor.assert_called_once_with("smtp.gmail.com", 587, timeout=20)
        self.assertEqual([call[0] for call in smtp.mock_calls],
                         ["ehlo", "starttls", "ehlo", "login", "send_message", "quit"])
        smtp.starttls.assert_called_once_with(context=context)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        sent_message = smtp.send_message.call_args.args[0]
        self.assertNotIn(SECRET, sent_message.as_string())
        smtp.set_debuglevel.assert_not_called()

    def test_no_login_if_starttls_fails(self):
        smtp = mock.Mock()
        smtp.starttls.side_effect = smtplib.SMTPException(SECRET)
        with mock.patch.object(report.smtplib, "SMTP", return_value=smtp):
            with self.assertRaises(smtplib.SMTPException):
                report.send_email("sender@gmail.com", "recipient@example.org", SECRET, "Subject", "Body")
        smtp.login.assert_not_called()
        smtp.send_message.assert_not_called()

    def test_recipient_refusal_is_not_success(self):
        smtp = mock.Mock()
        smtp.send_message.return_value = {"recipient@example.org": (550, SECRET)}
        with mock.patch.object(report.smtplib, "SMTP", return_value=smtp):
            with self.assertRaises(report.ReportError):
                report.send_email("sender@gmail.com", "recipient@example.org", SECRET, "Subject", "Body")

    def test_disconnect_after_data_acceptance_still_counts_as_success(self):
        smtp = mock.Mock()
        smtp.send_message.return_value = {}
        smtp.quit.side_effect = smtplib.SMTPServerDisconnected(SECRET)
        with mock.patch.object(report.smtplib, "SMTP", return_value=smtp):
            report.send_email("sender@gmail.com", "recipient@example.org", SECRET, "Subject", "Body")
        smtp.close.assert_called_once()

    def test_only_explicit_or_systemd_credential_path_is_read(self):
        credential = self.root / "gmail-app-password"
        credential.write_text("abcd efgh ijkl mnop\n")
        with mock.patch.dict(os.environ, {"CREDENTIALS_DIRECTORY": str(self.root)}, clear=True):
            self.assertEqual(report.read_password(None), "abcdefghijklmnop")
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(report.read_password(credential), "abcdefghijklmnop")
            with self.assertRaises(report.ReportError):
                report.read_password(None)

    def test_state_is_bounded_and_allowlisted(self):
        self.state.parent.mkdir()
        for content in ("{}", "not JSON", "x" * (report.MAX_STATE_BYTES + 1)):
            self.state.write_text(content)
            self.assertIsNone(report.read_state(self.state))
        state = report.comparable_state(self.current, NOW)
        state["secret"] = SECRET
        state["counters"]["sql"] = SECRET
        self.state.write_text(json.dumps(state))
        self.assertNotIn(SECRET, json.dumps(report.read_state(self.state)))

    def test_header_injection_config_is_rejected_without_echo(self):
        self.environment["OJS_PROGRESS_EMAIL_TO"] = "to@example.com\nBcc: " + SECRET
        code, output, errors, password, send = self.cli()
        self.assertEqual(code, 1)
        self.assertNotIn(SECRET, output + errors)
        password.assert_not_called()
        send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
