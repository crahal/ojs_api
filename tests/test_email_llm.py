from __future__ import annotations

from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import timedelta
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from test_email_progress import NOW, INVOCATION, snapshot
import email_progress as report


PRIVATE = "must-not-leave-the-host"
SUMMARY = "Rollback is still in progress. The comparable counters show that work has advanced."


class EvidenceTests(unittest.TestCase):
    def test_outbound_evidence_excludes_identifiers_paths_and_arbitrary_text(self):
        current = snapshot(remaining_rows=100)
        current["service"]["MainPID"] = 987654321
        current["service"]["Environment"] = PRIVATE
        current["event"]["sql"] = PRIVATE
        current["event"]["step"] = PRIVATE
        current["warnings"] = [PRIVATE]
        current["email"] = PRIVATE
        previous = report.comparable_state(snapshot(age=120, remaining_rows=140), NOW)
        previous["password"] = PRIVATE
        evidence = report.llm_evidence(current, previous)
        encoded = json.dumps(evidence)
        for secret in (PRIVATE, INVOCATION, "987654321", "/srv/", "MainPID", "InvocationID"):
            self.assertNotIn(secret, encoded)
        self.assertTrue(evidence["collection_incomplete"])
        self.assertIsNone(evidence["stage"]["step"])
        self.assertFalse(evidence["overall_completion_verified"])
        self.assertFalse(evidence["api_availability_verified"])

    def test_comparison_uses_matching_newer_observations_not_email_timestamps(self):
        previous = report.comparable_state(snapshot(age=120, remaining_rows=140), NOW)
        evidence = report.llm_evidence(snapshot(age=10, remaining_rows=100), previous)
        self.assertEqual(evidence["comparison"]["counter_changes"]["remaining_rows"], -40)
        self.assertEqual(evidence["comparison"]["observation_interval_seconds"], 110)

    def test_counters_are_never_compared_across_run_stage_or_older_observations(self):
        current = snapshot(age=10, remaining_rows=100)
        cases = [("invocation", "b" * 32), ("stage", "sql-import"),
                 ("step", "feeding-mysql"), ("logged_at", current["event"]["logged_at"])]
        for key, value in cases:
            previous = report.comparable_state(snapshot(age=120, remaining_rows=140), NOW)
            previous[key] = value
            self.assertNotIn("counter_changes", report.llm_evidence(current, previous)["comparison"])

    def test_no_unverified_completion_or_freshness_claim(self):
        for age, freshness in ((600, "stale"), (-20, "unknown"), (10, "recent")):
            evidence = report.llm_evidence(snapshot(age=age, event="completed"), None)
            self.assertEqual(evidence["stage"]["freshness"], freshness)
            self.assertFalse(evidence["overall_completion_verified"])
        current = snapshot()
        current["event"] = None
        current["service"] = {}
        evidence = report.llm_evidence(current, None)
        self.assertEqual(evidence["service_status"], "Unknown")
        self.assertIsNone(evidence["stage"])

    def test_non_numeric_resources_and_unrecognized_service_labels_are_omitted(self):
        current = snapshot()
        current["data_free"] = PRIVATE
        current["memory"]["MemAvailable"] = float("nan")
        current["service"]["ActiveState"] = PRIVATE
        evidence = report.llm_evidence(current, None)
        self.assertNotIn(PRIVATE, json.dumps(evidence))
        self.assertIsNone(evidence["resources_bytes"]["data_free"])
        self.assertIsNone(evidence["resources_bytes"]["host_memory_available"])


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / "state" / "last-sent.json"
        self.current = snapshot(remaining_rows=100)
        self.environment = {"OJS_PROGRESS_EMAIL_FROM": "sender@gmail.com",
                            "OJS_PROGRESS_EMAIL_TO": "recipient@example.org",
                            "OJS_PROGRESS_LLM_ENABLED": "1",
                            "CREDENTIALS_DIRECTORY": str(self.root / "credentials")}

    def run_report(self, *arguments, error=None, password_error=None):
        out, err = io.StringIO(), io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, self.environment, clear=True))
            stack.enter_context(mock.patch.object(report, "collect_snapshot", return_value=self.current))
            stack.enter_context(mock.patch.object(report, "utc_now", return_value=NOW))
            password = stack.enter_context(mock.patch.object(report, "read_password",
                                           return_value=PRIVATE, side_effect=password_error))
            send = stack.enter_context(mock.patch.object(report, "send_email"))
            generate = stack.enter_context(mock.patch.object(report.progress_llm, "generate_summary",
                                           return_value=SUMMARY, side_effect=error))
            stack.enter_context(redirect_stdout(out))
            stack.enter_context(redirect_stderr(err))
            result = report.main(["--state-file", str(self.state), *arguments])
        return result, out.getvalue() + err.getvalue(), password, send, generate

    def test_generated_assessment_precedes_unchanged_exact_facts(self):
        result, output, password, send, generate = self.run_report()
        self.assertEqual(result, 0)
        generate.assert_called_once()
        self.assertEqual(generate.call_args.kwargs["budget_path"], self.state.parent / "llm-budget.json")
        self.assertEqual(generate.call_args.kwargs["credential_path"], self.root / "credentials" / "openai-api-key")
        body = send.call_args.args[-1]
        self.assertTrue(body.startswith("AI assessment"))
        self.assertIn(SUMMARY, body)
        self.assertTrue(body.endswith(report.render_report(self.current)[1]))
        self.assertNotIn(PRIVATE, json.dumps(generate.call_args.args[0]))
        self.assertNotIn(SUMMARY, output)  # Scheduled runs don't dump generated text into logs.
        self.assertNotIn(PRIVATE, output)

    def test_ai_errors_fall_back_without_losing_email_or_leaking_errors(self):
        for error in (report.progress_llm.SummaryUnavailable(), RuntimeError(PRIVATE), TimeoutError(PRIVATE)):
            with self.subTest(error=type(error).__name__):
                result, output, password, send, generate = self.run_report("--force", error=error)
                self.assertEqual(result, 0)
                self.assertTrue(send.call_args.args[-1].startswith("AI assessment unavailable; factual report follows."))
                self.assertNotIn(PRIVATE, output)
                self.assertIn("accepted by SMTP", output)

    def test_dry_run_is_always_offline_and_does_not_create_budget_or_lock(self):
        result, output, password, send, generate = self.run_report("--dry-run")
        self.assertEqual(result, 0)
        generate.assert_not_called()
        password.assert_not_called()
        send.assert_not_called()
        self.assertFalse(self.state.parent.exists())

    def test_disabled_default_never_reads_openai_credentials_or_calls_api(self):
        self.environment.pop("OJS_PROGRESS_LLM_ENABLED")
        result, output, password, send, generate = self.run_report()
        self.assertEqual(result, 0)
        generate.assert_not_called()
        self.assertEqual(send.call_args.args[-1], report.render_report(self.current)[1])

    def test_email_deduplication_precedes_ai_call(self):
        self.state.parent.mkdir()
        report.write_state(self.state, report.comparable_state(self.current, NOW - timedelta(minutes=1)))
        result, output, password, send, generate = self.run_report()
        self.assertEqual(result, 0)
        generate.assert_not_called()
        send.assert_not_called()

    def test_bad_gmail_credential_file_does_not_waste_an_ai_attempt(self):
        result, output, password, send, generate = self.run_report(password_error=OSError(PRIVATE))
        self.assertEqual(result, 1)
        generate.assert_not_called()
        self.assertNotIn(PRIVATE, output)

    def test_paid_preview_does_not_read_gmail_send_or_advance_delivery_state(self):
        self.state.parent.mkdir()
        report.write_state(self.state, report.comparable_state(self.current, NOW))
        before = self.state.read_bytes()
        result, output, password, send, generate = self.run_report("--preview-llm")
        self.assertEqual(result, 0)
        self.assertIn(SUMMARY, output)
        generate.assert_called_once()
        password.assert_not_called()
        send.assert_not_called()
        self.assertEqual(before, self.state.read_bytes())

    def test_preview_error_has_nonzero_status_no_email_and_no_raw_details(self):
        result, output, password, send, generate = self.run_report("--preview-llm", error=RuntimeError(PRIVATE))
        self.assertEqual(result, 1)
        self.assertNotIn(PRIVATE, output)
        self.assertFalse(self.state.exists())
        send.assert_not_called()

    def test_known_errors_show_only_safe_diagnostics(self):
        for reason in ("authentication", "quota_or_rate_limit", "daily_limit", "monthly_limit", PRIVATE):
            for arguments in (("--preview-llm",), ("--force",)):
                with self.subTest(reason=reason, arguments=arguments):
                    error = report.progress_llm.SummaryUnavailable(reason)
                    result, output, password, send, generate = self.run_report(*arguments, error=error)
                    self.assertEqual(result, 1 if arguments == ("--preview-llm",) else 0)
                    self.assertIn(str(error), output)
                    self.assertNotIn(PRIVATE, output)

    def test_preview_must_acquire_same_lock_as_scheduled_report(self):
        with report.report_lock(self.state):
            result, output, password, send, generate = self.run_report("--preview-llm")
        self.assertEqual(result, 1)
        generate.assert_not_called()
        send.assert_not_called()

    def test_force_does_not_bypass_ai_budget(self):
        result, output, password, send, generate = self.run_report("--force", error=report.progress_llm.SummaryUnavailable())
        self.assertEqual(result, 0)
        self.assertNotIn("force", generate.call_args.kwargs)
        self.assertIn("AI assessment unavailable", send.call_args.args[-1])


if __name__ == "__main__":
    unittest.main()
