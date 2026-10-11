from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import progress_llm as llm


NOW = datetime(2026, 10, 11, 12, tzinfo=timezone.utc)
SECRET = "sk-test-" + "privateDoNotEcho" * 3
TEXT = "The rollback monitor is still reporting activity. A heartbeat alone does not establish forward progress."
EVIDENCE = {"service_status": "running", "stage": "recovery-rollback",
            "freshness": "recent", "counters": {"remaining_rows": 29001002}}


def response(text=TEXT, **changes):
    return json.dumps({"status": "completed", "error": None, "incomplete_details": None,
                       "output": [{"type": "message", "status": "completed", "role": "assistant",
                                   "content": [{"type": "output_text", "text": text, "annotations": []}]}],
                       **changes}).encode("utf-8")


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.key = self.root / "api-key"
        self.key.write_text(SECRET + "\n")
        self.budget = self.root / "state" / "llm-budget.json"

    def generate(self, *, now=NOW, evidence=None):
        with mock.patch.object(llm, "utc_now", return_value=now):
            return llm.generate_summary(EVIDENCE if evidence is None else evidence,
                                        credential_path=self.key, budget_path=self.budget)

    def assertSafe(self, exception):
        self.assertIn(exception.reason, llm.ERROR_MESSAGES)
        self.assertEqual(str(exception), llm.ERROR_MESSAGES[exception.reason])
        self.assertNotIn(SECRET, str(exception))

    def test_budget_is_reserved_before_network_and_saved_as_one_private_file(self):
        def network(payload, key):
            budget = json.loads(self.budget.read_text())
            self.assertEqual(budget["day_attempts"], 1)
            self.assertEqual(budget["month_attempts"], 1)
            self.assertEqual(key, SECRET)
            self.assertNotIn(SECRET, self.budget.read_text())
            self.assertEqual(self.budget.stat().st_mode & 0o777, 0o600)
            return response()

        with mock.patch.object(llm, "_bounded_request", side_effect=network) as network:
            self.assertEqual(self.generate(), TEXT)
        network.assert_called_once()
        self.assertEqual([path.name for path in self.budget.parent.iterdir()], ["llm-budget.json"])

    def test_missing_or_invalid_key_never_creates_budget_or_calls_network(self):
        for value in (None, "", "password", SECRET + "\nAnotherHeader: bad", "x" * 2048):
            with self.subTest(value_length=len(value) if value else 0):
                if value is None:
                    self.key.unlink(missing_ok=True)
                else:
                    self.key.write_text(value)
                with mock.patch.object(llm, "_bounded_request") as network:
                    with self.assertRaises(llm.SummaryUnavailable) as raised:
                        self.generate()
                self.assertSafe(raised.exception)
                self.assertEqual(raised.exception.reason, "invalid_key")
                network.assert_not_called()
                self.assertFalse(self.budget.exists())

    def test_eleventh_utc_day_attempt_is_rejected_without_reset_or_network(self):
        self.assertEqual(llm.DAILY_ATTEMPTS, 10)
        with mock.patch.object(llm, "_bounded_request", return_value=response()) as network:
            for offset in range(llm.DAILY_ATTEMPTS):
                self.generate(now=NOW + timedelta(seconds=offset))
            before = self.budget.read_bytes()
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                self.generate(now=NOW + timedelta(seconds=llm.DAILY_ATTEMPTS))
            self.assertEqual(raised.exception.reason, "daily_limit")
        self.assertEqual(network.call_count, llm.DAILY_ATTEMPTS)
        self.assertEqual(self.budget.read_bytes(), before)

    def test_failed_network_and_invalid_output_attempts_still_count(self):
        failures = [OSError(SECRET) if offset % 2 == 0 else response(status="incomplete")
                    for offset in range(llm.DAILY_ATTEMPTS)]
        with mock.patch.object(llm, "_bounded_request", side_effect=failures) as network:
            for offset in range(llm.DAILY_ATTEMPTS):
                with self.assertRaises(llm.SummaryUnavailable) as raised:
                    self.generate(now=NOW + timedelta(seconds=offset))
                self.assertSafe(raised.exception)
                self.assertEqual(raised.exception.reason,
                                 "network_or_api" if offset % 2 == 0 else "output_incomplete")
            before = self.budget.read_bytes()
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                self.generate(now=NOW + timedelta(seconds=llm.DAILY_ATTEMPTS))
            self.assertEqual(raised.exception.reason, "daily_limit")
        self.assertEqual(network.call_count, llm.DAILY_ATTEMPTS)
        self.assertEqual(self.budget.read_bytes(), before)
        budget = json.loads(self.budget.read_text())
        self.assertEqual((budget["day_attempts"], budget["month_attempts"]),
                         (llm.DAILY_ATTEMPTS, llm.DAILY_ATTEMPTS))

    def test_existing_two_attempt_budget_is_preserved_and_advances_to_three(self):
        previous = {"schema": 1, "day": "2026-10-11", "month": "2026-10",
                    "day_attempts": 2, "month_attempts": 2,
                    "last_attempt_at": NOW.isoformat(timespec="microseconds")}
        llm._save_budget(self.budget, previous)
        next_attempt = NOW + timedelta(seconds=1)

        def network(payload, key):
            reserved = json.loads(self.budget.read_text())
            self.assertEqual(reserved, {**previous, "day_attempts": 3, "month_attempts": 3,
                                       "last_attempt_at": next_attempt.isoformat(timespec="microseconds")})
            return response()

        with mock.patch.object(llm, "_bounded_request", side_effect=network) as request:
            self.assertEqual(self.generate(now=next_attempt), TEXT)
        request.assert_called_once()
        self.assertEqual(json.loads(self.budget.read_text())["month_attempts"], 3)

    def test_output_failure_keeps_specific_safe_reason_without_retry(self):
        with mock.patch.object(llm, "_bounded_request", return_value=response(status="incomplete")) as network:
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                self.generate()
        self.assertEqual(raised.exception.reason, "output_incomplete")
        self.assertSafe(raised.exception)
        network.assert_called_once()
        self.assertEqual(json.loads(self.budget.read_text())["day_attempts"], 1)

    def test_daily_rollover_keeps_monthly_count(self):
        with mock.patch.object(llm, "_bounded_request", return_value=response()):
            for offset in range(llm.DAILY_ATTEMPTS):
                self.generate(now=NOW + timedelta(seconds=offset))
            self.generate(now=NOW + timedelta(days=1))
        budget = json.loads(self.budget.read_text())
        self.assertEqual((budget["day_attempts"], budget["month_attempts"]), (1, llm.DAILY_ATTEMPTS + 1))

    def test_monthly_cap_rejects_new_day_and_new_month_can_start(self):
        self.assertEqual(llm.MONTHLY_ATTEMPTS, 65)
        llm._reserve_attempt(self.budget, NOW)
        budget = json.loads(self.budget.read_text())
        budget["month_attempts"] = llm.MONTHLY_ATTEMPTS
        self.budget.write_text(json.dumps(budget))
        with mock.patch.object(llm, "_bounded_request", return_value=response()) as network:
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                self.generate(now=NOW + timedelta(days=1))
            self.assertEqual(raised.exception.reason, "monthly_limit")
            network.assert_not_called()
            self.generate(now=datetime(2026, 11, 1, tzinfo=timezone.utc))
        budget = json.loads(self.budget.read_text())
        self.assertEqual((budget["day_attempts"], budget["month_attempts"]), (1, 1))
        self.assertEqual(budget["month"], "2026-11")

    def test_clock_rollback_cannot_restore_budget(self):
        llm._reserve_attempt(self.budget, NOW)
        before = self.budget.read_bytes()
        for offset in (timedelta(microseconds=1), timedelta(days=1), timedelta(days=40)):
            with self.subTest(offset=offset), mock.patch.object(llm, "_bounded_request") as network:
                with self.assertRaises(llm.SummaryUnavailable):
                    self.generate(now=NOW - offset)
                self.assertEqual(self.budget.read_bytes(), before)
                network.assert_not_called()

    def test_corrupt_existing_budget_is_never_reset_even_in_new_month(self):
        llm._reserve_attempt(self.budget, NOW)
        valid = json.loads(self.budget.read_text())
        cases = [b"", b"not JSON", b"{}", b"[]", b"x" * (llm.MAX_BUDGET_BYTES + 1)]
        for changes in ({"schema": True}, {"day_attempts": 0},
                        {"day_attempts": llm.DAILY_ATTEMPTS + 1, "month_attempts": llm.DAILY_ATTEMPTS + 1},
                        {"day_attempts": 2, "month_attempts": 1},
                        {"day_attempts": True}, {"month_attempts": llm.MONTHLY_ATTEMPTS + 1},
                        {"month_attempts": 0},
                        {"day": "2026-13-99"}, {"month": "2026-09"},
                        {"last_attempt_at": "2026-10-11T12:00:00"},
                        {"last_attempt_at": "2026-10-10T12:00:00+00:00"}, {"secret": SECRET}):
            cases.append(json.dumps({**valid, **changes}).encode())
        for encoded in cases:
            with self.subTest(encoded_length=len(encoded)):
                self.budget.write_bytes(encoded)
                with mock.patch.object(llm, "_bounded_request") as network:
                    with self.assertRaises(llm.SummaryUnavailable):
                        self.generate(now=datetime(2026, 11, 1, tzinfo=timezone.utc))
                network.assert_not_called()
                self.assertEqual(self.budget.read_bytes(), encoded)

    def test_nonprivate_budget_and_symlinks_fail_closed(self):
        llm._reserve_attempt(self.budget, NOW)
        self.budget.chmod(0o644)
        with mock.patch.object(llm, "_bounded_request") as network:
            with self.assertRaises(llm.SummaryUnavailable):
                self.generate()
        network.assert_not_called()
        self.budget.unlink()
        self.budget.symlink_to(self.root / "absent")
        with mock.patch.object(llm, "_bounded_request") as network:
            with self.assertRaises(llm.SummaryUnavailable):
                self.generate()
        network.assert_not_called()
        self.assertTrue(self.budget.is_symlink())

    def test_failed_durable_reservation_prevents_network(self):
        with mock.patch.object(llm.os, "fsync", side_effect=OSError(SECRET)), \
                mock.patch.object(llm, "_bounded_request") as network:
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                self.generate()
        self.assertSafe(raised.exception)
        network.assert_not_called()
        self.assertFalse(self.budget.exists())
        self.assertEqual(list(self.budget.parent.iterdir()), [])

    def test_fifo_budget_and_credential_fail_immediately_without_network(self):
        self.budget.parent.mkdir()
        os.mkfifo(self.budget, 0o600)
        started = time.monotonic()
        with mock.patch.object(llm, "_bounded_request") as network:
            with self.assertRaises(llm.SummaryUnavailable):
                self.generate()
        network.assert_not_called()
        self.assertLess(time.monotonic() - started, 1)
        self.budget.unlink()
        self.key.unlink()
        os.mkfifo(self.key, 0o600)
        with mock.patch.object(llm, "_bounded_request") as network:
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                self.generate()
        network.assert_not_called()
        self.assertEqual(raised.exception.reason, "invalid_key")
        self.assertFalse(self.budget.exists())

    def test_oversized_or_invalid_input_does_not_spend_budget(self):
        for evidence in ({"value": "a" * llm.MAX_INPUT_BYTES}, {"count": float("nan")},
                         {"value": object()}):
            with mock.patch.object(llm, "_bounded_request") as network:
                with self.assertRaises(llm.SummaryUnavailable):
                    self.generate(evidence=evidence)
                network.assert_not_called()
                self.assertFalse(self.budget.exists())


class RequestTests(unittest.TestCase):
    def test_fixed_model_payload_and_prompt_caps(self):
        payload = llm._payload(EVIDENCE)
        decoded = json.loads(payload)
        self.assertEqual(decoded["model"], "gpt-6-luna")
        self.assertEqual(decoded["service_tier"], "default")
        self.assertEqual(decoded["reasoning"], {"effort": "none"})
        self.assertEqual(decoded["max_output_tokens"], 512)
        self.assertEqual(decoded["tools"], [])
        self.assertFalse(decoded["store"])
        self.assertLessEqual(len(decoded["instructions"].encode()) + len(decoded["input"].encode()), 8192)
        self.assertEqual(json.loads(decoded["input"]), EVIDENCE)
        self.assertNotIn(SECRET, payload.decode())

    def https_fixture(self, *, code=200, body=None, headers=None):
        connection = mock.Mock()
        result = connection.getresponse.return_value
        result.status = code
        result.read.return_value = response() if body is None else body
        header_values = {"Content-Type": "application/json; charset=utf-8", **(headers or {})}
        result.getheader.side_effect = lambda key, default=None: header_values.get(key, default)
        return connection, result

    def test_https_fixed_origin_verified_tls_and_no_proxy_or_redirect_handler(self):
        connection, result = self.https_fixture()
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": "https://invalid.example", "OPENAI_BASE_URL": "https://invalid.example"}), \
                mock.patch.object(llm.http.client, "HTTPSConnection", return_value=connection) as factory:
            self.assertEqual(llm._https_post(b"payload", SECRET), response())
        self.assertEqual(factory.call_args.args, ("api.openai.com", 443))
        self.assertEqual(factory.call_args.kwargs["timeout"], 30)
        context = factory.call_args.kwargs["context"]
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)
        call = connection.request.call_args
        self.assertEqual(call.args, ("POST", "/v1/responses"))
        self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer " + SECRET)
        result.read.assert_called_once_with(llm.MAX_RESPONSE_BYTES + 1)
        connection.close.assert_called_once()

    def test_http_failures_and_redirects_do_not_retry_or_read_body(self):
        for code in (301, 302, 307, 401, 429, 500, 503):
            with self.subTest(code=code):
                connection, result = self.https_fixture(code=code)
                with mock.patch.object(llm.http.client, "HTTPSConnection", return_value=connection):
                    with self.assertRaises(llm.SummaryUnavailable) as raised:
                        llm._https_post(b"payload", SECRET)
                expected = {401: "authentication", 429: "quota_or_rate_limit"}.get(code, "network_or_api")
                self.assertEqual(raised.exception.reason, expected)
                connection.request.assert_called_once()
                result.read.assert_not_called()
                connection.close.assert_called_once()

    def test_oversized_and_non_json_responses_are_rejected(self):
        cases = [({"Content-Length": str(llm.MAX_RESPONSE_BYTES + 1)}, b"{}"),
                 ({"Content-Type": "text/html"}, b"private content"),
                 ({}, b"x" * (llm.MAX_RESPONSE_BYTES + 1))]
        for headers, body in cases:
            with self.subTest(headers=headers):
                connection, result = self.https_fixture(headers=headers, body=body)
                with mock.patch.object(llm.http.client, "HTTPSConnection", return_value=connection):
                    with self.assertRaises(llm.SummaryUnavailable):
                        llm._https_post(b"payload", SECRET)
                connection.request.assert_called_once()

    def test_worker_returns_response_without_network_in_parent(self):
        with mock.patch.object(llm, "_https_post", return_value=response()):
            self.assertEqual(llm._bounded_request(b"payload", SECRET), response())

    def test_worker_network_error_is_generic_and_has_no_traceback(self):
        with mock.patch.object(llm, "_https_post", side_effect=RuntimeError(SECRET)):
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                llm._bounded_request(b"payload", SECRET)
        self.assertNotIn(SECRET, str(raised.exception))
        self.assertEqual(raised.exception.reason, "network_or_api")

    def test_worker_transmits_only_closed_diagnostic_code(self):
        with mock.patch.object(llm, "_https_post", side_effect=llm.SummaryUnavailable("authentication")):
            with self.assertRaises(llm.SummaryUnavailable) as raised:
                llm._bounded_request(b"payload", SECRET)
        self.assertEqual(raised.exception.reason, "authentication")
        self.assertNotIn(SECRET, str(raised.exception))

    def test_exception_constructor_cannot_echo_unapproved_details(self):
        for unsafe in (SECRET, [SECRET], {"error": SECRET}):
            error = llm.SummaryUnavailable(unsafe)
            self.assertEqual(error.reason, "unavailable")
            self.assertNotIn(SECRET, str(error))

    def test_total_request_deadline_includes_slow_network_operations(self):
        def delayed(*args):
            time.sleep(5)
            return response()

        started = time.monotonic()
        with mock.patch.object(llm, "_https_post", side_effect=delayed), \
                mock.patch.object(llm, "REQUEST_SECONDS", 0.05):
            with self.assertRaises(llm.SummaryUnavailable):
                llm._bounded_request(b"payload", SECRET)
        self.assertLess(time.monotonic() - started, 2)


class OutputTests(unittest.TestCase):
    def test_all_content_failures_have_closed_diagnostics_without_response_text(self):
        text = "Recovery is complete in two hours. Restart at https://private.example.org/" + SECRET + "/123."
        with self.assertRaises(llm.SummaryUnavailable) as raised:
            llm._summary(response(text))
        error = raised.exception
        self.assertEqual(error.reason, "output_content")
        self.assertTrue({"status_complete", "word_two", "word_hours", "word_restart", "link", "numeric_characters"}
                        .issubset(error.checks))
        self.assertTrue(set(error.checks).issubset(llm._CONTENT_CHECKS))
        for private in (SECRET, "private.example.org", "123", text):
            self.assertNotIn(private, str(error))

    def test_diagnostic_constructor_ignores_arbitrary_checks(self):
        for checks in (SECRET, {SECRET: True}, [SECRET, "word_two", {}, "word_two"]):
            error = llm.SummaryUnavailable("output_content", checks=checks)
            self.assertNotIn(SECRET, str(error))
            self.assertTrue(set(error.checks).issubset(llm._CONTENT_CHECKS))
        self.assertEqual(llm.SummaryUnavailable("output_content", checks=["word_two", "word_two"]).checks,
                         ("word_two",))
        self.assertFalse(llm.SummaryUnavailable("authentication", checks=["word_two"]).checks)

    def test_short_plain_completed_summary_is_accepted(self):
        self.assertEqual(llm._summary(response()), TEXT)

    def test_benign_whitespace_is_normalized_not_rejected(self):
        for text in (" \n" + TEXT + "\r\n", TEXT.replace(". A", ".\n\nA"),
                     TEXT.replace(" ", "\t"), TEXT.replace(" ", "\u00a0")):
            with self.subTest(text=text):
                self.assertEqual(llm._summary(response(text)), TEXT)

    def test_reasoning_metadata_is_ignored_but_final_text_is_required(self):
        final = json.loads(response())["output"][0]
        metadata = {"type": "reasoning", "id": "rs_test", "summary": [],
                    "encrypted_content": SECRET}
        self.assertEqual(llm._summary(response(output=[metadata, final])), TEXT)
        self.assertEqual(llm._summary(response(output=[final, metadata])), TEXT)
        with self.assertRaises(llm.SummaryUnavailable):
            llm._summary(response(output=[metadata]))

    def test_explicit_completion_uncertainty_is_not_a_positive_claim(self):
        for text in (
            "Rollback is still in progress. This does not mean recovery is complete.",
            "The service is running. There is no evidence that recovery has finished.",
            "The monitor is active. We cannot yet confirm that the system is healthy.",
            "Rollback remains active. Recovery is not yet complete.",
            "Rollback remains active. It is unclear whether recovery has succeeded.",
            "The monitor is active. It doesn’t show that recovery has completed.",
        ):
            with self.subTest(text=text):
                self.assertEqual(llm._summary(response(text)), text)

    def test_drop_in_work_is_prose_not_a_database_command(self):
        text = "The comparable counters show a drop in remaining work. Overall recovery remains unverified."
        self.assertEqual(llm._summary(response(text)), text)
        for command in ("Drop the table", "DROP TABLE records", "Drop this database", "Drop all data"):
            with self.subTest(command=command), self.assertRaises(llm.SummaryUnavailable):
                llm._summary(response(command + ". The monitor is active."))

    def test_negation_cannot_hide_another_positive_claim_or_unsafe_content(self):
        for text in (
            "There is no evidence of an error. Recovery is complete.",
            "Recovery is not only complete. The system is healthy.",
            "This does not mean recovery is complete, but the system is healthy. Work is advancing.",
            "We cannot confirm recovery is complete; the system is healthy. Work is advancing.",
            "Recovery is not complete and the system is healthy. Work is advancing.",
            "There is no evidence of a problem, and recovery is complete. The monitor is active.",
            "We cannot confirm the active stage, therefore recovery is complete. The monitor is active.",
            "We cannot confirm the active stage—recovery is complete. The monitor is active.",
            "We cannot confirm recovery is complete. Restart the service.",
            "We cannot confirm recovery is complete. There are 100 rows remaining.",
        ):
            with self.subTest(text=text), self.assertRaises(llm.SummaryUnavailable):
                llm._summary(response(text))

    def test_validation_reasons_are_safe_and_specific(self):
        cases = [(b"not JSON " + SECRET.encode(), "output_format"),
                 (response(status="incomplete"), "output_incomplete"),
                 (response(output=[{"type": "function_call", "arguments": SECRET}]), "output_format"),
                 (response("Only a sentence."), "output_style"),
                 (response("There are 100 rows. Recovery is active."), "output_content")]
        for encoded, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaises(llm.SummaryUnavailable) as raised:
                    llm._summary(encoded)
                self.assertEqual(raised.exception.reason, reason)
                self.assertNotIn(SECRET, str(raised.exception))

    def test_numbers_urls_controls_commands_and_completion_claims_are_rejected(self):
        texts = ["There are 100 rows left. Progress looks promising.",
                 "There are １００ rows left. Progress looks promising.",
                 "This will take two hours. The monitor is alive.",
                 "See https://example.com. The monitor is alive.",
                 "See example.com for help. The monitor is alive.",
                 "The monitor is alive.\x00The stage is pending.",
                 "The monitor is alive.\u200bThe stage is pending.",
                 "Restart the service. The monitor is alive.",
                 "The recovery is complete. The database is healthy.",
                 "The service is running normally. All work has finished.",
                 "**The monitor is alive.** See [details](example.org)."]
        for text in texts:
            with self.subTest(text=text), self.assertRaises(llm.SummaryUnavailable):
                llm._summary(response(text))

    def test_length_sentence_and_word_bounds(self):
        for text in ("Only a sentence.", "First. Second. Third. Fourth. Fifth.",
                     "First sentence. " + "word " * 121 + "End.",
                     "A" * 1200 + ". End.", "First sentence. Missing final punctuation"):
            with self.subTest(length=len(text)), self.assertRaises(llm.SummaryUnavailable):
                llm._summary(response(text))

    def test_refusals_incomplete_errors_and_non_text_outputs_are_rejected(self):
        cases = [response(status="incomplete"), response(status="failed"),
                 response(error={"message": SECRET}), response(incomplete_details={"reason": "max_output_tokens"}),
                 response(output=[]), response(output=[{"type": "reasoning", "summary": []}]),
                 response(output=[{"type": "function_call", "name": "private", "arguments": SECRET}])]
        for changes in ({"type": "refusal", "refusal": SECRET},
                        {"type": "output_text", "text": TEXT, "annotations": [{"url": "https://private.example"}]}):
            cases.append(response(output=[{"type": "message", "status": "completed", "role": "assistant", "content": [changes]}]))
        for encoded in cases:
            with self.subTest(encoded_length=len(encoded)), self.assertRaises(llm.SummaryUnavailable):
                llm._summary(encoded)


if __name__ == "__main__":
    unittest.main()
