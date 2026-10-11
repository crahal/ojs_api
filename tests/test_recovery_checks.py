from __future__ import annotations

import copy
import re
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import recovery_checks as recovery
from run_pipeline import PIPELINE_VERSION, PipelineError


class AuditServer:
    def __init__(self):
        names = recovery._PREFIX_TABLES | set(recovery._CLEAN_TABLES) | recovery._RAW_TABLES
        self.tables = {name: ["BASE TABLE", "InnoDB"] for name in names | {"ojs_stage_keys"}}
        self.answers = {
            "recovery-other-clients": "0",
            "recovery-transactions": "0\t0\t0\t0",
            "recovery-clean-empty": "0\t0\t0\t0\t0",
            "recovery-routine-inventory": "0",
            "recovery-context-coverage": "4\t4\t0",
            "recovery-raw-count": "64\t1\t64",
            "recovery-index-count": "64\t64\t1\t64",
            "recovery-metadata-count": "64\t1\t64",
        }
        self.calls = []

    def execute(self, sql, *, database, password, label):
        self.calls.append((label, sql))
        if label in self.answers:
            answer = self.answers[label]
            if isinstance(answer, list):
                return answer.pop(0)
            return answer
        if label == "recovery-table-inventory":
            return "\n".join("\t".join([name, *fields]) for name, fields in sorted(self.tables.items()))
        if label in ("recovery-metadata-range", "recovery-source-coverage"):
            lower, upper = map(int, re.search(r"BETWEEN (\d+) AND (\d+)", sql).groups())
            count = upper - lower + 1
            return f"{count}\t0\t0\t0\t0\t0\t0" if label == "recovery-metadata-range" else f"{count}\t0"
        raise AssertionError(f"unexpected query: {label}")


class RecoveryValidationTests(unittest.TestCase):
    def setUp(self):
        self.server = AuditServer()
        self.checkpoint = {
            "pipeline_version": PIPELINE_VERSION, "snapshot_version": "2026-01-01",
            "phase": "finalizing", "source_sha256": "a" * 64,
            "build_sql_sha256": "b" * 64, "source_size_bytes": 123,
            "source_filename": "pkpbeacon-2026-01-01.sql.gz",
        }
        self.expected = copy.deepcopy(self.checkpoint)
        patcher = mock.patch.object(recovery, "Progress")
        self.progress = patcher.start()
        self.addCleanup(patcher.stop)

    def validate(self, **overrides):
        arguments = {"database": "fixture", "password": None,
                     "snapshot_version": "2026-01-01", "source_sha256": "a" * 64,
                     "build_sql_sha256": "b" * 64, "checkpoint": self.checkpoint,
                     "expected_checkpoint": self.expected}
        arguments.update(overrides)
        return recovery.validate_bootstrap_exact_keys(self.server, **arguments)

    def test_proves_all_metadata_ranges_and_complete_raw_coverage_without_writes(self):
        original = copy.deepcopy(self.checkpoint)
        result = self.validate()
        self.assertEqual(result["raw_source_rows"], 64)
        self.assertEqual(result["metadata_rows"], 64)
        self.assertEqual(result["verified_range_count"], 32)
        self.assertEqual(sum(item["metadata_rows"] for item in result["metadata_ranges"]), 64)
        self.assertEqual(result["metadata_ranges"][0]["min_id"], 1)
        self.assertEqual(result["metadata_ranges"][-1]["max_id"], 64)
        self.assertEqual(self.checkpoint, original)
        self.assertTrue(all(sql.lstrip().startswith("SELECT") for _, sql in self.server.calls))
        self.assertEqual(sum(label == "recovery-source-coverage" for label, _ in self.server.calls), 32)
        self.assertTrue(result["has_exact_key_table"])

    def test_accepts_no_exact_key_table_without_assuming_finalization_started(self):
        del self.server.tables["ojs_stage_keys"]
        self.assertFalse(self.validate()["has_exact_key_table"])

    def test_never_queries_or_reuses_existing_exact_key_rows(self):
        self.validate()
        self.assertFalse(any("FROM ojs_stage_keys" in sql for _, sql in self.server.calls))

    def test_checkpoint_differences_fail_before_database_access(self):
        for name, value in (("phase", "metadata_ready"), ("source_sha256", "c" * 64),
                            ("source_size_bytes", 124), ("source_filename", "private.sql"),
                            ("pipeline_version", "0"), ("build_sql_sha256", "d" * 64)):
            with self.subTest(name=name):
                changed = {**self.checkpoint, name: value}
                with self.assertRaisesRegex(PipelineError, "checkpoint"):
                    self.validate(checkpoint=changed)
                self.assertEqual(self.server.calls, [])

    def test_invalid_checksum_cannot_be_accepted(self):
        with self.assertRaisesRegex(PipelineError, "checksum"):
            self.validate(source_sha256="malformed")

    def test_cli_errors_use_the_running_main_pipeline_exception(self):
        class MainPipelineError(RuntimeError):
            pass
        pipeline_main = types.SimpleNamespace(
            __file__=str(Path(recovery.__file__).with_name("run_pipeline.py")),
            PipelineError=MainPipelineError,
        )
        with mock.patch.dict(sys.modules, {"__main__": pipeline_main}):
            with self.assertRaisesRegex(MainPipelineError, "refused"):
                recovery._fail("fixture rejection")

    def test_expected_checkpoint_cannot_override_recovery_boundary(self):
        altered = {**self.expected, "phase": "metadata_ready"}
        with self.assertRaisesRegex(PipelineError, "contradicts"):
            self.validate(expected_checkpoint=altered)

    def test_expected_checkpoint_requires_filename_size_and_every_original_field(self):
        for name in self.expected:
            with self.subTest(name=name):
                changed = {key: value for key, value in self.expected.items() if key != name}
                with self.assertRaisesRegex(PipelineError, "provenance fields"):
                    self.validate(expected_checkpoint=changed)

    def test_rejects_every_later_phase_even_if_tables_empty(self):
        for name in ("ojs_stage_effective_keys", "ojs_stage_assignments",
                     "ojs_pipeline_raw_stats", "ojs_pipeline_metadata", "ojs_unknown"):
            with self.subTest(name=name):
                self.server.tables[name] = ["BASE TABLE", "InnoDB"]
                with self.assertRaisesRegex(PipelineError, "later finalization"):
                    self.validate()
                del self.server.tables[name]

    def test_rejects_missing_raw_or_staged_tables(self):
        del self.server.tables["records"]
        with self.assertRaisesRegex(PipelineError, "missing"):
            self.validate()

    def test_rejects_missing_raw_versions_before_metadata_audit(self):
        del self.server.tables["versions"]
        with self.assertRaisesRegex(PipelineError, "required raw.*missing"):
            self.validate()
        self.assertFalse(any(label == "recovery-metadata-range" for label, _ in self.server.calls))

    def test_rejects_view_or_nontransactional_stage(self):
        for fields in (["VIEW", ""], ["BASE TABLE", "MyISAM"]):
            with self.subTest(fields=fields):
                self.server.tables["ojs_stage_sources"] = fields
                with self.assertRaisesRegex(PipelineError, "transactional engine"):
                    self.validate()

    def test_rejects_any_committed_catalogue_effect(self):
        for index in range(5):
            with self.subTest(index=index):
                counts = ["0"] * 5
                counts[index] = "1"
                self.server.answers["recovery-clean-empty"] = "\t".join(counts)
                with self.assertRaisesRegex(PipelineError, "untouched bootstrap"):
                    self.validate()

    def test_rejects_incomplete_pipeline_procedure(self):
        self.server.answers["recovery-routine-inventory"] = "1"
        with self.assertRaisesRegex(PipelineError, "procedure"):
            self.validate()

    def test_rejects_partial_metadata_and_nonbootstrap_parse_flags(self):
        self.server.answers["recovery-metadata-count"] = "63\t1\t64"
        with self.assertRaisesRegex(PipelineError, "incomplete"):
            self.validate()
        self.server.answers["recovery-metadata-count"] = "64\t1\t64"
        self.server.answers["recovery-index-count"] = "64\t63\t1\t64"
        with self.assertRaisesRegex(PipelineError, "parse flags"):
            self.validate()

    def test_context_coverage_cannot_hide_missing_or_altered_journals(self):
        for result in ("4\t3\t1", "4\t5\t0", "4\t4\t1", "0\t0\t0"):
            with self.subTest(result=result):
                self.server.answers["recovery-context-coverage"] = result
                with self.assertRaisesRegex(PipelineError, "normalized contexts"):
                    self.validate()

    def test_equal_counts_do_not_hide_missing_rows_or_hash_mismatches(self):
        for index in range(1, 7):
            with self.subTest(index=index):
                counts = ["2"] + ["0"] * 6
                counts[index] = "1"
                self.server.answers["recovery-metadata-range"] = "\t".join(counts)
                with self.assertRaisesRegex(PipelineError, "membership, identity or hash"):
                    self.validate()

    def test_source_coverage_includes_raw_rows_outside_index_bounds(self):
        self.server.answers["recovery-raw-count"] = "66\t0\t65"
        self.server.answers["recovery-source-coverage"] = "2\t1"
        with self.assertRaisesRegex(PipelineError, "omits qualifying"):
            self.validate()
        sql = next(sql for label, sql in self.server.calls if label == "recovery-source-coverage")
        self.assertIn("BETWEEN 0 AND 2", sql)

    def test_coverage_count_disagreement_is_rejected(self):
        self.server.answers["recovery-source-coverage"] = "1\t0"
        with self.assertRaisesRegex(PipelineError, "differs from qualifying"):
            self.validate()

    def test_pending_transactions_are_not_treated_as_completed_recovery(self):
        self.server.answers["recovery-transactions"] = "1\t1\t1\t500"
        with self.assertRaisesRegex(PipelineError, "still active"):
            self.validate()

    def test_any_other_client_is_rejected(self):
        self.server.answers["recovery-other-clients"] = "1"
        with self.assertRaisesRegex(PipelineError, "another client"):
            self.validate()

    def test_isolation_is_checked_again_after_long_audit(self):
        self.server.answers["recovery-other-clients"] = ["0", "1"]
        with self.assertRaisesRegex(PipelineError, "another client"):
            self.validate()

    def test_invalid_numeric_query_results_fail_closed(self):
        for value in ("", "64\t64", "64\tNULL\t1\t64", "-1\t64\t1\t64"):
            with self.subTest(value=value):
                self.server.answers["recovery-index-count"] = value
                with self.assertRaisesRegex(PipelineError, "invalid numeric"):
                    self.validate()

    def test_hashes_are_recomputed_from_raw_xml_and_null_safe_key_values(self):
        self.validate()
        sql = next(sql for label, sql in self.server.calls if label == "recovery-metadata-range")
        self.assertIn("s.metadata_hash <=> UNHEX(SHA2(r.metadata, 256))", sql)
        for kind, value, digest in recovery._KEY_FIELDS:
            self.assertIn(f"s.{digest} <=> UNHEX(SHA2(CONCAT('{kind}:', s.{value}), 256))", sql)
        self.assertIn("CAST(s.journal_title AS BINARY) <=> CAST(i.journal_title AS BINARY)", sql)
        self.assertIn("CAST(i.application AS BINARY) <=> CAST(c.application AS BINARY)", sql)


class RollbackWaitTests(unittest.TestCase):
    def setUp(self):
        self.server = AuditServer()

    @mock.patch.object(recovery, "Progress")
    @mock.patch.object(recovery.time, "sleep")
    def test_waits_past_ready_ping_until_background_recovery_finishes(self, sleep, progress):
        self.server.answers["recovery-transactions"] = ["1\t1\t1\t500", "1\t1\t1\t200", "0\t0\t0\t0"]
        recovery.wait_innodb_rollback(self.server, database="fixture", password=None)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(progress.return_value.__enter__.return_value.update.call_count, 3)

    @mock.patch.object(recovery, "Progress")
    @mock.patch.object(recovery.time, "sleep")
    def test_refuses_unknown_client_transaction_instead_of_waiting(self, sleep, progress):
        self.server.answers["recovery-transactions"] = "1\t0\t0\t50"
        with self.assertRaisesRegex(PipelineError, "ordinary client transaction"):
            recovery.wait_innodb_rollback(self.server, database="fixture", password=None)
        sleep.assert_not_called()

    @mock.patch.object(recovery, "Progress")
    @mock.patch.object(recovery.time, "sleep", side_effect=KeyboardInterrupt)
    def test_interrupt_does_not_kill_or_modify_recovering_mysql(self, sleep, progress):
        self.server.answers["recovery-transactions"] = "1\t1\t1\t500"
        with self.assertRaises(KeyboardInterrupt):
            recovery.wait_innodb_rollback(self.server, database="fixture", password=None)
        self.assertTrue(all(sql.lstrip().startswith("SELECT") for _, sql in self.server.calls))


if __name__ == "__main__":
    unittest.main()
