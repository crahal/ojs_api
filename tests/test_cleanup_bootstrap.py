from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import cleanup_bootstrap as cleanup


class BootstrapCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "data" / "raw"
        self.clean = self.root / "data" / "clean"
        self.raw.mkdir(parents=True)
        self.clean.mkdir()
        (self.root / "sql").mkdir()
        self.build = self.root / "sql" / "01_build_ojs_tables.sql"
        self.build.write_bytes(b"SELECT 1;\n")
        self.build_hash = hashlib.sha256(self.build.read_bytes()).hexdigest()
        self.args = cleanup.parser().parse_args([
            "--project-root", str(self.root), "--snapshot", "2026-01-01",
            "--snapshot", "2026-07-01"])
        self.coordinator = cleanup.update.Coordinator(self.args)
        self.health = mock.Mock()
        self.coordinator.check_health = self.health
        self.output = io.StringIO()
        self.old = [self.make_release(version, prune=True)
                    for version in ("2026-01-01", "2026-07-01")]
        self.latest = self.make_release("2026-10-01", live=True)

    def make_release(self, version, *, prune=False, live=False):
        archive = self.raw / f"pkpbeacon-{version}.sql.gz"
        dump = ("-- MySQL dump 10.13\nCREATE TABLE example (id INTEGER);\n"
                f"-- Dump completed on {version} 12:00:00\n").encode()
        archive.write_bytes(gzip.compress(dump))
        with contextlib.redirect_stderr(io.StringIO()):
            snapshot = cleanup.run_pipeline.inspect_snapshot(archive)
        source_hash = hashlib.sha256(dump).hexdigest()
        report = {"schema_version": 1, "snapshot": {
            "date": version, "completed_at": snapshot.completed_at.isoformat(),
            "source_filename": archive.name, "source_size_bytes": len(dump),
            "source_sha256": source_hash, "build_sql_sha256": self.build_hash},
            "release_guard": {"status": "passed"}}
        report_path = self.clean / f"pkpbeacon-changes-{version}.json"
        report_path.write_text(json.dumps(report))
        report_hash = hashlib.sha256(report_path.read_bytes()).hexdigest()
        export = self.clean / f"pkpbeacon-clean-{version}.sql.gz"
        export.write_bytes(gzip.compress(b"SELECT 1;"))
        export_hash = hashlib.sha256(export.read_bytes()).hexdigest()
        for artifact, digest in ((export, export_hash), (report_path, report_hash)):
            artifact.with_name(artifact.name + ".sha256").write_text(digest)
        (self.clean / f"pkpbeacon-release-{version}.json").write_text(json.dumps({
            "schema_version": 1, "snapshot_date": version,
            "clean_export_filename": export.name, "clean_export_sha256": export_hash,
            "change_report_filename": report_path.name, "change_report_sha256": report_hash}))
        if prune:
            export.unlink()
            export.with_name(export.name + ".sha256").unlink()
        if live:
            datadir = self.clean / f"mysql-{version}"
            datadir.mkdir()
            (datadir / cleanup.update.MARKER).write_text(json.dumps({
                "schema_version": 1, "snapshot_date": version,
                "source_sha256": source_hash, "build_sql_sha256": self.build_hash,
                "clean_only": True, "data_directory_name": datadir.name}))
            for name in ("mysql-live", "mysql-current"):
                (self.clean / name).symlink_to(datadir.name)
        return archive

    def run_cleanup(self):
        with contextlib.redirect_stdout(self.output), \
                mock.patch.object(cleanup.download_beacon, "validate_archive",
                                  side_effect=AssertionError("do not rescan raw archives")), \
                mock.patch.object(cleanup.publish_live, "verify_export_checksum",
                                  side_effect=AssertionError("do not scan giant export")):
            return cleanup.execute(self.args, self.coordinator)

    def test_removes_only_explicit_old_gzips_after_health_and_is_idempotent(self):
        legacy = self.raw / "pkpbeacon-2026-01-01.sql"
        legacy.write_text("unmanaged SQL")
        unlisted = self.make_release("2026-04-01", prune=True)

        def check_health(release, datadir):
            self.assertEqual(release.date, "2026-10-01")
            self.assertEqual(release.build_sql_sha256, self.build_hash)
            self.assertTrue(all(path.exists() for path in self.old))
            self.assertFalse(list(self.clean.glob("pkpbeacon-bootstrap-cleanup-*")))
        self.health.side_effect = check_health
        result = self.run_cleanup()
        self.assertEqual(result["removed_count"], 4)
        self.health.assert_called_once()
        self.assertTrue(all(path.exists() for path in (legacy, unlisted, self.latest)))
        for archive in self.old:
            self.assertFalse(archive.exists())
            self.assertFalse(archive.with_name(archive.name + ".metadata.json").exists())
            version = archive.name[10:20]
            audit = cleanup.document(cleanup.audit_path(self.clean, version))
            self.assertEqual(audit["phase"], "complete")
            self.assertTrue(all(entry["deleted"] for entry in audit["artifacts"]))
            self.assertTrue((self.clean / f"pkpbeacon-changes-{version}.json").exists())
        self.assertEqual(self.run_cleanup()["status"], "unchanged")
        self.health.assert_called_once()

    def test_without_live_release_defers_and_retains_files(self):
        (self.clean / "mysql-live").unlink()
        self.assertEqual(self.run_cleanup()["status"], "deferred")
        self.assertTrue(all(path.exists() for path in self.old))
        self.health.assert_not_called()

    def test_health_failure_deletes_nothing_and_creates_no_audit(self):
        self.health.side_effect = cleanup.update.UpdateError("API provenance mismatch")
        with self.assertRaisesRegex(cleanup.update.UpdateError, "API provenance"):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))
        self.assertFalse(list(self.clean.glob("pkpbeacon-bootstrap-cleanup-*")))

    def test_live_or_newer_allowlist_is_refused_before_any_deletion(self):
        for version in ("2026-10-01", "2026-12-01"):
            with self.subTest(version=version):
                self.args.snapshot = ["2026-01-01", version]
                with self.assertRaisesRegex(cleanup.update.UpdateError, "live or newer"):
                    self.run_cleanup()
                self.assertTrue(all(path.exists() for path in self.old))
        self.health.assert_not_called()

    def test_changed_gzip_refuses_all_cleanup(self):
        self.old[1].write_bytes(gzip.compress(b"different SQL"))
        with self.assertRaisesRegex(cleanup.update.UpdateError, "validated metadata"):
            self.run_cleanup()
        self.assertTrue(self.old[0].exists())
        self.health.assert_not_called()

    def test_report_tampering_refuses_all_cleanup_even_with_updated_sidecar(self):
        report = self.clean / "pkpbeacon-changes-2026-07-01.json"
        payload = json.loads(report.read_text())
        payload["snapshot"]["source_sha256"] = "d" * 64
        report.write_text(json.dumps(payload))
        report.with_name(report.name + ".sha256").write_text(hashlib.sha256(report.read_bytes()).hexdigest())
        with self.assertRaisesRegex(cleanup.update.UpdateError, "provenance"):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))
        self.health.assert_not_called()

    def test_revalidated_different_gzip_still_must_match_historical_report(self):
        changed = gzip.decompress(self.old[1].read_bytes()).replace(b"INTEGER", b"BIGINT")
        self.old[1].write_bytes(gzip.compress(changed))
        with contextlib.redirect_stderr(io.StringIO()):
            cleanup.run_pipeline.inspect_snapshot(self.old[1])
        with self.assertRaisesRegex(cleanup.update.UpdateError, "historical report"):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))
        self.health.assert_not_called()

    def test_symlinked_archive_and_missing_metadata_are_refused(self):
        archive = self.old[0]
        original = archive.read_bytes()
        archive.unlink()
        archive.symlink_to(self.latest.name)
        with self.assertRaisesRegex(cleanup.update.UpdateError, "regular file"):
            self.run_cleanup()
        archive.unlink()
        archive.write_bytes(original)
        archive.with_name(archive.name + ".metadata.json").unlink()
        with self.assertRaises(OSError):
            self.run_cleanup()
        self.assertTrue(self.latest.exists())
        self.assertTrue(all(path.exists() for path in self.old))

    def test_changed_build_and_pending_update_are_refused(self):
        original = self.build.read_bytes()
        self.build.write_bytes(b"changed build SQL")
        with self.assertRaisesRegex(cleanup.update.UpdateError, "build SQL"):
            self.run_cleanup()
        self.build.write_bytes(original)
        self.coordinator.journal.write_text("{}")
        with self.assertRaisesRegex(cleanup.update.UpdateError, "recovery"):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))

    def test_unfinished_build_or_disagreeing_current_pointer_prevents_cleanup(self):
        unfinished = self.clean / "mysql-2026-11-01.building"
        unfinished.mkdir()
        with self.assertRaisesRegex(cleanup.update.UpdateError, "unfinished"):
            self.run_cleanup()
        unfinished.rmdir()
        current = self.clean / "mysql-current"
        current.unlink()
        current.symlink_to("mysql-2026-11-01")
        with self.assertRaisesRegex(cleanup.update.UpdateError, "pointers"):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))

    def test_changes_during_api_check_prevent_deletion(self):
        self.health.side_effect = lambda *args: self.build.write_bytes(b"changed")
        with self.assertRaises(cleanup.update.UpdateError):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))

    def test_archive_replacement_during_health_is_retained(self):
        self.health.side_effect = lambda *args: self.old[0].write_bytes(b"replacement")
        with self.assertRaisesRegex(cleanup.update.UpdateError, "target changed"):
            self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))

    def test_interrupt_after_each_unlink_resumes_with_reverified_health(self):
        self.args.snapshot = ["2026-01-01"]
        real_sync = cleanup.update.sync_directory
        for index in (0, 1):
            with self.subTest(index=index):
                def interrupted(path):
                    if path == self.raw:
                        raise KeyboardInterrupt
                    return real_sync(path)
                with mock.patch.object(cleanup.update, "sync_directory", side_effect=interrupted):
                    with self.assertRaises(KeyboardInterrupt):
                        self.run_cleanup()
                audit = cleanup.document(cleanup.audit_path(self.clean, "2026-01-01"))
                self.assertEqual(audit["phase"], "prepared")
                self.assertFalse((self.raw / audit["artifacts"][index]["name"]).exists())
        result = self.run_cleanup()
        self.assertEqual(result["removed_count"], 0)
        self.assertEqual(self.health.call_count, 3)
        audit = cleanup.document(cleanup.audit_path(self.clean, "2026-01-01"))
        self.assertEqual(audit["phase"], "complete")
        self.assertTrue(all(entry["deleted"] for entry in audit["artifacts"]))

    def test_replaced_sidecar_during_recovery_is_retained(self):
        self.args.snapshot = ["2026-01-01"]
        release, _ = cleanup.live_release(self.coordinator)
        state = cleanup.prepare(self.coordinator, "2026-01-01", release)
        cleanup.update.atomic_json(cleanup.audit_path(self.clean, "2026-01-01"), state)
        self.old[0].unlink()
        sidecar = self.old[0].with_name(self.old[0].name + ".metadata.json")
        sidecar.write_text("{}")
        with self.assertRaisesRegex(cleanup.update.UpdateError, "target changed"):
            self.run_cleanup()
        self.assertTrue(sidecar.exists())
        self.health.assert_not_called()

    def test_orphan_sidecar_without_prior_audit_is_retained(self):
        self.old[0].unlink()
        with self.assertRaises(OSError):
            self.run_cleanup()
        self.assertTrue(self.old[0].with_name(self.old[0].name + ".metadata.json").exists())
        self.assertTrue(self.old[1].exists())
        self.health.assert_not_called()

    def test_resume_audit_cannot_redirect_deletion_to_unmanaged_sql(self):
        release, _ = cleanup.live_release(self.coordinator)
        state = cleanup.prepare(self.coordinator, "2026-01-01", release)
        state["artifacts"][0]["name"] = "pkpbeacon-2026-01-01.sql"
        legacy = self.raw / state["artifacts"][0]["name"]
        legacy.write_text("original local SQL")
        cleanup.update.atomic_json(cleanup.audit_path(self.clean, "2026-01-01"), state)
        with self.assertRaisesRegex(cleanup.update.UpdateError, "artifact identity"):
            self.run_cleanup()
        self.assertTrue(legacy.exists())
        self.assertTrue(all(path.exists() for path in self.old))

    def test_existing_update_locks_block_cleanup(self):
        for lock in (cleanup.update.automatic_lock(self.clean),
                     cleanup.run_pipeline.pipeline_lock(self.clean),
                     cleanup.publish_live.publisher_lock(self.root)):
            with lock, self.assertRaises(RuntimeError):
                self.run_cleanup()
        self.assertTrue(all(path.exists() for path in self.old))
        self.health.assert_not_called()

    def test_cli_rejects_non_dates_and_requires_explicit_allowlist(self):
        for arguments in ([], ["--snapshot", "../x"], ["--snapshot", "2026-02-30"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                cleanup.parser().parse_args(arguments)


if __name__ == "__main__":
    unittest.main()
