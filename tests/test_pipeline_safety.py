from __future__ import annotations

import gzip
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import run_pipeline


def minimal_dump(version: str) -> str:
    return (
        "-- MySQL dump 10.13\n"
        "CREATE TABLE example (id INTEGER);\n"
        f"-- Dump completed on {version} 12:00:00\n"
    )


def create_complete_release(
    clean: Path, version: str, *, snapshot: run_pipeline.Snapshot | None = None
) -> tuple[Path, Path]:
    export = clean / f"pkpbeacon-clean-{version}.sql.gz"
    report = clean / f"pkpbeacon-changes-{version}.json"
    export.write_bytes(b"clean export")
    payload = {}
    if snapshot is not None:
        metadata = run_pipeline.validate_cached_snapshot(snapshot.path)
        payload = {
            "schema_version": 1,
            "pipeline_version": run_pipeline.PIPELINE_VERSION,
            "snapshot": {
                "date": snapshot.version,
                "completed_at": snapshot.completed_at.isoformat(),
                "source_filename": snapshot.path.name,
                "source_size_bytes": snapshot.size,
                "source_sha256": metadata["uncompressed_sha256"],
            },
        }
    report.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    export_sha256 = run_pipeline.sha256_small_file(export)
    report_sha256 = run_pipeline.sha256_small_file(report)
    run_pipeline.write_checksum_sidecar(export, export_sha256)
    run_pipeline.write_checksum_sidecar(report, report_sha256)
    run_pipeline.publish_release_manifest(
        clean,
        version=version,
        export_path=export,
        export_sha256=export_sha256,
        report_path=report,
        report_sha256=report_sha256,
    )
    return export, report


def create_retained_history(root: Path):
    raw, clean = root / "raw", root / "clean"
    raw.mkdir()
    clean.mkdir()
    snapshots = []
    for version in ("2026-01-01", "2026-07-01"):
        path = raw / f"pkpbeacon-{version}.sql.gz"
        path.write_bytes(gzip.compress(minimal_dump(version).encode("utf-8")))
        snapshot = run_pipeline.inspect_snapshot(path)
        snapshots.append(snapshot)
        export, report = create_complete_release(clean, version, snapshot=snapshot)
        if version == "2026-01-01":
            export.unlink()
            run_pipeline.checksum_sidecar_path(export).unlink()
            retained_report = report
    return raw, clean, snapshots[0], snapshots[1], retained_report


class ExactKeysRecoveryFilesystemTests(unittest.TestCase):
    def recovery_fixture(self, root):
        version = "2026-01-01"
        source = root / f"pkpbeacon-{version}.sql"
        source.write_text(minimal_dump(version), encoding="utf-8")
        snapshot = run_pipeline.inspect_snapshot(source)
        clean = root / "clean"
        staging = clean / f"mysql-{version}.building"
        staging.mkdir(parents=True)
        (staging / "records.ibd").write_bytes(b"irreplaceable imported records")
        build_sql = Path(__file__).parents[1] / "sql" / "01_build_ojs_tables.sql"
        run_pipeline.write_build_state(
            staging, snapshot=snapshot,
            source_sha256=run_pipeline.sha256_small_file(source),
            build_sql_sha256=run_pipeline.sha256_small_file(build_sql),
            phase="finalizing",
        )
        return staging, dict(
            snapshot=snapshot, clean_dir=clean, build_sql=build_sql,
            database="fixture", root_password="fixture",
            api_db_user="api", api_db_password="fixture",
            release_thresholds=run_pipeline.ReleaseThresholds(.1, .1, .1, .1),
            allow_anomalous_release=False, buffer_pool_size="128M",
            progress_seconds=1, force_rebuild=False, verify_existing=False,
            verify_source_checksum=False, full_rescan=False, publish_current=False,
            metadata_workers=1, resume_building=True, compact_storage=True,
            recover_exact_keys=True,
        )

    @staticmethod
    def filesystem_state(root):
        result = {}
        for path in root.rglob("*"):
            identity = path.lstat().st_ino
            if path.is_symlink():
                value = ("symlink", identity, str(path.readlink()))
            elif path.is_file():
                value = ("file", identity, path.read_bytes())
            else:
                value = ("directory", identity)
            result[str(path.relative_to(root))] = value
        return result

    def assert_preflight_refused(self, root, options, message):
        before = self.filesystem_state(root)
        with mock.patch.object(run_pipeline.MySQLTools, "discover"), \
                mock.patch.object(run_pipeline, "MySQLServer", side_effect=AssertionError("must not create MySQL server")), \
                mock.patch.object(run_pipeline.shutil, "rmtree", side_effect=AssertionError("must not delete a database")), \
                mock.patch.object(Path, "unlink", side_effect=AssertionError("must not delete any artifacts")), \
                self.assertRaisesRegex(run_pipeline.PipelineError, message):
            run_pipeline.build_snapshot_database(**options)
        self.assertEqual(self.filesystem_state(root), before,
                         "rejected recovery must preserve every file, inode and symlink")

    def test_recovery_requires_compact_resume_without_force(self):
        for option, value in (("force_rebuild", True), ("compact_storage", False),
                              ("resume_building", False)):
            with self.subTest(option=option), TemporaryDirectory() as directory:
                root = Path(directory)
                _, options = self.recovery_fixture(root)
                options[option] = value
                self.assert_preflight_refused(root, options, "requires compact storage")

    def test_recovery_refuses_missing_or_symlinked_staging_and_checkpoint(self):
        for target in ("missing_staging", "staging", "checkpoint", "dangling_checkpoint"):
            with self.subTest(target=target), TemporaryDirectory() as directory:
                root = Path(directory)
                staging, options = self.recovery_fixture(root)
                if target in ("missing_staging", "staging"):
                    original = root / "original_staging"
                    staging.rename(original)
                    if target == "staging":
                        staging.symlink_to(original, target_is_directory=True)
                    message = "non-symlinked staging database"
                else:
                    checkpoint = run_pipeline.build_state_path(staging)
                    original = root / "original_checkpoint.json"
                    checkpoint.rename(original)
                    checkpoint.symlink_to(original if target == "checkpoint" else root / "missing.json")
                    message = "symlinked recovery checkpoint"
                self.assert_preflight_refused(root, options, message)

    def test_recovery_refuses_changed_or_invalid_checkpoint_before_startup(self):
        cases = (
            ("phase", "metadata_ready"),
            ("pipeline_version", "unexpected-version"),
            ("snapshot_version", "2025-12-01"),
            ("source_filename", "pkpbeacon-2025-12-01.sql"),
            ("source_size_bytes", 1),
            ("build_sql_sha256", "0" * 64),
            ("source_sha256", "invalid-digest"),
            ("source_sha256", None),
            ("missing", None),
            ("malformed", None),
            ("non_object", None),
        )
        for field, value in cases:
            with self.subTest(field=field, value=value), TemporaryDirectory() as directory:
                root = Path(directory)
                staging, options = self.recovery_fixture(root)
                path = run_pipeline.build_state_path(staging)
                if field == "missing":
                    path.unlink()
                elif field == "malformed":
                    path.write_text("{", encoding="utf-8")
                elif field == "non_object":
                    path.write_text("[]", encoding="utf-8")
                else:
                    state = run_pipeline.read_build_state(staging)
                    state[field] = value
                    path.write_text(json.dumps(state), encoding="utf-8")
                self.assert_preflight_refused(root, options, "checkpoint")

    def test_recovery_refuses_history_live_pointers_and_temporary_outputs(self):
        artifacts = (
            ("mysql-live", "symlink"),
            ("mysql-current", "dangling_symlink"),
            ("mysql-2025-12-01", "directory"),
            ("mysql-2026-01-01", "directory"),
            ("mysql-2026-07-01.building", "directory"),
            ("pkpbeacon-clean-2025-12-01.sql.gz", "file"),
            ("pkpbeacon-clean-2025-12-01.sql.gz.sha256", "file"),
            ("pkpbeacon-changes-2025-12-01.json", "file"),
            ("pkpbeacon-release-2025-12-01.json", "file"),
            ("pkpbeacon-clean-current.sql.gz", "dangling_symlink"),
            (".pkpbeacon-clean-2026-01-01.sql.gz.tmp", "file"),
            (".pkpbeacon-changes-2026-01-01.json.tmp", "file"),
            (".pkpbeacon-release-2026-01-01.json.tmp", "dangling_symlink"),
        )
        for name, kind in artifacts:
            with self.subTest(artifact=name), TemporaryDirectory() as directory:
                root = Path(directory)
                staging, options = self.recovery_fixture(root)
                artifact = options["clean_dir"] / name
                if kind == "symlink":
                    artifact.symlink_to(staging.name)
                elif kind == "dangling_symlink":
                    artifact.symlink_to("missing-target")
                elif kind == "directory":
                    artifact.mkdir()
                    (artifact / "keep.ibd").write_bytes(b"existing dated database")
                else:
                    artifact.write_bytes(b"existing release artifact")
                self.assert_preflight_refused(root, options, "bootstrap-only")

    def test_every_sql_connection_receives_required_session_settings(self):
        with TemporaryDirectory() as directory:
            _, options = self.recovery_fixture(Path(directory))
            prelude = run_pipeline.build_sql_prelude(
                snapshot=options["snapshot"], source_sha256="1" * 64,
                build_sql_sha256="2" * 64, mysql_version="8.4.6", full_rescan=False,
            )
        for statement in (
            "SET SESSION group_concat_max_len = 1073741824;",
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;",
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';",
        ):
            self.assertIn(statement, prelude)


class PipelineSafetyTest(unittest.TestCase):
    def test_force_rebuild_protects_live_pointer_even_when_candidate_is_newer(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            clean = root / "clean"
            live = clean / "mysql-2026-01-01"
            newer = clean / "mysql-2026-07-01"
            live.mkdir(parents=True)
            newer.mkdir()
            (live / "keep").write_text("serving database")
            (clean / "mysql-live").symlink_to(live.name)
            (clean / "mysql-current").symlink_to(newer.name)
            source = root / "pkpbeacon-2026-01-01.sql"
            source.write_text(minimal_dump("2026-01-01"))
            build_sql = Path(__file__).parents[1] / "sql" / "01_build_ojs_tables.sql"
            with mock.patch.object(run_pipeline.MySQLTools, "discover"):
                with self.assertRaisesRegex(run_pipeline.PipelineError, "mysql-live"):
                    run_pipeline.build_snapshot_database(
                        snapshot=run_pipeline.inspect_snapshot(source), clean_dir=clean,
                        build_sql=build_sql, database="fixture", root_password="fixture",
                        api_db_user="api", api_db_password="fixture",
                        release_thresholds=run_pipeline.ReleaseThresholds(.1, .1, .1, .1),
                        allow_anomalous_release=False, buffer_pool_size="128M",
                        progress_seconds=1, force_rebuild=True, verify_existing=False,
                        verify_source_checksum=False, full_rescan=False,
                        publish_current=True, metadata_workers=1, resume_building=False,
                    )
            self.assertTrue((live / "keep").exists())

    def test_late_historical_file_is_never_silently_skipped(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            clean = root / "clean"
            raw.mkdir()
            clean.mkdir()
            for version in ("2026-01-01", "2026-07-01"):
                (raw / f"pkpbeacon-{version}.sql").write_text(
                    minimal_dump(version),
                    encoding="utf-8",
                )
            create_complete_release(clean, "2026-07-01")
            target = run_pipeline.inspect_snapshot(
                raw / "pkpbeacon-2026-07-01.sql"
            )
            with self.assertRaisesRegex(
                run_pipeline.PipelineError,
                "arrived behind the published history",
            ):
                run_pipeline.snapshots_to_process(
                    target=target,
                    raw_dir=raw,
                    clean_dir=clean,
                    explicit_snapshot=False,
                    rebuild_history=False,
                )

    def test_retained_input_is_recognized_after_its_clean_export_was_pruned(self):
        with TemporaryDirectory() as directory:
            raw, clean, historical, target, _ = create_retained_history(Path(directory))
            self.assertNotIn(
                "source_url", run_pipeline.read_validated_metadata(historical.path)
            )
            with mock.patch(
                "download_beacon.validate_archive",
                side_effect=AssertionError("validated archives must not be rescanned"),
            ):
                self.assertTrue(
                    run_pipeline.retained_snapshot_matches_release(historical, clean)
                )
                self.assertEqual(
                    run_pipeline.snapshots_to_process(
                        target=target, raw_dir=raw, clean_dir=clean,
                        explicit_snapshot=False, rebuild_history=False,
                    ),
                    [],
                )
            next_path = raw / "pkpbeacon-2026-10-01.sql"
            next_path.write_text(minimal_dump("2026-10-01"), encoding="utf-8")
            next_snapshot = run_pipeline.inspect_snapshot(next_path)
            self.assertEqual(
                run_pipeline.snapshots_to_process(
                    target=next_snapshot, raw_dir=raw, clean_dir=clean,
                    explicit_snapshot=False, rebuild_history=False,
                ),
                [next_snapshot],
            )

    def test_unprocessed_older_archive_is_still_rejected(self):
        with TemporaryDirectory() as directory:
            raw, clean, historical, target, report = create_retained_history(Path(directory))
            run_pipeline.release_manifest_path(clean, historical.version).unlink()
            report.unlink()
            run_pipeline.checksum_sidecar_path(report).unlink()
            with self.assertRaisesRegex(run_pipeline.PipelineError, "arrived behind"):
                run_pipeline.snapshots_to_process(
                    target=target, raw_dir=raw, clean_dir=clean,
                    explicit_snapshot=False, rebuild_history=False,
                )

    def test_retained_history_requires_intact_manifest_and_report_checksums(self):
        for corruption in (
            "report", "report_and_sidecar", "missing_sidecar", "missing_manifest",
            "malformed_manifest", "wrong_manifest_date", "wrong_source_date",
        ):
            with self.subTest(corruption=corruption), TemporaryDirectory() as directory:
                raw, clean, historical, target, report = create_retained_history(Path(directory))
                manifest_path = run_pipeline.release_manifest_path(clean, historical.version)
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                if corruption in ("report", "report_and_sidecar", "wrong_source_date"):
                    payload = json.loads(report.read_text(encoding="utf-8"))
                    payload["snapshot"]["date"] = "2025-12-01"
                    report.write_text(json.dumps(payload), encoding="utf-8")
                    if corruption != "report":
                        report_hash = run_pipeline.sha256_small_file(report)
                        run_pipeline.write_checksum_sidecar(report, report_hash)
                        if corruption == "wrong_source_date":
                            manifest["change_report_sha256"] = report_hash
                elif corruption == "missing_sidecar":
                    run_pipeline.checksum_sidecar_path(report).unlink()
                elif corruption == "missing_manifest":
                    manifest_path.unlink()
                elif corruption == "malformed_manifest":
                    manifest = []
                elif corruption == "wrong_manifest_date":
                    manifest["snapshot_date"] = "2025-12-01"
                if manifest_path.exists():
                    manifest_path.chmod(0o644)
                    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaisesRegex(run_pipeline.PipelineError, "arrived behind"):
                    run_pipeline.snapshots_to_process(
                        target=target, raw_dir=raw, clean_dir=clean,
                        explicit_snapshot=False, rebuild_history=False,
                    )

    def test_changed_retained_archive_is_not_accepted_as_completed_history(self):
        with TemporaryDirectory() as directory:
            raw, clean, historical, target, _ = create_retained_history(Path(directory))
            changed_dump = minimal_dump(historical.version).replace("INTEGER", "BIGINT")
            historical.path.write_bytes(gzip.compress(changed_dump.encode("utf-8")))
            self.assertFalse(
                run_pipeline.retained_snapshot_matches_release(historical, clean)
            )
            # Discovery revalidates the gzip; its new, valid checksum must still
            # fail against the source checksum recorded by the completed build.
            with self.assertRaisesRegex(run_pipeline.PipelineError, "arrived behind"):
                run_pipeline.snapshots_to_process(
                    target=target, raw_dir=raw, clean_dir=clean,
                    explicit_snapshot=False, rebuild_history=False,
                )
            changed = run_pipeline.inspect_snapshot(historical.path)
            self.assertFalse(run_pipeline.retained_snapshot_matches_release(changed, clean))

    def test_incomplete_release_is_retried_and_complete_pointers_are_repaired(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            clean = root / "clean"
            raw.mkdir()
            clean.mkdir()
            version = "2026-07-01"
            snapshot_path = raw / f"pkpbeacon-{version}.sql"
            snapshot_path.write_text(minimal_dump(version), encoding="utf-8")
            target = run_pipeline.inspect_snapshot(snapshot_path)

            (clean / f"pkpbeacon-clean-{version}.sql.gz").write_bytes(b"partial")
            selected = run_pipeline.snapshots_to_process(
                target=target,
                raw_dir=raw,
                clean_dir=clean,
                explicit_snapshot=False,
                rebuild_history=False,
            )
            self.assertEqual(selected, [target])

            create_complete_release(clean, version)
            final_dir = clean / f"mysql-{version}"
            final_dir.mkdir()
            self.assertTrue(
                run_pipeline.repair_current_release_pointers(clean, version)
            )
            self.assertEqual((clean / "mysql-current").resolve(), final_dir)
            self.assertFalse(
                run_pipeline.repair_current_release_pointers(clean, version)
            )

    def test_pipeline_lock_is_shared_in_the_clean_directory(self):
        with TemporaryDirectory() as directory:
            clean = Path(directory) / "clean"
            with run_pipeline.pipeline_lock(clean) as lock_path:
                self.assertEqual(lock_path, clean / ".pipeline.lock")
                self.assertEqual(lock_path.stat().st_mode & 0o777, 0o444)
                with self.assertRaisesRegex(
                    run_pipeline.PipelineError,
                    "another data pipeline process",
                ):
                    with run_pipeline.pipeline_lock(clean):
                        self.fail("a competing pipeline acquired the lock")

    def test_release_guard_blocks_large_removal(self):
        report = {
            "previous_counts": {
                "active_sources": 100,
                "active_articles": 100,
            },
            "source_rows": {"active": 94},
            "articles": {"active": 79},
            "article_events": {
                "by_type": {"removed": 20, "merged": 1},
            },
        }
        anomalies = run_pipeline.release_anomalies(
            report,
            run_pipeline.ReleaseThresholds(0.10, 0.05, 0.05, 0.02),
        )
        self.assertEqual(
            {item["metric"] for item in anomalies},
            {"active_article_drop_fraction", "article_removal_fraction"},
        )

    def test_api_database_user_gets_only_required_select_grants(self):
        class FakeServer:
            sql = ""

            def execute(self, sql, **kwargs):
                self.sql = sql
                return ""

        server = FakeServer()
        run_pipeline.configure_api_database_user(
            server,
            database="fixture",
            password=None,
            api_user="ojs_api",
            api_password="test-password",
        )
        self.assertIn("CREATE USER IF NOT EXISTS 'ojs_api'@'%'", server.sql)
        self.assertEqual(server.sql.count("GRANT SELECT ON"), 4)
        self.assertNotIn("`records`", server.sql)
        self.assertNotIn("GRANT ALL", server.sql)

    def test_database_password_file_must_be_private(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "db-password"
            path.write_text("secret-value\n", encoding="utf-8")
            path.chmod(0o600)
            self.assertEqual(
                run_pipeline.read_private_secret(path, "fixture"),
                "secret-value",
            )
            path.chmod(0o644)
            with self.assertRaisesRegex(
                run_pipeline.PipelineError,
                "permissions are too open",
            ):
                run_pipeline.read_private_secret(path, "fixture")


if __name__ == "__main__":
    unittest.main()
