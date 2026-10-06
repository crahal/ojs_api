from __future__ import annotations

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


def create_complete_release(clean: Path, version: str) -> tuple[Path, Path]:
    export = clean / f"pkpbeacon-clean-{version}.sql.gz"
    report = clean / f"pkpbeacon-changes-{version}.json"
    export.write_bytes(b"clean export")
    report.write_text("{}\n", encoding="utf-8")
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
