from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import publish_live
import run_pipeline


def write_candidate_artifacts(
    directory: Path,
    *,
    release: str,
    source_sha256: str,
    build_sql_sha256: str,
) -> Path:
    export_path = directory / f"pkpbeacon-clean-{release}.sql.gz"
    export_payload = gzip.compress(b"SELECT 1;\n", mtime=0)
    export_path.write_bytes(export_payload)
    export_path.with_name(f"{export_path.name}.sha256").write_text(
        f"{hashlib.sha256(export_payload).hexdigest()}  {export_path.name}\n",
        encoding="ascii",
    )
    report_path = directory / f"pkpbeacon-changes-{release}.json"
    report_payload = (
        json.dumps(
            {
                "schema_version": 1,
                "snapshot": {
                    "date": release,
                    "source_sha256": source_sha256,
                    "build_sql_sha256": build_sql_sha256,
                },
            },
            sort_keys=True,
        )
        + "\n"
    )
    report_path.write_text(report_payload, encoding="utf-8")
    report_path.with_name(f"{report_path.name}.sha256").write_text(
        f"{hashlib.sha256(report_payload.encode()).hexdigest()}  "
        f"{report_path.name}\n",
        encoding="ascii",
    )
    run_pipeline.publish_release_manifest(
        directory,
        version=release,
        export_path=export_path,
        export_sha256=hashlib.sha256(export_payload).hexdigest(),
        report_path=report_path,
        report_sha256=hashlib.sha256(report_payload.encode()).hexdigest(),
    )
    return export_path


class PublishLiveTest(unittest.TestCase):
    def test_checksum_sidecar_and_digest_reader(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "pkpbeacon-clean-2026-07-01.sql.gz"
            payload = gzip.compress(b"SELECT 1;\n", mtime=0)
            path.write_bytes(payload)
            expected = hashlib.sha256(payload).hexdigest()
            path.with_name(f"{path.name}.sha256").write_text(
                f"{expected}  {path.name}\n",
                encoding="ascii",
            )
            self.assertEqual(publish_live.checksum_for(path), expected)
            publish_live.verify_export_checksum(path, expected)
            with path.open("rb") as source:
                reader = publish_live.DigestReader(source)
                with gzip.GzipFile(fileobj=reader, mode="rb") as uncompressed:
                    self.assertEqual(uncompressed.read(), b"SELECT 1;\n")
                self.assertEqual(reader.hexdigest(), expected)

    def test_bad_checksum_is_rejected_before_candidate_sql_is_used(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            path = write_candidate_artifacts(
                project_root,
                release="2026-07-01",
                source_sha256="a" * 64,
                build_sql_sha256="b" * 64,
            )
            path.write_bytes(path.read_bytes() + b"tampered")
            mysql = mock.Mock()
            with (
                mock.patch.object(
                    publish_live,
                    "ComposeMySQL",
                    return_value=mysql,
                ),
                mock.patch.object(
                    publish_live,
                    "table_count",
                    return_value=len(publish_live.CLEAN_TABLES),
                ),
                mock.patch.object(
                    publish_live,
                    "latest_snapshot",
                    return_value="2026-06-01",
                ),
            ):
                with self.assertRaisesRegex(
                    publish_live.PublishError,
                    "checksum mismatch before import",
                ):
                    publish_live.publish(
                        export_path=path,
                        database="pkpbeacon_db",
                        project_root=project_root,
                        keep_previous=False,
                    )
            mysql.execute.assert_not_called()
            mysql.import_export.assert_not_called()

    def test_same_date_and_provenance_is_a_safe_noop(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            source_sha256 = "a" * 64
            build_sql_sha256 = "b" * 64
            path = write_candidate_artifacts(
                project_root,
                release="2026-07-01",
                source_sha256=source_sha256,
                build_sql_sha256=build_sql_sha256,
            )
            mysql = mock.Mock()
            mysql.execute.side_effect = [
                f"{source_sha256}\t{build_sql_sha256}",
            ]
            with (
                mock.patch.object(
                    publish_live,
                    "ComposeMySQL",
                    return_value=mysql,
                ),
                mock.patch.object(
                    publish_live,
                    "table_count",
                    return_value=len(publish_live.CLEAN_TABLES),
                ),
                mock.patch.object(
                    publish_live,
                    "latest_snapshot",
                    return_value="2026-07-01",
                ),
                mock.patch.object(
                    publish_live,
                    "verify_export_checksum",
                ) as verify_export,
            ):
                changed = publish_live.publish(
                    export_path=path,
                    database="pkpbeacon_db",
                    project_root=project_root,
                    keep_previous=True,
                )

            self.assertFalse(changed)
            verify_export.assert_not_called()
            self.assertEqual(mysql.execute.call_count, 1)
            mysql.import_export.assert_not_called()

    def test_same_date_with_different_provenance_fails_loudly(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            path = write_candidate_artifacts(
                project_root,
                release="2026-07-01",
                source_sha256="a" * 64,
                build_sql_sha256="b" * 64,
            )
            mysql = mock.Mock()
            mysql.execute.return_value = f"{'c' * 64}\t{'b' * 64}"
            with (
                mock.patch.object(
                    publish_live,
                    "ComposeMySQL",
                    return_value=mysql,
                ),
                mock.patch.object(
                    publish_live,
                    "table_count",
                    return_value=len(publish_live.CLEAN_TABLES),
                ),
                mock.patch.object(
                    publish_live,
                    "latest_snapshot",
                    return_value="2026-07-01",
                ),
            ):
                with self.assertRaisesRegex(
                    publish_live.PublishError,
                    "different provenance in source_sha256",
                ):
                    publish_live.publish(
                        export_path=path,
                        database="pkpbeacon_db",
                        project_root=project_root,
                        keep_previous=False,
                    )

            mysql.import_export.assert_not_called()

    def test_same_date_requires_an_intact_change_report(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            path = write_candidate_artifacts(
                project_root,
                release="2026-07-01",
                source_sha256="a" * 64,
                build_sql_sha256="b" * 64,
            )
            report_path = project_root / "pkpbeacon-changes-2026-07-01.json"
            report_path.write_text("{}\n", encoding="utf-8")
            mysql = mock.Mock()
            with (
                mock.patch.object(
                    publish_live,
                    "ComposeMySQL",
                    return_value=mysql,
                ),
                mock.patch.object(
                    publish_live,
                    "table_count",
                    return_value=len(publish_live.CLEAN_TABLES),
                ),
                mock.patch.object(
                    publish_live,
                    "latest_snapshot",
                    return_value="2026-07-01",
                ),
            ):
                with self.assertRaisesRegex(
                    publish_live.PublishError,
                    "change report checksum mismatch before use",
                ):
                    publish_live.publish(
                        export_path=path,
                        database="pkpbeacon_db",
                        project_root=project_root,
                        keep_previous=False,
                    )

            mysql.execute.assert_not_called()
            mysql.import_export.assert_not_called()

    def test_staged_export_must_match_the_change_report_before_swap(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            path = write_candidate_artifacts(
                project_root,
                release="2026-07-01",
                source_sha256="a" * 64,
                build_sql_sha256="b" * 64,
            )
            mysql = mock.Mock()
            with (
                mock.patch.object(
                    publish_live,
                    "ComposeMySQL",
                    return_value=mysql,
                ),
                mock.patch.object(
                    publish_live,
                    "table_count",
                    side_effect=[len(publish_live.CLEAN_TABLES)] * 2,
                ),
                mock.patch.object(
                    publish_live,
                    "latest_snapshot",
                    side_effect=["2026-06-01", "2026-07-01"],
                ),
                mock.patch.object(
                    publish_live,
                    "snapshot_provenance",
                    return_value=("c" * 64, "b" * 64),
                ),
            ):
                with self.assertRaisesRegex(
                    publish_live.PublishError,
                    "does not match its change report in source_sha256",
                ):
                    publish_live.publish(
                        export_path=path,
                        database="pkpbeacon_db",
                        project_root=project_root,
                        keep_previous=False,
                    )

            executed = "\n".join(
                call.args[0] for call in mysql.execute.call_args_list
            )
            self.assertNotIn("RENAME TABLE", executed)

    def test_publisher_lock_is_nonblocking_and_released(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            previous_umask = os.umask(0o077)
            try:
                with publish_live.publisher_lock(project_root) as lock_path:
                    self.assertTrue(lock_path.is_file())
                    self.assertEqual(
                        lock_path,
                        project_root.resolve()
                        / "data"
                        / "clean"
                        / ".publish-live.lock",
                    )
                    self.assertEqual(lock_path.stat().st_mode & 0o777, 0o444)
                    with self.assertRaisesRegex(
                        publish_live.PublishError,
                        "another live publisher process is already running",
                    ):
                        with publish_live.publisher_lock(project_root):
                            self.fail("a competing publisher acquired the lock")
            finally:
                os.umask(previous_umask)

            with publish_live.publisher_lock(project_root):
                pass

    def test_publisher_lock_refuses_a_symlink(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            lock_directory = project_root / "data" / "clean"
            lock_directory.mkdir(parents=True)
            target = project_root / "unrelated-file"
            target.write_text("do not lock me\n", encoding="ascii")
            (lock_directory / ".publish-live.lock").symlink_to(target)

            with self.assertRaisesRegex(
                publish_live.PublishError,
                "could not safely open publisher lock",
            ):
                with publish_live.publisher_lock(project_root):
                    self.fail("publisher followed a lock-file symlink")

    def test_post_swap_validation_failure_still_rolls_back(self):
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            source_sha256 = "a" * 64
            build_sql_sha256 = "b" * 64
            path = write_candidate_artifacts(
                project_root,
                release="2026-07-01",
                source_sha256=source_sha256,
                build_sql_sha256=build_sql_sha256,
            )
            mysql = mock.Mock()
            with (
                mock.patch.object(
                    publish_live,
                    "ComposeMySQL",
                    return_value=mysql,
                ),
                mock.patch.object(
                    publish_live,
                    "table_count",
                    side_effect=[len(publish_live.CLEAN_TABLES)] * 2,
                ),
                mock.patch.object(
                    publish_live,
                    "latest_snapshot",
                    side_effect=["2026-06-01", "2026-07-01", None],
                ),
                mock.patch.object(
                    publish_live,
                    "snapshot_provenance",
                    return_value=(source_sha256, build_sql_sha256),
                ),
            ):
                with self.assertRaisesRegex(
                    publish_live.PublishError,
                    "post-swap live snapshot validation failed",
                ):
                    publish_live.publish(
                        export_path=path,
                        database="pkpbeacon_db",
                        project_root=project_root,
                        keep_previous=True,
                    )

            mysql.execute.assert_any_call(
                publish_live.rollback_sql(
                    "pkpbeacon_db",
                    "pkpbeacon_db_next",
                    "pkpbeacon_db_previous",
                    True,
                )
            )

    def test_atomic_swap_moves_old_and_new_tables_together(self):
        sql = publish_live.atomic_swap_sql(
            "pkpbeacon_db",
            "pkpbeacon_db_next",
            "pkpbeacon_db_previous",
            True,
        )
        self.assertEqual(sql.count(" TO "), len(publish_live.CLEAN_TABLES) * 2)
        for table in publish_live.CLEAN_TABLES:
            self.assertIn(
                f"`pkpbeacon_db`.`{table}` TO "
                f"`pkpbeacon_db_previous`.`{table}`",
                sql,
            )
            self.assertIn(
                f"`pkpbeacon_db_next`.`{table}` TO "
                f"`pkpbeacon_db`.`{table}`",
                sql,
            )

    def test_unsafe_database_name_is_rejected(self):
        with self.assertRaises(publish_live.PublishError):
            publish_live.atomic_swap_sql(
                "pkpbeacon_db; DROP DATABASE x",
                "next",
                "previous",
                True,
            )


@unittest.skipUnless(
    all(shutil.which(name) for name in ("mysqld", "mysql", "mysqladmin")),
    "MySQL 8 client and server binaries are required",
)
class PublishLiveIntegrationTest(unittest.TestCase):
    def test_atomic_swap_executes_in_mysql(self):
        project_root = Path(__file__).parents[1]
        with TemporaryDirectory(
            dir=project_root,
            prefix=".publish-live-test-",
        ) as directory:
            server = run_pipeline.MySQLServer(
                run_pipeline.MySQLTools.discover(),
                Path(directory) / "mysql",
                "128M",
            )
            try:
                server.initialize()
                server.start(None)
                setup = [
                    "CREATE DATABASE live;",
                    "CREATE DATABASE next_release;",
                    "CREATE DATABASE previous_release;",
                ]
                for table in publish_live.CLEAN_TABLES:
                    quoted = publish_live.quote_identifier(table)
                    setup.extend(
                        (
                            f"CREATE TABLE live.{quoted} (release_id INT NOT NULL);",
                            f"INSERT INTO live.{quoted} VALUES (1);",
                            f"CREATE TABLE next_release.{quoted} "
                            "(release_id INT NOT NULL);",
                            f"INSERT INTO next_release.{quoted} VALUES (2);",
                        )
                    )
                server.execute("\n".join(setup), password=None)
                server.execute(
                    publish_live.atomic_swap_sql(
                        "live",
                        "next_release",
                        "previous_release",
                        True,
                    ),
                    password=None,
                )
                for table in publish_live.CLEAN_TABLES:
                    quoted = publish_live.quote_identifier(table)
                    self.assertEqual(
                        server.execute(
                            f"SELECT release_id FROM live.{quoted};",
                            password=None,
                        ),
                        "2",
                    )
                    self.assertEqual(
                        server.execute(
                            f"SELECT release_id FROM previous_release.{quoted};",
                            password=None,
                        ),
                        "1",
                    )
                server.execute(
                    publish_live.rollback_sql(
                        "live",
                        "next_release",
                        "previous_release",
                        True,
                    ),
                    password=None,
                )
                for table in publish_live.CLEAN_TABLES:
                    quoted = publish_live.quote_identifier(table)
                    self.assertEqual(
                        server.execute(
                            f"SELECT release_id FROM live.{quoted};",
                            password=None,
                        ),
                        "1",
                    )
                    self.assertEqual(
                        server.execute(
                            f"SELECT release_id FROM next_release.{quoted};",
                            password=None,
                        ),
                        "2",
                    )
            finally:
                server.shutdown(None)


if __name__ == "__main__":
    unittest.main()
