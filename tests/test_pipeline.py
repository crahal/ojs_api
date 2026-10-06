from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import run_pipeline


def create_complete_release(clean: Path, version: str) -> None:
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


SCHEMA = """-- MySQL dump 10.13  Distrib 8.0.42
SET NAMES utf8mb4;

CREATE TABLE `endpoints` (
  `id` bigint unsigned NOT NULL,
  `application` varchar(32) DEFAULT NULL,
  `oai_url` varchar(2048) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `contexts` (
  `id` bigint unsigned NOT NULL,
  `endpoint_id` bigint unsigned NOT NULL,
  `name` varchar(512) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `issns` (
  `id` bigint unsigned NOT NULL,
  `context_id` bigint unsigned NOT NULL,
  `issn` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `issns_context_id_issn_unique` (`context_id`,`issn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `records` (
  `id` bigint unsigned NOT NULL,
  `context_id` bigint unsigned NOT NULL,
  `update_date` datetime NOT NULL,
  `publish_date` datetime DEFAULT NULL,
  `metadata` mediumtext NOT NULL,
  `removed_at` datetime DEFAULT NULL,
  `created_at` datetime(6) DEFAULT NULL,
  `modified_at` datetime(6) DEFAULT NULL,
  `identifier` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `records_context_id_identifier_unique` (`context_id`,`identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `versions` (
  `id` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `endpoints` VALUES
  (1,'ojs','https://one.example/index/oai'),
  (2,'ojs','https://mirror.example/index/oai');
INSERT INTO `contexts` VALUES
  (1,1,'Journal One'),
  (2,2,'Journal Mirror');
INSERT INTO `issns` VALUES
  (1,1,'1234-567X'),
  (2,2,'8765-4321');
INSERT INTO `versions` VALUES (1);
"""


def metadata(
    *,
    oai_identifier: str,
    title: str,
    creator: str,
    published: str,
    url: str,
    doi: str | None = None,
) -> str:
    doi_node = f"<dc:identifier>{doi}</dc:identifier>" if doi else ""
    return (
        '<record xmlns="http://www.openarchives.org/OAI/2.0/">'
        "<header>"
        f"<identifier>{oai_identifier}</identifier>"
        f"<datestamp>{published}T00:00:00Z</datestamp>"
        "</header>"
        "<metadata>"
        '<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/">'
        f"<dc:title>{title}</dc:title>"
        f"<dc:creator>{creator}</dc:creator>"
        f"<dc:date>{published}</dc:date>"
        "<dc:type>info:eu-repo/semantics/article</dc:type>"
        f"<dc:identifier>{url}</dc:identifier>"
        f"{doi_node}"
        "</oai_dc:dc>"
        "</metadata>"
        "</record>"
    )


def sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def record_row(
    *,
    record_id: int,
    context_id: int,
    identifier: int,
    metadata_xml: str,
    modified: str,
    removed_at: str | None = None,
) -> str:
    removed = "NULL" if removed_at is None else sql_quote(removed_at)
    return (
        f"({record_id},{context_id},{sql_quote(modified[:10] + ' 00:00:00')},"
        f"{sql_quote('2025-01-15 00:00:00')},{sql_quote(metadata_xml)},"
        f"{removed},{sql_quote('2025-01-01 00:00:00.000000')},"
        f"{sql_quote(modified + '.000000')},{identifier})"
    )


def dump(version: str, rows: list[str]) -> str:
    return (
        SCHEMA
        + "\nINSERT INTO `records` VALUES\n  "
        + ",\n  ".join(rows)
        + ";\n\n"
        + f"-- Dump completed on {version}  1:00:00\n"
    )


def fixture_rows(
    *,
    alpha_title: str = "Alpha Study",
    mirror_removed: bool = False,
    beta_doi: str | None = None,
    epsilon_removed: bool = False,
    include_zeta: bool = False,
    include_epsilon: bool = True,
) -> list[str]:
    rows = [
        record_row(
            record_id=1,
            context_id=1,
            identifier=101,
            metadata_xml=metadata(
                oai_identifier="oai:one:article/101",
                title=alpha_title,
                creator="Alice Author",
                published="2025-01-15",
                url="https://one.example/index.php/journal/article/view/101",
                doi="10.1234/alpha",
            ),
            modified="2026-07-01 00:00:00"
            if alpha_title != "Alpha Study"
            else "2026-01-01 00:00:00",
        ),
        record_row(
            record_id=2,
            context_id=2,
            identifier=201,
            metadata_xml=metadata(
                oai_identifier="oai:mirror:article/201",
                title="Alpha Study",
                creator="Alice Author",
                published="2025-01-15",
                url="https://mirror.example/index.php/journal/article/view/201",
                doi="10.1234/alpha",
            ),
            modified="2026-07-01 00:00:00"
            if mirror_removed
            else "2026-01-01 00:00:00",
            removed_at="2026-07-01 00:00:00" if mirror_removed else None,
        ),
        record_row(
            record_id=3,
            context_id=1,
            identifier=102,
            metadata_xml=metadata(
                oai_identifier="oai:one:article/102",
                title="Beta Study",
                creator="Bob Author",
                published="2025-02-15",
                url="https://one.example/index.php/journal/article/view/102",
                doi=beta_doi,
            ),
            modified="2026-07-01 00:00:00"
            if beta_doi
            else "2026-01-01 00:00:00",
        ),
        record_row(
            record_id=4,
            context_id=2,
            identifier=202,
            metadata_xml=metadata(
                oai_identifier="oai:mirror:article/202",
                title="Delta Study",
                creator="Dana Author",
                published="2025-03-15",
                url="https://mirror.example/index.php/journal/article/view/202",
                doi="10.1234/delta",
            ),
            modified="2026-01-01 00:00:00",
        ),
    ]
    if include_epsilon:
        rows.append(
            record_row(
                record_id=5,
                context_id=1,
                identifier=103,
                metadata_xml=metadata(
                    oai_identifier="oai:one:article/103",
                    title="Epsilon Study",
                    creator="Erin Author",
                    published="2025-04-15",
                    url=(
                        "https://one.example/index.php/journal/article/view/103/"
                        + ("x" * 2100)
                    ),
                ),
                modified="2026-07-01 00:00:00"
                if epsilon_removed
                else (
                    "2026-08-01 00:00:00"
                    if include_zeta
                    else "2026-01-01 00:00:00"
                ),
                removed_at=(
                    "2026-07-01 00:00:00" if epsilon_removed else None
                ),
            )
        )
    if include_zeta:
        rows.append(
            record_row(
                record_id=6,
                context_id=1,
                identifier=104,
                metadata_xml=metadata(
                    oai_identifier="oai:one:article/104",
                    title="Zeta Study",
                    creator="Zoe Author",
                    published="2026-07-15",
                    url="https://one.example/index.php/journal/article/view/104",
                ),
                modified="2026-07-01 00:00:00",
            )
        )
    return rows


JANUARY_DUMP = dump("2026-01-01", fixture_rows())
JULY_DUMP = dump(
    "2026-07-01",
    fixture_rows(
        alpha_title="Alpha Study Revised",
        mirror_removed=True,
        beta_doi="10.1234/delta",
        epsilon_removed=True,
        include_zeta=True,
    ),
)
AUGUST_DUMP = dump(
    "2026-08-01",
    fixture_rows(
        alpha_title="Alpha Study Revised",
        mirror_removed=True,
        beta_doi="10.1234/delta",
        epsilon_removed=False,
        include_zeta=False,
    ),
)

CHAIN_DUMP = dump(
    "2026-01-01",
    [
        record_row(
            record_id=10,
            context_id=1,
            identifier=110,
            metadata_xml=metadata(
                oai_identifier="oai:chain:10",
                title="Chain Article Number Ten",
                creator="Author Ten",
                published="2025-01-15",
                url="https://chain.example/index.php/journal/article/view/10",
                doi="10.1234/chain-a",
            ),
            modified="2026-01-01 00:00:00",
        ),
        record_row(
            record_id=11,
            context_id=1,
            identifier=111,
            metadata_xml=metadata(
                oai_identifier="oai:chain:11",
                title="Chain Article Number Eleven",
                creator="Author Eleven",
                published="2025-02-15",
                url=(
                    "https://chain.example/index.php/journal/article/view/"
                    "shared-11-12"
                ),
                doi="10.1234/chain-a",
            ),
            modified="2026-01-01 00:00:00",
        ),
        record_row(
            record_id=12,
            context_id=1,
            identifier=112,
            metadata_xml=metadata(
                oai_identifier="oai:chain:12",
                title="Chain Shared Fingerprint Twelve Thirteen",
                creator="Shared Chain Author",
                published="2025-03-15",
                url=(
                    "https://chain.example/index.php/journal/article/view/"
                    "shared-11-12"
                ),
                doi="10.1234/chain-b",
            ),
            modified="2026-01-01 00:00:00",
        ),
        record_row(
            record_id=13,
            context_id=1,
            identifier=113,
            metadata_xml=metadata(
                oai_identifier="oai:chain:shared-13-14",
                title="Chain Shared Fingerprint Twelve Thirteen",
                creator="Shared Chain Author",
                published="2025-03-15",
                url="https://chain.example/index.php/journal/article/view/13",
                doi="10.1234/chain-c",
            ),
            modified="2026-01-01 00:00:00",
        ),
        record_row(
            record_id=14,
            context_id=1,
            identifier=114,
            metadata_xml=metadata(
                oai_identifier="oai:chain:shared-13-14",
                title="Chain Article Number Fourteen",
                creator="Author Fourteen",
                published="2025-04-15",
                url="https://chain.example/index.php/journal/article/view/14",
                doi="10.1234/chain-d",
            ),
            modified="2026-01-01 00:00:00",
        ),
    ],
)


class PipelineUnitTest(unittest.TestCase):
    def test_compressed_snapshot_has_logical_size_hash_and_one_history_entry(self):
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            plain = raw / "pkpbeacon-2026-01-01.sql"
            archive = raw / "pkpbeacon-2026-01-01.sql.gz"
            content = JANUARY_DUMP.encode("utf-8")
            plain.write_bytes(content)
            archive.write_bytes(gzip.compress(content))
            snapshot = run_pipeline.inspect_snapshot(archive)
            self.assertEqual(snapshot.size, len(content))
            self.assertEqual(
                run_pipeline.snapshot_sha256(snapshot), hashlib.sha256(content).hexdigest()
            )
            self.assertEqual(run_pipeline.discover_snapshots(raw), [snapshot])
            self.assertFalse((raw / "pkpbeacon-2026-01-01.sql.part").exists())

    def test_compact_ddl_changes_only_records_storage_and_bounds_large_inserts(self):
        content = JANUARY_DUMP.encode("utf-8") + b"INSERT INTO `records` VALUES ('" + b"x" * 200000 + b"');\n"
        source = io.BytesIO(content)
        transformer = run_pipeline.RawRecordsDDL()
        output = bytearray()
        while chunk := source.readline(64 * 1024):
            output.extend(transformer.transform(chunk))
        transformer.finish()
        marker = b" ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
        self.assertEqual(output.count(marker), 1)
        self.assertEqual(bytes(output).replace(marker, b""), content)

    def test_compact_ddl_rejects_unexpected_records_engine(self):
        transformer = run_pipeline.RawRecordsDDL()
        transformer.transform(b"CREATE TABLE `records` (\n")
        with self.assertRaisesRegex(run_pipeline.PipelineError, "unsupported records"):
            transformer.transform(b") ENGINE=MyISAM;\n")

    def test_snapshot_timestamp_must_match_filename(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "pkpbeacon-2026-07-01.sql"
            path.write_text(JULY_DUMP, encoding="utf-8")
            snapshot = run_pipeline.inspect_snapshot(path)
            self.assertEqual(snapshot.version, "2026-07-01")
            self.assertEqual(
                snapshot.completed_at.isoformat(),
                "2026-07-01T01:00:00",
            )

            mismatched = Path(directory) / "pkpbeacon-2026-08-01.sql"
            mismatched.write_text(JULY_DUMP, encoding="utf-8")
            with self.assertRaisesRegex(
                run_pipeline.PipelineError,
                "does not match",
            ):
                run_pipeline.inspect_snapshot(mismatched)

    def test_sql_string_escapes_password_characters(self):
        self.assertEqual(
            run_pipeline.sql_string("a'b\\c"),
            "'a''b\\\\c'",
        )

    def test_pending_history_starts_after_latest_clean_export(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            clean = root / "clean"
            raw.mkdir()
            clean.mkdir()
            for version, content in (
                ("2026-01-01", JANUARY_DUMP),
                ("2026-07-01", JULY_DUMP),
                ("2026-08-01", AUGUST_DUMP),
            ):
                (raw / f"pkpbeacon-{version}.sql").write_text(
                    content,
                    encoding="utf-8",
                )
            create_complete_release(clean, "2026-01-01")
            target = run_pipeline.inspect_snapshot(
                raw / "pkpbeacon-2026-08-01.sql"
            )
            selected = run_pipeline.snapshots_to_process(
                target=target,
                raw_dir=raw,
                clean_dir=clean,
                explicit_snapshot=False,
                rebuild_history=False,
            )
            self.assertEqual(
                [item.version for item in selected],
                ["2026-07-01", "2026-08-01"],
            )

    def test_current_snapshot_is_noop_unless_verification_is_requested(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            clean = root / "clean"
            raw.mkdir()
            clean.mkdir()
            snapshot_path = raw / "pkpbeacon-2026-01-01.sql"
            snapshot_path.write_text(JANUARY_DUMP, encoding="utf-8")
            create_complete_release(clean, "2026-01-01")
            target = run_pipeline.inspect_snapshot(snapshot_path)

            self.assertEqual(
                run_pipeline.snapshots_to_process(
                    target=target,
                    raw_dir=raw,
                    clean_dir=clean,
                    explicit_snapshot=False,
                    rebuild_history=False,
                ),
                [],
            )
            self.assertEqual(
                run_pipeline.snapshots_to_process(
                    target=target,
                    raw_dir=raw,
                    clean_dir=clean,
                    explicit_snapshot=False,
                    rebuild_history=False,
                    verify_existing=True,
                ),
                [target],
            )

    def test_build_sql_has_parallel_metadata_phase(self):
        project_root = Path(__file__).parents[1]
        parts = run_pipeline.split_build_sql(
            project_root / "sql" / "01_build_ojs_tables.sql"
        )
        self.assertIn("CREATE TABLE ojs_stage_source_index", parts.prefix)
        self.assertIn("INSERT INTO ojs_stage_sources", parts.metadata)
        self.assertIn("@ojs_metadata_min_id", parts.metadata)
        self.assertIn("CREATE TABLE ojs_stage_keys", parts.suffix)

    def test_source_id_ranges_cover_bounds_without_overlap(self):
        ranges = run_pipeline.source_id_ranges(10, 22, 4)
        self.assertEqual(ranges, [(10, 13), (14, 17), (18, 21), (22, 22)])
        flattened = [
            source_id
            for minimum, maximum in ranges
            for source_id in range(minimum, maximum + 1)
        ]
        self.assertEqual(flattened, list(range(10, 23)))

    def test_change_report_counts_additions_removals_and_merges(self):
        class FakeServer:
            def execute(self, sql, **kwargs):
                if "active_article_count" in sql:
                    return "2026-01-01\t8\t7\t6\t5"
                if "GROUP BY event_type" in sql:
                    return "\n".join(
                        (
                            "added\tupsert\t2",
                            "merged\tdelete\t1",
                            "modified\tupsert\t1",
                            "removed\tdelete\t1",
                            "restored\tupsert\t1",
                        )
                    )
                return (
                    "2026-01-01\t12\t10\t9\t8\t2\t3\t2\t1\t1\t1\t1\t100\t105"
                )

        snapshot = run_pipeline.Snapshot(
            path=Path("pkpbeacon-2026-07-01.sql"),
            version="2026-07-01",
            completed_at=datetime(2026, 7, 1, 1),
            size=123,
        )
        counts = run_pipeline.BuildCounts(
            source_records=10,
            active_sources=8,
            articles=7,
            active_articles=5,
            removed_articles=1,
            merged_articles=1,
            events=6,
        )
        report = run_pipeline.collect_change_report(
            FakeServer(),
            database="fixture",
            password=None,
            snapshot=snapshot,
            source_sha256="a" * 64,
            build_sql_sha256="b" * 64,
            counts=counts,
        )
        self.assertEqual(report["source_rows"]["added"], 2)
        self.assertEqual(report["source_rows"]["missing_from_snapshot"], 1)
        self.assertEqual(
            report["article_events"]["by_type"],
            {
                "added": 2,
                "modified": 1,
                "removed": 1,
                "restored": 1,
                "merged": 1,
            },
        )
        self.assertEqual(report["article_events"]["by_operation"]["delete"], 2)


@unittest.skipUnless(
    all(
        shutil.which(name)
        for name in ("mysqld", "mysql", "mysqladmin", "mysqldump")
    ),
    "MySQL 8 client and server binaries are required",
)
class PipelineIntegrationTest(unittest.TestCase):
    def test_force_resume_replaces_staging_without_checkpoint(self):
        project_root = Path(__file__).parents[1]
        with TemporaryDirectory(
            dir=project_root,
            prefix=".pipeline-missing-checkpoint-test-",
        ) as directory:
            root = Path(directory)
            clean_dir = root / "clean"
            staging_dir = clean_dir / "mysql-2026-01-01.building"
            staging_dir.mkdir(parents=True)
            (staging_dir / "uncheckpointed-file").write_text(
                "disposable",
                encoding="ascii",
            )
            snapshot_path = root / "pkpbeacon-2026-01-01.sql"
            snapshot_path.write_text(JANUARY_DUMP, encoding="utf-8")

            env = os.environ.copy()
            env["MYSQL_ROOT_PASSWORD"] = "fixture-password"
            env["OJS_DB_API_PASSWORD"] = "fixture-api-password"
            result = subprocess.run(
                [
                    sys.executable,
                    str(project_root / "src" / "run_pipeline.py"),
                    "--snapshot",
                    str(snapshot_path),
                    "--clean-dir",
                    str(clean_dir),
                    "--force-rebuild",
                    "--resume-building",
                    "--mysql-buffer-pool-size",
                    "128M",
                    "--metadata-workers",
                    "2",
                    "--progress-seconds",
                    "1",
                ],
                cwd=project_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=180,
            )

            self.assertEqual(
                result.returncode,
                0,
                result.stdout + result.stderr,
            )
            self.assertIn(
                "without a valid checkpoint",
                result.stdout,
            )
            self.assertTrue(
                (clean_dir / "mysql-2026-01-01").is_dir()
            )
            self.assertFalse(
                (clean_dir / "mysql-2026-01-01" / "uncheckpointed-file")
                .exists()
            )

    def test_identity_frontier_converges_across_long_key_chain(self):
        project_root = Path(__file__).parents[1]
        with TemporaryDirectory(
            dir=project_root,
            prefix=".pipeline-chain-test-",
        ) as directory:
            root = Path(directory)
            clean_dir = root / "clean"
            snapshot_path = root / "pkpbeacon-2026-01-01.sql"
            snapshot_path.write_text(CHAIN_DUMP, encoding="utf-8")

            env = os.environ.copy()
            env["MYSQL_ROOT_PASSWORD"] = "fixture-password"
            env["OJS_DB_API_PASSWORD"] = "fixture-api-password"
            result = subprocess.run(
                [
                    sys.executable,
                    str(project_root / "src" / "run_pipeline.py"),
                    "--snapshot",
                    str(snapshot_path),
                    "--clean-dir",
                    str(clean_dir),
                    "--mysql-buffer-pool-size",
                    "128M",
                    "--metadata-workers",
                    "2",
                    "--progress-seconds",
                    "1",
                ],
                cwd=project_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=180,
            )
            self.assertEqual(
                result.returncode,
                0,
                result.stdout + result.stderr,
            )

            progress = [
                json.loads(line.removeprefix("[progress] "))
                for line in result.stderr.splitlines()
                if line.startswith("[progress] ")
            ]
            passes = [event for event in progress if event["event"] == "dedup-pass"]
            self.assertGreaterEqual(len(passes), 3)
            self.assertEqual(
                [event["pass_number"] for event in passes],
                list(range(1, len(passes) + 1)),
            )
            self.assertGreater(passes[0]["changed_rows"], 0)
            self.assertEqual(passes[-1]["changed_rows"], 0)
            self.assertIn("source_indexes", [event.get("step") for event in progress])
            extracted = [event["extracted_rows"] for event in progress if event["event"] == "metadata-rows"]
            self.assertEqual(sum(extracted), 5)
            completed = [
                event for event in progress
                if event["stage"] == "metadata-shard" and event["event"] == "completed"
            ]
            self.assertEqual(len(completed), len(extracted))
            self.assertNotIn("fixture-password", result.stderr)
            self.assertNotIn("<dc:", result.stderr)

            server = run_pipeline.MySQLServer(
                run_pipeline.MySQLTools.discover(),
                clean_dir / "mysql-2026-01-01",
                "128M",
            )
            try:
                server.start("fixture-password")
                source_assignments = server.execute(
                    """
                    SELECT source_record_id, article_id
                    FROM ojs_article_sources
                    ORDER BY source_record_id;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                articles = server.execute(
                    """
                    SELECT article_id, status, source_count
                    FROM ojs_articles
                    ORDER BY article_id;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                source_index_count = server.execute(
                    """
                    SELECT COUNT(DISTINCT index_name)
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = 'ojs_article_sources'
                      AND index_name <> 'PRIMARY';
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                server.execute(
                    """
                    ALTER TABLE ojs_article_sources
                        DROP INDEX idx_ojs_sources_oai_key,
                        DROP INDEX idx_ojs_sources_doi_key,
                        DROP INDEX idx_ojs_sources_url_key,
                        DROP INDEX idx_ojs_sources_fingerprint_key,
                        ADD INDEX idx_ojs_sources_oai_key (oai_key_hash),
                        ADD INDEX idx_ojs_sources_doi_key (doi_key_hash),
                        ADD INDEX idx_ojs_sources_url_key (url_key_hash),
                        ADD INDEX idx_ojs_sources_fingerprint_key (
                            fingerprint_key_hash
                        );
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                build_sql = (
                    project_root / "sql" / "01_build_ojs_tables.sql"
                ).read_text(encoding="utf-8")
                migration_start = build_sql.index(
                    "DROP PROCEDURE IF EXISTS ojs_ensure_source_indexes;"
                )
                migration_marker = (
                    "DROP PROCEDURE ojs_ensure_source_indexes;"
                )
                migration_end = (
                    build_sql.index(migration_marker, migration_start)
                    + len(migration_marker)
                )
                server.run_sql_text(
                    build_sql[migration_start:migration_end],
                    label="legacy source-index migration",
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                migrated_hash_indexes = server.execute(
                    """
                    SELECT
                        index_name,
                        GROUP_CONCAT(
                            column_name
                            ORDER BY seq_in_index
                            SEPARATOR ','
                        )
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = 'ojs_article_sources'
                      AND index_name IN (
                          'idx_ojs_sources_oai_key',
                          'idx_ojs_sources_doi_key',
                          'idx_ojs_sources_url_key',
                          'idx_ojs_sources_fingerprint_key'
                      )
                    GROUP BY index_name
                    ORDER BY index_name;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
            finally:
                server.shutdown("fixture-password")

            self.assertEqual(
                source_assignments.splitlines(),
                [f"{source_id}\t10" for source_id in range(10, 15)],
            )
            self.assertEqual(articles.splitlines(), ["10\tactive\t5"])
            self.assertEqual(source_index_count, "7")
            self.assertEqual(
                migrated_hash_indexes.splitlines(),
                [
                    "idx_ojs_sources_doi_key\tdoi_key_hash,article_id",
                    (
                        "idx_ojs_sources_fingerprint_key\t"
                        "fingerprint_key_hash,article_id"
                    ),
                    "idx_ojs_sources_oai_key\toai_key_hash,article_id",
                    "idx_ojs_sources_url_key\turl_key_hash,article_id",
                ],
            )

    def test_corrected_keys_stable_ids_and_alias_provenance(self):
        project_root = Path(__file__).parents[1]

        def identity_rows(
            version: str,
            *,
            include_alias: bool,
            include_old_key_reuser: bool,
            touch_noncanonical: bool = False,
            reuse_source_identity: bool = False,
        ) -> list[str]:
            rows = [
                record_row(
                    record_id=10,
                    context_id=2 if reuse_source_identity else 1,
                    identifier=999 if reuse_source_identity else 110,
                    metadata_xml=metadata(
                        oai_identifier="oai:identity:10",
                        title="Original Entity",
                        creator="Original Author",
                        published="2025-01-15",
                        url=(
                            "https://one.example/index.php/journal/"
                            "article/view/10"
                        ),
                        doi=(
                            "10.1234/corrected"
                            if include_alias
                            else "10.1234/retired"
                        ),
                    ),
                    modified=(
                        f"{version} 00:00:00"
                        if include_alias or touch_noncanonical
                        else "2026-01-01 00:00:00"
                    ),
                ),
                record_row(
                    record_id=20,
                    context_id=1,
                    identifier=120,
                    metadata_xml=metadata(
                        oai_identifier="oai:identity:20",
                        title="Independent Entity",
                        creator="Independent Author",
                        published="2025-02-15",
                        url=(
                            "https://one.example/index.php/journal/"
                            "article/view/20"
                        ),
                        doi="10.1234/independent",
                    ),
                    modified="2026-01-01 00:00:00",
                ),
            ]
            if include_alias:
                rows.append(
                    record_row(
                        record_id=5,
                        context_id=2,
                        identifier=205,
                        metadata_xml=metadata(
                            oai_identifier="oai:identity:5",
                            title="Canonical Mirror Entity",
                            creator="Mirror Author",
                            published="2025-01-15",
                            url=(
                                "https://mirror.example/index.php/journal/"
                                "article/view/5"
                            ),
                            doi="10.1234/corrected",
                        ),
                        modified="2026-07-01 00:00:00",
                    )
                )
            if include_old_key_reuser:
                rows.append(
                    record_row(
                        record_id=4,
                        context_id=1,
                        identifier=104,
                        metadata_xml=metadata(
                            oai_identifier="oai:identity:4",
                            title="Unrelated Retired Key Reuser",
                            creator="Different Author",
                            published="2025-03-15",
                            url=(
                                "https://one.example/index.php/journal/"
                                "article/view/4"
                            ),
                            doi="10.1234/retired",
                        ),
                        modified="2026-08-01 00:00:00",
                    )
                )
            return rows

        with TemporaryDirectory(
            dir=project_root,
            prefix=".pipeline-identity-regression-test-",
        ) as directory:
            root = Path(directory)
            clean_dir = root / "clean"
            snapshots = []
            cases = (
                ("2026-01-01", False, False, False),
                ("2026-07-01", True, False, False),
                ("2026-08-01", True, True, False),
                ("2026-09-01", True, True, True),
            )
            for version, include_alias, include_reuser, touch_alias in cases:
                path = root / f"pkpbeacon-{version}.sql"
                path.write_text(
                    dump(
                        version,
                        identity_rows(
                            version,
                            include_alias=include_alias,
                            include_old_key_reuser=include_reuser,
                            touch_noncanonical=touch_alias,
                        ),
                    ),
                    encoding="utf-8",
                )
                snapshots.append(path)

            env = os.environ.copy()
            env["MYSQL_ROOT_PASSWORD"] = "fixture-password"
            env["OJS_DB_API_PASSWORD"] = "fixture-api-password"
            for path in snapshots:
                result = subprocess.run(
                    [
                        sys.executable,
                        str(project_root / "src" / "run_pipeline.py"),
                        "--snapshot",
                        str(path),
                        "--clean-dir",
                        str(clean_dir),
                        "--mysql-buffer-pool-size",
                        "128M",
                        "--metadata-workers",
                        "2",
                        "--allow-anomalous-release",
                        "--progress-seconds",
                        "1",
                    ],
                    cwd=project_root,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=180,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    result.stdout + result.stderr,
                )

            datadir = clean_dir / "mysql-2026-09-01"
            server = run_pipeline.MySQLServer(
                run_pipeline.MySQLTools.discover(),
                datadir,
                "128M",
            )
            try:
                server.start("fixture-password")
                assignments = server.execute(
                    """
                    SELECT source_record_id, article_id
                    FROM ojs_article_sources
                    ORDER BY source_record_id;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                identity_keys = server.execute(
                    """
                    SELECT key_value, article_id
                    FROM ojs_article_keys
                    WHERE key_type = 'doi'
                      AND key_value IN (
                          '10.1234/corrected',
                          '10.1234/retired'
                      )
                    ORDER BY key_value;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                september_event = server.execute(
                    """
                    SELECT event_type
                    FROM ojs_article_events
                    WHERE snapshot_date = '2026-09-01'
                      AND article_id = 10;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                canonical_source = server.execute(
                    """
                    SELECT canonical_source_record_id
                    FROM ojs_articles
                    WHERE article_id = 10;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
            finally:
                server.shutdown("fixture-password")

            self.assertEqual(
                assignments.splitlines(),
                ["4\t4", "5\t10", "10\t10", "20\t20"],
            )
            self.assertEqual(
                identity_keys.splitlines(),
                ["10.1234/corrected\t10", "10.1234/retired\t4"],
            )
            self.assertEqual(canonical_source, "5")
            self.assertEqual(september_event, "modified")

            reused = root / "pkpbeacon-2026-10-01.sql"
            reused.write_text(
                dump(
                    "2026-10-01",
                    identity_rows(
                        "2026-10-01",
                        include_alias=True,
                        include_old_key_reuser=True,
                        reuse_source_identity=True,
                    ),
                ),
                encoding="utf-8",
            )
            failed = subprocess.run(
                [
                    sys.executable,
                    str(project_root / "src" / "run_pipeline.py"),
                    "--snapshot",
                    str(reused),
                    "--clean-dir",
                    str(clean_dir),
                    "--mysql-buffer-pool-size",
                    "128M",
                    "--metadata-workers",
                    "2",
                    "--allow-anomalous-release",
                    "--progress-seconds",
                    "1",
                ],
                cwd=project_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=180,
            )
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn(
                "source record ID was reused with a different identity tuple",
                failed.stdout + failed.stderr,
            )

    def test_noisy_key_threshold_counts_retained_aliases(self):
        project_root = Path(__file__).parents[1]

        def common_fingerprint_row(record_id: int, modified: str) -> str:
            return record_row(
                record_id=record_id,
                context_id=1,
                identifier=1000 + record_id,
                metadata_xml=metadata(
                    oai_identifier=f"oai:noisy:{record_id}",
                    title="A Deliberately Repeated Generic Article Title",
                    creator="Repeated Author",
                    published="2025-01-15",
                    url=(
                        "https://one.example/index.php/journal/article/view/"
                        f"noisy-{record_id}"
                    ),
                    doi=f"10.9999/noisy-{record_id}",
                ),
                modified=modified,
            )

        with TemporaryDirectory(
            dir=project_root,
            prefix=".pipeline-noisy-key-test-",
        ) as directory:
            root = Path(directory)
            clean_dir = root / "clean"
            january = root / "pkpbeacon-2026-01-01.sql"
            july = root / "pkpbeacon-2026-07-01.sql"
            january.write_text(
                dump(
                    "2026-01-01",
                    [common_fingerprint_row(50, "2026-01-01 00:00:00")],
                ),
                encoding="utf-8",
            )
            july.write_text(
                dump(
                    "2026-07-01",
                    [common_fingerprint_row(50, "2026-01-01 00:00:00")]
                    + [
                        common_fingerprint_row(
                            record_id,
                            "2026-07-01 00:00:00",
                        )
                        for record_id in range(1, 26)
                    ],
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["MYSQL_ROOT_PASSWORD"] = "fixture-password"
            env["OJS_DB_API_PASSWORD"] = "fixture-api-password"
            for path in (january, july):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(project_root / "src" / "run_pipeline.py"),
                        "--snapshot",
                        str(path),
                        "--clean-dir",
                        str(clean_dir),
                        "--mysql-buffer-pool-size",
                        "128M",
                        "--metadata-workers",
                        "2",
                        "--allow-anomalous-release",
                        "--progress-seconds",
                        "1",
                    ],
                    cwd=project_root,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=180,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    result.stdout + result.stderr,
                )

            server = run_pipeline.MySQLServer(
                run_pipeline.MySQLTools.discover(),
                clean_dir / "mysql-2026-07-01",
                "128M",
            )
            try:
                server.start("fixture-password")
                counts = server.execute(
                    """
                    SELECT
                        COUNT(DISTINCT article_id),
                        (
                            SELECT COUNT(*)
                            FROM ojs_articles
                            WHERE status = 'active'
                        ),
                        (
                            SELECT COUNT(*)
                            FROM ojs_article_keys
                            WHERE key_type = 'fingerprint'
                        )
                    FROM ojs_article_sources;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
            finally:
                server.shutdown("fixture-password")

            self.assertEqual(counts, "26\t26\t0")

    def test_resume_from_metadata_ready_checkpoint(self):
        project_root = Path(__file__).parents[1]
        build_sql = project_root / "sql" / "01_build_ojs_tables.sql"
        with TemporaryDirectory(
            dir=project_root,
            prefix=".pipeline-resume-test-",
        ) as directory:
            root = Path(directory)
            clean_dir = root / "clean"
            snapshot_path = root / "pkpbeacon-2026-01-01.sql"
            snapshot_path.write_text(JANUARY_DUMP, encoding="utf-8")
            snapshot = run_pipeline.inspect_snapshot(snapshot_path)
            staging_dir = clean_dir / "mysql-2026-01-01.building"
            tools = run_pipeline.MySQLTools.discover()
            server = run_pipeline.MySQLServer(tools, staging_dir, "128M")
            source_sha256 = ""
            try:
                server.initialize()
                server.start(None)
                run_pipeline.create_database(server, "pkpbeacon_db")
                source_sha256 = server.import_snapshot(
                    snapshot,
                    database="pkpbeacon_db",
                    password=None,
                    progress_seconds=1,
                )
                build_sql_sha256 = run_pipeline.sha256_small_file(build_sql)
                parts = run_pipeline.split_build_sql(build_sql)
                server.run_sql_text(
                    parts.prefix,
                    label="resume fixture source-index phase",
                    database="pkpbeacon_db",
                    password=None,
                    prelude=run_pipeline.build_sql_prelude(
                        snapshot=snapshot,
                        source_sha256=source_sha256,
                        build_sql_sha256=build_sql_sha256,
                        mysql_version=tools.version,
                        full_rescan=False,
                    ),
                )
                run_pipeline.write_build_state(
                    staging_dir,
                    snapshot=snapshot,
                    source_sha256=source_sha256,
                    build_sql_sha256=build_sql_sha256,
                    phase="metadata_ready",
                )
            finally:
                server.shutdown(None)

            env = os.environ.copy()
            env["MYSQL_ROOT_PASSWORD"] = "fixture-password"
            env["OJS_DB_API_PASSWORD"] = "fixture-api-password"
            result = subprocess.run(
                [
                    sys.executable,
                    str(project_root / "src" / "run_pipeline.py"),
                    "--snapshot",
                    str(snapshot_path),
                    "--clean-dir",
                    str(clean_dir),
                    "--resume-building",
                    "--metadata-workers",
                    "2",
                    "--mysql-buffer-pool-size",
                    "128M",
                    "--progress-seconds",
                    "1",
                ],
                cwd=project_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=180,
            )
            self.assertEqual(
                result.returncode,
                0,
                result.stdout + result.stderr,
            )
            self.assertTrue(
                (clean_dir / "mysql-2026-01-01").is_dir()
            )
            self.assertFalse(
                (clean_dir / "mysql-2026-01-01" / run_pipeline.BUILD_STATE_NAME)
                .exists()
            )

    def test_temporal_history_add_modify_merge_remove_restore(self):
        project_root = Path(__file__).parents[1]
        july_rows = fixture_rows(
            alpha_title="Alpha Study Revised",
            mirror_removed=True,
            beta_doi="10.1234/delta",
            epsilon_removed=True,
            include_zeta=True,
        )
        # A publisher can correct XML without changing either source timestamp.
        # The content hash must still produce the same modification event.
        july_rows[0] = july_rows[0].replace(
            "2026-07-01 00:00:00", "2026-01-01 00:00:00"
        )
        august_rows = fixture_rows(
            alpha_title="Alpha Study Revised",
            mirror_removed=True,
            beta_doi="10.1234/delta",
            epsilon_removed=False,
            include_zeta=False,
        )
        august_rows[0] = august_rows[0].replace(
            "2026-07-01 00:00:00", "2026-01-01 00:00:00"
        )
        with TemporaryDirectory(
            dir=project_root,
            prefix=".pipeline-test-",
        ) as directory:
            root = Path(directory)
            clean_dir = root / "clean"
            snapshots = []
            for version, content in (
                ("2026-01-01", JANUARY_DUMP),
                ("2026-07-01", dump("2026-07-01", july_rows)),
                ("2026-08-01", dump("2026-08-01", august_rows)),
            ):
                path = root / f"pkpbeacon-{version}.sql.gz"
                path.write_bytes(gzip.compress(content.encode("utf-8")))
                snapshots.append(path)

            env = os.environ.copy()
            env["MYSQL_ROOT_PASSWORD"] = "fixture-password"
            env["OJS_DB_API_PASSWORD"] = "fixture-api-password"
            for path in snapshots:
                result = subprocess.run(
                    [
                        sys.executable,
                        str(project_root / "src" / "run_pipeline.py"),
                        "--snapshot",
                        str(path),
                        "--clean-dir",
                        str(clean_dir),
                        "--mysql-buffer-pool-size",
                        "128M",
                        "--metadata-workers",
                        "2",
                        "--allow-anomalous-release",
                        "--compact-storage",
                        "--progress-seconds",
                        "1",
                    ],
                    cwd=project_root,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=180,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    result.stdout + result.stderr,
                )

            self.assertTrue(
                (clean_dir / "pkpbeacon-clean-2026-01-01.sql.gz").is_file()
            )
            self.assertTrue(
                (clean_dir / "pkpbeacon-clean-2026-08-01.sql.gz.sha256").is_file()
            )
            self.assertEqual(
                (clean_dir / "pkpbeacon-clean-latest.sql.gz").resolve(),
                clean_dir / "pkpbeacon-clean-2026-08-01.sql.gz",
            )

            datadir = clean_dir / "mysql-2026-08-01"
            marker = json.loads((datadir / "OJS_COMPACT_RELEASE.json").read_text())
            self.assertTrue(marker["clean_only"])
            self.assertEqual(marker["data_directory_name"], datadir.name)
            server = run_pipeline.MySQLServer(
                run_pipeline.MySQLTools.discover(),
                datadir,
                "128M",
            )
            try:
                server.start("fixture-password")
                with self.assertRaisesRegex(run_pipeline.PipelineError, "still using|still has"):
                    run_pipeline.assert_database_stopped(datadir)
                table_rows = server.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = DATABASE();",
                    database="pkpbeacon_db",
                    password="fixture-password",
                ).splitlines()
                self.assertEqual(
                    set(table_rows),
                    set(run_pipeline.CLEAN_EXPORT_TABLES) | {"ojs_pipeline_metadata"},
                )
                row_format = server.execute(
                    "SELECT row_format FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = 'ojs_articles';",
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                self.assertEqual(row_format.lower(), "compressed")
                articles = server.execute(
                    """
                    SELECT
                        article_id,
                        status,
                        COALESCE(merged_into_article_id, 0),
                        version_number,
                        DATE_FORMAT(date_added, '%Y-%m-%d'),
                        DATE_FORMAT(date_modified, '%Y-%m-%d'),
                        COALESCE(DATE_FORMAT(date_removed, '%Y-%m-%d'), '')
                    FROM ojs_articles
                    ORDER BY article_id;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
                events = server.execute(
                    """
                    SELECT
                        DATE_FORMAT(snapshot_date, '%Y-%m-%d'),
                        article_id,
                        event_type,
                        operation,
                        COALESCE(redirect_to_article_id, 0)
                    FROM ojs_article_events
                    ORDER BY event_id;
                    """,
                    database="pkpbeacon_db",
                    password="fixture-password",
                )
            finally:
                server.shutdown("fixture-password")

            run_pipeline.verify_existing_database(
                snapshot=run_pipeline.inspect_snapshot(snapshots[-1]),
                datadir=datadir,
                build_sql=project_root / "sql" / "01_build_ojs_tables.sql",
                database="pkpbeacon_db",
                root_password="fixture-password",
                buffer_pool_size="128M",
                progress_seconds=1,
                verify_source_checksum=True,
            )

            article_rows = articles.splitlines()
            self.assertIn("1\tactive\t0\t2\t2026-01-01\t2026-07-01\t", article_rows)
            self.assertIn("3\tactive\t0\t2\t2026-01-01\t2026-07-01\t", article_rows)
            self.assertIn(
                "4\tmerged\t3\t2\t2026-01-01\t2026-07-01\t2026-07-01",
                article_rows,
            )
            self.assertIn("5\tactive\t0\t3\t2026-01-01\t2026-08-01\t", article_rows)
            self.assertIn(
                "6\tremoved\t0\t2\t2026-07-01\t2026-08-01\t2026-08-01",
                article_rows,
            )

            event_rows = events.splitlines()
            self.assertIn("2026-07-01\t4\tmerged\tdelete\t3", event_rows)
            self.assertIn("2026-07-01\t5\tremoved\tdelete\t0", event_rows)
            self.assertIn("2026-08-01\t5\trestored\tupsert\t0", event_rows)
            self.assertIn("2026-08-01\t6\tremoved\tdelete\t0", event_rows)


if __name__ == "__main__":
    unittest.main()
