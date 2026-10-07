"""Real-MySQL parity checks for the storage-only staging transformation."""
from __future__ import annotations

import hashlib
import io
import json
import shutil
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).parent))
import run_pipeline
import test_pipeline as fixture


PROJECT_ROOT = Path(__file__).parents[1]
BUILD_SQL = PROJECT_ROOT / "sql" / "01_build_ojs_tables.sql"
# Keep this expectation independent of the renderer's own column list.
DEFERRED_FIELDS = (
    "creators", "subjects", "description", "publisher", "published", "types",
    "formats", "identifiers", "source_title", "languages", "relations",
    "coverage", "rights", "metadata_xml",
)
RAW_TABLES = ("records", "contexts", "endpoints", "issns", "versions")
MYSQL_AVAILABLE = all(
    shutil.which(name) for name in ("mysqld", "mysql", "mysqladmin", "mysqldump")
)


def rich_xml(revised: bool = False) -> str:
    # Distinct blocks exercise a real large payload, not just a repeated word.
    description = " ".join(
        hashlib.sha256(f"description-{number}".encode()).hexdigest()
        for number in range(768)
    )
    if revised:
        description += " corrected without updating the publisher timestamp"
    xml = fixture.metadata(
        oai_identifier="oai:long:" + "x" * 1300,
        title="Rich payload – café &amp; methods",
        creator="First Creator",
        published="2025-04-15",
        url="https://one.example/article/150",
        doi="10.1234/rich",
    )
    nodes = {
        "creator": "Second Creator",
        "subject": "Distinct subject",
        "description": description,
        "publisher": "Fixture publisher",
        "format": "application/pdf",
        "source": "Fixture source journal",
        "language": "en",
        "relation": "https://one.example/supplement/150",
        "coverage": "Worldwide",
        "rights": "Creative Commons attribution",
    }
    return xml.replace(
        "</oai_dc:dc>",
        "".join(f"<dc:{name}>{value}</dc:{name}>" for name, value in nodes.items())
        + "</oai_dc:dc>",
    )


def snapshot_rows(index: int) -> list[str]:
    rows = fixture.fixture_rows(
        alpha_title="Alpha Study" if index == 0 else "Alpha Study Revised",
        mirror_removed=index > 0,
        beta_doi=None if index == 0 else "10.1234/delta",
        epsilon_removed=index == 1,
        include_zeta=index == 1,
    )
    if index:
        rows[0] = rows[0].replace("2026-07-01 00:00:00", "2026-01-01 00:00:00")
    # Source 11 is unchanged and therefore absent from July's metadata stage.
    # Removing source 10 must select 11 via the raw-record fallback projection.
    for record_id in (10, 11):
        rows.append(fixture.record_row(
            record_id=record_id,
            context_id=1 if record_id == 10 else 2,
            identifier=1000 + record_id,
            metadata_xml=fixture.metadata(
                oai_identifier=f"oai:fallback:{record_id}",
                title="Canonical preferred longer title" if record_id == 10 else "Fallback",
                creator="Fallback Author",
                published="2025-05-15",
                url=f"https://one.example/fallback/{record_id}",
                doi="10.1234/fallback",
            ),
            modified=("2026-07-01 00:00:00" if index == 1 else "2026-08-01 00:00:00")
            if record_id == 10 and index else "2026-01-01 00:00:00",
            removed_at="2026-07-01 00:00:00" if record_id == 10 and index == 1 else None,
        ))
    rows.append(fixture.record_row(
        record_id=50,
        context_id=1,
        identifier=150,
        metadata_xml=rich_xml(revised=index > 0),
        # The metadata hash, not this unchanged timestamp, detects revision.
        modified="2026-01-01 00:00:00",
    ))
    # A real raw row outside the catalogue's valid-ISSN context scope must
    # still count in the report after compact mode reclaims the raw table.
    rows.append(fixture.record_row(
        record_id=60, context_id=3, identifier=160,
        metadata_xml="<record><metadata>Excluded context</metadata></record>",
        modified="2026-01-01 00:00:00",
    ))
    return rows


def snapshot_dump(version: str, index: int) -> str:
    return fixture.dump(version, snapshot_rows(index)).replace(
        "INSERT INTO `records` VALUES",
        "INSERT INTO `endpoints` VALUES (3,'ojs','https://excluded.example/oai');\n"
        "INSERT INTO `contexts` VALUES (3,3,'Journal without an ISSN');\n"
        "INSERT INTO `records` VALUES",
        1,
    )


def table_columns(server, database: str, table: str) -> list[str]:
    return server.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='{table}' "
        "ORDER BY ORDINAL_POSITION;"
    ).splitlines()


def logical_rows(server, database: str, table: str, columns=None) -> list[str]:
    """Encode every value losslessly, including binary hashes and SQL NULL."""
    columns = columns or table_columns(server, database, table)
    primary_key = server.execute(
        "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='{table}' "
        "AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX;"
    ).splitlines()
    if not columns or not primary_key:
        raise AssertionError(f"missing schema/primary key for {database}.{table}")
    projection = ",".join(
        f"IF(`{column}` IS NULL,'NULL',HEX(CAST(`{column}` AS BINARY)))"
        for column in columns
    )
    order = ",".join(f"`{column}`" for column in primary_key)
    return server.execute(
        f"SELECT {projection} FROM `{table}` ORDER BY {order};", database=database,
    ).splitlines()


@unittest.skipUnless(MYSQL_AVAILABLE, "MySQL 8.4 server and client tools required")
class CompactStorageEquivalenceTests(unittest.TestCase):
    def test_three_snapshot_history_matches_every_clean_table(self):
        with TemporaryDirectory(prefix="ojs-compact-equivalence-") as directory, \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            root = Path(directory)
            tools = run_pipeline.MySQLTools.discover()
            server = run_pipeline.MySQLServer(tools, root / "mysql", "128M")
            databases = ("fixture_standard", "fixture_compact")
            parts = {
                name: run_pipeline.split_build_sql(BUILD_SQL, compact_storage=index == 1)
                for index, name in enumerate(databases)
            }
            build_hash = run_pipeline.sha256_small_file(BUILD_SQL)
            try:
                server.initialize()
                server.start(None)
                for database in databases:
                    run_pipeline.create_database(server, database)
                for index, version in enumerate(("2026-01-01", "2026-07-01", "2026-08-01")):
                    with self.subTest(snapshot=version):
                        path = root / f"pkpbeacon-{version}.sql"
                        path.write_text(snapshot_dump(version, index), encoding="utf-8")
                        snapshot = run_pipeline.inspect_snapshot(path)
                        prelude = ""
                        for database in databases:
                            for table in RAW_TABLES:
                                server.execute(f"DROP TABLE IF EXISTS `{table}`;", database=database)
                            # The single daemon hosts isolated databases; use
                            # each mode's real raw-DDL import transformation.
                            server.compact_storage = database == databases[1]
                            source_hash = server.import_snapshot(
                                snapshot, database=database, password=None, progress_seconds=60,
                            )
                            self.assertEqual(server.execute(
                                "SELECT ROW_FORMAT FROM information_schema.TABLES "
                                f"WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='records';",
                            ) == "Compressed", server.compact_storage)
                            prelude = run_pipeline.build_sql_prelude(
                                snapshot=snapshot, source_sha256=source_hash,
                                build_sql_sha256=build_hash, mysql_version=tools.version,
                                full_rescan=False,
                            )
                            for label, sql in (("index", parts[database].prefix),
                                               ("metadata", parts[database].metadata)):
                                server.run_sql_text(
                                    sql, label=label, database=database, password=None,
                                    prelude=prelude,
                                )

                        retained = [
                            name for name in table_columns(server, databases[0], "ojs_stage_sources")
                            if name not in DEFERRED_FIELDS
                        ]
                        self.assertEqual(
                            logical_rows(server, databases[0], "ojs_stage_sources", retained),
                            logical_rows(server, databases[1], "ojs_stage_sources", retained),
                            "source identities, keys and original XML hashes must be unchanged",
                        )
                        nonnull = " OR ".join(f"`{name}` IS NOT NULL" for name in DEFERRED_FIELDS)
                        self.assertEqual(server.execute(
                            f"SELECT COUNT(*) FROM ojs_stage_sources WHERE {nonnull};",
                            database=databases[1],
                        ), "0")
                        payload_bytes = "+".join(
                            f"COALESCE(OCTET_LENGTH(`{name}`),0)" for name in DEFERRED_FIELDS
                        )
                        sizes = [int(server.execute(
                            f"SELECT COALESCE(SUM({payload_bytes}),0) FROM ojs_stage_sources;",
                            database=database,
                        )) for database in databases]
                        self.assertGreater(sizes[0], sizes[1])
                        self.assertEqual(sizes[1], 0)
                        if index < 2:
                            self.assertGreater(sizes[0], 90_000)
                        if index == 1:
                            for database in databases:
                                self.assertEqual(server.execute(
                                    "SELECT COUNT(*) FROM ojs_stage_sources WHERE source_record_id=11;",
                                    database=database,
                                ), "0", "July must exercise unchanged-source canonical fallback")

                        reports = []
                        for database in databases:
                            server.run_sql_text(
                                parts[database].suffix, label="finalize", database=database,
                                password=None, prelude=prelude,
                            )
                            counts, _ = run_pipeline.validate_database(server, database, None)
                            reports.append(run_pipeline.collect_change_report(
                                server, database=database, password=None, snapshot=snapshot,
                                source_sha256=source_hash, build_sql_sha256=build_hash,
                                counts=counts, compact_storage=database == databases[1],
                            ))
                        raw_count = server.execute("SELECT COUNT(*) FROM records;", database=databases[0])
                        self.assertEqual(int(raw_count), len(snapshot_rows(index)))
                        self.assertEqual(server.execute(
                            "SELECT COUNT(*) FROM records;", database=databases[1],
                        ), "0", "canonical payload must permit reclaiming imported raw records")
                        self.assertEqual(server.execute(
                            "SELECT id,snapshot_date,source_sha256,raw_source_rows,reclaimed "
                            "FROM ojs_pipeline_raw_stats;", database=databases[1],
                        ), f"1\t{version}\t{source_hash}\t{raw_count}\t1")
                        self.assertEqual(reports[0], reports[1], "raw reclamation must not change the report")
                        self.assertEqual(reports[1]["source_rows"]["raw_in_snapshot"], int(raw_count))
                        self.assertEqual(reports[1]["source_rows"]["excluded_outside_valid_issn_scope"], 1)
                        for table in run_pipeline.CLEAN_EXPORT_TABLES:
                            with self.subTest(table=table):
                                self.assertEqual(
                                    table_columns(server, databases[0], table),
                                    table_columns(server, databases[1], table),
                                )
                                self.assertEqual(
                                    logical_rows(server, databases[0], table),
                                    logical_rows(server, databases[1], table),
                                    f"compact mode changed {table} at {version}",
                                )
                        for database in databases:
                            self.assertEqual(server.execute(
                                "SELECT canonical_source_record_id FROM ojs_articles WHERE article_id=10;",
                                database=database,
                            ), "11" if index == 1 else "10")
                            self.assertEqual(server.execute(
                                "SELECT CHAR_LENGTH(source_oai_identifier),version_number,"
                                "SHA2(metadata_xml,256) FROM ojs_articles WHERE article_id=50;",
                                database=database,
                            ), f"1024\t{1 if index == 0 else 2}\t"
                               + hashlib.sha256(rich_xml(index > 0).encode()).hexdigest())
                            all_payload = " AND ".join(
                                f"`{name}` IS NOT NULL" for name in DEFERRED_FIELDS
                            )
                            self.assertEqual(server.execute(
                                f"SELECT COUNT(*) FROM ojs_articles WHERE article_id=50 AND {all_payload};",
                                database=database,
                            ), "1", "deferred fields must be restored into the canonical article")
                        if index == 0:
                            # Check real audit values, not just mocked response
                            # strings: neither a foreign binding nor an
                            # interrupted reclamation can produce a report.
                            for mutation in ("source_sha256=REPEAT('0',64)", "reclaimed=0"):
                                server.execute(
                                    f"UPDATE ojs_pipeline_raw_stats SET {mutation} WHERE id=1;",
                                    database=databases[1],
                                )
                                with self.assertRaisesRegex(run_pipeline.PipelineError, "raw storage audit"):
                                    run_pipeline.collect_change_report(
                                        server, database=databases[1], password=None,
                                        snapshot=snapshot, source_sha256=source_hash,
                                        build_sql_sha256=build_hash, counts=counts,
                                        compact_storage=True,
                                    )
                                with self.assertRaisesRegex(run_pipeline.PipelineError, "entered finalization"):
                                    run_pipeline.validate_resume_state(
                                        server, staging_dir=server.datadir, snapshot=snapshot,
                                        database=databases[1], password=None,
                                        build_sql_sha256=build_hash,
                                    )
                                server.execute(
                                    "UPDATE ojs_pipeline_raw_stats SET "
                                    f"source_sha256='{source_hash}',reclaimed=1 WHERE id=1;",
                                    database=databases[1],
                                )
                        # Production consumes the audit in its report and then
                        # prunes it; only the five clean tables enter history.
                        server.execute("DROP TABLE ojs_pipeline_raw_stats;", database=databases[1])
                self.assertEqual(set(server.execute(
                    "SELECT DISTINCT event_type FROM ojs_article_events;", database=databases[1],
                ).splitlines()), {"added", "modified", "merged", "removed", "restored"})
                self.assertEqual(server.execute(
                    "SELECT COUNT(DISTINCT article_id) FROM ojs_article_sources "
                    "WHERE source_record_id IN (3,4);", database=databases[1],
                ), "1")
            finally:
                server.shutdown(None)

    def test_legacy_compact_metadata_checkpoint_reuses_raw_import_and_index(self):
        with TemporaryDirectory(prefix="ojs-compact-resume-") as directory, \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            root = Path(directory)
            clean = root / "clean"
            path = root / "pkpbeacon-2026-01-01.sql"
            path.write_text(snapshot_dump("2026-01-01", 0), encoding="utf-8")
            snapshot = run_pipeline.inspect_snapshot(path)
            staging = clean / "mysql-2026-01-01.building"
            tools = run_pipeline.MySQLTools.discover()
            database = "pkpbeacon_db"
            server = run_pipeline.MySQLServer(tools, staging, "128M", compact_storage=True)
            build_hash = run_pipeline.sha256_small_file(BUILD_SQL)
            # Construct the previous compact representation explicitly, without
            # calling today's renderer: compressed tables, full staging payload.
            legacy = BUILD_SQL.read_text(encoding="utf-8").replace(
                "/* OJS_COMPACT_TABLE */", "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
            )
            prefix, remainder = legacy.split(run_pipeline.METADATA_BEGIN_MARKER, 1)
            metadata, _ = remainder.split(run_pipeline.METADATA_END_MARKER, 1)
            try:
                server.initialize()
                server.start(None)
                run_pipeline.create_database(server, database)
                source_hash = server.import_snapshot(
                    snapshot, database=database, password=None, progress_seconds=60,
                )
                prelude = run_pipeline.build_sql_prelude(
                    snapshot=snapshot, source_sha256=source_hash,
                    build_sql_sha256=build_hash, mysql_version=tools.version, full_rescan=False,
                )
                server.run_sql_text(prefix, label="legacy index", database=database,
                                    password=None, prelude=prelude)
                server.run_sql_text(
                    metadata, label="legacy partial metadata", database=database,
                    password=None, prelude=prelude + "\nSET @ojs_metadata_max_id=3;",
                )
                self.assertEqual(server.execute(
                    "SELECT COUNT(*) FROM ojs_stage_sources WHERE metadata_xml IS NOT NULL;",
                    database=database,
                ), "3")
                original_index = logical_rows(server, database, "ojs_stage_source_index")
                run_pipeline.write_build_state(
                    staging, snapshot=snapshot, source_sha256=source_hash,
                    build_sql_sha256=build_hash, phase="metadata_ready",
                )
            finally:
                server.shutdown(None)
            identities = {
                name: (staging / database / f"{name}.ibd").stat().st_ino
                for name in ("records", "ojs_stage_source_index")
            }
            staging_inode = staging.stat().st_ino
            original_start = run_pipeline.MySQLServer.start
            original_run = run_pipeline.MySQLServer.run_sql_text
            seen = []

            def checked_start(resumed, password):
                self.assertEqual(resumed.datadir.stat().st_ino, staging_inode)
                for name, inode in identities.items():
                    self.assertEqual((staging / database / f"{name}.ibd").stat().st_ino, inode)
                original_start(resumed, password)
                self.assertEqual(logical_rows(resumed, database, "ojs_stage_source_index"), original_index)
                seen.append("reused")

            def checked_run(resumed, sql, **kwargs):
                self.assertNotEqual(kwargs["label"], "source-index phase")
                if kwargs["label"] == "metadata-shard" and "truncated" not in seen:
                    self.assertEqual(resumed.execute(
                        "SELECT COUNT(*) FROM ojs_stage_sources;", database=database,
                    ), "0")
                    seen.append("truncated")
                result = original_run(resumed, sql, **kwargs)
                if kwargs["label"] == "metadata-shard":
                    self.assertEqual(resumed.execute(
                        "SELECT COUNT(*) FROM ojs_stage_sources WHERE metadata_xml IS NOT NULL;",
                        database=database,
                    ), "0")
                return result

            with mock.patch.object(run_pipeline.MySQLServer, "initialize", side_effect=AssertionError("must not reinitialize")), \
                    mock.patch.object(run_pipeline.MySQLServer, "import_snapshot", side_effect=AssertionError("must not reimport")), \
                    mock.patch.object(run_pipeline.MySQLServer, "start", new=checked_start), \
                    mock.patch.object(run_pipeline.MySQLServer, "run_sql_text", new=checked_run):
                final_dir, counts = run_pipeline.build_snapshot_database(
                    snapshot=snapshot, clean_dir=clean, build_sql=BUILD_SQL,
                    database=database, root_password="fixture-password",
                    api_db_user="fixture_api", api_db_password="fixture-api-password",
                    release_thresholds=run_pipeline.ReleaseThresholds(0.1, 0.1, 0.1, 0.1),
                    allow_anomalous_release=True, buffer_pool_size="128M", progress_seconds=60,
                    force_rebuild=True, verify_existing=False, verify_source_checksum=False,
                    full_rescan=False, publish_current=False, metadata_workers=1,
                    resume_building=True, compact_storage=True,
                )
            self.assertEqual(seen, ["reused", "truncated"])
            self.assertEqual(final_dir.stat().st_ino, staging_inode)
            self.assertIsNotNone(counts)
            self.assertFalse((final_dir / run_pipeline.BUILD_STATE_NAME).exists())
            report = json.loads(run_pipeline.change_report_path(clean, snapshot.version).read_text())
            self.assertEqual(report["snapshot"]["source_sha256"], source_hash)
            self.assertEqual(report["snapshot"]["build_sql_sha256"], build_hash)
            self.assertEqual(report["storage"]["profile"], "deferred-source-payload-v1")
            self.assertEqual(report["source_rows"]["raw_in_snapshot"], len(snapshot_rows(0)))
            self.assertEqual(report["source_rows"]["excluded_outside_valid_issn_scope"], 1)


if __name__ == "__main__":
    unittest.main()
