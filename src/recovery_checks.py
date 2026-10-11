"""Read-only, fail-closed checks for a bootstrap interrupted at exact keys.

The caller owns the source/pipeline locks and verifies the physical directory,
original SQL and source archive. These checks never rewrite checkpoints or
discard work. They deliberately do not support recovery after a later stage.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
import re
import sys
import time

from progress_logging import Progress


_PREFIX_TABLES = {"ojs_stage_contexts", "ojs_stage_source_index", "ojs_stage_sources"}
_CLEAN_TABLES = (
    "ojs_snapshots", "ojs_articles", "ojs_article_sources",
    "ojs_article_keys", "ojs_article_events",
)
_RAW_TABLES = {"records", "contexts", "endpoints", "issns", "versions"}
_IDENTITY_FIELDS = (
    "context_id", "endpoint_id", "source_identifier", "application",
    "journal_issn", "journal_title", "endpoint_oai_url", "record_update_date",
    "record_publish_date", "record_created_at", "record_modified_at",
    "source_removed_at", "is_active",
)
_TEXT_IDENTITY_FIELDS = {"application", "journal_issn", "journal_title", "endpoint_oai_url"}
_KEY_FIELDS = (
    ("oai", "oai_key_value", "oai_key_hash"),
    ("doi", "doi", "doi_key_hash"),
    ("url", "url_key_value", "url_key_hash"),
    ("fingerprint", "fingerprint_key_value", "fingerprint_key_hash"),
)


def _pipeline():
    # Running ``python src/run_pipeline.py`` names the existing module __main__.
    # Importing it again would create a different PipelineError class, bypassing
    # the CLI's safe error handler. Import lazily for ordinary library callers.
    main = sys.modules.get("__main__")
    main_file = getattr(main, "__file__", None)
    if main_file and Path(main_file).resolve() == Path(__file__).with_name("run_pipeline.py").resolve():
        return main
    import run_pipeline
    return run_pipeline


def _fail(message: str) -> None:
    raise _pipeline().PipelineError(f"exact-key recovery refused: {message}")


def _numbers(output: str, count: int, label: str) -> tuple[int, ...]:
    fields = output.strip().split("\t")
    if len(fields) != count or any(re.fullmatch(r"[0-9]+", item) is None for item in fields):
        _fail(f"invalid numeric result from {label}")
    return tuple(int(item) for item in fields)


def _query(server, sql: str, database: str, password: str | None, label: str) -> str:
    return server.execute(sql, database=database, password=password, label=label)


def _other_clients(server, database: str, password: str | None) -> None:
    output = _query(server, """
        SELECT COUNT(*) FROM information_schema.processlist
        WHERE ID <> CONNECTION_ID()
          AND NOT (USER IN ('system user', 'event_scheduler') AND COMMAND = 'Daemon');
        """, database, password, "recovery-other-clients")
    if _numbers(output, 1, "client isolation")[0]:
        _fail("another client is connected to the isolated database")


def _transactions(server, database: str, password: str | None) -> tuple[int, ...]:
    return _numbers(_query(server, """
        SELECT COUNT(*), COALESCE(SUM(trx_mysql_thread_id = 0), 0),
               COALESCE(SUM(trx_state = 'ROLLING BACK'), 0),
               COALESCE(SUM(trx_rows_modified), 0)
        FROM information_schema.innodb_trx
        WHERE trx_mysql_thread_id <> CONNECTION_ID();
        """, database, password, "recovery-transactions"), 4, "transaction inspection")


def wait_innodb_rollback(server, *, database: str, password: str | None,
                         progress_seconds: float = 30) -> None:
    """Wait without a kill deadline while crash-recovery transactions roll back.

    A successful ping is not sufficient: InnoDB accepts connections before
    background rollback finishes. Ordinary client transactions are refused.
    """
    with Progress("recovery-rollback", interval=progress_seconds) as progress:
        while True:
            _other_clients(server, database, password)
            total, recovered, rolling_back, remaining = _transactions(server, database, password)
            if recovered != total:
                _fail("an ordinary client transaction remains active")
            progress.update(pending_transactions=total, rollback_transactions=rolling_back,
                            remaining_rows=remaining)
            if total == 0:
                return
            time.sleep(min(30.0, float(progress_seconds)))


def _quiescent(server, database: str, password: str | None) -> None:
    _other_clients(server, database, password)
    if _transactions(server, database, password)[0]:
        _fail("InnoDB rollback or another transaction is still active")


def _equal(left: str, right: str, *, text: bool = False) -> str:
    # Case-insensitive catalogue collations must not conceal changed source text.
    if text:
        left, right = f"CAST({left} AS BINARY)", f"CAST({right} AS BINARY)"
    return f"({left} <=> {right})"


def _contexts(server, database: str, password: str | None) -> int:
    fields = ("endpoint_id", "application", "journal_issn", "journal_title", "endpoint_oai_url")
    mismatch = " OR ".join(
        f"NOT {_equal(f'c.{field}', f'expected.{field}', text=field in _TEXT_IDENTITY_FIELDS)}"
        for field in fields)
    counts = _numbers(_query(server, f"""
        SELECT COUNT(*), (SELECT COUNT(*) FROM ojs_stage_contexts),
               COALESCE(SUM(c.context_id IS NULL OR {mismatch}), 0)
        FROM (
            SELECT source_context.id AS context_id, source_context.endpoint_id,
                   NULLIF(TRIM(endpoint.application), '') AS application,
                   MIN(CASE WHEN LENGTH(REGEXP_REPLACE(UPPER(COALESCE(issn.issn, '')),
                                '[^0-9X]', '')) = 8
                            THEN REGEXP_REPLACE(UPPER(COALESCE(issn.issn, '')), '[^0-9X]', '')
                            ELSE NULL END) AS journal_issn,
                   NULLIF(TRIM(source_context.name), '') AS journal_title,
                   NULLIF(TRIM(endpoint.oai_url), '') AS endpoint_oai_url
            FROM contexts source_context
            INNER JOIN endpoints endpoint ON endpoint.id = source_context.endpoint_id
            INNER JOIN issns issn ON issn.context_id = source_context.id
            GROUP BY source_context.id, source_context.endpoint_id,
                     endpoint.application, source_context.name, endpoint.oai_url
            HAVING journal_issn IS NOT NULL
        ) expected
        LEFT JOIN ojs_stage_contexts c ON c.context_id = expected.context_id;
        """, database, password, "recovery-context-coverage"), 3, "context normalization")
    expected, actual, mismatches = counts
    if expected == 0 or actual != expected or mismatches:
        _fail("normalized contexts are incomplete or differ from original raw input")
    return actual


def _checkpoint(checkpoint: dict, expected: dict | None, snapshot_version: str,
                source_sha256: str, build_sql_sha256: str) -> None:
    if not isinstance(checkpoint, dict):
        _fail("checkpoint is not an object")
    for digest in (source_sha256, build_sql_sha256):
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            _fail("invalid original source or SQL checksum")
    try:
        if date.fromisoformat(snapshot_version).isoformat() != snapshot_version:
            _fail("invalid snapshot date")
    except (TypeError, ValueError):
        _fail("invalid snapshot date")
    required = {
        "pipeline_version": _pipeline().PIPELINE_VERSION, "snapshot_version": snapshot_version,
        "phase": "finalizing", "source_sha256": source_sha256,
        "build_sql_sha256": build_sql_sha256,
    }
    if expected is not None:
        if not isinstance(expected, dict):
            _fail("expected checkpoint is not an object")
        if any(key not in expected for key in (*required, "source_filename", "source_size_bytes")):
            _fail("expected checkpoint does not contain the original provenance fields")
        if any(expected.get(key) != value for key, value in required.items()):
            _fail("expected checkpoint contradicts the original provenance")
        required.update(expected)
    if any(checkpoint.get(key) != value for key, value in required.items()):
        _fail("checkpoint does not match the original finalizing build")
    if checkpoint.get("source_filename") not in (
        f"pkpbeacon-{snapshot_version}.sql", f"pkpbeacon-{snapshot_version}.sql.gz",
    ):
        _fail("checkpoint has an invalid source filename")
    if type(checkpoint.get("source_size_bytes")) is not int or checkpoint["source_size_bytes"] <= 0:
        _fail("checkpoint has an invalid logical source size")


def validate_bootstrap_exact_keys(
    server, *, database: str, password: str | None, snapshot_version: str,
    source_sha256: str, build_sql_sha256: str, checkpoint: dict,
    expected_checkpoint: dict | None = None, progress_seconds: float = 30,
    range_count: int = 32,
) -> dict:
    """Prove that complete bootstrap metadata can enter exact keys again.

    Tables from any later phase, even empty tables, are rejected. The exact-key
    table may be absent, partially populated, or complete because the caller
    will regenerate it. Metadata membership, identity and hashes must all agree
    with the retained raw input. Range counts are evidence independent of logs.
    """
    source_id_ranges = _pipeline().source_id_ranges
    _checkpoint(checkpoint, expected_checkpoint, snapshot_version, source_sha256,
                build_sql_sha256)
    if type(range_count) is not int or not 1 <= range_count <= 1024:
        _fail("invalid audit range count")
    _quiescent(server, database, password)
    output = _query(server, """
        SELECT table_name, table_type, COALESCE(engine, '')
        FROM information_schema.tables WHERE table_schema = DATABASE()
        ORDER BY table_name;
        """, database, password, "recovery-table-inventory")
    tables = {}
    for line in output.splitlines():
        fields = line.split("\t")
        if len(fields) != 3 or fields[0] in tables:
            _fail("invalid table inventory")
        tables[fields[0]] = fields[1:]
    required = _PREFIX_TABLES | set(_CLEAN_TABLES) | _RAW_TABLES
    if not required.issubset(tables):
        _fail("required raw, source-index, metadata or clean tables are missing")
    allowed = _PREFIX_TABLES | set(_CLEAN_TABLES) | {"ojs_stage_keys"}
    if any(name.startswith("ojs_") and name not in allowed for name in tables):
        _fail("later finalization, raw reclamation or provenance tables exist")
    if any(tables[name] != ["BASE TABLE", "InnoDB"] for name in required | (set(tables) & allowed)):
        _fail("required tables do not use the expected transactional engine")
    existing = ", ".join(f"EXISTS(SELECT 1 FROM {name} LIMIT 1)" for name in _CLEAN_TABLES)
    if any(_numbers(_query(server, f"SELECT {existing};", database, password,
                          "recovery-clean-empty"), len(_CLEAN_TABLES), "bootstrap clean tables")):
        _fail("clean catalogue is not an untouched bootstrap")
    if _numbers(_query(server, """
        SELECT COUNT(*) FROM information_schema.routines
        WHERE routine_schema = DATABASE() AND LEFT(routine_name, 4) = 'ojs_';
        """, database, password, "recovery-routine-inventory"), 1, "routine inventory")[0]:
        _fail("an unfinished pipeline procedure exists")

    # Exact primary-key counts/bounds avoid InnoDB's estimated TABLE_ROWS.
    with Progress("recovery-metadata-audit", interval=progress_seconds) as progress:
        context_count = _contexts(server, database, password)
        raw_count, raw_min, raw_max = _numbers(_query(server,
            "SELECT COUNT(*), COALESCE(MIN(id), 0), COALESCE(MAX(id), 0) FROM records;",
            database, password, "recovery-raw-count"), 3, "raw count")
        index_count, parse_count, index_min, index_max = _numbers(_query(server, """
            SELECT COUNT(*), COALESCE(SUM(needs_metadata_parse = 1), 0),
                   COALESCE(MIN(source_record_id), 0), COALESCE(MAX(source_record_id), 0)
            FROM ojs_stage_source_index;
            """, database, password, "recovery-index-count"), 4, "source-index count")
        metadata_count, metadata_min, metadata_max = _numbers(_query(server, """
            SELECT COUNT(*), COALESCE(MIN(source_record_id), 0),
                   COALESCE(MAX(source_record_id), 0) FROM ojs_stage_sources;
            """, database, password, "recovery-metadata-count"), 3, "metadata count")
        if raw_count == 0 or index_count == 0:
            _fail("bootstrap raw input or source index is empty")
        if index_count != parse_count or parse_count != metadata_count:
            _fail("bootstrap metadata is incomplete or source parse flags disagree")
        if (index_min, index_max) != (metadata_min, metadata_max):
            _fail("metadata bounds differ from the source index")
        if index_min < raw_min or index_max > raw_max:
            _fail("source-index bounds are outside the raw input")

        # Audit the original 32 metadata ranges independently of journal retention.
        ranges = source_id_ranges(index_min, index_max, range_count)
        identity_mismatch = " OR ".join(
            f"NOT {_equal(f's.{field}', f'i.{field}', text=field in _TEXT_IDENTITY_FIELDS)}"
            for field in _IDENTITY_FIELDS)
        raw_pairs = (
            ("context_id", "r.context_id"), ("source_identifier", "r.identifier"),
            ("endpoint_id", "c.endpoint_id"), ("application", "c.application"),
            ("journal_issn", "c.journal_issn"), ("journal_title", "c.journal_title"),
            ("endpoint_oai_url", "c.endpoint_oai_url"), ("record_update_date", "r.update_date"),
            ("record_publish_date", "r.publish_date"), ("record_created_at", "r.created_at"),
            ("record_modified_at", "r.modified_at"), ("source_removed_at", "r.removed_at"),
            ("is_active", "(r.removed_at IS NULL)"),
        )
        raw_mismatch = " OR ".join(
            f"NOT {_equal(f'i.{field}', value, text=field in _TEXT_IDENTITY_FIELDS)}"
            for field, value in raw_pairs)
        key_mismatch = " OR ".join(
            f"NOT (s.{digest} <=> UNHEX(SHA2(CONCAT('{kind}:', s.{value}), 256)))"
            for kind, value, digest in _KEY_FIELDS)
        audits = []
        checked = 0
        for position, (lower, upper) in enumerate(ranges, 1):
            progress.update(completed_ranges=position - 1, total_ranges=len(ranges),
                            range_min_id=lower, range_max_id=upper, checked_rows=checked)
            result = _numbers(_query(server, f"""
                SELECT COUNT(*), COALESCE(SUM(s.source_record_id IS NULL), 0),
                       COALESCE(SUM(r.id IS NULL OR c.context_id IS NULL), 0),
                       COALESCE(SUM({identity_mismatch}), 0),
                       COALESCE(SUM({raw_mismatch}), 0),
                       COALESCE(SUM(NOT (s.metadata_hash <=> UNHEX(SHA2(r.metadata, 256)))), 0),
                       COALESCE(SUM({key_mismatch}), 0)
                FROM ojs_stage_source_index i FORCE INDEX (PRIMARY)
                LEFT JOIN ojs_stage_sources s ON s.source_record_id = i.source_record_id
                LEFT JOIN records r ON r.id = i.source_record_id
                LEFT JOIN ojs_stage_contexts c ON c.context_id = r.context_id
                WHERE i.source_record_id BETWEEN {lower} AND {upper};
                """, database, password, "recovery-metadata-range"), 7, "metadata range")
            count, missing, absent_raw, identity, raw_identity, metadata_hash, keys = result
            if any(result[1:]):
                _fail(f"metadata range {position} failed membership, identity or hash validation")
            checked += count
            audits.append({"range": position, "min_id": lower, "max_id": upper,
                           "source_index_rows": count, "metadata_rows": count})
            progress.event("range-verified", completed_ranges=position, checked_rows=checked)
        if checked != index_count:
            _fail("audited source-index rows differ from the exact count")

        # A count and complete PK membership prove no extra metadata rows. Prove
        # source-index completeness independently over *all* raw rows, including
        # those outside index bounds. The contexts join preserves original scope.
        qualifying = 0
        for lower, upper in source_id_ranges(raw_min, raw_max, range_count):
            count, missing = _numbers(_query(server, f"""
                SELECT COUNT(*), COALESCE(SUM(i.source_record_id IS NULL), 0)
                FROM records r FORCE INDEX (PRIMARY)
                INNER JOIN ojs_stage_contexts c ON c.context_id = r.context_id
                LEFT JOIN ojs_stage_source_index i ON i.source_record_id = r.id
                WHERE r.id BETWEEN {lower} AND {upper};
                """, database, password, "recovery-source-coverage"), 2, "raw source coverage")
            if missing:
                _fail("source index omits qualifying raw records")
            qualifying += count
            progress.update(covered_source_rows=qualifying)
        if qualifying != index_count:
            _fail("source index differs from qualifying raw input")
    _quiescent(server, database, password)
    return {"schema_version": 1, "recovery_boundary": "bootstrap_exact_keys",
            "snapshot_version": snapshot_version, "source_sha256": source_sha256,
            "build_sql_sha256": build_sql_sha256, "raw_source_rows": raw_count,
            "normalized_context_rows": context_count,
            "source_index_rows": index_count, "metadata_rows": metadata_count,
            "metadata_ranges": audits, "verified_range_count": len(audits),
            "has_exact_key_table": "ojs_stage_keys" in tables,
            "clean_tables_empty": True, "metadata_hashes_verified": True,
            "identity_keys_verified": True, "source_coverage_verified": True}
