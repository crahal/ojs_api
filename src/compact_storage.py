"""Render a smaller temporary representation of the unchanged build SQL.

Source identity and metadata hashes are still computed from the original XML.
Payload-only values are deferred until canonical sources are selected, avoiding
an XML and Dublin Core payload copy for every source alias. The source schema
is retained so existing metadata-ready checkpoints can use normal truncation.
Completed work tables are reclaimed before later stages build their own tables.
Raw rows are counted and reclaimed only after canonical payloads own their XML.
"""
from __future__ import annotations

import re


COMPACT_STORAGE_PROFILE = "deferred-source-payload-v1"
_PAYLOAD_COLUMNS = (
    ("creators", "MEDIUMTEXT", "//dc:creator"),
    ("subjects", "MEDIUMTEXT", "//dc:subject"),
    ("description", "MEDIUMTEXT", "//dc:description"),
    ("publisher", "TEXT", "//dc:publisher"),
    ("published", "TEXT", "//dc:date"),
    ("types", "TEXT", "//dc:type"),
    ("formats", "TEXT", "//dc:format"),
    ("identifiers", "MEDIUMTEXT", "//dc:identifier"),
    ("source_title", "TEXT", "//dc:source"),
    ("languages", "TEXT", "//dc:language"),
    ("relations", "MEDIUMTEXT", "//dc:relation"),
    ("coverage", "TEXT", "//dc:coverage"),
    ("rights", "MEDIUMTEXT", "//dc:rights"),
    ("metadata_xml", "MEDIUMTEXT", None),
)
DEFERRED_PAYLOAD_FIELDS = tuple(name for name, _, _ in _PAYLOAD_COLUMNS)
_METADATA_BEGIN = "-- OJS_PIPELINE_METADATA_BEGIN"
_METADATA_END = "-- OJS_PIPELINE_METADATA_END"
_CANONICAL_BEGIN = (
    "INSERT INTO ojs_stage_canonical_payload\nSELECT\n"
    "    needed.article_id,\n    staged.source_record_id,\n"
)
_CANONICAL_SOURCE_JOIN = (
    "INNER JOIN ojs_stage_sources staged\n"
    "    ON staged.source_record_id = needed.source_record_id;"
)
_CANONICAL_JOIN = "FROM ojs_stage_payload_needed needed\n" + _CANONICAL_SOURCE_JOIN
_EARLY_RECLAIM = (
    ("source_indexes", (
        "ojs_stage_source_index", "ojs_stage_contexts", "ojs_stage_assignments",
        "ojs_stage_existing_candidates", "ojs_stage_merge_map", "ojs_stage_key_labels",
        "ojs_stage_source_labels", "ojs_stage_frontier_sources", "ojs_stage_changed_keys",
        "ojs_stage_next_frontier", "ojs_stage_keys",
    )),
    ("article_rollup", (
        "ojs_stage_effective_keys", "ojs_stage_touched_keys", "ojs_stage_noisy_keys",
    )),
)
_CANONICAL_FALLBACK_END = (
    "FROM ojs_stage_payload_needed needed\n"
    "INNER JOIN ojs_article_sources source_alias\n"
    "    ON source_alias.source_record_id = needed.source_record_id\n"
    "INNER JOIN records r\n"
    "    ON r.id = needed.source_record_id\n"
    "LEFT JOIN ojs_stage_sources staged\n"
    "    ON staged.source_record_id = needed.source_record_id\n"
    "WHERE staged.source_record_id IS NULL;\n"
)
_SOURCE_RELEASE = "DROP TABLE ojs_stage_sources;\n"
_RAW_RELEASE = """SELECT 'OJS_PROGRESS_V1', 'stage', 'raw_storage_release';
CREATE TABLE ojs_pipeline_raw_stats (
    id TINYINT UNSIGNED NOT NULL,
    snapshot_date DATE NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    raw_source_rows BIGINT UNSIGNED NOT NULL,
    reclaimed TINYINT UNSIGNED NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;
INSERT INTO ojs_pipeline_raw_stats (
    id, snapshot_date, source_sha256, raw_source_rows, reclaimed
)
SELECT 1, @ojs_snapshot_date, @ojs_source_sha256, COUNT(*), 0
FROM records;
TRUNCATE TABLE records;
UPDATE ojs_pipeline_raw_stats SET reclaimed = 1 WHERE id = 1;

"""


class CompactStorageError(ValueError):
    """The SQL no longer has the supported storage transformation shape."""


def _require_once(text: str, expected: str, label: str) -> None:
    if text.count(expected) != 1:
        raise CompactStorageError(f"compact storage requires exactly one {label}")


def _table_schema(sql: str, table: str) -> str:
    start = f"CREATE TABLE {table} (\n"
    _require_once(sql, start, f"{table} definition")
    _, remainder = sql.split(start, 1)
    schema, separator, _ = remainder.partition("\n) ENGINE=InnoDB")
    if not separator:
        raise CompactStorageError(f"compact storage requires an InnoDB {table} definition")
    return schema


def _progress_anchor(stage: str) -> str:
    return f"SELECT 'OJS_PROGRESS_V1', 'stage', '{stage}';\n"


def _reclaim_completed_worktables(sql: str) -> str:
    phases = ("source_reconciliation", "source_indexes", "key_reconciliation",
              "article_rollup", "stage_cleanup")
    positions = []
    for phase in phases:
        anchor = _progress_anchor(phase)
        _require_once(sql, anchor, f"{phase} progress anchor")
        positions.append(sql.index(anchor))
    if positions != sorted(positions):
        raise CompactStorageError("compact storage reclamation phases are out of order")

    for phase, tables in _EARLY_RECLAIM:
        anchor = _progress_anchor(phase)
        before, after = sql.split(anchor, 1)
        final_cleanup = after.split(_progress_anchor("stage_cleanup"), 1)[1]
        drops = []
        for table in tables:
            _table_schema(before, table)
            drop = f"DROP TABLE IF EXISTS {table};\n"
            _require_once(after, drop, f"eventual cleanup for {table}")
            _require_once(final_cleanup, drop, f"final cleanup for {table}")
            remaining = after.replace(drop, "", 1)
            if re.search(rf"\b{re.escape(table)}\b", remaining, re.IGNORECASE):
                raise CompactStorageError(f"compact storage found a later use of {table}")
            drops.append(drop)
        sql = before + "-- Compact storage: reclaim completed work tables.\n" + "".join(drops) + "\n" + anchor + after
    return sql


def _reclaim_raw_records(sql: str) -> str:
    _require_once(sql, _SOURCE_RELEASE, "source payload release")
    _require_once(sql, _CANONICAL_FALLBACK_END, "completed canonical fallback")
    _require_once(sql, "WHERE staged.source_record_id IS NULL;\n", "canonical fallback final predicate")
    _require_once(sql, _progress_anchor("article_state"), "article state progress anchor")
    before, after = sql.split(_SOURCE_RELEASE, 1)
    if before.count("INSERT INTO ojs_stage_canonical_payload\nSELECT\n") != 2:
        raise CompactStorageError("compact storage requires both canonical payload inserts before raw release")
    if _CANONICAL_FALLBACK_END not in before or _progress_anchor("article_state") not in after:
        raise CompactStorageError("compact storage raw release phases are out of order")
    if re.search(r"\brecords\b", after, re.IGNORECASE):
        raise CompactStorageError("compact storage found a later use of raw records")
    if re.search(r"\bojs_pipeline_raw_stats\b", sql, re.IGNORECASE):
        raise CompactStorageError("compact storage found existing raw reclamation statistics")
    # Keep the imported table schema for raw-schema validation. The exact count
    # and source binding survive for reporting; an incomplete audit row is not
    # proof of a completed release. Never replace pre-existing audit evidence.
    return before + _RAW_RELEASE + _SOURCE_RELEASE + after


def render_compact_sql(sql: str) -> str:
    """Defer staged payload and reclaim finished work, rejecting unsupported SQL.

    This does not change the source file, existing table definitions, metadata
    hashes, matching keys, canonical choice, or unchanged-source fallback.
    Raw source archives are untouched. Existing compact-table marker
    substitution may run before or after this function.
    """
    if not isinstance(sql, str):
        raise CompactStorageError("compact storage requires SQL text")
    for marker in (_METADATA_BEGIN, _METADATA_END):
        _require_once(sql, marker, marker)
    if sql.index(_METADATA_BEGIN) >= sql.index(_METADATA_END):
        raise CompactStorageError("compact storage metadata markers are out of order")
    prefix, remainder = sql.split(_METADATA_BEGIN, 1)
    metadata, suffix = remainder.split(_METADATA_END, 1)
    source_schema = _table_schema(prefix, "ojs_stage_sources")
    canonical_schema = _table_schema(suffix, "ojs_stage_canonical_payload")
    _require_once(suffix, _CANONICAL_BEGIN, "staged canonical payload projection")
    _require_once(suffix, _CANONICAL_JOIN, "staged canonical payload join")
    _require_once(suffix, _CANONICAL_SOURCE_JOIN, "canonical source join")
    if suffix.index(_CANONICAL_BEGIN) >= suffix.index(_CANONICAL_JOIN):
        raise CompactStorageError("compact storage canonical projection and join are out of order")
    before_canonical, canonical = suffix.split(_CANONICAL_BEGIN, 1)
    projection, after_canonical = canonical.split(_CANONICAL_JOIN, 1)

    for name, column_type, xpath in _PAYLOAD_COLUMNS:
        declaration = f"    {name} {column_type} NULL,"
        _require_once(source_schema, declaration, f"nullable source payload column {name}")
        _require_once(canonical_schema, declaration, f"canonical payload column {name}")
        staged_projection = f"    normalized.{name},\n"
        _require_once(metadata, staged_projection, f"metadata projection for {name}")
        canonical_projection = f"    staged.{name},\n"
        _require_once(projection, canonical_projection, f"canonical projection for {name}")
        if len(re.findall(rf"\bstaged\.{name}\b", sql)) != 1:
            raise CompactStorageError(f"compact storage found an unexpected staged {name} consumer")
        if xpath is not None:
            extraction = f"NULLIF(ExtractValue(r.metadata, '{xpath}'), '') AS {name}"
            _require_once(metadata, extraction, f"original extraction for {name}")
            payload_value = f"NULLIF(ExtractValue(r.metadata, '{xpath}'), '')"
        else:
            _require_once(metadata, "r.metadata AS metadata_xml", "original XML extraction")
            _require_once(metadata, "UNHEX(SHA2(normalized.metadata_xml, 256))", "original XML hash")
            payload_value = "r.metadata"
        # Only the outer INSERT projection changes. Published/identifiers/
        # relations remain available inside normalization for year/DOI/URL keys.
        metadata = metadata.replace(staged_projection, "    NULL,\n", 1)
        projection = projection.replace(canonical_projection, f"    {payload_value},\n", 1)

    joined = _CANONICAL_JOIN.removesuffix(";") + (
        "\nINNER JOIN records r\n    ON r.id = staged.source_record_id;"
    )
    suffix = before_canonical + _CANONICAL_BEGIN + projection + joined + after_canonical
    rendered = prefix + _METADATA_BEGIN + metadata + _METADATA_END + suffix
    return _reclaim_raw_records(_reclaim_completed_worktables(rendered))
