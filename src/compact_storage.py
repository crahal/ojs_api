"""Render a smaller temporary representation of the unchanged build SQL.

Source identity and metadata hashes are still computed from the original XML.
Payload-only values are deferred until canonical sources are selected, avoiding
an XML and Dublin Core payload copy for every source alias. The source schema
is retained so existing metadata-ready checkpoints can use normal truncation.
Completed work tables are reclaimed before later stages build their own tables.
Raw rows are counted and reclaimed only after canonical payloads own their XML.
Key membership stores hashes, not repeated key text. Original retained key text
is saved once per touched hash before persistent lookups are reconciled.
"""
from __future__ import annotations

import re


COMPACT_STORAGE_PROFILE = "deferred-source-payload-hash-keys-v2"
_KEY_TABLES = (
    "ojs_stage_keys", "ojs_stage_effective_keys", "ojs_stage_noisy_keys",
    "ojs_stage_touched_keys", "ojs_stage_key_labels", "ojs_stage_changed_keys",
)
# Lexical order agrees with VARCHAR order for these four fixed ASCII labels.
_KEY_TYPE = "ENUM('doi','fingerprint','oai','url')"
_STAGED_KEY_VALUE = """CASE effective.key_type
            WHEN 'oai' THEN staged.oai_key_value
            WHEN 'doi' THEN staged.doi
            WHEN 'url' THEN staged.url_key_value
            WHEN 'fingerprint' THEN staged.fingerprint_key_value
        END"""
_EXACT_KEYS_INSERT = """INSERT INTO ojs_stage_keys
SELECT source_record_id, 'oai', oai_key_hash, oai_key_value
FROM ojs_stage_sources
WHERE oai_key_hash IS NOT NULL
UNION ALL
SELECT source_record_id, 'doi', doi_key_hash, doi
FROM ojs_stage_sources
WHERE doi_key_hash IS NOT NULL
UNION ALL
SELECT source_record_id, 'url', url_key_hash, url_key_value
FROM ojs_stage_sources
WHERE url_key_hash IS NOT NULL
UNION ALL
SELECT
    source_record_id,
    'fingerprint',
    fingerprint_key_hash,
    fingerprint_key_value
FROM ojs_stage_sources
WHERE fingerprint_key_hash IS NOT NULL;
"""
_RETAINED_KEYS_END = """) retained
LEFT JOIN ojs_article_keys identity_key
    ON identity_key.key_type = retained.key_type
   AND identity_key.key_hash = retained.key_hash;
"""
_TOUCHED_KEYS_INSERT = """INSERT INTO ojs_stage_touched_keys
SELECT
    effective.key_type,
    effective.key_hash,
    MAX(COALESCE(identity_key.key_value, effective.key_value)),
    MIN(identity_key.date_added)
FROM ojs_stage_effective_keys effective
INNER JOIN ojs_stage_sources staged
    ON staged.source_record_id = effective.source_record_id
LEFT JOIN ojs_article_keys identity_key
    ON identity_key.key_type = effective.key_type
   AND identity_key.key_hash = effective.key_hash
GROUP BY effective.key_type, effective.key_hash;
"""
_TOUCHED_KEYS_UPDATE = """    key_value = COALESCE(
        ojs_stage_touched_keys.key_value,
        VALUES(key_value)
    ),
"""
_RECONCILE_KEYS_JOIN = """INNER JOIN ojs_stage_touched_keys touched
    ON touched.key_type = effective.key_type
   AND touched.key_hash = effective.key_hash
"""
_RECONCILE_KEY_VALUE = "COALESCE(MAX(effective.key_value), MAX(touched.key_value))"
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


def _defer_key_values(sql: str) -> str:
    """Preserve exact per-alias values while storing only hash membership.

    The final join is one-to-one on the metadata stage's primary key, never a
    join of all aliases sharing a hash. Retained values must be snapshotted
    separately: touched.key_value can instead contain a new staged value when
    the original persistent lookup was absent. Coalescing those prematurely
    would not preserve the original NULL and MAX semantics.
    """
    if re.search(r"\bretained_key_value\b", sql, re.IGNORECASE):
        raise CompactStorageError("compact storage found existing retained_key_value")
    source_schema = _table_schema(sql, "ojs_stage_sources")
    for name, column_type in (("oai_key_value", "VARCHAR(2048)"),
                              ("doi", "VARCHAR(255)"),
                              ("url_key_value", "VARCHAR(2048)"),
                              ("fingerprint_key_value", "VARCHAR(2048)")):
        _require_once(source_schema, f"    {name} {column_type} NULL,", f"source key value {name}")
    _require_once(source_schema, "    PRIMARY KEY (source_record_id)", "unique source key-value lookup")

    # Every insertion and consumer of the two removed text columns is checked
    # before replacing it. Reject extra consumers, including alternate aliases
    # and unqualified SELECTs, instead of returning SQL that fails after hours.
    expected_references = {"ojs_stage_keys": 11, "ojs_stage_effective_keys": 10}
    for table, expected in expected_references.items():
        if len(re.findall(rf"\b{table}\b", sql, re.IGNORECASE)) != expected:
            raise CompactStorageError(f"compact storage found unexpected references to {table}")
    for column, count in (("staged_key.key_value", 1), ("effective.key_value", 3)):
        if len(re.findall(rf"\b{re.escape(column)}\b", sql, re.IGNORECASE)) != count:
            raise CompactStorageError(f"compact storage found unexpected consumers of {column}")
    if len(re.findall(r"\bkey_value\b", sql, re.IGNORECASE)) != 21:
        raise CompactStorageError("compact storage found unexpected key value consumers")

    for table in _KEY_TABLES:
        schema = _table_schema(sql, table)
        key_type = "    key_type VARCHAR(16) NOT NULL,"
        _require_once(schema, key_type, f"{table} key type")
        compact_schema = schema.replace(key_type, f"    key_type {_KEY_TYPE} NOT NULL,", 1)
        if table in ("ojs_stage_keys", "ojs_stage_effective_keys"):
            nullable = "NOT NULL" if table == "ojs_stage_keys" else "NULL"
            declaration = f"    key_value VARCHAR(2048) {nullable},\n"
            _require_once(schema, declaration, f"{table} key value")
            compact_schema = compact_schema.replace(declaration, "", 1)
        elif table == "ojs_stage_touched_keys":
            _require_once(schema, "    key_value VARCHAR(2048) NULL,", "touched key value")
            declaration = "    date_added DATE NULL,\n"
            _require_once(schema, declaration, "touched key date")
            compact_schema = compact_schema.replace(
                declaration, declaration + "    retained_key_value VARCHAR(2048) NULL,\n", 1,
            )
        original = f"CREATE TABLE {table} (\n{schema}\n) ENGINE=InnoDB"
        replacement = f"CREATE TABLE {table} (\n{compact_schema}\n) ENGINE=InnoDB"
        sql = sql.replace(original, replacement, 1)

    _require_once(sql, _EXACT_KEYS_INSERT, "exact key insertion")
    membership = _EXACT_KEYS_INSERT
    for text in (", oai_key_value", ", doi\n", ", url_key_value"):
        membership = membership.replace(text, "\n" if text.endswith("\n") else "", 1)
    membership = membership.replace("fingerprint_key_hash,\n    fingerprint_key_value", "fingerprint_key_hash", 1)
    sql = sql.replace(_EXACT_KEYS_INSERT, membership, 1)

    staged = """INSERT INTO ojs_stage_effective_keys
SELECT
    staged_key.source_record_id,
    staged_key.key_type,
    staged_key.key_hash,
    staged_key.key_value,
    prior.article_id,
    1
FROM ojs_stage_keys staged_key
LEFT JOIN ojs_article_sources prior
    ON prior.source_record_id = staged_key.source_record_id;
"""
    _require_once(sql, staged, "staged effective key insertion")
    sql = sql.replace(staged, staged.replace("    staged_key.key_value,\n", "", 1), 1)
    retained = """INSERT INTO ojs_stage_effective_keys
SELECT
    retained.source_record_id,
    retained.key_type,
    retained.key_hash,
    identity_key.key_value,
    retained.article_id,
    retained.is_present
FROM ("""
    _require_once(sql, retained, "retained effective key insertion")
    _require_once(sql, _RETAINED_KEYS_END, "retained effective key lookup")
    sql = sql.replace(retained, retained.replace("    identity_key.key_value,\n", "", 1), 1)
    sql = sql.replace(_RETAINED_KEYS_END, ") retained;\n", 1)

    _require_once(sql, _TOUCHED_KEYS_INSERT, "touched staged key insertion")
    touched = _TOUCHED_KEYS_INSERT.replace("effective.key_value", _STAGED_KEY_VALUE, 1).replace(
        "    MIN(identity_key.date_added)\n",
        "    MIN(identity_key.date_added),\n    MAX(identity_key.key_value)\n", 1,
    )
    sql = sql.replace(_TOUCHED_KEYS_INSERT, touched, 1)
    for alias in ("retired", "noisy"):
        projection = f"""INSERT INTO ojs_stage_touched_keys
SELECT
    {alias}.key_type,
    {alias}.key_hash,
    identity_key.key_value,
    identity_key.date_added
"""
        _require_once(sql, projection, f"touched {alias} key insertion")
        sql = sql.replace(projection, projection.replace(
            "    identity_key.date_added\n",
            "    identity_key.date_added,\n    identity_key.key_value\n", 1,
        ), 1)
    if sql.count(_TOUCHED_KEYS_UPDATE) != 2:
        raise CompactStorageError("compact storage requires both touched key updates")
    sql = sql.replace(_TOUCHED_KEYS_UPDATE, _TOUCHED_KEYS_UPDATE + """    retained_key_value = COALESCE(
        ojs_stage_touched_keys.retained_key_value,
        VALUES(retained_key_value)
    ),
""")

    _require_once(sql, _RECONCILE_KEYS_JOIN, "reconciled key membership join")
    if sql.count(_RECONCILE_KEY_VALUE) != 2:
        raise CompactStorageError("compact storage requires both reconciled key value expressions")
    sql = sql.replace(_RECONCILE_KEYS_JOIN, _RECONCILE_KEYS_JOIN + """LEFT JOIN ojs_stage_sources staged
    ON staged.source_record_id = effective.source_record_id
""", 1)
    # Use presence, not COALESCE, to preserve missing retained identity values.
    key_value = ("COALESCE(MAX(CASE WHEN staged.source_record_id IS NULL "
                 "THEN touched.retained_key_value ELSE " + _STAGED_KEY_VALUE
                 + " END), MAX(touched.key_value))")
    sql = sql.replace(_RECONCILE_KEY_VALUE, key_value)
    return sql


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

    This does not change the source file, persistent table definitions, metadata
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
    return _reclaim_raw_records(_reclaim_completed_worktables(_defer_key_values(rendered)))
