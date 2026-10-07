from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
from compact_storage import (
    COMPACT_STORAGE_PROFILE,
    DEFERRED_PAYLOAD_FIELDS,
    CompactStorageError,
    render_compact_sql,
)


EXPECTED_PAYLOAD = {
    "creators": "//dc:creator",
    "subjects": "//dc:subject",
    "description": "//dc:description",
    "publisher": "//dc:publisher",
    "published": "//dc:date",
    "types": "//dc:type",
    "formats": "//dc:format",
    "identifiers": "//dc:identifier",
    "source_title": "//dc:source",
    "languages": "//dc:language",
    "relations": "//dc:relation",
    "coverage": "//dc:coverage",
    "rights": "//dc:rights",
    "metadata_xml": None,
}
EXPECTED_EARLY_RECLAIM = {
    "source_indexes": (
        "ojs_stage_source_index", "ojs_stage_contexts", "ojs_stage_assignments",
        "ojs_stage_existing_candidates", "ojs_stage_merge_map", "ojs_stage_key_labels",
        "ojs_stage_source_labels", "ojs_stage_frontier_sources", "ojs_stage_changed_keys",
        "ojs_stage_next_frontier", "ojs_stage_keys",
    ),
    "article_rollup": (
        "ojs_stage_effective_keys", "ojs_stage_touched_keys", "ojs_stage_noisy_keys",
    ),
}


def select_expressions(projection: str) -> list[str]:
    """Split a fixture SELECT at top-level commas, preserving SQL expressions."""
    expressions = []
    depth = start = index = 0
    quote = None
    while index < len(projection):
        character = projection[index]
        if quote:
            if character == "\\":
                index += 2
                continue
            if character == quote:
                if projection[index:index + 2] == quote * 2:
                    index += 2
                    continue
                quote = None
        elif character in "'\"":
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            expressions.append(projection[start:index].strip())
            start = index + 1
        index += 1
    if depth or quote:
        raise AssertionError("unbalanced SQL projection")
    expressions.append(projection[start:].strip())
    return expressions


def metadata_values(sql: str) -> tuple[dict[str, str], str]:
    body = sql.split("INSERT INTO ojs_stage_sources (\n", 1)[1]
    columns, body = body.split("\n)\nSELECT\n", 1)
    projection, normalization = body.split("\nFROM (\n", 1)
    expressions = select_expressions(projection)
    names = [name.strip() for name in columns.split(",")]
    if len(names) != len(expressions):
        raise AssertionError("metadata INSERT columns do not match SELECT")
    return dict(zip(names, expressions)), normalization.split("-- OJS_PIPELINE_METADATA_END", 1)[0]


def canonical_values(sql: str) -> dict[str, str]:
    schema = sql.split("CREATE TABLE ojs_stage_canonical_payload (\n", 1)[1].split(
        "\n) ENGINE=InnoDB", 1
    )[0]
    names = re.findall(
        r"^    ([a-z_]+) (?:BIGINT|VARCHAR|CHAR|DATETIME|TEXT|MEDIUMTEXT|BINARY)\b",
        schema, re.MULTILINE,
    )
    projection = sql.split("INSERT INTO ojs_stage_canonical_payload\nSELECT\n", 1)[1].split(
        "\nFROM ojs_stage_payload_needed needed\n", 1
    )[0]
    expressions = select_expressions(projection)
    if len(names) != len(expressions):
        raise AssertionError("canonical INSERT columns do not match SELECT")
    return dict(zip(names, expressions))


class CompactStorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = (PROJECT_ROOT / "sql" / "01_build_ojs_tables.sql").read_text(encoding="utf-8")

    def test_only_payload_insert_values_are_deferred_and_identity_calculation_is_unchanged(self):
        original, original_normalization = metadata_values(self.sql)
        rendered, rendered_normalization = metadata_values(render_compact_sql(self.sql))
        self.assertEqual(set(rendered), set(original))
        self.assertEqual(rendered_normalization, original_normalization)
        self.assertEqual(set(DEFERRED_PAYLOAD_FIELDS), set(EXPECTED_PAYLOAD))
        for name, value in original.items():
            with self.subTest(column=name):
                self.assertEqual(rendered[name], "NULL" if name in EXPECTED_PAYLOAD else value)
        self.assertEqual(rendered["metadata_hash"], "UNHEX(SHA2(normalized.metadata_xml, 256))")
        for identity in ("title", "first_creator", "publication_year", "doi", "article_url",
                         "source_oai_identifier", "oai_key_hash", "doi_key_hash",
                         "url_key_hash", "fingerprint_key_hash"):
            self.assertNotEqual(rendered[identity], "NULL")
        for dependency in ("extracted.published", "extracted.identifiers", "extracted.relations"):
            self.assertIn(dependency, rendered_normalization)

    def test_canonical_fields_are_reconstructed_exactly_and_hashes_stay_unchanged(self):
        original = canonical_values(self.sql)
        rendered_sql = render_compact_sql(self.sql)
        rendered = canonical_values(rendered_sql)
        self.assertEqual(set(rendered), set(original))
        for name, value in original.items():
            with self.subTest(column=name):
                if name in EXPECTED_PAYLOAD:
                    xpath = EXPECTED_PAYLOAD[name]
                    expected = "r.metadata" if xpath is None else f"NULLIF(ExtractValue(r.metadata, '{xpath}'), '')"
                    self.assertEqual(rendered[name], expected)
                else:
                    self.assertEqual(rendered[name], value)
        self.assertIn(
            "INNER JOIN records r\n    ON r.id = staged.source_record_id;", rendered_sql
        )
        fallback = "-- A preferred alias can be unchanged and therefore absent"
        fallback_end = "WHERE staged.source_record_id IS NULL;"
        self.assertEqual(
            self.sql.split(fallback, 1)[1].split(fallback_end, 1)[0],
            rendered_sql.split(fallback, 1)[1].split(fallback_end, 1)[0],
        )

    def test_schema_and_metadata_phase_boundaries_remain_checkpoint_compatible(self):
        rendered = render_compact_sql(self.sql)
        begin = "-- OJS_PIPELINE_METADATA_BEGIN"
        self.assertEqual(self.sql.split(begin, 1)[0], rendered.split(begin, 1)[0])
        for table in ("ojs_stage_sources", "ojs_stage_canonical_payload"):
            def schema(text):
                return text.split(f"CREATE TABLE {table} (\n", 1)[1].split("\n) ENGINE=InnoDB", 1)[0]
            self.assertEqual(schema(self.sql), schema(rendered))
        self.assertEqual(rendered.count(begin), 1)
        self.assertEqual(rendered.count("-- OJS_PIPELINE_METADATA_END"), 1)
        self.assertEqual(COMPACT_STORAGE_PROFILE, "deferred-source-payload-v1")
        self.assertEqual(len(DEFERRED_PAYLOAD_FIELDS), 14)

    def test_existing_table_compression_can_run_before_or_after_rendering(self):
        marker = "/* OJS_COMPACT_TABLE */"
        storage = "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
        self.assertEqual(
            render_compact_sql(self.sql.replace(marker, storage)),
            render_compact_sql(self.sql).replace(marker, storage),
        )

    def test_completed_worktables_are_reclaimed_at_safe_phase_boundaries(self):
        rendered = render_compact_sql(self.sql)
        for stage, tables in EXPECTED_EARLY_RECLAIM.items():
            anchor = f"SELECT 'OJS_PROGRESS_V1', 'stage', '{stage}';\n"
            block = "-- Compact storage: reclaim completed work tables.\n" + "".join(
                f"DROP TABLE IF EXISTS {table};\n" for table in tables
            ) + "\n" + anchor
            self.assertEqual(rendered.count(block), 1)
            before, after = rendered.split(anchor, 1)
            for table in tables:
                with self.subTest(stage=stage, table=table):
                    drop = f"DROP TABLE IF EXISTS {table};\n"
                    self.assertEqual(rendered.count(drop), self.sql.count(drop) + 1)
                    self.assertEqual(after.count(drop), 1)
                    self.assertNotRegex(after.replace(drop, "", 1), rf"\b{table}\b")
                    self.assertLess(before.index(f"CREATE TABLE {table} ("), before.rindex(drop))
        cleanup = "SELECT 'OJS_PROGRESS_V1', 'stage', 'stage_cleanup';\n"
        self.assertEqual(self.sql.split(cleanup, 1)[1], rendered.split(cleanup, 1)[1])
        for protected in ("records", "ojs_articles", "ojs_article_sources", "ojs_article_keys"):
            self.assertEqual(
                rendered.count(f"DROP TABLE IF EXISTS {protected};"),
                self.sql.count(f"DROP TABLE IF EXISTS {protected};"),
            )
        self.assertEqual(
            self.sql, (PROJECT_ROOT / "sql" / "01_build_ojs_tables.sql").read_text(encoding="utf-8")
        )

    def test_any_later_worktable_consumer_prevents_early_reclamation(self):
        for stage, tables in EXPECTED_EARLY_RECLAIM.items():
            anchor = f"SELECT 'OJS_PROGRESS_V1', 'stage', '{stage}';\n"
            for table in tables:
                for reference in (table, f"`{table}`", table.upper()):
                    changed = self.sql.replace(anchor, anchor + f"SELECT * FROM {reference};\n", 1)
                    with self.subTest(stage=stage, table=reference), self.assertRaises(CompactStorageError):
                        render_compact_sql(changed)

    def test_missing_changed_or_duplicate_final_cleanup_fails_closed(self):
        cleanup = "SELECT 'OJS_PROGRESS_V1', 'stage', 'stage_cleanup';\n"
        before, final = self.sql.split(cleanup, 1)
        for tables in EXPECTED_EARLY_RECLAIM.values():
            for table in tables:
                drop = f"DROP TABLE IF EXISTS {table};\n"
                for replacement in ("", drop + drop, f"DROP TABLE {table};\n"):
                    changed = before + cleanup + final.replace(drop, replacement, 1)
                    with self.subTest(table=table, replacement=replacement), self.assertRaises(CompactStorageError):
                        render_compact_sql(changed)

    def test_reclamation_requires_definitions_and_ordered_unique_phase_anchors(self):
        stages = ("source_reconciliation", "source_indexes", "key_reconciliation", "article_rollup", "stage_cleanup")
        for stage in stages:
            anchor = f"SELECT 'OJS_PROGRESS_V1', 'stage', '{stage}';\n"
            for replacement in ("", anchor + anchor):
                with self.subTest(stage=stage, duplicate=bool(replacement)), self.assertRaises(CompactStorageError):
                    render_compact_sql(self.sql.replace(anchor, replacement, 1))
        for first, second in zip(stages, stages[1:]):
            changed = self.sql.replace(f"'stage', '{first}'", "'stage', 'temporary'").replace(
                f"'stage', '{second}'", f"'stage', '{first}'"
            ).replace("'stage', 'temporary'", f"'stage', '{second}'")
            with self.subTest(reversed=(first, second)), self.assertRaises(CompactStorageError):
                render_compact_sql(changed)
        for tables in EXPECTED_EARLY_RECLAIM.values():
            for table in tables:
                with self.subTest(missing_definition=table), self.assertRaises(CompactStorageError):
                    render_compact_sql(self.sql.replace(f"CREATE TABLE {table} (\n", f"CREATE TABLE renamed_{table} (\n", 1))

    def test_exact_raw_count_is_bound_and_saved_before_raw_rows_are_reclaimed(self):
        rendered = render_compact_sql(self.sql)
        release = "SELECT 'OJS_PROGRESS_V1', 'stage', 'raw_storage_release';\n"
        before, after = rendered.split(release, 1)
        audit, subsequent = after.split("DROP TABLE ojs_stage_sources;\n", 1)
        self.assertEqual(before.count("INSERT INTO ojs_stage_canonical_payload\nSELECT\n"), 2)
        self.assertTrue(before.rfind("WHERE staged.source_record_id IS NULL;") > before.rfind("INNER JOIN records r"))
        self.assertIn("CREATE TABLE ojs_pipeline_raw_stats (\n", audit)
        for column in ("id TINYINT UNSIGNED NOT NULL", "snapshot_date DATE NOT NULL",
                       "source_sha256 CHAR(64) NOT NULL", "raw_source_rows BIGINT UNSIGNED NOT NULL",
                       "reclaimed TINYINT UNSIGNED NOT NULL", "PRIMARY KEY (id)"):
            self.assertIn(column, audit)
        count = "SELECT 1, @ojs_snapshot_date, @ojs_source_sha256, COUNT(*), 0\nFROM records;\n"
        truncate = "TRUNCATE TABLE records;\n"
        complete = "UPDATE ojs_pipeline_raw_stats SET reclaimed = 1 WHERE id = 1;\n"
        self.assertEqual(audit.count(count), 1)
        self.assertEqual(audit.count(truncate), 1)
        self.assertEqual(audit.count(complete), 1)
        self.assertLess(audit.index(count), audit.index(truncate))
        self.assertLess(audit.index(truncate), audit.index(complete))
        self.assertNotIn("DROP TABLE", audit)
        self.assertNotIn(truncate, self.sql)
        self.assertEqual(rendered.count(truncate), 1)
        self.assertNotRegex(subsequent, r"\brecords\b")
        self.assertEqual(self.sql.split("DROP TABLE ojs_stage_sources;\n", 1)[1], subsequent)

    def test_raw_reclamation_rejects_later_consumers_and_existing_audit_evidence(self):
        source_release = "DROP TABLE ojs_stage_sources;\n"
        for reference in ("records", "`records`", "RECORDS", "raw.records"):
            changed = self.sql.replace(source_release, source_release + f"SELECT * FROM {reference};\n", 1)
            with self.subTest(reference=reference), self.assertRaises(CompactStorageError):
                render_compact_sql(changed)
        for reference in ("ojs_pipeline_raw_stats", "OJS_PIPELINE_RAW_STATS"):
            with self.subTest(existing=reference), self.assertRaises(CompactStorageError):
                render_compact_sql(self.sql + f"\nSELECT * FROM {reference};\n")

    def test_raw_reclamation_requires_both_canonical_paths_and_correct_phase_order(self):
        source_release = "DROP TABLE ojs_stage_sources;\n"
        fallback_end = "WHERE staged.source_record_id IS NULL;\n"
        article_state = "SELECT 'OJS_PROGRESS_V1', 'stage', 'article_state';\n"
        for anchor in (source_release, fallback_end, article_state):
            for replacement in ("", anchor + anchor):
                with self.subTest(anchor=anchor, duplicate=bool(replacement)), self.assertRaises(CompactStorageError):
                    render_compact_sql(self.sql.replace(anchor, replacement, 1))
        missing_fallback_insert = self.sql.rsplit("INSERT INTO ojs_stage_canonical_payload\nSELECT\n", 1)
        variants = (
            "INSERT INTO another_table\nSELECT\n".join(missing_fallback_insert),
            self.sql.replace(source_release, "", 1).replace(fallback_end, source_release + fallback_end, 1),
            self.sql.replace(article_state, "", 1).replace(source_release, article_state + source_release, 1),
        )
        for index, changed in enumerate(variants):
            with self.subTest(variant=index), self.assertRaises(CompactStorageError):
                render_compact_sql(changed)

    def test_missing_or_duplicated_projections_fail_closed(self):
        for token in (
            "-- OJS_PIPELINE_METADATA_BEGIN", "-- OJS_PIPELINE_METADATA_END",
            "    normalized.description,\n", "    staged.description,\n",
            "INNER JOIN ojs_stage_sources staged\n    ON staged.source_record_id = needed.source_record_id;",
        ):
            for replacement in ("", token + token):
                with self.subTest(token=token, duplicate=bool(replacement)):
                    with self.assertRaises(CompactStorageError):
                        render_compact_sql(self.sql.replace(token, replacement, 1))

    def test_changed_schema_or_extraction_semantics_fail_closed(self):
        source_start = self.sql.index("CREATE TABLE ojs_stage_sources (")
        changed_schema = self.sql[:source_start] + self.sql[source_start:].replace(
            "    description MEDIUMTEXT NULL,", "    description VARCHAR(255) NULL,", 1
        )
        variants = (
            changed_schema,
            self.sql.replace("CREATE TABLE ojs_stage_canonical_payload (", "CREATE TABLE renamed_payload (", 1),
            self.sql.replace("NULLIF(ExtractValue(r.metadata, '//dc:subject'), '') AS subjects",
                             "ExtractValue(r.metadata, '//dc:subject') AS subjects", 1),
            self.sql.replace("UNHEX(SHA2(normalized.metadata_xml, 256))", "UNHEX(MD5(normalized.metadata_xml))", 1),
            self.sql + "\nSELECT staged.description FROM ojs_stage_sources staged;\n",
            render_compact_sql(self.sql),
        )
        for index, sql in enumerate(variants):
            with self.subTest(variant=index), self.assertRaises(CompactStorageError):
                render_compact_sql(sql)

    def test_invalid_input_and_reversed_metadata_boundaries_fail_closed(self):
        for sql in (None, "", self.sql.replace("-- OJS_PIPELINE_METADATA_BEGIN", "-- TEMP").replace(
            "-- OJS_PIPELINE_METADATA_END", "-- OJS_PIPELINE_METADATA_BEGIN"
        ).replace("-- TEMP", "-- OJS_PIPELINE_METADATA_END")):
            with self.subTest(sql_type=type(sql).__name__), self.assertRaises(CompactStorageError):
                render_compact_sql(sql)


if __name__ == "__main__":
    unittest.main()
