#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from tqdm import tqdm

try:
    import splink.comparison_library as cl
    from splink import DuckDBAPI, Linker, SettingsCreator
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    cl = None
    DuckDBAPI = None
    Linker = None
    SettingsCreator = None
    SPLINK_IMPORT_ERROR = exc
else:
    SPLINK_IMPORT_ERROR = None


REQUIRED_COLUMNS = {"id"}

@dataclass(frozen=True)
class MetadataAvailability:
    has_issn: bool
    has_name: bool
    has_metadata_title: bool
    has_metadata_identifier: bool
    has_metadata_article_url: bool

    @property
    def match_rules(self) -> tuple[str, ...]:
        rules: list[str] = []
        if self.has_name or self.has_metadata_title:
            rules.append("exact_normalized_title")
        if self.has_metadata_identifier:
            rules.append("exact_paper_identifier")
        if self.has_metadata_article_url:
            rules.append("exact_article_url")
        return tuple(rules)

    @property
    def match_description(self) -> tuple[str, ...]:
        rules: list[str] = []
        if self.has_name or self.has_metadata_title:
            rules.append("exact_normalized_title: normalized titles match exactly")
        if self.has_metadata_identifier:
            rules.append("exact_paper_identifier: normalized paper-level identifiers match exactly")
        if self.has_metadata_article_url:
            rules.append("exact_article_url: normalized article URLs match exactly")
        return tuple(rules)


@dataclass(frozen=True)
class ParquetStats:
    path: Path
    row_count: int
    row_groups: int
    file_size_bytes: int
    column_count: int
    columns: list[str]
    column_types: list[str]
    min_row_group_rows: int | None
    max_row_group_rows: int | None
    avg_row_group_rows: float | None


def timestamp_now() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def format_bytes(num_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:,.3f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def format_schema_preview(
    columns: list[str], column_types: list[str], max_fields: int = 8
) -> str:
    preview_pairs = [
        f"{column}:{column_type}"
        for column, column_type in zip(columns[:max_fields], column_types[:max_fields])
    ]
    if len(columns) > max_fields:
        preview_pairs.append(f"... +{len(columns) - max_fields} more")
    return ", ".join(preview_pairs)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class ProgressReporter:
    def __init__(self, *, disable: bool, refresh_seconds: float) -> None:
        self.disable = disable
        self.refresh_seconds = refresh_seconds
        self.phase_bar = tqdm(
            total=5,
            desc="Dedupe pipeline",
            unit="step",
            dynamic_ncols=True,
            leave=True,
            disable=disable,
            file=sys.stderr,
        )
        self.activity_bar: tqdm | None = None
        self.activity_thread: threading.Thread | None = None
        self.stop_event: threading.Event | None = None

    def log(self, message: str) -> None:
        line = f"[{timestamp_now()}] {message}"
        if self.disable:
            print(line, file=sys.stderr, flush=True)
            return
        tqdm.write(line, file=sys.stderr)

    def start_phase(self, description: str) -> None:
        if self.disable:
            return
        self.phase_bar.set_description(description)
        self.phase_bar.set_postfix_str(f"ts={timestamp_now()}", refresh=True)

    def complete_phase(self, description: str) -> None:
        if self.disable:
            return
        self.phase_bar.set_description(description)
        self.phase_bar.set_postfix_str(f"ts={timestamp_now()}", refresh=False)
        self.phase_bar.update(1)
        self.phase_bar.refresh()

    def start_activity(self, description: str) -> None:
        if self.disable:
            return
        if self.activity_bar is not None:
            self.stop_activity()
        self.activity_bar = tqdm(
            total=0,
            desc=description,
            bar_format="{desc} | elapsed: {elapsed} | {postfix}",
            dynamic_ncols=True,
            leave=False,
            disable=self.disable,
            file=sys.stderr,
        )
        self.stop_event = threading.Event()
        self.activity_thread = threading.Thread(target=self._pulse_activity, daemon=True)
        self.activity_thread.start()

    def _pulse_activity(self) -> None:
        assert self.activity_bar is not None
        assert self.stop_event is not None
        while not self.stop_event.wait(self.refresh_seconds):
            self.activity_bar.set_postfix_str(f"ts={timestamp_now()}", refresh=True)

    def stop_activity(self) -> None:
        if self.activity_bar is None:
            return
        assert self.stop_event is not None
        assert self.activity_thread is not None
        self.stop_event.set()
        self.activity_thread.join()
        self.activity_bar.set_postfix_str(f"ts={timestamp_now()}", refresh=True)
        self.activity_bar.close()
        self.activity_bar = None
        self.activity_thread = None
        self.stop_event = None

    def close(self) -> None:
        self.stop_activity()
        if not self.disable:
            self.phase_bar.close()


def inspect_parquet(path: Path) -> ParquetStats:
    parquet_file = pq.ParquetFile(path)
    schema = parquet_file.schema_arrow
    row_group_rows = [
        parquet_file.metadata.row_group(index).num_rows
        for index in range(parquet_file.metadata.num_row_groups)
    ]
    return ParquetStats(
        path=path,
        row_count=parquet_file.metadata.num_rows,
        row_groups=parquet_file.metadata.num_row_groups,
        file_size_bytes=path.stat().st_size,
        column_count=len(schema.names),
        columns=list(schema.names),
        column_types=[str(field.type) for field in schema],
        min_row_group_rows=min(row_group_rows) if row_group_rows else None,
        max_row_group_rows=max(row_group_rows) if row_group_rows else None,
        avg_row_group_rows=(
            sum(row_group_rows) / len(row_group_rows) if row_group_rows else None
        ),
    )


def log_parquet_stats(prefix: str, stats: ParquetStats, logger: ProgressReporter) -> None:
    logger.log(
        f"{prefix} {stats.path.name}: shape={stats.row_count:,} x {stats.column_count}, "
        f"{stats.row_groups} row groups, {format_bytes(stats.file_size_bytes)}"
    )
    if stats.avg_row_group_rows is not None:
        logger.log(
            f"{prefix} row groups {stats.path.name}: min={stats.min_row_group_rows:,}, "
            f"avg={stats.avg_row_group_rows:,.1f}, max={stats.max_row_group_rows:,} rows/group"
        )
    logger.log(
        f"{prefix} schema {stats.path.name}: "
        f"{format_schema_preview(stats.columns, stats.column_types, max_fields=16)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Deduplicate the joined OJS parquet into a conservative, API-ready entity table "
            "using Splink with DuckDB."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw/jonied.parquet"),
        help="Input parquet to deduplicate. Default: %(default)s",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/clean/deduplicated.parquet"),
        help="Output parquet path. Default: %(default)s",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output parquet if it already exists.",
    )
    parser.add_argument(
        "--memory-limit",
        default="1GB",
        help="DuckDB memory limit. Default: %(default)s",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="DuckDB worker threads. Default: %(default)s",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path("/tmp/ojs_api_dedup_duckdb_tmp"),
        help="Directory for DuckDB spill files. Default: %(default)s",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm-style progress updates.",
    )
    parser.add_argument(
        "--progress-refresh-seconds",
        type=float,
        default=1.0,
        help="Minimum seconds between progress refreshes. Default: %(default)s",
    )
    parser.add_argument(
        "--max-title-block-size",
        type=int,
        default=2000,
        help=(
            "Safety cap for title blocks when matching by normalized title. "
            "Title keys with more than this many observations are excluded from title matching. "
            "0 disables the cap. Default: %(default)s"
        ),
    )
    args = parser.parse_args()
    if args.threads <= 0:
        parser.error("--threads must be a positive integer")
    if args.progress_refresh_seconds <= 0:
        parser.error("--progress-refresh-seconds must be a positive number")
    if args.max_title_block_size < 0:
        parser.error("--max-title-block-size must be zero or a positive integer")
    return args


def infer_metadata_availability(columns: list[str]) -> MetadataAvailability:
    column_set = set(columns)
    return MetadataAvailability(
        has_issn="issn" in column_set,
        has_name="name" in column_set,
        has_metadata_title="metadata_title" in column_set,
        has_metadata_identifier="metadata_identifier" in column_set,
        has_metadata_article_url="metadata_article_url" in column_set,
    )


def ensure_input_schema(
    input_stats: ParquetStats,
    metadata_availability: MetadataAvailability,
) -> None:
    column_set = set(input_stats.columns)
    missing = sorted(REQUIRED_COLUMNS - column_set)
    if not metadata_availability.match_rules:
        missing.append("one of: metadata_title/name, metadata_identifier, metadata_article_url")
    if missing:
        raise ValueError(
            f"{input_stats.path} is missing required columns: {', '.join(missing)}"
        )


def configure_duckdb(connection: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    connection.execute(f"SET memory_limit = {sql_literal(args.memory_limit)}")
    connection.execute(f"SET threads = {args.threads}")
    connection.execute(f"SET temp_directory = {sql_literal(str(args.temp_dir.resolve()))}")
    connection.execute("SET preserve_insertion_order = false")


def prepare_observation_table(
    connection: duckdb.DuckDBPyConnection,
    input_path: Path,
    metadata_availability: MetadataAvailability,
    max_title_block_size: int,
) -> tuple[int, int]:
    issn_expr = "NULLIF(TRIM(issn), '')" if metadata_availability.has_issn else "NULL"
    name_expr = "NULLIF(TRIM(name), '')" if metadata_availability.has_name else "NULL"
    title_expr = (
        "COALESCE(NULLIF(TRIM(metadata_title), ''), NULLIF(TRIM(name), ''))"
        if metadata_availability.has_metadata_title and metadata_availability.has_name
        else "NULLIF(TRIM(metadata_title), '')"
        if metadata_availability.has_metadata_title
        else "NULLIF(TRIM(name), '')"
        if metadata_availability.has_name
        else "NULL"
    )
    identifier_expr = (
        "NULLIF(TRIM(metadata_identifier), '')"
        if metadata_availability.has_metadata_identifier
        else "NULL"
    )
    article_url_expr = (
        "NULLIF(TRIM(metadata_article_url), '')"
        if metadata_availability.has_metadata_article_url
        else "NULL"
    )

    title_key_expr = (
        "NULLIF("
        "TRIM("
        "REGEXP_REPLACE("
        f"REGEXP_REPLACE(LOWER(COALESCE({title_expr}, '')), '[^a-z0-9\\\\s]+', ' ', 'g'),"
        "'\\\\s+',"
        "' ',"
        "'g'"
        ")"
        "),"
        "''"
        ")"
    )
    identifier_key_expr = (
        f"NULLIF(TRIM(REGEXP_REPLACE(LOWER({identifier_expr}), '\\\\s+', ' ', 'g')), '')"
    )
    article_url_key_expr = (
        "NULLIF("
        "REGEXP_REPLACE("
        f"TRIM(REGEXP_REPLACE(LOWER({article_url_expr}), '\\\\s+', ' ', 'g')),"
        "'/+$',"
        "''"
        "),"
        "''"
        ")"
    )
    title_block_cap = (
        f"CASE WHEN title_key IS NULL OR title_key_count <= {max_title_block_size} THEN title_key ELSE NULL END"
        if max_title_block_size > 0
        else "title_key"
    )

    sql = f"""
        CREATE OR REPLACE TEMP TABLE journal_observations AS
        WITH base AS (
            SELECT
                id,
                {issn_expr} AS issn,
                {name_expr} AS name,
                {title_expr} AS metadata_preferred_title,
                {identifier_expr} AS metadata_preferred_identifier,
                {article_url_expr} AS metadata_preferred_article_url,
                CASE
                    WHEN LENGTH(REGEXP_REPLACE(UPPER(COALESCE({issn_expr}, '')), '[^0-9X]', '', 'g')) = 8
                        THEN REGEXP_REPLACE(UPPER(COALESCE({issn_expr}, '')), '[^0-9X]', '', 'g')
                    ELSE NULL
                END AS issn_key,
                {title_key_expr} AS title_key,
                {identifier_key_expr} AS metadata_identifier_key,
                {article_url_key_expr} AS metadata_article_url_key
            FROM read_parquet({sql_literal(str(input_path))})
            WHERE COALESCE(TRIM(COALESCE({title_expr}, {name_expr})), '') <> ''
               OR COALESCE(TRIM({issn_expr}), '') <> ''
               OR COALESCE(TRIM({identifier_expr}), '') <> ''
               OR COALESCE(TRIM({article_url_expr}), '') <> ''
        ),
        title_counted AS (
            SELECT
                *,
                COUNT(*) OVER (PARTITION BY title_key) AS title_key_count,
                {title_block_cap} AS title_key_blocked
            FROM base
        ),
        observations AS (
            SELECT
                MIN(id) AS representative_id,
                COUNT(*) AS source_row_count,
                issn,
                name,
                metadata_preferred_title AS title,
                metadata_preferred_identifier AS identifier,
                metadata_preferred_article_url AS article_url,
                issn_key,
                title_key_blocked AS title_key,
                metadata_identifier_key,
                metadata_article_url_key
            FROM title_counted
            GROUP BY
                issn,
                name,
                title,
                identifier,
                article_url,
                issn_key,
                title_key_blocked,
                metadata_identifier_key,
                metadata_article_url_key
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    COALESCE(title_key, ''),
                    COALESCE(issn_key, ''),
                    representative_id
            ) AS unique_id,
            representative_id,
            source_row_count,
            issn,
            name,
            COALESCE(title, name) AS canonical_name_candidate,
            issn_key,
            title_key,
            metadata_identifier_key,
            metadata_article_url_key,
            title AS metadata_title
        FROM observations
    """
    connection.execute(sql)
    observation_count = connection.execute(
        "SELECT COUNT(*) FROM journal_observations"
    ).fetchone()[0]
    represented_rows = connection.execute(
        "SELECT COALESCE(SUM(source_row_count), 0) FROM journal_observations"
    ).fetchone()[0]
    return observation_count, represented_rows


def build_splink_settings(metadata_availability: MetadataAvailability) -> SettingsCreator:
    if SettingsCreator is None or cl is None:
        raise RuntimeError("Splink is not installed")
    comparisons: list[object] = []
    blocking_rules: list[str] = []

    if metadata_availability.has_name or metadata_availability.has_metadata_title:
        comparisons.append(cl.ExactMatch("title_key"))
        blocking_rules.append("l.title_key = r.title_key AND l.title_key IS NOT NULL")

    if metadata_availability.has_metadata_identifier:
        comparisons.append(cl.ExactMatch("metadata_identifier_key"))
        blocking_rules.append(
            "l.metadata_identifier_key = r.metadata_identifier_key "
            "AND l.metadata_identifier_key IS NOT NULL"
        )

    if metadata_availability.has_metadata_article_url:
        comparisons.append(cl.ExactMatch("metadata_article_url_key"))
        blocking_rules.append(
            "l.metadata_article_url_key = r.metadata_article_url_key "
            "AND l.metadata_article_url_key IS NOT NULL"
        )

    if not comparisons or not blocking_rules:
        raise ValueError(
            "No dedupe comparison keys available. Need one of: metadata_title/name, "
            "metadata_identifier, metadata_article_url."
        )

    return SettingsCreator(
        link_type="dedupe_only",
        comparisons=comparisons,
        blocking_rules_to_generate_predictions=blocking_rules,
    )


def build_match_rule_case_sql(match_rules: tuple[str, ...]) -> str:
    return (
        "CASE CAST(match_key AS VARCHAR)\n"
        + "".join(
            f"    WHEN {sql_literal(str(idx))} THEN {sql_literal(rule_name)}\n"
            for idx, rule_name in enumerate(match_rules)
        )
        + "    ELSE 'match_key_' || CAST(match_key AS VARCHAR)\n"
        + "END"
    )


def run_splink_dedupe(
    connection: duckdb.DuckDBPyConnection,
    metadata_availability: MetadataAvailability,
) -> tuple[int, int, list[dict[str, object]]]:
    if DuckDBAPI is None or Linker is None:
        raise RuntimeError(
            "Splink is required for this script. Install it with `python -m pip install splink`."
        ) from SPLINK_IMPORT_ERROR

    relation = connection.table("journal_observations")
    linker = Linker(
        relation,
        build_splink_settings(metadata_availability=metadata_availability),
        DuckDBAPI(connection),
    )
    pairwise_matches = linker.inference.deterministic_link()
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(pairwise_matches)

    pairwise_matches.as_duckdbpyrelation().create_view("splink_pairs")
    clusters.as_duckdbpyrelation().create_view("splink_clusters")

    pair_count = connection.execute("SELECT COUNT(*) FROM splink_pairs").fetchone()[0]
    cluster_count = connection.execute(
        "SELECT COUNT(DISTINCT cluster_id) FROM splink_clusters"
    ).fetchone()[0]
    match_rules = metadata_availability.match_rules
    case_sql = build_match_rule_case_sql(match_rules=match_rules)
    rule_counts = connection.execute(
        f"""
        SELECT
            {case_sql} AS match_rule,
            COUNT(*) AS pair_count
        FROM splink_pairs
        GROUP BY 1
        ORDER BY 2 DESC, 1
        """
    ).fetchall()
    return cluster_count, pair_count, [
        {"match_rule": match_rule, "pair_count": pair_total}
        for match_rule, pair_total in rule_counts
    ]


def build_output_table(
    connection: duckdb.DuckDBPyConnection,
    match_rules: tuple[str, ...],
) -> tuple[int, int, int]:
    case_sql = build_match_rule_case_sql(match_rules=match_rules)
    sql = f"""
        CREATE OR REPLACE TEMP TABLE deduplicated_entities AS
        WITH edge_rules AS (
            SELECT
                unique_id_l,
                unique_id_r,
                {case_sql} AS match_rule
            FROM splink_pairs
        ),
        cluster_rules AS (
            SELECT
                cluster_id,
                LIST(DISTINCT match_rule ORDER BY match_rule) AS matched_rules
            FROM (
                SELECT c.cluster_id, e.match_rule
                FROM splink_clusters c
                JOIN edge_rules e
                    ON c.unique_id = e.unique_id_l
                UNION ALL
                SELECT c.cluster_id, e.match_rule
                FROM splink_clusters c
                JOIN edge_rules e
                    ON c.unique_id = e.unique_id_r
            )
            GROUP BY cluster_id
        ),
        ranked AS (
            SELECT
                cluster_id,
                unique_id,
                representative_id,
                source_row_count,
                issn,
                name,
                issn_key,
                title_key,
                canonical_name_candidate,
                ROW_NUMBER() OVER (
                    PARTITION BY cluster_id
                    ORDER BY
                        (issn_key IS NOT NULL) DESC,
                        source_row_count DESC,
                        LENGTH(COALESCE(canonical_name_candidate, '')) DESC,
                        representative_id
                ) AS canonical_rank
            FROM splink_clusters
        ),
        rollup AS (
            SELECT
                cluster_id AS dedupe_id,
                MAX(CASE WHEN canonical_rank = 1 THEN representative_id END) AS canonical_id,
                MAX(CASE WHEN canonical_rank = 1 THEN issn END) AS canonical_issn,
                MAX(CASE WHEN canonical_rank = 1 THEN canonical_name_candidate END) AS canonical_name,
                MAX(CASE WHEN canonical_rank = 1 THEN issn_key END) AS canonical_issn_key,
                MAX(CASE WHEN canonical_rank = 1 THEN title_key END) AS canonical_name_key,
                CAST(SUM(source_row_count) AS BIGINT) AS source_row_count,
                COUNT(*) AS journal_observation_count,
                COUNT(*) > 1 AS is_merged_cluster,
                LIST(unique_id ORDER BY canonical_rank, unique_id) AS matched_observation_ids,
                LIST(representative_id ORDER BY canonical_rank, representative_id) AS source_ids,
                LIST(DISTINCT issn ORDER BY issn) FILTER (WHERE issn IS NOT NULL) AS all_issns,
                LIST(DISTINCT canonical_name_candidate ORDER BY canonical_name_candidate) FILTER (WHERE canonical_name_candidate IS NOT NULL) AS all_names
            FROM ranked
            GROUP BY cluster_id
        )
        SELECT
            r.dedupe_id,
            r.canonical_id,
            r.canonical_issn,
            r.canonical_name,
            r.canonical_issn_key,
            r.canonical_name_key,
            r.source_row_count,
            r.journal_observation_count,
            r.is_merged_cluster,
            CASE
                WHEN cr.matched_rules IS NULL THEN ['singleton']
                ELSE cr.matched_rules
            END AS matched_rules,
            r.matched_observation_ids,
            r.source_ids,
            r.all_issns,
            r.all_names
        FROM rollup r
        LEFT JOIN cluster_rules cr
            ON r.dedupe_id = cr.cluster_id
        ORDER BY
            COALESCE(r.canonical_name_key, ''),
            COALESCE(r.canonical_issn_key, ''),
            r.canonical_id
    """
    connection.execute(sql)
    output_row_count, merged_cluster_count, merged_observation_count = connection.execute(
        """
        SELECT
            COUNT(*) AS output_rows,
            COUNT(*) FILTER (WHERE is_merged_cluster) AS merged_clusters,
            COALESCE(SUM(journal_observation_count - 1) FILTER (WHERE is_merged_cluster), 0) AS merged_observations
        FROM deduplicated_entities
        """
    ).fetchone()
    return output_row_count, merged_cluster_count, merged_observation_count


def write_output_parquet(
    connection: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    copy_sql = f"""
        COPY (
            SELECT * FROM deduplicated_entities
        )
        TO {sql_literal(str(output_path))}
        (FORMAT PARQUET, CODEC 'ZSTD')
    """
    connection.execute(copy_sql)


def build_summary_payload(
    *,
    input_stats: ParquetStats,
    output_stats: ParquetStats,
    input_path: Path,
    output_path: Path,
    match_rule_descriptions: tuple[str, ...],
    observation_count: int,
    represented_rows: int,
    cluster_count: int,
    pair_count: int,
    output_row_count: int,
    merged_cluster_count: int,
    merged_observation_count: int,
    rule_counts: list[dict[str, object]],
    args: argparse.Namespace,
) -> dict[str, object]:
    bytes_per_row = (
        output_stats.file_size_bytes / output_stats.row_count
        if output_stats.row_count
        else None
    )
    exact_duplicate_rows_removed = max(input_stats.row_count - observation_count, 0)
    return {
        "generated_at": timestamp_now(),
        "input_path": str(input_path),
        "output_path": str(output_path),
        "dedupe_strategy": {
            "approach": "splink_deterministic_dedupe",
            "description": (
                "Exact dedupe on OR-key matching. Exact duplicate raw rows are collapsed first, "
                "then Splink links when any configured exact key matches."
            ),
            "blocking_rules": list(match_rule_descriptions),
            "max_title_block_size": args.max_title_block_size,
        },
        "input_file": {
            "shape": {
                "rows": input_stats.row_count,
                "columns": input_stats.column_count,
            },
            "file_size_bytes": input_stats.file_size_bytes,
            "file_size_human": format_bytes(input_stats.file_size_bytes),
            "row_groups": input_stats.row_groups,
            "columns": [
                {"name": name, "type": column_type}
                for name, column_type in zip(input_stats.columns, input_stats.column_types)
            ],
        },
        "prepared_observations": {
            "journal_observation_count": observation_count,
            "represented_source_rows": represented_rows,
            "exact_duplicate_rows_removed": exact_duplicate_rows_removed,
        },
        "splink_pairs": {
            "pair_count": pair_count,
            "pair_counts_by_rule": rule_counts,
        },
        "output_file": {
            "shape": {
                "rows": output_row_count,
                "columns": output_stats.column_count,
            },
            "file_size_bytes": output_stats.file_size_bytes,
            "file_size_human": format_bytes(output_stats.file_size_bytes),
            "row_groups": output_stats.row_groups,
            "row_group_rows": {
                "min": output_stats.min_row_group_rows,
                "avg": output_stats.avg_row_group_rows,
                "max": output_stats.max_row_group_rows,
            },
            "bytes_per_row": bytes_per_row,
            "merged_cluster_count": merged_cluster_count,
            "merged_observation_count": merged_observation_count,
            "columns": [
                {"name": name, "type": column_type}
                for name, column_type in zip(output_stats.columns, output_stats.column_types)
            ],
        },
        "duckdb": {
            "memory_limit": args.memory_limit,
            "threads": args.threads,
            "temp_dir": str(args.temp_dir.resolve()),
        },
        "counts": {
            "cluster_count": cluster_count,
            "deduplicated_entity_count": output_row_count,
        },
    }


def write_summary_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def deduplicate(args: argparse.Namespace) -> None:
    if SPLINK_IMPORT_ERROR is not None:
        raise RuntimeError(
            "Splink is not installed. Install it with `python -m pip install splink`."
        ) from SPLINK_IMPORT_ERROR

    progress = ProgressReporter(
        disable=args.no_progress or not sys.stderr.isatty(),
        refresh_seconds=args.progress_refresh_seconds,
    )
    try:
        input_path = args.input.resolve()
        output_path = args.output.resolve()
        summary_path = output_path.with_name(f"{output_path.name}.summary.json")
        temp_output_path = output_path.with_name(f".{output_path.name}.tmp")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Input file not found: {input_path}. Run `python src/1_join_raw.py` first."
            )
        if output_path.exists() and not args.overwrite:
            raise FileExistsError(
                f"{output_path} already exists. Use --overwrite to replace it."
            )
        if temp_output_path.exists():
            temp_output_path.unlink()

        temp_dir = args.temp_dir.resolve()
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        progress.log(f"Deduplicating {input_path}")
        progress.log(f"Output -> {output_path}")
        progress.log(
            f"Using DuckDB memory_limit={args.memory_limit}, threads={args.threads}, temp_dir={temp_dir}"
        )

        progress.start_phase("Dedupe pipeline: inspect input")
        input_stats = inspect_parquet(input_path)
        metadata_availability = infer_metadata_availability(input_stats.columns)
        ensure_input_schema(input_stats, metadata_availability)
        progress.log(
            "Metadata availability:"
            f" issn={metadata_availability.has_issn},"
            f" metadata_title={metadata_availability.has_metadata_title},"
            f" metadata_identifier={metadata_availability.has_metadata_identifier},"
            f" metadata_article_url={metadata_availability.has_metadata_article_url},"
            f" name={metadata_availability.has_name}"
        )
        progress.log(
            f"Title match safety cap (observations per normalized title): {args.max_title_block_size}"
        )
        log_parquet_stats("Input", input_stats, progress)
        progress.complete_phase("Dedupe pipeline: input inspected")

        connection = duckdb.connect(database=":memory:")
        success = False
        observation_count = 0
        represented_rows = 0
        cluster_count = 0
        pair_count = 0
        output_row_count = 0
        merged_cluster_count = 0
        merged_observation_count = 0
        rule_counts: list[dict[str, object]] = []
        try:
            configure_duckdb(connection, args)

            progress.start_phase("Dedupe pipeline: prepare observations")
            progress.start_activity("Reducing raw rows to journal observations")
            observation_count, represented_rows = prepare_observation_table(
                connection,
                input_path,
                metadata_availability,
                args.max_title_block_size,
            )
            progress.stop_activity()
            progress.log(
                f"Prepared journal observations: {observation_count:,} distinct identity rows "
                f"representing {represented_rows:,} source rows"
            )
            progress.complete_phase("Dedupe pipeline: observations prepared")

            progress.start_phase("Dedupe pipeline: run Splink")
            progress.start_activity("Running Splink deterministic dedupe")
            cluster_count, pair_count, rule_counts = run_splink_dedupe(
                connection,
                metadata_availability,
            )
            progress.stop_activity()
            progress.log(
                f"Configured comparison rules: {', '.join(metadata_availability.match_rules)}"
            )
            progress.log(
                f"Splink produced {pair_count:,} matched pair(s) across {cluster_count:,} cluster(s)"
            )
            for rule_record in rule_counts:
                progress.log(
                    f"Match rule {rule_record['match_rule']}: {rule_record['pair_count']:,} pair(s)"
                )
            progress.complete_phase("Dedupe pipeline: Splink complete")

            progress.start_phase("Dedupe pipeline: write output")
            progress.start_activity("Aggregating clusters and writing parquet")
            output_row_count, merged_cluster_count, merged_observation_count = build_output_table(
                connection,
                metadata_availability.match_rules,
            )
            write_output_parquet(connection, temp_output_path)
            progress.stop_activity()
            temp_output_path.replace(output_path)
            progress.log(
                f"Wrote deduplicated entities: {output_row_count:,} rows, "
                f"{merged_cluster_count:,} merged cluster(s), {merged_observation_count:,} merged observation(s)"
            )
            progress.complete_phase("Dedupe pipeline: output written")

            progress.start_phase("Dedupe pipeline: inspect output")
            output_stats = inspect_parquet(output_path)
            log_parquet_stats("Output", output_stats, progress)
            summary_payload = build_summary_payload(
                input_stats=input_stats,
                output_stats=output_stats,
                input_path=input_path,
                output_path=output_path,
                match_rule_descriptions=metadata_availability.match_description,
                observation_count=observation_count,
                represented_rows=represented_rows,
                cluster_count=cluster_count,
                pair_count=pair_count,
                output_row_count=output_row_count,
                merged_cluster_count=merged_cluster_count,
                merged_observation_count=merged_observation_count,
                rule_counts=rule_counts,
                args=args,
            )
            write_summary_json(summary_path, summary_payload)
            progress.log(f"Wrote output summary -> {summary_path}")
            progress.complete_phase("Dedupe pipeline: done")
            success = True
        finally:
            progress.stop_activity()
            connection.close()
            if not success and temp_output_path.exists():
                temp_output_path.unlink()
            shutil.rmtree(temp_dir, ignore_errors=True)
    finally:
        progress.close()


def main() -> int:
    args = parse_args()
    try:
        deduplicate(args)
    except KeyboardInterrupt:
        print(f"[{timestamp_now()}] Interrupted.", file=sys.stderr, flush=True)
        return 130
    except Exception as exc:
        print(f"[{timestamp_now()}] Deduplication failed: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
