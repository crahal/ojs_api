#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


REQUIRED_TABLES = ("records", "contexts", "issns", "endpoints")
RECORD_METADATA_ALIAS = "_record_metadata"
WHITESPACE_RE = re.compile(r"\s+")

RECORD_SOURCE_COLUMNS = (
    "id",
    "context_id",
    "update_date",
    "publish_date",
    "removed_at",
    "created_at",
    "modified_at",
    "identifier",
)

CONTEXT_SOURCE_COLUMNS = (
    "id",
    "endpoint_id",
    "set_spec",
    "name",
    "group_id",
    "harvested_at",
    "sync_succeeded_at",
    "sync_started_at",
    "sync_failed_at",
    "errors",
    "failures",
    "last_error",
    "removed",
    "disabled",
    "created_at",
    "modified_at",
    "group_order",
    "doaj_id",
    "old_harvested_at",
    "old_sync_succeeded_at",
)

ISSN_SOURCE_COLUMNS = (
    "id",
    "context_id",
    "issn",
    "format",
    "marc",
    "country",
    "url",
    "created_at",
    "modified_at",
)

ENDPOINT_SOURCE_COLUMNS = (
    "id",
    "application",
    "oai_url",
    "oai_url_normalized",
    "stats_id",
    "host",
    "first_beacon",
    "last_beacon",
    "last_oai_response",
    "admin_email",
    "earliest_datestamp",
    "repository_name",
    "sync_succeeded_at",
    "sync_started_at",
    "sync_failed_at",
    "errors",
    "failures",
    "disabled",
    "last_error",
    "country_tld",
    "country_ip",
    "created_at",
    "modified_at",
)

WIDE_OUTPUT_COLUMNS = (
    ("c.id", "id"),
    ("c.id", "record_id"),
    ("c.context_id", "record_context_id"),
    ("c.update_date", "record_update_date"),
    ("c.publish_date", "record_publish_date"),
    ("c.identifier", "record_identifier"),
    ("c.created_at", "record_created_at"),
    ("c.modified_at", "record_modified_at"),
    ("c.removed_at", "record_removed_at"),
    ("b.id", "context_id"),
    ("b.endpoint_id", "context_endpoint_id"),
    ("b.set_spec", "context_set_spec"),
    ("b.name", "name"),
    ("b.name", "context_name"),
    ("b.group_id", "context_group_id"),
    ("b.harvested_at", "context_harvested_at"),
    ("b.sync_succeeded_at", "context_sync_succeeded_at"),
    ("b.sync_started_at", "context_sync_started_at"),
    ("b.sync_failed_at", "context_sync_failed_at"),
    ("b.errors", "context_errors"),
    ("b.failures", "context_failures"),
    ("b.last_error", "context_last_error"),
    ("b.removed", "context_removed"),
    ("b.disabled", "context_disabled"),
    ("b.created_at", "context_created_at"),
    ("b.modified_at", "context_modified_at"),
    ("b.group_order", "context_group_order"),
    ("b.doaj_id", "context_doaj_id"),
    ("b.old_harvested_at", "context_old_harvested_at"),
    ("b.old_sync_succeeded_at", "context_old_sync_succeeded_at"),
    ("d.id", "issn_id"),
    ("d.context_id", "issn_context_id"),
    ("d.issn", "issn"),
    ("d.format", "issn_format"),
    ("d.marc", "issn_marc"),
    ("d.country", "issn_country"),
    ("d.url", "issn_url"),
    ("d.created_at", "issn_created_at"),
    ("d.modified_at", "issn_modified_at"),
    ("e.id", "endpoint_id"),
    ("e.application", "endpoint_application"),
    ("e.oai_url", "endpoint_oai_url"),
    ("e.oai_url_normalized", "endpoint_oai_url_normalized"),
    ("e.stats_id", "endpoint_stats_id"),
    ("e.host", "endpoint_host"),
    ("e.first_beacon", "endpoint_first_beacon"),
    ("e.last_beacon", "endpoint_last_beacon"),
    ("e.last_oai_response", "endpoint_last_oai_response"),
    ("e.admin_email", "endpoint_admin_email"),
    ("e.earliest_datestamp", "endpoint_earliest_datestamp"),
    ("e.repository_name", "endpoint_repository_name"),
    ("e.sync_succeeded_at", "endpoint_sync_succeeded_at"),
    ("e.sync_started_at", "endpoint_sync_started_at"),
    ("e.sync_failed_at", "endpoint_sync_failed_at"),
    ("e.errors", "endpoint_errors"),
    ("e.failures", "endpoint_failures"),
    ("e.disabled", "endpoint_disabled"),
    ("e.last_error", "endpoint_last_error"),
    ("e.country_tld", "endpoint_country_tld"),
    ("e.country_ip", "endpoint_country_ip"),
    ("e.created_at", "endpoint_created_at"),
    ("e.modified_at", "endpoint_modified_at"),
)

METADATA_HEADER_FIELDS = (
    ("metadata_oai_identifier", "identifier"),
    ("metadata_oai_datestamp", "datestamp"),
    ("metadata_oai_set_spec", "setSpec"),
)

METADATA_SINGLE_FIELDS = (
    ("metadata_title", "title"),
    ("metadata_description", "description"),
    ("metadata_publisher", "publisher"),
    ("metadata_date", "date"),
    ("metadata_type", "type"),
    ("metadata_format", "format"),
    ("metadata_source", "source"),
    ("metadata_coverage", "coverage"),
    ("metadata_rights", "rights"),
)

METADATA_MULTI_FIELDS = (
    ("metadata_creator", "creator"),
    ("metadata_subject", "subject"),
    ("metadata_contributor", "contributor"),
    ("metadata_relation", "relation"),
    ("metadata_language", "language"),
)

METADATA_EXTRACT_FIELDS = (
    *tuple(name for name, _ in METADATA_HEADER_FIELDS),
    *tuple(name for name, _ in METADATA_SINGLE_FIELDS),
    *tuple(name for name, _ in METADATA_MULTI_FIELDS),
    "metadata_identifier",
    "metadata_article_url",
)


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


def format_schema_preview(columns: list[str], column_types: list[str], max_fields: int = 8) -> str:
    preview_pairs = [
        f"{column}:{column_type}"
        for column, column_type in zip(columns[:max_fields], column_types[:max_fields])
    ]
    if len(columns) > max_fields:
        preview_pairs.append(f"... +{len(columns) - max_fields} more")
    return ", ".join(preview_pairs)


def normalize_whitespace(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = WHITESPACE_RE.sub(" ", html.unescape(value).strip())
    return normalized or None


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output


def collect_text_values(parent: ET.Element | None, tag: str) -> list[str]:
    if parent is None:
        return []
    values: list[str] = []
    for node in parent.findall(f".//{{*}}{tag}"):
        value = normalize_whitespace("".join(node.itertext()).strip())
        if value:
            values.append(value)
    return values


def extract_metadata_fields(raw: str | bytes | bytearray | None) -> tuple[dict[str, str | None], bool | None]:
    defaults = {field_name: None for field_name in METADATA_EXTRACT_FIELDS}
    if raw is None:
        return defaults, None
    raw_text = raw.decode("utf-8", errors="ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    if not raw_text.strip():
        return defaults, None
    try:
        root = ET.fromstring(raw_text)
    except ET.ParseError:
        return defaults, False

    header = root.find(".//{*}header")
    metadata_root = root.find(".//{*}metadata")
    parsed: dict[str, str | None] = defaults.copy()

    for field_name, tag in METADATA_HEADER_FIELDS:
        parsed[field_name] = normalize_whitespace(" ".join(collect_text_values(header, tag)))
    for field_name, tag in METADATA_SINGLE_FIELDS:
        parsed[field_name] = normalize_whitespace(" ".join(collect_text_values(metadata_root, tag)))
    for field_name, tag in METADATA_MULTI_FIELDS:
        values = unique_preserve_order(collect_text_values(metadata_root, tag))
        parsed[field_name] = "; ".join(values)

    identifier_values = unique_preserve_order(collect_text_values(metadata_root, "identifier"))
    if identifier_values:
        parsed["metadata_identifier"] = "; ".join(identifier_values)
        for identifier in identifier_values:
            identifier_lower = identifier.lower()
            if identifier_lower.startswith("http://") or identifier_lower.startswith("https://"):
                parsed["metadata_article_url"] = identifier
                break

    return parsed, True


class ProgressReporter:
    def __init__(self, *, disable: bool, refresh_seconds: float) -> None:
        self.disable = disable
        self.refresh_seconds = refresh_seconds
        self.phase_bar = tqdm(
            total=4,
            desc="Join pipeline",
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

    def iter_tables(self, total: int):
        return tqdm(
            total=total,
            desc="Inspect inputs",
            unit="table",
            dynamic_ncols=True,
            leave=False,
            disable=self.disable,
            file=sys.stderr,
        )

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

    def update_activity(self, message: str) -> None:
        if self.disable or self.activity_bar is None:
            return
        self.activity_bar.set_postfix_str(
            f"{message}, ts={timestamp_now()}",
            refresh=True,
        )

    def close(self) -> None:
        self.stop_activity()
        if not self.disable:
            self.phase_bar.close()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def comma_join(columns: tuple[str, ...] | list[str]) -> str:
    return ", ".join(columns)


def build_output_columns() -> list[tuple[str, str]]:
    output_columns = list(WIDE_OUTPUT_COLUMNS)
    output_columns.append((RECORD_METADATA_ALIAS, RECORD_METADATA_ALIAS))
    return output_columns


def build_projection_list() -> dict[str, tuple[str, ...]]:
    records_columns = list(RECORD_SOURCE_COLUMNS)
    records_columns.append(f"metadata AS {RECORD_METADATA_ALIAS}")
    return {
        "records": tuple(records_columns),
        "contexts": CONTEXT_SOURCE_COLUMNS,
        "issns": ISSN_SOURCE_COLUMNS,
        "endpoints": ENDPOINT_SOURCE_COLUMNS,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Join the raw per-table parquet outputs into one wide flat parquet file "
            "using the Jonas query row set."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/raw_parquet"),
        help="Directory containing table parquet files. Default: %(default)s",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/jonied.parquet"),
        help="Output parquet path. Default: %(default)s",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output parquet if it already exists.",
    )
    parser.add_argument(
        "--stream-batch-rows",
        type=int,
        default=5000,
        help=(
            "Rows per Arrow batch when streaming the join to parquet. "
            "Required for metadata unpacking. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--include-record-metadata",
        action="store_true",
        help=(
            "Deprecated alias for backward compatibility. Metadata unpacking is now always enabled "
            "and writes metadata_* columns."
        ),
    )
    parser.add_argument(
        "--max-metadata-text",
        type=int,
        default=0,
        help=(
            "Optional byte cap for each parsed metadata field value. 0 means no cap. "
            "Applied while unpacking records.metadata."
        ),
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
        "--allow-parallel-streaming",
        action="store_true",
        help=(
            "Allow multi-threaded DuckDB execution during streaming metadata unpacking. "
            "Disabled by default because some environments hit native DuckDB/PyArrow segfaults."
        ),
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path("/tmp/ojs_api_duckdb_tmp"),
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
    args = parser.parse_args()
    if args.threads <= 0:
        parser.error("--threads must be a positive integer")
    if args.progress_refresh_seconds <= 0:
        parser.error("--progress-refresh-seconds must be a positive number")
    if args.stream_batch_rows <= 0:
        parser.error("--stream-batch-rows must be a positive integer")
    if args.max_metadata_text < 0:
        parser.error("--max-metadata-text must be zero or a positive integer")
    return args


def ensure_required_inputs(input_dir: Path) -> dict[str, Path]:
    table_paths: dict[str, Path] = {}
    missing: list[str] = []
    for table_name in REQUIRED_TABLES:
        path = input_dir / f"{table_name}.parquet"
        if path.exists():
            table_paths[table_name] = path.resolve()
        else:
            missing.append(str(path))
    if missing:
        missing_text = "\n".join(missing)
        raise FileNotFoundError(
            "Required parquet inputs are missing. Run src/0_convert_db.py until these "
            f"tables exist:\n{missing_text}"
        )
    return table_paths


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
    shape_text = f"{stats.row_count:,} x {stats.column_count}"
    logger.log(
        f"{prefix} {stats.path.name}: shape={shape_text}, {stats.row_groups} row groups, "
        f"{format_bytes(stats.file_size_bytes)}"
    )
    if stats.avg_row_group_rows is not None:
        logger.log(
            f"{prefix} row groups {stats.path.name}: min={stats.min_row_group_rows:,}, "
            f"avg={stats.avg_row_group_rows:,.1f}, max={stats.max_row_group_rows:,} rows/group"
        )
    logger.log(f"{prefix} schema {stats.path.name}: {format_schema_preview(stats.columns, stats.column_types)}")


def build_summary_payload(
    *,
    output_stats: ParquetStats,
    output_path: Path,
    input_stats_by_table: dict[str, ParquetStats],
    total_input_rows: int,
    total_input_bytes: int,
    memory_limit: str,
    threads: int,
    query: str,
    metadata_fields: tuple[str, ...],
    metadata_parse_failures: int,
    max_metadata_text: int,
    writer_strategy: str,
    stream_batch_rows: int | None,
) -> dict[str, object]:
    bytes_per_row = (
        output_stats.file_size_bytes / output_stats.row_count
        if output_stats.row_count
        else None
    )
    return {
        "generated_at": timestamp_now(),
        "output_path": str(output_path),
        "output_file_name": output_path.name,
        "shape": {
            "rows": output_stats.row_count,
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
        "columns": [
            {"name": name, "type": column_type}
            for name, column_type in zip(output_stats.columns, output_stats.column_types)
        ],
        "input_totals": {
            "rows": total_input_rows,
            "file_size_bytes": total_input_bytes,
            "file_size_human": format_bytes(total_input_bytes),
        },
        "input_tables": {
            table_name: {
                "path": str(stats.path),
                "shape": {
                    "rows": stats.row_count,
                    "columns": stats.column_count,
                },
                "file_size_bytes": stats.file_size_bytes,
                "file_size_human": format_bytes(stats.file_size_bytes),
                "row_groups": stats.row_groups,
                "row_group_rows": {
                    "min": stats.min_row_group_rows,
                    "avg": stats.avg_row_group_rows,
                    "max": stats.max_row_group_rows,
                },
                "columns": [
                    {"name": name, "type": column_type}
                    for name, column_type in zip(stats.columns, stats.column_types)
                ],
            }
            for table_name, stats in input_stats_by_table.items()
        },
        "duckdb": {
            "memory_limit": memory_limit,
            "threads": threads,
        },
        "join_projection": {
            "variant": "wide_denormalized",
            "metadata_fields": list(metadata_fields),
            "metadata_parse_failures": metadata_parse_failures,
            "max_metadata_text": max_metadata_text,
        },
        "writer": {
            "strategy": writer_strategy,
            "stream_batch_rows": stream_batch_rows,
        },
        "query_sql": query.strip(),
    }


def write_summary_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_join_query(table_paths: dict[str, Path]) -> str:
    records_path = sql_literal(str(table_paths["records"]))
    contexts_path = sql_literal(str(table_paths["contexts"]))
    issns_path = sql_literal(str(table_paths["issns"]))
    endpoints_path = sql_literal(str(table_paths["endpoints"]))
    projections = build_projection_list()
    output_columns = build_output_columns()
    select_sql = ",\n            ".join(
        f"{expression} AS {alias}"
        for expression, alias in output_columns
    )

    return f"""
        WITH
        records_src AS (
            SELECT {comma_join(projections["records"])}
            FROM read_parquet({records_path})
        ),
        contexts_src AS (
            SELECT {comma_join(projections["contexts"])}
            FROM read_parquet({contexts_path})
        ),
        issns_src AS (
            SELECT {comma_join(projections["issns"])}
            FROM read_parquet({issns_path})
        ),
        endpoints_src AS (
            SELECT {comma_join(projections["endpoints"])}
            FROM read_parquet({endpoints_path})
        )
        SELECT
            {select_sql}
        FROM records_src c
        INNER JOIN contexts_src b ON c.context_id = b.id
        INNER JOIN issns_src d ON b.id = d.context_id
        INNER JOIN endpoints_src e ON b.endpoint_id = e.id
    """


def choose_execution_threads(args: argparse.Namespace) -> int:
    if args.allow_parallel_streaming:
        return args.threads
    return 1


def build_output_schema(base_schema: pa.Schema) -> pa.Schema:
    keep_fields = [field for field in base_schema if field.name != RECORD_METADATA_ALIAS]
    return pa.schema(
        [
            *keep_fields,
            *[pa.field(field_name, pa.string()) for field_name in METADATA_EXTRACT_FIELDS],
        ]
    )


def apply_metadata_fields(
    batch: pa.RecordBatch,
    *,
    metadata_fields: tuple[str, ...],
    max_metadata_text: int,
) -> tuple[pa.Table, int]:
    metadata_index = batch.schema.get_field_index(RECORD_METADATA_ALIAS)
    if metadata_index < 0:
        raise ValueError(f"Missing expected metadata source column: {RECORD_METADATA_ALIAS}")

    metadata_rows = batch.column(metadata_index).to_pylist()
    parsed_values: dict[str, list[str | None]] = {
        field_name: [None] * batch.num_rows for field_name in metadata_fields
    }
    parse_failures = 0

    for row_idx, raw_metadata in enumerate(metadata_rows):
        extracted, parsed = extract_metadata_fields(raw_metadata)
        if parsed is False:
            parse_failures += 1
        for field_name in metadata_fields:
            value = extracted[field_name]
            if value is not None and max_metadata_text > 0:
                value = value[:max_metadata_text]
            parsed_values[field_name][row_idx] = value

    batch_arrays = [
        batch.column(index)
        for index in range(batch.num_columns)
        if index != metadata_index
    ]
    batch_field_names = [
        field.name for index, field in enumerate(batch.schema)
        if index != metadata_index
    ]
    table = pa.Table.from_arrays(batch_arrays, names=batch_field_names)
    for field_name in metadata_fields:
        table = table.append_column(field_name, pa.array(parsed_values[field_name], type=pa.string()))
    return table, parse_failures


def write_join_via_streaming(
    connection: duckdb.DuckDBPyConnection,
    *,
    query: str,
    output_path: Path,
    rows_per_batch: int,
    progress: ProgressReporter,
    max_metadata_text: int,
) -> tuple[int, int, int]:
    schema_table = connection.execute(f"{query}\nLIMIT 0").fetch_arrow_table()
    output_schema = build_output_schema(schema_table.schema)
    writer = pq.ParquetWriter(output_path, output_schema, compression="zstd")
    total_rows = 0
    batch_count = 0
    parse_failures = 0
    last_update = time.monotonic()
    try:
        connection.execute(query)
        reader = connection.fetch_record_batch(rows_per_batch=rows_per_batch)
        progress.log(
            f"Streaming join output in batches of up to {rows_per_batch:,} rows"
        )
        for batch in reader:
            if batch.num_rows == 0:
                continue
            output_batch, batch_failures = apply_metadata_fields(
                batch,
                metadata_fields=METADATA_EXTRACT_FIELDS,
                max_metadata_text=max_metadata_text,
            )
            writer.write_table(output_batch)
            total_rows += batch.num_rows
            parse_failures += batch_failures
            batch_count += 1
            now = time.monotonic()
            if now - last_update >= progress.refresh_seconds:
                progress.update_activity(
                    f"rows={total_rows:,} batches={batch_count:,}"
                )
                last_update = now
        progress.update_activity(f"rows={total_rows:,} batches={batch_count:,}")
    finally:
        writer.close()
    return total_rows, batch_count, parse_failures


def convert_query_to_parquet(args: argparse.Namespace) -> None:
    progress = ProgressReporter(
        disable=args.no_progress or not sys.stderr.isatty(),
        refresh_seconds=args.progress_refresh_seconds,
    )
    try:
        table_paths = ensure_required_inputs(args.input_dir.resolve())

        output_path = args.output.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_output_path = output_path.with_name(f".{output_path.name}.tmp")

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

        query = build_join_query(table_paths)
        execution_threads = choose_execution_threads(args)
        writer_strategy = "streaming_arrow_batches_with_metadata_unpack"

        progress.log(f"Joining parquet inputs from {args.input_dir.resolve()}")
        progress.log(f"Output -> {output_path}")
        progress.log(
            f"Using DuckDB memory_limit={args.memory_limit}, threads={execution_threads}, "
            f"temp_dir={temp_dir}"
        )
        if not args.allow_parallel_streaming and args.threads != execution_threads:
            progress.log(
                "Safety mode: forcing threads=1 for streaming metadata unpacking to avoid "
                "native DuckDB/PyArrow segfaults. Use --allow-parallel-streaming to opt in."
            )
        progress.log(
            "Projection variant=wide_denormalized, "
            f"metadata_fields={len(METADATA_EXTRACT_FIELDS)}"
        )
        progress.log(f"Writer strategy={writer_strategy}")
        if args.max_metadata_text:
            progress.log(
                f"Truncating metadata values to {format_bytes(args.max_metadata_text)} each when non-zero."
            )

        progress.start_phase("Join pipeline: inspect inputs")
        total_input_rows = 0
        total_input_bytes = 0
        input_stats_by_table: dict[str, ParquetStats] = {}
        with progress.iter_tables(len(REQUIRED_TABLES)) as table_bar:
            for table_name in REQUIRED_TABLES:
                stats = inspect_parquet(table_paths[table_name])
                input_stats_by_table[table_name] = stats
                total_input_rows += stats.row_count
                total_input_bytes += stats.file_size_bytes
                log_parquet_stats("Input", stats, progress)
                table_bar.set_postfix_str(f"last={table_name} ts={timestamp_now()}", refresh=False)
                table_bar.update(1)
        progress.log(
            f"Input totals: {total_input_rows:,} rows across {len(REQUIRED_TABLES)} tables, "
            f"{format_bytes(total_input_bytes)} on disk"
        )
        progress.complete_phase("Join pipeline: inputs inspected")

        connection = duckdb.connect(database=":memory:")
        success = False
        try:
            progress.start_phase("Join pipeline: running join")
            progress.start_activity("Running DuckDB join")
            connection.execute(
                f"SET memory_limit = {sql_literal(args.memory_limit)}"
            )
            connection.execute(f"SET threads = {execution_threads}")
            connection.execute(f"SET temp_directory = {sql_literal(str(temp_dir))}")
            connection.execute("SET preserve_insertion_order = false")
            written_rows, batch_count, parse_failures = write_join_via_streaming(
                connection,
                query=query,
                output_path=temp_output_path,
                rows_per_batch=args.stream_batch_rows,
                max_metadata_text=args.max_metadata_text,
                progress=progress,
            )
            progress.log(
                f"Streaming writer flushed {written_rows:,} rows across {batch_count:,} batch(es) "
                f"(metadata parse fallback count: {parse_failures:,})"
            )
            progress.stop_activity()
            progress.complete_phase("Join pipeline: join written")
            success = True
        finally:
            progress.stop_activity()
            connection.close()
            if not success and temp_output_path.exists():
                temp_output_path.unlink()
            shutil.rmtree(temp_dir, ignore_errors=True)

        temp_output_path.replace(output_path)

        progress.start_phase("Join pipeline: inspect output")
        output_stats = inspect_parquet(output_path)
        bytes_per_row = (
            output_stats.file_size_bytes / output_stats.row_count
            if output_stats.row_count
            else None
        )
        progress.log(
            f"Finished {output_path.name}: shape={output_stats.row_count:,} x "
            f"{output_stats.column_count}, {output_stats.row_groups} row groups, "
            f"{format_bytes(output_stats.file_size_bytes)}"
        )
        if output_stats.avg_row_group_rows is not None:
            progress.log(
                f"Output row groups {output_path.name}: min={output_stats.min_row_group_rows:,}, "
                f"avg={output_stats.avg_row_group_rows:,.1f}, max={output_stats.max_row_group_rows:,} rows/group"
            )
        if bytes_per_row is not None:
            progress.log(
                f"Output density {output_path.name}: {bytes_per_row:,.2f} bytes/row"
            )
        progress.log(
            f"Output fields {output_path.name}: "
            f"{', '.join(f'{name}:{column_type}' for name, column_type in zip(output_stats.columns, output_stats.column_types))}"
        )
        progress.complete_phase("Join pipeline: output inspected")

        summary_path = output_path.with_name(f"{output_path.name}.summary.json")
        progress.start_phase("Join pipeline: write summary")
        summary_payload = build_summary_payload(
            output_stats=output_stats,
            output_path=output_path,
            input_stats_by_table=input_stats_by_table,
            total_input_rows=total_input_rows,
            total_input_bytes=total_input_bytes,
            memory_limit=args.memory_limit,
            threads=execution_threads,
            query=query,
            metadata_fields=METADATA_EXTRACT_FIELDS,
            metadata_parse_failures=parse_failures,
            max_metadata_text=args.max_metadata_text,
            writer_strategy=writer_strategy,
            stream_batch_rows=(
                args.stream_batch_rows if writer_strategy.startswith("streaming") else None
            ),
        )
        write_summary_json(summary_path, summary_payload)
        progress.log(f"Wrote output summary -> {summary_path}")
        progress.complete_phase("Join pipeline: summary written")
    finally:
        progress.close()


def main() -> int:
    args = parse_args()
    try:
        convert_query_to_parquet(args)
    except KeyboardInterrupt:
        print(f"[{timestamp_now()}] Interrupted.", file=sys.stderr, flush=True)
        return 130
    except Exception as exc:
        print(f"[{timestamp_now()}] Join failed: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
