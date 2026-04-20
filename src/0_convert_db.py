#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import gzip
import io
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, TextIO

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


CREATE_TABLE_RE = re.compile(r"^CREATE TABLE `([^`]+)` \($")
INSERT_PREFIX_RE = re.compile(
    r"^INSERT INTO `([^`]+)`(?:\s+\((.*?)\))?\s+VALUES\s",
    re.DOTALL,
)
IN_ROW_SPECIAL_RE = re.compile(r"[',)]")
IN_STRING_SPECIAL_RE = re.compile(r"[\\']")
INT_BASE_TYPES = {"bigint", "int", "integer", "mediumint", "smallint", "tinyint"}
FLOAT_BASE_TYPES = {"double", "float", "real"}


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    sql_definition: str
    arrow_type: pa.DataType
    kind: str

    @property
    def sql_base_type(self) -> str:
        return extract_sql_base_type(self.sql_definition)

    @property
    def is_nullable(self) -> bool:
        return " not null" not in f" {self.sql_definition.strip().lower()} "

    @property
    def arrow_type_name(self) -> str:
        return str(self.arrow_type)


@dataclass
class TableSchema:
    name: str
    columns: list[ColumnSchema]
    index_by_name: dict[str, int] = field(init=False)
    arrow_schema: pa.Schema = field(init=False)

    def __post_init__(self) -> None:
        self.index_by_name = {column.name: idx for idx, column in enumerate(self.columns)}
        self.arrow_schema = pa.schema(
            [pa.field(column.name, column.arrow_type) for column in self.columns]
        )

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]


@dataclass(frozen=True)
class ParquetFileStats:
    row_count: int
    row_groups: int
    file_size_bytes: int


class ProgressReporter:
    def __init__(
        self,
        *,
        input_path: Path,
        disable: bool,
        refresh_seconds: float,
    ) -> None:
        self.disable = disable
        self.refresh_seconds = refresh_seconds
        self.last_refresh_at = 0.0
        self.phase = "Reading dump"
        self.current_table: str | None = None
        self.current_rows = 0
        self.written_tables = 0
        self.skipped_tables = 0
        total = None if input_path.suffix == ".gz" else input_path.stat().st_size
        unit = "chars" if total is None else "B"
        unit_scale = total is not None
        self.bar = tqdm(
            total=total,
            desc=self.phase,
            unit=unit,
            unit_scale=unit_scale,
            dynamic_ncols=True,
            leave=True,
            mininterval=refresh_seconds,
            disable=disable,
            file=sys.stderr,
        )
        self.refresh(force=True)

    def write(self, message: str) -> None:
        line = f"[{timestamp_now()}] {message}"
        if self.disable:
            print(line, file=sys.stderr, flush=True)
            return
        tqdm.write(line, file=sys.stderr)

    def advance(self, chunk: str) -> None:
        if self.disable:
            return
        self.bar.update(len(chunk))
        self.refresh()

    def set_phase(
        self,
        phase: str,
        *,
        table_name: str | None = None,
        clear_table: bool = False,
    ) -> None:
        self.phase = phase
        if table_name is not None:
            self.current_table = table_name
        elif clear_table:
            self.current_table = None
            self.current_rows = 0
        self.refresh(force=True)

    def start_table(self, table_name: str) -> None:
        self.phase = "Writing table"
        self.current_table = table_name
        self.current_rows = 0
        self.refresh(force=True)

    def update_table_rows(self, table_name: str, total_rows: int) -> None:
        self.current_table = table_name
        self.current_rows = total_rows
        self.refresh()

    def note_written_table(self, table_name: str, total_rows: int) -> None:
        self.written_tables += 1
        self.current_table = table_name
        self.current_rows = total_rows
        self.phase = "Reading dump"
        self.refresh(force=True)

    def note_skipped_table(self, table_name: str) -> None:
        self.skipped_tables += 1
        self.current_table = table_name
        self.phase = "Reading dump"
        self.refresh(force=True)

    def close(self, *, success: bool) -> None:
        if self.disable:
            return
        if self.bar.total is not None and self.bar.n < self.bar.total:
            self.bar.update(self.bar.total - self.bar.n)
        self.phase = "Done" if success else "Failed"
        self.refresh(force=True)
        self.bar.close()

    def refresh(self, *, force: bool = False) -> None:
        if self.disable:
            return
        now = time.monotonic()
        if not force and (now - self.last_refresh_at) < self.refresh_seconds:
            return
        description = self.phase
        if self.current_table:
            description = f"{self.phase}: {truncate_label(self.current_table, 32)}"
        postfix = (
            f"rows={self.current_rows:,} "
            f"written={self.written_tables:,} "
            f"skipped={self.skipped_tables:,} "
            f"ts={timestamp_now()}"
        )
        self.bar.set_description(description, refresh=False)
        self.bar.set_postfix_str(postfix, refresh=False)
        self.bar.refresh()
        self.last_refresh_at = now


class TableParquetWriter:
    def __init__(
        self,
        schema: TableSchema,
        output_dir: Path,
        compression: str,
        batch_rows: int,
        max_batch_bytes: int,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.schema = schema
        self.output_dir = output_dir
        self.compression = compression
        self.batch_rows = batch_rows
        self.max_batch_bytes = max_batch_bytes
        self.log = log
        self.final_path = output_dir / f"{schema.name}.parquet"
        self.temp_path = output_dir / f".{schema.name}.parquet.tmp"
        if self.temp_path.exists():
            self.temp_path.unlink()
        self.writer: pq.ParquetWriter | None = None
        self.buffers: list[list[object | None]] = [[] for _ in schema.columns]
        self.buffered_rows = 0
        self.buffered_bytes_estimate = 0
        self.total_rows = 0

    def append_insert_row(
        self,
        values: list[object | None],
        column_mapping: list[int] | None,
    ) -> None:
        if column_mapping is None:
            if len(values) != len(self.schema.columns):
                raise ValueError(
                    f"Table {self.schema.name} expected {len(self.schema.columns)} values, "
                    f"received {len(values)}"
                )
            for idx, value in enumerate(values):
                self.buffers[idx].append(value)
            row_for_size = values
        else:
            if len(values) != len(column_mapping):
                raise ValueError(
                    f"Table {self.schema.name} expected {len(column_mapping)} inserted values, "
                    f"received {len(values)}"
                )
            row = [None] * len(self.schema.columns)
            for inserted_idx, output_idx in enumerate(column_mapping):
                row[output_idx] = values[inserted_idx]
            for idx, value in enumerate(row):
                self.buffers[idx].append(value)
            row_for_size = row
        self.buffered_rows += 1
        self.buffered_bytes_estimate += estimate_row_size_bytes(row_for_size)
        self.total_rows += 1
        if (
            self.buffered_rows >= self.batch_rows
            or self.buffered_bytes_estimate >= self.max_batch_bytes
        ):
            self.flush()

    def flush(self) -> None:
        if self.buffered_rows == 0:
            return
        if self.writer is None:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(
                self.temp_path,
                self.schema.arrow_schema,
                compression=self.compression,
            )
        self._write_batch(self.buffers, self.buffered_rows)
        self.buffers = [[] for _ in self.schema.columns]
        self.buffered_rows = 0
        self.buffered_bytes_estimate = 0

    def _write_batch(
        self,
        column_buffers: list[list[object | None]],
        row_count: int,
    ) -> None:
        try:
            self._write_batch_once(column_buffers, row_count)
        except Exception as exc:
            if row_count <= 1:
                raise ValueError(
                    f"Failed to write a single-row batch for table {self.schema.name}: "
                    f"{format_row_preview(self.schema, column_buffers)}"
                ) from exc
            if self.log is not None:
                self.log(
                    f"Batch flush failed for {self.schema.name} "
                    f"({row_count:,} rows, est. {format_bytes(self.buffered_bytes_estimate)}): {exc}. "
                    "Retrying in smaller chunks."
                )
            self._write_batch_split(column_buffers, row_count)

    def _write_batch_once(
        self,
        column_buffers: list[list[object | None]],
        row_count: int,
    ) -> None:
        if self.writer is None:
            raise RuntimeError("Parquet writer must be initialized before writing")
        arrays = [
            pa.array(values, type=column.arrow_type)
            for values, column in zip(column_buffers, self.schema.columns)
        ]
        table = pa.Table.from_arrays(arrays, schema=self.schema.arrow_schema)
        self.writer.write_table(
            table,
            row_group_size=min(self.batch_rows, row_count),
        )

    def _write_batch_split(
        self,
        column_buffers: list[list[object | None]],
        row_count: int,
    ) -> None:
        split_point = row_count // 2
        left_buffers = [values[:split_point] for values in column_buffers]
        right_buffers = [values[split_point:] for values in column_buffers]
        for split_label, split_buffers, split_rows in (
            ("left", left_buffers, split_point),
            ("right", right_buffers, row_count - split_point),
        ):
            if split_rows == 0:
                continue
            try:
                self._write_batch_once(split_buffers, split_rows)
            except Exception as exc:
                if self.log is not None:
                    self.log(
                        f"Retry flush failed for {self.schema.name} "
                        f"{split_label} split ({split_rows:,} rows): {exc}"
                    )
                if split_rows <= 1:
                    raise ValueError(
                        f"Failed to write row for table {self.schema.name}: "
                        f"{format_row_preview(self.schema, split_buffers)}"
                    ) from exc
                self._write_batch_split(split_buffers, split_rows)

    def close(self, write_empty: bool = False) -> bool:
        if self.buffered_rows:
            self.flush()
        if self.writer is None and not write_empty:
            return False
        if self.writer is None:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(
                self.temp_path,
                self.schema.arrow_schema,
                compression=self.compression,
            )
            empty_arrays = [
                pa.array([], type=column.arrow_type) for column in self.schema.columns
            ]
            empty_table = pa.Table.from_arrays(empty_arrays, schema=self.schema.arrow_schema)
            self.writer.write_table(empty_table)
        self.writer.close()
        self.temp_path.replace(self.final_path)
        return True


class ConversionContext:
    def __init__(self, args: argparse.Namespace) -> None:
        self.output_dir = args.output_dir
        self.selected_tables = set(args.tables) if args.tables else None
        self.overwrite = args.overwrite
        self.skip_existing = args.skip_existing
        self.compression = args.compression
        self.batch_rows = args.batch_rows
        self.max_batch_bytes = args.max_batch_bytes
        self.report_every = args.report_every
        self.max_prefix_chars = args.max_prefix_chars
        self.max_field_chars = args.max_field_chars
        self.schemas: dict[str, TableSchema] = {}
        self.skipped_existing: set[str] = set()
        self.written_tables: set[str] = set()
        self.tables_with_rows: set[str] = set()
        self.open_writer: TableParquetWriter | None = None
        self.open_table_name: str | None = None
        self.progress = ProgressReporter(
            input_path=args.input,
            disable=args.no_progress or not sys.stderr.isatty(),
            refresh_seconds=args.progress_refresh_seconds,
        )
        self.table_stats_path = self.output_dir / "_table_stats.parquet"
        self.table_stats_json_path = self.output_dir / "_table_stats.json"
        self.column_stats_path = self.output_dir / "_column_stats.parquet"

    def log(self, message: str) -> None:
        self.progress.write(message)

    def wants_table(self, table_name: str) -> bool:
        return self.selected_tables is None or table_name in self.selected_tables

    def note_schema(self, schema: TableSchema) -> None:
        self.schemas[schema.name] = schema

    def require_schema(self, table_name: str) -> TableSchema:
        schema = self.schemas.get(table_name)
        if schema is None:
            raise KeyError(f"No schema found before INSERT for table {table_name}")
        return schema

    def should_convert(self, table_name: str) -> bool:
        if not self.wants_table(table_name):
            return False
        final_path = self.output_dir / f"{table_name}.parquet"
        if final_path.exists() and not self.overwrite:
            if self.skip_existing:
                if table_name not in self.skipped_existing:
                    self.skipped_existing.add(table_name)
                    self.progress.note_skipped_table(table_name)
                    self.log(f"Skipping existing {final_path}")
                return False
            raise FileExistsError(
                f"{final_path} already exists. Use --overwrite or --skip-existing."
            )
        return True

    def get_writer(self, table_name: str) -> TableParquetWriter:
        if self.open_writer is not None and self.open_table_name != table_name:
            self.close_open_writer()
        if self.open_writer is None:
            schema = self.require_schema(table_name)
            self.open_writer = TableParquetWriter(
                schema=schema,
                output_dir=self.output_dir,
                compression=self.compression,
                batch_rows=self.batch_rows,
                max_batch_bytes=self.max_batch_bytes,
                log=self.log,
            )
            self.open_table_name = table_name
            self.progress.start_table(table_name)
            self.log(f"Writing {table_name} -> {self.open_writer.final_path}")
        return self.open_writer

    def note_rows_written(self, table_name: str, total_rows: int) -> None:
        if table_name not in self.tables_with_rows:
            self.tables_with_rows.add(table_name)
        if total_rows == 1 or total_rows % self.batch_rows == 0:
            self.progress.update_table_rows(table_name, total_rows)
        if self.report_every and total_rows % self.report_every == 0:
            self.progress.update_table_rows(table_name, total_rows)
            self.log(f"{table_name}: wrote {total_rows:,} rows")

    def close_open_writer(self) -> None:
        if self.open_writer is None or self.open_table_name is None:
            return
        if self.open_writer.close():
            self.written_tables.add(self.open_table_name)
            self.progress.note_written_table(
                self.open_table_name,
                self.open_writer.total_rows,
            )
            self.log(
                f"Finished {self.open_table_name}: {self.open_writer.total_rows:,} rows"
            )
        self.open_writer = None
        self.open_table_name = None
        gc.collect()

    def write_empty_outputs(self) -> None:
        for table_name, schema in self.schemas.items():
            if not self.wants_table(table_name):
                continue
            if table_name in self.tables_with_rows:
                continue
            final_path = self.output_dir / f"{table_name}.parquet"
            if final_path.exists() and not self.overwrite:
                if self.skip_existing:
                    if table_name not in self.skipped_existing:
                        self.skipped_existing.add(table_name)
                        self.progress.note_skipped_table(table_name)
                        self.log(f"Skipping existing {final_path}")
                    continue
                raise FileExistsError(
                    f"{final_path} already exists. Use --overwrite or --skip-existing."
                )
            writer = TableParquetWriter(
                schema=schema,
                output_dir=self.output_dir,
                compression=self.compression,
                batch_rows=self.batch_rows,
                max_batch_bytes=self.max_batch_bytes,
                log=self.log,
            )
            self.progress.start_table(table_name)
            writer.close(write_empty=True)
            self.written_tables.add(table_name)
            self.progress.note_written_table(table_name, 0)
            self.log(f"Finished {table_name}: 0 rows")

    def get_missing_selected_tables(self) -> list[str]:
        if self.selected_tables is None:
            return []
        return sorted(self.selected_tables - self.schemas.keys())

    def iter_selected_schemas(self) -> list[TableSchema]:
        return [
            self.schemas[table_name]
            for table_name in sorted(self.schemas)
            if self.wants_table(table_name)
        ]

    def build_table_stats_records(self) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        for schema in self.iter_selected_schemas():
            final_path = self.output_dir / f"{schema.name}.parquet"
            parquet_stats = read_parquet_file_stats(final_path) if final_path.exists() else None
            row_count = parquet_stats.row_count if parquet_stats is not None else None
            row_groups = parquet_stats.row_groups if parquet_stats is not None else None
            file_size_bytes = (
                parquet_stats.file_size_bytes if parquet_stats is not None else None
            )
            file_size_mb = (
                round(file_size_bytes / (1024 * 1024), 3)
                if file_size_bytes is not None
                else None
            )
            bytes_per_row = (
                round(file_size_bytes / row_count, 3)
                if file_size_bytes is not None and row_count not in (None, 0)
                else None
            )
            records.append(
                {
                    "table_name": schema.name,
                    "parquet_path": str(final_path),
                    "parquet_exists": final_path.exists(),
                    "row_count": row_count,
                    "column_count": len(schema.columns),
                    "row_groups": row_groups,
                    "parquet_size_bytes": file_size_bytes,
                    "parquet_size_mb": file_size_mb,
                    "bytes_per_row": bytes_per_row,
                    "written_this_run": schema.name in self.written_tables,
                    "skipped_existing": schema.name in self.skipped_existing,
                    "field_names": [column.name for column in schema.columns],
                    "field_sql_types": [column.sql_base_type for column in schema.columns],
                    "field_arrow_types": [column.arrow_type_name for column in schema.columns],
                    "field_nullable": [column.is_nullable for column in schema.columns],
                    "field_sql_definitions": [
                        column.sql_definition for column in schema.columns
                    ],
                }
            )
        return records

    def build_column_stats_records(self) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        for schema in self.iter_selected_schemas():
            for ordinal_position, column in enumerate(schema.columns, start=1):
                records.append(
                    {
                        "table_name": schema.name,
                        "ordinal_position": ordinal_position,
                        "column_name": column.name,
                        "sql_definition": column.sql_definition,
                        "sql_base_type": column.sql_base_type,
                        "inferred_kind": column.kind,
                        "arrow_type": column.arrow_type_name,
                        "nullable": column.is_nullable,
                    }
                )
        return records

    def write_stats_outputs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.progress.set_phase("Writing stats", clear_table=True)
        table_records = self.build_table_stats_records()
        column_records = self.build_column_stats_records()

        write_records_as_parquet(
            records=table_records,
            schema=table_stats_schema(),
            path=self.table_stats_path,
            compression=self.compression,
        )
        write_records_as_parquet(
            records=column_records,
            schema=column_stats_schema(),
            path=self.column_stats_path,
            compression=self.compression,
        )
        with self.table_stats_json_path.open("w", encoding="utf-8") as handle:
            json.dump(table_records, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

        self.log(f"Wrote table stats -> {self.table_stats_path}")
        self.log(f"Wrote column stats -> {self.column_stats_path}")
        self.log(f"Wrote table stats JSON -> {self.table_stats_json_path}")

        preview = sorted(
            table_records,
            key=lambda record: (
                record["parquet_size_bytes"] is not None,
                record["parquet_size_bytes"] or -1,
            ),
            reverse=True,
        )[:5]
        for record in preview:
            row_count = record["row_count"]
            size_mb = record["parquet_size_mb"]
            row_count_display = f"{row_count:,}" if isinstance(row_count, int) else "unknown"
            size_display = f"{size_mb:,.3f} MiB" if isinstance(size_mb, float) else "unknown"
            row_groups = record["row_groups"]
            row_groups_display = (
                f"{row_groups:,}" if isinstance(row_groups, int) else "unknown"
            )
            self.log(
                f"Stats {record['table_name']}: {row_count_display} rows, "
                f"{record['column_count']} columns, {size_display}, "
                f"{row_groups_display} row groups"
            )

    def close_progress(self, *, success: bool) -> None:
        self.progress.close(success=success)


class InsertStatementHandler:
    def __init__(self, context: ConversionContext) -> None:
        self.context = context
        self.prefix_buffer = ""
        self.mode = "prefix"
        self.complete = False
        self.table_name: str | None = None
        self.schema: TableSchema | None = None
        self.writer: TableParquetWriter | None = None
        self.column_mapping: list[int] | None = None

        self.in_string = False
        self.escape_next = False
        self.in_row = False
        self.current_buffer = io.StringIO()
        self.current_field_chars = 0
        self.current_row: list[object | None] = []
        self.current_token_is_quoted = False

    def feed(self, chunk: str) -> bool:
        if self.complete:
            return True
        if self.mode == "prefix":
            self.prefix_buffer += chunk
            if len(self.prefix_buffer) > self.context.max_prefix_chars:
                raise ValueError(
                    "INSERT prefix exceeded the safety limit. "
                    "Increase --max-prefix-chars if needed."
                )
            match = INSERT_PREFIX_RE.match(self.prefix_buffer)
            if match is None:
                return False
            self.table_name = match.group(1)
            insert_columns = parse_insert_columns(match.group(2))
            self.schema = self.context.require_schema(self.table_name)
            self.column_mapping = build_column_mapping(self.schema, insert_columns)
            if self.context.should_convert(self.table_name):
                self.writer = self.context.get_writer(self.table_name)
                self.mode = "parse"
            else:
                self.mode = "skip"
            remainder = self.prefix_buffer[match.end() :]
            self.prefix_buffer = ""
            if remainder:
                self._consume(remainder)
            return self.complete

        self._consume(chunk)
        return self.complete

    def _consume(self, text: str) -> None:
        if self.mode == "skip":
            self._consume_skip(text)
        else:
            self._consume_parse(text)

    def _consume_skip(self, text: str) -> None:
        idx = 0
        text_len = len(text)
        while idx < text_len:
            if self.in_string:
                if self.escape_next:
                    self.escape_next = False
                    idx += 1
                    continue
                match = IN_STRING_SPECIAL_RE.search(text, idx)
                if match is None:
                    return
                idx = match.start() + 1
                if match.group() == "\\":
                    if idx >= text_len:
                        self.escape_next = True
                        return
                    idx += 1
                    continue
                self.in_string = False
                continue

            quote_idx = text.find("'", idx)
            semicolon_idx = text.find(";", idx)
            if quote_idx == -1 and semicolon_idx == -1:
                return
            if semicolon_idx != -1 and (quote_idx == -1 or semicolon_idx < quote_idx):
                self.complete = True
                return
            self.in_string = True
            idx = quote_idx + 1

    def _consume_parse(self, text: str) -> None:
        idx = 0
        text_len = len(text)
        while idx < text_len:
            if self.in_string:
                idx = self._consume_string_segment(text, idx)
                continue

            if not self.in_row:
                char = text[idx]
                idx += 1
                if char in " \t\r\n,":
                    continue
                if char == "(":
                    self.in_row = True
                    self.current_row = []
                    self._reset_token()
                    continue
                if char == ";":
                    self.complete = True
                    return
                raise ValueError(
                    f"Unexpected character {char!r} while parsing INSERT for {self.table_name}"
                )

            match = IN_ROW_SPECIAL_RE.search(text, idx)
            if match is None:
                self._append_fragment(text[idx:])
                return
            if match.start() > idx:
                self._append_fragment(text[idx : match.start()])
            special = match.group()
            idx = match.start() + 1
            if special == "'":
                prefix = self.current_buffer.getvalue().strip().lower()
                if prefix and not (prefix.startswith("_") or prefix in {"x", "b", "n"}):
                    raise ValueError(
                        f"Unexpected token prefix {prefix!r} before quoted value "
                        f"while parsing INSERT for {self.table_name}"
                    )
                self.current_buffer = io.StringIO()
                self.current_field_chars = 0
                self.in_string = True
                self.current_token_is_quoted = True
                continue
            if special == ",":
                self._end_value()
                continue
            self._end_value()
            self._end_row()
            self.in_row = False

    def _consume_string_segment(self, text: str, idx: int) -> int:
        text_len = len(text)
        if self.escape_next:
            if idx >= text_len:
                return idx
            self._append_fragment(decode_mysql_escape(text[idx]))
            self.escape_next = False
            return idx + 1

        while idx < text_len:
            match = IN_STRING_SPECIAL_RE.search(text, idx)
            if match is None:
                self._append_fragment(text[idx:])
                return text_len
            if match.start() > idx:
                self._append_fragment(text[idx : match.start()])
            special = match.group()
            idx = match.start() + 1
            if special == "\\":
                if idx >= text_len:
                    self.escape_next = True
                    return idx
                self._append_fragment(decode_mysql_escape(text[idx]))
                idx += 1
                continue
            self.in_string = False
            return idx
        return idx

    def _append_fragment(self, fragment: str) -> None:
        if not fragment:
            return
        self.current_buffer.write(fragment)
        self.current_field_chars += len(fragment)
        self._check_field_size()

    def _check_field_size(self) -> None:
        if self.current_field_chars > self.context.max_field_chars:
            raise ValueError(
                "A single field exceeded the safety limit. "
                "Increase --max-field-chars if the data is expected to contain very large values."
            )

    def _end_value(self) -> None:
        if self.schema is None:
            raise RuntimeError("Schema must be available before parsing values")
        column_idx = len(self.current_row)
        if self.column_mapping is None:
            target_idx = column_idx
        else:
            if column_idx >= len(self.column_mapping):
                raise ValueError(
                    f"Too many values while parsing INSERT for table {self.schema.name}"
                )
            target_idx = self.column_mapping[column_idx]
        if target_idx >= len(self.schema.columns):
            raise ValueError(
                f"Column index {target_idx} out of range for table {self.schema.name}"
            )
        raw_value = self.current_buffer.getvalue()
        value = coerce_sql_value(
            raw_value=raw_value,
            was_quoted=self.current_token_is_quoted,
            column=self.schema.columns[target_idx],
        )
        self.current_row.append(value)
        self._reset_token()

    def _end_row(self) -> None:
        if self.schema is None:
            raise RuntimeError("Schema must be available before ending a row")
        if self.writer is None:
            raise RuntimeError("Writer must be available before ending a row")
        expected_values = (
            len(self.schema.columns)
            if self.column_mapping is None
            else len(self.column_mapping)
        )
        if len(self.current_row) != expected_values:
            raise ValueError(
                f"Table {self.schema.name} expected {expected_values} values, "
                f"received {len(self.current_row)}"
            )
        self.writer.append_insert_row(self.current_row, self.column_mapping)
        self.context.note_rows_written(self.schema.name, self.writer.total_rows)
        self.current_row = []

    def _reset_token(self) -> None:
        self.current_buffer = io.StringIO()
        self.current_field_chars = 0
        self.current_token_is_quoted = False


def decode_mysql_escape(char: str) -> str:
    escapes = {
        "0": "\0",
        "b": "\b",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "Z": "\x1a",
        "\\": "\\",
        "'": "'",
        '"': '"',
    }
    return escapes.get(char, char)


def parse_insert_columns(raw_columns: str | None) -> list[str] | None:
    if not raw_columns:
        return None
    columns = re.findall(r"`([^`]+)`", raw_columns)
    if not columns:
        raise ValueError("Could not parse INSERT column list")
    return columns


def build_column_mapping(
    schema: TableSchema, insert_columns: list[str] | None
) -> list[int] | None:
    if insert_columns is None:
        return None
    mapping = []
    for column_name in insert_columns:
        try:
            mapping.append(schema.index_by_name[column_name])
        except KeyError as exc:
            raise KeyError(
                f"INSERT references unknown column {column_name!r} on table {schema.name}"
            ) from exc
    direct_mapping = list(range(len(schema.columns)))
    if mapping == direct_mapping:
        return None
    return mapping


def coerce_sql_value(
    raw_value: str,
    was_quoted: bool,
    column: ColumnSchema,
) -> object | None:
    if was_quoted:
        return raw_value

    token = raw_value.strip()
    if token == "":
        raise ValueError("Encountered an empty unquoted SQL value")
    upper = token.upper()
    if upper == "NULL" or token == r"\N":
        return None

    if column.kind == "integer":
        if token.lower().startswith("0x"):
            return int(token, 16)
        return int(token)
    if column.kind == "float":
        return float(token)
    return token


def infer_column_schema(name: str, sql_definition: str) -> ColumnSchema:
    lowered = sql_definition.strip().lower()
    base_type = extract_sql_base_type(sql_definition)

    if base_type in INT_BASE_TYPES:
        arrow_type = pa.uint64() if "unsigned" in lowered else pa.int64()
        kind = "integer"
    elif base_type in FLOAT_BASE_TYPES:
        arrow_type = pa.float64()
        kind = "float"
    else:
        arrow_type = pa.string()
        kind = "string"

    return ColumnSchema(
        name=name,
        sql_definition=sql_definition,
        arrow_type=arrow_type,
        kind=kind,
    )


def parse_column_definition(line: str) -> ColumnSchema | None:
    match = re.match(r"^\s*`([^`]+)`\s+(.*)$", line.rstrip("\n"))
    if match is None:
        return None
    column_name, sql_definition = match.groups()
    return infer_column_schema(column_name, sql_definition.rstrip(","))


def extract_sql_base_type(sql_definition: str) -> str:
    lowered = sql_definition.strip().lower()
    match = re.match(r"([a-z]+)", lowered)
    if match is not None:
        return match.group(1)
    return lowered.split()[0] if lowered else ""


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


def truncate_label(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3] + "..."


def preview_value(value: object | None, max_length: int = 120) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        normalized = value.replace("\n", "\\n").replace("\r", "\\r")
        return repr(truncate_label(normalized, max_length))
    return truncate_label(repr(value), max_length)


def format_row_preview(
    schema: TableSchema,
    column_buffers: list[list[object | None]],
    max_columns: int = 4,
) -> str:
    preview_parts: list[str] = []
    for index, column in enumerate(schema.columns[:max_columns]):
        values = column_buffers[index]
        value = values[0] if values else None
        preview_parts.append(f"{column.name}={preview_value(value)}")
    if len(schema.columns) > max_columns:
        preview_parts.append(f"... +{len(schema.columns) - max_columns} more columns")
    return ", ".join(preview_parts)


def estimate_value_size_bytes(value: object | None) -> int:
    if value is None:
        return 8
    if isinstance(value, bool):
        return 8
    if isinstance(value, int):
        return 16
    if isinstance(value, float):
        return 16
    if isinstance(value, str):
        return 49 + len(value)
    return 64


def estimate_row_size_bytes(values: list[object | None]) -> int:
    return 64 + sum(estimate_value_size_bytes(value) for value in values)


def read_parquet_file_stats(path: Path) -> ParquetFileStats:
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    return ParquetFileStats(
        row_count=metadata.num_rows,
        row_groups=metadata.num_row_groups,
        file_size_bytes=path.stat().st_size,
    )


def table_stats_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("table_name", pa.string()),
            pa.field("parquet_path", pa.string()),
            pa.field("parquet_exists", pa.bool_()),
            pa.field("row_count", pa.int64()),
            pa.field("column_count", pa.int64()),
            pa.field("row_groups", pa.int64()),
            pa.field("parquet_size_bytes", pa.int64()),
            pa.field("parquet_size_mb", pa.float64()),
            pa.field("bytes_per_row", pa.float64()),
            pa.field("written_this_run", pa.bool_()),
            pa.field("skipped_existing", pa.bool_()),
            pa.field("field_names", pa.list_(pa.string())),
            pa.field("field_sql_types", pa.list_(pa.string())),
            pa.field("field_arrow_types", pa.list_(pa.string())),
            pa.field("field_nullable", pa.list_(pa.bool_())),
            pa.field("field_sql_definitions", pa.list_(pa.string())),
        ]
    )


def column_stats_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("table_name", pa.string()),
            pa.field("ordinal_position", pa.int64()),
            pa.field("column_name", pa.string()),
            pa.field("sql_definition", pa.string()),
            pa.field("sql_base_type", pa.string()),
            pa.field("inferred_kind", pa.string()),
            pa.field("arrow_type", pa.string()),
            pa.field("nullable", pa.bool_()),
        ]
    )


def write_records_as_parquet(
    records: list[dict[str, object]],
    schema: pa.Schema,
    path: Path,
    compression: str,
) -> None:
    if records:
        table = pa.Table.from_pylist(records, schema=schema)
    else:
        arrays = [pa.array([], type=field.type) for field in schema]
        table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, path, compression=compression)


def open_sql_dump(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="")
    return path.open("r", encoding="utf-8", errors="replace", newline="")


def convert_dump(args: argparse.Namespace) -> None:
    context = ConversionContext(args)
    current_create_table: str | None = None
    current_columns: list[ColumnSchema] = []
    pending_line = ""
    insert_handler: InsertStatementHandler | None = None
    success = False
    try:
        with open_sql_dump(args.input) as handle:
            while True:
                chunk = handle.readline(args.read_chars)
                if chunk == "":
                    break
                context.progress.advance(chunk)

                if insert_handler is not None:
                    if insert_handler.feed(chunk):
                        insert_handler = None
                    continue

                if not pending_line and chunk.startswith("INSERT INTO "):
                    insert_handler = InsertStatementHandler(context)
                    if insert_handler.feed(chunk):
                        insert_handler = None
                    continue

                pending_line += chunk
                if len(pending_line) > args.max_non_insert_chars:
                    raise ValueError(
                        "A non-INSERT line exceeded the safety limit. "
                        "Increase --max-non-insert-chars if needed."
                    )
                if not chunk.endswith("\n"):
                    continue

                line = pending_line
                pending_line = ""
                stripped = line.strip()
                if not stripped:
                    continue

                if current_create_table is not None:
                    if stripped.startswith(")"):
                        context.note_schema(
                            TableSchema(name=current_create_table, columns=current_columns)
                        )
                        current_create_table = None
                        current_columns = []
                        continue
                    column = parse_column_definition(line)
                    if column is not None:
                        current_columns.append(column)
                    continue

                if stripped == "UNLOCK TABLES;":
                    context.close_open_writer()
                    continue

                create_match = CREATE_TABLE_RE.match(stripped)
                if create_match is not None:
                    context.close_open_writer()
                    current_create_table = create_match.group(1)
                    current_columns = []
                    continue

            if insert_handler is not None and not insert_handler.complete:
                raise ValueError("Reached EOF while still parsing an INSERT statement")
            if pending_line.strip():
                raise ValueError("Reached EOF with an incomplete non-INSERT line")

        context.close_open_writer()
        context.write_empty_outputs()
        missing_selected_tables = context.get_missing_selected_tables()
        if missing_selected_tables:
            context.log(
                "Selected tables not found in dump: "
                + ", ".join(missing_selected_tables)
            )
        context.write_stats_outputs()
        context.log(
            f"Done. Wrote {len(context.written_tables):,} parquet file(s); "
            f"skipped {len(context.skipped_existing):,} existing file(s)."
        )
        success = True
    finally:
        context.close_progress(success=success)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stream a MySQL dump into one Parquet file per table without loading the "
            "full dump into memory."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw_sql/database.sql"),
        help="Path to the mysqldump file (.sql or .sql.gz). Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw_parquet"),
        help="Directory for per-table parquet files. Default: %(default)s",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Optional list of table names to convert. Default: convert every table found.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing parquet outputs.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tables whose parquet output already exists.",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=5_000,
        help="Rows buffered before each Parquet write. Default: %(default)s",
    )
    parser.add_argument(
        "--max-batch-bytes",
        type=int,
        default=134_217_728,
        help=(
            "Estimated in-memory batch size cap before each Parquet write. "
            "This protects wide/text-heavy tables even when --batch-rows is large. "
            "Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec. Default: %(default)s",
    )
    parser.add_argument(
        "--report-every",
        type=int,
        default=100_000,
        help="Progress report interval in rows per table. Default: %(default)s",
    )
    parser.add_argument(
        "--read-chars",
        type=int,
        default=1_048_576,
        help=(
            "Maximum characters read from the dump at a time. This caps memory even if "
            "mysqldump emitted very long INSERT lines. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--max-prefix-chars",
        type=int,
        default=1_048_576,
        help="Safety cap for an INSERT prefix before VALUES. Default: %(default)s",
    )
    parser.add_argument(
        "--max-field-chars",
        type=int,
        default=67_108_864,
        help="Safety cap for a single parsed field value. Default: %(default)s",
    )
    parser.add_argument(
        "--max-non-insert-chars",
        type=int,
        default=1_048_576,
        help="Safety cap for non-INSERT lines. Default: %(default)s",
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
        help="Minimum seconds between progress bar refreshes. Default: %(default)s",
    )
    args = parser.parse_args()
    if args.batch_rows <= 0:
        parser.error("--batch-rows must be a positive integer")
    if args.max_batch_bytes <= 0:
        parser.error("--max-batch-bytes must be a positive integer")
    if args.read_chars <= 0:
        parser.error("--read-chars must be a positive integer")
    if args.max_prefix_chars <= 0:
        parser.error("--max-prefix-chars must be a positive integer")
    if args.max_field_chars <= 0:
        parser.error("--max-field-chars must be a positive integer")
    if args.max_non_insert_chars <= 0:
        parser.error("--max-non-insert-chars must be a positive integer")
    if args.progress_refresh_seconds <= 0:
        parser.error("--progress-refresh-seconds must be a positive number")
    return args


def main() -> int:
    args = parse_args()
    if not args.input.exists():
        print(f"[{timestamp_now()}] Input file not found: {args.input}", file=sys.stderr)
        return 1
    try:
        convert_dump(args)
    except KeyboardInterrupt:
        print(f"[{timestamp_now()}] Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"[{timestamp_now()}] Conversion failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
