#!/usr/bin/env python3
"""Run the reproducible PKP Beacon raw-to-temporal-SQL pipeline."""

from __future__ import annotations

import argparse
import concurrent.futures
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Callable

from download_beacon import read_validated_metadata, validate_cached_snapshot
from progress_logging import Progress, progress_interval
from source_activity import (
    SourceActivityBusy, SourceActivityError, source_activity,
    source_activity_child_options,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
DEFAULT_BUILD_SQL = PROJECT_ROOT / "sql" / "01_build_ojs_tables.sql"
DEFAULT_DATABASE = "pkpbeacon_db"
PIPELINE_VERSION = "3"
BUFFER_SIZE = 8 * 1024 * 1024
METADATA_SHARDS_PER_WORKER = 32
METADATA_BEGIN_MARKER = "-- OJS_PIPELINE_METADATA_BEGIN"
METADATA_END_MARKER = "-- OJS_PIPELINE_METADATA_END"
BUILD_STATE_NAME = "OJS_PIPELINE_STATE.json"
SNAPSHOT_NAME_RE = re.compile(r"pkpbeacon-(\d{4}-\d{2}-\d{2})\.sql(?:\.gz)?$")
CLEAN_EXPORT_NAME_RE = re.compile(
    r"pkpbeacon-clean-(\d{4}-\d{2}-\d{2})\.sql\.gz$"
)
DUMP_FOOTER_RE = re.compile(
    rb"-- Dump completed on (\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}:\d{2})"
)
CLEAN_EXPORT_TABLES = (
    "ojs_snapshots",
    "ojs_articles",
    "ojs_article_sources",
    "ojs_article_keys",
    "ojs_article_events",
)
API_READ_TABLES = (
    "ojs_snapshots",
    "ojs_articles",
    "ojs_article_sources",
    "ojs_article_events",
)
CHANGE_EVENT_TYPES = ("added", "modified", "removed", "restored", "merged")
CHANGE_OPERATIONS = ("upsert", "delete")
SQL_PROGRESS_STAGES = frozenset({
    "schema", "context_normalization", "source_index",
    "source_identity_validation", "metadata_extraction", "exact_keys",
    "effective_keys", "noisy_keys", "touched_keys", "existing_candidates",
    "initial_assignments", "deduplication", "article_merges",
    "source_reconciliation", "source_indexes", "key_reconciliation",
    "article_rollup", "canonical_payload", "article_state", "article_events",
    "article_updates", "snapshot_counts", "stage_cleanup", "sql_complete",
})
SAFE_SQL_ERRORS = (
    "source record ID was reused with a different identity tuple",
    "identity labels did not converge within 64 passes",
    "snapshot has already been applied to the clean catalogue",
    "@ojs_snapshot_date must be set by the pipeline",
)


def consume_sql_progress(line: bytes, progress: Progress) -> None:
    """Forward only complete protocol rows; SQL results are never log messages."""
    fields = line.rstrip(b"\r\n").split(b"\t")
    if len(fields) < 3 or fields[0] != b"OJS_PROGRESS_V1":
        return
    if fields[1] == b"stage" and len(fields) == 3:
        stage = fields[2].decode("ascii", errors="replace")
        if stage in SQL_PROGRESS_STAGES:
            progress.event("sql-stage", step=stage)
    elif fields[1] == b"dedup_pass" and len(fields) == 4:
        if all(re.fullmatch(rb"[0-9]{1,20}", item) for item in fields[2:]):
            pass_number, changed_rows = map(int, fields[2:])
            if 1 <= pass_number <= 64:
                progress.event(
                    "dedup-pass", step="deduplication",
                    pass_number=pass_number, changed_rows=changed_rows,
                )
    elif fields[1] == b"metadata_rows" and len(fields) == 3:
        if re.fullmatch(rb"[0-9]{1,20}", fields[2]):
            progress.event("metadata-rows", extracted_rows=int(fields[2]))


def bounded_lines(stream: BinaryIO):
    """Discard oversized lines without allocating memory proportional to output."""
    oversized = False
    while fragment := stream.readline(512):
        complete = fragment.endswith(b"\n")
        if complete and not oversized:
            yield fragment
        oversized = not complete


def safe_mysql_error(line: bytes) -> str | None:
    match = re.match(rb"ERROR ([0-9]{1,5})(?: \([A-Z0-9]{5}\))?(?: at line ([0-9]{1,10}))?:", line)
    if not match:
        return None
    detail = f"mysql_error={int(match[1])}"
    if match[2]:
        detail += f" sql_line={int(match[2])}"
    for message in SAFE_SQL_ERRORS:
        if message.encode("ascii") in line:
            detail += f" ({message})"
            break
    return detail


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        try:
            process.terminate()
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


class PipelineError(RuntimeError):
    pass


class PipelineInterrupted(KeyboardInterrupt):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(f"signal {signum}")


@contextmanager
def interruptible_pipeline():
    """Turn termination into normal cleanup; repeated signals cannot break it."""
    previous = {}
    if threading.current_thread() is threading.main_thread():
        def interrupted(signum, frame):
            for watched in (signal.SIGINT, signal.SIGTERM):
                signal.signal(watched, signal.SIG_IGN)
            raise PipelineInterrupted(signum)

        for watched in (signal.SIGINT, signal.SIGTERM):
            previous[watched] = signal.signal(watched, interrupted)
    try:
        yield
    finally:
        for watched, handler in previous.items():
            signal.signal(watched, handler)


@dataclass(frozen=True)
class Snapshot:
    path: Path
    version: str
    completed_at: datetime
    size: int


@dataclass(frozen=True)
class CleanExport:
    path: Path
    version: str


@dataclass(frozen=True)
class BuildCounts:
    source_records: int
    active_sources: int
    articles: int
    active_articles: int
    removed_articles: int
    merged_articles: int
    events: int


@dataclass(frozen=True)
class ReleaseThresholds:
    active_source_drop_fraction: float
    active_article_drop_fraction: float
    article_removal_fraction: float
    article_merge_fraction: float

    def as_dict(self) -> dict[str, float]:
        return {
            "active_source_drop_fraction": self.active_source_drop_fraction,
            "active_article_drop_fraction": self.active_article_drop_fraction,
            "article_removal_fraction": self.article_removal_fraction,
            "article_merge_fraction": self.article_merge_fraction,
        }


@dataclass(frozen=True)
class BuildSQLParts:
    prefix: str
    metadata: str
    suffix: str


@dataclass(frozen=True)
class MySQLTools:
    mysqld: str
    mysql: str
    mysqladmin: str
    mysqldump: str
    version: str

    @classmethod
    def discover(cls) -> "MySQLTools":
        paths: dict[str, str] = {}
        for name in ("mysqld", "mysql", "mysqladmin", "mysqldump"):
            path = shutil.which(name)
            if path is None:
                raise PipelineError(
                    f"required MySQL executable is not on PATH: {name}"
                )
            paths[name] = path
        version_result = subprocess.run(
            [paths["mysqld"], "--version"],
            text=True,
            capture_output=True,
        )
        version_output = (
            version_result.stdout.strip() or version_result.stderr.strip()
        )
        version_match = re.search(r"\bVer\s+(\d+\.\d+\.\d+)", version_output)
        if version_result.returncode != 0 or version_match is None:
            raise PipelineError(
                f"could not determine MySQL server version: {version_output}"
            )
        version = version_match.group(1)
        if not version.startswith("8.4."):
            raise PipelineError(
                f"MySQL 8.4 is required for reproducible data directories; found {version}"
            )
        return cls(**paths, version=version)


def format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return str(value)


def inspect_snapshot(path: Path) -> Snapshot:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise PipelineError(f"SQL snapshot does not exist: {resolved}")

    name_match = SNAPSHOT_NAME_RE.fullmatch(resolved.name)
    if not name_match:
        raise PipelineError(
            "snapshot name must use pkpbeacon-YYYY-MM-DD.sql[.gz]: "
            f"{resolved.name}"
        )

    if resolved.name.endswith(".gz"):
        try:
            metadata = validate_cached_snapshot(resolved)
            completed_at = datetime.strptime(
                metadata["dump_datetime"], "%Y-%m-%d %H:%M:%S"
            )
            size = int(metadata["uncompressed_size"])
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            raise PipelineError(f"invalid compressed snapshot {resolved}: {exc}") from exc
        version = completed_at.date().isoformat()
        if name_match.group(1) != version:
            raise PipelineError(
                f"snapshot filename date {name_match.group(1)} does not match "
                f"dump timestamp {version}"
            )
        return Snapshot(resolved, version, completed_at, size)

    size = resolved.stat().st_size
    with resolved.open("rb") as source:
        prefix = source.read(4096)
        source.seek(max(0, size - 8192))
        tail = source.read()
    if b"MySQL dump" not in prefix:
        raise PipelineError(f"snapshot is not a MySQL dump: {resolved}")

    footer_match = DUMP_FOOTER_RE.search(tail)
    if footer_match is None:
        raise PipelineError(f"snapshot has no MySQL dump timestamp: {resolved}")
    footer_date = footer_match.group(1).decode("ascii")
    footer_time = footer_match.group(2).decode("ascii")
    filename_date = name_match.group(1)
    if filename_date != footer_date:
        raise PipelineError(
            f"snapshot filename date {filename_date} does not match "
            f"dump timestamp {footer_date}"
        )
    completed_at = datetime.strptime(
        f"{footer_date} {footer_time}",
        "%Y-%m-%d %H:%M:%S",
    )
    return Snapshot(
        path=resolved,
        version=footer_date,
        completed_at=completed_at,
        size=size,
    )


def discover_snapshots(raw_dir: Path, through: str | None = None) -> list[Snapshot]:
    snapshots: dict[str, Snapshot] = {}
    for path in sorted(raw_dir.glob("pkpbeacon-????-??-??.sql*")):
        match = SNAPSHOT_NAME_RE.fullmatch(path.name)
        if match is None or (through is not None and match.group(1) > through):
            continue
        # Prefer the archive when both representations of a date exist.
        snapshots[match.group(1)] = inspect_snapshot(path)
    return sorted(snapshots.values(), key=lambda item: item.version)


def open_snapshot(path: Path) -> BinaryIO:
    return gzip.open(path, "rb") if path.name.endswith(".gz") else path.open("rb")


def snapshot_sha256(snapshot: Snapshot, progress_seconds: float = 30) -> str:
    """Hash logical SQL bytes, independent of their storage representation."""
    digest = hashlib.sha256()
    processed = 0
    with Progress(
        "snapshot-checksum", interval=progress_seconds,
        total_bytes=snapshot.size, processed_bytes=0, disk_path=snapshot.path.parent,
    ) as progress, open_snapshot(snapshot.path) as source:
        while chunk := source.read(BUFFER_SIZE):
            digest.update(chunk)
            processed += len(chunk)
            progress.update(processed_bytes=processed)
        if processed != snapshot.size:
            raise PipelineError("snapshot size changed while hashing logical SQL")
    return digest.hexdigest()


def release_manifest_path(clean_dir: Path, version: str) -> Path:
    return clean_dir / f"pkpbeacon-release-{version}.json"


def _declared_checksum(path: Path) -> str | None:
    sidecar = path.with_name(f"{path.name}.sha256")
    if not path.is_file() or not sidecar.is_file():
        return None
    try:
        fields = sidecar.read_text(encoding="ascii").strip().split()
    except (OSError, UnicodeError):
        return None
    if not fields or re.fullmatch(r"[0-9a-f]{64}", fields[0]) is None:
        return None
    return fields[0]


def release_artifacts_complete(clean_dir: Path, version: str) -> bool:
    """Use the atomic manifest as the commit marker for a built release."""
    manifest_path = release_manifest_path(clean_dir, version)
    export_path = clean_dir / f"pkpbeacon-clean-{version}.sql.gz"
    report_path = clean_dir / f"pkpbeacon-changes-{version}.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = {
            "schema_version": 1,
            "snapshot_date": version,
            "clean_export_filename": export_path.name,
            "clean_export_sha256": _declared_checksum(export_path),
            "change_report_filename": report_path.name,
            "change_report_sha256": _declared_checksum(report_path),
        }
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return all(manifest.get(key) == value for key, value in expected.items()) and all(
        value is not None for key, value in expected.items() if key.endswith("sha256")
    )


def retained_snapshot_matches_release(snapshot: Snapshot, clean_dir: Path) -> bool:
    """Recognize completed history after its bulky clean export was pruned.

    Require the retained commit marker and checksummed report to bind the input
    to its original SQL hash. Only a current, stat-bound gzip validation cache
    can supply that hash without rescanning a potentially enormous raw file.
    """
    if not snapshot.path.name.endswith(".gz"):
        return False
    metadata = read_validated_metadata(snapshot.path)
    if metadata is None:
        return False
    report_path = change_report_path(clean_dir, snapshot.version)
    try:
        documents = []
        for path in (release_manifest_path(clean_dir, snapshot.version), report_path):
            with path.open("rb") as source:
                content = source.read(BUFFER_SIZE + 1)
            if len(content) > BUFFER_SIZE:
                return False
            document = json.loads(content)
            if not isinstance(document, dict):
                return False
            documents.append(document)
        manifest, report = documents
        report_sha256 = hashlib.sha256(content).hexdigest()
        expected_manifest = {
            "schema_version": 1,
            "snapshot_date": snapshot.version,
            "clean_export_filename": clean_export_path(clean_dir, snapshot.version).name,
            "change_report_filename": report_path.name,
            "change_report_sha256": report_sha256,
        }
        if any(manifest.get(key) != value for key, value in expected_manifest.items()):
            return False
        export_sha256 = manifest.get("clean_export_sha256")
        if not isinstance(export_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", export_sha256):
            return False
        if _declared_checksum(report_path) != report_sha256:
            return False
        recorded = report.get("snapshot")
        if report.get("schema_version") != 1 or not isinstance(recorded, dict):
            return False
        expected_source = {
            "date": snapshot.version,
            "completed_at": snapshot.completed_at.isoformat(),
            "source_size_bytes": snapshot.size,
            "source_sha256": metadata["uncompressed_sha256"],
        }
        # Compression may change the filename without changing the logical SQL.
        source_names = {
            f"pkpbeacon-{snapshot.version}.sql",
            f"pkpbeacon-{snapshot.version}.sql.gz",
        }
        return (
            metadata["dump_date"] == snapshot.version
            and metadata["dump_datetime"] == snapshot.completed_at.isoformat(sep=" ")
            and metadata["uncompressed_size"] == snapshot.size
            and recorded.get("source_filename") in source_names
            and all(recorded.get(key) == value for key, value in expected_source.items())
        )
    except (OSError, UnicodeError, ValueError, TypeError, KeyError):
        return False


def discover_clean_exports(
    clean_dir: Path,
    *,
    require_complete: bool = False,
) -> list[CleanExport]:
    exports: list[CleanExport] = []
    for path in clean_dir.glob("pkpbeacon-clean-????-??-??.sql.gz"):
        match = CLEAN_EXPORT_NAME_RE.fullmatch(path.name)
        if match is None or not path.is_file():
            continue
        version = match.group(1)
        if require_complete and not release_artifacts_complete(clean_dir, version):
            continue
        exports.append(CleanExport(path.resolve(), version))
    return sorted(exports, key=lambda item: item.version)


def previous_clean_export(clean_dir: Path, version: str) -> CleanExport | None:
    candidates = [
        item for item in discover_clean_exports(clean_dir, require_complete=True)
        if item.version < version
    ]
    return candidates[-1] if candidates else None


def sha256_file(path: Path, progress_seconds: float = 30) -> str:
    digest = hashlib.sha256()
    total = path.stat().st_size
    processed = 0
    with Progress(
        "file-checksum", interval=progress_seconds, total_bytes=total,
        processed_bytes=0, disk_path=path.parent,
    ) as progress, path.open("rb") as source:
        while chunk := source.read(BUFFER_SIZE):
            digest.update(chunk)
            processed += len(chunk)
            progress.update(processed_bytes=processed)
    return digest.hexdigest()


def sha256_small_file(path: Path, progress_seconds: float | None = None) -> str:
    if path.stat().st_size > BUFFER_SIZE:
        return sha256_file(path, progress_interval(progress_seconds))
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(BUFFER_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def split_build_sql(path: Path, compact_storage: bool = False) -> BuildSQLParts:
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace(
        "/* OJS_COMPACT_TABLE */",
        "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8" if compact_storage else "",
    )
    if sql.count(METADATA_BEGIN_MARKER) != 1:
        raise PipelineError(
            f"{path.name} must contain exactly one {METADATA_BEGIN_MARKER}"
        )
    if sql.count(METADATA_END_MARKER) != 1:
        raise PipelineError(
            f"{path.name} must contain exactly one {METADATA_END_MARKER}"
        )
    prefix, remainder = sql.split(METADATA_BEGIN_MARKER, 1)
    metadata, suffix = remainder.split(METADATA_END_MARKER, 1)
    if not metadata.strip():
        raise PipelineError(f"{path.name} has an empty metadata phase")
    return BuildSQLParts(prefix=prefix, metadata=metadata, suffix=suffix)


def source_id_ranges(
    minimum: int,
    maximum: int,
    shard_count: int,
) -> list[tuple[int, int]]:
    if minimum > maximum:
        return []
    shard_count = max(1, min(shard_count, maximum - minimum + 1))
    width = (maximum - minimum + 1 + shard_count - 1) // shard_count
    return [
        (start, min(start + width - 1, maximum))
        for start in range(minimum, maximum + 1, width)
    ]


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def command_error(
    label: str,
    result: subprocess.CompletedProcess[object],
) -> PipelineError:
    raw_stderr = result.stderr or ""
    if isinstance(raw_stderr, bytes):
        stderr = raw_stderr.decode("utf-8", errors="replace").strip()
    else:
        stderr = str(raw_stderr).strip()
    detail = f": {stderr[-4000:]}" if stderr else ""
    return PipelineError(
        f"{label} failed with exit code {result.returncode}{detail}"
    )


class RawRecordsDDL:
    """Change only the small, line-oriented mysqldump records declaration.

    Input arrives in bounded readline fragments; extended INSERT statements
    are forwarded untouched, even when a single line is many megabytes long.
    Unexpected records DDL fails closed instead of silently losing compression.
    """

    def __init__(self) -> None:
        self.line_start = True
        self.in_records = False
        self.complete = False
        self.ddl_bytes = 0

    def transform(self, chunk: bytes) -> bytes:
        at_start = self.line_start
        self.line_start = chunk.endswith(b"\n")
        if at_start and chunk.rstrip(b"\r\n") == b"CREATE TABLE `records` (":
            if self.in_records or self.complete:
                raise PipelineError("unexpected duplicate records table declaration")
            self.in_records = True
        if not self.in_records:
            return chunk
        self.ddl_bytes += len(chunk)
        if self.ddl_bytes > 256 * 1024:
            raise PipelineError("records table declaration exceeds the safe DDL limit")
        if at_start and chunk.startswith(b")"):
            if not re.fullmatch(rb"\) ENGINE=InnoDB[^\r\n]*;\r?\n", chunk):
                raise PipelineError("unsupported records table declaration for compression")
            if re.search(rb"\b(?:ROW_FORMAT|KEY_BLOCK_SIZE)\s*=", chunk, re.I):
                raise PipelineError("records table already specifies storage options")
            self.in_records = False
            self.complete = True
            return chunk.replace(
                b") ENGINE=InnoDB",
                b") ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
                1,
            )
        return chunk

    def finish(self) -> None:
        if not self.complete or self.in_records:
            raise PipelineError("no complete records table declaration found for compression")


class MySQLServer:
    def __init__(
        self,
        tools: MySQLTools,
        datadir: Path,
        buffer_pool_size: str,
        compact_storage: bool = False,
        progress_seconds: float | None = None,
    ) -> None:
        self.tools = tools
        self.datadir = datadir.resolve()
        self.buffer_pool_size = buffer_pool_size
        self.compact_storage = compact_storage
        self.runtime = tempfile.TemporaryDirectory(prefix="ojs-api-mysql-")
        runtime_path = Path(self.runtime.name)
        self.socket = runtime_path / "mysql.sock"
        self.pid_file = runtime_path / "mysql.pid"
        self.server_log = runtime_path / "mysqld.log"
        self.disk_tmp = self.datadir.parent / f".{self.datadir.name}-mysql-tmp"
        self.running = False
        self.progress_seconds = progress_interval(progress_seconds)
        self._clients: set[subprocess.Popen] = set()
        self._clients_lock = threading.Lock()
        self._clients_cancelled = False

    def process_pid(self) -> int | None:
        try:
            return int(self.pid_file.read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            return None

    def cancel_clients(self) -> None:
        """Prevent queued workers from starting and stop all active mysql clients."""
        with self._clients_lock:
            self._clients_cancelled = True
            clients = tuple(self._clients)
        for process in clients:
            stop_process(process)

    def _run_mysql(
        self,
        feed: Callable[[BinaryIO, Progress], None],
        *,
        label: str,
        database: str | None,
        password: str | None,
        capture_result: bool = False,
        stream_progress: bool = True,
        interval: float | None = None,
        **fields: object,
    ) -> str:
        # Register under the cancellation lock so an interrupted worker pool
        # cannot create a new client after cancel_clients() has taken its snapshot.
        with self._clients_lock:
            if self._clients_cancelled:
                raise PipelineError("MySQL work was cancelled")
            process = subprocess.Popen(
                self._client_args(database), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=self._password_env(password),
            )
            self._clients.add(process)
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        result = bytearray()
        errors: list[str] = []
        reader_errors: list[BaseException] = []
        readers: list[threading.Thread] = []
        try:
            with Progress(
                label, interval=interval or self.progress_seconds,
                process_pid=self.process_pid() or process.pid,
                disk_path=self.datadir.parent, **fields,
            ) as progress:
                def read_stdout() -> None:
                    try:
                        if capture_result:
                            while chunk := process.stdout.read(64 * 1024):
                                if len(result) + len(chunk) > BUFFER_SIZE:
                                    raise PipelineError("MySQL query result exceeds bounded capture limit")
                                result.extend(chunk)
                        else:
                            for line in bounded_lines(process.stdout):
                                if stream_progress:
                                    consume_sql_progress(line, progress)
                    except BaseException as exc:
                        reader_errors.append(exc)
                        stop_process(process)

                def read_stderr() -> None:
                    try:
                        for line in bounded_lines(process.stderr):
                            detail = safe_mysql_error(line)
                            if detail is not None:
                                errors[:] = [detail]
                    except BaseException as exc:
                        reader_errors.append(exc)
                        stop_process(process)

                for target in (read_stdout, read_stderr):
                    reader = threading.Thread(target=target, name="ojs-mysql-output", daemon=True)
                    reader.start()
                    readers.append(reader)
                try:
                    try:
                        feed(process.stdin, progress)
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
                    return_code = process.wait()
                    for reader in readers:
                        reader.join()
                    if reader_errors:
                        raise reader_errors[0]
                    if return_code != 0:
                        detail = f": {errors[0]}" if errors else ""
                        raise PipelineError(f"MySQL {label} failed with exit code {return_code}{detail}")
                except BaseException:
                    stop_process(process)
                    raise
        finally:
            stop_process(process)
            for stream in (process.stdin, process.stdout, process.stderr):
                try:
                    stream.close()
                except (BrokenPipeError, OSError):
                    pass
            for reader in readers:
                reader.join()
            with self._clients_lock:
                self._clients.discard(process)
        return result.decode("utf-8", errors="replace").strip()

    def _daemon_error_codes(self) -> str:
        try:
            with self.server_log.open("rb") as source:
                source.seek(0, os.SEEK_END)
                source.seek(max(0, source.tell() - 8192))
                tail = source.read(8192)
        except OSError:
            return ""
        codes = re.findall(rb"\[ERROR\] \[(MY-[0-9]{6})\]", tail)
        return "/".join(code.decode("ascii") for code in codes[-4:])

    @staticmethod
    def _password_env(password: str | None) -> dict[str, str]:
        env = os.environ.copy()
        if password:
            env["MYSQL_PWD"] = password
        else:
            env.pop("MYSQL_PWD", None)
        return env

    def _client_args(self, database: str | None = None) -> list[str]:
        args = [
            self.tools.mysql,
            "--no-defaults",
            "--protocol=socket",
            f"--socket={self.socket}",
            "--user=root",
            "--batch",
            "--unbuffered",
            "--skip-column-names",
            "--binary-mode=1",
            "--disable-named-commands",
            "--local-infile=0",
        ]
        if database:
            args.append(database)
        return args

    def _run_control(
        self, args: list[str], *, label: str, password: str | None = None,
        allow_failure: bool = False,
    ):
        code = None
        try:
            with Progress(label, interval=self.progress_seconds, disk_path=self.datadir.parent):
                process = subprocess.Popen(
                    args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env=self._password_env(password),
                )
                try:
                    code = process.wait()
                    if code != 0:
                        daemon_codes = self._daemon_error_codes()
                        detail = f" daemon_errors={daemon_codes}" if daemon_codes else ""
                        raise PipelineError(f"{label} failed with exit code {code}{detail}")
                finally:
                    stop_process(process)
        except PipelineError:
            if not allow_failure or code is None:
                raise
        return subprocess.CompletedProcess(args, code)

    def initialize(self) -> None:
        self.datadir.mkdir(parents=True)
        result = self._run_control(
            [
                self.tools.mysqld,
                "--no-defaults",
                "--initialize-insecure",
                f"--datadir={self.datadir}",
                f"--log-error={self.server_log}",
            ],
            label="mysql-initialize",
        )
        if result.returncode != 0:
            error = command_error("MySQL initialization", result)
            raise error

    def start(self, password: str | None) -> None:
        self.disk_tmp.mkdir(parents=True, exist_ok=True)
        max_connections = os.environ.get(
            "OJS_BUILDER_MYSQL_MAX_CONNECTIONS",
            "8",
        )
        temptable_max_ram = os.environ.get(
            "OJS_BUILDER_MYSQL_TEMPTABLE_MAX_RAM",
            "64M",
        )
        result = self._run_control(
            [
                self.tools.mysqld,
                "--no-defaults",
                "--daemonize",
                f"--datadir={self.datadir}",
                f"--socket={self.socket}",
                f"--pid-file={self.pid_file}",
                f"--log-error={self.server_log}",
                f"--tmpdir={self.disk_tmp}",
                f"--innodb-buffer-pool-size={self.buffer_pool_size}",
                f"--max-connections={max_connections}",
                f"--temptable-max-ram={temptable_max_ram}",
                "--tmp-table-size=16M",
                "--max-heap-table-size=16M",
                "--innodb-redo-log-capacity=256M",
                "--innodb-file-per-table=ON",
                "--skip-networking",
                "--mysqlx=0",
                "--skip-log-bin",
                "--local-infile=0",
                "--secure-file-priv=NULL",
                "--character-set-server=utf8mb4",
                "--collation-server=utf8mb4_0900_ai_ci",
            ],
            label="mysql-start",
        )
        if result.returncode != 0:
            shutil.rmtree(self.disk_tmp, ignore_errors=True)
            error = command_error("MySQL startup", result)
            raise error
        self.running = True

        deadline = time.monotonic() + 30
        ping_args = [
            self.tools.mysqladmin,
            "--no-defaults",
            "--protocol=socket",
            f"--socket={self.socket}",
            "--user=root",
            "ping",
            "--silent",
        ]
        while time.monotonic() < deadline:
            ping = subprocess.run(
                ping_args,
                env=self._password_env(password),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if ping.returncode == 0:
                return
            time.sleep(0.25)
        raise PipelineError(f"MySQL did not become ready; log: {self.server_log}")

    def execute(
        self,
        sql: str,
        *,
        database: str | None = None,
        password: str | None = None,
        label: str = "statement",
    ) -> str:
        return self._run_mysql(
            lambda target, progress: target.write(sql.encode("utf-8")),
            label=label, database=database, password=password, capture_result=True,
        )

    def run_sql_file(
        self,
        path: Path,
        *,
        database: str,
        password: str | None,
        prelude: str = "",
    ) -> None:
        def feed(target: BinaryIO, progress: Progress) -> None:
            if prelude:
                target.write((prelude.rstrip("\n") + "\n").encode("utf-8"))
            with path.open("rb") as source:
                while chunk := source.read(BUFFER_SIZE):
                    target.write(chunk)

        self._run_mysql(feed, label="sql-script", database=database, password=password)

    def run_sql_text(
        self,
        sql: str,
        *,
        label: str,
        database: str,
        password: str | None,
        prelude: str = "",
        **progress_fields: object,
    ) -> None:
        statement = prelude
        if statement and not statement.endswith("\n"):
            statement += "\n"
        statement += sql
        self._run_mysql(
            lambda target, progress: target.write(statement.encode("utf-8")),
            label=label, database=database, password=password, **progress_fields,
        )

    def _stream_into_mysql(
        self,
        source: BinaryIO,
        *,
        label: str,
        total: int,
        database: str,
        password: str | None,
        progress_seconds: float,
        calculate_sha256: bool,
        compact_records: bool = False,
    ) -> str | None:
        digest = hashlib.sha256() if calculate_sha256 else None
        processed = 0
        ddl = RawRecordsDDL() if compact_records else None
        def feed(target: BinaryIO, progress: Progress) -> None:
            nonlocal processed
            read_chunk = (
                (lambda: source.readline(64 * 1024))
                if ddl is not None else (lambda: source.read(BUFFER_SIZE))
            )
            while chunk := read_chunk():
                if digest is not None:
                    digest.update(chunk)
                target.write(ddl.transform(chunk) if ddl else chunk)
                processed += len(chunk)
                progress.update(processed_bytes=processed)
            if ddl is not None:
                ddl.finish()
            if total > 0 and processed != total:
                raise PipelineError(f"MySQL import consumed {processed} bytes; expected {total}")
            progress.update(step="waiting-for-mysql")

        self._run_mysql(
            feed, label="sql-import", database=database, password=password,
            interval=progress_seconds, total_bytes=total or None,
            processed_bytes=0, step="feeding-mysql", stream_progress=False,
        )
        return digest.hexdigest() if digest is not None else None

    def import_snapshot(
        self,
        snapshot: Snapshot,
        *,
        database: str,
        password: str | None,
        progress_seconds: float,
    ) -> str:
        with open_snapshot(snapshot.path) as source:
            digest = self._stream_into_mysql(
                source,
                label=snapshot.path.name,
                total=snapshot.size,
                database=database,
                password=password,
                progress_seconds=progress_seconds,
                calculate_sha256=True,
                compact_records=self.compact_storage,
            )
        assert digest is not None
        if snapshot.path.name.endswith(".gz"):
            expected = validate_cached_snapshot(snapshot.path)["uncompressed_sha256"]
            if digest != expected:
                raise PipelineError("imported SQL hash differs from validated gzip metadata")
        return digest

    def import_clean_export(
        self,
        clean_export: CleanExport,
        *,
        database: str,
        password: str | None,
        progress_seconds: float,
    ) -> None:
        verify_checksum_sidecar(clean_export.path, progress_seconds=progress_seconds)
        with gzip.open(clean_export.path, "rb") as source:
            self._stream_into_mysql(
                source,
                label=clean_export.path.name,
                total=0,
                database=database,
                password=password,
                progress_seconds=progress_seconds,
                calculate_sha256=False,
            )

    def export_clean_tables(
        self,
        output_path: Path,
        *,
        database: str,
        password: str | None,
    ) -> None:
        args = [
            self.tools.mysqldump,
            "--no-defaults",
            "--protocol=socket",
            f"--socket={self.socket}",
            "--user=root",
            "--single-transaction",
            "--quick",
            "--skip-lock-tables",
            "--skip-add-locks",
            "--skip-comments",
            "--skip-dump-date",
            "--set-gtid-purged=OFF",
            "--no-tablespaces",
            "--hex-blob",
            "--default-character-set=utf8mb4",
            database,
            *CLEAN_EXPORT_TABLES,
        ]
        with Progress(
            "clean-export", interval=self.progress_seconds,
            process_pid=self.process_pid(), disk_path=output_path.parent,
            processed_bytes=0, compressed_bytes=0,
        ) as progress:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=self._password_env(password),
            )
            assert process.stdout is not None
            processed = 0
            try:
                with output_path.open("wb") as raw_output:
                    with gzip.GzipFile(
                        filename="", mode="wb", compresslevel=6,
                        fileobj=raw_output, mtime=0,
                    ) as compressed:
                        while chunk := process.stdout.read(BUFFER_SIZE):
                            compressed.write(chunk)
                            processed += len(chunk)
                            progress.update(processed_bytes=processed, compressed_bytes=raw_output.tell())
                    progress.update(compressed_bytes=raw_output.tell(), step="waiting-for-mysqldump")
                return_code = process.wait()
                if return_code != 0:
                    raise PipelineError(f"clean SQL export failed with exit code {return_code}")
            except BaseException:
                stop_process(process)
                output_path.unlink(missing_ok=True)
                raise
            finally:
                stop_process(process)
                process.stdout.close()

    def shutdown(self, password: str | None) -> None:
        self.cancel_clients()
        if not self.running and not self.pid_file.exists():
            shutil.rmtree(self.disk_tmp, ignore_errors=True)
            self.runtime.cleanup()
            return
        result = self._run_control(
            [
                self.tools.mysqladmin,
                "--no-defaults",
                "--protocol=socket",
                f"--socket={self.socket}",
                "--user=root",
                "shutdown",
            ],
            label="mysql-shutdown", password=password, allow_failure=True,
        )
        if result.returncode != 0 and self.pid_file.exists():
            try:
                pid = int(self.pid_file.read_text(encoding="ascii").strip())
                os.kill(pid, signal.SIGTERM)
            except (OSError, ValueError):
                pass
        with Progress(
            "mysql-shutdown-wait", interval=self.progress_seconds,
            process_pid=self.process_pid(), disk_path=self.datadir.parent,
        ):
            deadline = time.monotonic() + 120
            while self.pid_file.exists() and time.monotonic() < deadline:
                time.sleep(0.25)
            if self.pid_file.exists():
                raise PipelineError(
                    f"MySQL did not shut down cleanly; retaining {self.datadir} "
                    "and refusing to publish or delete its working files"
                )
        self.running = False
        shutil.rmtree(self.disk_tmp, ignore_errors=True)
        self.runtime.cleanup()


def create_database(server: MySQLServer, database: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise PipelineError(f"unsafe database name: {database}")
    server.execute(
        f"CREATE DATABASE `{database}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
    )
    # Checkpoints must never become durable ahead of the SQL they describe.
    server.execute("SET GLOBAL innodb_flush_log_at_trx_commit=1;")


def assert_database_stopped(datadir: Path) -> None:
    """Refuse offline verification/deletion while a MySQL process owns the data."""
    target = datadir.resolve()
    for process in Path("/proc").glob("[0-9]*"):
        try:
            if (process / "comm").read_text().strip() != "mysqld":
                continue
            arguments = (process / "cmdline").read_bytes().decode().split("\0")
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as exc:
            raise PipelineError("cannot inspect MySQL processes before offline work") from exc
        for index, argument in enumerate(arguments):
            declared = None
            if argument.startswith("--datadir="):
                declared = argument.split("=", 1)[1]
            elif argument == "--datadir" and index + 1 < len(arguments):
                declared = arguments[index + 1]
            if declared and Path(declared).resolve() == target:
                raise PipelineError(f"MySQL is still using database directory: {target}")
    # InnoDB's POSIX write lock also detects a container whose internal datadir
    # path differs from the host path, without requiring a Docker installation.
    tablespace = target / "ibdata1"
    if tablespace.is_symlink():
        raise PipelineError(f"refusing symlinked InnoDB system tablespace: {tablespace}")
    if tablespace.exists():
        with tablespace.open("r+b") as source:
            try:
                fcntl.lockf(source.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise PipelineError(f"InnoDB still has the database open: {target}") from exc
            finally:
                fcntl.lockf(source.fileno(), fcntl.LOCK_UN)


def validate_database(
    server: MySQLServer,
    database: str,
    password: str | None,
    *,
    require_raw: bool = True,
) -> tuple[BuildCounts, str]:
    if require_raw:
        raw_tables = server.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_type = 'BASE TABLE'
              AND table_name IN ('contexts', 'endpoints', 'issns', 'records', 'versions');
            """,
            database=database,
            password=password,
            label="validate-raw-tables",
        )
        if raw_tables != "5":
            raise PipelineError(
                f"database validation found {raw_tables or '0'} of 5 required raw tables"
            )

    output = server.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM ojs_article_sources),
            (SELECT COUNT(*) FROM ojs_article_sources WHERE is_active = 1),
            (SELECT COUNT(*) FROM ojs_articles),
            (SELECT COUNT(*) FROM ojs_articles WHERE status = 'active'),
            (SELECT COUNT(*) FROM ojs_articles WHERE status = 'removed'),
            (SELECT COUNT(*) FROM ojs_articles WHERE status = 'merged'),
            (
                SELECT event_count
                FROM ojs_snapshots
                ORDER BY snapshot_date DESC
                LIMIT 1
            );
        SELECT
            (
                SELECT COUNT(*)
                FROM ojs_article_sources source_alias
                INNER JOIN ojs_article_keys identity_key
                    ON identity_key.key_type = 'oai'
                   AND identity_key.key_hash = source_alias.oai_key_hash
                WHERE source_alias.article_id <> identity_key.article_id
            )
            +
            (
                SELECT COUNT(*)
                FROM ojs_article_sources source_alias
                INNER JOIN ojs_article_keys identity_key
                    ON identity_key.key_type = 'doi'
                   AND identity_key.key_hash = source_alias.doi_key_hash
                WHERE source_alias.article_id <> identity_key.article_id
            )
            +
            (
                SELECT COUNT(*)
                FROM ojs_article_sources source_alias
                INNER JOIN ojs_article_keys identity_key
                    ON identity_key.key_type = 'url'
                   AND identity_key.key_hash = source_alias.url_key_hash
                WHERE source_alias.article_id <> identity_key.article_id
            )
            +
            (
                SELECT COUNT(*)
                FROM ojs_article_sources source_alias
                INNER JOIN ojs_article_keys identity_key
                    ON identity_key.key_type = 'fingerprint'
                   AND identity_key.key_hash =
                       source_alias.fingerprint_key_hash
                WHERE source_alias.article_id <> identity_key.article_id
            );
        SELECT VERSION();
        """,
        database=database,
        password=password,
        label="validate-counts-and-identities",
    )
    lines = output.splitlines()
    if len(lines) != 3:
        raise PipelineError(f"unexpected validation output: {output!r}")
    try:
        values = tuple(int(value) for value in lines[0].split("\t"))
    except ValueError as exc:
        raise PipelineError(f"invalid validation counts: {lines[0]!r}") from exc
    if len(values) != 7:
        raise PipelineError(f"invalid validation counts: {lines[0]!r}")
    counts = BuildCounts(*values)
    if counts.source_records <= 0 or counts.articles <= 0:
        raise PipelineError(
            "clean-table validation returned no sources or articles"
        )
    if counts.active_sources > counts.source_records:
        raise PipelineError("active source count exceeds source count")
    if (
        counts.active_articles
        + counts.removed_articles
        + counts.merged_articles
        != counts.articles
    ):
        raise PipelineError("article status counts do not sum to article count")
    try:
        identity_conflicts = int(lines[1])
    except ValueError as exc:
        raise PipelineError(
            f"invalid identity-conflict count: {lines[1]!r}"
        ) from exc
    if identity_conflicts:
        raise PipelineError(
            "identity label propagation did not converge: "
            f"{identity_conflicts} source-key assignments conflict"
        )
    return counts, lines[2]


def prune_build_tables(
    server: MySQLServer, *, database: str, password: str | None
) -> None:
    """Keep only temporal state and provenance in the publishable data directory."""
    retained = set(CLEAN_EXPORT_TABLES) | {"ojs_pipeline_metadata"}
    names = server.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE';",
        database=database,
        password=password,
    ).splitlines()
    if not retained.issubset(names):
        raise PipelineError("refusing raw cleanup before all clean tables exist")
    discarded = sorted(set(names) - retained)
    if any(not re.fullmatch(r"[A-Za-z0-9_]+", name) for name in discarded):
        raise PipelineError("unexpected raw table identifier during cleanup")
    if discarded:
        server.execute(
            "SET FOREIGN_KEY_CHECKS=0;\n"
            + "\n".join(f"DROP TABLE `{name}`;" for name in discarded)
            + "\nSET FOREIGN_KEY_CHECKS=1;",
            database=database,
            password=password,
        )
        print(f"[cleanup] dropped {len(discarded)} imported raw/work tables")


def write_provenance(
    server: MySQLServer,
    *,
    database: str,
    password: str | None,
    snapshot: Snapshot,
    source_sha256: str,
    build_sql_sha256: str,
    mysql_version: str,
    counts: BuildCounts,
) -> None:
    server.execute(
        f"""
        DROP TABLE IF EXISTS ojs_pipeline_metadata;
        CREATE TABLE ojs_pipeline_metadata (
            id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
            pipeline_version VARCHAR(32) NOT NULL,
            snapshot_date DATE NOT NULL,
            dump_completed_at DATETIME NOT NULL,
            source_filename VARCHAR(255) NOT NULL,
            source_size_bytes BIGINT UNSIGNED NOT NULL,
            source_sha256 CHAR(64) NOT NULL,
            build_sql_sha256 CHAR(64) NOT NULL,
            mysql_version VARCHAR(128) NOT NULL,
            source_record_count BIGINT UNSIGNED NOT NULL,
            active_source_count BIGINT UNSIGNED NOT NULL,
            article_count BIGINT UNSIGNED NOT NULL,
            active_article_count BIGINT UNSIGNED NOT NULL,
            removed_article_count BIGINT UNSIGNED NOT NULL,
            merged_article_count BIGINT UNSIGNED NOT NULL,
            event_count BIGINT UNSIGNED NOT NULL,
            built_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        );
        INSERT INTO ojs_pipeline_metadata VALUES (
            1,
            {sql_string(PIPELINE_VERSION)},
            {sql_string(snapshot.version)},
            {sql_string(snapshot.completed_at.strftime("%Y-%m-%d %H:%M:%S"))},
            {sql_string(snapshot.path.name)},
            {snapshot.size},
            {sql_string(source_sha256)},
            {sql_string(build_sql_sha256)},
            {sql_string(mysql_version)},
            {counts.source_records},
            {counts.active_sources},
            {counts.articles},
            {counts.active_articles},
            {counts.removed_articles},
            {counts.merged_articles},
            {counts.events},
            CURRENT_TIMESTAMP(6)
        );
        """,
        database=database,
        password=password,
    )


def change_report_path(clean_dir: Path, version: str) -> Path:
    return clean_dir / f"pkpbeacon-changes-{version}.json"


def collect_change_report(
    server: MySQLServer,
    *,
    database: str,
    password: str | None,
    snapshot: Snapshot,
    source_sha256: str,
    build_sql_sha256: str,
    counts: BuildCounts,
) -> dict[str, object]:
    """Build a deterministic, release-level audit summary from committed rows."""
    previous_output = server.execute(
        f"""
        SELECT
            DATE_FORMAT(snapshot_date, '%Y-%m-%d'),
            source_record_count,
            active_source_count,
            article_count,
            active_article_count
        FROM ojs_snapshots
        WHERE snapshot_date < {sql_string(snapshot.version)}
        ORDER BY snapshot_date DESC
        LIMIT 1;
        """,
        database=database,
        password=password,
        label="change-report-prior-snapshot",
    )
    previous_counts: dict[str, object] | None = None
    if previous_output:
        previous_fields = previous_output.split("\t")
        if len(previous_fields) != 5:
            raise PipelineError(
                f"unexpected previous-snapshot summary: {previous_output!r}"
            )
        try:
            previous_values = [int(value) for value in previous_fields[1:]]
        except ValueError as exc:
            raise PipelineError(
                f"invalid previous-snapshot counts: {previous_output!r}"
            ) from exc
        previous_counts = {
            "date": previous_fields[0],
            "source_rows_retained": previous_values[0],
            "active_sources": previous_values[1],
            "articles_retained": previous_values[2],
            "active_articles": previous_values[3],
        }

    source_output = server.execute(
        f"""
        SELECT
            COALESCE(
                (
                    SELECT DATE_FORMAT(MAX(snapshot_date), '%Y-%m-%d')
                    FROM ojs_snapshots
                    WHERE snapshot_date < {sql_string(snapshot.version)}
                ),
                '-'
            ),
            (SELECT COUNT(*) FROM records),
            COUNT(*),
            COALESCE(SUM(is_present = 1), 0),
            COALESCE(SUM(is_active = 1), 0),
            COALESCE(
                SUM(date_added = {sql_string(snapshot.version)}),
                0
            ),
            COALESCE(
                SUM(
                    date_added < {sql_string(snapshot.version)}
                    AND date_modified = {sql_string(snapshot.version)}
                ),
                0
            ),
            COALESCE(
                SUM(
                    date_removed = {sql_string(snapshot.version)}
                    AND is_active = 0
                ),
                0
            ),
            COALESCE(
                SUM(
                    date_modified = {sql_string(snapshot.version)}
                    AND is_present = 0
                ),
                0
            ),
            COALESCE(
                SUM(
                    date_removed = {sql_string(snapshot.version)}
                    AND is_present = 1
                    AND is_active = 0
                ),
                0
            ),
            COALESCE(SUM(is_present = 1 AND title IS NULL), 0),
            COALESCE(
                SUM(
                    is_present = 1
                    AND oai_key_hash IS NULL
                    AND doi_key_hash IS NULL
                    AND url_key_hash IS NULL
                    AND fingerprint_key_hash IS NULL
                ),
                0
            ),
            COALESCE(
                (
                    SELECT MIN(event_id)
                    FROM ojs_article_events
                    WHERE snapshot_date = {sql_string(snapshot.version)}
                ),
                0
            ),
            COALESCE(
                (
                    SELECT MAX(event_id)
                    FROM ojs_article_events
                    WHERE snapshot_date = {sql_string(snapshot.version)}
                ),
                0
            )
        FROM ojs_article_sources;
        """,
        database=database,
        password=password,
        label="change-report-source-counts",
    )
    fields = source_output.split("\t")
    if len(fields) != 14:
        raise PipelineError(
            f"unexpected change-report source summary: {source_output!r}"
        )
    try:
        source_values = [int(value) for value in fields[1:]]
    except ValueError as exc:
        raise PipelineError(
            f"invalid change-report source counts: {source_output!r}"
        ) from exc
    (
        raw_source_rows,
        source_total,
        source_present,
        source_active,
        source_added,
        source_changed,
        source_newly_inactive,
        source_disappeared,
        source_marked_removed,
        source_without_title,
        source_without_identity_key,
        first_event_id,
        last_event_id,
    ) = source_values

    by_type = {event_type: 0 for event_type in CHANGE_EVENT_TYPES}
    by_operation = {operation: 0 for operation in CHANGE_OPERATIONS}
    event_output = server.execute(
        f"""
        SELECT event_type, operation, COUNT(*)
        FROM ojs_article_events
        WHERE snapshot_date = {sql_string(snapshot.version)}
        GROUP BY event_type, operation
        ORDER BY event_type, operation;
        """,
        database=database,
        password=password,
        label="change-report-event-counts",
    )
    for line in event_output.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            raise PipelineError(f"invalid change-report event row: {line!r}")
        event_type, operation, raw_count = parts
        if event_type not in by_type or operation not in by_operation:
            raise PipelineError(
                "unexpected article event in change report: "
                f"{event_type!r}/{operation!r}"
            )
        try:
            event_count = int(raw_count)
        except ValueError as exc:
            raise PipelineError(
                f"invalid article event count: {raw_count!r}"
            ) from exc
        by_type[event_type] += event_count
        by_operation[operation] += event_count

    reported_events = sum(by_type.values())
    if reported_events != counts.events:
        raise PipelineError(
            "change-report event count does not match validated snapshot: "
            f"{reported_events} != {counts.events}"
        )
    if source_total != counts.source_records or source_active != counts.active_sources:
        raise PipelineError(
            "change-report source counts do not match validated snapshot"
        )
    previous_date = None if fields[0] == "-" else fields[0]
    if previous_counts is not None and previous_counts["date"] != previous_date:
        raise PipelineError(
            "change-report previous snapshot date is internally inconsistent"
        )

    return {
        "schema_version": 1,
        "pipeline_version": PIPELINE_VERSION,
        "snapshot": {
            "date": snapshot.version,
            "completed_at": snapshot.completed_at.isoformat(),
            "source_filename": snapshot.path.name,
            "source_size_bytes": snapshot.size,
            "source_sha256": source_sha256,
            "build_sql_sha256": build_sql_sha256,
            "previous_snapshot_date": previous_date,
        },
        "previous_counts": previous_counts,
        "source_rows": {
            "raw_in_snapshot": raw_source_rows,
            "excluded_outside_valid_issn_scope": max(
                0,
                raw_source_rows - source_present,
            ),
            "retained_total": source_total,
            "present_in_snapshot": source_present,
            "active": source_active,
            "added": source_added,
            "changed_existing": source_changed,
            "newly_inactive": source_newly_inactive,
            "missing_from_snapshot": source_disappeared,
            "marked_removed_by_source": source_marked_removed,
            "present_without_title": source_without_title,
            "present_without_identity_key": source_without_identity_key,
        },
        "articles": {
            "retained_total": counts.articles,
            "active": counts.active_articles,
            "removed_tombstones": counts.removed_articles,
            "merged_tombstones": counts.merged_articles,
        },
        "article_events": {
            "total": reported_events,
            "first_event_id": first_event_id or None,
            "last_event_id": last_event_id or None,
            "by_type": by_type,
            "by_operation": by_operation,
        },
    }


def stage_change_report(path: Path, report: dict[str, object]) -> Path:
    temporary = path.with_name(f".{path.name}.tmp")
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as destination:
        destination.write(payload)
        destination.flush()
        os.fsync(destination.fileno())
    return temporary


def release_anomalies(
    report: dict[str, object],
    thresholds: ReleaseThresholds,
) -> list[dict[str, object]]:
    previous = report.get("previous_counts")
    if previous is None:
        return []
    if not isinstance(previous, dict):
        raise PipelineError("change report has invalid previous_counts")
    source_rows = report.get("source_rows")
    articles = report.get("articles")
    article_events = report.get("article_events")
    if not all(
        isinstance(value, dict)
        for value in (source_rows, articles, article_events)
    ):
        raise PipelineError("change report is missing release-guard counts")
    assert isinstance(source_rows, dict)
    assert isinstance(articles, dict)
    assert isinstance(article_events, dict)
    by_type = article_events.get("by_type")
    if not isinstance(by_type, dict):
        raise PipelineError("change report has invalid article event counts")

    def fraction(numerator: int, denominator: int) -> float:
        return numerator / denominator if denominator > 0 else 0.0

    previous_active_sources = int(previous["active_sources"])
    current_active_sources = int(source_rows["active"])
    previous_active_articles = int(previous["active_articles"])
    current_active_articles = int(articles["active"])
    metrics = (
        (
            "active_source_drop_fraction",
            fraction(
                max(0, previous_active_sources - current_active_sources),
                previous_active_sources,
            ),
            thresholds.active_source_drop_fraction,
            previous_active_sources - current_active_sources,
            previous_active_sources,
        ),
        (
            "active_article_drop_fraction",
            fraction(
                max(0, previous_active_articles - current_active_articles),
                previous_active_articles,
            ),
            thresholds.active_article_drop_fraction,
            previous_active_articles - current_active_articles,
            previous_active_articles,
        ),
        (
            "article_removal_fraction",
            fraction(int(by_type.get("removed", 0)), previous_active_articles),
            thresholds.article_removal_fraction,
            int(by_type.get("removed", 0)),
            previous_active_articles,
        ),
        (
            "article_merge_fraction",
            fraction(int(by_type.get("merged", 0)), previous_active_articles),
            thresholds.article_merge_fraction,
            int(by_type.get("merged", 0)),
            previous_active_articles,
        ),
    )
    return [
        {
            "metric": name,
            "observed_fraction": observed,
            "maximum_fraction": maximum,
            "affected_rows": max(0, affected),
            "comparison_rows": comparison,
        }
        for name, observed, maximum, affected, comparison in metrics
        if observed > maximum
    ]


def publish_change_report(path: Path, report: dict[str, object]) -> None:
    temporary = stage_change_report(path, report)
    os.replace(temporary, path)
    path.chmod(0o444)
    write_checksum_sidecar(path, sha256_small_file(path))


def publish_release_manifest(
    clean_dir: Path,
    *,
    version: str,
    export_path: Path,
    export_sha256: str,
    report_path: Path,
    report_sha256: str,
) -> Path:
    """Atomically commit a complete, checksummed release artifact set."""
    manifest_path = release_manifest_path(clean_dir, version)
    temporary = manifest_path.with_name(f".{manifest_path.name}.tmp")
    payload = {
        "schema_version": 1,
        "snapshot_date": version,
        "clean_export_filename": export_path.name,
        "clean_export_sha256": export_sha256,
        "change_report_filename": report_path.name,
        "change_report_sha256": report_sha256,
    }
    with temporary.open("w", encoding="utf-8") as destination:
        json.dump(payload, destination, indent=2, sort_keys=True)
        destination.write("\n")
        destination.flush()
        os.fsync(destination.fileno())
    os.replace(temporary, manifest_path)
    manifest_path.chmod(0o444)
    return manifest_path


def verify_change_report(
    path: Path,
    *,
    snapshot: Snapshot,
    source_sha256: str,
    build_sql_sha256: str,
    counts: BuildCounts,
) -> None:
    verify_checksum_sidecar(path)
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        snapshot_report = report["snapshot"]
        source_rows = report["source_rows"]
        articles = report["articles"]
        events = report["article_events"]
        actual = {
            "schema_version": report["schema_version"],
            "pipeline_version": report["pipeline_version"],
            "snapshot_date": snapshot_report["date"],
            "source_filename": snapshot_report["source_filename"],
            "source_sha256": snapshot_report["source_sha256"],
            "build_sql_sha256": snapshot_report["build_sql_sha256"],
            "source_records": source_rows["retained_total"],
            "active_sources": source_rows["active"],
            "articles": articles["retained_total"],
            "active_articles": articles["active"],
            "removed_articles": articles["removed_tombstones"],
            "merged_articles": articles["merged_tombstones"],
            "events": events["total"],
        }
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise PipelineError(f"invalid change report: {path}") from exc
    expected: dict[str, object] = {
        "schema_version": 1,
        "pipeline_version": PIPELINE_VERSION,
        "snapshot_date": snapshot.version,
        "source_filename": snapshot.path.name,
        "source_sha256": source_sha256,
        "build_sql_sha256": build_sql_sha256,
        "source_records": counts.source_records,
        "active_sources": counts.active_sources,
        "articles": counts.articles,
        "active_articles": counts.active_articles,
        "removed_articles": counts.removed_articles,
        "merged_articles": counts.merged_articles,
        "events": counts.events,
    }
    mismatches = [
        f"{name}: expected {value!r}, got {actual[name]!r}"
        for name, value in expected.items()
        if actual[name] != value
    ]
    if mismatches:
        raise PipelineError(
            "change-report validation failed:\n" + "\n".join(mismatches)
        )


def verify_existing_database(
    *,
    snapshot: Snapshot,
    datadir: Path,
    build_sql: Path,
    database: str,
    root_password: str,
    buffer_pool_size: str,
    progress_seconds: float,
    verify_source_checksum: bool,
) -> BuildCounts:
    assert_database_stopped(datadir)
    expected_source_sha256 = None
    if verify_source_checksum:
        print(f"[verify] hashing {snapshot.path.name}")
        expected_source_sha256 = snapshot_sha256(snapshot, progress_seconds)

    server = MySQLServer(
        MySQLTools.discover(), datadir, buffer_pool_size,
        progress_seconds=progress_seconds,
    )
    try:
        server.start(root_password)
        counts, _ = validate_database(
            server, database, root_password, require_raw=False
        )
        metadata = server.execute(
            """
            SELECT
                pipeline_version,
                DATE_FORMAT(snapshot_date, '%Y-%m-%d'),
                source_filename,
                source_size_bytes,
                source_sha256,
                build_sql_sha256,
                source_record_count,
                active_source_count,
                article_count,
                active_article_count,
                removed_article_count,
                merged_article_count,
                event_count
            FROM ojs_pipeline_metadata
            WHERE id = 1;
            """,
            database=database,
            password=root_password,
        )
    finally:
        server.shutdown(root_password)

    fields = metadata.split("\t")
    if len(fields) != 13:
        raise PipelineError(f"invalid pipeline provenance row: {metadata!r}")
    (
        pipeline_version,
        snapshot_date,
        source_filename,
        source_size,
        source_sha256,
        build_sql_sha256,
        source_records,
        active_sources,
        articles,
        active_articles,
        removed_articles,
        merged_articles,
        events,
    ) = fields
    expected = {
        "pipeline_version": PIPELINE_VERSION,
        "snapshot_date": snapshot.version,
        "source_filename": snapshot.path.name,
        "source_size": str(snapshot.size),
        "build_sql_sha256": sha256_small_file(build_sql),
        "source_records": str(counts.source_records),
        "active_sources": str(counts.active_sources),
        "articles": str(counts.articles),
        "active_articles": str(counts.active_articles),
        "removed_articles": str(counts.removed_articles),
        "merged_articles": str(counts.merged_articles),
        "events": str(counts.events),
    }
    actual = {
        "pipeline_version": pipeline_version,
        "snapshot_date": snapshot_date,
        "source_filename": source_filename,
        "source_size": source_size,
        "build_sql_sha256": build_sql_sha256,
        "source_records": source_records,
        "active_sources": active_sources,
        "articles": articles,
        "active_articles": active_articles,
        "removed_articles": removed_articles,
        "merged_articles": merged_articles,
        "events": events,
    }
    mismatches = [
        f"{key}: expected {expected[key]!r}, got {actual[key]!r}"
        for key in expected
        if expected[key] != actual[key]
    ]
    if expected_source_sha256 and source_sha256 != expected_source_sha256:
        mismatches.append(
            "source_sha256: stored checksum does not match the SQL snapshot"
        )
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        mismatches.append("source_sha256: stored checksum is invalid")
    if mismatches:
        raise PipelineError("provenance validation failed:\n" + "\n".join(mismatches))
    verify_checksum_sidecar(
        clean_export_path(datadir.parent, snapshot.version),
        progress_seconds=progress_seconds,
    )
    verify_change_report(
        change_report_path(datadir.parent, snapshot.version),
        snapshot=snapshot,
        source_sha256=source_sha256,
        build_sql_sha256=build_sql_sha256,
        counts=counts,
    )
    print(
        "[verify] provenance, clean tables, clean export, and change report match"
    )
    return counts


def set_root_password(
    server: MySQLServer,
    current_password: str | None,
    new_password: str,
) -> None:
    server.execute(
        f"ALTER USER 'root'@'localhost' IDENTIFIED BY {sql_string(new_password)};"
        "SET GLOBAL innodb_flush_log_at_trx_commit=1;",
        password=current_password,
    )


def read_private_secret(path: Path, label: str) -> str:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    if not resolved.is_file():
        raise PipelineError(f"{label} file does not exist: {resolved}")
    if resolved.stat().st_mode & 0o077:
        raise PipelineError(
            f"{label} file permissions are too open: {resolved}; "
            "run chmod 600 on it"
        )
    try:
        value = resolved.read_text(encoding="utf-8").rstrip("\n")
    except OSError as exc:
        raise PipelineError(f"could not read {label} file: {resolved}") from exc
    if not value or "\n" in value or "\r" in value:
        raise PipelineError(f"{label} file must contain exactly one nonempty line")
    return value


def configure_api_database_user(
    server: MySQLServer,
    *,
    database: str,
    password: str | None,
    api_user: str,
    api_password: str,
) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,32}", api_user):
        raise PipelineError(
            "OJS_DB_API_USER must use 1-32 letters, digits, dots, dashes, "
            "or underscores"
        )
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise PipelineError(f"unsafe database name: {database}")
    account = f"{sql_string(api_user)}@'%'"
    grants = "\n".join(
        f"GRANT SELECT ON `{database}`.`{table}` TO {account};"
        for table in API_READ_TABLES
    )
    server.execute(
        f"""
        CREATE USER IF NOT EXISTS {account}
            IDENTIFIED BY {sql_string(api_password)};
        ALTER USER {account} IDENTIFIED BY {sql_string(api_password)};
        REVOKE ALL PRIVILEGES, GRANT OPTION FROM {account};
        {grants}
        """,
        password=password,
    )
    print(
        f"[security] provisioned SELECT-only MySQL account {api_user!r} "
        f"for {len(API_READ_TABLES)} API tables"
    )


def clean_export_path(clean_dir: Path, version: str) -> Path:
    return clean_dir / f"pkpbeacon-clean-{version}.sql.gz"


def checksum_sidecar_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.sha256")


def write_checksum_sidecar(path: Path, checksum: str) -> None:
    sidecar = checksum_sidecar_path(path)
    temporary = sidecar.with_name(f".{sidecar.name}.tmp")
    temporary.write_text(f"{checksum}  {path.name}\n", encoding="ascii")
    os.replace(temporary, sidecar)


def verify_checksum_sidecar(path: Path, *, progress_seconds: float | None = None) -> None:
    if not path.is_file():
        raise PipelineError(f"clean SQL export does not exist: {path}")
    sidecar = checksum_sidecar_path(path)
    if not sidecar.is_file():
        raise PipelineError(f"clean SQL checksum does not exist: {sidecar}")
    fields = sidecar.read_text(encoding="ascii").strip().split()
    if len(fields) < 1 or not re.fullmatch(r"[0-9a-f]{64}", fields[0]):
        raise PipelineError(f"invalid clean SQL checksum file: {sidecar}")
    actual = sha256_small_file(path, progress_seconds)
    if fields[0] != actual:
        raise PipelineError(
            f"clean SQL checksum mismatch for {path.name}: "
            f"expected {fields[0]}, got {actual}"
        )


def publish_symlink(link: Path, target: Path) -> None:
    temporary = link.with_name(f".{link.name}.tmp")
    if link.exists() and not link.is_symlink():
        raise PipelineError(f"current pointer is not a symlink: {link}")
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(target.name)
    os.replace(temporary, link)


def publish_current_database(clean_dir: Path, versioned_dir: Path) -> Path:
    current = clean_dir / "mysql-current"
    publish_symlink(current, versioned_dir)
    return current


def publish_current_export(clean_dir: Path, export_path: Path) -> Path:
    current = clean_dir / "pkpbeacon-clean-latest.sql.gz"
    publish_symlink(current, export_path)
    return current


def publish_current_report(clean_dir: Path, report_path: Path) -> Path:
    current = clean_dir / "pkpbeacon-changes-latest.json"
    publish_symlink(current, report_path)
    return current


def repair_current_release_pointers(clean_dir: Path, version: str) -> bool:
    """Finish pointer publication after an interrupted artifact commit."""
    if not release_artifacts_complete(clean_dir, version):
        raise PipelineError(
            f"release {version} has no valid atomic completion manifest"
        )
    final_dir = clean_dir / f"mysql-{version}"
    final_export = clean_export_path(clean_dir, version)
    final_report = change_report_path(clean_dir, version)
    if not final_dir.is_dir():
        raise PipelineError(
            f"complete release {version} is missing its serving database: "
            f"{final_dir}"
        )
    targets = (
        (clean_dir / "mysql-current", final_dir),
        (clean_dir / "pkpbeacon-clean-latest.sql.gz", final_export),
        (clean_dir / "pkpbeacon-changes-latest.json", final_report),
    )
    changed = False
    for link, target in targets:
        if link.is_symlink() and link.resolve() == target.resolve():
            continue
        publish_symlink(link, target)
        changed = True
    return changed


def build_sql_prelude(
    *,
    snapshot: Snapshot,
    source_sha256: str,
    build_sql_sha256: str,
    mysql_version: str,
    full_rescan: bool,
) -> str:
    return "\n".join(
        (
            f"SET @ojs_snapshot_date = {sql_string(snapshot.version)};",
            "SET @ojs_snapshot_completed_at = "
            f"{sql_string(snapshot.completed_at.strftime('%Y-%m-%d %H:%M:%S'))};",
            f"SET @ojs_source_filename = {sql_string(snapshot.path.name)};",
            f"SET @ojs_source_size_bytes = {snapshot.size};",
            f"SET @ojs_source_sha256 = {sql_string(source_sha256)};",
            f"SET @ojs_build_sql_sha256 = {sql_string(build_sql_sha256)};",
            f"SET @ojs_mysql_version = {sql_string(mysql_version)};",
            f"SET @ojs_full_rescan = {1 if full_rescan else 0};",
            "SET @ojs_metadata_min_id = 0;",
            "SET @ojs_metadata_max_id = 18446744073709551615;",
        )
    )


def build_state_path(staging_dir: Path) -> Path:
    return staging_dir / BUILD_STATE_NAME


def write_build_state(
    staging_dir: Path,
    *,
    snapshot: Snapshot,
    source_sha256: str,
    build_sql_sha256: str,
    phase: str,
) -> None:
    path = build_state_path(staging_dir)
    temporary = path.with_suffix(".tmp")
    payload = json.dumps(
        {
            "pipeline_version": PIPELINE_VERSION,
            "snapshot_version": snapshot.version,
            "source_filename": snapshot.path.name,
            "source_size_bytes": snapshot.size,
            "source_sha256": source_sha256,
            "build_sql_sha256": build_sql_sha256,
            "phase": phase,
        },
        sort_keys=True,
    ) + "\n"
    with temporary.open("w", encoding="utf-8") as destination:
        destination.write(payload)
        destination.flush()
        os.fsync(destination.fileno())
    os.replace(temporary, path)
    descriptor = os.open(staging_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_build_state(staging_dir: Path) -> dict[str, object] | None:
    path = build_state_path(staging_dir)
    if not path.is_file():
        return None
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"invalid build checkpoint: {path}") from exc
    if not isinstance(state, dict):
        raise PipelineError(f"invalid build checkpoint: {path}")
    return state


def validate_resume_state(
    server: MySQLServer,
    *,
    staging_dir: Path,
    snapshot: Snapshot,
    database: str,
    password: str | None,
    build_sql_sha256: str,
) -> str | None:
    state = read_build_state(staging_dir)
    if state is not None:
        expected = {
            "pipeline_version": PIPELINE_VERSION,
            "snapshot_version": snapshot.version,
            "source_filename": snapshot.path.name,
            "source_size_bytes": snapshot.size,
            "build_sql_sha256": build_sql_sha256,
            "phase": "metadata_ready",
        }
        mismatches = [
            key
            for key, value in expected.items()
            if state.get(key) != value
        ]
        if mismatches:
            raise PipelineError(
                "staging checkpoint is not resumable; mismatched fields: "
                + ", ".join(mismatches)
            )
        source_sha256 = state.get("source_sha256")
        if not isinstance(source_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}",
            source_sha256,
        ):
            raise PipelineError("staging checkpoint has an invalid source SHA-256")
        return source_sha256

    # Compatibility path for a pre-checkpoint build interrupted exactly during
    # the metadata insert. Both inserts are atomic, so a nonempty source index
    # and empty metadata stage identify the safe phase boundary unambiguously.
    status = server.execute(
        """
        SELECT
            EXISTS(SELECT 1 FROM ojs_stage_source_index LIMIT 1),
            EXISTS(SELECT 1 FROM ojs_stage_sources LIMIT 1);
        """,
        database=database,
        password=password,
    )
    if status != "1\t0":
        raise PipelineError(
            "staging database has no metadata-ready checkpoint and cannot "
            "be adopted safely"
        )
    print("[resume] adopting pre-checkpoint metadata-ready staging database")
    return None


def extract_metadata_in_parallel(
    server: MySQLServer,
    *,
    metadata_sql: str,
    database: str,
    password: str | None,
    workers: int,
    progress_seconds: float,
) -> None:
    bounds = server.execute(
        """
        SELECT
            COALESCE(MIN(source_record_id), 0),
            COALESCE(MAX(source_record_id), 0)
        FROM ojs_stage_source_index
        WHERE needs_metadata_parse = 1;
        """,
        database=database,
        password=password,
        label="metadata-range-bounds",
    )
    try:
        minimum, maximum = (int(value) for value in bounds.split("\t"))
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"invalid metadata source-ID bounds: {bounds!r}") from exc
    if minimum == 0 and maximum == 0:
        print("[transform] no new or changed metadata rows")
        return

    ranges = source_id_ranges(
        minimum,
        maximum,
        workers * METADATA_SHARDS_PER_WORKER,
    )
    print(
        f"[transform] extracting metadata with {workers} workers "
        f"across {len(ranges)} indexed ranges"
    )
    completed = 0

    def run_shard(item: tuple[int, tuple[int, int]]) -> int:
        index, (start, end) = item
        prelude = "\n".join(
            (
                f"SET @ojs_metadata_min_id = {start};",
                f"SET @ojs_metadata_max_id = {end};",
            )
        )
        server.run_sql_text(
            metadata_sql,
            label="metadata-shard",
            database=database,
            password=password,
            prelude=prelude,
            shard=index + 1, total_ranges=len(ranges),
            range_min_id=start, range_max_id=end,
        )
        return index

    items = list(enumerate(ranges))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = set()
        try:
            for item in items:
                futures.add(executor.submit(run_shard, item))
            for future in concurrent.futures.as_completed(futures):
                index = future.result()
                completed += 1
                start, end = ranges[index]
                print(
                    f"[transform] metadata ranges: {completed}/{len(ranges)} complete; "
                    f"shard={index + 1} range_min_id={start} range_max_id={end}",
                    flush=True,
                )
        except BaseException:
            for future in futures:
                future.cancel()
            server.cancel_clients()
            raise


def build_snapshot_database(
    *,
    snapshot: Snapshot,
    clean_dir: Path,
    build_sql: Path,
    database: str,
    root_password: str,
    api_db_user: str,
    api_db_password: str,
    release_thresholds: ReleaseThresholds,
    allow_anomalous_release: bool,
    buffer_pool_size: str,
    progress_seconds: float,
    force_rebuild: bool,
    verify_existing: bool,
    verify_source_checksum: bool,
    full_rescan: bool,
    publish_current: bool,
    metadata_workers: int,
    resume_building: bool,
    compact_storage: bool = False,
) -> tuple[Path, BuildCounts | None]:
    tools = MySQLTools.discover()
    final_dir = clean_dir / f"mysql-{snapshot.version}"
    staging_dir = clean_dir / f"mysql-{snapshot.version}.building"
    final_export = clean_export_path(clean_dir, snapshot.version)
    final_report = change_report_path(clean_dir, snapshot.version)
    final_manifest = release_manifest_path(clean_dir, snapshot.version)
    temporary_export = clean_dir / f".{final_export.name}.tmp"
    temporary_report = clean_dir / f".{final_report.name}.tmp"
    temporary_manifest = clean_dir / f".{final_manifest.name}.tmp"
    build_sql_sha256 = sha256_small_file(build_sql)

    for owned_dir in (final_dir, staging_dir):
        if owned_dir.exists():
            assert_database_stopped(owned_dir)
    if final_dir.exists():
        if not force_rebuild:
            print(f"[publish] database already built: {final_dir}")
            counts = verify_existing_database(
                snapshot=snapshot,
                datadir=final_dir,
                build_sql=build_sql,
                database=database,
                root_password=root_password,
                buffer_pool_size=buffer_pool_size,
                progress_seconds=progress_seconds,
                verify_source_checksum=verify_source_checksum,
            )
            export_sha256 = _declared_checksum(final_export)
            report_sha256 = _declared_checksum(final_report)
            if export_sha256 is None or report_sha256 is None:
                raise PipelineError(
                    "verified release is missing a valid export/report checksum"
                )
            publish_release_manifest(
                clean_dir,
                version=snapshot.version,
                export_path=final_export,
                export_sha256=export_sha256,
                report_path=final_report,
                report_sha256=report_sha256,
            )
            if publish_current:
                publish_current_database(clean_dir, final_dir)
                publish_current_export(clean_dir, final_export)
                publish_current_report(clean_dir, final_report)
            return final_dir, counts
        if final_dir.is_symlink():
            raise PipelineError(f"refusing to replace symlinked database: {final_dir}")
        for pointer_name in ("mysql-current", "mysql-live"):
            current_database = clean_dir / pointer_name
            if (
                current_database.is_symlink()
                and current_database.resolve() == final_dir.resolve()
            ):
                raise PipelineError(
                    "refusing to force-rebuild the current serving database "
                    f"directory: {final_dir} ({pointer_name}); build in another "
                    "clean directory"
                )
        # Invalidate the prior commit marker before replacing any same-date
        # artifact. A crash from this point is therefore retried, not skipped.
        final_manifest.unlink(missing_ok=True)
        shutil.rmtree(final_dir)
    elif force_rebuild:
        final_manifest.unlink(missing_ok=True)

    resuming = staging_dir.exists() and resume_building
    if resuming and force_rebuild:
        state = read_build_state(staging_dir)
        if state is None:
            print(
                "[recover] replacing generated staging database without a "
                f"valid checkpoint: {staging_dir}"
            )
            shutil.rmtree(staging_dir)
            resuming = False
        else:
            resumable_fields = {
                "pipeline_version": PIPELINE_VERSION,
                "snapshot_version": snapshot.version,
                "source_filename": snapshot.path.name,
                "source_size_bytes": snapshot.size,
                "build_sql_sha256": build_sql_sha256,
                "phase": "metadata_ready",
            }
            if any(
                state.get(name) != value
                for name, value in resumable_fields.items()
            ):
                print(
                    "[recover] replacing non-resumable generated staging "
                    f"database: {staging_dir}"
                )
                shutil.rmtree(staging_dir)
                resuming = False
    if staging_dir.exists() and not resuming:
        if not force_rebuild:
            raise PipelineError(
                f"incomplete staging database exists: {staging_dir}; "
                "inspect it, use --resume-building, or rerun with --force-rebuild"
            )
        shutil.rmtree(staging_dir)
    temporary_export.unlink(missing_ok=True)
    temporary_report.unlink(missing_ok=True)
    temporary_manifest.unlink(missing_ok=True)

    prior_export = previous_clean_export(clean_dir, snapshot.version)
    print(f"[initialize] {staging_dir}")
    if prior_export:
        print(
            f"[history] prior clean state: {prior_export.path.name}"
        )
    else:
        print("[history] first observed snapshot; no prior clean state")

    server = MySQLServer(
        tools, staging_dir, buffer_pool_size, compact_storage,
        progress_seconds=progress_seconds,
    )
    active_password: str | None = None
    success = False
    counts: BuildCounts | None = None
    try:
        build_parts = split_build_sql(build_sql, compact_storage)
        prelude_factory: Callable[[], str] = lambda: build_sql_prelude(
            snapshot=snapshot,
            source_sha256=source_sha256,
            build_sql_sha256=build_sql_sha256,
            mysql_version=tools.version,
            full_rescan=full_rescan,
        )

        if resuming:
            print(f"[resume] starting {staging_dir}")
            server.start(active_password)
            checkpoint_sha256 = validate_resume_state(
                server,
                staging_dir=staging_dir,
                snapshot=snapshot,
                database=database,
                password=active_password,
                build_sql_sha256=build_sql_sha256,
            )
            print(f"[checksum] verifying raw {snapshot.path.name} for resume")
            source_sha256 = snapshot_sha256(snapshot, progress_seconds)
            if checkpoint_sha256 and source_sha256 != checkpoint_sha256:
                raise PipelineError(
                    "raw SQL checksum does not match the staging checkpoint"
                )
            write_build_state(
                staging_dir,
                snapshot=snapshot,
                source_sha256=source_sha256,
                build_sql_sha256=build_sql_sha256,
                phase="metadata_ready",
            )
            server.execute(
                "TRUNCATE TABLE ojs_stage_sources;",
                database=database,
                password=active_password,
            )
            (staging_dir / "PIPELINE_FAILED.txt").unlink(missing_ok=True)
        else:
            server.initialize()
            server.start(active_password)
            create_database(server, database)

            print(f"[transfer] loading raw {snapshot.path.name}")
            source_sha256 = server.import_snapshot(
                snapshot,
                database=database,
                password=active_password,
                progress_seconds=progress_seconds,
            )
            if prior_export:
                print(f"[transfer] loading history {prior_export.path.name}")
                server.import_clean_export(
                    prior_export,
                    database=database,
                    password=active_password,
                    progress_seconds=progress_seconds,
                )

            print(
                f"[transform] applying {build_sql.name} "
                f"({'full' if full_rescan else 'incremental'} metadata scan)"
            )
            server.run_sql_text(
                build_parts.prefix,
                label="source-index phase",
                database=database,
                password=active_password,
                prelude=prelude_factory(),
            )
            write_build_state(
                staging_dir,
                snapshot=snapshot,
                source_sha256=source_sha256,
                build_sql_sha256=build_sql_sha256,
                phase="metadata_ready",
            )

        extract_metadata_in_parallel(
            server,
            metadata_sql=build_parts.metadata,
            database=database,
            password=active_password,
            workers=metadata_workers,
            progress_seconds=progress_seconds,
        )
        write_build_state(
            staging_dir,
            snapshot=snapshot,
            source_sha256=source_sha256,
            build_sql_sha256=build_sql_sha256,
            phase="finalizing",
        )
        server.run_sql_text(
            build_parts.suffix,
            label="temporal finalization phase",
            database=database,
            password=active_password,
            prelude=prelude_factory(),
        )

        print("[validate] checking temporal SQL tables")
        counts, mysql_version = validate_database(
            server,
            database,
            active_password,
        )
        write_provenance(
            server,
            database=database,
            password=active_password,
            snapshot=snapshot,
            source_sha256=source_sha256,
            build_sql_sha256=build_sql_sha256,
            mysql_version=mysql_version,
            counts=counts,
        )
        report = collect_change_report(
            server,
            database=database,
            password=active_password,
            snapshot=snapshot,
            source_sha256=source_sha256,
            build_sql_sha256=build_sql_sha256,
            counts=counts,
        )
        anomalies = release_anomalies(report, release_thresholds)
        guard_status = (
            "overridden"
            if anomalies and allow_anomalous_release
            else "blocked"
            if anomalies
            else "passed"
        )
        report["release_guard"] = {
            "status": guard_status,
            "thresholds": release_thresholds.as_dict(),
            "anomalies": anomalies,
        }
        if anomalies and not allow_anomalous_release:
            quarantine_report = clean_dir / (
                f"pkpbeacon-changes-{snapshot.version}.quarantined.json"
            )
            publish_change_report(quarantine_report, report)
            metrics = ", ".join(str(item["metric"]) for item in anomalies)
            raise PipelineError(
                "release blocked by row-loss/merge safety thresholds "
                f"({metrics}); inspect {quarantine_report.name} and rerun "
                "manually with --allow-anomalous-release only after approval"
            )
        stage_change_report(final_report, report)

        prune_build_tables(server, database=database, password=active_password)
        cleaned_counts, _ = validate_database(
            server, database, active_password, require_raw=False
        )
        if cleaned_counts != counts:
            raise PipelineError("clean table counts changed during raw cleanup")

        print(f"[export] writing {final_export.name}")
        server.export_clean_tables(
            temporary_export,
            database=database,
            password=active_password,
        )
        configure_api_database_user(
            server,
            database=database,
            password=active_password,
            api_user=api_db_user,
            api_password=api_db_password,
        )
        set_root_password(server, active_password, root_password)
        active_password = root_password
        success = True
    except BaseException as exc:
        temporary_export.unlink(missing_ok=True)
        temporary_report.unlink(missing_ok=True)
        temporary_manifest.unlink(missing_ok=True)
        try:
            (staging_dir / "PIPELINE_FAILED.txt").write_text(
                f"{datetime.now().isoformat()}\n{exc}\n",
                encoding="utf-8",
            )
        except OSError:
            pass
        raise
    finally:
        server.shutdown(active_password)

    if not success or counts is None:
        raise PipelineError("database build did not complete")

    build_state_path(staging_dir).unlink(missing_ok=True)
    (staging_dir / "PIPELINE_FAILED.txt").unlink(missing_ok=True)
    os.replace(staging_dir, final_dir)
    marker = final_dir / "OJS_COMPACT_RELEASE.json"
    marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_date": snapshot.version,
                "source_sha256": source_sha256,
                "build_sql_sha256": build_sql_sha256,
                "clean_only": True,
                "pipeline_version": PIPELINE_VERSION,
                "data_directory_name": final_dir.name,
            },
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_export, final_export)
    os.replace(temporary_report, final_report)
    final_report.chmod(0o444)
    export_sha256 = sha256_file(final_export, progress_seconds)
    write_checksum_sidecar(final_export, export_sha256)
    report_sha256 = sha256_small_file(final_report)
    write_checksum_sidecar(final_report, report_sha256)
    publish_release_manifest(
        clean_dir,
        version=snapshot.version,
        export_path=final_export,
        export_sha256=export_sha256,
        report_path=final_report,
        report_sha256=report_sha256,
    )
    print(
        f"[publish] clean SQL {final_export.name} "
        f"({format_bytes(final_export.stat().st_size)})"
    )
    print(
        f"[report] {final_report.name}: "
        + ", ".join(
            f"{event_type}="
            f"{report['article_events']['by_type'][event_type]}"  # type: ignore[index]
            for event_type in CHANGE_EVENT_TYPES
        )
    )
    if publish_current:
        current_database = publish_current_database(clean_dir, final_dir)
        current_export = publish_current_export(clean_dir, final_export)
        current_report = publish_current_report(clean_dir, final_report)
        print(f"[publish] {current_database.name} -> {final_dir.name}")
        print(f"[publish] {current_export.name} -> {final_export.name}")
        print(f"[publish] {current_report.name} -> {final_report.name}")
    return final_dir, counts


def acquire_snapshot(args: argparse.Namespace) -> Snapshot:
    if args.snapshot:
        return inspect_snapshot(args.snapshot)

    latest = args.raw_dir / "pkpbeacon-latest.sql.gz"
    if not args.skip_download:
        command = [
            sys.executable,
            str(PROJECT_ROOT / "src" / "download_beacon.py"),
            "--raw-dir",
            str(args.raw_dir),
        ]
        if args.force_download:
            command.append("--force")
        print("[download] checking PKP Beacon")
        result = subprocess.run(command, **source_activity_child_options(args.raw_dir))
        if result.returncode != 0:
            raise PipelineError("Beacon download stage failed")
    if not latest.exists():
        latest = args.raw_dir / "pkpbeacon-latest.sql"
    if not latest.exists():
        raise PipelineError(
            f"latest snapshot pointer does not exist: {latest}; "
            "run without --skip-download"
        )
    return inspect_snapshot(latest)


def snapshots_to_process(
    *,
    target: Snapshot,
    raw_dir: Path,
    clean_dir: Path,
    explicit_snapshot: bool,
    rebuild_history: bool,
    verify_existing: bool = False,
) -> list[Snapshot]:
    exports = discover_clean_exports(clean_dir, require_complete=True)
    if rebuild_history:
        snapshots = discover_snapshots(raw_dir, through=target.version)
        if not snapshots:
            raise PipelineError("no dated raw SQL snapshots were found")
        return snapshots
    if explicit_snapshot:
        if exports and target.version < exports[-1].version:
            raise PipelineError(
                f"refusing to publish older explicit snapshot {target.version} "
                f"over clean release {exports[-1].version}; rebuild history in "
                "chronological order or use an isolated clean directory"
            )
        return [target]

    latest_export_version = exports[-1].version if exports else ""
    if latest_export_version:
        exported_versions = {item.version for item in exports}
        late_backfills = [
            item.version
            for item in discover_snapshots(raw_dir, through=latest_export_version)
            if item.version not in exported_versions
            and not retained_snapshot_matches_release(item, clean_dir)
        ]
        if late_backfills:
            raise PipelineError(
                "raw snapshot(s) arrived behind the published history: "
                + ", ".join(late_backfills)
                + "; refusing to skip them. Stop serving and run a controlled "
                "chronological history rebuild before the next publication."
            )
    pending = [
        item
        for item in discover_snapshots(raw_dir, through=target.version)
        if item.version > latest_export_version
    ]
    if pending:
        return pending
    if verify_existing:
        return [target]
    return []


@contextmanager
def pipeline_lock(clean_dir: Path):
    clean_dir.mkdir(parents=True, exist_ok=True)
    lock_path = clean_dir / ".pipeline.lock"
    common_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    common_flags |= getattr(os, "O_NOFOLLOW", 0)
    created = False
    try:
        descriptor = os.open(
            lock_path,
            common_flags | os.O_CREAT | os.O_EXCL,
            0o444,
        )
        created = True
    except FileExistsError:
        try:
            descriptor = os.open(lock_path, common_flags)
        except OSError as exc:
            raise PipelineError(
                f"could not safely open pipeline lock {lock_path}: {exc}"
            ) from exc
    except OSError as exc:
        raise PipelineError(
            f"could not safely create pipeline lock {lock_path}: {exc}"
        ) from exc
    if created:
        os.fchmod(descriptor, 0o444)
    with os.fdopen(descriptor, "r", encoding="ascii") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineError(
                "another data pipeline process is already running"
            ) from exc
        try:
            yield lock_path
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def progress_argument(value: str) -> float:
    try:
        return progress_interval(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Download, extract, import, temporally deduplicate, validate, "
            "and export PKP Beacon SQL snapshots."
        )
    )
    result.add_argument(
        "--snapshot",
        type=Path,
        help="use an existing pkpbeacon-YYYY-MM-DD.sql[.gz] instead of downloading",
    )
    result.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    result.add_argument("--clean-dir", type=Path, default=DEFAULT_CLEAN_DIR)
    result.add_argument("--build-sql", type=Path, default=DEFAULT_BUILD_SQL)
    result.add_argument("--database", default=DEFAULT_DATABASE)
    result.add_argument(
        "--api-db-user",
        default=os.environ.get("OJS_DB_API_USER", "ojs_api"),
        help="SELECT-only MySQL account provisioned for the API",
    )
    result.add_argument(
        "--skip-download",
        action="store_true",
        help="use the latest local SQL snapshot without contacting PKP",
    )
    result.add_argument(
        "--download-only",
        action="store_true",
        help="stop after downloading and validating compressed raw SQL",
    )
    result.add_argument(
        "--force-download",
        action="store_true",
        help="redownload and verify the remote snapshot; never overwrite immutable input",
    )
    result.add_argument(
        "--force-rebuild",
        action="store_true",
        help="replace existing outputs for snapshots selected by this run",
    )
    result.add_argument(
        "--rebuild-history",
        action="store_true",
        help="process every local raw snapshot through the selected target",
    )
    result.add_argument(
        "--full-rescan",
        action="store_true",
        help="reparse XML for all in-scope records, not only changed records",
    )
    result.add_argument(
        "--keep-intermediate-databases",
        action="store_true",
        help="retain MySQL data directories for non-current history snapshots",
    )
    result.add_argument(
        "--verify-existing",
        action="store_true",
        help="cold-start and verify an already-built current snapshot",
    )
    result.add_argument(
        "--verify-source-checksum",
        action="store_true",
        help="also recompute the full raw SQL SHA-256 while verifying",
    )
    result.add_argument(
        "--mysql-buffer-pool-size",
        default=os.environ.get("OJS_MYSQL_BUFFER_POOL_SIZE", "768M"),
    )
    result.add_argument(
        "--compact-storage",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("OJS_COMPACT_STORAGE", "0") == "1",
        help="create large raw and clean tables with native InnoDB compression",
    )
    result.add_argument(
        "--metadata-workers",
        type=int,
        default=os.environ.get("OJS_METADATA_WORKERS", "1"),
        help="parallel MySQL workers for XML metadata extraction (default: 1)",
    )
    result.add_argument(
        "--resume-building",
        action="store_true",
        help="resume an eligible post-index .building database",
    )
    result.add_argument(
        "--max-active-source-drop-fraction",
        type=float,
        default=os.environ.get("OJS_MAX_ACTIVE_SOURCE_DROP_FRACTION", "0.10"),
    )
    result.add_argument(
        "--max-active-article-drop-fraction",
        type=float,
        default=os.environ.get("OJS_MAX_ACTIVE_ARTICLE_DROP_FRACTION", "0.05"),
    )
    result.add_argument(
        "--max-article-removal-fraction",
        type=float,
        default=os.environ.get("OJS_MAX_ARTICLE_REMOVAL_FRACTION", "0.05"),
    )
    result.add_argument(
        "--max-article-merge-fraction",
        type=float,
        default=os.environ.get("OJS_MAX_ARTICLE_MERGE_FRACTION", "0.01"),
    )
    result.add_argument(
        "--allow-anomalous-release",
        action="store_true",
        help=(
            "publish despite release-guard anomalies; use only after reviewing "
            "the quarantined change report"
        ),
    )
    result.add_argument(
        "--progress-seconds", type=progress_argument,
        default=os.environ.get("OJS_PROGRESS_SECONDS", "30"),
        help="heartbeat interval, 1–3600 seconds (default: OJS_PROGRESS_SECONDS or 30)",
    )
    return result


def _main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)
    args = parser().parse_args(argv)
    args.raw_dir = args.raw_dir.expanduser().resolve()
    args.clean_dir = args.clean_dir.expanduser().resolve()
    args.build_sql = args.build_sql.expanduser().resolve()
    if not args.build_sql.is_file():
        print(f"error: build SQL does not exist: {args.build_sql}", file=sys.stderr)
        return 2
    if args.metadata_workers < 1:
        print("error: --metadata-workers must be at least 1", file=sys.stderr)
        return 2
    threshold_values = (
        args.max_active_source_drop_fraction,
        args.max_active_article_drop_fraction,
        args.max_article_removal_fraction,
        args.max_article_merge_fraction,
    )
    if any(value < 0 or value > 1 for value in threshold_values):
        print("error: release threshold fractions must be between 0 and 1", file=sys.stderr)
        return 2
    release_thresholds = ReleaseThresholds(*threshold_values)

    try:
        with source_activity(args.raw_dir), pipeline_lock(args.clean_dir):
            target = acquire_snapshot(args)
            print(
                f"[snapshot] target {target.path.name}; "
                f"completed {target.completed_at.isoformat()}; "
                f"{format_bytes(target.size)}"
            )
            if args.download_only:
                return 0

            root_password = os.environ.get("MYSQL_ROOT_PASSWORD", "")
            if not root_password:
                raise PipelineError("MYSQL_ROOT_PASSWORD must be set")
            api_db_password = os.environ.get("OJS_DB_API_PASSWORD", "")
            if not api_db_password:
                api_password_path = Path(
                    os.environ.get(
                        "OJS_DB_API_PASSWORD_FILE",
                        ".secrets/db-api-password",
                    )
                )
                api_db_password = read_private_secret(
                    api_password_path,
                    "API database password",
                )

            snapshots = snapshots_to_process(
                target=target,
                raw_dir=args.raw_dir,
                clean_dir=args.clean_dir,
                explicit_snapshot=args.snapshot is not None,
                rebuild_history=args.rebuild_history,
                verify_existing=args.verify_existing,
            )
            if not snapshots:
                repaired = repair_current_release_pointers(
                    args.clean_dir,
                    target.version,
                )
                if repaired:
                    print(
                        "[recover] repaired current database, export, and "
                        "change-report pointers"
                    )
                print(
                    "[complete] no newer PKP Beacon snapshot; "
                    f"{target.version} is already clean"
                )
                return 0
            if len(snapshots) > 1:
                print(
                    "[history] processing "
                    + ", ".join(item.version for item in snapshots)
                )

            latest_counts: BuildCounts | None = None
            latest_dir: Path | None = None
            for index, snapshot in enumerate(snapshots):
                is_target = index == len(snapshots) - 1
                latest_dir, latest_counts = build_snapshot_database(
                    snapshot=snapshot,
                    clean_dir=args.clean_dir,
                    build_sql=args.build_sql,
                    database=args.database,
                    root_password=root_password,
                    api_db_user=args.api_db_user,
                    api_db_password=api_db_password,
                    release_thresholds=release_thresholds,
                    allow_anomalous_release=args.allow_anomalous_release,
                    buffer_pool_size=args.mysql_buffer_pool_size,
                    progress_seconds=args.progress_seconds,
                    force_rebuild=args.force_rebuild,
                    verify_existing=args.verify_existing,
                    verify_source_checksum=(
                        args.verify_source_checksum and is_target
                    ),
                    full_rescan=args.full_rescan,
                    publish_current=is_target,
                    metadata_workers=args.metadata_workers,
                    resume_building=args.resume_building,
                    compact_storage=args.compact_storage,
                )
                if not is_target and not args.keep_intermediate_databases:
                    shutil.rmtree(latest_dir)
                    print(
                        f"[cleanup] removed intermediate database {latest_dir.name}; "
                        "its clean SQL export is retained"
                    )

            if latest_counts is None or latest_dir is None:
                raise PipelineError("no snapshot was processed")
            print(
                "[complete] "
                f"{latest_counts.source_records} source aliases; "
                f"{latest_counts.active_articles} active articles; "
                f"{latest_counts.removed_articles} removed; "
                f"{latest_counts.merged_articles} merged; "
                f"{latest_counts.events} events in target snapshot"
            )
    except SourceActivityBusy:
        print(json.dumps({"status": "skipped", "reason": "source_activity_busy",
                          "message": "Download or wrangling already in progress; no new download started."}))
        return 0
    except (OSError, PipelineError, SourceActivityError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except PipelineInterrupted as exc:
        print(f"error: pipeline interrupted by signal {exc.signum}", file=sys.stderr)
        return 128 + exc.signum
    except KeyboardInterrupt:
        print("error: pipeline interrupted", file=sys.stderr)
        return 130
    return 0


def main(argv: list[str] | None = None) -> int:
    with interruptible_pipeline():
        return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
