#!/usr/bin/env python3
"""Atomically publish a clean SQL export into the running Compose MySQL."""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORT = PROJECT_ROOT / "data" / "clean" / "pkpbeacon-clean-latest.sql.gz"
EXPORT_RE = re.compile(r"pkpbeacon-clean-(\d{4}-\d{2}-\d{2})\.sql\.gz$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
BUFFER_SIZE = 8 * 1024 * 1024
CLEAN_TABLES = (
    "ojs_snapshots",
    "ojs_articles",
    "ojs_article_sources",
    "ojs_article_keys",
    "ojs_article_events",
)


class PublishError(RuntimeError):
    pass


def quote_identifier(value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value) or len(value) > 64:
        raise PublishError(f"unsafe MySQL identifier: {value!r}")
    return f"`{value}`"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def checksum_for(path: Path) -> str:
    return artifact_checksum_for(path, "clean export")


def artifact_checksum_for(path: Path, description: str) -> str:
    sidecar = path.with_name(f"{path.name}.sha256")
    if not sidecar.is_file():
        raise PublishError(f"{description} checksum does not exist: {sidecar}")
    fields = sidecar.read_text(encoding="ascii").strip().split()
    if not fields or not re.fullmatch(r"[0-9a-f]{64}", fields[0]):
        raise PublishError(f"invalid {description} checksum: {sidecar}")
    return fields[0]


def verify_export_checksum(path: Path, expected_checksum: str) -> None:
    """Read and authenticate the complete compressed export before use."""
    verify_artifact_checksum(
        path,
        expected_checksum,
        "clean export",
        before="import",
    )


def verify_artifact_checksum(
    path: Path,
    expected_checksum: str,
    description: str,
    *,
    before: str = "use",
) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(BUFFER_SIZE):
            digest.update(chunk)
    actual_checksum = digest.hexdigest()
    if actual_checksum != expected_checksum:
        raise PublishError(
            f"{description} checksum mismatch before {before}: "
            f"expected {expected_checksum}, got {actual_checksum}"
        )


def candidate_provenance(
    export_path: Path,
    release: str,
    export_sha256: str,
) -> tuple[str, str]:
    report_path = export_path.with_name(f"pkpbeacon-changes-{release}.json")
    if not report_path.is_file():
        raise PublishError(
            f"candidate change report does not exist: {report_path}"
        )
    expected_checksum = artifact_checksum_for(report_path, "change report")
    verify_artifact_checksum(report_path, expected_checksum, "change report")
    manifest_path = export_path.with_name(f"pkpbeacon-release-{release}.json")
    if not manifest_path.is_file():
        raise PublishError(
            f"candidate release completion manifest does not exist: {manifest_path}"
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if report["schema_version"] != 1:
            raise PublishError(
                f"unsupported candidate change report schema: {report_path}"
            )
        snapshot = report["snapshot"]
        report_release = snapshot["date"]
        source_sha256 = snapshot["source_sha256"]
        build_sql_sha256 = snapshot["build_sql_sha256"]
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeError) as exc:
        raise PublishError(
            f"invalid candidate change report: {report_path}"
        ) from exc
    expected_manifest = {
        "schema_version": 1,
        "snapshot_date": release,
        "clean_export_filename": export_path.name,
        "clean_export_sha256": export_sha256,
        "change_report_filename": report_path.name,
        "change_report_sha256": expected_checksum,
    }
    manifest_mismatches = [
        name
        for name, value in expected_manifest.items()
        if manifest.get(name) != value
    ]
    if manifest_mismatches:
        raise PublishError(
            "candidate release completion manifest does not match: "
            + ", ".join(manifest_mismatches)
        )
    if report_release != release:
        raise PublishError(
            "candidate change report date does not match the clean export: "
            f"{report_release!r} != {release!r}"
        )
    invalid_fields = [
        name
        for name, value in (
            ("source_sha256", source_sha256),
            ("build_sql_sha256", build_sql_sha256),
        )
        if not isinstance(value, str)
        or not re.fullmatch(r"[0-9a-f]{64}", value)
    ]
    if invalid_fields:
        raise PublishError(
            "candidate change report has invalid provenance fields: "
            + ", ".join(invalid_fields)
        )
    return source_sha256, build_sql_sha256


@contextmanager
def publisher_lock(project_root: Path) -> Iterator[Path]:
    """Serialize publishers for one Compose project without waiting."""
    canonical_root = project_root.resolve()
    lock_directory = canonical_root / "data" / "clean"
    lock_directory.mkdir(parents=True, exist_ok=True)
    lock_path = lock_directory / ".publish-live.lock"
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
            raise PublishError(
                f"could not safely open publisher lock {lock_path}: {exc}"
            ) from exc
    except OSError as exc:
        raise PublishError(
            f"could not safely create publisher lock {lock_path}: {exc}"
        ) from exc
    if created:
        os.fchmod(descriptor, 0o444)
    with os.fdopen(descriptor, "r", encoding="ascii") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PublishError(
                "another live publisher process is already running for "
                f"{canonical_root}"
            ) from exc
        try:
            yield lock_path
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


class DigestReader:
    def __init__(self, source: BinaryIO) -> None:
        self.source = source
        self.digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        chunk = self.source.read(size)
        self.digest.update(chunk)
        return chunk

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


class ComposeMySQL:
    def __init__(self, project_root: Path) -> None:
        if shutil.which("docker") is None:
            raise PublishError("docker is not on PATH")
        self.prefix = [
            "docker",
            "compose",
            "--project-directory",
            str(project_root),
            "exec",
            "-T",
            "mysql",
        ]

    def command(self, database: str | None = None) -> list[str]:
        shell = (
            'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; '
            "exec mysql --protocol=socket --user=root --batch "
            '--skip-column-names --default-character-set=utf8mb4 "$@"'
        )
        command = [*self.prefix, "sh", "-eu", "-c", shell, "mysql-client"]
        if database is not None:
            command.append(f"--database={database}")
        return command

    def execute(self, sql: str, database: str | None = None) -> str:
        result = subprocess.run(
            self.command(database),
            input=sql,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            raise PublishError(
                "live MySQL command failed: " + result.stderr.strip()[-4000:]
            )
        return result.stdout.strip()

    def import_export(
        self,
        export_path: Path,
        database: str,
        expected_checksum: str,
    ) -> None:
        with tempfile.TemporaryFile() as stderr:
            process = subprocess.Popen(
                self.command(database),
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=stderr,
            )
            assert process.stdin is not None
            hashing_source: DigestReader | None = None
            try:
                with export_path.open("rb") as raw_source:
                    hashing_source = DigestReader(raw_source)
                    with gzip.GzipFile(fileobj=hashing_source, mode="rb") as source:
                        while chunk := source.read(BUFFER_SIZE):
                            process.stdin.write(chunk)
                process.stdin.close()
                process.stdin = None
                return_code = process.wait()
            except (BrokenPipeError, EOFError, OSError) as exc:
                process.kill()
                process.wait()
                raise PublishError(f"could not stream clean export: {exc}") from exc

            stderr.seek(0)
            error_text = stderr.read().decode("utf-8", errors="replace")
            if return_code != 0:
                raise PublishError(
                    "clean export import failed: " + error_text.strip()[-4000:]
                )
            assert hashing_source is not None
            actual_checksum = hashing_source.hexdigest()
            if actual_checksum != expected_checksum:
                raise PublishError(
                    "clean export checksum mismatch after import: "
                    f"expected {expected_checksum}, got {actual_checksum}"
                )


def table_count(mysql: ComposeMySQL, database: str) -> int:
    names = ", ".join(sql_string(name) for name in CLEAN_TABLES)
    value = mysql.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema = {sql_string(database)} "
        f"AND table_name IN ({names}) AND table_type = 'BASE TABLE';"
    )
    return int(value or 0)


def latest_snapshot(mysql: ComposeMySQL, database: str) -> str | None:
    if table_count(mysql, database) != len(CLEAN_TABLES):
        return None
    value = mysql.execute(
        "SELECT COALESCE(DATE_FORMAT(MAX(snapshot_date), '%Y-%m-%d'), '') "
        "FROM ojs_snapshots;",
        database,
    )
    return value or None


def snapshot_provenance(
    mysql: ComposeMySQL,
    database: str,
    release: str,
) -> tuple[str, str]:
    value = mysql.execute(
        "SELECT source_sha256, build_sql_sha256 FROM ojs_snapshots "
        f"WHERE snapshot_date = {sql_string(release)} LIMIT 1;",
        database,
    )
    fields = value.split("\t")
    if len(fields) != 2 or any(
        re.fullmatch(r"[0-9a-f]{64}", field) is None for field in fields
    ):
        raise PublishError(
            f"live snapshot {release} has missing or invalid provenance"
        )
    return fields[0], fields[1]


def atomic_swap_sql(
    live_database: str,
    staging_database: str,
    previous_database: str,
    has_live_tables: bool,
) -> str:
    live = quote_identifier(live_database)
    staging = quote_identifier(staging_database)
    previous = quote_identifier(previous_database)
    renames: list[str] = []
    for table in CLEAN_TABLES:
        quoted_table = quote_identifier(table)
        if has_live_tables:
            renames.append(
                f"{live}.{quoted_table} TO {previous}.{quoted_table}"
            )
        renames.append(f"{staging}.{quoted_table} TO {live}.{quoted_table}")
    return "SET SESSION lock_wait_timeout = 300; RENAME TABLE\n  " + ",\n  ".join(
        renames
    ) + ";"


def rollback_sql(
    live_database: str,
    staging_database: str,
    previous_database: str,
    had_live_tables: bool,
) -> str:
    live = quote_identifier(live_database)
    staging = quote_identifier(staging_database)
    previous = quote_identifier(previous_database)
    renames: list[str] = []
    for table in CLEAN_TABLES:
        quoted_table = quote_identifier(table)
        renames.append(f"{live}.{quoted_table} TO {staging}.{quoted_table}")
        if had_live_tables:
            renames.append(
                f"{previous}.{quoted_table} TO {live}.{quoted_table}"
            )
    return "SET SESSION lock_wait_timeout = 300; RENAME TABLE\n  " + ",\n  ".join(
        renames
    ) + ";"


def _publish_locked(
    *,
    export_path: Path,
    database: str,
    project_root: Path,
    keep_previous: bool,
) -> bool:
    resolved_export = export_path.resolve(strict=True)
    match = EXPORT_RE.fullmatch(resolved_export.name)
    if match is None:
        raise PublishError(
            "clean export must be named pkpbeacon-clean-YYYY-MM-DD.sql.gz"
        )
    release = match.group(1)
    expected_checksum = checksum_for(resolved_export)
    candidate_source_sha256, candidate_build_sql_sha256 = candidate_provenance(
        resolved_export,
        release,
        expected_checksum,
    )
    staging_database = f"{database}_next"
    previous_database = f"{database}_previous"
    for name in (database, staging_database, previous_database):
        quote_identifier(name)

    mysql = ComposeMySQL(project_root)
    live_table_count = table_count(mysql, database)
    if live_table_count not in (0, len(CLEAN_TABLES)):
        raise PublishError(
            f"live database has only {live_table_count}/{len(CLEAN_TABLES)} "
            "required API tables"
        )
    live_release = latest_snapshot(mysql, database)
    if live_release == release:
        live_source_sha256, live_build_sql_sha256 = snapshot_provenance(
            mysql,
            database,
            release,
        )
        provenance_mismatches = [
            name
            for name, live_value, candidate_value in (
                (
                    "source_sha256",
                    live_source_sha256,
                    candidate_source_sha256,
                ),
                (
                    "build_sql_sha256",
                    live_build_sql_sha256,
                    candidate_build_sql_sha256,
                ),
            )
            if live_value != candidate_value
        ]
        if provenance_mismatches:
            raise PublishError(
                f"live snapshot {release} has different provenance in "
                + ", ".join(provenance_mismatches)
                + "; refusing to skip or replace a same-date release"
            )
        print(f"[live] {release} is already being served")
        return False
    if live_release is not None and live_release > release:
        raise PublishError(
            f"refusing to replace newer live release {live_release} with {release}"
        )

    # Candidate SQL is never executed until its complete compressed bytes have
    # matched the sidecar. Exact same-release no-ops above remain cheap.
    verify_export_checksum(resolved_export, expected_checksum)
    mysql.execute(
        f"DROP DATABASE IF EXISTS {quote_identifier(staging_database)}; "
        f"CREATE DATABASE {quote_identifier(staging_database)} "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
    )
    swapped = False
    try:
        print(f"[live] importing {resolved_export.name} into {staging_database}")
        mysql.import_export(
            resolved_export,
            staging_database,
            expected_checksum,
        )
        staged_count = table_count(mysql, staging_database)
        if staged_count != len(CLEAN_TABLES):
            raise PublishError(
                f"staging database has {staged_count}/{len(CLEAN_TABLES)} "
                "required API tables"
            )
        staged_release = latest_snapshot(mysql, staging_database)
        if staged_release != release:
            raise PublishError(
                f"staged snapshot is {staged_release!r}, expected {release!r}"
            )
        staged_source_sha256, staged_build_sql_sha256 = snapshot_provenance(
            mysql,
            staging_database,
            release,
        )
        staged_mismatches = [
            name
            for name, staged_value, candidate_value in (
                (
                    "source_sha256",
                    staged_source_sha256,
                    candidate_source_sha256,
                ),
                (
                    "build_sql_sha256",
                    staged_build_sql_sha256,
                    candidate_build_sql_sha256,
                ),
            )
            if staged_value != candidate_value
        ]
        if staged_mismatches:
            raise PublishError(
                "staged clean export does not match its change report in "
                + ", ".join(staged_mismatches)
            )

        mysql.execute(
            f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database)} "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci; "
            f"DROP DATABASE IF EXISTS {quote_identifier(previous_database)}; "
            f"CREATE DATABASE {quote_identifier(previous_database)} "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
        )
        mysql.execute(
            atomic_swap_sql(
                database,
                staging_database,
                previous_database,
                live_table_count == len(CLEAN_TABLES),
            )
        )
        swapped = True
        if latest_snapshot(mysql, database) != release:
            raise PublishError("post-swap live snapshot validation failed")
    except Exception:
        if swapped:
            try:
                mysql.execute(
                    rollback_sql(
                        database,
                        staging_database,
                        previous_database,
                        live_table_count == len(CLEAN_TABLES),
                    )
                )
                print("[live] rolled back the failed table swap", file=sys.stderr)
            except Exception as rollback_error:
                print(
                    f"[live] automatic rollback failed: {rollback_error}",
                    file=sys.stderr,
                )
        try:
            mysql.execute(
                f"DROP DATABASE IF EXISTS {quote_identifier(staging_database)};"
            )
        except Exception:
            pass
        raise

    mysql.execute(
        f"DROP DATABASE IF EXISTS {quote_identifier(staging_database)};"
    )
    if not keep_previous:
        mysql.execute(
            f"DROP DATABASE IF EXISTS {quote_identifier(previous_database)};"
        )
    print(f"[live] atomically published snapshot {release}")
    return True


def publish(
    *,
    export_path: Path,
    database: str,
    project_root: Path,
    keep_previous: bool,
) -> bool:
    with publisher_lock(project_root):
        return _publish_locked(
            export_path=export_path,
            database=database,
            project_root=project_root,
            keep_previous=keep_previous,
        )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Atomically publish a clean export to Compose MySQL."
    )
    result.add_argument("--export", type=Path, default=DEFAULT_EXPORT)
    result.add_argument(
        "--database",
        default=os.getenv("OJS_DB_NAME", "pkpbeacon_db"),
    )
    result.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    result.add_argument("--keep-previous", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        publish(
            export_path=args.export,
            database=args.database,
            project_root=args.project_root.resolve(),
            keep_previous=args.keep_previous,
        )
    except (OSError, PublishError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("error: live publication interrupted", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
