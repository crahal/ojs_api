#!/usr/bin/env python3
"""Build and switch a compact Beacon release, retaining rollback until verified.

The live pointer is independent of the pipeline's candidate pointer. A durable
journal makes an interrupted switch roll back before another build can start.
Only authenticated, generated artifacts are eligible for bounded retention.
"""
from __future__ import annotations

import argparse
import base64
import fcntl
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

import download_beacon
import publish_live
import run_pipeline
from progress_logging import Progress, progress_interval

ROOT = Path(__file__).resolve().parents[1]
DATE = r"\d{4}-\d{2}-\d{2}"
DATADIR_RE = re.compile(rf"mysql-({DATE})$")
MARKER = "OJS_COMPACT_RELEASE.json"
GIB = 1024 ** 3
COMMAND_TIMEOUT_MESSAGE = "command timed out; check service availability and timeout settings"


class UpdateError(RuntimeError):
    pass


def read_json(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise UpdateError(f"expected a regular JSON file: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise UpdateError(f"expected a JSON object: {path}")
    return value


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path: Path, value: dict) -> None:
    if path.is_symlink():
        raise UpdateError(f"refusing symlinked state file: {path}")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump(value, destination, indent=2, sort_keys=True)
            destination.write("\n")
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def automatic_lock(clean: Path):
    clean.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(clean / ".automatic-update.lock",
                         os.O_CREAT | os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise UpdateError("another automatic update is already running") from exc
        yield
    finally:
        os.close(descriptor)


def safe_child(directory: Path, name: str) -> Path:
    if Path(name).name != name or name in ("", ".", ".."):
        raise UpdateError("state contains an unsafe artifact name")
    path = directory / name
    if path.is_symlink() or path.parent.resolve() != directory.resolve():
        raise UpdateError(f"refusing symlinked artifact: {path}")
    return path


def live_directory(clean: Path, name: str = "mysql-live") -> Path | None:
    link = clean / name
    if not link.is_symlink():
        if link.exists():
            raise UpdateError(f"expected a managed symlink: {link}")
        return None
    target = link.resolve(strict=True)
    if target.parent != clean or not DATADIR_RE.fullmatch(target.name):
        raise UpdateError(f"unexpected database pointer target: {link}")
    if not target.is_dir():
        raise UpdateError(f"database pointer is not a directory: {link}")
    return target


def set_live(clean: Path, target: Path | None) -> None:
    link = clean / "mysql-live"
    if link.exists() and not link.is_symlink():
        raise UpdateError(f"refusing to replace unmanaged live directory: {link}")
    if target is None:
        link.unlink(missing_ok=True)
    else:
        descriptor, temporary = tempfile.mkstemp(prefix=".mysql-live.", dir=clean)
        os.close(descriptor)
        Path(temporary).unlink()
        try:
            Path(temporary).symlink_to(target.name)
            os.replace(temporary, link)
        finally:
            Path(temporary).unlink(missing_ok=True)
    sync_directory(clean)


@dataclass(frozen=True)
class Release:
    date: str
    datadir: Path
    export: Path
    source_sha256: str
    build_sql_sha256: str
    report: dict


def validate_release(clean: Path, date: str, *, require_directory: bool = True) -> Release:
    if not re.fullmatch(DATE, date):
        raise UpdateError("invalid release date")
    export = safe_child(clean, f"pkpbeacon-clean-{date}.sql.gz")
    for name in (export.name + ".sha256", f"pkpbeacon-changes-{date}.json",
                 f"pkpbeacon-changes-{date}.json.sha256", f"pkpbeacon-release-{date}.json"):
        path = safe_child(clean, name)
        if not path.is_file():
            raise UpdateError(f"incomplete release: {path}")
    with Progress("update-release-checksums", disk_path=clean,
                  step="clean-export") as progress:
        checksum = publish_live.checksum_for(export)
        publish_live.verify_export_checksum(export, checksum)
        progress.event("export-verified", step="release-provenance")
        source_hash, build_hash = publish_live.candidate_provenance(export, date, checksum)
    report = read_json(clean / f"pkpbeacon-changes-{date}.json")
    if report.get("release_guard", {}).get("status") not in ("passed", "overridden"):
        raise UpdateError(f"release guard has not passed for {date}")
    datadir = safe_child(clean, f"mysql-{date}")
    if require_directory:
        marker = read_json(datadir / MARKER)
        expected = {"schema_version": 1, "snapshot_date": date,
                    "source_sha256": source_hash, "build_sql_sha256": build_hash,
                    "clean_only": True, "data_directory_name": datadir.name}
        if any(marker.get(key) != value for key, value in expected.items()):
            raise UpdateError(f"invalid compact database ownership marker: {datadir}")
    return Release(date, datadir, export, source_hash, build_hash, report)


class Runner:
    """Commands never include secret values, and failures do not echo stderr."""
    def run(self, command: list[str], *, cwd: Path, env: dict,
            timeout: float = 180) -> str:
        try:
            result = subprocess.run(command, cwd=cwd, env=env, text=True,
                                    capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            # TimeoutExpired contains argv and captured output; neither is
            # suitable for coordinator logs or exception chaining.
            raise UpdateError(COMMAND_TIMEOUT_MESSAGE) from None
        if result.returncode:
            if "Beacon authentication failed" in result.stderr:
                raise UpdateError("Beacon authentication failed; check the configured credentials file")
            raise UpdateError(f"{Path(command[0]).name} command failed (exit {result.returncode})")
        return result.stdout

    def child(self, command: list[str], *, cwd: Path, env: dict,
              reserve_check, timeout: float) -> None:
        # Interrupt Python first so its finally block shuts down local mysqld;
        # client subprocesses are interrupted with the same process group.
        process = subprocess.Popen(command, cwd=cwd, env=env, start_new_session=True)
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                reserve_check()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise UpdateError("update exceeded its configured runtime")
                try:
                    process.wait(timeout=min(15, remaining))
                except subprocess.TimeoutExpired:
                    pass
            if process.returncode:
                raise UpdateError(f"update child failed (exit {process.returncode})")
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=180)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
            raise


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise UpdateError("API health check refused an HTTP redirect")


class Coordinator:
    def __init__(self, args: argparse.Namespace, runner: Runner | None = None):
        self.args = args
        self.root = args.project_root.expanduser().resolve()
        self.clean = self.root / "data" / "clean"
        self.raw = self.root / "data" / "raw"
        for directory in (self.root / "data", self.clean, self.raw):
            if directory.is_symlink():
                raise UpdateError(f"refusing symlinked data directory: {directory}")
        self.env = os.environ.copy()
        self.runner = runner or Runner()
        self.journal = self.clean / ".compact-update.json"
        self.deadline = time.monotonic() + args.max_runtime_hours * 3600
        self._compatible_tools: run_pipeline.MySQLTools | None = None

    def command(self, command, *, env=None, timeout=180):
        return self.runner.run(command, cwd=self.root, env=env or self.env, timeout=timeout)

    def capacity(self, minimum: float) -> None:
        path = self.clean if self.clean.exists() else self.root
        free = shutil.disk_usage(path).free
        if free < minimum * GIB:
            raise UpdateError(f"disk reserve reached: {free / GIB:.1f} GiB free; "
                              f"{minimum:g} GiB required; live database retained")

    def memory_capacity(self) -> None:
        minimum_mb = int(self.env.get("OJS_MIN_AVAILABLE_MEMORY_MB", "512"))
        if minimum_mb < 0:
            raise UpdateError("OJS_MIN_AVAILABLE_MEMORY_MB must be nonnegative")
        memory = Path("/proc/meminfo").read_text()
        available = re.search(r"^MemAvailable:\s+(\d+) kB$", memory, re.MULTILINE)
        if available and int(available.group(1)) < minimum_mb * 1024:
            raise UpdateError(f"less than {minimum_mb} MiB memory is available for the build")

    def child(self, command, *, stage: str = "update-child") -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise UpdateError("update exceeded its configured runtime")
        # The child reports data progress. Parent heartbeats only show that
        # supervision continues; reserve checks must not reset data-idle time.
        with Progress(stage, disk_path=self.clean, step="supervise-child"):
            self.runner.child(command, cwd=self.root, env=self.env,
                              reserve_check=lambda: self.capacity(self.args.reserve_gb),
                              timeout=remaining)

    def download_command(self) -> list[str]:
        credentials = self.args.credentials_file
        if not credentials.is_absolute():
            credentials = self.root / credentials
        return [sys.executable, str(self.root / "src" / "download_beacon.py"),
                "--raw-dir", str(self.raw), "--url", self.args.url,
                "--credentials-file", str(credentials)]

    def probe(self) -> dict:
        with Progress("update-source-check", disk_path=self.root):
            output = self.command(self.download_command() + ["--check", "--json"])
            try:
                probe = json.loads(output)
                if not isinstance(probe.get("needs_download"), bool):
                    raise ValueError
                return probe
            except (ValueError, TypeError, AttributeError) as exc:
                raise UpdateError("downloader returned invalid check JSON") from exc

    def compose(self, datadir: Path, *arguments: str) -> str:
        self.compatible_mysql_tools()
        env = dict(self.env, OJS_LIVE_DATA_DIR=str(datadir.resolve(strict=True)))
        return self.command(["docker", "compose", "--project-directory", str(self.root),
                             *arguments], env=env, timeout=max(180, self.args.health_timeout))

    def compatible_mysql_tools(self) -> run_pipeline.MySQLTools:
        """Reject physical-datadir handover between different MySQL patches.

        This is deliberately lazy: unchanged HEAD checks need no MySQL tools.
        A coordinator invocation uses one fixed environment and tool version.
        """
        if self._compatible_tools is not None:
            return self._compatible_tools
        image = self.env.get("OJS_MYSQL_IMAGE", "mysql:8.4.0")
        match = re.fullmatch(r"[^\s@]+:(8\.4\.\d+)", image)
        if match is None:
            raise UpdateError("OJS_MYSQL_IMAGE must use an exact numeric MySQL 8.4 patch tag")
        tools = run_pipeline.MySQLTools.discover()
        if tools.version != match.group(1):
            raise UpdateError(f"host MySQL {tools.version} does not match serving image patch "
                              f"{match.group(1)}; update both together before publication")
        self._compatible_tools = tools
        return tools

    def protected_paths(self) -> list[Path]:
        identifiers = self.command(["docker", "ps", "--all", "--quiet"]).split()
        protected = []
        if identifiers:
            # All containers, including stopped containers and other projects.
            payload = json.loads(self.command(["docker", "inspect", *identifiers]))
            for container in payload:
                protected.extend(Path(mount["Source"]).resolve()
                                 for mount in container.get("Mounts", []) if mount.get("Source"))
        for process in Path("/proc").glob("[0-9]*"):
            try:
                if process.joinpath("comm").read_text().strip() != "mysqld":
                    continue
                arguments = process.joinpath("cmdline").read_bytes().decode().split("\0")
                for index, value in enumerate(arguments):
                    if value.startswith("--datadir="):
                        protected.append(Path(value.split("=", 1)[1]).resolve())
                    elif value == "--datadir" and index + 1 < len(arguments):
                        protected.append(Path(arguments[index + 1]).resolve())
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError as exc:
                raise UpdateError("cannot inspect running database processes safely") from exc
        return protected

    def require_unmounted(self, path: Path) -> None:
        canonical = path.resolve()
        for protected in self.protected_paths():
            if canonical == protected or canonical in protected.parents or protected in canonical.parents:
                raise UpdateError(f"artifact is mounted or used by MySQL: {path}")

    def guard_build_directories(self) -> None:
        live = live_directory(self.clean)
        for path in self.clean.glob("mysql-????-??-??*"):
            if not re.fullmatch(rf"mysql-{DATE}(?:\.building)?", path.name):
                continue
            safe_child(self.clean, path.name)
            if path != live:
                self.require_unmounted(path)

    def verify_database(self, release: Release) -> None:
        tools = self.compatible_mysql_tools()
        self.require_unmounted(release.datadir)
        password = self.env.get("MYSQL_ROOT_PASSWORD")
        if not password:
            raise UpdateError("MYSQL_ROOT_PASSWORD must be set")
        server = run_pipeline.MySQLServer(tools, release.datadir,
                                         self.env.get("OJS_MYSQL_BUFFER_POOL_SIZE", "768M"))
        database = self.env.get("OJS_DB_NAME", "pkpbeacon_db")
        try:
            server.start(password)
            counts, _ = run_pipeline.validate_database(server, database, password, require_raw=False)
            tables = server.execute("SELECT table_name FROM information_schema.tables "
                                    "WHERE table_schema = DATABASE() ORDER BY table_name;",
                                    database=database, password=password).splitlines()
            if set(tables) != set(run_pipeline.CLEAN_EXPORT_TABLES) | {"ojs_pipeline_metadata"}:
                raise UpdateError("candidate database is not clean-only")
            provenance = server.execute("SELECT DATE_FORMAT(snapshot_date, '%Y-%m-%d'), "
                                        "source_sha256, build_sql_sha256 FROM ojs_snapshots "
                                        "ORDER BY snapshot_date DESC LIMIT 1;",
                                        database=database, password=password).split("\t")
            if provenance != [release.date, release.source_sha256, release.build_sql_sha256]:
                raise UpdateError("candidate database provenance does not match release")
            report = release.report
            expected_counts = (report["source_rows"]["retained_total"], report["source_rows"]["active"],
                               report["articles"]["retained_total"], report["articles"]["active"],
                               report["articles"]["removed_tombstones"], report["articles"]["merged_tombstones"],
                               report["article_events"]["total"])
            if tuple(vars(counts).values()) != expected_counts:
                raise UpdateError("candidate database counts do not match release report")
        finally:
            server.shutdown(password)

    def credentials(self) -> tuple[str, str]:
        path = Path(self.env.get("OJS_API_CREDENTIALS_HOST_FILE", ".secrets/api-client.env"))
        if not path.is_absolute():
            path = self.root / path
        if path.stat().st_mode & 0o077:
            raise UpdateError("API credentials file permissions must be private")
        values = {}
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                key, separator, value = line.partition("=")
                if not separator or key not in ("OJS_API_USERNAME", "OJS_API_KEY"):
                    raise UpdateError("invalid API credentials file")
                values[key] = value.strip()
        if not values.get("OJS_API_USERNAME") or not values.get("OJS_API_KEY"):
            raise UpdateError("API credentials file is incomplete")
        return values["OJS_API_USERNAME"], values["OJS_API_KEY"]

    def check_health(self, release: Release | None, datadir: Path) -> None:
        with Progress("update-api-health", disk_path=self.clean):
            self._check_health(release, datadir)

    def _check_health(self, release: Release | None, datadir: Path) -> None:
        username, password = self.credentials()
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        base = self.args.api_url.rstrip("/")
        parsed = urlsplit(base)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise UpdateError("API check URL must not contain credentials or query parameters")
        if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in
                                              ("127.0.0.1", "localhost", "::1")):
            raise UpdateError("API credentials require HTTPS or a loopback URL")
        deadline = time.monotonic() + self.args.health_timeout
        while True:
            try:
                ids = self.compose(datadir, "ps", "--quiet", "mysql", "ojs-api").split()
                if len(ids) != 2:
                    raise UpdateError("Compose services are not running")
                states = json.loads(self.command(["docker", "inspect", *ids]))
                if any(item.get("State", {}).get("Health", {}).get("Status") != "healthy" for item in states):
                    raise UpdateError("Compose services are not healthy")
                mysql = next((item for item in states if item.get("Config", {}).get("Labels", {}).get(
                    "com.docker.compose.service") == "mysql"), None)
                if mysql is None or not any(mount.get("Destination") == "/var/lib/mysql" and
                                            Path(mount.get("Source", "")).resolve() == datadir
                                            for mount in mysql.get("Mounts", [])):
                    raise UpdateError("serving MySQL mount does not match selected database")
                opener = build_opener(NoRedirect())
                for endpoint in ("/health", "/meta"):
                    request = Request(base + endpoint, headers={"Authorization": "Basic " + token})
                    with opener.open(request, timeout=5) as response:
                        payload = json.load(response)
                if release is not None:
                    snapshot = payload.get("latest_snapshot", {})
                    expected = {"snapshot_date": release.date, "source_sha256": release.source_sha256,
                                "build_sql_sha256": release.build_sql_sha256}
                    if any(snapshot.get(key) != value for key, value in expected.items()):
                        raise UpdateError("authenticated API release provenance does not match candidate")
                return
            except Exception:
                if time.monotonic() >= deadline:
                    raise UpdateError("API release verification timed out; no old artifacts were deleted") from None
                time.sleep(min(3, max(0, deadline - time.monotonic())))

    def rollback(self, state: dict) -> None:
        with Progress("update-rollback", disk_path=self.clean):
            self._rollback(state)

    def _rollback(self, state: dict) -> None:
        candidate = safe_child(self.clean, state["candidate"])
        previous = safe_child(self.clean, state["previous"]) if state.get("previous") else None
        self.compose(candidate, "stop", "ojs-api", "mysql")
        set_live(self.clean, previous)
        if previous is not None:
            self.compose(previous, "up", "--detach", "--force-recreate", "mysql", "ojs-api")
            self.check_health(None, previous)
        else:
            # Remove stopped first-install containers, whose bind mounts would
            # otherwise prevent a safe cold verification on the next attempt.
            self.compose(candidate, "down")
        self.journal.unlink()
        sync_directory(self.clean)

    def recover(self) -> None:
        if not self.journal.exists():
            return
        state = read_json(self.journal)
        if state.get("schema_version") != 1 or not DATADIR_RE.fullmatch(state.get("candidate", "")):
            raise UpdateError("unrecognized update recovery journal")
        if state.get("previous") is not None and not DATADIR_RE.fullmatch(state["previous"]):
            raise UpdateError("invalid previous database in recovery journal")
        self.compatible_mysql_tools()
        if state.get("phase") == "verified":
            release = validate_release(self.clean, state["snapshot_date"])
            if live_directory(self.clean) != release.datadir:
                raise UpdateError("verified journal and live pointer disagree")
            self.check_health(release, release.datadir)
            self.finish(state, release)
        elif state.get("phase") in ("prepared", "switching"):
            print("[recover] restoring the previous live database before continuing")
            self.rollback(state)
        else:
            raise UpdateError("unknown update recovery phase")

    def promote(self, release: Release) -> None:
        with Progress("update-promotion", disk_path=self.clean):
            self._promote(release)

    def _promote(self, release: Release) -> None:
        self.compatible_mysql_tools()
        previous = live_directory(self.clean)
        if previous == release.datadir:
            self.compose(release.datadir, "up", "--detach", "mysql", "ojs-api")
            self.check_health(release, release.datadir)
            self.finish({"schema_version": 1, "phase": "verified", "candidate": release.datadir.name,
                         "previous": None, "snapshot_date": release.date}, release)
            return
        if previous and previous.name > release.datadir.name:
            raise UpdateError("refusing to replace live database with an older release")
        self.capacity(self.args.reserve_gb)
        self.credentials()  # Fail before stopping services if the health check cannot authenticate.
        with Progress("update-cold-verification", disk_path=self.clean):
            self.verify_database(release)
        state = {"schema_version": 1, "phase": "prepared", "candidate": release.datadir.name,
                 "previous": previous.name if previous else None, "snapshot_date": release.date,
                 "started_at": datetime.now(timezone.utc).isoformat()}
        atomic_json(self.journal, state)
        try:
            self.compose(previous or release.datadir, "stop", "ojs-api", "mysql")
            state["phase"] = "switching"
            atomic_json(self.journal, state)
            set_live(self.clean, release.datadir)
            self.compose(release.datadir, "up", "--detach", "--force-recreate", "mysql", "ojs-api")
            self.check_health(release, release.datadir)
        except BaseException:
            self.rollback(state)
            raise
        state["phase"] = "verified"
        atomic_json(self.journal, state)
        self.finish(state, release)

    def cleanup_plan(self, current: Release) -> list[dict]:
        plan = []
        for manifest in sorted(self.clean.glob("pkpbeacon-release-????-??-??.json")):
            date = manifest.name.removeprefix("pkpbeacon-release-").removesuffix(".json")
            if date >= current.date:
                continue
            try:
                old = validate_release(self.clean, date, require_directory=False)
                paths = []
                if old.datadir.exists():
                    validate_release(self.clean, date)
                    paths.append(old.datadir)
                raw = safe_child(self.raw, f"pkpbeacon-{date}.sql.gz")
                if raw.exists():
                    metadata = download_beacon.read_validated_metadata(raw)
                    if metadata and metadata.get("source_url") == self.args.url and metadata.get("uncompressed_sha256") == old.source_sha256:
                        paths.extend((raw, safe_child(self.raw, raw.name + ".metadata.json")))
                paths.extend((old.export, old.export.with_name(old.export.name + ".sha256")))
                for path in paths:
                    self.require_unmounted(path)
                    if path.is_dir():
                        for base, directories, files in os.walk(path, followlinks=False):
                            if any((Path(base) / name).is_symlink() for name in directories + files):
                                raise UpdateError(f"refusing cleanup of a tree containing symlinks: {path}")
                    info = path.stat(follow_symlinks=False)
                    size = sum(item.stat().st_size for item in path.rglob("*") if item.is_file()) if path.is_dir() else info.st_size
                    plan.append({"parent": "raw" if path.parent == self.raw else "clean", "name": path.name,
                                 "device": info.st_dev, "inode": info.st_ino, "bytes": size,
                                 "directory": path.is_dir(), "deleted": False})
            except (OSError, ValueError, RuntimeError) as exc:
                print(f"[retain] {date}: artifacts not eligible for automatic cleanup ({type(exc).__name__})")
        return plan

    def finish(self, state: dict, release: Release) -> None:
        with Progress("update-cleanup", disk_path=self.clean,
                      step="plan-cleanup", deleted_artifacts=0, deleted_bytes=0) as progress:
            self._finish(state, release, progress)

    def _finish(self, state: dict, release: Release, progress: Progress) -> None:
        if self.args.no_prune:
            progress.update(step="cleanup-disabled")
            self.journal.unlink(missing_ok=True)
            sync_directory(self.clean)
            return
        if "cleanup" not in state:
            state["cleanup"] = self.cleanup_plan(release)
            atomic_json(self.journal, state)
        progress.update(step="remove-owned-artifacts", total_artifacts=len(state["cleanup"]))
        deleted_artifacts = 0
        deleted_bytes = 0
        for entry in state["cleanup"]:
            directory = self.raw if entry["parent"] == "raw" else self.clean
            path = safe_child(directory, entry["name"])
            # The journal is a deletion authorization only for precisely named
            # generated artifacts validated before the first deletion.
            allowed = (re.fullmatch(rf"pkpbeacon-{DATE}\.sql\.gz(?:\.metadata\.json)?", path.name) if directory == self.raw else
                       DATADIR_RE.fullmatch(path.name) or re.fullmatch(rf"pkpbeacon-clean-{DATE}\.sql\.gz(?:\.sha256)?", path.name))
            if not allowed or path == release.datadir or path == release.export:
                raise UpdateError("unsafe artifact in cleanup journal")
            artifact_date = re.search(DATE, path.name).group(0)
            if artifact_date >= release.date:
                raise UpdateError("cleanup journal contains a current or future artifact")
            if path.exists():
                info = path.stat(follow_symlinks=False)
                if (info.st_dev, info.st_ino) != (entry["device"], entry["inode"]):
                    raise UpdateError(f"cleanup target changed since validation: {path}")
                self.require_unmounted(path)
                if entry["directory"]:
                    if not shutil.rmtree.avoids_symlink_attacks:
                        raise UpdateError("safe directory cleanup is unavailable on this platform")
                    shutil.rmtree(path)
                else:
                    path.unlink()
            entry["deleted"] = True
            atomic_json(self.journal, state)
            deleted_artifacts += 1
            deleted_bytes += entry["bytes"]
            progress.update(deleted_artifacts=deleted_artifacts, deleted_bytes=deleted_bytes)
        audit_path = self.clean / f"pkpbeacon-cleanup-{release.date}.json"
        prior_entries = read_json(audit_path).get("artifacts", []) if audit_path.exists() else []
        entries = {(entry["parent"], entry["name"], entry["device"], entry["inode"]): entry
                   for entry in prior_entries + state["cleanup"]}
        audit = {"schema_version": 1, "release": release.date,
                 "completed_at": datetime.now(timezone.utc).isoformat(),
                 "deleted_count": sum(entry["deleted"] for entry in entries.values()),
                 "deleted_bytes": sum(entry["bytes"] for entry in entries.values() if entry["deleted"]),
                 "artifacts": list(entries.values())}
        atomic_json(audit_path, audit)
        self.journal.unlink(missing_ok=True)
        sync_directory(self.clean)
        print(f"[cleanup] removed {audit['deleted_count']} generated artifacts ({audit['deleted_bytes']} bytes)")

    def execute(self) -> dict:
        if self.args.check:
            return {"probe": self.probe(), "live": str(live_directory(self.clean)) if self.clean.exists() and live_directory(self.clean) else None,
                    "recovery_pending": self.journal.exists()}
        with automatic_lock(self.clean):
            with run_pipeline.pipeline_lock(self.clean), publish_live.publisher_lock(self.root):
                self.recover()
            if not self.args.publish_only:
                probe = self.probe()
                current = live_directory(self.clean)
                known = probe.get("known_snapshot")
                known_name = Path(known).name if isinstance(known, str) else ""
                known_match = re.fullmatch(rf"pkpbeacon-({DATE})\.sql\.gz", known_name)
                if not probe["needs_download"] and current is not None and known_match and current.name == f"mysql-{known_match.group(1)}":
                    metadata = download_beacon.read_validated_metadata(safe_child(self.raw, known_name))
                    marker = read_json(current / MARKER)
                    if metadata and metadata.get("uncompressed_sha256") == marker.get("source_sha256"):
                        return {"status": "unchanged", "release": known_match.group(1)}
                self.compatible_mysql_tools()
                self.capacity(self.args.min_free_gb)
                self.memory_capacity()
                if probe["needs_download"]:
                    self.child(self.download_command(), stage="update-download")
                self.guard_build_directories()
                self.child([sys.executable, str(self.root / "src" / "run_pipeline.py"),
                            "--raw-dir", str(self.raw), "--clean-dir", str(self.clean),
                            "--skip-download", "--resume-building", "--force-rebuild", "--compact-storage"],
                           stage="update-build")
                # Builds can last days; recheck after possible host package changes.
                self._compatible_tools = None
            with run_pipeline.pipeline_lock(self.clean), publish_live.publisher_lock(self.root):
                candidate = live_directory(self.clean, "mysql-current")
                if candidate is None:
                    raise UpdateError("no completed pipeline candidate is available")
                release = validate_release(self.clean, DATADIR_RE.fullmatch(candidate.name).group(1))
                self.promote(release)
                return {"status": "published", "release": release.date}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--project-root", type=Path, default=ROOT)
    result.add_argument("--url", default=os.getenv("OJS_BEACON_URL", download_beacon.DEFAULT_URL))
    result.add_argument("--credentials-file", type=Path, default=Path(os.getenv("OJS_BEACON_CREDENTIALS_FILE", ".secrets/beacon.ini")))
    result.add_argument("--check", action="store_true", help="read-only HEAD and local status; no locks or writes")
    result.add_argument("--publish-only", action="store_true", help="promote the completed pipeline candidate without downloading or building")
    result.add_argument("--no-prune", action="store_true", help="keep prior generated releases after successful promotion")
    result.add_argument("--min-free-gb", type=float, default=float(os.getenv("OJS_MIN_FREE_GB", "20")))
    result.add_argument("--reserve-gb", type=float, default=float(os.getenv("OJS_DISK_RESERVE_GB", "8")))
    result.add_argument("--max-runtime-hours", type=float, default=float(os.getenv("OJS_MAX_RUNTIME_HOURS", "168")))
    result.add_argument("--health-timeout", type=float, default=300)
    result.add_argument("--api-url", default=f"http://127.0.0.1:{os.getenv('OJS_API_PORT', '8000')}")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    limits = (args.min_free_gb, args.reserve_gb, args.max_runtime_hours, args.health_timeout)
    if not all(math.isfinite(value) for value in limits) or min(args.min_free_gb, args.reserve_gb) < 0 or args.max_runtime_hours <= 0 or args.health_timeout <= 0:
        print("error: invalid capacity or timeout setting", file=sys.stderr)
        return 2
    def interrupt(signum, frame):
        raise UpdateError("update interrupted; recovery state retained")

    signal.signal(signal.SIGTERM, interrupt)
    try:
        progress_interval()
        print(json.dumps(Coordinator(args).execute(), sort_keys=True))
        return 0
    except subprocess.TimeoutExpired:
        # Also protect timeouts raised by nested pipeline/control operations.
        print(f"error: {COMMAND_TIMEOUT_MESSAGE}", file=sys.stderr)
        return 1
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("error: update interrupted; recovery state retained", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
