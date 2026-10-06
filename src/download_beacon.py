"""Fetch immutable, compressed Beacon snapshots and cheaply check for updates."""

from __future__ import annotations

import argparse
import base64
import configparser
import fcntl
import getpass
import gzip
import hashlib
import http.client
import json
import os
import re
import shutil
import sys
import tempfile
import zlib
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from progress_logging import Progress, progress_interval
from source_activity import SourceActivityBusy, source_activity

DEFAULT_URL = "https://beacon.publicknowledgeproject.org/mysql/pkpbeacon.gz"
DEFAULT_USERNAME = "beacon-research"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CREDENTIALS_FILE = Path.home() / ".config" / "ojs-api" / "beacon.ini"
BUFFER_SIZE = 8 * 1024 * 1024
DEFAULT_MIN_FREE_BYTES = 512 * 1024**2
DEFAULT_MAX_EXPANDED_BYTES = 1024**4
USER_AGENT = "ojs-api-beacon-downloader/2.0"
SNAPSHOT_PATTERN = re.compile(r"pkpbeacon-(\d{4}-\d{2}-\d{2})\.sql\.gz\Z")
STATE_NAME = ".pkpbeacon-remote.json"
PART_NAME = ".pkpbeacon-download.sql.gz.part"


@dataclass(frozen=True)
class RemoteFile:
    size: int | None
    last_modified: datetime | None
    etag: str | None


class SameOriginRedirectHandler(HTTPRedirectHandler):
    """Never forward Basic authentication to a different origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urljoin(req.full_url, newurl)
        if _origin(req.full_url) != _origin(target):
            raise HTTPError(
                req.full_url, code, "refusing cross-origin redirect", headers, fp
            )
        redirected = super().redirect_request(req, fp, code, msg, headers, target)
        # urllib turns redirected HEAD requests into GET unless preserved here.
        if redirected is not None and req.get_method() == "HEAD":
            redirected.method = "HEAD"
        return redirected


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parsed = urlsplit(url)
    default_port = 443 if parsed.scheme.lower() == "https" else 80
    return parsed.scheme.lower(), parsed.hostname, parsed.port or default_port


def _require_secure_url(url: str, allow_insecure_localhost: bool = False) -> None:
    parsed = urlsplit(url)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credentials must not be embedded in the Beacon URL")
    if parsed.scheme == "https" and parsed.hostname:
        return
    if (
        allow_insecure_localhost
        and parsed.scheme == "http"
        and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        return
    raise ValueError("authenticated Beacon requests require HTTPS")


def _headers(username: str, password: str) -> dict[str, str]:
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    return {
        "Authorization": f"Basic {credentials}",
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "identity",
    }


def read_credentials(path: Path) -> tuple[str | None, str | None]:
    try:
        if not path.exists():
            return None, None
        if path.stat().st_mode & 0o077:
            raise RuntimeError(
                "credentials file permissions are too open; run chmod 600 on it"
            )
        config = configparser.ConfigParser(interpolation=None)
        with path.open(encoding="utf-8") as source:
            config.read_file(source)
        if not config.has_section("beacon"):
            raise RuntimeError("credentials file has no [beacon] section")
        return config.get("beacon", "username", fallback=None), config.get(
            "beacon", "password", fallback=None
        )
    except (OSError, UnicodeError, configparser.Error):
        # ConfigParser errors include offending lines, which may contain passwords.
        raise RuntimeError(
            "could not read credentials file; check its INI format and access"
        ) from None


def _integer_header(headers, name: str) -> int | None:
    try:
        value = int(headers.get(name, ""))
        return value if value >= 0 else None
    except (TypeError, ValueError):
        return None


def _modified_header(headers) -> datetime | None:
    value = headers.get("Last-Modified")
    if not value:
        return None
    try:
        modified = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=UTC)
    return modified.astimezone(UTC)


def _remote_from_headers(headers) -> RemoteFile:
    return RemoteFile(
        _integer_header(headers, "Content-Length"),
        _modified_header(headers),
        headers.get("ETag"),
    )


def _remote_identity(remote: RemoteFile) -> dict:
    return {
        "size": remote.size,
        "last_modified": remote.last_modified.isoformat()
        if remote.last_modified
        else None,
        "etag": remote.etag,
    }


def _if_range(remote: RemoteFile) -> str | None:
    if remote.etag and not remote.etag.startswith("W/"):
        return remote.etag
    if remote.last_modified:
        return format_datetime(remote.last_modified.astimezone(UTC), usegmt=True)
    return None


def probe_remote(
    url: str,
    username: str,
    password: str,
    timeout: float,
    *,
    allow_insecure_localhost: bool = False,
) -> RemoteFile:
    _require_secure_url(url, allow_insecure_localhost)
    request = Request(url, headers=_headers(username, password), method="HEAD")
    try:
        with Progress("beacon-source-check"), build_opener(SameOriginRedirectHandler()).open(
            request, timeout=timeout
        ) as response:
            return _remote_from_headers(response.headers)
    except HTTPError as exc:
        if exc.code in (401, 403):
            raise RuntimeError("Beacon authentication failed") from None
        raise RuntimeError(f"Beacon metadata request failed: HTTP {exc.code}") from None
    except (URLError, OSError, http.client.HTTPException):
        raise RuntimeError(
            "Beacon metadata request failed; check connectivity and TLS"
        ) from None


def snapshot_version(remote: RemoteFile, override: str | None) -> str:
    """Return a date hint; only the validated SQL footer determines the filename."""
    if override:
        try:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", override):
                raise ValueError
            return date.fromisoformat(override).isoformat()
        except ValueError:
            raise ValueError("--version must use YYYY-MM-DD") from None
    return (remote.last_modified or datetime.now(UTC)).date().isoformat()


def _format_bytes(value: int | None) -> str:
    if value is None:
        return "unknown"
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return str(value)


def _env_bytes(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be a nonnegative integer byte count") from None
    if value < 0:
        raise ValueError(f"{name} must be a nonnegative integer byte count")
    return value


def _stat_binding(path: Path) -> dict:
    stat = path.stat()
    return {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ctime_ns": stat.st_ctime_ns,
        "inode": stat.st_ino,
        "device": stat.st_dev,
    }


class _HashingReader:
    def __init__(self, source, progress: Progress):
        self.source = source
        self.progress = progress
        self.digest = hashlib.sha256()
        self.size = 0

    def read(self, size=-1):
        chunk = self.source.read(size)
        self.digest.update(chunk)
        self.size += len(chunk)
        self.progress.update(processed_bytes=self.size)
        return chunk


def validate_archive(path: Path, *, max_expanded_bytes: int | None = None) -> dict:
    """Check CRC, SQL header/footer and both hashes without writing expanded SQL."""
    if max_expanded_bytes is None:
        max_expanded_bytes = _env_bytes(
            "OJS_BEACON_MAX_EXPANDED_BYTES", DEFAULT_MAX_EXPANDED_BYTES
        )
    if max_expanded_bytes < 0:
        raise ValueError("maximum expanded size must be nonnegative")
    before = _stat_binding(path)
    with Progress("beacon-archive-validation", process_pid=os.getpid(), disk_path=path.parent,
                  processed_bytes=0, total_bytes=before["size"], expanded_bytes=0,
                  step="decompress-and-hash") as progress:
        return _validate_archive(path, before, max_expanded_bytes, progress)


def _validate_archive(path: Path, before: dict, max_expanded_bytes: int,
                      progress: Progress) -> dict:
    digest = hashlib.sha256()
    total = 0
    prefix = b""
    tail = b""
    try:
        with path.open("rb") as compressed:
            reader = _HashingReader(compressed, progress)
            with gzip.GzipFile(fileobj=reader, mode="rb") as source:
                while chunk := source.read(BUFFER_SIZE):
                    total += len(chunk)
                    if total > max_expanded_bytes:
                        raise RuntimeError(
                            "expanded SQL exceeds OJS_BEACON_MAX_EXPANDED_BYTES"
                        )
                    if len(prefix) < 4096:
                        prefix = (prefix + chunk)[:4096]
                    tail = (tail + chunk)[-4096:]
                    digest.update(chunk)
                    progress.update(expanded_bytes=total)
    except (EOFError, OSError, zlib.error):
        raise RuntimeError(
            "downloaded gzip file failed validation (CRC, truncation, or format)"
        ) from None
    progress.update(step="sql-header-and-footer")
    if b"MySQL dump" not in prefix:
        raise RuntimeError("downloaded file is gzip data but not a MySQL dump")
    match = re.search(
        rb"(?:^|\n)-- Dump completed on (\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}:\d{2})\s*\Z",
        tail,
    )
    if not match:
        raise RuntimeError("MySQL dump has no final extraction timestamp")
    try:
        completed = datetime.strptime(  # noqa: DTZ007 -- footer has no timezone
            re.sub(rb"\s+", b" ", match.group(1)).decode("ascii"), "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        raise RuntimeError("MySQL dump has an invalid extraction timestamp") from None
    after = _stat_binding(path)
    if before != after or reader.size != after["size"]:
        raise RuntimeError("snapshot changed during gzip validation")
    return {
        "schema_version": 1,
        "dump_date": completed.date().isoformat(),
        "dump_datetime": completed.isoformat(sep=" "),
        "compressed_sha256": reader.digest.hexdigest(),
        "compressed_size": reader.size,
        "uncompressed_sha256": digest.hexdigest(),
        "uncompressed_size": total,
        "stat": after,
        "validated_at": datetime.now(UTC).isoformat(),
    }


def _metadata_path(path: Path) -> Path:
    return path.with_name(path.name + ".metadata.json")


def _read_json(path: Path) -> dict | None:
    try:
        # A corrupt sidecar must not allocate unbounded RAM.
        with path.open(encoding="utf-8") as source:
            text = source.read(65537)
        if len(text) > 65536:
            return None
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except (OSError, UnicodeError, ValueError):
        return None


def _write_json(path: Path, data: dict) -> None:
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".beacon-",
            suffix=".json.tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            json.dump(data, output, sort_keys=True, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def read_validated_metadata(path: Path) -> dict | None:
    """Return cached validation only when required fields and file stats agree."""
    path = path.resolve()
    metadata = _read_json(_metadata_path(path))
    if (
        not metadata
        or type(metadata.get("schema_version")) is not int
        or metadata["schema_version"] != 1
    ):
        return None
    for key in ("compressed_sha256", "uncompressed_sha256"):
        if not isinstance(metadata.get(key), str) or not re.fullmatch(
            r"[0-9a-f]{64}", metadata[key]
        ):
            return None
    for key in ("compressed_size", "uncompressed_size"):
        if type(metadata.get(key)) is not int or metadata[key] <= 0:
            return None
    try:
        completed = datetime.strptime(  # noqa: DTZ007 -- footer has no timezone
            metadata["dump_datetime"], "%Y-%m-%d %H:%M:%S"
        )
        if metadata["dump_date"] != completed.date().isoformat():
            return None
        binding = _stat_binding(path)
        cached_binding = metadata.get("stat")
        if not isinstance(cached_binding, dict) or any(
            type(cached_binding.get(key)) is not int for key in binding
        ):
            return None
        if cached_binding != binding or metadata["compressed_size"] != binding["size"]:
            return None
        match = SNAPSHOT_PATTERN.fullmatch(path.name)
        if match and match.group(1) != metadata["dump_date"]:
            return None
    except (OSError, KeyError, TypeError, ValueError):
        return None
    return metadata


def validate_cached_snapshot(path: Path) -> dict:
    """Pipeline entry point: use a bound sidecar or validate once and create it."""
    path = path.resolve()
    metadata = read_validated_metadata(path)
    if metadata is not None:
        return metadata
    metadata = validate_archive(path)
    match = SNAPSHOT_PATTERN.fullmatch(path.name)
    if match and match.group(1) != metadata["dump_date"]:
        raise RuntimeError("snapshot filename date differs from the SQL footer date")
    _write_json(_metadata_path(path), metadata)
    return metadata


def known_remote_snapshot(raw_dir: Path, url: str, remote: RemoteFile) -> Path | None:
    """Resolve a previous HEAD identity to the actual SQL footer date."""
    if not _if_range(remote):
        return None
    state = _read_json(raw_dir / STATE_NAME)
    if (
        not state
        or state.get("schema_version") != 1
        or state.get("source_url") != url
        or state.get("remote") != _remote_identity(remote)
    ):
        return None
    filename = state.get("snapshot")
    if not isinstance(filename, str) or not SNAPSHOT_PATTERN.fullmatch(filename):
        return None
    path = raw_dir / filename
    metadata = read_validated_metadata(path)
    if not metadata or metadata["compressed_sha256"] != state.get("compressed_sha256"):
        return None
    if remote.size is not None and metadata["compressed_size"] != remote.size:
        return None
    return path


def _update_latest_snapshot(raw_dir: Path) -> Path:
    snapshots = [
        path
        for path in raw_dir.glob("pkpbeacon-*.sql.gz")
        if path.is_file() and SNAPSHOT_PATTERN.fullmatch(path.name)
    ]
    if not snapshots:
        raise RuntimeError("no versioned compressed SQL snapshots found")
    newest = max(snapshots, key=lambda path: path.name)
    latest = raw_dir / "pkpbeacon-latest.sql.gz"
    temporary = raw_dir / ".pkpbeacon-latest.sql.gz.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(newest.name)
    os.replace(temporary, latest)
    return latest


def _check_free_space(path: Path, incoming_bytes: int, floor: int) -> None:
    if shutil.disk_usage(path.parent).free < incoming_bytes + floor:
        raise RuntimeError(
            "insufficient free space for compressed download and OJS_BEACON_MIN_FREE_BYTES reserve"
        )


def _download_response(
    response, part_path: Path, offset: int, remote: RemoteFile, floor: int
) -> int:
    with Progress("beacon-transfer", process_pid=os.getpid(), disk_path=part_path.parent,
                  processed_bytes=offset, total_bytes=remote.size) as progress:
        return _stream_download_response(response, part_path, offset, remote, floor, progress)


def _stream_download_response(
    response, part_path: Path, offset: int, remote: RemoteFile, floor: int,
    progress: Progress,
) -> int:
    received = _remote_from_headers(response.headers)
    if (remote.etag and remote.etag != received.etag) or (
        remote.last_modified and remote.last_modified != received.last_modified
    ):
        raise RuntimeError(
            "remote identity changed between HEAD and GET; retry the download"
        )
    status = response.status
    response_size = received.size
    expected_size = remote.size
    if offset and status == 206:
        match = re.fullmatch(
            r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", "")
        )
        if not match or int(match.group(1)) != offset:
            raise RuntimeError("unexpected Content-Range in resumed download")
        start, end, total = map(int, match.groups())
        if (
            end < start
            or end != total - 1
            or (expected_size is not None and total != expected_size)
        ):
            raise RuntimeError("unexpected Content-Range size in resumed download")
        if response_size is not None and response_size != end - start + 1:
            raise RuntimeError("Content-Range and Content-Length disagree")
        expected_size = total
        mode = "ab"
    elif status == 200:
        offset = 0
        mode = "wb"
        if (
            expected_size is not None
            and response_size is not None
            and response_size != expected_size
        ):
            raise RuntimeError(
                "remote size changed between HEAD and GET; retry the download"
            )
        expected_size = expected_size if expected_size is not None else response_size
    else:
        raise RuntimeError(f"unexpected download response: HTTP {status}")
    _check_free_space(part_path, max(0, (expected_size or 0) - offset), floor)
    downloaded = offset
    progress.update(processed_bytes=offset, total_bytes=expected_size, step="receive")
    with part_path.open(mode) as destination:
        while chunk := response.read(BUFFER_SIZE):
            if expected_size is not None and downloaded + len(chunk) > expected_size:
                raise RuntimeError("download exceeded the advertised compressed size")
            _check_free_space(part_path, len(chunk), floor)
            destination.write(chunk)
            downloaded += len(chunk)
            progress.update(processed_bytes=downloaded)
        progress.update(step="sync-download")
        destination.flush()
        os.fsync(destination.fileno())
    if expected_size is not None and downloaded != expected_size:
        raise RuntimeError(
            f"incomplete download: got {downloaded} bytes, expected {expected_size}"
        )
    return downloaded


def download_snapshot(
    url: str,
    username: str,
    password: str,
    raw_dir: Path,
    version: str,
    remote: RemoteFile,
    timeout: float,
    force: bool = False,
    *,
    allow_insecure_localhost: bool = False,
    expected_version: str | None = None,
) -> Path:
    """Keep gzip immutable; force only bypasses the HEAD cache/resume state.

    ``version`` remains a compatibility date hint; the SQL footer is authoritative.
    ``expected_version`` optionally enforces a requested date.
    """
    with source_activity(raw_dir):
        _require_secure_url(url, allow_insecure_localhost)
        raw_dir.mkdir(parents=True, exist_ok=True)
        with (raw_dir / ".pkpbeacon-download.lock").open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("another Beacon download is already running") from None
            return _download_snapshot_locked(
                url, username, password, raw_dir, remote, timeout, force, expected_version
            )


def _download_snapshot_locked(
    url, username, password, raw_dir, remote, timeout, force, expected_version
):
    if remote.size == 0:
        raise RuntimeError("remote Beacon archive is empty")
    existing = None if force else known_remote_snapshot(raw_dir, url, remote)
    if existing is not None:
        if (
            expected_version
            and SNAPSHOT_PATTERN.fullmatch(existing.name).group(1) != expected_version
        ):
            raise RuntimeError("requested version differs from the SQL footer date")
        _update_latest_snapshot(raw_dir)
        print(f"Remote unchanged; snapshot already validated: {existing}")
        return existing
    archive_path = raw_dir / PART_NAME
    partial_metadata_path = _metadata_path(archive_path)
    identity_path = raw_dir / (PART_NAME + ".identity.json")
    identity = {
        "schema_version": 1,
        "source_url": url,
        "remote": _remote_identity(remote),
    }
    validator = _if_range(remote)
    saved_identity = _read_json(identity_path)
    offset = archive_path.stat().st_size if archive_path.exists() else 0
    if (
        force
        or not validator
        or saved_identity != identity
        or (remote.size is not None and offset > remote.size)
    ):
        archive_path.unlink(missing_ok=True)
        partial_metadata_path.unlink(missing_ok=True)
        offset = 0
    _write_json(identity_path, identity)
    floor = _env_bytes("OJS_BEACON_MIN_FREE_BYTES", DEFAULT_MIN_FREE_BYTES)
    if remote.size is None or offset != remote.size:
        headers = _headers(username, password)
        if offset:
            headers["Range"] = f"bytes={offset}-"
            headers["If-Range"] = validator
            print(f"Resuming at {_format_bytes(offset)}.", file=sys.stderr)
        elif remote.etag and not remote.etag.startswith("W/"):
            headers["If-Match"] = remote.etag
        elif remote.last_modified:
            headers["If-Unmodified-Since"] = format_datetime(
                remote.last_modified.astimezone(UTC), usegmt=True
            )
        request = Request(url, headers=headers, method="GET")
        try:
            with Progress("beacon-connect"):
                response = build_opener(SameOriginRedirectHandler()).open(request, timeout=timeout)
            with response:
                _download_response(response, archive_path, offset, remote, floor)
        except HTTPError as exc:
            if exc.code in (401, 403):
                raise RuntimeError("Beacon authentication failed") from None
            raise RuntimeError(f"Beacon download failed: HTTP {exc.code}") from None
        except (URLError, OSError, http.client.HTTPException):
            raise RuntimeError(
                "Beacon download interrupted; a verified partial can be resumed"
            ) from None
    print(
        "Validating gzip and SQL in memory; keeping only compressed bytes.",
        file=sys.stderr,
    )
    try:
        metadata = validate_cached_snapshot(archive_path)
    except RuntimeError:
        archive_path.unlink(missing_ok=True)
        partial_metadata_path.unlink(missing_ok=True)
        identity_path.unlink(missing_ok=True)
        raise
    if expected_version and metadata["dump_date"] != expected_version:
        raise RuntimeError("requested version differs from the SQL footer date")
    final_path = raw_dir / f"pkpbeacon-{metadata['dump_date']}.sql.gz"
    if final_path.exists():
        previous = validate_cached_snapshot(final_path)
        if previous["compressed_sha256"] != metadata["compressed_sha256"]:
            raise RuntimeError(
                f"immutable snapshot revision detected for {metadata['dump_date']}; existing bytes were preserved"
            )
        archive_path.unlink()
        metadata = previous
    else:
        legacy_path = raw_dir / f"pkpbeacon-{metadata['dump_date']}.sql"
        if legacy_path.exists():
            legacy_hash = hashlib.sha256()
            processed_bytes = 0
            with Progress("beacon-legacy-checksum", process_pid=os.getpid(), disk_path=raw_dir,
                          processed_bytes=0, total_bytes=legacy_path.stat().st_size) as progress, legacy_path.open("rb") as legacy:
                while chunk := legacy.read(BUFFER_SIZE):
                    legacy_hash.update(chunk)
                    processed_bytes += len(chunk)
                    progress.update(processed_bytes=processed_bytes)
                if legacy_hash.hexdigest() != metadata["uncompressed_sha256"]:
                    raise RuntimeError(
                        f"immutable legacy snapshot revision detected for {metadata['dump_date']}; existing bytes were preserved"
                    )
        # Hard-link publication fails atomically if another process created this name.
        try:
            os.link(archive_path, final_path)
        except FileExistsError:
            raise RuntimeError(
                "snapshot appeared during publication; retry without overwriting it"
            ) from None
        archive_path.unlink()
        final_path.chmod(0o444)
        if remote.last_modified:
            timestamp = remote.last_modified.timestamp()
            os.utime(final_path, (timestamp, timestamp))
        metadata["stat"] = _stat_binding(final_path)
    metadata.update({"source_url": url, "remote": _remote_identity(remote)})
    _write_json(_metadata_path(final_path), metadata)
    _write_json(
        raw_dir / STATE_NAME,
        {
            "schema_version": 1,
            "source_url": url,
            "remote": _remote_identity(remote),
            "snapshot": final_path.name,
            "compressed_sha256": metadata["compressed_sha256"],
            "checked_at": datetime.now(UTC).isoformat(),
        },
    )
    identity_path.unlink(missing_ok=True)
    partial_metadata_path.unlink(missing_ok=True)
    latest = _update_latest_snapshot(raw_dir)
    if (
        remote.last_modified
        and remote.last_modified.date().isoformat() != metadata["dump_date"]
    ):
        print(
            "Remote Last-Modified date differs from the SQL footer; recorded their mapping.",
            file=sys.stderr,
        )
    print(
        f"Saved {final_path} ({_format_bytes(metadata['compressed_size'])} compressed; {_format_bytes(metadata['uncompressed_size'])} SQL); {latest.name} -> {latest.readlink()}"
    )
    return final_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check or download an immutable compressed PKP Beacon MySQL snapshot."
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--raw-dir", type=Path, default=PROJECT_ROOT / "data" / "raw")
    parser.add_argument(
        "--username", help="override the credentials file or PKP_BEACON_USERNAME"
    )
    parser.add_argument(
        "--credentials-file",
        type=Path,
        default=Path(
            os.environ.get("OJS_BEACON_CREDENTIALS_FILE", str(DEFAULT_CREDENTIALS_FILE))
        ),
        help="private INI file (or OJS_BEACON_CREDENTIALS_FILE)",
    )
    parser.add_argument(
        "--prompt-password",
        action="store_true",
        help="read the password securely from the terminal",
    )
    parser.add_argument("--version", help="require this SQL footer date (YYYY-MM-DD)")
    parser.add_argument(
        "--check",
        action="store_true",
        help="HEAD only: show remote metadata and whether downloading is necessary",
    )
    parser.add_argument(
        "--json", action="store_true", help="emit machine-readable --check results"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="download again to verify bytes; never overwrite an immutable snapshot",
    )
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument(
        "--allow-insecure-localhost", action="store_true", help=argparse.SUPPRESS
    )
    return parser


def _execute(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    progress_interval()
    file_username, file_password = read_credentials(args.credentials_file)
    username = (
        args.username
        or os.environ.get("PKP_BEACON_USERNAME")
        or file_username
        or DEFAULT_USERNAME
    )
    password = os.environ.get("PKP_BEACON_PASSWORD") or file_password
    if args.prompt_password:
        password = getpass.getpass("PKP Beacon password: ")
    if not password:
        parser.error(
            "set PKP_BEACON_PASSWORD, provide a private credentials file, or pass --prompt-password"
        )
    remote = probe_remote(
        args.url,
        username,
        password,
        args.timeout,
        allow_insecure_localhost=args.allow_insecure_localhost,
    )
    version = snapshot_version(remote, args.version)
    if args.check:
        existing = known_remote_snapshot(args.raw_dir, args.url, remote)
        if (
            args.version
            and existing
            and SNAPSHOT_PATTERN.fullmatch(existing.name).group(1) != version
        ):
            raise RuntimeError("requested version differs from the SQL footer date")
        result = {
            "remote": _remote_identity(remote),
            "known_snapshot": str(existing) if existing else None,
            "needs_download": bool(args.force or existing is None),
            "date_hint": version,
        }
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print(f"Remote size: {_format_bytes(remote.size)}")
            print(
                f"Last modified: {result['remote']['last_modified'] or 'not provided'}"
            )
            print(f"ETag: {remote.etag or 'not provided'}")
            print(
                f"Known snapshot: {existing or 'none; SQL footer determines the date'}"
            )
            print(f"Download needed: {'yes' if result['needs_download'] else 'no'}")
        return 0
    download_snapshot(
        args.url,
        username,
        password,
        args.raw_dir,
        version,
        remote,
        args.timeout,
        args.force,
        allow_insecure_localhost=args.allow_insecure_localhost,
        expected_version=args.version,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.json and not args.check:
        parser.error("--json requires --check")
    try:
        # A read-only HEAD check must neither create a lock nor acquire one.
        with nullcontext() if args.check else source_activity(args.raw_dir):
            return _execute(args, parser)
    except SourceActivityBusy:
        print(json.dumps({"status": "skipped", "reason": "source_activity_busy"}, sort_keys=True))
        return 0
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
