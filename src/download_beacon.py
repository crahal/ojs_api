#!/usr/bin/env python3
"""Download a versioned PKP Beacon SQL snapshot."""

from __future__ import annotations

import argparse
import base64
import configparser
import getpass
import gzip
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

DEFAULT_URL = "https://beacon.publicknowledgeproject.org/mysql/pkpbeacon.gz"
DEFAULT_USERNAME = "beacon-research"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CREDENTIALS_FILE = Path.home() / ".config" / "ojs-api" / "beacon.ini"
BUFFER_SIZE = 8 * 1024 * 1024
USER_AGENT = "ojs-api-beacon-downloader/1.0"


@dataclass(frozen=True)
class RemoteFile:
    size: int | None
    last_modified: datetime | None
    etag: str | None


class SameOriginRedirectHandler(HTTPRedirectHandler):
    """Allow redirects without sending credentials to another origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urljoin(req.full_url, newurl)
        if _origin(req.full_url) != _origin(target):
            raise HTTPError(
                req.full_url,
                code,
                f"refusing authenticated redirect to {target}",
                headers,
                fp,
            )
        return super().redirect_request(req, fp, code, msg, headers, target)


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parsed = urlsplit(url)
    default_port = 443 if parsed.scheme.lower() == "https" else 80
    return parsed.scheme.lower(), parsed.hostname, parsed.port or default_port


def _headers(username: str, password: str) -> dict[str, str]:
    credentials = base64.b64encode(
        f"{username}:{password}".encode("utf-8")
    ).decode("ascii")
    return {
        "Authorization": f"Basic {credentials}",
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "identity",
    }


def read_credentials(path: Path) -> tuple[str | None, str | None]:
    if not path.exists():
        return None, None
    if path.stat().st_mode & 0o077:
        raise RuntimeError(
            f"credentials file permissions are too open: {path}; "
            "run chmod 600 on it"
        )

    config = configparser.ConfigParser(interpolation=None)
    try:
        with path.open(encoding="utf-8") as source:
            config.read_file(source)
    except (OSError, configparser.Error) as exc:
        raise RuntimeError(f"could not read credentials file {path}: {exc}") from exc

    if not config.has_section("beacon"):
        raise RuntimeError(f"credentials file has no [beacon] section: {path}")
    username = config.get("beacon", "username", fallback=None)
    password = config.get("beacon", "password", fallback=None)
    return username, password


def _integer_header(headers, name: str) -> int | None:
    value = headers.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _modified_header(headers) -> datetime | None:
    value = headers.get("Last-Modified")
    if not value:
        return None
    try:
        modified = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=UTC)
    return modified.astimezone(UTC)


def probe_remote(
    url: str,
    username: str,
    password: str,
    timeout: float,
) -> RemoteFile:
    opener = build_opener(SameOriginRedirectHandler())
    request = Request(
        url,
        headers=_headers(username, password),
        method="HEAD",
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            return RemoteFile(
                size=_integer_header(response.headers, "Content-Length"),
                last_modified=_modified_header(response.headers),
                etag=response.headers.get("ETag"),
            )
    except HTTPError as exc:
        if exc.code in (401, 403):
            raise RuntimeError("Beacon authentication failed") from exc
        raise RuntimeError(f"Beacon metadata request failed: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Beacon metadata request failed: {exc.reason}") from exc


def snapshot_version(remote: RemoteFile, override: str | None) -> str:
    if override:
        try:
            return datetime.strptime(override, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise ValueError("--version must use YYYY-MM-DD") from exc
    timestamp = remote.last_modified or datetime.now(UTC)
    return timestamp.date().isoformat()


def _format_bytes(value: int | None) -> str:
    if value is None:
        return "unknown"
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return str(value)


def _extract_sql_gzip(archive_path: Path, sql_path: Path) -> tuple[int, str]:
    total = 0
    prefix = b""
    tail = b""
    try:
        with gzip.open(archive_path, "rb") as source, sql_path.open("wb") as destination:
            while chunk := source.read(BUFFER_SIZE):
                if not prefix:
                    prefix = chunk[:4096]
                tail = (tail + chunk)[-4096:]
                destination.write(chunk)
                total += len(chunk)
            destination.flush()
            os.fsync(destination.fileno())
    except (EOFError, OSError) as exc:
        raise RuntimeError(f"downloaded gzip file failed validation: {exc}") from exc

    if b"MySQL dump" not in prefix:
        raise RuntimeError("downloaded file is gzip data but not a MySQL dump")
    match = re.search(rb"-- Dump completed on (\d{4}-\d{2}-\d{2})\s", tail)
    if not match:
        raise RuntimeError("MySQL dump has no extraction timestamp")
    return total, match.group(1).decode("ascii")


def _update_latest_snapshot(raw_dir: Path) -> Path:
    pattern = re.compile(r"pkpbeacon-\d{4}-\d{2}-\d{2}\.sql$")
    snapshots = [
        path
        for path in raw_dir.glob("pkpbeacon-*.sql")
        if path.is_file() and pattern.fullmatch(path.name)
    ]
    if not snapshots:
        raise RuntimeError(f"no versioned SQL snapshots found in {raw_dir}")

    newest = max(snapshots, key=lambda path: path.name)
    latest = raw_dir / "pkpbeacon-latest.sql"
    temporary = raw_dir / ".pkpbeacon-latest.sql.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(newest.name)
    os.replace(temporary, latest)
    return latest


def _download_response(
    response,
    part_path: Path,
    offset: int,
    expected_size: int | None,
) -> int:
    status = getattr(response, "status", response.getcode())
    if offset and status == 206:
        content_range = response.headers.get("Content-Range", "")
        if not re.match(rf"bytes {offset}-\d+/(?:\d+|\*)$", content_range):
            raise RuntimeError(f"unexpected Content-Range: {content_range or 'missing'}")
        mode = "ab"
    elif offset and status == 200:
        print("Server did not resume the partial file; restarting.", file=sys.stderr)
        offset = 0
        mode = "wb"
    elif status == 200:
        mode = "wb"
    else:
        raise RuntimeError(f"unexpected download response: HTTP {status}")

    if expected_size is None:
        response_size = _integer_header(response.headers, "Content-Length")
        if response_size is not None:
            expected_size = offset + response_size

    downloaded = offset
    last_report = 0.0
    with part_path.open(mode) as destination:
        while chunk := response.read(BUFFER_SIZE):
            destination.write(chunk)
            downloaded += len(chunk)
            now = time.monotonic()
            if now - last_report >= 5:
                if expected_size:
                    percent = downloaded / expected_size * 100
                    progress = (
                        f"{_format_bytes(downloaded)} / "
                        f"{_format_bytes(expected_size)} ({percent:.1f}%)"
                    )
                else:
                    progress = _format_bytes(downloaded)
                print(f"Downloaded {progress}", file=sys.stderr)
                last_report = now
        destination.flush()
        os.fsync(destination.fileno())
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
) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    provisional_path = raw_dir / f"pkpbeacon-{version}.sql"
    archive_path = raw_dir / f".pkpbeacon-{version}.sql.gz.part"
    sql_part_path = raw_dir / f".pkpbeacon-{version}.sql.part"

    if provisional_path.exists() and not force:
        provisional_path.chmod(0o444)
        _update_latest_snapshot(raw_dir)
        print(f"Snapshot already present: {provisional_path}")
        return provisional_path
    if force:
        archive_path.unlink(missing_ok=True)
        sql_part_path.unlink(missing_ok=True)

    offset = archive_path.stat().st_size if archive_path.exists() else 0
    if remote.size is not None and offset > remote.size:
        raise RuntimeError(
            f"partial file is larger than the remote file: {archive_path}; "
            "remove it or use --force"
        )

    if remote.size is None or offset < remote.size:
        headers = _headers(username, password)
        if offset:
            headers["Range"] = f"bytes={offset}-"
            print(f"Resuming at {_format_bytes(offset)}.", file=sys.stderr)
        request = Request(url, headers=headers, method="GET")
        opener = build_opener(SameOriginRedirectHandler())
        try:
            with opener.open(request, timeout=timeout) as response:
                downloaded = _download_response(
                    response,
                    archive_path,
                    offset,
                    remote.size,
                )
        except HTTPError as exc:
            if exc.code in (401, 403):
                raise RuntimeError("Beacon authentication failed") from exc
            raise RuntimeError(f"Beacon download failed: HTTP {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"Beacon download failed: {exc.reason}") from exc
    else:
        downloaded = offset

    if remote.size is not None and downloaded != remote.size:
        raise RuntimeError(
            f"incomplete download: got {downloaded} bytes, expected {remote.size}"
        )

    print("Extracting and validating the MySQL dump.", file=sys.stderr)
    sql_part_path.unlink(missing_ok=True)
    sql_size, dump_version = _extract_sql_gzip(archive_path, sql_part_path)
    final_path = raw_dir / f"pkpbeacon-{dump_version}.sql"
    if final_path.exists() and not force:
        sql_part_path.unlink()
        archive_path.unlink()
        _update_latest_snapshot(raw_dir)
        print(f"Snapshot already present: {final_path}")
        return final_path
    os.replace(sql_part_path, final_path)
    final_path.chmod(0o444)
    archive_path.unlink()
    if remote.last_modified:
        timestamp = remote.last_modified.timestamp()
        os.utime(final_path, (timestamp, timestamp))
    latest = _update_latest_snapshot(raw_dir)
    print(
        f"Saved {final_path} "
        f"({_format_bytes(sql_size)} SQL); "
        f"{latest.name} -> {final_path.name}"
    )
    return final_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download a versioned PKP Beacon MySQL dump into data/raw."
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
    )
    parser.add_argument(
        "--username",
        help="override the credentials file or PKP_BEACON_USERNAME",
    )
    parser.add_argument(
        "--credentials-file",
        type=Path,
        default=DEFAULT_CREDENTIALS_FILE,
        help=f"local INI file (default: {DEFAULT_CREDENTIALS_FILE})",
    )
    parser.add_argument(
        "--prompt-password",
        action="store_true",
        help="read the password securely from the terminal",
    )
    parser.add_argument(
        "--version",
        help="override the snapshot date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="show remote metadata and destination without downloading",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing snapshot for the same version",
    )
    parser.add_argument("--timeout", type=float, default=60)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        file_username, file_password = read_credentials(args.credentials_file)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

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
            "set PKP_BEACON_PASSWORD or pass --prompt-password; "
            "the password is never accepted as a command-line argument"
        )

    try:
        remote = probe_remote(args.url, username, password, args.timeout)
        version = snapshot_version(remote, args.version)
        destination = args.raw_dir / f"pkpbeacon-{version}.sql"
        if args.check:
            print(f"Remote size: {_format_bytes(remote.size)}")
            print(
                "Last modified: "
                + (
                    remote.last_modified.isoformat()
                    if remote.last_modified
                    else "not provided"
                )
            )
            print(f"ETag: {remote.etag or 'not provided'}")
            print(f"Destination: {destination}")
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
        )
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
