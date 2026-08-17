#!/usr/bin/env python3
"""Discover and atomically download dated SQL snapshots from an HTML index."""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INDEX_URL = "https://example.com/ojs-data/"
DEFAULT_MAX_EXPANDED_GB = 500
USER_AGENT = "ojs-api-snapshot-scraper/1.0"
BUFFER_SIZE = 8 * 1024 * 1024
MAX_INDEX_BYTES = 10 * 1024 * 1024
SNAPSHOT_NAME_RE = re.compile(
    r"pkpbeacon-(?P<version>\d{4}-\d{2}-\d{2})\.sql(?P<gzip>\.gz)?$"
)
DUMP_FOOTER_RE = re.compile(
    rb"-- Dump completed on (\d{4}-\d{2}-\d{2})\s+"
    rb"(\d{1,2}:\d{2}:\d{2})"
)


class ScrapeError(RuntimeError):
    pass


@dataclass(frozen=True)
class RemoteSnapshot:
    version: str
    url: str
    compressed: bool

    @property
    def filename(self) -> str:
        suffix = ".sql.gz" if self.compressed else ".sql"
        return f"pkpbeacon-{self.version}{suffix}"


@dataclass(frozen=True)
class ScrapeResult:
    discovered: tuple[str, ...]
    downloaded: tuple[str, ...]
    already_present: tuple[str, ...]
    missing: tuple[str, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.downloaded)


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.links.append(value)
                return


def origin(url: str) -> tuple[str, str | None, int | None]:
    parsed = urlsplit(url)
    default_port = 443 if parsed.scheme.lower() == "https" else 80
    return parsed.scheme.lower(), parsed.hostname, parsed.port or default_port


def validate_http_url(url: str, label: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ScrapeError(f"{label} must be an absolute HTTP(S) URL: {url!r}")
    if parsed.username is not None or parsed.password is not None:
        raise ScrapeError(f"{label} must not contain credentials")


class SafeRedirectHandler(HTTPRedirectHandler):
    """Prevent credential leaks and unexpected cross-origin redirects."""

    def __init__(self, *, allow_cross_origin: bool) -> None:
        super().__init__()
        self.allow_cross_origin = allow_cross_origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urljoin(req.full_url, newurl)
        validate_http_url(target, "redirect target")
        changed_origin = origin(req.full_url) != origin(target)
        if changed_origin and req.has_header("Authorization"):
            raise HTTPError(
                req.full_url,
                code,
                "refusing to send source credentials across origins",
                headers,
                fp,
            )
        if changed_origin and not self.allow_cross_origin:
            raise HTTPError(
                req.full_url,
                code,
                f"refusing cross-origin redirect to {target}",
                headers,
                fp,
            )
        return super().redirect_request(req, fp, code, msg, headers, target)


def request_headers(
    url: str,
    *,
    authenticated_origin: tuple[str, str | None, int | None],
    username: str | None,
    password: str | None,
) -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "identity",
    }
    if username and password and origin(url) == authenticated_origin:
        token = base64.b64encode(
            f"{username}:{password}".encode("utf-8")
        ).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
    return headers


def parse_snapshot_links(
    html: str,
    index_url: str,
    *,
    allow_cross_origin: bool = False,
) -> list[RemoteSnapshot]:
    """Return one deterministic candidate per dated snapshot in an index."""
    validate_http_url(index_url, "index URL")
    parser = LinkParser()
    parser.feed(html)
    index_origin = origin(index_url)
    by_version: dict[str, RemoteSnapshot] = {}

    for href in parser.links:
        candidate_url = urljoin(index_url, href)
        try:
            validate_http_url(candidate_url, "snapshot link")
        except ScrapeError:
            continue
        if not allow_cross_origin and origin(candidate_url) != index_origin:
            continue
        filename = Path(unquote(urlsplit(candidate_url).path)).name
        match = SNAPSHOT_NAME_RE.fullmatch(filename)
        if match is None:
            continue
        version = match.group("version")
        try:
            date.fromisoformat(version)
        except ValueError:
            continue
        candidate = RemoteSnapshot(
            version=version,
            url=candidate_url,
            compressed=match.group("gzip") is not None,
        )
        current = by_version.get(version)
        if current is None:
            by_version[version] = candidate
        elif current.url == candidate.url:
            continue
        elif candidate.compressed and not current.compressed:
            by_version[version] = candidate
        elif current.compressed and not candidate.compressed:
            continue
        else:
            raise ScrapeError(
                "source index has multiple different files for snapshot "
                f"{version}: {current.filename}"
            )

    return [by_version[key] for key in sorted(by_version)]


def fetch_index(
    index_url: str,
    *,
    username: str | None,
    password: str | None,
    timeout: float,
) -> str:
    validate_http_url(index_url, "index URL")
    request = Request(
        index_url,
        headers=request_headers(
            index_url,
            authenticated_origin=origin(index_url),
            username=username,
            password=password,
        ),
        method="GET",
    )
    opener = build_opener(SafeRedirectHandler(allow_cross_origin=False))
    try:
        with opener.open(request, timeout=timeout) as response:
            payload = response.read(MAX_INDEX_BYTES + 1)
            if len(payload) > MAX_INDEX_BYTES:
                raise ScrapeError(
                    f"source index is larger than {MAX_INDEX_BYTES} bytes"
                )
            charset = response.headers.get_content_charset() or "utf-8"
    except HTTPError as exc:
        if exc.code in (401, 403):
            raise ScrapeError("source index authentication failed") from exc
        raise ScrapeError(
            f"source index request failed: HTTP {exc.code}"
        ) from exc
    except URLError as exc:
        raise ScrapeError(f"source index request failed: {exc.reason}") from exc
    try:
        return payload.decode(charset)
    except (LookupError, UnicodeDecodeError) as exc:
        raise ScrapeError(
            f"source index is not valid text in charset {charset!r}"
        ) from exc


def validate_sql_dump(path: Path, expected_version: str) -> None:
    size = path.stat().st_size
    if size <= 0:
        raise ScrapeError(f"downloaded snapshot is empty: {path.name}")
    with path.open("rb") as source:
        prefix = source.read(4096)
        source.seek(max(0, size - 8192))
        tail = source.read()
    if b"MySQL dump" not in prefix:
        raise ScrapeError(f"download is not a MySQL dump: {path.name}")
    match = DUMP_FOOTER_RE.search(tail)
    if match is None:
        raise ScrapeError(f"MySQL dump has no completion timestamp: {path.name}")
    dump_version = match.group(1).decode("ascii")
    if dump_version != expected_version:
        raise ScrapeError(
            f"snapshot filename date {expected_version} does not match "
            f"dump completion date {dump_version}"
        )


def _stream_response(response, part_path: Path, offset: int) -> None:
    status = getattr(response, "status", response.getcode())
    content_length = response.headers.get("Content-Length")
    expected_response_size = int(content_length) if content_length else None
    if offset and status == 206:
        content_range = response.headers.get("Content-Range", "")
        if not re.fullmatch(rf"bytes {offset}-\d+/(?:\d+|\*)", content_range):
            raise ScrapeError(
                f"unexpected Content-Range: {content_range or 'missing'}"
            )
        mode = "ab"
    elif status == 200:
        offset = 0
        mode = "wb"
    else:
        raise ScrapeError(f"unexpected snapshot response: HTTP {status}")

    received = 0
    last_report = 0.0
    with part_path.open(mode) as destination:
        while chunk := response.read(BUFFER_SIZE):
            destination.write(chunk)
            received += len(chunk)
            now = time.monotonic()
            if now - last_report >= 30:
                print(
                    f"[scrape] {part_path.name}: {offset + received} bytes",
                    file=sys.stderr,
                )
                last_report = now
        destination.flush()
        os.fsync(destination.fileno())
    if expected_response_size is not None and received != expected_response_size:
        raise ScrapeError(
            f"incomplete response: got {received} bytes, "
            f"expected {expected_response_size}"
        )


def download_snapshot(
    snapshot: RemoteSnapshot,
    raw_dir: Path,
    *,
    index_url: str,
    username: str | None,
    password: str | None,
    timeout: float,
    allow_cross_origin: bool,
    max_expanded_bytes: int = DEFAULT_MAX_EXPANDED_GB * 1024**3,
) -> bool:
    """Download one immutable snapshot; return True only when newly installed."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    destination = raw_dir / f"pkpbeacon-{snapshot.version}.sql"
    if destination.exists():
        validate_sql_dump(destination, snapshot.version)
        destination.chmod(0o444)
        return False

    download_part = raw_dir / f".{snapshot.filename}.download.part"
    sql_part = raw_dir / f".pkpbeacon-{snapshot.version}.sql.part"
    offset = download_part.stat().st_size if download_part.exists() else 0
    headers = request_headers(
        snapshot.url,
        authenticated_origin=origin(index_url),
        username=username,
        password=password,
    )
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = Request(snapshot.url, headers=headers, method="GET")
    opener = build_opener(
        SafeRedirectHandler(allow_cross_origin=allow_cross_origin)
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            _stream_response(response, download_part, offset)
    except HTTPError as exc:
        if exc.code in (401, 403):
            raise ScrapeError("snapshot authentication failed") from exc
        raise ScrapeError(
            f"snapshot download failed: HTTP {exc.code}"
        ) from exc
    except URLError as exc:
        raise ScrapeError(f"snapshot download failed: {exc.reason}") from exc

    sql_part.unlink(missing_ok=True)
    try:
        if snapshot.compressed:
            expanded = 0
            with gzip.open(download_part, "rb") as source, sql_part.open(
                "wb"
            ) as output:
                while chunk := source.read(BUFFER_SIZE):
                    expanded += len(chunk)
                    if expanded > max_expanded_bytes:
                        raise ScrapeError(
                            "expanded snapshot exceeds configured size limit"
                        )
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
        else:
            if download_part.stat().st_size > max_expanded_bytes:
                raise ScrapeError(
                    "snapshot exceeds configured expanded-size limit"
                )
            os.replace(download_part, sql_part)
        validate_sql_dump(sql_part, snapshot.version)
        sql_part.chmod(0o444)
        os.replace(sql_part, destination)
    except (EOFError, OSError, ScrapeError) as exc:
        sql_part.unlink(missing_ok=True)
        download_part.unlink(missing_ok=True)
        if isinstance(exc, ScrapeError):
            raise
        raise ScrapeError(
            f"could not extract or install {snapshot.filename}: {exc}"
        ) from exc
    download_part.unlink(missing_ok=True)
    return True


def publish_latest_pointer(raw_dir: Path) -> Path:
    snapshots = [
        path
        for path in raw_dir.glob("pkpbeacon-????-??-??.sql")
        if SNAPSHOT_NAME_RE.fullmatch(path.name) and path.is_file()
    ]
    if not snapshots:
        raise ScrapeError(f"no dated SQL snapshots exist in {raw_dir}")
    newest = max(snapshots, key=lambda path: path.name)
    latest = raw_dir / "pkpbeacon-latest.sql"
    if (latest.exists() or latest.is_symlink()) and not latest.is_symlink():
        raise ScrapeError(f"latest snapshot pointer is not a symlink: {latest}")
    temporary = raw_dir / ".pkpbeacon-latest.sql.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(newest.name)
    os.replace(temporary, latest)
    return newest


def scrape(
    *,
    index_url: str,
    raw_dir: Path,
    username: str | None,
    password: str | None,
    timeout: float,
    allow_cross_origin: bool,
    check_only: bool,
    max_expanded_bytes: int,
) -> ScrapeResult:
    if bool(username) != bool(password):
        raise ScrapeError(
            "OJS_SOURCE_USERNAME and OJS_SOURCE_PASSWORD must be set together"
        )
    if username and urlsplit(index_url).scheme.lower() != "https":
        raise ScrapeError("authenticated source indexes must use HTTPS")
    html = fetch_index(
        index_url,
        username=username,
        password=password,
        timeout=timeout,
    )
    candidates = parse_snapshot_links(
        html,
        index_url,
        allow_cross_origin=allow_cross_origin,
    )
    if not candidates:
        raise ScrapeError(
            "source index contains no pkpbeacon-YYYY-MM-DD.sql[.gz] links"
        )

    downloaded: list[str] = []
    already_present: list[str] = []
    missing: list[str] = []
    for candidate in candidates:
        destination_name = f"pkpbeacon-{candidate.version}.sql"
        if check_only:
            target = raw_dir / destination_name
            (already_present if target.exists() else missing).append(
                destination_name
            )
            continue
        installed = download_snapshot(
            candidate,
            raw_dir,
            index_url=index_url,
            username=username,
            password=password,
            timeout=timeout,
            allow_cross_origin=allow_cross_origin,
            max_expanded_bytes=max_expanded_bytes,
        )
        (downloaded if installed else already_present).append(destination_name)

    if not check_only:
        publish_latest_pointer(raw_dir)
    return ScrapeResult(
        discovered=tuple(
            f"pkpbeacon-{candidate.version}.sql" for candidate in candidates
        ),
        downloaded=tuple(downloaded),
        already_present=tuple(already_present),
        missing=tuple(missing),
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Scrape an HTML index for dated PKP Beacon SQL snapshots and "
            "install every missing file atomically."
        )
    )
    result.add_argument(
        "--index-url",
        default=os.environ.get("OJS_SOURCE_INDEX_URL", DEFAULT_INDEX_URL),
        help=(
            "HTML page containing snapshot links (default is a placeholder; "
            "set OJS_SOURCE_INDEX_URL in production)"
        ),
    )
    result.add_argument(
        "--raw-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
    )
    result.add_argument("--timeout", type=float, default=60)
    result.add_argument(
        "--allow-cross-origin",
        action="store_true",
        help="allow unauthenticated snapshot links on another HTTP(S) origin",
    )
    result.add_argument(
        "--check",
        action="store_true",
        help="report missing files without downloading them",
    )
    result.add_argument(
        "--json",
        action="store_true",
        help="emit a machine-readable summary",
    )
    result.add_argument(
        "--max-expanded-gb",
        type=int,
        default=os.environ.get(
            "OJS_SOURCE_MAX_EXPANDED_GB",
            str(DEFAULT_MAX_EXPANDED_GB),
        ),
        help="reject a snapshot that expands beyond this many GiB",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.timeout <= 0 or args.max_expanded_gb <= 0:
        print(
            "error: --timeout and --max-expanded-gb must be positive",
            file=sys.stderr,
        )
        return 2
    try:
        result = scrape(
            index_url=args.index_url,
            raw_dir=args.raw_dir.expanduser().resolve(),
            username=os.environ.get("OJS_SOURCE_USERNAME"),
            password=os.environ.get("OJS_SOURCE_PASSWORD"),
            timeout=args.timeout,
            allow_cross_origin=args.allow_cross_origin,
            check_only=args.check,
            max_expanded_bytes=args.max_expanded_gb * 1024**3,
        )
    except (OSError, ScrapeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(
            json.dumps(
                {
                    "changed": result.changed,
                    "discovered": result.discovered,
                    "downloaded": result.downloaded,
                    "already_present": result.already_present,
                    "missing": result.missing,
                },
                sort_keys=True,
            )
        )
    elif args.check and result.missing:
        print(
            f"[scrape] {len(result.missing)} snapshot(s) would be downloaded: "
            + ", ".join(result.missing)
        )
    elif result.downloaded:
        print(
            "[scrape] installed "
            f"{len(result.downloaded)} new snapshot(s): "
            + ", ".join(result.downloaded)
        )
    else:
        print("[scrape] no new snapshot files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
