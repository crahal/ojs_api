from __future__ import annotations

import contextlib
import gzip
import io
import json
import os
import sys
import threading
import unittest
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import scrape_updates


def sql_dump(version: str) -> bytes:
    return (
        b"-- MySQL dump 10.13\n"
        b"CREATE TABLE example (id INTEGER);\n"
        + f"-- Dump completed on {version} 12:00:00\n".encode("ascii")
    )


@contextlib.contextmanager
def busy_source(raw_dir):
    ready, release = threading.Event(), threading.Event()
    errors = []

    def hold():
        try:
            with scrape_updates.source_activity(raw_dir):
                ready.set()
                release.wait(10)
        except BaseException as exc:
            errors.append(exc)
            ready.set()

    worker = threading.Thread(target=hold)
    worker.start()
    try:
        if not ready.wait(2):
            raise AssertionError("source lock holder did not start")
        if errors:
            raise errors[0]
        yield
    finally:
        release.set()
        worker.join(2)


class FakeResponse(io.BytesIO):
    def __init__(self, payload: bytes, status: int = 200) -> None:
        super().__init__(payload)
        self.status = status
        self.headers = Message()
        self.headers["Content-Length"] = str(len(payload))

    def getcode(self) -> int:
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


class FakeOpener:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(self.payload)


class ScrapeUpdatesTest(unittest.TestCase):
    def scrape(self, raw_dir, *, check_only=False):
        return scrape_updates.scrape(
            index_url="https://example.com/ojs-data/", raw_dir=raw_dir,
            username=None, password=None, timeout=10, allow_cross_origin=False,
            check_only=check_only, max_expanded_bytes=1024**2)

    def test_busy_scrape_skips_before_fetching_index(self):
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            with busy_source(raw), patch.object(scrape_updates, "fetch_index") as fetch:
                with self.assertRaises(scrape_updates.SourceActivityBusy):
                    self.scrape(raw)
                fetch.assert_not_called()

    def test_busy_cli_emits_clear_skipped_json(self):
        output = io.StringIO()
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            with busy_source(raw), patch.object(scrape_updates, "fetch_index") as fetch, \
                    contextlib.redirect_stdout(output):
                code = scrape_updates.main(["--raw-dir", str(raw)])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue()),
                             {"status": "skipped", "reason": "source_activity_busy"})
            fetch.assert_not_called()

    def test_busy_public_download_does_not_connect_or_change_files(self):
        snapshot = scrape_updates.RemoteSnapshot(
            "2026-07-01", "https://example.com/pkpbeacon-2026-07-01.sql.gz", True)
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            with busy_source(raw), patch.object(scrape_updates, "build_opener") as opener:
                before = set(raw.iterdir())
                with self.assertRaises(scrape_updates.SourceActivityBusy):
                    scrape_updates.download_snapshot(
                        snapshot, raw, index_url="https://example.com/", username=None,
                        password=None, timeout=10, allow_cross_origin=False)
                self.assertEqual(set(raw.iterdir()), before)
                opener.assert_not_called()

    def test_read_only_check_does_not_acquire_lock_or_create_raw_directory(self):
        html = '<a href="pkpbeacon-2026-07-01.sql.gz">snapshot</a>'
        with TemporaryDirectory() as directory:
            raw = Path(directory) / "not-created"
            with patch.object(scrape_updates, "source_activity",
                              side_effect=AssertionError("read-only checks must not lock")), \
                    patch.object(scrape_updates, "fetch_index", return_value=html):
                result = self.scrape(raw, check_only=True)
            self.assertEqual(result.missing, ("pkpbeacon-2026-07-01.sql",))
            self.assertFalse(raw.exists())

    def test_other_raw_directory_is_independent_and_nested_download_works(self):
        html = '<a href="pkpbeacon-2026-07-01.sql.gz">snapshot</a>'
        opener = FakeOpener(gzip.compress(sql_dump("2026-07-01")))
        with TemporaryDirectory() as directory:
            raw = Path(directory) / "selected"
            with busy_source(Path(directory) / "occupied"), \
                    patch.object(scrape_updates, "fetch_index", return_value=html), \
                    patch.object(scrape_updates, "build_opener", return_value=opener):
                result = self.scrape(raw)
            self.assertEqual(result.downloaded, ("pkpbeacon-2026-07-01.sql",))
            self.assertTrue((raw / "pkpbeacon-latest.sql").is_symlink())

    def test_invalid_inherited_capability_is_an_error_not_a_skipped_scrape(self):
        output, errors = io.StringIO(), io.StringIO()
        with TemporaryDirectory() as directory, \
                patch.dict(os.environ, {"OJS_SOURCE_ACTIVITY_FD": "-1"}), \
                patch.object(scrape_updates, "fetch_index") as fetch, \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = scrape_updates.main(["--raw-dir", directory])
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("invalid inherited source activity descriptor", errors.getvalue())
        fetch.assert_not_called()

    def test_index_parser_is_deterministic_and_prefers_gzip(self):
        html = """
        <a href="pkpbeacon-2026-07-01.sql">plain</a>
        <a href="pkpbeacon-2026-07-01.sql.gz">compressed</a>
        <a href="pkpbeacon-2026-01-01.sql.gz?download=1">older</a>
        <a href="pkpbeacon-2026-02-31.sql.gz">invalid date</a>
        <a href="https://other.example/pkpbeacon-2026-08-01.sql.gz">other</a>
        """
        snapshots = scrape_updates.parse_snapshot_links(
            html,
            "https://example.com/ojs-data/",
        )
        self.assertEqual(
            [(item.version, item.compressed) for item in snapshots],
            [("2026-01-01", True), ("2026-07-01", True)],
        )
        self.assertTrue(snapshots[0].url.endswith("?download=1"))

    def test_cross_origin_links_require_explicit_opt_in(self):
        html = (
            '<a href="https://cdn.example/pkpbeacon-2026-07-01.sql.gz">x</a>'
        )
        self.assertEqual(
            scrape_updates.parse_snapshot_links(
                html,
                "https://example.com/ojs-data/",
            ),
            [],
        )
        snapshots = scrape_updates.parse_snapshot_links(
            html,
            "https://example.com/ojs-data/",
            allow_cross_origin=True,
        )
        self.assertEqual(len(snapshots), 1)

    def test_compressed_snapshot_is_validated_and_installed_atomically(self):
        version = "2026-07-01"
        remote = scrape_updates.RemoteSnapshot(
            version=version,
            url=f"https://example.com/ojs-data/pkpbeacon-{version}.sql.gz",
            compressed=True,
        )
        opener = FakeOpener(gzip.compress(sql_dump(version)))
        with TemporaryDirectory() as directory:
            raw_dir = Path(directory)
            with patch.object(scrape_updates, "build_opener", return_value=opener):
                installed = scrape_updates.download_snapshot(
                    remote,
                    raw_dir,
                    index_url="https://example.com/ojs-data/",
                    username=None,
                    password=None,
                    timeout=10,
                    allow_cross_origin=False,
                )
            target = raw_dir / f"pkpbeacon-{version}.sql"
            self.assertTrue(installed)
            self.assertEqual(target.read_bytes(), sql_dump(version))
            self.assertEqual(target.stat().st_mode & 0o777, 0o444)
            self.assertFalse(any(raw_dir.glob("*.part")))
            self.assertFalse(
                scrape_updates.download_snapshot(
                    remote,
                    raw_dir,
                    index_url="https://example.com/ojs-data/",
                    username=None,
                    password=None,
                    timeout=10,
                    allow_cross_origin=False,
                )
            )

    def test_footer_date_must_match_link_date(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.sql"
            path.write_bytes(sql_dump("2026-06-01"))
            with self.assertRaisesRegex(
                scrape_updates.ScrapeError,
                "does not match",
            ):
                scrape_updates.validate_sql_dump(path, "2026-07-01")

    def test_latest_pointer_advances_to_newest_local_snapshot(self):
        with TemporaryDirectory() as directory:
            raw_dir = Path(directory)
            for version in ("2026-01-01", "2026-07-01"):
                (raw_dir / f"pkpbeacon-{version}.sql").write_bytes(
                    sql_dump(version)
                )
            newest = scrape_updates.publish_latest_pointer(raw_dir)
            latest = raw_dir / "pkpbeacon-latest.sql"
            self.assertEqual(newest.name, "pkpbeacon-2026-07-01.sql")
            self.assertTrue(latest.is_symlink())
            self.assertEqual(latest.resolve(), newest)

    def test_invalid_expansion_limit_environment_is_an_argparse_error(self):
        with (
            patch.dict(os.environ, {"OJS_SOURCE_MAX_EXPANDED_GB": "not-a-number"}),
            patch("sys.stderr", io.StringIO()),
        ):
            with self.assertRaises(SystemExit) as raised:
                scrape_updates.parser().parse_args([])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
