from __future__ import annotations

import gzip
import io
import os
import sys
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
