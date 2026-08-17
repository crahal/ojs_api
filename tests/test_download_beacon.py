from __future__ import annotations

import base64
import gzip
import sys
import threading
import unittest
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import download_beacon


class BeaconHandler(BaseHTTPRequestHandler):
    username = "test-user"
    password = "test-password"
    sql = (
        b"-- MySQL dump 10.13\n"
        b"CREATE TABLE example (id INTEGER);\n"
        b"-- Dump completed on 2026-07-01 12:00:00\n"
    )
    payload = gzip.compress(sql)

    def _authenticated(self):
        token = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()
        return self.headers.get("Authorization") == f"Basic {token}"

    def _headers(self, status, length, content_range=None):
        self.send_response(status)
        self.send_header("Content-Length", str(length))
        self.send_header("Last-Modified", "Wed, 01 Jul 2026 12:00:00 GMT")
        if content_range:
            self.send_header("Content-Range", content_range)
        self.end_headers()

    def do_HEAD(self):
        if not self._authenticated():
            self._headers(401, 0)
            return
        self._headers(200, len(self.payload))

    def do_GET(self):
        if not self._authenticated():
            self._headers(401, 0)
            return
        start = 0
        if self.headers.get("Range"):
            start = int(self.headers["Range"].removeprefix("bytes=").removesuffix("-"))
            body = self.payload[start:]
            self._headers(
                206,
                len(body),
                f"bytes {start}-{len(self.payload) - 1}/{len(self.payload)}",
            )
        else:
            body = self.payload
            self._headers(200, len(body))
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


class DownloadBeaconTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), BeaconHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}/pkpbeacon.gz"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_probe_and_resumed_download(self):
        remote = download_beacon.probe_remote(
            self.url,
            BeaconHandler.username,
            BeaconHandler.password,
            5,
        )
        self.assertEqual(remote.size, len(BeaconHandler.payload))
        self.assertEqual(
            remote.last_modified,
            datetime(2026, 7, 1, 12, tzinfo=UTC),
        )
        self.assertEqual(download_beacon.snapshot_version(remote, None), "2026-07-01")

        with TemporaryDirectory() as directory:
            raw_dir = Path(directory)
            part = raw_dir / ".pkpbeacon-2026-07-01.sql.gz.part"
            part.write_bytes(BeaconHandler.payload[:10])
            result = download_beacon.download_snapshot(
                self.url,
                BeaconHandler.username,
                BeaconHandler.password,
                raw_dir,
                "2026-07-01",
                remote,
                5,
            )
            self.assertEqual(result.read_bytes(), BeaconHandler.sql)
            self.assertFalse(part.exists())
            latest = raw_dir / "pkpbeacon-latest.sql"
            self.assertTrue(latest.is_symlink())
            self.assertEqual(latest.resolve(), result)

    def test_bad_password_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "authentication failed"):
            download_beacon.probe_remote(
                self.url,
                BeaconHandler.username,
                "wrong",
                5,
            )

    def test_credentials_file_requires_private_permissions(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "beacon.ini"
            path.write_text(
                "[beacon]\nusername = test-user\npassword = value#with-hash\n",
                encoding="utf-8",
            )
            path.chmod(0o600)
            self.assertEqual(
                download_beacon.read_credentials(path),
                ("test-user", "value#with-hash"),
            )

            path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "permissions are too open"):
                download_beacon.read_credentials(path)


if __name__ == "__main__":
    unittest.main()
