from __future__ import annotations

import base64
import contextlib
import gzip
import hashlib
import io
import json
import os
import sys
import threading
import unittest
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import download_beacon


def progress_events(output):
    return [json.loads(line.removeprefix("[progress] "))
            for line in output.splitlines() if line.startswith("[progress] ")]


class HeartbeatOutput(io.StringIO):
    def __init__(self):
        super().__init__()
        self.heartbeat = threading.Event()

    def write(self, value):
        result = super().write(value)
        if '"event": "heartbeat"' in value:
            self.heartbeat.set()
        return result


class BeaconHandler(BaseHTTPRequestHandler):
    username = "test-user"
    password = "test-password"
    sql = b"-- MySQL dump 10.13\nCREATE TABLE example (id INTEGER);\n-- Dump completed on 2026-07-01 12:00:00\n"

    def _authenticated(self):
        token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return self.headers.get("Authorization") == f"Basic {token}"

    def _headers(self, status, length, content_range=None):
        self.send_response(status)
        self.send_header("Content-Length", str(length))
        if self.server.modified:
            self.send_header("Last-Modified", self.server.modified)
        if self.server.etag:
            self.send_header("ETag", self.server.etag)
        if content_range:
            self.send_header("Content-Range", content_range)
        self.end_headers()

    def do_HEAD(self):
        self.server.requests.append(("HEAD", dict(self.headers)))
        if not self._authenticated():
            self._headers(401, 0)
            return
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header(
                "Location", f"http://localhost:{self.server.server_port}/pkpbeacon.gz"
            )
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/same-origin-redirect":
            self.send_response(302)
            self.send_header("Location", "/pkpbeacon.gz")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._headers(200, len(self.server.payload))

    def do_GET(self):
        self.server.requests.append(("GET", dict(self.headers)))
        if not self._authenticated():
            self._headers(401, 0)
            return
        start = 0
        requested_range = self.headers.get("Range")
        if (
            requested_range
            and self.server.resume
            and self.headers.get("If-Range") in {self.server.etag, self.server.modified}
        ):
            start = int(requested_range.removeprefix("bytes=").removesuffix("-"))
            body = self.server.payload[start:]
            self._headers(
                206,
                len(body),
                self.server.content_range
                or f"bytes {start}-{len(self.server.payload) - 1}/{len(self.server.payload)}",
            )
        else:
            body = self.server.payload
            self._headers(200, len(body))
        if self.server.interrupt_bytes is not None:
            body = body[: self.server.interrupt_bytes]
            self.server.interrupt_bytes = None
            self.close_connection = True
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

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

    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.raw_dir = Path(self.directory.name)
        self.server.payload = gzip.compress(BeaconHandler.sql, mtime=0)
        self.server.etag = '"snapshot-one"'
        # Deliberately different from the SQL footer date.
        self.server.modified = "Fri, 03 Jul 2026 08:00:00 GMT"
        self.server.resume = True
        self.server.interrupt_bytes = None
        self.server.content_range = None
        self.server.requests = []

    def probe(self):
        return download_beacon.probe_remote(
            self.url,
            BeaconHandler.username,
            BeaconHandler.password,
            5,
            allow_insecure_localhost=True,
        )

    def download(self, remote=None, **kwargs):
        remote = remote or self.probe()
        return download_beacon.download_snapshot(
            self.url,
            BeaconHandler.username,
            BeaconHandler.password,
            self.raw_dir,
            download_beacon.snapshot_version(remote, None),
            remote,
            5,
            allow_insecure_localhost=True,
            **kwargs,
        )

    def get_requests(self):
        return [headers for method, headers in self.server.requests if method == "GET"]

    def write_partial(self, remote, data, *, identity=True):
        part = self.raw_dir / download_beacon.PART_NAME
        part.write_bytes(data)
        if identity:
            (self.raw_dir / (download_beacon.PART_NAME + ".identity.json")).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "source_url": self.url,
                        "remote": download_beacon._remote_identity(remote),
                    }
                )
            )
        return part

    def test_compressed_only_snapshot_hashes_footer_and_latest(self):
        remote = self.probe()
        self.assertEqual(remote.size, len(self.server.payload))
        self.assertEqual(remote.last_modified, datetime(2026, 7, 3, 8, tzinfo=UTC))
        result = self.download(remote)
        self.assertEqual(result.name, "pkpbeacon-2026-07-01.sql.gz")
        self.assertEqual(result.read_bytes(), self.server.payload)
        self.assertEqual(result.stat().st_mode & 0o777, 0o444)
        self.assertFalse(list(self.raw_dir.glob("*.sql")))
        self.assertFalse(list(self.raw_dir.glob("*.sql.part")))
        metadata = download_beacon.read_validated_metadata(result)
        self.assertEqual(metadata["dump_datetime"], "2026-07-01 12:00:00")
        self.assertEqual(
            metadata["compressed_sha256"],
            hashlib.sha256(self.server.payload).hexdigest(),
        )
        self.assertEqual(
            metadata["uncompressed_sha256"],
            hashlib.sha256(BeaconHandler.sql).hexdigest(),
        )
        self.assertEqual(metadata["uncompressed_size"], len(BeaconHandler.sql))
        self.assertEqual((self.raw_dir / "pkpbeacon-latest.sql.gz").resolve(), result)
        self.assertFalse((self.raw_dir / download_beacon.PART_NAME).exists())

    def test_daily_head_is_noop_when_modified_date_differs_from_footer(self):
        result = self.download()
        with patch.object(
            download_beacon,
            "validate_archive",
            side_effect=AssertionError("must not reinflate"),
        ):
            self.assertEqual(self.download(), result)
        self.assertEqual(
            [method for method, _ in self.server.requests], ["HEAD", "GET", "HEAD"]
        )

    def test_json_check_uses_credentials_env_path_and_only_head(self):
        result = self.download()
        credentials = self.raw_dir / "test-beacon.ini"
        credentials.write_text(
            f"[beacon]\nusername={BeaconHandler.username}\npassword={BeaconHandler.password}\n"
        )
        credentials.chmod(0o600)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.dict(
                os.environ,
                {"OJS_BEACON_CREDENTIALS_FILE": str(credentials)},
                clear=True,
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            code = download_beacon.main(
                [
                    "--url",
                    self.url,
                    "--raw-dir",
                    str(self.raw_dir),
                    "--check",
                    "--json",
                    "--allow-insecure-localhost",
                ]
            )
        self.assertEqual(code, 0)
        value = json.loads(stdout.getvalue())
        self.assertFalse(value["needs_download"])
        self.assertEqual(value["known_snapshot"], str(result))
        self.assertEqual(value["date_hint"], "2026-07-03")
        self.assertEqual(len(self.get_requests()), 1)
        events = progress_events(stderr.getvalue())
        self.assertEqual([event["event"] for event in events], ["started", "completed"])
        self.assertTrue(all(event["stage"] == "beacon-source-check" for event in events))
        self.assertNotIn(BeaconHandler.password, stderr.getvalue())
        self.assertNotIn(self.url, stderr.getvalue())

    def test_blocked_transfer_keeps_heartbeats_and_reports_byte_totals(self):
        output = HeartbeatOutput()
        remote = download_beacon.RemoteFile(4, None, None)

        class Response:
            status = 200
            headers = {"Content-Length": "4"}

            def __init__(self):
                self.pending = True

            def read(self, size):
                if not self.pending:
                    return b""
                if not output.heartbeat.wait(2):
                    raise AssertionError("no heartbeat during blocked response read")
                self.pending = False
                return b"data"

        progress = download_beacon.Progress
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(output), \
             patch.object(download_beacon, "Progress", side_effect=lambda label, **kwargs:
                          progress(label, interval=0.01, **kwargs)):
            received = download_beacon._download_response(
                Response(), self.raw_dir / "partial", 0, remote, 0)
        self.assertEqual(received, 4)
        self.assertEqual(stdout.getvalue(), "")
        events = progress_events(output.getvalue())
        heartbeat = next(event for event in events if event["event"] == "heartbeat")
        self.assertEqual(heartbeat["processed_bytes"], 0)
        self.assertEqual(events[-1]["processed_bytes"], 4)
        self.assertEqual(events[-1]["total_bytes"], 4)
        self.assertEqual(events[-1]["event"], "completed")
        self.assertNotIn('"data"', output.getvalue())

    def test_blocked_archive_validation_keeps_heartbeats_without_sql_content(self):
        archive = self.raw_dir / "archive.sql.gz"
        archive.write_bytes(self.server.payload)
        output = HeartbeatOutput()
        progress = download_beacon.Progress
        original_read = download_beacon._HashingReader.read

        def blocked_read(reader, size=-1):
            if not output.heartbeat.wait(2):
                raise AssertionError("no heartbeat during blocked archive read")
            return original_read(reader, size)

        with contextlib.redirect_stderr(output), \
             patch.object(download_beacon, "Progress", side_effect=lambda label, **kwargs:
                          progress(label, interval=0.01, **kwargs)), \
             patch.object(download_beacon._HashingReader, "read", blocked_read):
            metadata = download_beacon.validate_archive(archive)
        events = progress_events(output.getvalue())
        self.assertTrue(any(event["event"] == "heartbeat" for event in events))
        self.assertEqual(events[-1]["processed_bytes"], len(self.server.payload))
        self.assertEqual(events[-1]["expanded_bytes"], len(BeaconHandler.sql))
        self.assertEqual(events[-1]["event"], "completed")
        self.assertEqual(metadata["uncompressed_sha256"], hashlib.sha256(BeaconHandler.sql).hexdigest())
        self.assertNotIn("CREATE TABLE", output.getvalue())

    def test_archive_failure_reports_stage_without_dump_payload(self):
        archive = self.raw_dir / "archive.sql.gz"
        archive.write_bytes(gzip.compress(b"private-payload-password"))
        output = io.StringIO()
        with contextlib.redirect_stderr(output), self.assertRaisesRegex(RuntimeError, "not a MySQL dump"):
            download_beacon.validate_archive(archive)
        events = progress_events(output.getvalue())
        self.assertEqual(events[-1]["event"], "failed")
        self.assertEqual(events[-1]["stage"], "beacon-archive-validation")
        self.assertNotIn("private-payload-password", output.getvalue())

    def test_changed_validator_same_bytes_revalidates_once_then_noop(self):
        result = self.download()
        original_stat = result.stat()
        self.server.etag = '"new-validator"'
        self.assertEqual(self.download(), result)
        self.assertEqual(self.download(), result)
        self.assertEqual(len(self.get_requests()), 2)
        self.assertEqual(result.stat().st_ino, original_stat.st_ino)
        self.assertEqual(result.stat().st_mtime_ns, original_stat.st_mtime_ns)

    def test_same_date_revision_is_rejected_even_with_force(self):
        result = self.download()
        original = result.read_bytes()
        self.server.etag = '"changed-bytes"'
        self.server.payload = gzip.compress(
            BeaconHandler.sql.replace(b"example", b"revised"), mtime=0
        )
        for force in (False, True):
            with (
                self.subTest(force=force),
                self.assertRaisesRegex(RuntimeError, "immutable snapshot revision"),
            ):
                self.download(force=force)
            self.assertEqual(result.read_bytes(), original)
        self.assertEqual((self.raw_dir / "pkpbeacon-latest.sql.gz").resolve(), result)
        requests = len(self.get_requests())
        with (
            patch.object(
                download_beacon,
                "validate_archive",
                side_effect=AssertionError("must not reinflate known revision"),
            ),
            self.assertRaisesRegex(RuntimeError, "immutable snapshot revision"),
        ):
            self.download()
        self.assertEqual(len(self.get_requests()), requests)

    def test_new_footer_date_gets_new_immutable_snapshot(self):
        previous = self.download()
        self.server.etag = '"snapshot-two"'
        self.server.payload = gzip.compress(
            BeaconHandler.sql.replace(b"2026-07-01", b"2026-07-02"), mtime=0
        )
        newest = self.download()
        self.assertNotEqual(previous, newest)
        self.assertTrue(previous.exists())
        self.assertEqual(newest.name, "pkpbeacon-2026-07-02.sql.gz")
        self.assertEqual((self.raw_dir / "pkpbeacon-latest.sql.gz").resolve(), newest)

    def test_interrupted_download_resumes_only_matching_identity(self):
        self.server.interrupt_bytes = 17
        with self.assertRaisesRegex(RuntimeError, "incomplete download|interrupted"):
            self.download()
        part = self.raw_dir / download_beacon.PART_NAME
        self.assertEqual(part.read_bytes(), self.server.payload[:17])
        result = self.download()
        request = self.get_requests()[-1]
        self.assertEqual(request["Range"], "bytes=17-")
        self.assertEqual(request["If-Range"], self.server.etag)
        self.assertEqual(result.read_bytes(), self.server.payload)

    def test_stale_partial_is_restarted_without_range(self):
        remote = self.probe()
        self.write_partial(remote, b"stale bytes")
        self.server.etag = '"replacement"'
        result = self.download()
        self.assertNotIn("Range", self.get_requests()[-1])
        self.assertEqual(result.read_bytes(), self.server.payload)

    def test_partial_without_saved_identity_is_never_reused(self):
        self.write_partial(self.probe(), b"untrusted bytes", identity=False)
        self.download()
        self.assertNotIn("Range", self.get_requests()[-1])

    def test_unknown_remote_identity_never_resumes_or_skips_get(self):
        self.server.etag = None
        self.server.modified = None
        remote = self.probe()
        self.write_partial(remote, self.server.payload[:17])
        self.download(remote)
        self.download()
        self.assertEqual(len(self.get_requests()), 2)
        self.assertTrue(all("Range" not in request for request in self.get_requests()))

    def test_last_modified_is_used_for_if_range_without_strong_etag(self):
        self.server.etag = 'W/"weak-validator"'
        remote = self.probe()
        self.write_partial(remote, self.server.payload[:17])
        self.download(remote)
        self.assertEqual(self.get_requests()[-1]["If-Range"], self.server.modified)

    def test_server_ignoring_range_restarts_safely(self):
        remote = self.probe()
        self.write_partial(remote, self.server.payload[:17])
        self.server.resume = False
        result = self.download(remote)
        self.assertEqual(result.read_bytes(), self.server.payload)

    def test_bad_content_range_is_rejected(self):
        remote = self.probe()
        self.write_partial(remote, self.server.payload[:17])
        self.server.content_range = (
            f"bytes 18-{len(self.server.payload) - 1}/{len(self.server.payload)}"
        )
        with self.assertRaisesRegex(RuntimeError, "Content-Range"):
            self.download(remote)
        self.assertFalse(list(self.raw_dir.glob("pkpbeacon-*.sql.gz")))

    def test_remote_rotation_between_head_and_get_is_rejected(self):
        remote = self.probe()
        self.server.etag = '"rotated"'
        with self.assertRaisesRegex(RuntimeError, "remote identity changed"):
            self.download(remote)
        self.assertFalse(list(self.raw_dir.glob("pkpbeacon-*.sql.gz")))

    def test_corrupt_gzip_never_publishes_and_can_retry(self):
        good = self.server.payload
        corrupted = bytearray(good)
        corrupted[-8] ^= 1
        self.server.payload = bytes(corrupted)
        with self.assertRaisesRegex(RuntimeError, "gzip file failed validation"):
            self.download()
        self.assertFalse(list(self.raw_dir.glob("pkpbeacon-*.sql.gz")))
        self.assertFalse((self.raw_dir / download_beacon.PART_NAME).exists())
        self.server.payload = good
        self.assertEqual(self.download().read_bytes(), good)

    def test_truncated_gzip_and_invalid_deflate_never_publish(self):
        malformed_deflate = b"\x1f\x8b\x08\x00" + bytes(6) + b"\x07" + bytes(8)
        for payload in (self.server.payload[:-4], malformed_deflate):
            with self.subTest(payload=payload):
                self.server.payload = payload
                with self.assertRaisesRegex(
                    RuntimeError, "gzip file failed validation"
                ):
                    self.download()
                self.assertFalse((self.raw_dir / download_beacon.PART_NAME).exists())
                self.assertFalse(list(self.raw_dir.glob("pkpbeacon-*.sql.gz")))

    def test_missing_footer_and_non_mysql_payloads_are_rejected(self):
        for sql, message in [
            (b"-- MySQL dump\nSELECT 1;\n", "final extraction timestamp"),
            (b"not a SQL dump\n", "not a MySQL dump"),
        ]:
            with self.subTest(sql=sql):
                self.server.payload = gzip.compress(sql)
                with self.assertRaisesRegex(RuntimeError, message):
                    self.download()
                self.assertFalse(list(self.raw_dir.glob("*.sql")))

    def test_expanded_size_guard_and_disk_floor_prevent_publication(self):
        with (
            patch.dict(os.environ, {"OJS_BEACON_MAX_EXPANDED_BYTES": "20"}),
            self.assertRaisesRegex(RuntimeError, "MAX_EXPANDED_BYTES"),
        ):
            self.download()
        with (
            patch.dict(os.environ, {"OJS_BEACON_MIN_FREE_BYTES": str(2**63)}),
            self.assertRaisesRegex(RuntimeError, "insufficient free space"),
        ):
            self.download()
        self.assertFalse(list(self.raw_dir.glob("pkpbeacon-*.sql.gz")))

    def test_metadata_fast_path_and_stat_invalidation(self):
        result = self.download()
        with patch.object(
            download_beacon,
            "validate_archive",
            side_effect=AssertionError("must not reinflate"),
        ):
            self.assertEqual(
                download_beacon.validate_cached_snapshot(result)["dump_date"],
                "2026-07-01",
            )
        before = result.stat()
        result.chmod(0o644)
        result.write_bytes(self.server.payload)
        os.utime(result, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertIsNone(download_beacon.read_validated_metadata(result))
        self.assertIsNone(
            download_beacon.known_remote_snapshot(self.raw_dir, self.url, self.probe())
        )
        self.assertEqual(
            download_beacon.validate_cached_snapshot(result)["uncompressed_size"],
            len(BeaconHandler.sql),
        )

    def test_malformed_metadata_does_not_bypass_validation(self):
        result = self.download()
        sidecar = Path(str(result) + ".metadata.json")
        metadata = json.loads(sidecar.read_text())
        metadata["uncompressed_sha256"] = "wrong"
        sidecar.write_text(json.dumps(metadata))
        self.assertIsNone(download_beacon.read_validated_metadata(result))
        with patch.object(
            download_beacon, "validate_archive", wraps=download_beacon.validate_archive
        ) as validate:
            download_beacon.validate_cached_snapshot(result)
        validate.assert_called_once()

    def test_legacy_sql_is_preserved_and_checked_for_revisions(self):
        legacy = self.raw_dir / "pkpbeacon-2026-07-01.sql"
        legacy.write_bytes(BeaconHandler.sql.replace(b"example", b"revised"))
        with self.assertRaisesRegex(RuntimeError, "immutable legacy snapshot revision"):
            self.download()
        legacy.write_bytes(BeaconHandler.sql)
        result = self.download()
        self.assertEqual(result.read_bytes(), self.server.payload)
        self.assertEqual(legacy.read_bytes(), BeaconHandler.sql)

    def test_expected_date_is_enforced_on_download_and_cached_result(self):
        with self.assertRaisesRegex(RuntimeError, "requested version differs"):
            self.download(expected_version="2026-07-03")
        self.download(expected_version="2026-07-01")
        with self.assertRaisesRegex(RuntimeError, "requested version differs"):
            self.download(expected_version="2026-07-03")

    def test_authentication_requires_https_and_explicit_loopback_exception(self):
        with self.assertRaisesRegex(ValueError, "require HTTPS"):
            download_beacon.probe_remote(self.url, "user", "password", 5)
        with self.assertRaisesRegex(ValueError, "require HTTPS"):
            download_beacon.probe_remote(
                "http://example.com/dump.gz",
                "user",
                "password",
                5,
                allow_insecure_localhost=True,
            )
        with self.assertRaisesRegex(ValueError, "embedded"):
            download_beacon.probe_remote(
                "https://user:secret@example.com/dump.gz", "user", "password", 5
            )
        self.assertEqual(self.server.requests, [])

    def test_bad_password_and_cross_origin_redirect_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "authentication failed"):
            download_beacon.probe_remote(
                self.url,
                BeaconHandler.username,
                "wrong",
                5,
                allow_insecure_localhost=True,
            )
        with self.assertRaisesRegex(RuntimeError, "HTTP 302"):
            download_beacon.probe_remote(
                self.url.replace("/pkpbeacon.gz", "/redirect"),
                BeaconHandler.username,
                BeaconHandler.password,
                5,
                allow_insecure_localhost=True,
            )

    def test_same_origin_redirect_preserves_head(self):
        remote = download_beacon.probe_remote(
            self.url.replace("/pkpbeacon.gz", "/same-origin-redirect"),
            BeaconHandler.username,
            BeaconHandler.password,
            5,
            allow_insecure_localhost=True,
        )
        self.assertEqual(remote.size, len(self.server.payload))
        self.assertEqual(
            [method for method, _ in self.server.requests], ["HEAD", "HEAD"]
        )

    def test_credentials_permissions_and_parse_errors_never_leak_values(self):
        path = self.raw_dir / "beacon.ini"
        path.write_text("[beacon]\nusername = test-user\npassword = value#with-hash\n")
        path.chmod(0o600)
        self.assertEqual(
            download_beacon.read_credentials(path), ("test-user", "value#with-hash")
        )
        path.chmod(0o644)
        with self.assertRaisesRegex(RuntimeError, "permissions are too open"):
            download_beacon.read_credentials(path)
        path.chmod(0o600)
        path.write_text(
            "[beacon]\npassword=secret-password\npassword=secret-duplicate\n"
        )
        with self.assertRaises(RuntimeError) as raised:
            download_beacon.read_credentials(path)
        self.assertNotIn("secret", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
