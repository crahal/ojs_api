"""Real-process exclusion across the public update/download entry points."""
from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))
import compact_update
import run_pipeline
from source_activity import source_activity


class SourceActivityIntegrationTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.raw = self.root / "data" / "raw"
        self.clean = self.root / "data" / "clean"
        self.env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(SRC))
        self.env.pop("OJS_SOURCE_ACTIVITY_FD", None)

    def assert_download_commands_skip(self):
        commands = (
            ["download_beacon.py", "--raw-dir", str(self.raw),
             "--credentials-file", str(self.root / "does-not-exist.ini")],
            ["compact_update.py", "--project-root", str(self.root)],
            ["run_pipeline.py", "--raw-dir", str(self.raw),
             "--clean-dir", str(self.clean), "--download-only"],
        )
        for script, *arguments in commands:
            with self.subTest(script=script):
                result = subprocess.run([sys.executable, str(SRC / script), *arguments],
                                        env=self.env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                message = json.loads(result.stdout)
                self.assertEqual(message["status"], "skipped")
                self.assertEqual(message["reason"], "source_activity_busy")
                self.assertNotIn("[progress]", result.stderr)

    def test_busy_clis_do_not_need_credentials_or_start_network_work(self):
        with source_activity(self.raw):
            self.assert_download_commands_skip()
        self.assertEqual([p.name for p in self.raw.iterdir()], [".source-activity.lock"])
        self.assertFalse(self.clean.exists())

    def test_pipeline_holds_source_exclusion_during_wrangling(self):
        target = mock.Mock(path=self.raw / "pkpbeacon-2026-10-01.sql.gz",
                           version="2026-10-01", size=100)
        target.completed_at.isoformat.return_value = "2026-10-01T01:00:00"
        counts = mock.Mock(source_records=10, active_articles=9,
                           removed_articles=0, merged_articles=1, events=10)

        def build(**kwargs):
            self.assert_download_commands_skip()
            return self.clean / "mysql-2026-10-01", counts

        with mock.patch.dict(os.environ, MYSQL_ROOT_PASSWORD="fixture",
                             OJS_DB_API_PASSWORD="fixture"), \
             mock.patch.object(run_pipeline, "acquire_snapshot", return_value=target), \
             mock.patch.object(run_pipeline, "snapshots_to_process", return_value=[target]), \
             mock.patch.object(run_pipeline, "build_snapshot_database", side_effect=build) as builder, \
             contextlib.redirect_stdout(io.StringIO()):
            result = run_pipeline.main(["--raw-dir", str(self.raw),
                                        "--clean-dir", str(self.clean), "--skip-download"])
        self.assertEqual(result, 0)
        builder.assert_called_once()
        with source_activity(self.raw):
            pass  # Released after completion, ready for the next daily check.

    def test_coordinator_passes_lease_to_only_source_children(self):
        args = compact_update.parser().parse_args(["--project-root", str(self.root),
                                                  "--reserve-gb", "0"])
        coordinator = compact_update.Coordinator(args)
        code = (
            "import sys; from pathlib import Path; "
            "from source_activity import source_activity; "
            "lease=source_activity(Path(sys.argv[1])); "
            "lease.__enter__(); lease.__exit__(None,None,None)"
        )
        coordinator.env = self.env
        with source_activity(self.raw), contextlib.redirect_stderr(io.StringIO()):
            coordinator.child([sys.executable, "-c", code, str(self.raw)])
            self.assert_download_commands_skip()
            # Generic subprocesses must not inherit a lock FD (e.g. MySQL/Docker).
            result = coordinator.command([sys.executable, "-c",
                "import os; print(os.environ.get('OJS_SOURCE_ACTIVITY_FD', 'absent'))"])
            self.assertEqual(result.strip(), "absent")

    def test_direct_pipeline_downloader_inherits_lease(self):
        self.raw.mkdir(parents=True)
        snapshot = self.raw / "pkpbeacon-2026-10-01.sql"
        snapshot.write_text("-- MySQL dump 10.13\n-- Dump completed on 2026-10-01 01:00:00\n")
        (self.raw / "pkpbeacon-latest.sql").symlink_to(snapshot.name)
        args = run_pipeline.parser().parse_args(["--raw-dir", str(self.raw)])
        original_run = subprocess.run

        def run_source_child(command, **options):
            self.assertEqual(Path(command[1]).name, "download_beacon.py")
            self.assertTrue(options["pass_fds"])
            code = (
                "from pathlib import Path; from source_activity import source_activity; "
                "import sys; lease=source_activity(Path(sys.argv[1])); "
                "lease.__enter__(); lease.__exit__(None,None,None)"
            )
            options["env"]["PYTHONPATH"] = str(SRC)
            return original_run([sys.executable, "-c", code, str(self.raw)],
                                timeout=10, **options)

        with source_activity(self.raw), \
             mock.patch.object(run_pipeline.subprocess, "run", side_effect=run_source_child), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(run_pipeline.acquire_snapshot(args).version, "2026-10-01")


if __name__ == "__main__":
    unittest.main()
