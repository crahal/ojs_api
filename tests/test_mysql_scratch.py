"""External scratch safety and real MySQL temporary-tablespace routing."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import run_pipeline


MYSQL_AVAILABLE = all(
    shutil.which(name) for name in ("mysqld", "mysql", "mysqladmin", "mysqldump")
)


class MySQLScratchTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory(prefix="ojs-scratch-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        patcher = mock.patch.dict(os.environ, OJS_MYSQL_TMP_ROOT=str(self.scratch))
        patcher.start()
        self.addCleanup(patcher.stop)

    def server(self, datadir=None):
        server = run_pipeline.MySQLServer(
            run_pipeline.MySQLTools("unused", "unused", "unused", "unused", "8.4.0"),
            datadir or self.root / "mysql", "128M",
        )
        self.addCleanup(server.runtime.cleanup)
        return server

    def mocked_start(self, server):
        with mock.patch.object(server, "_run_control", return_value=subprocess.CompletedProcess([], 0)) as control, \
                mock.patch.object(run_pipeline.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)):
            server.start(None)
        return control.call_args.args[0]

    def mocked_shutdown(self, server):
        with mock.patch.object(server, "_run_control", return_value=subprocess.CompletedProcess([], 0)):
            server.shutdown(None)

    def test_explicit_configuration_and_unset_default(self):
        self.assertEqual(run_pipeline.mysql_scratch_root(), self.scratch)
        self.assertIsNone(run_pipeline.mysql_scratch_root(configured=""))
        with mock.patch.dict(os.environ, OJS_MYSQL_TMP_ROOT=""):
            server = self.server()
            arguments = self.mocked_start(server)
            self.assertEqual(server.disk_tmp, self.root / ".mysql-mysql-tmp")
            self.assertFalse(any(arg.startswith("--innodb-temp-tablespaces-dir=") for arg in arguments))
            self.mocked_shutdown(server)
            self.assertFalse(server.disk_tmp.exists())

    def test_root_validation_is_read_only_and_rejects_unsafe_paths(self):
        file = self.root / "file"
        file.write_text("retained")
        link = self.root / "linked"
        link.symlink_to(self.scratch, target_is_directory=True)
        unsafe = (
            "relative/scratch", "/", "/tmp", "//tmp", "/srv", "/var", "/etc", "/usr/local",
            str(self.root / "missing"), str(file), str(link), str(link / "nested"),
            str(self.scratch / ".." / "scratch"), str(self.root / "scratch:other"),
            str(run_pipeline.PROJECT_ROOT), str(run_pipeline.DEFAULT_RAW_DIR),
            str(run_pipeline.DEFAULT_CLEAN_DIR),
        )
        before = set(self.root.iterdir())
        for configured in unsafe:
            with self.subTest(configured=configured), self.assertRaises(run_pipeline.PipelineError):
                run_pipeline.mysql_scratch_root(configured=configured)
        self.assertEqual(set(self.root.iterdir()), before)
        self.assertEqual(file.read_text(), "retained")

    def test_data_trees_and_datadir_overlap_are_rejected(self):
        for name in ("data", "raw", "clean"):
            path = self.root / name / "scratch"
            path.mkdir(parents=True)
            with self.subTest(name=name), self.assertRaises(run_pipeline.PipelineError):
                run_pipeline.mysql_scratch_root(configured=str(path))
        for datadir in (self.scratch, self.scratch / "mysql", self.root):
            with self.subTest(datadir=datadir), self.assertRaises(run_pipeline.PipelineError):
                run_pipeline.mysql_scratch_root(datadir, configured=str(self.scratch))

    def test_unique_children_route_both_temp_locations_and_preserve_siblings(self):
        sentinel = self.scratch / "unrelated"
        sentinel.write_text("retained")
        servers = (self.server(self.root / "first" / "mysql"),
                   self.server(self.root / "second" / "mysql"),
                   self.server(self.root / "first" / "mysql"))
        children = []
        for server in servers:
            arguments = self.mocked_start(server)
            child = server.disk_tmp.parent
            children.append(child)
            self.assertEqual(child.parent, self.scratch)
            self.assertEqual(server.innodb_tmp.parent, child)
            self.assertIn(f"--tmpdir={server.disk_tmp}", arguments)
            self.assertIn(f"--innodb-temp-tablespaces-dir={server.innodb_tmp}", arguments)
            self.assertFalse(any(arg.startswith("--innodb-data-home-dir=") for arg in arguments))
            marker = json.loads((child / ".ojs-mysql-scratch.json").read_text())
            self.assertEqual(marker["datadir"], str(server.datadir))
        self.assertEqual(len(set(children)), 3)
        for server in servers:
            self.mocked_shutdown(server)
        self.assertEqual(list(self.scratch.iterdir()), [sentinel])

    def test_startup_exception_retains_scratch_even_after_shutdown(self):
        server = self.server()
        with mock.patch.object(server, "_run_control", side_effect=run_pipeline.PipelineError("failed")), \
                self.assertRaisesRegex(run_pipeline.PipelineError, "failed"):
            server.start(None)
        child = server.disk_tmp.parent
        marker = child / ".ojs-mysql-scratch.json"
        self.assertTrue(marker.is_file())
        server.shutdown(None)
        self.assertTrue(marker.is_file())

    def test_shutdown_failure_retains_scratch(self):
        for error in (subprocess.CompletedProcess([], 1), run_pipeline.PipelineError("failed")):
            with self.subTest(error=error):
                server = self.server()
                self.mocked_start(server)
                child = server.disk_tmp.parent
                if isinstance(error, Exception):
                    with mock.patch.object(server, "_run_control", side_effect=error), \
                            self.assertRaises(run_pipeline.PipelineError):
                        server.shutdown(None)
                    self.mocked_shutdown(server)
                else:
                    with mock.patch.object(server, "_run_control", return_value=error):
                        server.shutdown(None)
                self.assertTrue((child / ".ojs-mysql-scratch.json").is_file())

    def test_readiness_failure_retains_scratch_after_successful_shutdown(self):
        server = self.server()
        with mock.patch.object(server, "_run_control", return_value=subprocess.CompletedProcess([], 0)), \
                mock.patch.object(run_pipeline.time, "monotonic", side_effect=(0, 31)), \
                self.assertRaisesRegex(run_pipeline.PipelineError, "did not become ready"):
            server.start(None)
        self.assertTrue(server.running)
        child = server.disk_tmp.parent
        self.mocked_shutdown(server)
        self.assertTrue((child / ".ojs-mysql-scratch.json").is_file())

    def test_changed_or_missing_ownership_marker_prevents_deletion(self):
        for change in ("overwrite", "remove", "symlink"):
            with self.subTest(change=change):
                server = self.server()
                self.mocked_start(server)
                child = server.disk_tmp.parent
                marker = child / ".ojs-mysql-scratch.json"
                if change == "overwrite":
                    marker.write_text("not owned")
                else:
                    original = self.root / f"original-{change}"
                    marker.rename(original)
                    if change == "symlink":
                        marker.symlink_to(original)
                with self.assertRaisesRegex(run_pipeline.PipelineError, "ownership"):
                    self.mocked_shutdown(server)
                self.assertTrue(child.is_dir())

    def test_replaced_child_is_not_deleted(self):
        server = self.server()
        self.mocked_start(server)
        child = server.disk_tmp.parent
        retained = child.with_name(child.name + "-retained")
        child.rename(retained)
        child.symlink_to(retained, target_is_directory=True)
        with self.assertRaisesRegex(run_pipeline.PipelineError, "ownership"):
            self.mocked_shutdown(server)
        self.assertTrue(child.is_symlink())
        self.assertTrue((retained / ".ojs-mysql-scratch.json").is_file())

    def test_root_revalidated_before_creating_scratch(self):
        server = self.server()
        retained = self.root / "retained"
        self.scratch.rename(retained)
        self.scratch.symlink_to(retained, target_is_directory=True)
        with self.assertRaisesRegex(run_pipeline.PipelineError, "symlinks"):
            server.start(None)
        self.assertEqual(list(retained.iterdir()), [])

    @unittest.skipUnless(MYSQL_AVAILABLE, "MySQL 8.4 server and client tools required")
    def test_real_mysql_routes_temp_files_and_preserves_persistent_data_on_restart(self):
        tools = run_pipeline.MySQLTools.discover()
        sentinel = self.scratch / "unrelated"
        sentinel.write_text("retained")
        server = run_pipeline.MySQLServer(tools, self.root / "mysql", "128M")
        try:
            server.initialize()
            server.start(None)
            self.assertEqual(Path(server.execute("SELECT @@tmpdir;")), server.disk_tmp)
            self.assertEqual(Path(server.execute("SELECT @@innodb_temp_tablespaces_dir;")), server.innodb_tmp)
            self.assertTrue(any(server.innodb_tmp.glob("*.ibt")))
            server.execute("CREATE DATABASE fixture; CREATE TABLE fixture.persistent (id INT PRIMARY KEY); "
                           "INSERT INTO fixture.persistent VALUES (42);")
            self.assertTrue((server.datadir / "fixture" / "persistent.ibd").is_file())
            first_child = server.disk_tmp.parent
        finally:
            server.shutdown(None)
        self.assertFalse(first_child.exists())
        self.assertEqual(list(self.scratch.iterdir()), [sentinel])
        restarted = run_pipeline.MySQLServer(tools, self.root / "mysql", "128M")
        try:
            restarted.start(None)
            self.assertNotEqual(first_child, restarted.disk_tmp.parent)
            self.assertEqual(restarted.execute("SELECT id FROM fixture.persistent;"), "42")
            self.assertTrue(any(restarted.innodb_tmp.glob("*.ibt")))
        finally:
            restarted.shutdown(None)
        self.assertEqual(list(self.scratch.iterdir()), [sentinel])


if __name__ == "__main__":
    unittest.main()
