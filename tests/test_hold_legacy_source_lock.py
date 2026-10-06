from __future__ import annotations

import json
import os
import selectors
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import hold_legacy_source_lock as legacy
from source_activity import SourceActivityBusy, source_activity


SCRIPT = Path(legacy.__file__)


@unittest.skipUnless(sys.platform == "linux" and hasattr(os, "pidfd_open"),
                     "Linux pidfds are required")
class LegacySourceLockTest(unittest.TestCase):
    def process(self, code: str):
        child = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.stop, child)
        return child

    @staticmethod
    def stop(child):
        if child.poll() is None:
            child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream is not None:
                stream.close()

    def guard(self, raw: Path, *pids: int):
        command = [sys.executable, str(SCRIPT), "--raw-dir", str(raw)]
        for pid in pids:
            command.extend(("--pid", str(pid)))
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 text=True)
        self.addCleanup(self.stop, child)
        return child

    def acquired(self, guard):
        with selectors.DefaultSelector() as selector:
            selector.register(guard.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(timeout=5), "guard did not report acquisition")
        event = json.loads(guard.stdout.readline())
        self.assertEqual(event["event"], "guard-acquired")
        return event

    def test_short_lived_process_releases_lock_and_duplicate_pids_are_deduplicated(self):
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            target = self.process("import sys; sys.stdin.read(1)")
            guard = self.guard(raw, target.pid, target.pid)
            self.assertEqual(self.acquired(guard)["pids"], [target.pid])
            with self.assertRaises(SourceActivityBusy), source_activity(raw):
                pass
            target.stdin.write("x")
            target.stdin.flush()
            target.wait(timeout=5)
            output, error = guard.communicate(timeout=5)
            self.assertEqual(guard.returncode, 0, error)
            self.assertEqual(json.loads(output)["event"], "guard-released")
            with source_activity(raw):
                pass

    def test_parent_exit_does_not_release_lock_while_child_is_alive(self):
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            parent = self.process(
                "import subprocess,sys,time; "
                "child=subprocess.Popen([sys.executable,'-c','import sys; sys.stdin.read(1)'],"
                "stdin=sys.stdin,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
                "print(child.pid,flush=True); time.sleep(60)"
            )
            descendant = int(parent.stdout.readline())
            guard = self.guard(raw, parent.pid, descendant)
            self.acquired(guard)
            parent.terminate()
            parent.wait(timeout=5)
            self.assertIsNone(guard.poll())
            with self.assertRaises(SourceActivityBusy), source_activity(raw):
                pass
            parent.stdin.write("x")
            parent.stdin.flush()
            output, error = guard.communicate(timeout=5)
            self.assertEqual(guard.returncode, 0, error)
            self.assertEqual(json.loads(output)["event"], "guard-released")

    def test_busy_bridge_exits_nonzero_without_reporting_acquisition(self):
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            target = self.process("import sys; sys.stdin.read(1)")
            with source_activity(raw):
                guard = self.guard(raw, target.pid)
                output, error = guard.communicate(timeout=5)
            self.assertEqual(guard.returncode, 1)
            self.assertEqual(output, "")
            self.assertIn("was not acquired", error)

    def test_invalid_pid_is_rejected(self):
        for value in ("0", "-1", "no-pid", str(2**40)):
            with self.subTest(value=value), TemporaryDirectory() as directory:
                result = subprocess.run([sys.executable, str(SCRIPT), "--raw-dir", directory,
                                         "--pid", value], capture_output=True, text=True,
                                        timeout=5)
                self.assertEqual(result.returncode, 2)
                self.assertNotIn("guard-acquired", result.stdout)

    def test_exited_process_fails_before_lock_acquisition(self):
        target = self.process("pass")
        target.wait(timeout=5)
        with TemporaryDirectory() as directory, mock.patch.object(legacy, "source_activity") as lock:
            with self.assertRaisesRegex(RuntimeError, "already exited"):
                legacy.hold(Path(directory), [target.pid])
            lock.assert_not_called()

    def test_permission_failure_closes_pidfds_before_taking_lock(self):
        target = self.process("import sys; sys.stdin.read(1)")
        descriptor = os.pidfd_open(target.pid)
        with TemporaryDirectory() as directory, \
                mock.patch.object(legacy.os, "pidfd_open",
                                  side_effect=[descriptor, PermissionError("denied")]), \
                mock.patch.object(legacy, "source_activity") as lock:
            with self.assertRaisesRegex(RuntimeError, "cannot monitor PID"):
                legacy.hold(Path(directory), [target.pid, target.pid + 1])
            lock.assert_not_called()
        with self.assertRaises(OSError):
            os.fstat(descriptor)


if __name__ == "__main__":
    unittest.main()
