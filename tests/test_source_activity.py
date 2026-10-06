from __future__ import annotations

import concurrent.futures
import fcntl
import os
import selectors
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

SOURCE_DIR = Path(__file__).parents[1] / "src"
sys.path.insert(0, str(SOURCE_DIR))
from source_activity import (
    INHERITED_FD_ENV,
    LOCK_NAME,
    SourceActivityBusy,
    SourceActivityError,
    source_activity,
    source_activity_child_options,
)


PRELUDE = """
import os, sys
from pathlib import Path
from source_activity import source_activity, SourceActivityBusy, INHERITED_FD_ENV
raw = Path(sys.argv[1])
"""
PROBE = PRELUDE + """
try:
    with source_activity(raw):
        print('acquired')
except SourceActivityBusy:
    print('busy')
    sys.exit(7)
"""


class SourceActivityTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.raw = self.root / "raw"
        environment = mock.patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        os.environ.pop(INHERITED_FD_ENV, None)

    def child_env(self):
        env = dict(os.environ, PYTHONPATH=str(SOURCE_DIR))
        env.pop(INHERITED_FD_ENV, None)
        return env

    def run_child(self, code=PROBE, *, options=None, raw=None):
        return subprocess.run(
            [sys.executable, "-u", "-c", code, str(raw or self.raw)],
            capture_output=True, text=True, timeout=10,
            **(options or {"env": self.child_env()}),
        )

    def spawn_child(self, code, options):
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", code, str(self.raw)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, **options,
        )

        def cleanup():
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)
        self.addCleanup(cleanup)
        return child

    def assert_ready(self, child):
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(timeout=10), "child did not become ready")
        self.assertEqual(child.stdout.readline().strip(), "ready")

    def test_same_thread_nesting_and_canonical_alias_share_one_lease(self):
        with source_activity(self.raw) as outer:
            alias = self.root / "alias"
            alias.symlink_to(self.raw, target_is_directory=True)
            with source_activity(alias) as inner:
                self.assertIs(inner, outer)
                self.assertEqual(source_activity_child_options(alias), outer.child_options())
            self.assertEqual(self.run_child().returncode, 7)
        self.assertEqual(self.run_child().returncode, 0)
        self.assertTrue((self.raw / LOCK_NAME).is_file())

    def test_unrelated_threads_contend(self):
        def contender():
            with source_activity(self.raw):
                self.fail("unrelated thread entered an existing lease")
        with source_activity(self.raw):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                with self.assertRaises(SourceActivityBusy):
                    pool.submit(contender).result(timeout=10)

    def test_independent_directories_do_not_contend(self):
        with source_activity(self.raw):
            result = self.run_child(raw=self.root / "other")
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_exception_releases_only_own_descriptor_and_keeps_lock_file(self):
        real_flock = fcntl.flock
        with mock.patch("source_activity.fcntl.flock", wraps=real_flock) as flock:
            with self.assertRaisesRegex(RuntimeError, "fixture failure"):
                with source_activity(self.raw):
                    raise RuntimeError("fixture failure")
            self.assertTrue(all(call.args[1] != fcntl.LOCK_UN for call in flock.call_args_list))
        self.assertTrue((self.raw / LOCK_NAME).exists())
        self.assertEqual(self.run_child().returncode, 0)

    def test_inherited_child_and_grandchild_can_use_the_parent_lease(self):
        code = PRELUDE + """
import subprocess
with source_activity(raw) as lease:
    assert INHERITED_FD_ENV not in os.environ
    result = subprocess.run(
        [sys.executable, '-c', %r, str(raw)],
        capture_output=True, text=True, timeout=5, **lease.child_options())
    assert result.returncode == 0, result.stderr
    print(result.stdout.strip())
""" % PROBE
        with source_activity(self.raw) as lease:
            result = self.run_child(code, options=lease.child_options(self.child_env()))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "acquired")
            # Child/grandchild closure must not explicitly unlock the parent.
            self.assertEqual(self.run_child().returncode, 7)

    def test_parent_context_exit_keeps_a_surviving_child_locked(self):
        code = PRELUDE + """
with source_activity(raw):
    print('ready', flush=True)
    sys.stdin.readline()
"""
        with source_activity(self.raw) as lease:
            child = self.spawn_child(code, lease.child_options(self.child_env()))
            self.assert_ready(child)
        self.assertEqual(self.run_child().returncode, 7)
        _, stderr = child.communicate("finish\n", timeout=10)
        self.assertEqual(child.returncode, 0, stderr)
        self.assertEqual(self.run_child().returncode, 0)

    def test_child_crash_does_not_unlock_parent_or_leave_stale_lock(self):
        code = PRELUDE + """
with source_activity(raw):
    print('ready', flush=True)
    sys.stdin.readline()
"""
        with source_activity(self.raw) as lease:
            child = self.spawn_child(code, lease.child_options(self.child_env()))
            self.assert_ready(child)
            child.kill()
            child.communicate(timeout=10)
            self.assertEqual(self.run_child().returncode, 7)
        self.assertEqual(self.run_child().returncode, 0)

    def test_inherited_capability_is_not_reused_by_unrelated_child_threads(self):
        code = PRELUDE + """
import concurrent.futures
def contender():
    try:
        with source_activity(raw):
            return 'acquired'
    except SourceActivityBusy:
        return 'busy'
with source_activity(raw):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(contender).result(timeout=5) == 'busy'
"""
        with source_activity(self.raw) as lease:
            result = self.run_child(code, options=lease.child_options(self.child_env()))
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_inherited_original_descriptor_is_closed_and_token_consumed(self):
        code = PRELUDE + """
descriptor = int(os.environ[INHERITED_FD_ENV])
with source_activity(raw):
    os.fstat(descriptor)
assert INHERITED_FD_ENV not in os.environ
try:
    os.fstat(descriptor)
except OSError:
    pass
else:
    raise AssertionError('inherited descriptor leaked')
try:
    with source_activity(raw):
        raise AssertionError('parent lease was released')
except SourceActivityBusy:
    pass
"""
        with source_activity(self.raw) as lease:
            result = self.run_child(code, options=lease.child_options(self.child_env()))
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_child_options_replaces_inheritance_without_mutating_input_environment(self):
        env = {"EXAMPLE": "retained", INHERITED_FD_ENV: "stale"}
        with source_activity(self.raw) as lease:
            options = lease.child_options(env)
            self.assertEqual(options["env"]["EXAMPLE"], "retained")
            self.assertEqual(options["env"][INHERITED_FD_ENV], str(options["pass_fds"][0]))
            self.assertFalse(os.get_inheritable(options["pass_fds"][0]))
            self.assertEqual(env[INHERITED_FD_ENV], "stale")
        with self.assertRaises(SourceActivityError):
            lease.child_options(env)
        with self.assertRaises(SourceActivityError):
            source_activity_child_options(self.raw, env)

    def test_malformed_or_closed_inherited_fd_fails_instead_of_skipping(self):
        for value in ("", "not-a-number", "-1", "0", "2", "2147483647", "99999999999"):
            with self.subTest(value=value), mock.patch.dict(os.environ, {INHERITED_FD_ENV: value}):
                with self.assertRaises(SourceActivityError) as raised:
                    with source_activity(self.raw):
                        self.fail("invalid inherited descriptor was accepted")
                self.assertNotIsInstance(raised.exception, SourceActivityBusy)

    def test_inherited_fd_without_pass_fds_fails_closed(self):
        with source_activity(self.raw) as lease:
            options = lease.child_options(self.child_env())
            result = self.run_child(options={"env": options["env"]})
            self.assertNotEqual(result.returncode, 0)
            self.assertNotEqual(result.returncode, 7)
            self.assertIn("inherited source activity descriptor", result.stderr)

    def test_inherited_fd_must_match_requested_raw_directory(self):
        other = self.root / "other"
        with source_activity(other):
            pass
        with source_activity(self.raw) as lease:
            result = self.run_child(raw=other, options=lease.child_options(self.child_env()))
            self.assertNotEqual(result.returncode, 0)
            self.assertNotEqual(result.returncode, 7)
            self.assertIn("inherited source activity descriptor", result.stderr)

    def test_symlink_or_nonregular_lock_file_is_rejected(self):
        self.raw.mkdir()
        path = self.raw / LOCK_NAME
        target = self.root / "target"
        target.write_text("keep")
        path.symlink_to(target)
        with self.assertRaises(SourceActivityError):
            with source_activity(self.raw):
                self.fail("symlinked lock was accepted")
        path.unlink()
        os.mkfifo(path)
        with self.assertRaises(SourceActivityError):
            with source_activity(self.raw):
                self.fail("FIFO lock was accepted")
        self.assertEqual(target.read_text(), "keep")

    def test_permissions_failure_is_not_reported_as_busy(self):
        with mock.patch("source_activity.os.open", side_effect=PermissionError("fixture denied")):
            with self.assertRaises(SourceActivityError) as raised:
                with source_activity(self.raw):
                    self.fail("permissions failure was ignored")
            self.assertNotIsInstance(raised.exception, SourceActivityBusy)

    def test_replaced_lock_inode_is_detected_before_lease_is_used(self):
        with source_activity(self.raw) as lease:
            path = self.raw / LOCK_NAME
            path.rename(self.raw / "old-lock")
            path.write_text("")
            with self.assertRaises(SourceActivityError):
                lease.child_options()
            with self.assertRaises(SourceActivityError):
                with source_activity(self.raw):
                    self.fail("replaced lock inode was accepted")


if __name__ == "__main__":
    unittest.main()
