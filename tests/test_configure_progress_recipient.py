from __future__ import annotations

from contextlib import ExitStack, redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
import stat
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import configure_progress_email as email_setup
import configure_progress_recipient as configure


RECIPIENT = "jalperin@sfu.ca"
FIXTURE_SECRET = "fixture-only-gmail-secret"


class ConfigureProgressRecipientTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "ojs-api"
        self.path.mkdir(mode=0o700)
        self.primary = self.path / email_setup.ENVIRONMENT_FILE
        self.primary.write_text("OJS_PROGRESS_EMAIL_FROM=sender@gmail.com\nOJS_PROGRESS_EMAIL_TO=sender@gmail.com\n")
        self.primary.chmod(0o600)
        self.password = self.path / email_setup.PASSWORD_FILE
        self.password.write_text(FIXTURE_SECRET)
        self.password.chmod(0o600)
        self.target = self.path / "progress-email-jalperin.env"
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.object(email_setup, "CONFIG_DIR", self.path))
        self.patches.enter_context(mock.patch.object(configure.os, "geteuid", return_value=0))
        original_fstat, original_stat = os.fstat, os.stat

        def root_owned(information):
            fields = list(information)
            fields[4:6] = [0, 0]
            return os.stat_result(fields)

        self.patches.enter_context(mock.patch.object(configure.os, "fstat", side_effect=lambda fd: root_owned(original_fstat(fd))))
        self.patches.enter_context(mock.patch.object(configure.os, "stat", side_effect=lambda *a, **kw: root_owned(original_stat(*a, **kw))))
        self.chown = self.patches.enter_context(mock.patch.object(configure.os, "fchown"))

    def run_main(self, name="jalperin", recipient=RECIPIENT):
        output, error = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            result = configure.main(["--name=" + name, "--recipient=" + recipient])
        combined = output.getvalue() + error.getvalue()
        self.assertNotIn(FIXTURE_SECRET, combined)
        return result, combined

    def test_success_writes_only_private_recipient_override_without_reading_primary_or_secret(self):
        before = {path.name: (path.read_bytes(), path.stat()) for path in (self.primary, self.password)}
        with mock.patch.object(configure.os, "open", wraps=os.open) as opened:
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual(self.target.read_text(), f"OJS_PROGRESS_EMAIL_TO={RECIPIENT}\n")
        self.assertEqual(stat.S_IMODE(self.target.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o700)
        self.assertEqual({p.name for p in self.path.iterdir()}, {self.primary.name, self.password.name, self.target.name})
        self.chown.assert_called_once()
        self.assertEqual(self.chown.call_args.args[1:], (0, 0))
        self.assertIn("No email was sent or timer enabled", output)
        for path in (self.primary, self.password):
            content, information = before[path.name]
            self.assertEqual(path.read_bytes(), content)
            self.assertEqual(path.stat(), information)
            self.assertFalse(any(call.args[0] == path.name for call in opened.call_args_list))
        target_call = next(call for call in opened.call_args_list if call.args[0] == self.target.name)
        for flag in (os.O_WRONLY, os.O_CREAT, os.O_EXCL, os.O_NOFOLLOW, os.O_CLOEXEC):
            self.assertTrue(target_call.args[1] & flag)

    def test_non_root_is_refused_before_opening_configuration(self):
        with mock.patch.object(configure.os, "geteuid", return_value=1000), \
                mock.patch.object(configure, "open_config_directory") as opened:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        opened.assert_not_called()
        self.assertFalse(self.target.exists())

    def test_valid_instance_names_have_only_the_requested_filename(self):
        for name in ("a", "a0_-", "a" * 32):
            with self.subTest(name=name):
                result, output = self.run_main(name=name)
                self.assertEqual(result, 0, output)
                target = self.path / f"progress-email-{name}.env"
                self.assertEqual(target.read_text(), f"OJS_PROGRESS_EMAIL_TO={RECIPIENT}\n")
                target.unlink()

    def test_invalid_names_and_addresses_are_rejected_without_echo_or_files(self):
        for name in ("", "../password", "a/b", "a.b", "-a", "1a", "A", "é", "a" * 33, "a\nBad"):
            with self.subTest(name=name), mock.patch.object(configure, "open_config_directory") as opened:
                result, _ = self.run_main(name=name)
                self.assertEqual(result, 1)
                opened.assert_not_called()
        for recipient in ("to@example.org\nBcc: bad@example.org", "$(id)@example.org", "a;b@example.org",
                          "a@example.org b@example.org", "a@example.org\r", "a@localhost"):
            with self.subTest(recipient=recipient), mock.patch.object(configure, "open_config_directory") as opened:
                result, output = self.run_main(recipient=recipient)
                self.assertEqual(result, 1)
                self.assertNotIn(recipient, output)
                opened.assert_not_called()
        self.assertFalse(self.target.exists())

    def test_existing_target_is_never_opened_or_overwritten(self):
        self.target.write_text("existing recipient")
        before = self.target.stat()
        with mock.patch.object(configure.os, "open", wraps=os.open) as opened:
            result, output = self.run_main()
        self.assertEqual(result, 1)
        self.assertIn("refusing to read or overwrite", output)
        self.assertEqual(self.target.read_text(), "existing recipient")
        self.assertEqual(self.target.stat(), before)
        self.assertFalse(any(call.args[0] == self.target.name for call in opened.call_args_list))

    def test_target_symlink_and_fifo_are_refused_without_following_or_blocking(self):
        for kind in ("symlink", "fifo"):
            with self.subTest(kind=kind):
                if kind == "symlink":
                    self.target.symlink_to(self.path / "missing")
                else:
                    os.mkfifo(self.target, 0o600)
                result, _ = self.run_main()
                self.assertEqual(result, 1)
                self.target.unlink()
        self.assertEqual(self.password.read_text(), FIXTURE_SECRET)

    def test_missing_or_nonprivate_primary_is_refused(self):
        self.primary.unlink()
        result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.primary.write_text("fixture")
        self.primary.chmod(0o644)
        result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(stat.S_IMODE(self.primary.stat().st_mode), 0o644)
        self.assertFalse(self.target.exists())

    def test_symlink_fifo_and_directory_primary_are_refused(self):
        self.primary.unlink()
        for kind in ("symlink", "fifo", "directory"):
            with self.subTest(kind=kind):
                if kind == "symlink":
                    self.primary.symlink_to(self.password)
                elif kind == "fifo":
                    os.mkfifo(self.primary, 0o600)
                else:
                    self.primary.mkdir(mode=0o600)
                result, _ = self.run_main()
                self.assertEqual(result, 1)
                self.assertFalse(self.target.exists())
                if kind == "directory":
                    self.primary.rmdir()
                else:
                    self.primary.unlink()

    def test_primary_wrong_owner_or_group_is_refused(self):
        original_stat = os.stat
        for index in (4, 5):
            def wrong_owner(*args, **kwargs):
                information = original_stat(*args, **kwargs)
                if args[0] == email_setup.ENVIRONMENT_FILE:
                    fields = list(information)
                    fields[index] = 1234
                    return os.stat_result(fields)
                return information

            with self.subTest(index=index), mock.patch.object(configure.os, "stat", side_effect=wrong_owner):
                result, _ = self.run_main()
                self.assertEqual(result, 1)
        self.assertFalse(self.target.exists())

    def test_unsafe_directory_permissions_are_not_changed(self):
        self.path.chmod(0o755)
        result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o755)
        self.chown.assert_not_called()
        self.assertFalse(self.target.exists())

    def test_symlink_directory_and_ancestor_are_refused(self):
        linked = Path(self.temporary.name) / "linked"
        linked.symlink_to(self.path, target_is_directory=True)
        parent_link = Path(self.temporary.name) / "parent-link"
        parent_link.symlink_to(Path(self.temporary.name), target_is_directory=True)
        for path in (linked, parent_link / "ojs-api"):
            with self.subTest(path=path), mock.patch.object(email_setup, "CONFIG_DIR", path):
                result, _ = self.run_main()
                self.assertEqual(result, 1)
        self.assertFalse(self.target.exists())

    def test_exclusive_create_defeats_target_creation_race(self):
        original_open = os.open

        def competing_open(name, *args, **kwargs):
            if name == self.target.name:
                self.target.symlink_to(self.password)
            return original_open(name, *args, **kwargs)

        with mock.patch.object(configure.os, "open", side_effect=competing_open):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertTrue(self.target.is_symlink())
        self.assertEqual(self.password.read_text(), FIXTURE_SECRET)

    def test_permissions_are_set_before_writing_and_files_are_synced(self):
        actions = []
        original_chmod, original_write, original_sync = os.fchmod, os.write, os.fsync
        with mock.patch.object(configure.os, "fchmod", side_effect=lambda *a: (actions.append("chmod"), original_chmod(*a))[1]), \
                mock.patch.object(configure.os, "write", side_effect=lambda *a: (actions.append("write"), original_write(*a))[1]), \
                mock.patch.object(configure.os, "fsync", side_effect=lambda *a: (actions.append("sync"), original_sync(*a))[1]):
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual(actions, ["chmod", "write", "sync", "sync"])

    def test_failed_or_zero_write_removes_only_new_recipient_and_hides_exception(self):
        for failure in (OSError(FIXTURE_SECRET), 0):
            with self.subTest(failure=type(failure).__name__), \
                    mock.patch.object(configure.os, "write", **({"side_effect": failure} if isinstance(failure, Exception) else {"return_value": failure})):
                result, _ = self.run_main()
            self.assertEqual(result, 1)
            self.assertFalse(self.target.exists())
            self.assertTrue(self.primary.exists())
            self.assertEqual(self.password.read_text(), FIXTURE_SECRET)

    def test_failed_sync_removes_new_recipient(self):
        with mock.patch.object(configure.os, "fsync", side_effect=OSError(FIXTURE_SECRET)):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertFalse(self.target.exists())

    def test_short_writes_are_completed_without_truncation(self):
        original_write = os.write
        with mock.patch.object(configure.os, "write", side_effect=lambda fd, data: original_write(fd, data[:4])):
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual(self.target.read_text(), f"OJS_PROGRESS_EMAIL_TO={RECIPIENT}\n")

    def test_ownership_or_mode_failure_rolls_back_before_writing(self):
        for operation in ("fchown", "fchmod"):
            with self.subTest(operation=operation), \
                    mock.patch.object(configure.os, operation, side_effect=OSError(FIXTURE_SECRET)), \
                    mock.patch.object(configure.os, "write") as write:
                result, _ = self.run_main()
                self.assertEqual(result, 1)
                write.assert_not_called()
                self.assertFalse(self.target.exists())

    def test_rollback_does_not_remove_replacement_inode(self):
        def replace_then_fail(*args):
            self.target.unlink()
            self.target.symlink_to(self.password)
            raise OSError(FIXTURE_SECRET)

        with mock.patch.object(configure.os, "write", side_effect=replace_then_fail):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertTrue(self.target.is_symlink())
        self.assertEqual(self.password.read_text(), FIXTURE_SECRET)

    def test_process_environment_and_umask_are_preserved(self):
        previous = os.umask(0o027)
        try:
            with mock.patch.dict(os.environ, {"OJS_PROGRESS_EMAIL_TO": "ignored@example.org"}, clear=True):
                before = dict(os.environ)
                result, output = self.run_main()
                self.assertEqual(result, 0, output)
                self.assertEqual(dict(os.environ), before)
            self.assertEqual(os.umask(0o027), 0o027)
        finally:
            os.umask(previous)


if __name__ == "__main__":
    unittest.main()
