from __future__ import annotations

from contextlib import ExitStack, redirect_stderr, redirect_stdout
import getpass
import io
import os
from pathlib import Path
import stat
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import configure_progress_email as configure


PASSWORD = "abcdefghijklmnop"
ARGUMENTS = ["--sender", "charlierahal@gmail.com", "--recipient", "charlierahal@gmail.com"]


class ConfigureProgressEmailTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "ojs-api"
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.object(configure, "CONFIG_DIR", self.path))
        self.patches.enter_context(mock.patch.object(configure.os, "geteuid", return_value=0))
        # Exercise real descriptor operations as an ordinary test user, simulating root ownership.
        original_fstat = os.fstat

        def root_stat(descriptor):
            information = list(original_fstat(descriptor))
            information[4:6] = [0, 0]
            return os.stat_result(information)

        self.patches.enter_context(mock.patch.object(configure.os, "fstat", side_effect=root_stat))
        self.chown = self.patches.enter_context(mock.patch.object(configure.os, "fchown"))

    def run_main(self):
        output, error = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            result = configure.main(ARGUMENTS)
        combined = output.getvalue() + error.getvalue()
        self.assertNotIn(PASSWORD, combined)
        return result, combined

    def test_success_writes_only_private_expected_files(self):
        with mock.patch.object(configure, "read_password", return_value=PASSWORD):
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual({path.name for path in self.path.iterdir()},
                         {configure.PASSWORD_FILE, configure.ENVIRONMENT_FILE})
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o700)
        for path in self.path.iterdir():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual((self.path / configure.PASSWORD_FILE).read_text(), PASSWORD + "\n")
        environment = (self.path / configure.ENVIRONMENT_FILE).read_text()
        self.assertEqual(environment, "OJS_PROGRESS_EMAIL_FROM=charlierahal@gmail.com\n"
                                     "OJS_PROGRESS_EMAIL_TO=charlierahal@gmail.com\n")
        self.assertNotIn(PASSWORD, environment)
        self.assertEqual(self.chown.call_count, 3)
        for call in self.chown.call_args_list:
            self.assertEqual(call.args[1:], (0, 0))

    def test_non_root_refused_before_prompt_or_files(self):
        with mock.patch.object(configure.os, "geteuid", return_value=1000), \
                mock.patch.object(configure, "read_password") as prompt:
            result, output = self.run_main()
        self.assertEqual(result, 1)
        self.assertIn("root", output)
        prompt.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_existing_targets_never_read_or_overwritten(self):
        for name in (configure.PASSWORD_FILE, configure.ENVIRONMENT_FILE):
            with self.subTest(name=name):
                self.path.mkdir(mode=0o700, exist_ok=True)
                target = self.path / name
                target.write_text("existing private material")
                with mock.patch.object(configure, "read_password") as prompt:
                    result, output = self.run_main()
                self.assertEqual(result, 1)
                self.assertIn("refusing to read or overwrite", output)
                prompt.assert_not_called()
                self.assertEqual(target.read_text(), "existing private material")
                self.assertEqual(list(self.path.iterdir()), [target])
                target.unlink()

    def test_dangling_symlink_target_is_refused(self):
        self.path.mkdir(mode=0o700)
        target = self.path / configure.PASSWORD_FILE
        target.symlink_to(self.path / "absent")
        with mock.patch.object(configure, "read_password") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertTrue(target.is_symlink())
        prompt.assert_not_called()

    def test_symlink_directory_is_refused_without_changing_referent(self):
        destination = Path(self.temporary.name) / "other"
        destination.mkdir(mode=0o755)
        self.path.symlink_to(destination, target_is_directory=True)
        with mock.patch.object(configure, "read_password") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o755)
        self.assertEqual(list(destination.iterdir()), [])
        self.chown.assert_not_called()
        prompt.assert_not_called()

    def test_symlink_ancestor_is_refused(self):
        actual = Path(self.temporary.name) / "actual"
        actual.mkdir()
        ancestor = Path(self.temporary.name) / "linked"
        ancestor.symlink_to(actual, target_is_directory=True)
        with mock.patch.object(configure, "CONFIG_DIR", ancestor / "ojs-api"), \
                mock.patch.object(configure, "read_password") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(list(actual.iterdir()), [])
        prompt.assert_not_called()

    def test_existing_insecure_directory_is_not_chmodded(self):
        self.path.mkdir(mode=0o755)
        with mock.patch.object(configure, "read_password") as prompt:
            result, output = self.run_main()
        self.assertEqual(result, 1)
        self.assertIn("0700", output)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o755)
        self.chown.assert_not_called()
        prompt.assert_not_called()

    def test_wrong_directory_owner_is_refused(self):
        self.path.mkdir(mode=0o700)
        information = list(self.path.stat())
        information[4] = 1234
        with mock.patch.object(configure.os, "fstat", return_value=os.stat_result(information)), \
                mock.patch.object(configure, "read_password") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.chown.assert_not_called()
        prompt.assert_not_called()

    def test_broad_relative_and_traversing_directories_are_refused(self):
        for path in (Path("/"), Path("/etc"), Path("/tmp"), Path("ojs-api"),
                     Path("/etc/../ojs-api"), Path("/ojs-api")):
            with self.subTest(path=path), mock.patch.object(configure, "CONFIG_DIR", path), \
                    mock.patch.object(configure.os, "open") as opened:
                with self.assertRaises(configure.ConfigurationError):
                    configure.open_config_directory()
                opened.assert_not_called()

    def test_header_and_shell_injection_addresses_are_refused(self):
        for address in ("x@gmail.com\nBcc:other@example.com", "x@gmail.com\r", " x@gmail.com",
                        "$(echo unsafe)@gmail.com", "x@gmail.com;id", "x`id`@gmail.com",
                        '"x"@gmail.com', "Name <x@gmail.com>", "x@gmail.com\x00",
                        "x@gmail.com\nOJS_PROGRESS_EMAIL_PASSWORD=oops"):
            with self.subTest(address=address):
                with self.assertRaises(configure.ConfigurationError) as error:
                    configure.validate_address(address)
                self.assertNotIn(address, str(error.exception))

    def test_invalid_cli_address_fails_before_prompt(self):
        with mock.patch.object(configure, "read_password") as prompt, \
                mock.patch(__name__ + ".ARGUMENTS", ["--sender", "bad;address", "--recipient", "a@gmail.com"]):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertFalse(self.path.exists())
        prompt.assert_not_called()

    def terminal(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(mock.patch.object(configure.sys, "stdin", mock.Mock(isatty=lambda: True)))
        stack.enter_context(mock.patch.object(configure.os, "open", return_value=123))
        stream = mock.MagicMock()
        stream.__enter__.return_value = stream
        stream.isatty.return_value = True
        stack.enter_context(mock.patch.object(configure.os, "fdopen", return_value=stream))
        return stream

    def test_non_tty_input_is_refused_without_prompt(self):
        with mock.patch.object(configure.sys, "stdin", io.StringIO(PASSWORD)), \
                mock.patch.object(configure.getpass, "getpass") as prompt:
            with self.assertRaises(configure.ConfigurationError):
                configure.read_password()
        prompt.assert_not_called()

    def test_controlling_terminal_is_required(self):
        with mock.patch.object(configure.sys, "stdin", mock.Mock(isatty=lambda: True)), \
                mock.patch.object(configure.os, "open", side_effect=OSError(PASSWORD)), \
                mock.patch.object(configure.getpass, "getpass") as prompt:
            with self.assertRaises(configure.ConfigurationError) as error:
                configure.read_password()
        self.assertNotIn(PASSWORD, str(error.exception))
        prompt.assert_not_called()

    def test_non_tty_controlling_terminal_is_refused(self):
        stream = self.terminal()
        stream.isatty.return_value = False
        with mock.patch.object(configure.getpass, "getpass") as prompt:
            with self.assertRaises(configure.ConfigurationError):
                configure.read_password()
        prompt.assert_not_called()

    def test_getpass_fallback_cannot_echo_or_read_input(self):
        self.terminal()
        # The real fallback warns before its first read; promoting that warning must stop it.
        with mock.patch.object(configure.getpass, "getpass", side_effect=getpass.fallback_getpass), \
                mock.patch.object(getpass, "_raw_input") as raw_input:
            with self.assertRaises(configure.ConfigurationError):
                configure.read_password()
        raw_input.assert_not_called()

    def test_google_grouping_spaces_are_removed(self):
        self.terminal()
        with mock.patch.object(configure.getpass, "getpass", return_value="abcd efgh ijkl mnop"):
            self.assertEqual(configure.read_password(), PASSWORD)

    def test_invalid_passwords_are_refused_without_repeating_input(self):
        self.terminal()
        for password in ("short", PASSWORD + "q", PASSWORD[:-1] + "0", PASSWORD[:-1] + "é",
                         PASSWORD + "\n", "abcd\tefghijklmnop"):
            with self.subTest(password=password), \
                    mock.patch.object(configure.getpass, "getpass", return_value=password):
                with self.assertRaises(configure.ConfigurationError) as error:
                    configure.read_password()
                self.assertNotIn(password, str(error.exception))

    def test_creation_race_preserves_competing_file_and_cleans_own_file(self):
        self.path.mkdir(mode=0o700)
        directory = os.open(self.path, configure.DIRECTORY_FLAGS)
        self.addCleanup(os.close, directory)
        original_open = os.open

        def raced_open(name, flags, *args, **kwargs):
            if name == configure.ENVIRONMENT_FILE:
                (self.path / name).write_text("competing configuration")
            return original_open(name, flags, *args, **kwargs)

        with mock.patch.object(configure.os, "open", side_effect=raced_open):
            with self.assertRaises(FileExistsError):
                configure.store_credentials(directory, PASSWORD, "a@gmail.com", "a@gmail.com")
        self.assertFalse((self.path / configure.PASSWORD_FILE).exists())
        self.assertEqual((self.path / configure.ENVIRONMENT_FILE).read_text(), "competing configuration")

    def test_write_failure_cleans_created_files_and_hides_exception_input(self):
        with mock.patch.object(configure, "read_password", return_value=PASSWORD), \
                mock.patch.object(configure.os, "write", side_effect=OSError(PASSWORD)):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(list(self.path.iterdir()), [])

    def test_permissions_are_set_before_any_secret_write_and_files_are_synced(self):
        original_chmod, original_write, original_fsync = os.fchmod, os.write, os.fsync
        private = set()
        writes = []
        synced = []

        def chmod(descriptor, mode):
            original_chmod(descriptor, mode)
            if mode == 0o600:
                private.add(descriptor)

        def write(descriptor, data):
            self.assertIn(descriptor, private)
            self.assertEqual(len(private), 2)
            writes.append(descriptor)
            return original_write(descriptor, data)

        def fsync(descriptor):
            synced.append(descriptor)
            return original_fsync(descriptor)

        with mock.patch.object(configure, "read_password", return_value=PASSWORD), \
                mock.patch.object(configure.os, "fchmod", side_effect=chmod), \
                mock.patch.object(configure.os, "write", side_effect=write), \
                mock.patch.object(configure.os, "fsync", side_effect=fsync):
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual(len(writes), 2)
        self.assertTrue(set(writes).issubset(synced))
        self.assertGreaterEqual(len(synced), 3)


if __name__ == "__main__":
    unittest.main()
