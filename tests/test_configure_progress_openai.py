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
import configure_progress_email as email_setup
import configure_progress_openai as configure


KEY = "sk-proj-test_only_" + "aB12_-" * 12


class ConfigureProgressOpenAITest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "ojs-api"
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.object(email_setup, "CONFIG_DIR", self.path))
        self.patches.enter_context(mock.patch.object(configure.os, "geteuid", return_value=0))
        original_fstat = os.fstat

        def root_stat(descriptor):
            information = list(original_fstat(descriptor))
            information[4:6] = [0, 0]
            return os.stat_result(information)

        self.patches.enter_context(mock.patch.object(configure.os, "fstat", side_effect=root_stat))
        self.chown = self.patches.enter_context(mock.patch.object(configure.os, "fchown"))

    def run_main(self, arguments=None):
        output, error = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            result = configure.main([] if arguments is None else arguments)
        combined = output.getvalue() + error.getvalue()
        self.assertNotIn(KEY, combined)
        return result, combined

    def test_success_stores_one_private_key(self):
        with mock.patch.object(configure, "read_key", return_value=KEY):
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o700)
        target = self.path / configure.KEY_FILE
        self.assertEqual(list(self.path.iterdir()), [target])
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        self.assertEqual(target.read_text(), KEY + "\n")
        self.assertEqual(self.chown.call_count, 2)
        for call in self.chown.call_args_list:
            self.assertEqual(call.args[1:], (0, 0))

    def test_gmail_credentials_and_environment_are_unchanged(self):
        self.path.mkdir(mode=0o700)
        credentials = [self.path / email_setup.PASSWORD_FILE, self.path / email_setup.ENVIRONMENT_FILE]
        for path in credentials:
            path.write_text("fixture material")
            path.chmod(0o600)
        before = [path.stat() for path in credentials]
        with mock.patch.object(configure, "read_key", return_value=KEY), \
                mock.patch.dict(os.environ, {"OPENAI_API_KEY": "ignored-environment-value"}, clear=True):
            environment_before = dict(os.environ)
            result, output = self.run_main()
            self.assertEqual(dict(os.environ), environment_before)
        self.assertEqual(result, 0, output)
        for path, information in zip(credentials, before):
            after = path.stat()
            self.assertEqual((after.st_ino, after.st_size, after.st_mtime_ns, after.st_mode),
                             (information.st_ino, information.st_size, information.st_mtime_ns, information.st_mode))
            self.assertEqual(path.read_text(), "fixture material")
        self.assertEqual((self.path / configure.KEY_FILE).read_text(), KEY + "\n")

    def test_non_root_is_refused_before_prompt_or_files(self):
        with mock.patch.object(configure.os, "geteuid", return_value=1000), \
                mock.patch.object(configure, "read_key") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        prompt.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_existing_key_is_not_opened_read_or_overwritten(self):
        self.path.mkdir(mode=0o700)
        target = self.path / configure.KEY_FILE
        target.write_text("existing fixture key")
        with mock.patch.object(configure, "read_key") as prompt, \
                mock.patch.object(configure.os, "open", wraps=os.open) as opened:
            result, output = self.run_main()
        self.assertEqual(result, 1)
        self.assertIn("refusing to read or overwrite", output)
        prompt.assert_not_called()
        self.assertFalse(any(call.args[0] == configure.KEY_FILE for call in opened.call_args_list))
        self.assertEqual(target.read_text(), "existing fixture key")

    def test_existing_dangling_symlink_key_is_refused(self):
        self.path.mkdir(mode=0o700)
        target = self.path / configure.KEY_FILE
        target.symlink_to(self.path / "absent")
        with mock.patch.object(configure, "read_key") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertTrue(target.is_symlink())
        prompt.assert_not_called()

    def test_symlink_ancestor_is_refused(self):
        actual = Path(self.temporary.name) / "actual"
        actual.mkdir()
        link = Path(self.temporary.name) / "link"
        link.symlink_to(actual, target_is_directory=True)
        with mock.patch.object(email_setup, "CONFIG_DIR", link / "ojs-api"), \
                mock.patch.object(configure, "read_key") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(list(actual.iterdir()), [])
        prompt.assert_not_called()

    def test_symlink_final_directory_is_refused_without_modification(self):
        actual = Path(self.temporary.name) / "actual"
        actual.mkdir(mode=0o755)
        self.path.symlink_to(actual, target_is_directory=True)
        with mock.patch.object(configure, "read_key") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(stat.S_IMODE(actual.stat().st_mode), 0o755)
        self.assertEqual(list(actual.iterdir()), [])
        self.chown.assert_not_called()
        prompt.assert_not_called()

    def test_insecure_existing_directory_is_not_chmodded(self):
        self.path.mkdir(mode=0o755)
        with mock.patch.object(configure, "read_key") as prompt:
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
                mock.patch.object(configure, "read_key") as prompt:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.chown.assert_not_called()
        prompt.assert_not_called()

    def test_cli_never_accepts_or_repeats_key_arguments(self):
        with mock.patch.object(configure, "read_key") as prompt:
            for arguments in ([KEY], ["--key", KEY], ["--key=" + KEY], ["--config-dir", "/tmp"]):
                with self.subTest(arguments=arguments):
                    result, output = self.run_main(arguments)
                    self.assertEqual(result, 2)
                    self.assertIn("takes no arguments", output)
        prompt.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_help_needs_no_root_prompt_or_files(self):
        with mock.patch.object(configure.os, "geteuid", return_value=1000), \
                mock.patch.object(configure, "read_key") as prompt:
            result, output = self.run_main(["--help"])
        self.assertEqual(result, 0)
        self.assertIn("hidden terminal prompt", output)
        prompt.assert_not_called()
        self.assertFalse(self.path.exists())

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

    def test_non_tty_stdin_is_refused_without_prompt(self):
        with mock.patch.object(configure.sys, "stdin", io.StringIO(KEY)), \
                mock.patch.object(configure.getpass, "getpass") as prompt:
            with self.assertRaises(configure.ConfigurationError):
                configure.read_key()
        prompt.assert_not_called()

    def test_controlling_terminal_is_required_without_echoing_errors(self):
        with mock.patch.object(configure.sys, "stdin", mock.Mock(isatty=lambda: True)), \
                mock.patch.object(configure.os, "open", side_effect=OSError(KEY)), \
                mock.patch.object(configure.getpass, "getpass") as prompt:
            with self.assertRaises(configure.ConfigurationError) as error:
                configure.read_key()
        self.assertNotIn(KEY, str(error.exception))
        prompt.assert_not_called()

    def test_non_tty_controlling_terminal_is_refused(self):
        stream = self.terminal()
        stream.isatty.return_value = False
        with mock.patch.object(configure.getpass, "getpass") as prompt:
            with self.assertRaises(configure.ConfigurationError):
                configure.read_key()
        prompt.assert_not_called()

    def test_getpass_fallback_cannot_echo_or_read_input(self):
        self.terminal()
        with mock.patch.object(configure.getpass, "getpass", side_effect=getpass.fallback_getpass), \
                mock.patch.object(getpass, "_raw_input") as raw_input:
            with self.assertRaises(configure.ConfigurationError):
                configure.read_key()
        raw_input.assert_not_called()

    def test_hidden_prompt_and_flexible_lengths_preserve_key_exactly(self):
        stream = self.terminal()
        for key in ("sk-" + "a" * 29, KEY, "sk-" + "Z9_-" * 127 + "x"):
            with self.subTest(length=len(key)), \
                    mock.patch.object(configure.getpass, "getpass", return_value=key) as prompt:
                self.assertEqual(configure.read_key(), key)
                prompt.assert_called_once_with("OpenAI project API key (hidden): ", stream=stream)

    def test_invalid_key_shape_whitespace_and_length_are_refused_without_echo(self):
        self.terminal()
        keys = ("sk-" + "a" * 28, "sk-" + "a" * 510, "not-sk-" + "a" * 40,
                " " + KEY, KEY + " ", KEY + "\n", KEY + "\r", KEY + "\t", KEY + "é",
                KEY + ";", KEY + "$(id)", KEY + "\x00", KEY.replace("_", " ", 1))
        for key in keys:
            with self.subTest(length=len(key)), \
                    mock.patch.object(configure.getpass, "getpass", return_value=key):
                with self.assertRaises(configure.ConfigurationError) as error:
                    configure.read_key()
                self.assertNotIn(key, str(error.exception))

    def test_key_created_after_preflight_is_preserved(self):
        def race():
            (self.path / configure.KEY_FILE).write_text("competing fixture")
            return KEY

        with mock.patch.object(configure, "read_key", side_effect=race):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual((self.path / configure.KEY_FILE).read_text(), "competing fixture")

    def test_symlink_created_after_preflight_is_preserved_and_not_followed(self):
        other = Path(self.temporary.name) / "other"
        other.write_text("unrelated fixture")

        def race():
            (self.path / configure.KEY_FILE).symlink_to(other)
            return KEY

        with mock.patch.object(configure, "read_key", side_effect=race):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertTrue((self.path / configure.KEY_FILE).is_symlink())
        self.assertEqual(other.read_text(), "unrelated fixture")

    def test_partial_write_failure_removes_only_own_key(self):
        self.path.mkdir(mode=0o700)
        gmail = self.path / email_setup.PASSWORD_FILE
        gmail.write_text("gmail fixture")
        original_write = os.write
        calls = 0

        def partial_write(descriptor, payload):
            nonlocal calls
            calls += 1
            if calls == 1:
                return original_write(descriptor, payload[:5])
            raise OSError(KEY)

        with mock.patch.object(configure, "read_key", return_value=KEY), \
                mock.patch.object(configure.os, "write", side_effect=partial_write):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(list(self.path.iterdir()), [gmail])
        self.assertEqual(gmail.read_text(), "gmail fixture")

    def test_failure_does_not_unlink_replacement_inode(self):
        replacement = self.path / configure.KEY_FILE

        def replace_then_fail(descriptor, payload):
            replacement.unlink()
            replacement.write_text("replacement fixture")
            raise OSError(KEY)

        with mock.patch.object(configure, "read_key", return_value=KEY), \
                mock.patch.object(configure.os, "write", side_effect=replace_then_fail):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(replacement.read_text(), "replacement fixture")

    def test_permission_failure_removes_created_file_before_any_write(self):
        self.path.mkdir(mode=0o700)
        with mock.patch.object(configure, "read_key", return_value=KEY), \
                mock.patch.object(configure.os, "fchmod", side_effect=OSError(KEY)), \
                mock.patch.object(configure.os, "write") as write:
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(list(self.path.iterdir()), [])
        write.assert_not_called()

    def test_fsync_failure_removes_created_key(self):
        self.path.mkdir(mode=0o700)
        with mock.patch.object(configure, "read_key", return_value=KEY), \
                mock.patch.object(configure.os, "fsync", side_effect=OSError(KEY)):
            result, _ = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(list(self.path.iterdir()), [])

    def test_permissions_precede_writes_and_key_and_directory_are_synced(self):
        self.path.mkdir(mode=0o700)
        original_chmod, original_write, original_sync = os.fchmod, os.write, os.fsync
        private, written, synced = set(), [], []

        def chmod(descriptor, mode):
            original_chmod(descriptor, mode)
            if mode == 0o600:
                private.add(descriptor)

        def write(descriptor, payload):
            self.assertIn(descriptor, private)
            written.append(descriptor)
            return original_write(descriptor, payload)

        def fsync(descriptor):
            synced.append(descriptor)
            return original_sync(descriptor)

        with mock.patch.object(configure, "read_key", return_value=KEY), \
                mock.patch.object(configure.os, "fchmod", side_effect=chmod), \
                mock.patch.object(configure.os, "write", side_effect=write), \
                mock.patch.object(configure.os, "fsync", side_effect=fsync):
            result, output = self.run_main()
        self.assertEqual(result, 0, output)
        self.assertEqual(len(written), 1)
        self.assertEqual(synced[0], written[0])
        self.assertEqual(len(synced), 2)
        self.assertNotEqual(synced[0], synced[1])


if __name__ == "__main__":
    unittest.main()
