#!/usr/bin/env python3
"""Interactively store a private OpenAI project API key; no API requests or unit changes."""
from __future__ import annotations

import getpass
import os
import re
import sys
import warnings

from configure_progress_email import ConfigurationError, open_config_directory


KEY_FILE = "progress-openai-key"


def check_absent(directory: int) -> None:
    try:
        os.stat(KEY_FILE, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise ConfigurationError("OpenAI credential already exists; refusing to read or overwrite it.")


def validate_key(key: str) -> str:
    if not 32 <= len(key) <= 512 or re.fullmatch(r"sk-[A-Za-z0-9_-]+", key) is None:
        raise ConfigurationError("Enter a complete OpenAI API key beginning with sk-, without whitespace.")
    return key


def read_key() -> str:
    # Match the Gmail helper's terminal-only input and fail before getpass can echo a fallback.
    if not sys.stdin.isatty():
        raise ConfigurationError("Run this command in an interactive terminal; piped input is refused.")
    try:
        terminal = os.open("/dev/tty", os.O_RDWR | os.O_NOCTTY | os.O_CLOEXEC)
        with os.fdopen(terminal, "w", encoding="utf-8") as stream:
            if not stream.isatty():
                raise ConfigurationError("A real interactive terminal is required.")
            with warnings.catch_warnings():
                warnings.simplefilter("error", getpass.GetPassWarning)
                key = getpass.getpass("OpenAI project API key (hidden): ", stream=stream)
    except (OSError, EOFError, getpass.GetPassWarning):
        raise ConfigurationError("Secure terminal input is unavailable; no credential was saved.") from None
    return validate_key(key)


def store_key(directory: int, key: str) -> None:
    payload = memoryview((validate_key(key) + "\n").encode("ascii"))
    descriptor = None
    try:
        descriptor = os.open(KEY_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o600)
        while payload:
            written = os.write(descriptor, payload)
            if written <= 0:
                raise OSError("Could not write private configuration.")
            payload = payload[written:]
        os.fsync(descriptor)
        os.fsync(directory)
    except BaseException:
        if descriptor is not None:
            try:
                existing = os.stat(KEY_FILE, dir_fd=directory, follow_symlinks=False)
                created = os.fstat(descriptor)
                if (existing.st_dev, existing.st_ino) == (created.st_dev, created.st_ino):
                    os.unlink(KEY_FILE, dir_fd=directory)
                    os.fsync(directory)
            except OSError:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if arguments in (["--help"], ["-h"]):
        print(__doc__ + "\nRun with sudo and no arguments; the key is requested through a hidden terminal prompt.")
        return 0
    if arguments:
        # Never repeat unexpected arguments, which could contain a mistakenly pasted key.
        print("error: This command takes no arguments. Enter the key only at its hidden prompt.", file=sys.stderr)
        return 2
    previous_umask = os.umask(0o077)
    directory = None
    try:
        if os.geteuid() != 0:
            raise ConfigurationError("Run this configuration command as root with sudo.")
        directory = open_config_directory()
        check_absent(directory)
        key = read_key()
        store_key(directory, key)
        print("Saved private OpenAI credential. No API request was made or service configuration changed.")
        return 0
    except ConfigurationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except (Exception, KeyboardInterrupt):
        print("error: OpenAI credential configuration failed; no existing credentials were overwritten.",
              file=sys.stderr)
        return 1
    finally:
        if directory is not None:
            os.close(directory)
        os.umask(previous_umask)


if __name__ == "__main__":
    raise SystemExit(main())
