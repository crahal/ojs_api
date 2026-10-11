#!/usr/bin/env python3
"""Interactively install private Gmail credentials; never send mail or enable units."""
from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path
import re
import stat
import sys
import warnings


CONFIG_DIR = Path("/etc/ojs-api")
PASSWORD_FILE = "progress-email-password"
ENVIRONMENT_FILE = "progress-email.env"
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
ADDRESS = re.compile(
    r"[A-Za-z0-9_+-]+(?:\.[A-Za-z0-9_+-]+)*@"
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}"
)


class ConfigurationError(Exception):
    """A safe, fixed diagnostic that never contains input or credentials."""


def validate_address(value: str) -> str:
    if (len(value) > 254 or len(value.split("@", 1)[0]) > 64
            or ADDRESS.fullmatch(value) is None):
        raise ConfigurationError("Sender and recipient must be plain email addresses.")
    return value


def open_config_directory() -> int:
    """Pin every path component, rejecting symlinks and unsafe existing directories."""
    path = CONFIG_DIR
    if (not path.is_absolute() or len(path.parts) < 3 or path.name != "ojs-api"
            or ".." in path.parts):
        raise ConfigurationError("Refusing an unsafe configuration directory.")
    parent = os.open("/", DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, DIRECTORY_FLAGS, dir_fd=parent)
            os.close(parent)
            parent = child
        created = False
        try:
            os.mkdir(path.name, mode=0o700, dir_fd=parent)
            created = True
        except FileExistsError:
            pass
        directory = os.open(path.name, DIRECTORY_FLAGS, dir_fd=parent)
        try:
            information = os.fstat(directory)
            if information.st_uid != 0:
                raise ConfigurationError("Configuration directory must be owned by root.")
            if created:
                os.fchown(directory, 0, 0)
                os.fchmod(directory, 0o700)
                os.fsync(parent)
                information = os.fstat(directory)
            if information.st_gid != 0 or stat.S_IMODE(information.st_mode) != 0o700:
                raise ConfigurationError("Existing configuration directory must be root:root mode 0700.")
            return directory
        except BaseException:
            os.close(directory)
            raise
    finally:
        os.close(parent)


def check_absent(directory: int) -> None:
    for name in (PASSWORD_FILE, ENVIRONMENT_FILE):
        try:
            os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise ConfigurationError("Credential files already exist; refusing to read or overwrite them.")


def read_password() -> str:
    if not sys.stdin.isatty():
        raise ConfigurationError("Run this command in an interactive terminal; piped input is refused.")
    try:
        terminal = os.open("/dev/tty", os.O_RDWR | os.O_NOCTTY | os.O_CLOEXEC)
        with os.fdopen(terminal, "w", encoding="utf-8") as stream:
            if not stream.isatty():
                raise ConfigurationError("A real interactive terminal is required.")
            with warnings.catch_warnings():
                warnings.simplefilter("error", getpass.GetPassWarning)
                password = getpass.getpass("Gmail app password (hidden): ", stream=stream)
    except (OSError, EOFError, getpass.GetPassWarning):
        raise ConfigurationError("Secure terminal input is unavailable; no credential was saved.") from None
    password = password.replace(" ", "")
    if re.fullmatch(r"[A-Za-z]{16}", password) is None:
        raise ConfigurationError("A Gmail app password must contain exactly 16 ASCII letters.")
    return password


def store_credentials(directory: int, password: str, sender: str, recipient: str) -> None:
    records: list[tuple[str, int]] = []
    payloads = (
        (PASSWORD_FILE, (password + "\n").encode("ascii")),
        (ENVIRONMENT_FILE,
         f"OJS_PROGRESS_EMAIL_FROM={sender}\nOJS_PROGRESS_EMAIL_TO={recipient}\n".encode("ascii")),
    )
    try:
        # Reserve both destinations before writing either. Never follow or replace a target.
        for name, _ in payloads:
            descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                                 | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
            records.append((name, descriptor))
            os.fchown(descriptor, 0, 0)
            os.fchmod(descriptor, 0o600)
        for (_, descriptor), (_, payload) in zip(records, payloads):
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError("Could not write private configuration.")
                remaining = remaining[written:]
            os.fsync(descriptor)
        os.fsync(directory)
    except BaseException:
        for name, descriptor in records:
            try:
                existing = os.stat(name, dir_fd=directory, follow_symlinks=False)
                created = os.fstat(descriptor)
                if (existing.st_dev, existing.st_ino) == (created.st_dev, created.st_ino):
                    os.unlink(name, dir_fd=directory)
            except OSError:
                pass
        raise
    finally:
        for _, descriptor in records:
            os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sender", required=True)
    parser.add_argument("--recipient", required=True)
    args = parser.parse_args(argv)
    previous_umask = os.umask(0o077)
    directory = None
    try:
        if os.geteuid() != 0:
            raise ConfigurationError("Run this configuration command as root with sudo.")
        sender = validate_address(args.sender)
        recipient = validate_address(args.recipient)
        directory = open_config_directory()
        check_absent(directory)
        password = read_password()
        store_credentials(directory, password, sender, recipient)
        print("Saved private progress email configuration. No email was sent or timer enabled.")
        return 0
    except ConfigurationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except (Exception, KeyboardInterrupt):
        # Never include an exception message: a terminal or I/O failure may contain input.
        print("error: Progress email configuration failed; no existing credentials were overwritten.",
              file=sys.stderr)
        return 1
    finally:
        if directory is not None:
            os.close(directory)
        os.umask(previous_umask)


if __name__ == "__main__":
    raise SystemExit(main())
