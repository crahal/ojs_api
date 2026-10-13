#!/usr/bin/env python3
"""Add one private progress-email recipient override; never send mail or enable units."""
from __future__ import annotations

import argparse
import os
import re
import stat
import sys

from configure_progress_email import (
    ConfigurationError, ENVIRONMENT_FILE, open_config_directory, validate_address,
)


def validate_name(value: str) -> str:
    if re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", value) is None:
        raise ConfigurationError("Recipient name must be 1–32 lowercase letters, digits, underscores or hyphens, starting with a letter.")
    return value


def check_primary_configuration(directory: int) -> None:
    try:
        information = os.stat(ENVIRONMENT_FILE, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        raise ConfigurationError("Configure the primary progress email account first.") from None
    if (not stat.S_ISREG(information.st_mode) or information.st_uid != 0
            or information.st_gid != 0 or stat.S_IMODE(information.st_mode) != 0o600):
        raise ConfigurationError("Primary progress email configuration must be a regular root:root mode 0600 file.")


def store_recipient(directory: int, name: str, recipient: str) -> None:
    target = f"progress-email-{validate_name(name)}.env"
    payload = memoryview(f"OJS_PROGRESS_EMAIL_TO={validate_address(recipient)}\n".encode("ascii"))
    check_primary_configuration(directory)
    try:
        os.stat(target, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        raise ConfigurationError("Recipient configuration already exists; refusing to read or overwrite it.")
    descriptor = None
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o600)
        while payload:
            written = os.write(descriptor, payload)
            if written <= 0:
                raise OSError("Could not write recipient configuration.")
            payload = payload[written:]
        os.fsync(descriptor)
        os.fsync(directory)
    except BaseException:
        if descriptor is not None:
            try:
                existing = os.stat(target, dir_fd=directory, follow_symlinks=False)
                created = os.fstat(descriptor)
                if (existing.st_dev, existing.st_ino) == (created.st_dev, created.st_ino):
                    os.unlink(target, dir_fd=directory)
                    os.fsync(directory)
            except OSError:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--recipient", required=True)
    args = parser.parse_args(argv)
    previous_umask = os.umask(0o077)
    directory = None
    try:
        if os.geteuid() != 0:
            raise ConfigurationError("Run this configuration command as root with sudo.")
        name = validate_name(args.name)
        recipient = validate_address(args.recipient)
        directory = open_config_directory()
        store_recipient(directory, name, recipient)
        print("Saved private recipient configuration. No email was sent or timer enabled.")
        return 0
    except ConfigurationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except (Exception, KeyboardInterrupt):
        print("error: Recipient configuration failed; no existing files were overwritten.", file=sys.stderr)
        return 1
    finally:
        if directory is not None:
            os.close(directory)
        os.umask(previous_umask)


if __name__ == "__main__":
    raise SystemExit(main())
