"""Exclude competing downloads/builds sharing a raw directory on Linux.

Only explicitly selected source-processing children inherit a lease. Its open
file description remains locked until the last owner closes its descriptor,
including when a parent exits before a child. The lock file is never removed.
"""
from __future__ import annotations

import fcntl
import os
import re
import stat
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Mapping


LOCK_NAME = ".source-activity.lock"
INHERITED_FD_ENV = "OJS_SOURCE_ACTIVITY_FD"
_thread_state = threading.local()
_inheritance_mutex = threading.Lock()


class SourceActivityError(RuntimeError):
    """The source lock or inherited capability could not be validated."""


class SourceActivityBusy(SourceActivityError):
    """Another download or build owns this raw directory."""


def _registry() -> dict[Path, SourceActivityLease]:
    if getattr(_thread_state, "pid", None) != os.getpid():
        _thread_state.pid = os.getpid()
        _thread_state.leases = {}
    return _thread_state.leases


def _check_descriptor(descriptor: int, path: Path) -> None:
    opened, named = os.fstat(descriptor), path.lstat()
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(named.st_mode)
        or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
    ):
        raise SourceActivityError(f"source activity descriptor does not match {path}")


class SourceActivityLease:
    def __init__(self, raw_dir: Path, descriptor: int):
        self.raw_dir = raw_dir
        self.path = raw_dir / LOCK_NAME
        self._descriptor = descriptor
        self._thread = threading.get_ident()
        self._pid = os.getpid()
        self._depth = 1
        self._closed = False

    def _validate(self) -> None:
        if self._closed or self._pid != os.getpid() or self._thread != threading.get_ident():
            raise SourceActivityError("source activity lease is inactive or belongs to another thread/process")
        _check_descriptor(self._descriptor, self.path)

    def child_options(self, env: Mapping[str, str] | None = None) -> dict:
        """Give one source-processing child the current lease explicitly."""
        self._validate()
        child_env = dict(os.environ if env is None else env)
        child_env.pop(INHERITED_FD_ENV, None)
        child_env[INHERITED_FD_ENV] = str(self._descriptor)
        return {"env": child_env, "pass_fds": (self._descriptor,)}


def _acquire(raw_dir: Path) -> SourceActivityLease:
    path = raw_dir / LOCK_NAME
    raw_dir.mkdir(parents=True, exist_ok=True)
    # An inherited capability belongs to one control flow, not every thread in
    # the child process. Consume it only after all validation succeeds.
    with _inheritance_mutex:
        inherited = os.environ.get(INHERITED_FD_ENV)
        if inherited is not None:
            if not re.fullmatch(r"[0-9]{1,10}", inherited) or not 3 <= int(inherited) <= 2**31 - 1:
                raise SourceActivityError("invalid inherited source activity descriptor")
            descriptor = int(inherited)
            try:
                _check_descriptor(descriptor, path)
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                _check_descriptor(descriptor, path)
                os.set_inheritable(descriptor, False)
            except (OSError, SourceActivityError) as exc:
                raise SourceActivityError("inherited source activity descriptor is invalid or unavailable") from exc
            os.environ.pop(INHERITED_FD_ENV)
            return SourceActivityLease(raw_dir, descriptor)

    descriptor = None
    try:
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
        try:
            descriptor = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o444)
            os.fchmod(descriptor, 0o444)
        except FileExistsError:
            descriptor = os.open(path, flags)
        _check_descriptor(descriptor, path)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SourceActivityBusy(f"source download or processing already running for {raw_dir}") from exc
        _check_descriptor(descriptor, path)
        lease = SourceActivityLease(raw_dir, descriptor)
        descriptor = None
        return lease
    finally:
        if descriptor is not None:
            os.close(descriptor)


@contextmanager
def source_activity(raw_dir: Path):
    """Hold one raw-directory lease, reentrant only in the owning thread.

    A valid inherited descriptor is adopted and closed at the outermost exit;
    its environment entry is consumed. No owner issues LOCK_UN, which would
    also unlock descriptors inherited by still-running children.
    """
    try:
        canonical = Path(raw_dir).expanduser().resolve()
        registry = _registry()
        lease = registry.get(canonical)
        if lease is None:
            lease = _acquire(canonical)
            registry[canonical] = lease
        else:
            lease._validate()
            lease._depth += 1
    except OSError as exc:
        raise SourceActivityError(f"could not acquire source activity lock: {exc}") from exc
    try:
        yield lease
    finally:
        lease._depth -= 1
        if lease._depth == 0:
            registry.pop(canonical, None)
            lease._closed = True
            os.close(lease._descriptor)


def source_activity_child_options(raw_dir: Path, env: Mapping[str, str] | None = None) -> dict:
    """Return subprocess options for the active lease in the current thread."""
    canonical = Path(raw_dir).expanduser().resolve()
    lease = _registry().get(canonical)
    if lease is None:
        raise SourceActivityError(f"no source activity lease is held for {canonical}")
    return lease.child_options(env)
