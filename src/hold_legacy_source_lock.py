#!/usr/bin/env python3
"""Hold the source lease until explicitly named pre-upgrade Linux processes exit."""

from __future__ import annotations

import argparse
import errno
import json
import os
import selectors
import sys
from pathlib import Path

from source_activity import SourceActivityBusy, SourceActivityError, source_activity


def positive_pid(value: str) -> int:
    try:
        pid = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("PID must be a positive integer") from exc
    if not 0 < pid <= 2**31 - 1:
        raise argparse.ArgumentTypeError("PID must be a positive Linux process ID")
    return pid


def hold(raw_dir: Path, pids: list[int]) -> None:
    if not pids or any(type(pid) is not int or not 0 < pid <= 2**31 - 1 for pid in pids):
        raise RuntimeError("at least one positive Linux process ID is required")
    if not hasattr(os, "pidfd_open"):
        raise RuntimeError("this migration guard requires Linux pidfd_open support")
    monitored = sorted(set(pids))
    descriptors: dict[int, int] = {}
    with selectors.DefaultSelector() as selector:
        try:
            # Pin every process identity before taking the lease. A PID number
            # alone could later refer to an unrelated process after reuse.
            for pid in monitored:
                try:
                    descriptor = os.pidfd_open(pid, 0)
                except OSError as exc:
                    if exc.errno == errno.ESRCH:
                        reason = "process has already exited"
                    elif exc.errno in (errno.EPERM, errno.EACCES):
                        reason = "permission denied"
                    else:
                        reason = str(exc)
                    raise RuntimeError(f"cannot monitor PID {pid}: {reason}") from exc
                descriptors[descriptor] = pid
                selector.register(descriptor, selectors.EVENT_READ)
            exited = [descriptors[key.fd] for key, _ in selector.select(timeout=0)]
            if exited:
                raise RuntimeError(f"processes exited before guard acquisition: {exited}")
            with source_activity(raw_dir):
                print(json.dumps({"event": "guard-acquired", "pids": monitored}), flush=True)
                while descriptors:
                    for key, _ in selector.select():
                        selector.unregister(key.fd)
                        os.close(key.fd)
                        del descriptors[key.fd]
            print(json.dumps({"event": "guard-released", "pids": monitored}), flush=True)
        finally:
            for descriptor in descriptors:
                os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", required=True, type=Path)
    parser.add_argument("--pid", required=True, action="append", type=positive_pid,
                        help="live legacy process to monitor; repeat for its active children")
    args = parser.parse_args(argv)
    try:
        hold(args.raw_dir, args.pid)
    except SourceActivityBusy:
        print("error: source activity is busy; migration guard was not acquired", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, SourceActivityError) as exc:
        print(f"error: migration guard failed: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("error: migration guard interrupted", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
