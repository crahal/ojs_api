#!/usr/bin/env python3
"""Remove explicitly authorized bootstrap gzips after verified publication.

Only dated raw gzips and their validated metadata sidecars are eligible. A
durable per-snapshot audit is written before deletion and retained afterwards
so an interrupted two-file cleanup can safely resume without its original gzip.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import compact_update as update
import download_beacon
import publish_live
import run_pipeline

MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
HASH = re.compile(r"[0-9a-f]{64}")


def identity(path: Path) -> dict:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise update.UpdateError(f"expected a regular file: {path.name}")
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def small_bytes(path: Path) -> bytes:
    identity(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as source:
        content = source.read(MAX_DOCUMENT_BYTES + 1)
    if len(content) > MAX_DOCUMENT_BYTES:
        raise update.UpdateError(f"oversized metadata file: {path.name}")
    return content


def document(path: Path) -> dict:
    value = json.loads(small_bytes(path))
    if not isinstance(value, dict):
        raise update.UpdateError(f"expected a JSON object: {path.name}")
    return value


def report_provenance(clean: Path, version: str) -> tuple[dict, dict, str]:
    """Authenticate retained report bytes against the completed manifest."""
    report_path = clean / f"pkpbeacon-changes-{version}.json"
    content = small_bytes(report_path)
    report = json.loads(content)
    manifest = document(clean / f"pkpbeacon-release-{version}.json")
    checksum = hashlib.sha256(content).hexdigest()
    declared = small_bytes(report_path.with_name(report_path.name + ".sha256")).split()
    expected = {"schema_version": 1, "snapshot_date": version,
                "clean_export_filename": f"pkpbeacon-clean-{version}.sql.gz",
                "change_report_filename": report_path.name,
                "change_report_sha256": checksum}
    if (not declared or declared[0] != checksum.encode("ascii")
            or any(manifest.get(key) != value for key, value in expected.items())
            or not HASH.fullmatch(str(manifest.get("clean_export_sha256", "")))):
        raise update.UpdateError(f"invalid completed release provenance: {version}")
    snapshot = report.get("snapshot") if isinstance(report, dict) else None
    if (not isinstance(snapshot, dict) or report.get("schema_version") != 1
            or snapshot.get("date") != version
            or not all(HASH.fullmatch(str(snapshot.get(key, "")))
                       for key in ("source_sha256", "build_sql_sha256"))):
        raise update.UpdateError(f"invalid report source provenance: {version}")
    return report, manifest, checksum


def live_release(coordinator: update.Coordinator) -> tuple[update.Release | None, dict]:
    """Check committed live provenance without hashing the large clean export."""
    root, clean = coordinator.root, coordinator.clean
    guard = {}
    for path in (root, root / "data", clean):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise update.UpdateError(f"expected an unsymlinked directory: {path.name}")
        guard[str(path)] = (info.st_dev, info.st_ino)
    live = update.live_directory(clean)
    if live is None:
        return None, guard
    for path in (coordinator.raw, root / "sql", live):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise update.UpdateError(f"expected an unsymlinked directory: {path.name}")
        guard[str(path)] = (info.st_dev, info.st_ino)
    for pointer in ("mysql-live", "mysql-current"):
        path = clean / pointer
        if not path.is_symlink() or os.readlink(path) != live.name:
            raise update.UpdateError("live and current database pointers must agree")
        info = path.lstat()
        guard[pointer] = (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns)
    if coordinator.journal.exists() or coordinator.journal.is_symlink():
        raise update.UpdateError("automatic update recovery is still pending")
    if any(clean.glob("mysql-????-??-??.building")):
        raise update.UpdateError("an unfinished database build is present")
    version = live.name.removeprefix("mysql-")
    date.fromisoformat(version)
    export = clean / f"pkpbeacon-clean-{version}.sql.gz"
    paths = [export, export.with_name(export.name + ".sha256"),
             clean / f"pkpbeacon-changes-{version}.json",
             clean / f"pkpbeacon-changes-{version}.json.sha256",
             clean / f"pkpbeacon-release-{version}.json", live / update.MARKER,
             root / "sql" / "01_build_ojs_tables.sql"]
    for path in paths:
        guard[str(path)] = identity(path)
    report, manifest, _ = report_provenance(clean, version)
    source_hash = report["snapshot"]["source_sha256"]
    build_hash = report["snapshot"]["build_sql_sha256"]
    declared = small_bytes(paths[1]).split()
    marker = document(live / update.MARKER)
    expected_marker = {"schema_version": 1, "snapshot_date": version,
                       "source_sha256": source_hash, "build_sql_sha256": build_hash,
                       "clean_only": True, "data_directory_name": live.name}
    release_guard = report.get("release_guard")
    if (not declared or declared[0].decode("ascii") != manifest["clean_export_sha256"]
            or any(marker.get(key) != value for key, value in expected_marker.items())
            or not isinstance(release_guard, dict)
            or release_guard.get("status") not in ("passed", "overridden")
            or hashlib.sha256(small_bytes(paths[-1])).hexdigest() != build_hash):
        raise update.UpdateError("live release marker, guard, or build SQL does not match")
    return update.Release(version, live, export, source_hash, build_hash, report), guard


def audit_path(clean: Path, version: str) -> Path:
    return clean / f"pkpbeacon-bootstrap-cleanup-{version}.json"


def check_targets(raw: Path, state: dict) -> None:
    for entry in state["artifacts"]:
        path = raw / entry["name"]
        if path.exists() or path.is_symlink():
            if entry["deleted"] or identity(path) != entry["stat"]:
                raise update.UpdateError(f"cleanup target changed since validation: {path.name}")


def prepare(coordinator: update.Coordinator, version: str, release: update.Release) -> dict | None:
    raw, clean = coordinator.raw, coordinator.clean
    if version >= release.date:
        raise update.UpdateError("refusing cleanup of a live or newer snapshot")
    names = [f"pkpbeacon-{version}.sql.gz", f"pkpbeacon-{version}.sql.gz.metadata.json"]
    path = audit_path(clean, version)
    if not path.exists() and not path.is_symlink() and not any(
            (raw / name).exists() or (raw / name).is_symlink() for name in names):
        return None
    report, _, report_hash = report_provenance(clean, version)
    source_hash = report["snapshot"]["source_sha256"]
    if path.exists() or path.is_symlink():
        state = document(path)
        expected = {"schema_version": 1, "kind": "bootstrap-snapshot-cleanup",
                    "project_root": str(coordinator.root), "snapshot": version,
                    "source_sha256": source_hash, "report_sha256": report_hash}
        artifacts = state.get("artifacts")
        if (any(state.get(key) != value for key, value in expected.items())
                or not isinstance(artifacts, list) or len(artifacts) != 2
                or state.get("phase") not in ("prepared", "complete")):
            raise update.UpdateError("invalid bootstrap cleanup audit")
        for entry, name in zip(artifacts, names):
            if (not isinstance(entry, dict) or entry.get("name") != name
                    or type(entry.get("deleted")) is not bool
                    or not isinstance(entry.get("stat"), dict)
                    or set(entry["stat"]) != {"device", "inode", "size", "mtime_ns", "ctime_ns"}
                    or any(type(value) is not int or value < 0 for value in entry["stat"].values())):
                raise update.UpdateError("invalid bootstrap cleanup artifact identity")
        if state["phase"] == "complete" and not all(entry["deleted"] for entry in artifacts):
            raise update.UpdateError("incomplete bootstrap cleanup audit")
        check_targets(raw, state)
        return state
    identities = [identity(raw / name) for name in names]
    if identities[1]["size"] > MAX_DOCUMENT_BYTES:
        raise update.UpdateError(f"oversized snapshot metadata: {version}")
    metadata = download_beacon.read_validated_metadata(raw / names[0])
    if metadata is None:
        raise update.UpdateError(f"snapshot lacks unchanged validated metadata: {version}")
    snapshot = run_pipeline.Snapshot(raw / names[0], version,
                                    datetime.strptime(metadata["dump_datetime"], "%Y-%m-%d %H:%M:%S"),
                                    metadata["uncompressed_size"])
    if not run_pipeline.retained_snapshot_matches_release(snapshot, clean):
        raise update.UpdateError(f"snapshot does not match completed historical report: {version}")
    state = {"schema_version": 1, "kind": "bootstrap-snapshot-cleanup",
             "project_root": str(coordinator.root), "snapshot": version,
             "source_sha256": source_hash, "report_sha256": report_hash,
             "phase": "prepared", "verified_release": release.date,
             "prepared_at": datetime.now(timezone.utc).isoformat(),
             "artifacts": [{"name": name, "stat": binding, "deleted": False}
                           for name, binding in zip(names, identities)]}
    check_targets(raw, state)
    return state


def execute(args: argparse.Namespace, coordinator: update.Coordinator | None = None) -> dict:
    root = args.project_root.expanduser().absolute()
    if any(path.is_symlink() for path in (root, *root.parents)):
        raise update.UpdateError("refusing a symlinked project root")
    coordinator = coordinator or update.Coordinator(args)
    clean = coordinator.clean
    with update.automatic_lock(clean), run_pipeline.pipeline_lock(clean), \
            publish_live.publisher_lock(coordinator.root):
        release, guard = live_release(coordinator)
        if release is None:
            return {"status": "deferred", "reason": "no live release", "removed_count": 0}
        plans = [state for version in sorted(set(args.snapshot))
                 if (state := prepare(coordinator, version, release)) is not None]
        if not any(state["phase"] != "complete" for state in plans):
            return {"status": "unchanged", "removed_count": 0}
        coordinator.check_health(release, release.datadir)
        removed_count = removed_bytes = 0
        for state in plans:
            if state["phase"] == "complete":
                continue
            path = audit_path(clean, state["snapshot"])
            update.atomic_json(path, state)
            for entry in state["artifacts"]:
                if live_release(coordinator)[1] != guard:
                    raise update.UpdateError("live release or build changed during cleanup")
                check_targets(coordinator.raw, state)
                target = coordinator.raw / entry["name"]
                if not entry["deleted"]:
                    if target.exists():
                        target.unlink()
                        update.sync_directory(coordinator.raw)
                        removed_count += 1
                        removed_bytes += entry["stat"]["size"]
                        print(f"[bootstrap-cleanup] removed {target.name} ({entry['stat']['size']} bytes)", flush=True)
                    else:
                        entry["recovered_missing"] = True
                    entry["deleted"] = True
                    update.atomic_json(path, state)
            state["phase"] = "complete"
            state["completed_at"] = datetime.now(timezone.utc).isoformat()
            update.atomic_json(path, state)
        return {"status": "cleaned", "verified_release": release.date,
                "removed_count": removed_count, "removed_bytes": removed_bytes}


def snapshot_date(value: str) -> str:
    try:
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("snapshot must be an exact YYYY-MM-DD date") from None
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--project-root", type=Path, default=update.ROOT)
    result.add_argument("--snapshot", action="append", required=True, type=snapshot_date,
                        help="exact historical snapshot date authorized for deletion; repeat as needed")
    result.add_argument("--api-url", default=f"http://127.0.0.1:{os.getenv('OJS_API_PORT', '8000')}")
    result.add_argument("--health-timeout", type=float, default=300)
    result.set_defaults(max_runtime_hours=1)
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if not math.isfinite(args.health_timeout) or args.health_timeout <= 0:
        print("error: health timeout must be positive and finite", file=sys.stderr)
        return 2
    try:
        print(json.dumps(execute(args), sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        detail = str(exc)[:240] if isinstance(exc, update.UpdateError) else type(exc).__name__
        print(f"error: bootstrap cleanup refused: {detail}; audit resumable", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("error: bootstrap cleanup interrupted; audit resumable", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
