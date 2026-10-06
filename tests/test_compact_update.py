from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import compact_update as update


def progress_events(output):
    return [json.loads(line.removeprefix("[progress] "))
            for line in output.splitlines() if line.startswith("[progress] ")]


class HeartbeatOutput(io.StringIO):
    def __init__(self):
        super().__init__()
        self.heartbeat = threading.Event()

    def write(self, value):
        result = super().write(value)
        if '"event": "heartbeat"' in value:
            self.heartbeat.set()
        return result


def artifact(path: Path, payload: bytes):
    path.write_bytes(payload)
    checksum = hashlib.sha256(payload).hexdigest()
    path.with_name(path.name + ".sha256").write_text(checksum + "  " + path.name + "\n")
    return checksum


def release(root: Path, date: str):
    clean = root / "data" / "clean"
    clean.mkdir(parents=True, exist_ok=True)
    datadir = clean / f"mysql-{date}"
    datadir.mkdir(exist_ok=True)
    (datadir / "ibdata1").write_bytes(b"database" * 20)
    source_hash = hashlib.sha256(date.encode()).hexdigest()
    report = {"schema_version": 1, "snapshot": {"date": date, "source_sha256": source_hash,
                                               "build_sql_sha256": "b" * 64},
              "release_guard": {"status": "passed"},
              "source_rows": {"retained_total": 2, "active": 1},
              "articles": {"retained_total": 2, "active": 1, "removed_tombstones": 1, "merged_tombstones": 0},
              "article_events": {"total": 3}}
    report_file = clean / f"pkpbeacon-changes-{date}.json"
    report_hash = artifact(report_file, json.dumps(report).encode())
    export = clean / f"pkpbeacon-clean-{date}.sql.gz"
    export_hash = artifact(export, gzip.compress(b"SELECT 1;"))
    (clean / f"pkpbeacon-release-{date}.json").write_text(json.dumps({
        "schema_version": 1, "snapshot_date": date, "clean_export_filename": export.name,
        "clean_export_sha256": export_hash, "change_report_filename": report_file.name,
        "change_report_sha256": report_hash}))
    (datadir / update.MARKER).write_text(json.dumps({"schema_version": 1, "snapshot_date": date,
        "source_sha256": source_hash, "build_sql_sha256": "b" * 64, "clean_only": True,
        "data_directory_name": datadir.name}))
    return update.validate_release(clean, date)


class FakeRunner:
    def __init__(self, root):
        self.root = root
        self.commands = []
        self.children = []
        self.probe = {"needs_download": True, "known_snapshot": None}
        self.mount = None
        self.other_mounts = []
        self.on_child = None
        self.fail_start = False

    def run(self, command, *, cwd, env, timeout):
        self.commands.append((list(command), dict(env)))
        if "--check" in command:
            return json.dumps(self.probe)
        if command[:4] == ["docker", "ps", "--all", "--quiet"]:
            return "\n".join(((["mine"] if self.mount else []) + [f"other{i}" for i in range(len(self.other_mounts))]))
        if command[:2] == ["docker", "inspect"]:
            mounts = ([self.mount] if self.mount else []) + self.other_mounts
            return json.dumps([{"Mounts": [{"Source": str(path)}]} for path in mounts])
        if command[:2] == ["docker", "compose"]:
            self.assert_pinned(env)
            if "up" in command:
                if self.fail_start:
                    self.fail_start = False
                    raise update.UpdateError("simulated start failure")
                self.mount = Path(env["OJS_LIVE_DATA_DIR"])
            if "down" in command:
                self.mount = None
            return ""
        raise AssertionError(command)

    def assert_pinned(self, env):
        path = Path(env["OJS_LIVE_DATA_DIR"])
        assert path.is_absolute() and path.name.startswith("mysql-20") and not path.is_symlink()

    def child(self, command, *, cwd, env, reserve_check, timeout, pass_fds=()):
        reserve_check()
        self.children.append(command)
        if self.on_child:
            self.on_child(command)


class FakeCoordinator(update.Coordinator):
    def __init__(self, args, runner):
        super().__init__(args, runner)
        self.health_calls = []
        self.cold_checks = []
        self.fail_health = False

    def compatible_mysql_tools(self):
        return update.run_pipeline.MySQLTools("mysqld", "mysql", "mysqladmin", "mysqldump", "8.4.0")

    def credentials(self):
        return "client", "secret"

    def verify_database(self, candidate):
        self.require_unmounted(candidate.datadir)
        self.cold_checks.append(candidate.date)

    def check_health(self, candidate, datadir):
        self.health_calls.append((candidate.date if candidate else None, datadir))
        if self.fail_health:
            self.fail_health = False
            raise update.UpdateError("simulated API mismatch")
        if self.runner.mount != datadir:
            raise AssertionError("serving mount mismatch")


class CompactUpdateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.clean = self.root / "data" / "clean"
        self.raw = self.root / "data" / "raw"
        self.clean.mkdir(parents=True)
        self.raw.mkdir()
        self.args = update.parser().parse_args(["--project-root", str(self.root), "--publish-only",
                                               "--min-free-gb", "0", "--reserve-gb", "0"])
        self.runner = FakeRunner(self.root)
        self.coordinator = FakeCoordinator(self.args, self.runner)

    def pointer(self, name, candidate):
        path = self.clean / name
        path.unlink(missing_ok=True)
        path.symlink_to(candidate.datadir.name)

    def old_and_new(self):
        old = release(self.root, "2026-01-01")
        new = release(self.root, "2026-02-01")
        self.pointer("mysql-live", old)
        self.pointer("mysql-current", new)
        self.runner.mount = old.datadir
        return old, new

    def test_initial_build_promotes_without_previous_database(self):
        candidate = release(self.root, "2026-01-01")
        self.pointer("mysql-current", candidate)
        result = self.coordinator.execute()
        self.assertEqual(result["status"], "published")
        self.assertEqual(update.live_directory(self.clean), candidate.datadir)
        self.assertEqual(self.coordinator.cold_checks, [candidate.date])
        self.assertFalse(self.coordinator.journal.exists())
        for command, env in self.runner.commands:
            if "compose" in command:
                self.assertEqual(env["OJS_LIVE_DATA_DIR"], str(candidate.datadir))

    def test_publish_progress_names_verification_promotion_and_cleanup(self):
        candidate = release(self.root, "2026-01-01")
        self.pointer("mysql-current", candidate)
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.coordinator.execute()
        events = progress_events(output.getvalue())
        completed = {event["stage"] for event in events if event["event"] == "completed"}
        self.assertTrue({"update-release-checksums", "update-cold-verification",
                         "update-promotion", "update-cleanup"} <= completed)
        self.assertNotIn("secret", output.getvalue())

    def test_build_supervision_keeps_heartbeats_without_claiming_data_progress(self):
        output = HeartbeatOutput()
        progress = update.Progress

        def blocked_child(command):
            self.assertTrue(output.heartbeat.wait(2), "no heartbeat while child was blocked")

        self.runner.on_child = blocked_child
        self.coordinator.env["MYSQL_ROOT_PASSWORD"] = "private-password"
        with update.source_activity(self.raw), contextlib.redirect_stderr(output), \
             mock.patch.object(update, "Progress", side_effect=lambda label, **kwargs:
                               progress(label, interval=0.01, **kwargs)):
            self.coordinator.child(["private-command", "https://private-host"], stage="update-build")
        events = progress_events(output.getvalue())
        self.assertTrue(any(event["event"] == "heartbeat" for event in events))
        self.assertTrue(all(event["stage"] == "update-build" for event in events))
        self.assertTrue(all(event["step"] == "supervise-child" for event in events))
        self.assertTrue(all("processed_bytes" not in event and "reserve_checks" not in event for event in events))
        self.assertEqual(events[-1]["event"], "completed")
        for private in ("private-password", "private-command", "private-host"):
            self.assertNotIn(private, output.getvalue())

    def test_release_checksum_verification_keeps_heartbeats(self):
        candidate = release(self.root, "2026-01-01")
        output = HeartbeatOutput()
        progress = update.Progress
        verify = update.publish_live.verify_export_checksum

        def blocked_checksum(*args):
            self.assertTrue(output.heartbeat.wait(2), "no heartbeat while checksum was blocked")
            verify(*args)

        with contextlib.redirect_stderr(output), \
             mock.patch.object(update, "Progress", side_effect=lambda label, **kwargs:
                               progress(label, interval=0.01, **kwargs)), \
             mock.patch.object(update.publish_live, "verify_export_checksum", side_effect=blocked_checksum):
            update.validate_release(self.clean, candidate.date)
        events = progress_events(output.getvalue())
        self.assertTrue(any(event["event"] == "heartbeat" for event in events))
        self.assertEqual(events[-1]["stage"], "update-release-checksums")
        self.assertEqual(events[-1]["event"], "completed")

    def test_failed_promotion_logs_rollback_without_exception_content(self):
        self.old_and_new()
        self.coordinator.fail_health = True
        output = io.StringIO()
        with contextlib.redirect_stderr(output), self.assertRaises(update.UpdateError):
            self.coordinator.execute()
        events = progress_events(output.getvalue())
        self.assertTrue(any(event["stage"] == "update-rollback" and event["event"] == "completed"
                            for event in events))
        self.assertEqual(events[-1]["stage"], "update-promotion")
        self.assertEqual(events[-1]["event"], "failed")
        self.assertNotIn("simulated API mismatch", output.getvalue())

    def test_success_prunes_owned_old_database_export_and_keeps_reports(self):
        old, new = self.old_and_new()
        legacy = self.raw / "pkpbeacon-2026-01-01.sql"
        legacy.write_text("user supplied SQL")
        self.coordinator.execute()
        self.assertFalse(old.datadir.exists())
        self.assertFalse(old.export.exists())
        self.assertTrue(new.datadir.exists())
        self.assertTrue(new.export.exists())
        self.assertTrue(legacy.exists())
        self.assertTrue((self.clean / f"pkpbeacon-release-{old.date}.json").exists())
        self.assertTrue((self.clean / f"pkpbeacon-changes-{old.date}.json").exists())
        audit = update.read_json(self.clean / f"pkpbeacon-cleanup-{new.date}.json")
        self.assertEqual(audit["deleted_count"], 3)
        self.assertGreater(audit["deleted_bytes"], 0)

    def test_health_failure_rolls_back_and_never_prunes(self):
        old, new = self.old_and_new()
        self.coordinator.fail_health = True
        with self.assertRaisesRegex(update.UpdateError, "API mismatch"):
            self.coordinator.execute()
        self.assertEqual(update.live_directory(self.clean), old.datadir)
        self.assertEqual(self.runner.mount, old.datadir)
        self.assertTrue(old.export.exists())
        self.assertTrue(new.export.exists())
        self.assertFalse(self.coordinator.journal.exists())

    def test_initial_health_failure_removes_stopped_candidate_containers(self):
        candidate = release(self.root, "2026-01-01")
        self.pointer("mysql-current", candidate)
        self.coordinator.fail_health = True
        with self.assertRaisesRegex(update.UpdateError, "API mismatch"):
            self.coordinator.execute()
        self.assertIsNone(update.live_directory(self.clean))
        self.assertIsNone(self.runner.mount)
        self.assertTrue(candidate.datadir.exists())
        self.assertFalse(self.coordinator.journal.exists())

    def test_start_failure_rolls_back_to_previous(self):
        old, new = self.old_and_new()
        self.runner.fail_start = True
        with self.assertRaisesRegex(update.UpdateError, "start failure"):
            self.coordinator.execute()
        self.assertEqual(update.live_directory(self.clean), old.datadir)
        self.assertEqual(self.runner.mount, old.datadir)
        self.assertTrue(new.datadir.exists())

    def test_interrupted_switch_recovery_restores_old_before_retry(self):
        old, new = self.old_and_new()
        self.pointer("mysql-live", new)
        self.runner.mount = new.datadir
        update.atomic_json(self.coordinator.journal, {"schema_version": 1, "phase": "switching",
            "candidate": new.datadir.name, "previous": old.datadir.name, "snapshot_date": new.date})
        self.coordinator.recover()
        self.assertEqual(update.live_directory(self.clean), old.datadir)
        self.assertEqual(self.runner.mount, old.datadir)
        self.assertTrue(old.export.exists())
        self.assertTrue(new.datadir.exists())

    def test_verified_recovery_finishes_pruning_after_rechecking_health(self):
        old, new = self.old_and_new()
        self.pointer("mysql-live", new)
        self.runner.mount = new.datadir
        update.atomic_json(self.coordinator.journal, {"schema_version": 1, "phase": "verified",
            "candidate": new.datadir.name, "previous": old.datadir.name, "snapshot_date": new.date})
        self.coordinator.recover()
        self.assertEqual(self.coordinator.health_calls[0][0], new.date)
        self.assertFalse(old.datadir.exists())
        self.assertFalse(self.coordinator.journal.exists())

    def test_cleanup_journal_recovers_mid_directory_deletion(self):
        old, new = self.old_and_new()
        self.pointer("mysql-live", new)
        self.runner.mount = new.datadir
        state = {"schema_version": 1, "phase": "verified", "candidate": new.datadir.name,
                 "previous": old.datadir.name, "snapshot_date": new.date,
                 "cleanup": self.coordinator.cleanup_plan(new)}
        update.atomic_json(self.coordinator.journal, state)
        (old.datadir / update.MARKER).unlink()
        self.coordinator.recover()
        self.assertFalse(old.datadir.exists())
        self.assertFalse(old.export.exists())

    def test_cleanup_journal_rejects_replaced_inode(self):
        old, new = self.old_and_new()
        self.runner.mount = new.datadir
        state = {"schema_version": 1, "phase": "verified", "candidate": new.datadir.name,
                 "previous": old.datadir.name, "snapshot_date": new.date,
                 "cleanup": self.coordinator.cleanup_plan(new)}
        old.datadir.rename(self.clean / "saved-user-directory")
        old.datadir.mkdir()
        with self.assertRaisesRegex(update.UpdateError, "changed since validation"):
            self.coordinator.finish(state, new)
        self.assertTrue(old.datadir.exists())

    def test_all_containers_including_other_stopped_mounts_protect_cleanup(self):
        old, new = self.old_and_new()
        self.runner.other_mounts = [old.datadir]
        self.coordinator.execute()
        self.assertTrue(old.datadir.exists())
        self.assertTrue(old.export.exists())

    def test_symlink_inside_old_database_prevents_cleanup(self):
        old, new = self.old_and_new()
        outside = self.root / "valuable"
        outside.write_text("keep me")
        (old.datadir / "external").symlink_to(outside)
        self.coordinator.execute()
        self.assertTrue(old.datadir.exists())
        self.assertEqual(outside.read_text(), "keep me")

    def test_unowned_database_is_retained(self):
        old, new = self.old_and_new()
        (old.datadir / update.MARKER).unlink()
        self.coordinator.execute()
        self.assertTrue(old.datadir.exists())
        self.assertTrue(old.export.exists())

    def test_corrupt_candidate_export_cannot_stop_live_database(self):
        old, new = self.old_and_new()
        new.export.write_bytes(b"tampered")
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            self.coordinator.execute()
        self.assertEqual(self.runner.mount, old.datadir)
        self.assertFalse(any("stop" in command for command, _ in self.runner.commands))

    def test_no_prune_keeps_previous_artifacts(self):
        old, new = self.old_and_new()
        self.args.no_prune = True
        self.coordinator.execute()
        self.assertTrue(old.datadir.exists())
        self.assertTrue(old.export.exists())
        self.assertEqual(update.live_directory(self.clean), new.datadir)

    def test_same_live_publish_starts_services(self):
        candidate = release(self.root, "2026-01-01")
        self.pointer("mysql-live", candidate)
        self.pointer("mysql-current", candidate)
        self.coordinator.execute()
        self.assertEqual(self.runner.mount, candidate.datadir)
        self.assertEqual(self.coordinator.cold_checks, [])

    def test_raw_cleanup_requires_download_ownership_and_preserves_newest(self):
        old, new = self.old_and_new()
        paths = []
        for candidate in (old, new):
            raw = self.raw / f"pkpbeacon-{candidate.date}.sql.gz"
            raw.write_bytes(b"downloaded archive")
            raw.with_name(raw.name + ".metadata.json").write_text("{}")
            paths.append(raw)
        metadata = {"source_url": self.args.url, "uncompressed_sha256": old.source_sha256}
        with mock.patch.object(update.download_beacon, "read_validated_metadata", return_value=metadata):
            self.coordinator.execute()
        self.assertFalse(paths[0].exists())
        self.assertFalse(paths[0].with_name(paths[0].name + ".metadata.json").exists())
        self.assertTrue(paths[1].exists())

    def test_user_gzip_without_download_ownership_is_not_deleted(self):
        old, new = self.old_and_new()
        raw = self.raw / f"pkpbeacon-{old.date}.sql.gz"
        raw.write_bytes(b"user archive")
        with mock.patch.object(update.download_beacon, "read_validated_metadata", return_value={"uncompressed_sha256": old.source_sha256}):
            self.coordinator.execute()
        self.assertTrue(raw.exists())

    def test_unchanged_head_skips_build_and_capacity_gate(self):
        old = release(self.root, "2026-01-01")
        self.pointer("mysql-live", old)
        self.args.publish_only = False
        raw = self.raw / f"pkpbeacon-{old.date}.sql.gz"
        self.runner.probe = {"needs_download": False, "known_snapshot": str(raw)}
        with mock.patch.object(update.download_beacon, "read_validated_metadata", return_value={"uncompressed_sha256": old.source_sha256}), \
             mock.patch.object(self.coordinator, "capacity", side_effect=AssertionError("must skip")):
            result = self.coordinator.execute()
        self.assertEqual(result["status"], "unchanged")
        self.assertFalse(self.runner.children)

    def test_new_head_downloads_builds_and_promotes(self):
        self.args.publish_only = False
        def build(command):
            if "run_pipeline.py" in " ".join(command):
                # The child must acquire the same pipeline lock for itself.
                with update.run_pipeline.pipeline_lock(self.clean):
                    pass
                candidate = release(self.root, "2026-02-01")
                self.pointer("mysql-current", candidate)
        self.runner.on_child = build
        result = self.coordinator.execute()
        self.assertEqual(result["release"], "2026-02-01")
        self.assertEqual(len(self.runner.children), 2)
        self.assertIn("--compact-storage", self.runner.children[-1])
        self.assertIn("--resume-building", self.runner.children[-1])

    def test_mounted_staging_database_prevents_child_rebuild(self):
        self.args.publish_only = False
        staging = self.clean / "mysql-2026-01-01.building"
        staging.mkdir()
        self.runner.other_mounts = [staging]
        with self.assertRaisesRegex(update.UpdateError, "mounted or used"):
            self.coordinator.execute()
        self.assertEqual(len(self.runner.children), 1)  # Download only.
        self.assertTrue(staging.exists())

    def test_same_release_retry_keeps_previous_deletion_audit(self):
        old, new = self.old_and_new()
        self.coordinator.execute()
        path = self.clean / f"pkpbeacon-cleanup-{new.date}.json"
        before = update.read_json(path)
        self.coordinator.execute()
        after = update.read_json(path)
        self.assertEqual(before["deleted_count"], after["deleted_count"])
        self.assertEqual(before["artifacts"], after["artifacts"])

    def test_check_creates_no_files_or_locks(self):
        self.args.check = True
        before = sorted(self.root.rglob("*"))
        result = self.coordinator.execute()
        self.assertEqual(before, sorted(self.root.rglob("*")))
        self.assertIn("probe", result)

    def test_disk_floor_prevents_download_and_switch(self):
        self.args.publish_only = False
        self.args.min_free_gb = 20
        with mock.patch.object(update.shutil, "disk_usage", return_value=mock.Mock(free=update.GIB)):
            with self.assertRaisesRegex(update.UpdateError, "disk reserve"):
                self.coordinator.execute()
        self.assertFalse(self.runner.children)

    def test_automatic_lock_excludes_second_run(self):
        with update.automatic_lock(self.clean):
            with self.assertRaisesRegex(update.UpdateError, "already running"):
                self.coordinator.execute()

    def test_publisher_lock_prevents_switch(self):
        self.old_and_new()
        with update.publish_live.publisher_lock(self.root):
            with self.assertRaisesRegex(RuntimeError, "already running"):
                self.coordinator.execute()
        self.assertFalse(self.coordinator.cold_checks)

    def test_pipeline_lock_prevents_switch(self):
        self.old_and_new()
        with update.run_pipeline.pipeline_lock(self.clean):
            with self.assertRaisesRegex(RuntimeError, "already running"):
                self.coordinator.execute()
        self.assertFalse(self.coordinator.cold_checks)

    def test_reserve_monitor_interrupts_child_and_waits_for_shutdown(self):
        process = mock.Mock(pid=1234)
        process.poll.return_value = None
        with mock.patch.object(update.subprocess, "Popen", return_value=process), \
             mock.patch.object(update.os, "killpg") as kill:
            with self.assertRaisesRegex(update.UpdateError, "reserve"):
                update.Runner().child(["fake"], cwd=self.root, env={}, timeout=60,
                                      reserve_check=mock.Mock(side_effect=update.UpdateError("reserve")))
        kill.assert_called_once_with(1234, update.signal.SIGINT)
        process.wait.assert_called_once_with(timeout=180)

    def test_real_health_check_requires_authenticated_provenance_and_exact_mount(self):
        candidate = release(self.root, "2026-01-01")
        self.args.health_timeout = 1
        coordinator = update.Coordinator(self.args, self.runner)
        states = [{"State": {"Health": {"Status": "healthy"}},
                   "Config": {"Labels": {"com.docker.compose.service": service}},
                   "Mounts": [{"Destination": "/var/lib/mysql", "Source": str(candidate.datadir)}]}
                  for service in ("mysql", "ojs-api")]
        opener = mock.Mock()
        responses = iter([io.BytesIO(b'{"status":"ok"}'), io.BytesIO(json.dumps({
            "latest_snapshot": {"snapshot_date": candidate.date, "source_sha256": candidate.source_sha256,
                                "build_sql_sha256": candidate.build_sql_sha256}}).encode())])
        output = HeartbeatOutput()
        progress = update.Progress

        def blocked_request(*args, **kwargs):
            self.assertTrue(output.heartbeat.wait(2), "no heartbeat during blocked API health request")
            return next(responses)

        opener.open.side_effect = blocked_request
        with mock.patch.object(coordinator, "credentials", return_value=("client", "secret")), \
             mock.patch.object(coordinator, "compose", return_value="mysql-id\napi-id"), \
             mock.patch.object(coordinator, "command", return_value=json.dumps(states)), \
             mock.patch.object(update, "build_opener", return_value=opener), \
             mock.patch.object(update, "Progress", side_effect=lambda label, **kwargs:
                               progress(label, interval=0.01, **kwargs)), \
             contextlib.redirect_stderr(output):
            coordinator.check_health(candidate, candidate.datadir)
        request = opener.open.call_args_list[-1].args[0]
        self.assertEqual(request.full_url, self.args.api_url + "/meta")
        self.assertTrue(request.get_header("Authorization").startswith("Basic "))
        events = progress_events(output.getvalue())
        self.assertTrue(any(event["event"] == "heartbeat" for event in events))
        self.assertTrue(all(event["stage"] == "update-api-health" for event in events))
        self.assertEqual(events[-1]["event"], "completed")
        self.assertNotIn("secret", output.getvalue())
        self.assertNotIn(request.get_header("Authorization"), output.getvalue())
        self.assertNotIn(self.args.api_url, output.getvalue())

    def test_real_health_check_rejects_wrong_provenance(self):
        candidate = release(self.root, "2026-01-01")
        self.args.health_timeout = 0.001
        coordinator = update.Coordinator(self.args, self.runner)
        states = [{"State": {"Health": {"Status": "healthy"}},
                   "Config": {"Labels": {"com.docker.compose.service": service}},
                   "Mounts": [{"Destination": "/var/lib/mysql", "Source": str(candidate.datadir)}]}
                  for service in ("mysql", "ojs-api")]
        opener = mock.Mock()
        opener.open.side_effect = lambda *args, **kwargs: io.BytesIO(b'{"latest_snapshot":{"snapshot_date":"1900-01-01"}}')
        with mock.patch.object(coordinator, "credentials", return_value=("client", "secret")), \
             mock.patch.object(coordinator, "compose", return_value="mysql-id\napi-id"), \
             mock.patch.object(coordinator, "command", return_value=json.dumps(states)), \
             mock.patch.object(update, "build_opener", return_value=opener):
            with self.assertRaisesRegex(update.UpdateError, "verification timed out"):
                coordinator.check_health(candidate, candidate.datadir)

    def test_cold_database_verification_checks_clean_tables_and_provenance(self):
        candidate = release(self.root, "2026-01-01")
        coordinator = update.Coordinator(self.args, self.runner)
        coordinator.env["MYSQL_ROOT_PASSWORD"] = "test-only"
        coordinator.env["OJS_MYSQL_IMAGE"] = "mysql:8.4.0"
        server = mock.Mock()
        server.execute.side_effect = ["\n".join(update.run_pipeline.CLEAN_EXPORT_TABLES + ("ojs_pipeline_metadata",)),
                                      "\t".join((candidate.date, candidate.source_sha256, candidate.build_sql_sha256))]
        counts = update.run_pipeline.BuildCounts(2, 1, 2, 1, 1, 0, 3)
        tools = update.run_pipeline.MySQLTools("mysqld", "mysql", "mysqladmin", "mysqldump", "8.4.0")
        with mock.patch.object(update.run_pipeline.MySQLTools, "discover", return_value=tools), \
             mock.patch.object(update.run_pipeline, "MySQLServer", return_value=server), \
             mock.patch.object(update.run_pipeline, "validate_database", return_value=(counts, "8.4.0")):
            coordinator.verify_database(candidate)
        server.start.assert_called_once_with("test-only")
        server.shutdown.assert_called_once_with("test-only")

    def real_coordinator(self, host_version="8.4.1", image="mysql:8.4.0"):
        coordinator = update.Coordinator(self.args, self.runner)
        coordinator.env["OJS_MYSQL_IMAGE"] = image
        tools = update.run_pipeline.MySQLTools("mysqld", "mysql", "mysqladmin", "mysqldump", host_version)
        patch = mock.patch.object(update.run_pipeline.MySQLTools, "discover", return_value=tools)
        discovery = patch.start()
        self.addCleanup(patch.stop)
        return coordinator, discovery

    def test_patch_mismatch_rejects_candidate_before_cold_start_or_live_stop(self):
        old, candidate = self.old_and_new()
        coordinator, _ = self.real_coordinator()
        with mock.patch.object(update.run_pipeline, "MySQLServer") as server:
            with self.assertRaisesRegex(update.UpdateError, "host MySQL 8.4.1 does not match serving image patch 8.4.0"):
                coordinator.execute()
        server.assert_not_called()
        self.assertEqual(update.live_directory(self.clean), old.datadir)
        self.assertFalse(any("compose" in command for command, _ in self.runner.commands))

    def test_patch_mismatch_rejects_direct_cold_verification(self):
        candidate = release(self.root, "2026-01-01")
        coordinator, _ = self.real_coordinator()
        with mock.patch.object(update.run_pipeline, "MySQLServer") as server:
            with self.assertRaisesRegex(update.UpdateError, "does not match"):
                coordinator.verify_database(candidate)
        server.assert_not_called()

    def test_same_live_restart_also_checks_exact_patch(self):
        candidate = release(self.root, "2026-01-01")
        self.pointer("mysql-live", candidate)
        self.pointer("mysql-current", candidate)
        coordinator, _ = self.real_coordinator()
        with self.assertRaisesRegex(update.UpdateError, "does not match"):
            coordinator.execute()
        self.assertFalse(any("compose" in command for command, _ in self.runner.commands))

    def test_pending_recovery_checks_patch_before_changing_pointer(self):
        old, candidate = self.old_and_new()
        self.pointer("mysql-live", candidate)
        self.runner.mount = candidate.datadir
        coordinator, _ = self.real_coordinator()
        state = {"schema_version": 1, "phase": "switching", "candidate": candidate.datadir.name,
                 "previous": old.datadir.name, "snapshot_date": candidate.date}
        update.atomic_json(coordinator.journal, state)
        with self.assertRaisesRegex(update.UpdateError, "does not match"):
            coordinator.recover()
        self.assertEqual(update.live_directory(self.clean), candidate.datadir)
        self.assertEqual(update.read_json(coordinator.journal), state)
        self.assertFalse(any("compose" in command for command, _ in self.runner.commands))

    def test_unpinned_or_non_84_image_is_rejected(self):
        coordinator, discovery = self.real_coordinator()
        for image in ("mysql:8.4", "mysql:latest", "mysql:8.0.40", "mysql", "mysql:8.4.0@sha256:abc"):
            with self.subTest(image=image):
                coordinator.env["OJS_MYSQL_IMAGE"] = image
                with self.assertRaisesRegex(update.UpdateError, "exact numeric MySQL 8.4 patch"):
                    coordinator.compatible_mysql_tools()
        discovery.assert_not_called()

    def test_matching_patch_tools_are_reused_within_operation(self):
        coordinator, discovery = self.real_coordinator(host_version="8.4.3", image="registry.example:5000/mysql:8.4.3")
        first = coordinator.compatible_mysql_tools()
        self.assertIs(coordinator.compatible_mysql_tools(), first)
        discovery.assert_called_once_with()

    def test_real_unchanged_check_does_not_discover_mysql_tools(self):
        current = release(self.root, "2026-01-01")
        self.pointer("mysql-live", current)
        self.args.publish_only = False
        coordinator, discovery = self.real_coordinator()
        self.runner.probe = {"needs_download": False,
                             "known_snapshot": str(self.raw / f"pkpbeacon-{current.date}.sql.gz")}
        with mock.patch.object(update.download_beacon, "read_validated_metadata", return_value={"uncompressed_sha256": current.source_sha256}):
            self.assertEqual(coordinator.execute()["status"], "unchanged")
        discovery.assert_not_called()

    def test_patch_mismatch_on_new_source_prevents_all_child_commands(self):
        self.args.publish_only = False
        coordinator, discovery = self.real_coordinator()
        with self.assertRaisesRegex(update.UpdateError, "does not match"):
            coordinator.execute()
        discovery.assert_called_once_with()
        self.assertFalse(self.runner.children)
        self.assertFalse(any("compose" in command for command, _ in self.runner.commands))

    def test_runner_exposes_auth_failure_without_secret_stderr(self):
        result = subprocess.CompletedProcess(["python"], 1, stdout="", stderr="error: Beacon authentication failed\nprivate detail")
        with mock.patch.object(update.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(update.UpdateError, "^Beacon authentication failed; check the configured credentials file$"):
                update.Runner().run(["python"], cwd=self.root, env={})

    def test_runner_timeout_suppresses_command_and_captured_output(self):
        command = ["python", "--url", "https://example.invalid/?token=private-url-token",
                   "--credentials-file", "/private/private-credentials.ini"]
        timeout = subprocess.TimeoutExpired(command, 1, output=b"private-output", stderr=b"private-stderr")
        with mock.patch.object(update.subprocess, "run", side_effect=timeout):
            with self.assertRaises(update.UpdateError) as raised:
                update.Runner().run(command, cwd=self.root, env={"PRIVATE": "private-env"})
        self.assertEqual(str(raised.exception), update.COMMAND_TIMEOUT_MESSAGE)
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_check_timeout_logs_failure_without_url_or_credentials_path(self):
        private_url = "https://example.invalid/?token=private-url-token"
        private_path = "/private/private-credentials.ini"
        timeout = subprocess.TimeoutExpired(
            ["python", "--url", private_url, "--credentials-file", private_path], 1,
            output=b"private-output", stderr=b"private-stderr")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr), \
             mock.patch.object(update.subprocess, "run", side_effect=timeout), \
             mock.patch.object(update.signal, "signal"):
            status = update.main(["--project-root", str(self.root), "--check", "--url", private_url,
                                  "--credentials-file", private_path])
        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("error: " + update.COMMAND_TIMEOUT_MESSAGE, stderr.getvalue())
        self.assertEqual(progress_events(stderr.getvalue())[-1]["event"], "failed")
        for private in (private_url, private_path, "private-url-token", "private-output", "private-stderr"):
            self.assertNotIn(private, stderr.getvalue())

    def test_nested_timeout_uses_sanitized_main_fallback(self):
        timeout = subprocess.TimeoutExpired(["private-command", "private-url-token"], 1)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), \
             mock.patch.object(update.Coordinator, "execute", side_effect=timeout), \
             mock.patch.object(update.signal, "signal"):
            status = update.main(["--project-root", str(self.root), "--check"])
        self.assertEqual(status, 1)
        self.assertEqual(stderr.getvalue(), "error: " + update.COMMAND_TIMEOUT_MESSAGE + "\n")


if __name__ == "__main__":
    unittest.main()
