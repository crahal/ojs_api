#!/usr/bin/env python3
"""Send a small, read-only recovery snapshot through Gmail.

Only systemd properties, a bounded journal tail, filesystem free space, and
host memory are observed. This program never opens a database or loads .env.
Journal text and exceptions are untrusted and must never enter the report.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from email.message import EmailMessage
import fcntl
import json
import math
import os
from pathlib import Path
import re
import selectors
import shutil
import smtplib
import ssl
import subprocess
import sys
import tempfile
import time

import progress_llm
from progress_observations import observed_activity, recent_milestones
from progress_summary import overview, stage_info


SERVICE = "ojs-api-update.service"
DEFAULT_STATE = Path("/var/lib/ojs-api-report/last-sent.json")
JOURNAL_LINES = 300
MILESTONE_LINES = 128
# A separate bounded, filtered tail retains phase transitions when hours of
# heartbeats have pushed them out of the ordinary tail. Never read SQL content.
MILESTONE_PATTERN = (
    r'^\[progress\] (?=.*"event"\s*:\s*"(?:started|completed|failed|sql-stage)")'
    r'(?=.*"stage"\s*:\s*"(?:recovery-rollback|recovery-metadata-audit|'
    r'temporal finalization phase|source-index phase|sql-import|'
    r'update-build|update-recover-exact-keys)")'
)
STALE_SECONDS = 300
MIN_SEND_INTERVAL_SECONDS = 11 * 60 * 60
MAX_STATE_BYTES = 16 * 1024
PROPERTIES = (
    "ActiveState", "SubState", "Result", "InvocationID", "MainPID",
    "MemoryCurrent", "ExecMainCode", "ExecMainStatus",
)
# Closed vocabularies deliberately exclude arbitrary query labels or payloads.
STAGES = frozenset({
    "recovery-rollback", "recovery-metadata-audit", "snapshot-checksum",
    "file-checksum", "source-index phase", "temporal finalization phase",
    "metadata-shard", "sql-import", "clean-export", "mysql-initialize",
    "mysql-start", "mysql-shutdown", "mysql-shutdown-wait",
    "validate-raw-tables", "validate-counts-and-identities",
    "update-source-check", "update-release-checksums", "update-api-health",
    "update-rollback", "update-promotion", "update-cold-verification",
    "update-cleanup", "update-graceful-shutdown", "update-recover-exact-keys",
    "update-download", "update-build", "beacon-source-check",
    "beacon-archive-validation", "beacon-transfer", "beacon-connect",
    "beacon-legacy-checksum",
})
SUPERVISOR_STAGES = frozenset({
    "update-recover-exact-keys", "update-download", "update-build",
})
STEPS = frozenset({
    "schema", "context_normalization", "source_index",
    "source_identity_validation", "metadata_extraction", "exact_keys",
    "effective_keys", "noisy_keys", "touched_keys", "existing_candidates",
    "initial_assignments", "deduplication", "article_merges",
    "source_reconciliation", "source_indexes", "key_reconciliation",
    "raw_storage_release", "article_rollup", "canonical_payload",
    "article_state", "article_events", "article_updates", "snapshot_counts",
    "stage_cleanup", "sql_complete", "wait-for-child-rollback",
    "supervise-child", "clean-export", "release-provenance", "plan-cleanup",
    "cleanup-disabled", "remove-owned-artifacts", "waiting-for-mysql",
    "feeding-mysql", "waiting-for-mysqldump", "decompress-and-hash",
    "sql-header-and-footer", "receive", "sync-download",
})
EVENTS = frozenset({
    "started", "heartbeat", "completed", "failed", "sql-stage",
    "dedup-pass", "metadata-rows", "range-verified", "export-verified",
})
COUNTERS = (
    "remaining_rows", "pending_transactions", "rollback_transactions",
    "completed_ranges", "total_ranges", "checked_rows", "covered_source_rows",
    "extracted_rows", "processed_bytes", "total_bytes", "compressed_bytes",
    "pass_number", "changed_rows", "deleted_artifacts", "deleted_bytes",
    "total_artifacts",
)
ACTIVE_STATES = frozenset({
    "active", "activating", "deactivating", "inactive", "failed",
    "reloading", "maintenance", "refreshing",
})
SUB_STATES = frozenset({
    "running", "start", "start-pre", "start-post", "exited", "dead",
    "failed", "stop", "stop-sigterm", "stop-sigkill", "stop-post",
    "final-sigterm", "final-sigkill", "auto-restart", "reload",
    "reload-signal", "reload-notify", "cleaning", "condition",
})
RESULTS = frozenset({
    "success", "resources", "protocol", "timeout", "exit-code", "signal",
    "core-dump", "watchdog", "start-limit-hit", "oom-kill", "exec-condition",
})
INVOCATION = re.compile(r"[0-9a-f]{32}\Z")
ADDRESS = re.compile(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")


class ReportError(Exception):
    """An error whose external details must not be printed."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_stamp(value) -> datetime | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def number(value) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if 0 <= value <= 10 ** 24 and math.isfinite(value):
        return value
    return None


def integer(value) -> int | None:
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,20}", value):
        return int(value)
    return None


def diagnostic(command: list[str], *, timeout: float = 15,
               limit: int = 2 * 1024 * 1024) -> str:
    """Bound time and captured bytes; discard stderr without displaying it."""
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          stdin=subprocess.DEVNULL) as process:
        assert process.stdout is not None
        output = bytearray()
        deadline = time.monotonic() + timeout
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise ReportError()
                    fragment = os.read(process.stdout.fileno(), 16384)
                    if not fragment:
                        break
                    output.extend(fragment)
                    if len(output) > limit:
                        raise ReportError()
            code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
            if code:
                raise ReportError()
            return output.decode("utf-8", errors="replace")
        finally:
            if process.poll() is None:
                # This is solely the diagnostic child created above.
                process.kill()
                process.wait()


def parse_service(output: str) -> dict:
    service = {}
    vocabularies = {"ActiveState": ACTIVE_STATES, "SubState": SUB_STATES,
                    "Result": RESULTS}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key not in PROPERTIES:
            continue
        if key in vocabularies:
            if value in vocabularies[key]:
                service[key] = value
        elif key == "InvocationID":
            if INVOCATION.fullmatch(value) and value != "0" * 32:
                service[key] = value
        else:
            parsed = integer(value)
            # systemd uses UINT64_MAX when cgroup accounting is unavailable.
            if parsed is not None and parsed < 2 ** 64 - 1:
                service[key] = parsed
    return service


def read_service() -> dict:
    return parse_service(diagnostic([
        "systemctl", "show", SERVICE, "--no-pager",
        "--property=" + ",".join(PROPERTIES),
    ], limit=16384))


def parse_events(output: str, invocation: str) -> list[dict]:
    if not INVOCATION.fullmatch(invocation):
        return []
    events = []
    for line in output.splitlines()[-JOURNAL_LINES:]:
        if len(line) > 16384:
            continue
        try:
            entry = json.loads(line)
            if not isinstance(entry, dict) or entry.get("_SYSTEMD_INVOCATION_ID") != invocation:
                continue
            message = entry.get("MESSAGE")
            if not isinstance(message, str) or not message.startswith("[progress] ") or len(message) > 4096:
                continue
            payload = json.loads(message[len("[progress] "):])
            if not isinstance(payload, dict):
                continue
            stage, event = payload.get("stage"), payload.get("event")
            if not isinstance(stage, str) or stage not in STAGES or not isinstance(event, str) or event not in EVENTS:
                continue
            micros = integer(entry.get("__REALTIME_TIMESTAMP"))
            if micros is None:
                continue
            logged_at = datetime.fromtimestamp(micros / 1_000_000, timezone.utc)
            safe = {"stage": stage, "event": event, "logged_at": stamp(logged_at)}
            step = payload.get("step")
            if isinstance(step, str) and step in STEPS:
                safe["step"] = step
            for key in (*COUNTERS, "elapsed_seconds", "idle_seconds", "process_cpu_seconds"):
                value = number(payload.get(key))
                if value is not None:
                    safe[key] = value
            pid = payload.get("process_pid")
            if isinstance(pid, int) and not isinstance(pid, bool) and 0 < pid < 2 ** 31:
                safe["process_pid"] = pid
            # --grep with a line limit can return newest-first. Preserve the
            # journal's microsecond ordering before formatting times to seconds;
            # audit completion and the next SQL phase can share a second.
            events.append((micros, safe))
        except (ValueError, OverflowError, OSError, RecursionError):
            continue
    return [event for _, event in sorted(events, key=lambda item: item[0])]


def select_event(events: list[dict]) -> dict | None:
    # Parent supervisors and short control queries are not the actual recovery
    # phase. A stale substantive event remains explicitly labelled stale.
    substantive = [event for event in events if event["stage"] not in SUPERVISOR_STAGES]
    return (substantive or events or [None])[-1]


def free_bytes(path: Path) -> int | None:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


def host_memory() -> dict:
    try:
        with Path("/proc/meminfo").open(encoding="ascii") as stream:
            content = stream.read(32768)
        memory = {}
        for line in content.splitlines():
            match = re.fullmatch(r"(MemTotal|MemAvailable):\s+([0-9]{1,20}) kB", line)
            if match:
                memory[match[1]] = int(match[2]) * 1024
        return memory
    except (OSError, UnicodeError):
        return {}


def collect_snapshot(project_root: Path) -> dict:
    snapshot = {"observed_at": stamp(utc_now()), "service": {}, "event": None,
                "milestones": [], "activity": {},
                "warnings": [], "root_free": free_bytes(Path("/")),
                "data_free": free_bytes(project_root / "data"), "memory": host_memory()}
    try:
        service = read_service()
        snapshot["service"] = service
    except (OSError, subprocess.SubprocessError, ReportError):
        snapshot["warnings"].append("Service properties unavailable.")
        return snapshot
    invocation = service.get("InvocationID")
    if not invocation:
        snapshot["warnings"].append("No current invocation ID; historical journal entries excluded.")
        return snapshot
    try:
        output = diagnostic([
            "journalctl", "--unit=" + SERVICE, "--no-pager", "--output=json",
            "--output-fields=MESSAGE,_SYSTEMD_INVOCATION_ID,__REALTIME_TIMESTAMP",
            "--lines=" + str(JOURNAL_LINES), "_SYSTEMD_INVOCATION_ID=" + invocation,
        ])
        events = parse_events(output, invocation)
        milestones = []
        try:
            milestone_output = diagnostic([
                "journalctl", "--unit=" + SERVICE, "--no-pager", "--output=json",
                "--output-fields=MESSAGE,_SYSTEMD_INVOCATION_ID,__REALTIME_TIMESTAMP",
                "--lines=" + str(MILESTONE_LINES), "--grep=" + MILESTONE_PATTERN,
                "_SYSTEMD_INVOCATION_ID=" + invocation,
            ], timeout=10, limit=512 * 1024)
            milestones = recent_milestones(parse_events(milestone_output, invocation))
        except (OSError, subprocess.SubprocessError, ReportError):
            snapshot["warnings"].append("Earlier phase milestones unavailable; recent observations retained.")
        # Do not combine an old run's journal with a new run's service state.
        latest_service = read_service()
        snapshot["service"] = latest_service
        if latest_service.get("InvocationID") == invocation:
            snapshot["event"] = select_event(events)
            snapshot["milestones"] = milestones
            snapshot["activity"] = observed_activity(
                events, snapshot["event"], observed_at=stamp(utc_now()))
        else:
            snapshot["warnings"].append("Service invocation changed during collection; stage omitted.")
    except (OSError, subprocess.SubprocessError, ReportError):
        snapshot["warnings"].append("Current-invocation journal unavailable or exceeded its bounded limit.")
    snapshot["observed_at"] = stamp(utc_now())
    return snapshot


def service_status(service: dict) -> tuple[str, str]:
    active, result = service.get("ActiveState"), service.get("Result")
    if active == "failed" or (active == "inactive" and result not in (None, "success")):
        return "Failed", "Systemd reports a service failure. Recovery completion is unverified."
    if active in {"active", "activating", "reloading", "refreshing"}:
        return "Running", "The recovery job is running; long-running oneshot jobs can remain in systemd's starting state."
    if active == "deactivating":
        return "Stopping", "The service is stopping; rollback or shutdown may still be running."
    if active == "inactive" and result == "success" and service.get("ExecMainCode") == 1:
        if service.get("ExecMainStatus") == 0:
            return "Job exited", "Systemd reports a successful service exit; recovery/data completion is unverified."
    if active == "inactive":
        return "Idle", "The service is inactive; no completed recovery is inferred."
    return "Unknown", "Current service status could not be established."


def read_state(path: Path) -> dict | None:
    try:
        with path.open(encoding="utf-8") as stream:
            content = stream.read(MAX_STATE_BYTES + 1)
        if len(content.encode("utf-8")) > MAX_STATE_BYTES:
            return None
        raw = json.loads(content)
        if not isinstance(raw, dict) or raw.get("schema") != 1:
            return None
        invocation, stage, step = raw.get("invocation"), raw.get("stage"), raw.get("step")
        if invocation is not None and (not isinstance(invocation, str) or not INVOCATION.fullmatch(invocation)):
            return None
        if stage is not None and (not isinstance(stage, str) or stage not in STAGES):
            return None
        if step is not None and (not isinstance(step, str) or step not in STEPS):
            return None
        sent_at, logged_at = parse_stamp(raw.get("sent_at")), parse_stamp(raw.get("logged_at"))
        if sent_at is None or not isinstance(raw.get("counters"), dict):
            return None
        if raw.get("logged_at") is not None and logged_at is None:
            return None
        return {"schema": 1, "invocation": invocation, "stage": stage, "step": step,
                "sent_at": stamp(sent_at), "logged_at": stamp(logged_at) if logged_at else None,
                "counters": {key: value for key in COUNTERS
                             if (value := number(raw["counters"].get(key))) is not None}}
    except (OSError, ValueError, UnicodeError, RecursionError):
        return None


def comparable_state(snapshot: dict, sent_at: datetime) -> dict:
    event = snapshot.get("event") or {}
    return {"schema": 1, "sent_at": stamp(sent_at),
            "invocation": snapshot["service"].get("InvocationID"),
            "stage": event.get("stage"), "step": event.get("step"),
            "logged_at": event.get("logged_at"),
            "counters": {key: event[key] for key in COUNTERS if key in event}}


def comparisons(snapshot: dict, previous: dict | None) -> list[str]:
    event = snapshot.get("event")
    if not previous or not event:
        return ["No comparable previously sent stage counters."]
    if (snapshot["service"].get("InvocationID") != previous.get("invocation")
            or event["stage"] != previous.get("stage")
            or event.get("step") != previous.get("step")):
        return ["Invocation or stage changed; counters are not compared across runs or stages."]
    current_at = parse_stamp(event.get("logged_at"))
    previous_at = parse_stamp(previous.get("logged_at"))
    if current_at is None or previous_at is None or current_at <= previous_at:
        return ["No newer stage observation since the previous successful email."]
    changes = []
    for key in COUNTERS:
        if key in event and key in previous["counters"]:
            delta = event[key] - previous["counters"][key]
            changes.append(f"{key}: {delta:+,}")
    if not changes:
        return ["No comparable counters in the two stage observations."]
    return ["Counter changes since email accepted at " + previous["sent_at"] + ":",
            "; ".join(changes),
            "Changes are observations, not a completion estimate."]


def quantity(value) -> str:
    return "unavailable" if value is None else f"{value / (1024 ** 3):.2f} GiB"


def llm_evidence(snapshot: dict, previous: dict | None) -> dict:
    """Build a second, narrow allowlist for data allowed to leave this host.

    Do not pass rendered reports: they contain local identifiers and paths.
    All strings below are constants or validated closed-vocabulary labels.
    """
    source_service = snapshot.get("service") or {}
    service = {}
    for key, vocabulary in (("ActiveState", ACTIVE_STATES),
                            ("SubState", SUB_STATES), ("Result", RESULTS)):
        value = source_service.get(key)
        if isinstance(value, str) and value in vocabulary:
            service[key] = value
    for key in ("ExecMainCode", "ExecMainStatus"):
        if number(source_service.get(key)) is not None:
            service[key] = source_service[key]
    status, explanation = service_status(service)
    memory = snapshot.get("memory") or {}
    evidence = {
        "service_status": status,
        "service_observation": explanation,
        # Translate systemd locally: 'dead' means inactive, and 'success' on a
        # running oneshot is not a pipeline outcome. Do not ask the model to
        # infer process health from these raw implementation labels.
        "stage": None,
        "comparison": {"status": "unavailable"},
        "resources_bytes": {
            "root_free": number(snapshot.get("root_free")),
            "data_free": number(snapshot.get("data_free")),
            "host_memory_available": number(memory.get("MemAvailable")),
            "host_memory_total": number(memory.get("MemTotal")),
            "service_memory": number(source_service.get("MemoryCurrent")),
        },
        "collection_incomplete": bool(snapshot.get("warnings")),
        "overall_completion_verified": False,
        "api_availability_verified": False,
    }
    event = snapshot.get("event") or {}
    stage, event_type, step = event.get("stage"), event.get("event"), event.get("step")
    if not isinstance(stage, str) or stage not in STAGES:
        return evidence
    observed = parse_stamp(snapshot.get("observed_at"))
    logged = parse_stamp(event.get("logged_at"))
    age = (observed - logged).total_seconds() if observed and logged else -1
    evidence["stage"] = {
        "name": stage,
        "event": event_type if isinstance(event_type, str) and event_type in EVENTS else None,
        "step": step if isinstance(step, str) and step in STEPS else None,
        "freshness": "unknown" if age < 0 else "stale" if age > STALE_SECONDS else "recent",
        "age_seconds": age if age >= 0 else None,
        "idle_seconds": number(event.get("idle_seconds")),
        "counters": {key: value for key in COUNTERS
                     if (value := number(event.get(key))) is not None},
    }
    label, meaning = stage_info(stage, evidence["stage"]["step"])
    evidence["stage"].update(label=label, meaning=meaning)
    evidence["instrumentation"] = (
        "idle_seconds is time without a reported counter change or milestone, not CPU idle time. "
        "Long SQL statements may emit no intermediate row counters. No counters does not "
        "mean the stage details are missing or the SQL process is dead."
    )
    activity = snapshot.get("activity") or {}
    activity_at = parse_stamp(activity.get("logged_at"))
    activity_age = (observed - activity_at).total_seconds() if observed and activity_at else -1
    cpu, interval = number(activity.get("cpu_seconds")), number(activity.get("interval_seconds"))
    if cpu is not None and interval is not None and interval > 0 and 0 <= activity_age <= STALE_SECONDS:
        evidence["process_activity"] = {
            "cpu_seconds_increase": cpu, "interval_seconds": interval,
            "meaning": "CPU use by the monitored process, not a count of rows or proof that a SQL statement has returned.",
        }
    evidence["milestones"] = []
    for milestone in (snapshot.get("milestones") or [])[-4:]:
        name, kind, substep = milestone.get("stage"), milestone.get("event"), milestone.get("step")
        if (not isinstance(name, str) or name not in STAGES or not isinstance(kind, str)
                or kind not in {"started", "completed", "failed", "sql-stage"}):
            continue
        if not isinstance(substep, str) or substep not in STEPS:
            substep = None
        milestone_at = parse_stamp(milestone.get("logged_at"))
        if not observed or not milestone_at or milestone_at > observed:
            continue
        milestone_label, _ = stage_info(name, substep)
        evidence["milestones"].append({"stage": name, "label": milestone_label, "event": kind})
    if not previous:
        return evidence
    invocation = source_service.get("InvocationID")
    if (not isinstance(invocation, str) or not INVOCATION.fullmatch(invocation)
            or invocation != previous.get("invocation")):
        evidence["comparison"] = {"status": "different_run"}
        return evidence
    if stage != previous.get("stage") or step != previous.get("step"):
        old_stage, old_step = previous.get("stage"), previous.get("step")
        old_label, _ = stage_info(old_stage, old_step)
        evidence["comparison"] = {"status": "stage_changed_same_run", "previous_stage_label": old_label,
                                  "meaning": "The same recovery run has changed stage; counters are not comparable across stages."}
        return evidence
    old_logged = parse_stamp(previous.get("logged_at"))
    if not logged or not old_logged or logged <= old_logged:
        evidence["comparison"] = {"status": "no_newer_observation"}
        return evidence
    old_counters = previous.get("counters") or {}
    deltas = {}
    for key, value in evidence["stage"]["counters"].items():
        old = number(old_counters.get(key))
        if old is not None:
            deltas[key] = value - old
    evidence["comparison"] = {
        "status": "same_run_and_stage" if deltas else "no_shared_counters",
        "observation_interval_seconds": (logged - old_logged).total_seconds(),
        "counter_changes": deltas,
    }
    return evidence


def ai_assessment(snapshot: dict, previous: dict | None, state_file: Path) -> str:
    if os.environ.get("OJS_PROGRESS_LLM_ENABLED") != "1":
        raise progress_llm.SummaryUnavailable()
    directory = os.environ.get("CREDENTIALS_DIRECTORY")
    if not directory:
        raise progress_llm.SummaryUnavailable()
    return progress_llm.generate_summary(
        llm_evidence(snapshot, previous),
        credential_path=Path(directory) / "openai-api-key",
        budget_path=state_file.parent / "llm-budget.json",
    )


def render_report(snapshot: dict, previous: dict | None = None) -> tuple[str, str]:
    service, event = snapshot["service"], snapshot.get("event")
    status, description = service_status(service)
    label, _ = stage_info((event or {}).get("stage"), (event or {}).get("step"))
    observed, logged = parse_stamp(snapshot.get("observed_at")), parse_stamp((event or {}).get("logged_at"))
    age = (observed - logged).total_seconds() if observed and logged else -1
    if status != "Running" or not 0 <= age <= STALE_SECONDS or (event or {}).get("event") in {"completed", "failed"}:
        label = "Last stage: " + label
    subject = f"[OJS recovery] {status} | {label} - {snapshot['observed_at']}"
    lines = ["OJS recovery progress report", "Observed (UTC): " + snapshot["observed_at"],
             "", *overview(snapshot, previous), "", "Technical details", description, "Service: " + SERVICE,
             "Systemd: " + "; ".join(f"{key}={service.get(key, 'unavailable')}"
                                        for key in ("ActiveState", "SubState", "Result", "MainPID")),
             "Invocation: " + service.get("InvocationID", "unavailable"), ""]
    if service.get("SubState") == "dead":
        lines.append("Systemd's 'dead' label means the service is inactive; it is not, by itself, a crash report.")
    if event:
        lines.append("Last observed stage: " + event["stage"])
        if event.get("step"):
            lines.append("Stage step: " + event["step"])
        lines.append("Stage event: " + event["event"] + " at " + event["logged_at"])
        observed, logged = parse_stamp(snapshot["observed_at"]), parse_stamp(event["logged_at"])
        age = (observed - logged).total_seconds() if observed and logged else -1
        if age < 0:
            lines.append("Freshness unknown: journal timestamp is ahead of the observation clock.")
        elif age > STALE_SECONDS:
            lines.append(f"STALE stage observation ({age:,.0f} seconds old); current stage is unconfirmed.")
        else:
            lines.append(f"Recent stage observation ({age:,.0f} seconds old).")
        if "idle_seconds" in event:
            lines.append(f"At that event, {event['idle_seconds']:,.0f} seconds since a counter change or milestone.")
        values = [f"{key}={event[key]:,}" for key in COUNTERS if key in event]
        if values:
            lines.append("Counters: " + "; ".join(values))
        if event["event"] == "completed":
            lines.append("This stage completed; that does not establish completion of the whole recovery.")
        elif event["event"] == "failed":
            lines.append("This stage reported failure; service status above is the current systemd observation.")
        lines.extend(["", *comparisons(snapshot, previous)])
    else:
        lines.append("No recognized progress event for the current invocation in the bounded journal tail.")
    lines.extend(["", "Current service memory: " + quantity(service.get("MemoryCurrent")),
                  "Current host memory available / total: " + quantity(snapshot["memory"].get("MemAvailable"))
                  + " / " + quantity(snapshot["memory"].get("MemTotal")),
                  "Disk free, root / project data: " + quantity(snapshot["root_free"])
                  + " / " + quantity(snapshot["data_free"]),
                  "", "Heartbeats indicate that the monitor is alive, not that SQL is advancing.",
                  "No overall percentage or ETA is inferred. Only systemd, bounded journal tails, and host resources were read."])
    lines.extend(snapshot["warnings"])
    return subject, "\n".join(lines) + "\n"


def add_assessment(body: str, assessment: str) -> str:
    """Keep measured status first; optional prose never replaces the facts."""
    heading, separator, details = body.partition("\nTechnical details\n")
    return heading + "\n" + assessment + "\n" + separator + details


def email_address(value: str | None) -> str:
    if not value or len(value) > 254 or ADDRESS.fullmatch(value) is None:
        raise ReportError()
    return value


def read_password(explicit: Path | None) -> str:
    if explicit is None:
        directory = os.environ.get("CREDENTIALS_DIRECTORY")
        if not directory:
            raise ReportError()
        explicit = Path(directory) / "gmail-app-password"
    with explicit.open(encoding="ascii") as stream:
        value = stream.read(513)
    password = value.strip().replace(" ", "")
    if len(value) > 512 or not password or len(password) > 256 or not password.isprintable():
        raise ReportError()
    return password


def send_email(sender: str, recipient: str, password: str, subject: str, body: str) -> None:
    message = EmailMessage()
    message["From"], message["To"], message["Subject"] = sender, recipient, subject
    message.set_content(body)
    smtp = smtplib.SMTP("smtp.gmail.com", 587, timeout=20)
    try:
        smtp.ehlo()
        smtp.starttls(context=ssl.create_default_context())
        smtp.ehlo()
        smtp.login(sender, password)
        if smtp.send_message(message, from_addr=sender, to_addrs=[recipient]):
            raise ReportError()
    finally:
        # A disconnect after DATA acceptance must not undo acceptance or cause
        # an unnecessary resend. Failure before acceptance still propagates.
        try:
            smtp.quit()
        except Exception:
            try:
                smtp.close()
            except Exception:
                pass


@contextmanager
def report_lock(state_file: Path):
    state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(str(state_file) + ".lock",
                         os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(descriptor)


def write_state(path: Path, state: dict) -> None:
    encoded = (json.dumps(state, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    if len(encoded) > MAX_STATE_BYTES:
        raise ReportError()
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", prefix=".last-sent-", dir=path.parent,
                                         delete=False) as stream:
            temporary = Path(stream.name)
            os.fchmod(stream.fileno(), 0o600)
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print safe preview; do not read credentials, send, or write state.")
    mode.add_argument("--preview-llm", action="store_true", help="Explicit paid AI preview; no email or delivery-state change; normal AI attempt caps apply.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--password-file", type=Path, help="Explicit credential path for manual use.")
    parser.add_argument("--force", action="store_true", help="Send even if a report was accepted less than 11 hours ago.")
    args = parser.parse_args(argv)
    accepted = False
    try:
        if args.dry_run:
            snapshot = collect_snapshot(args.project_root)
            subject, body = render_report(snapshot, read_state(args.state_file))
            print("Subject: " + subject + "\n\n" + body, end="")
            return 0
        if args.preview_llm:
            with report_lock(args.state_file) as acquired:
                if not acquired:
                    print("AI preview unavailable: another report is in progress.", file=sys.stderr)
                    return 1
                try:
                    summary = ai_assessment(collect_snapshot(args.project_root),
                                            read_state(args.state_file), args.state_file)
                except progress_llm.SummaryUnavailable as error:
                    print(f"AI preview unavailable: {error}", file=sys.stderr)
                    return 1
                except Exception:
                    print("AI preview unavailable; check private configuration, API access, or attempt limits.", file=sys.stderr)
                    return 1
                print("AI assessment (interpretation; exact figures remain in the factual report):\n" + summary)
                return 0
        sender = email_address(os.environ.get("OJS_PROGRESS_EMAIL_FROM"))
        recipient = email_address(os.environ.get("OJS_PROGRESS_EMAIL_TO"))
        with report_lock(args.state_file) as acquired:
            if not acquired:
                print("Progress report skipped: another report is in progress.")
                return 0
            previous = read_state(args.state_file)
            sent_at = parse_stamp(previous.get("sent_at")) if previous else None
            if not args.force and sent_at is not None and (utc_now() - sent_at).total_seconds() < MIN_SEND_INTERVAL_SECONDS:
                print("Recent progress report already sent; skipped.")
                return 0
            snapshot = collect_snapshot(args.project_root)
            subject, body = render_report(snapshot, previous)
            password = read_password(args.password_file)
            if os.environ.get("OJS_PROGRESS_LLM_ENABLED") == "1":
                try:
                    summary = ai_assessment(snapshot, previous, args.state_file)
                    body = add_assessment(body, "AI interpretation (exact figures and status are recorded above):\n" + summary)
                    print("AI assessment generated; factual observations retained.")
                except progress_llm.SummaryUnavailable as error:
                    body = add_assessment(body, "Optional AI interpretation unavailable; the recorded report is unaffected.")
                    print(f"AI assessment unavailable; sending factual report. {error}")
                except Exception:
                    body = add_assessment(body, "Optional AI interpretation unavailable; the recorded report is unaffected.")
                    print("AI assessment unavailable; sending factual report.")
            send_email(sender, recipient, password, subject, body)
            accepted = True
            write_state(args.state_file, comparable_state(snapshot, utc_now()))
        print("Progress email accepted by SMTP; comparison state saved.")
        return 0
    except Exception:
        if accepted:
            print("Progress email accepted by SMTP, but comparison state could not be saved.", file=sys.stderr)
        else:
            print("Progress email was not sent; check reporting configuration and service diagnostics.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
