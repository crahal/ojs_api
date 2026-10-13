"""Pure interpretations of sanitized, current-invocation progress events."""
from __future__ import annotations

from datetime import datetime, timezone
import math


CPU_WINDOW_SECONDS = 60
SUPERVISORS = frozenset({"update-recover-exact-keys", "update-build", "update-download"})
HIGH_LEVEL_STAGES = frozenset({
    "recovery-rollback", "recovery-metadata-audit", "temporal finalization phase",
    "source-index phase", "sql-import",
})
MILESTONE_EVENTS = frozenset({"started", "completed", "failed", "sql-stage"})
MILESTONE_COUNTERS = ("checked_rows", "completed_ranges", "total_ranges", "remaining_rows")
AUDIT_SCAN_NOTE = (
    "Metadata audit scan completed; final safety checks and overall recovery are not confirmed."
)


def _timestamp(value) -> datetime | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
    except (ValueError, OverflowError):
        return None


def _sample(event: dict) -> tuple | None:
    stage, step = event.get("stage"), event.get("step")
    pid, cpu = event.get("process_pid"), event.get("process_cpu_seconds")
    logged = _timestamp(event.get("logged_at"))
    if (not isinstance(stage, str) or not stage or (step is not None and not isinstance(step, str))
            or type(pid) is not int or pid <= 0 or isinstance(cpu, bool)
            or not isinstance(cpu, (int, float)) or not 0 <= cpu <= 10 ** 24
            or not math.isfinite(cpu) or logged is None):
        return None
    return stage, step, pid, cpu, logged


def observed_activity(events: list[dict], selected_event: dict | None, *, observed_at: str) -> dict:
    """Measure recent process CPU consumption, never SQL work or total lifetime CPU.

    Only a contiguous epoch ending at the selected event is comparable. Known
    supervisor chatter is separate from work-stage observations; a supervisor
    failure ends comparability. Zero CPU change is a valid observation, not
    evidence that SQL is stuck. The caller supplies time to keep this pure.
    """
    if not isinstance(selected_event, dict):
        return {}
    latest, observed = _sample(selected_event), _timestamp(observed_at)
    if latest is None or observed is None or latest[0] in SUPERVISORS:
        return {}
    age = (observed - latest[4]).total_seconds()
    if not 0 <= age <= CPU_WINDOW_SECONDS:
        return {}
    # The selected event may occur in both bounded journal queries. Identify
    # its last identical sample without assuming object identity or sorting
    # away a clock reversal in the supplied event sequence.
    anchor = next((position for position in range(len(events) - 1, -1, -1)
                   if isinstance(events[position], dict) and _sample(events[position]) == latest), None)
    if anchor is None:
        return {}
    earliest = latest
    for event in reversed(events[:anchor]):
        if not isinstance(event, dict):
            break
        if event.get("stage") in SUPERVISORS:
            if event.get("event") == "failed":
                break
            continue
        sample = _sample(event)
        if sample is None or sample[:3] != latest[:3]:
            break
        if sample[4] == earliest[4] and sample[3] == earliest[3]:
            continue  # An identical observation cannot add time or activity.
        if sample[4] >= earliest[4] or sample[3] > earliest[3]:
            break
        if (latest[4] - sample[4]).total_seconds() > CPU_WINDOW_SECONDS:
            break
        earliest = sample
    interval = (latest[4] - earliest[4]).total_seconds()
    if interval <= 0:
        return {}
    return {"cpu_seconds": latest[3] - earliest[3], "interval_seconds": interval,
            "logged_at": latest[4].isoformat(timespec="seconds")}


def recent_milestones(events: list[dict]) -> list[dict]:
    """Keep at most four dated milestones, favoring work phases and failures.

    The journal parser owns the closed stage/step vocabulary. Copy only the
    fields needed for display, never arbitrary payloads or process identifiers.
    A cleanup completion must not crowd a preceding failure out of this list.
    """
    candidates, seen = [], set()
    for position, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        stage, kind, step = event.get("stage"), event.get("event"), event.get("step")
        logged = _timestamp(event.get("logged_at"))
        if (not isinstance(stage, str) or not stage or not isinstance(kind, str)
                or kind not in MILESTONE_EVENTS
                or logged is None or (step is not None and not isinstance(step, str))
                or (stage in SUPERVISORS and kind != "failed")):
            continue
        identity = stage, kind, step, logged
        if identity in seen:
            continue
        seen.add(identity)
        milestone = {"stage": stage, "event": kind, "logged_at": logged.isoformat(timespec="seconds")}
        if step is not None:
            milestone["step"] = step
        for field in MILESTONE_COUNTERS:
            value = event.get(field)
            if (not isinstance(value, bool) and isinstance(value, (int, float))
                    and 0 <= value <= 10 ** 24 and math.isfinite(value)):
                milestone[field] = value
        if stage == "recovery-metadata-audit" and kind == "completed":
            milestone["note"] = AUDIT_SCAN_NOTE
        priority = 2 if kind == "failed" else 1 if stage in HIGH_LEVEL_STAGES else 0
        candidates.append((priority, logged, position, milestone))
    chosen = sorted(candidates, key=lambda item: item[:3])[-4:]
    return [item[3] for item in sorted(chosen, key=lambda item: (item[1], item[2]))]
