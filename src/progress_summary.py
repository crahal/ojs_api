"""Human descriptions of sanitized recovery observations, without I/O.

The caller supplies journal events from the current service invocation. These
helpers describe recorded facts; they never infer completion from a later stage.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
import re


STALE_SECONDS = 300
_INVOCATION = re.compile(r"[0-9a-f]{32}\Z")
_AUDIT_NOTE = "Metadata audit scan completed; final safety checks and overall recovery are not confirmed."
_FALLBACK = ("Unrecognized recovery stage", "A description of this recorded stage is unavailable.")
_STAGES = {
    "recovery-rollback": (
        "Undoing an interrupted database transaction",
        "MySQL is undoing earlier work before recovery can continue."),
    "recovery-metadata-audit": (
        "Checking recovered metadata",
        "Stored metadata is being checked against the source records before it can be reused."),
    "sql-import": ("Importing source records", "The source SQL is being loaded into the working database."),
    "metadata-shard": ("Extracting article metadata", "An assigned range of source records is being parsed into article metadata."),
    "source-index phase": ("Indexing source records", "The pipeline is preparing source identities and metadata extraction."),
    "temporal finalization phase": ("Building the article catalogue", "The pipeline is matching records and constructing the catalogue and its change history."),
    "snapshot-checksum": ("Verifying the source snapshot", "The source snapshot is being read and checked against its expected content."),
    "file-checksum": ("Verifying a file checksum", "A file is being read to verify its content."),
    "clean-export": ("Exporting the finished catalogue", "Catalogue tables are being written to a compressed export."),
    "mysql-initialize": ("Preparing the working database", "The working MySQL data directory is being initialized."),
    "mysql-start": ("Starting the working database", "The pipeline is waiting for its working MySQL instance to start."),
    "mysql-shutdown": ("Requesting database shutdown", "The working database is being asked to shut down cleanly."),
    "mysql-shutdown-wait": ("Waiting for database shutdown", "The pipeline is waiting for MySQL to finish shutdown, which can include rollback."),
    "validate-raw-tables": ("Checking imported tables", "The imported source tables are being checked."),
    "validate-counts-and-identities": ("Checking catalogue counts and identities", "The built catalogue is being checked for expected counts and record identities."),
    "update-source-check": ("Checking for a source update", "The coordinator is checking the available source snapshot."),
    "update-release-checksums": ("Verifying release files", "The export and release provenance are being checked."),
    "update-api-health": ("Checking the API", "The coordinator is checking whether the API responds as expected."),
    "update-rollback": ("Restoring the previous release", "The coordinator is restoring the previous release after a release-switch problem."),
    "update-promotion": ("Switching to the new release", "The coordinator is switching the serving catalogue to the candidate release."),
    "update-cold-verification": ("Checking the restarted release", "The candidate release is being checked after restart."),
    "update-cleanup": ("Cleaning up retained build files", "The coordinator is removing build artifacts authorized by its cleanup plan."),
    "update-graceful-shutdown": ("Waiting for a safe stop", "The coordinator is waiting for child processes and any rollback to finish."),
    "update-recover-exact-keys": ("Supervising catalogue recovery", "The coordinator is supervising the recovery process; its detailed stage is not shown here."),
    "update-download": ("Supervising a source download", "The coordinator is supervising the source download."),
    "update-build": ("Supervising a catalogue build", "The coordinator is supervising the catalogue build."),
    "beacon-source-check": ("Checking the source archive", "The source archive metadata is being checked."),
    "beacon-archive-validation": ("Validating the downloaded archive", "The archive is being decompressed and checked."),
    "beacon-transfer": ("Downloading the source archive", "The source archive is being transferred to this host."),
    "beacon-connect": ("Connecting to the source archive", "A connection to the source archive is being established."),
    "beacon-legacy-checksum": ("Verifying an existing source archive", "An existing archive is being checked before reuse."),
}
_SQL_STEPS = {
    "schema": ("Preparing catalogue tables", "The SQL tables needed for the build are being prepared."),
    "context_normalization": ("Normalizing journal identities", "Journal and endpoint identities are being normalized for matching."),
    "source_index": ("Indexing source records", "Source record identities are being indexed."),
    "source_identity_validation": ("Checking source identities", "The source identities are being checked for consistency."),
    "metadata_extraction": ("Extracting article metadata", "Article metadata is being extracted from source records."),
    "exact_keys": (
        "Building article matching keys",
        "This step builds DOI, OAI, URL and fingerprint keys for matching records. Its long INSERT does not report intermediate row counts."),
    "effective_keys": ("Combining current and retained matching keys", "Current source keys and retained article aliases are being combined for matching."),
    "noisy_keys": ("Filtering unreliable matching keys", "Overly common keys are being filtered to avoid incorrect article matches."),
    "touched_keys": ("Finding affected matching keys", "The matching keys affected by this snapshot are being identified."),
    "existing_candidates": ("Finding existing article matches", "Existing catalogue articles are being considered as matches for source records."),
    "initial_assignments": ("Assigning initial article identities", "Source records are receiving initial article identity assignments."),
    "deduplication": ("Reconciling duplicate article identities", "Repeated matching passes are bringing linked source records under consistent article identities."),
    "article_merges": ("Merging matched article identities", "Article identities found to refer to the same article are being merged."),
    "source_reconciliation": ("Reconciling source records", "Source records are being reconciled with their catalogue article identities."),
    "source_indexes": ("Updating source indexes", "Indexes on the reconciled source records are being built."),
    "key_reconciliation": ("Updating retained matching keys", "The catalogue's retained article matching keys are being reconciled."),
    "raw_storage_release": ("Releasing imported raw storage", "Raw working storage is being released after its required data has been retained."),
    "article_rollup": ("Combining article metadata", "Matched source metadata is being combined at article level."),
    "canonical_payload": ("Building canonical article records", "Canonical metadata payloads are being constructed for catalogue articles."),
    "article_state": ("Recording article state", "Current article state is being recorded."),
    "article_events": ("Recording article changes", "Article change events are being recorded."),
    "article_updates": ("Applying article updates", "The catalogue's article records are being updated."),
    "snapshot_counts": ("Counting the built snapshot", "Summary counts for this snapshot are being calculated."),
    "stage_cleanup": ("Removing SQL working tables", "SQL working tables are being removed."),
    "sql_complete": ("Reaching the end of catalogue SQL", "The SQL completion marker was reached; later validation and release work are separate."),
}


def stage_info(stage, step=None) -> tuple[str, str]:
    """Return only fixed, human-readable descriptions; never echo a label."""
    if not isinstance(stage, str) or stage not in _STAGES:
        return _FALLBACK
    if stage in {"temporal finalization phase", "source-index phase"} and isinstance(step, str):
        return _SQL_STEPS.get(step, _STAGES[stage])
    return _STAGES[stage]


def _timestamp(value) -> datetime | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def _number(value) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 10 ** 24:
        return None
    return value if math.isfinite(value) else None


def _count(value) -> int | None:
    value = _number(value)
    return int(value) if value is not None and value == int(value) else None


def _counter(event: dict, key: str) -> int | None:
    # Existing progress events have flat fields; also accept a sanitized
    # nested counters dictionary to make this helper independent of storage.
    counters = event.get("counters")
    return _count(counters.get(key) if isinstance(counters, dict) and key in counters else event.get(key))


def _duration(seconds: int | float) -> str:
    if seconds < 60:
        return f"{seconds:,.1f}".rstrip("0").rstrip(".") + " seconds"
    minutes = int(seconds) // 60
    if minutes < 60:
        return f"{minutes:,} minute" + ("s" if minutes != 1 else "")
    hours, minutes = divmod(minutes, 60)
    return f"{hours:,} hours" + (f" {minutes} minutes" if minutes else "")


def _freshness(event: dict, observed: datetime | None) -> tuple[bool, str]:
    logged = _timestamp(event.get("logged_at"))
    if observed is None or logged is None:
        return False, "observation time unavailable"
    age = (observed - logged).total_seconds()
    if age < 0:
        return False, "observation time is ahead of the report clock"
    return age <= STALE_SECONDS, _duration(age) + " ago" + ("; stale" if age > STALE_SECONDS else "")


def _same_invocation(service: dict, previous: dict) -> bool:
    invocation = service.get("InvocationID")
    return (isinstance(invocation, str) and bool(_INVOCATION.fullmatch(invocation))
            and invocation != "0" * 32 and invocation == previous.get("invocation"))


def _comparison(service: dict, event: dict, previous: dict | None,
                observed: datetime | None) -> str | None:
    if not isinstance(previous, dict):
        return None
    invocation = service.get("InvocationID")
    old_invocation = previous.get("invocation")
    if not _same_invocation(service, previous):
        if (isinstance(invocation, str) and _INVOCATION.fullmatch(invocation)
                and isinstance(old_invocation, str) and _INVOCATION.fullmatch(old_invocation)
                and invocation != old_invocation):
            return "The service run changed since the previous report; earlier counters are not comparable."
        return "A comparison with the previous report is unavailable because the service run could not be matched."
    logged, old_logged = _timestamp(event.get("logged_at")), _timestamp(previous.get("logged_at"))
    if logged is None or old_logged is None or observed is None or logged > observed:
        return "A comparison with the previous report is unavailable because observation times are uncertain."
    if logged <= old_logged:
        return "There is no newer stage observation than in the previous report."
    if (event.get("stage"), event.get("step")) != (previous.get("stage"), previous.get("step")):
        before = stage_info(previous.get("stage"), previous.get("step"))[0]
        after = stage_info(event.get("stage"), event.get("step"))[0]
        return f"Stage changed within the same run: {before} → {after}."
    keys = {"recovery-rollback": ("remaining_rows", "remaining-row"),
            "recovery-metadata-audit": ("checked_rows", "checked-row"),
            "metadata-shard": ("extracted_rows", "extracted-row")}
    key, label = keys.get(event.get("stage"), (None, None))
    if key is None:
        if event.get("stage") == "temporal finalization phase" and event.get("step") == "exact_keys":
            return "Still at the same stage as the previous report; this step has no intermediate row counter."
        return None
    current, old = _counter(event, key), _counter(previous, key)
    if current is None or old is None:
        return None
    if current == old:
        return f"Since the previous report, the {label} counter is unchanged."
    direction = "fell" if current < old else "rose"
    return f"Since the previous report, the {label} counter {direction} by {abs(current - old):,} (same run and stage)."


def _counters(event: dict) -> str | None:
    stage = event.get("stage")
    if stage == "recovery-rollback":
        remaining, pending = _counter(event, "remaining_rows"), _counter(event, "pending_transactions")
        if remaining is not None:
            return f"Recorded rollback counter: {remaining:,} rows remaining to undo" + (
                f"; {pending:,} transactions pending." if pending is not None else ".")
    if stage == "recovery-metadata-audit":
        done, total, rows = (_counter(event, key) for key in ("completed_ranges", "total_ranges", "checked_rows"))
        if done is not None and total is not None and total > 0 and done <= total:
            tenths = done * 1000 // total
            percentage = str(tenths // 10) if tenths % 10 == 0 else f"{tenths / 10:.1f}"
            text = f"Metadata range scan: {done:,} of {total:,} ranges verified ({percentage}% of this scan)"
            if rows is not None:
                text += f", covering {rows:,} rows"
            return text + (". Other validation checks and recovery work are separate." if done == total else ".")
        if rows is not None:
            return f"Metadata range scan: {rows:,} rows checked; the range total is unavailable or inconsistent."
    if stage == "metadata-shard":
        rows = _counter(event, "extracted_rows")
        if rows is not None:
            return f"Recorded metadata extraction counter: {rows:,} rows extracted."
    if stage == "temporal finalization phase" and event.get("step") == "deduplication":
        pass_number, changed = _counter(event, "pass_number"), _counter(event, "changed_rows")
        if pass_number is not None and changed is not None:
            return f"Latest matching pass: {pass_number:,}; {changed:,} identity assignments changed in that pass."
    return None


def _activity(snapshot: dict, observed: datetime | None, running: bool) -> str:
    sample = snapshot.get("activity")
    if not isinstance(sample, dict):
        return "No recent process CPU activity sample is available."
    cpu, interval = _number(sample.get("cpu_seconds")), _number(sample.get("interval_seconds"))
    recent, when = _freshness(sample, observed)
    if cpu is None or interval is None or interval <= 0:
        return "No usable process CPU activity sample is available."
    if not recent:
        return f"Process CPU activity is unavailable for the present moment ({when})."
    prefix = "Recent monitored process activity" if running else "Last sampled monitored process activity"
    if cpu == 0:
        return f"{prefix}: no measurable CPU increase across {_duration(interval)}; this alone does not establish a stalled process."
    return (f"{prefix}: {cpu:,.1f} CPU seconds across {_duration(interval)}. "
            "This measures process activity, not rows processed or query completion.")


def _milestones(snapshot: dict, observed: datetime | None) -> list[str]:
    raw = snapshot.get("milestones")
    if not isinstance(raw, list) or observed is None:
        return []
    invocation = (snapshot.get("service") or {}).get("InvocationID")
    selected = []
    seen = set()
    for milestone in raw[-300:]:
        if not isinstance(milestone, dict):
            continue
        stage, step, event = milestone.get("stage"), milestone.get("step"), milestone.get("event")
        if (not isinstance(stage, str) or stage not in _STAGES or not isinstance(event, str)
                or event not in {"started", "completed", "failed", "sql-stage"}):
            continue
        if milestone.get("invocation", invocation) != invocation:
            continue
        logged = _timestamp(milestone.get("logged_at"))
        if logged is None or logged > observed:
            continue
        label = stage_info(stage, step)[0]
        action = {"started": "started", "completed": "completed", "failed": "reported failure", "sql-stage": "reached"}[event]
        if stage == "recovery-metadata-audit" and event == "completed":
            label = "Metadata audit scan"
        identity = (logged, label, action)
        if identity in seen:
            continue
        seen.add(identity)
        text = f"Recorded milestone: {label} {action} at {logged:%d %b %H:%M UTC}."
        if stage == "recovery-metadata-audit" and event == "completed":
            done, total, rows = (_counter(milestone, key) for key in ("completed_ranges", "total_ranges", "checked_rows"))
            details = []
            if done is not None and total is not None and total > 0 and done <= total:
                details.append(f"{done:,} of {total:,} ranges verified")
            if rows is not None:
                details.append(f"{rows:,} rows checked")
            if details:
                text += " " + "; ".join(details) + "."
            # Render only the known audit caution, never arbitrary note text.
            if milestone.get("note") in (None, _AUDIT_NOTE):
                text += " Final safety checks and overall recovery remain unconfirmed."
        selected.append((logged, text))
    return [text for _, text in sorted(selected, key=lambda item: item[0])[-4:]]


def overview(snapshot: dict, previous: dict | None = None) -> list[str]:
    """Describe current state, measured changes and up to four recorded milestones."""
    service = snapshot.get("service") or {}
    event = snapshot.get("event") or {}
    observed = _timestamp(snapshot.get("observed_at"))
    active, substate, result = service.get("ActiveState"), service.get("SubState"), service.get("Result")
    running = active in {"active", "activating", "reloading", "refreshing"}
    failed = active == "failed" or (active == "inactive" and result not in (None, "success"))
    if failed:
        lines = ["Stopped with a reported service failure."]
    elif active == "inactive" or (active is None and substate == "dead"):
        if result == "success" and service.get("ExecMainCode") == 1 and service.get("ExecMainStatus") == 0:
            lines = ["Stopped: the service exited successfully; overall recovery completion remains unverified."]
        else:
            lines = ["Stopped: the service is inactive."]
    elif active == "deactivating":
        lines = ["Stopping: shutdown or rollback may still be active."]
    elif running:
        lines = ["The recovery service is running."]
    else:
        lines = ["The current service state is unavailable."]
    if event:
        label, meaning = stage_info(event.get("stage"), event.get("step"))
        recent, when = _freshness(event, observed)
        event_type = event.get("event")
        if event_type == "failed":
            lines.append(f"Last recorded stage failure: {label} ({when}).")
        elif event_type == "completed":
            lines.append(f"Last recorded stage: {label} completed ({when}); the next stage is not established by this event.")
        elif running and recent:
            lines.append(f"Current observed stage: {label} ({when}).")
        else:
            lines.append(f"Last observed stage: {label} ({when}); this does not establish the current stage.")
        # Descriptions explain the operation, not a claim it is still running.
        lines.append("What this stage does: " + meaning)
        counters = _counters(event)
        if counters:
            lines.append(counters)
        comparison = _comparison(service, event, previous, observed)
        if comparison:
            lines.append(comparison)
        idle = _number(event.get("idle_seconds"))
        if idle is not None and event_type not in {"completed", "failed"}:
            lines.append(f"Last instrumented update: {_duration(idle)} before that observation; CPU activity is measured separately.")
    else:
        lines.append("No stage observation is available for this service run.")
        if isinstance(previous, dict) and not _same_invocation(service, previous):
            lines.append("The previous report cannot establish the stage of this service run.")
    lines.append(_activity(snapshot, observed, running))
    lines.extend(_milestones(snapshot, observed))
    return lines
