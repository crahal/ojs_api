from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from progress_summary import overview, stage_info


NOW = datetime(2026, 10, 13, 19, 45, tzinfo=timezone.utc)
RUN = "a" * 32
PRIVATE = "private-password-SQL-and-path"


def event(stage="temporal finalization phase", *, step="exact_keys", kind="heartbeat", age=10, **values):
    return {"stage": stage, "step": step, "event": kind,
            "logged_at": (NOW - timedelta(seconds=age)).isoformat(), **values}


def snapshot(current=None, **values):
    return {"observed_at": NOW.isoformat(),
            "service": {"ActiveState": "activating", "SubState": "start", "Result": "success", "InvocationID": RUN},
            "event": event() if current is None else current, **values}


def previous(current, **values):
    return {"invocation": RUN, "stage": current["stage"], "step": current.get("step"),
            "logged_at": (NOW - timedelta(hours=12)).isoformat(), "counters": {}, **values}


class StageDescriptionTests(unittest.TestCase):
    def test_matching_keys_has_concrete_meaning_and_no_intermediate_counter(self):
        label, meaning = stage_info("temporal finalization phase", "exact_keys")
        self.assertEqual(label, "Building article matching keys")
        for word in ("DOI", "OAI", "URL", "fingerprint", "INSERT", "intermediate row counts"):
            self.assertIn(word, meaning)

    def test_known_stages_have_useful_descriptions(self):
        for stage, step, expected in (("recovery-rollback", None, "undoing"),
                                      ("recovery-metadata-audit", None, "metadata"),
                                      ("sql-import", None, "source"),
                                      ("temporal finalization phase", "deduplication", "duplicate")):
            with self.subTest(stage=stage):
                self.assertIn(expected, stage_info(stage, step)[0].lower())

    def test_unknown_labels_are_not_echoed_or_used_as_step_descriptions(self):
        self.assertNotIn(PRIVATE, str(stage_info(PRIVATE, PRIVATE)))
        self.assertNotIn(PRIVATE, str(stage_info("temporal finalization phase", PRIVATE)))
        self.assertNotIn("matching keys", stage_info("sql-import", "exact_keys")[0])
        self.assertEqual(stage_info([], {}), stage_info(PRIVATE))


class OverviewTests(unittest.TestCase):
    def test_same_exact_key_stage_is_explicit_without_claiming_row_progress(self):
        current = event()
        text = "\n".join(overview(snapshot(current), previous(current)))
        self.assertIn("Still at the same stage as the previous report", text)
        self.assertIn("no intermediate row counter", text)

    def test_current_exact_key_stage_is_named_and_explained(self):
        text = "\n".join(overview(snapshot()))
        self.assertIn("Current observed stage: Building article matching keys", text)
        self.assertIn("does not report intermediate row counts", text)
        self.assertNotIn("%", text)
        self.assertNotIn("ETA", text)

    def test_stale_and_future_events_never_claim_current_stage(self):
        for age, expected in ((301, "stale"), (-1, "ahead of the report clock")):
            with self.subTest(age=age):
                text = "\n".join(overview(snapshot(event(age=age))))
                self.assertIn(expected, text)
                self.assertIn("Last observed stage", text)
                self.assertNotIn("Current observed stage", text)

    def test_missing_event_and_invalid_time_are_honest(self):
        current = snapshot()
        current["event"] = None
        text = "\n".join(overview(current))
        self.assertIn("No stage observation", text)
        current["event"] = event(logged_at=PRIVATE)
        text = "\n".join(overview(current))
        self.assertIn("observation time unavailable", text)
        self.assertNotIn(PRIVATE, text)

    def test_terminal_service_dominates_old_heartbeat(self):
        for active, result, expected in (("inactive", "success", "service is inactive"),
                                         ("failed", "exit-code", "reported service failure"),
                                         ("inactive", "exit-code", "reported service failure")):
            with self.subTest(active=active, result=result):
                current = snapshot(event(age=1))
                current["service"].update(ActiveState=active, SubState="dead", Result=result)
                lines = overview(current)
                self.assertIn(expected, lines[0])
                self.assertIn("Last observed stage", "\n".join(lines))
                self.assertNotIn("Current observed stage", "\n".join(lines))
                self.assertNotIn("crash", "\n".join(lines).lower())

    def test_dead_without_active_state_is_inactive_not_crashed(self):
        current = snapshot()
        current["service"] = {"SubState": "dead"}
        self.assertEqual(overview(current)[0], "Stopped: the service is inactive.")

    def test_successful_exit_does_not_mean_recovery_is_complete(self):
        current = snapshot()
        current["service"].update(ActiveState="inactive", Result="success", ExecMainCode=1, ExecMainStatus=0)
        self.assertIn("service exited successfully; overall recovery completion remains unverified", overview(current)[0])

    def test_stage_failure_and_completed_stage_are_not_claimed_running(self):
        for kind, expected in (("failed", "Last recorded stage failure"),
                               ("completed", "the next stage is not established")):
            with self.subTest(kind=kind):
                text = "\n".join(overview(snapshot(event(kind=kind, age=600))))
                self.assertIn(expected, text)
                self.assertNotIn("Current observed stage", text)

    def test_audit_range_percentage_is_only_of_valid_range_scan(self):
        for done, total, percentage in ((0, 32, "0%"), (16, 32, "50%"), (32, 32, "100%"), (31, 32, "96.8%")):
            with self.subTest(done=done):
                current = event("recovery-metadata-audit", step=None,
                                completed_ranges=done, total_ranges=total, checked_rows=31345623)
                text = "\n".join(overview(snapshot(current)))
                self.assertIn(percentage + " of this scan", text)
                self.assertIn("31,345,623 rows", text)
                if done == total:
                    self.assertIn("Other validation checks and recovery work are separate", text)

    def test_invalid_audit_totals_do_not_produce_percentages(self):
        for done, total in ((0, 0), (1, 0), (-1, 32), (33, 32), (1, None),
                            (True, 32), (1, float("nan")), (1.5, 32), (1, -2)):
            with self.subTest(done=done, total=total):
                current = event("recovery-metadata-audit", step=None,
                                completed_ranges=done, total_ranges=total, checked_rows=100)
                text = "\n".join(overview(snapshot(current)))
                self.assertNotIn("%", text)

    def test_non_audit_counters_cannot_create_an_overall_percentage(self):
        current = event("sql-import", step=None, completed_ranges=16, total_ranges=32, processed_bytes=50, total_bytes=100)
        self.assertNotIn("%", "\n".join(overview(snapshot(current))))

    def test_stage_change_and_restart_are_distinguished_without_counter_deltas(self):
        current = event()
        old = previous(current, stage="recovery-metadata-audit", step=None, counters={"checked_rows": 31345623})
        text = "\n".join(overview(snapshot(current), old))
        self.assertIn("Stage changed within the same run", text)
        self.assertIn("Checking recovered metadata → Building article matching keys", text)
        old["invocation"] = "b" * 32
        text = "\n".join(overview(snapshot(current), old))
        self.assertIn("service run changed", text)
        self.assertNotIn("Stage changed within the same run", text)

    def test_rollback_delta_requires_same_run_stage_and_newer_observation(self):
        current = event("recovery-rollback", step=None, remaining_rows=100)
        old = previous(current, counters={"remaining_rows": 150})
        text = "\n".join(overview(snapshot(current), old))
        self.assertIn("remaining-row counter fell by 50", text)
        for changes in ({"stage": "sql-import"}, {"step": "changed"},
                        {"invocation": "b" * 32}, {"logged_at": current["logged_at"]}):
            with self.subTest(changes=changes):
                self.assertNotIn("fell by 50", "\n".join(overview(snapshot(current), {**old, **changes})))

    def test_cpu_activity_and_instrumentation_age_are_separate(self):
        current = snapshot(event(idle_seconds=24120),
                           activity={"cpu_seconds": 3.1, "interval_seconds": 30, "logged_at": event()["logged_at"]})
        text = "\n".join(overview(current))
        self.assertIn("3.1 CPU seconds across 30 seconds", text)
        self.assertIn("6 hours 42 minutes", text)
        self.assertIn("CPU activity is measured separately", text)
        self.assertIn("not rows processed or query completion", text)
        self.assertNotIn("%", text)
        self.assertNotIn("frozen", text)

    def test_zero_invalid_stale_and_future_cpu_samples(self):
        current = snapshot(activity={"cpu_seconds": 0, "interval_seconds": 30, "logged_at": event()["logged_at"]})
        self.assertIn("no measurable CPU increase", "\n".join(overview(current)))
        for cpu, interval, age in ((1, 0, 1), (-1, 30, 1), (float("nan"), 30, 1), (1, 30, 301), (1, 30, -10)):
            current["activity"] = {"cpu_seconds": cpu, "interval_seconds": interval,
                                   "logged_at": event(age=age)["logged_at"]}
            text = "\n".join(overview(current))
            self.assertNotIn("Recent monitored process activity:", text)

    def test_recent_cpu_observation_on_stopped_service_is_past_tense(self):
        current = snapshot(activity={"cpu_seconds": 2, "interval_seconds": 30, "logged_at": event()["logged_at"]})
        current["service"]["ActiveState"] = "inactive"
        text = "\n".join(overview(current))
        self.assertIn("Last sampled monitored process activity", text)
        self.assertNotIn("Recent monitored process activity", text)

    def test_audit_milestone_retains_rows_after_moving_to_key_build(self):
        milestone = event("recovery-metadata-audit", step=None, kind="completed", age=24120,
                          completed_ranges=32, total_ranges=32, checked_rows=31345623,
                          note="Metadata audit scan completed; final safety checks and overall recovery are not confirmed.")
        text = "\n".join(overview(snapshot(milestones=[milestone])))
        self.assertIn("Metadata audit scan completed", text)
        self.assertIn("32 of 32 ranges verified; 31,345,623 rows checked", text)
        self.assertIn("Final safety checks and overall recovery remain unconfirmed", text)
        self.assertIn("Current observed stage: Building article matching keys", text)

    def test_milestones_are_bounded_dated_and_do_not_invent_success(self):
        milestones = [event("recovery-rollback", step=None, kind="started", age=1000)]
        milestones.extend(event(kind="sql-stage", step=step, age=900-index) for index, step in enumerate(
            ("exact_keys", "effective_keys", "noisy_keys", "deduplication", "article_merges")))
        text = "\n".join(overview(snapshot(milestones=milestones)))
        self.assertEqual(text.count("Recorded milestone:"), 4)
        self.assertNotIn("rollback completed", text.lower())
        self.assertNotIn("recovery completed", text.lower())

    def test_malformed_private_and_other_run_milestones_are_not_echoed(self):
        milestones = [event(kind="completed", invocation="b" * 32), event(kind=[]),
                      event(PRIVATE, kind="completed"), event(kind="completed", age=-1),
                      event("recovery-metadata-audit", step=None, kind="completed", note=PRIVATE)]
        text = "\n".join(overview(snapshot(milestones=milestones)))
        self.assertNotIn(PRIVATE, text)
        self.assertEqual(text.count("Recorded milestone:"), 1)


if __name__ == "__main__":
    unittest.main()
