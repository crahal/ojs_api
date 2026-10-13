from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import progress_observations as observations


NOW = datetime(2026, 10, 13, 12, tzinfo=timezone.utc)
OBSERVED_AT = NOW.isoformat(timespec="seconds")


def event(age=0, cpu=100, *, stage="temporal finalization phase", step="exact_keys",
          pid=123, kind="heartbeat", **fields):
    return {"stage": stage, "step": step, "process_pid": pid,
            "process_cpu_seconds": cpu, "event": kind,
            "logged_at": (NOW - timedelta(seconds=age)).isoformat(timespec="seconds"), **fields}


class ActivityTests(unittest.TestCase):
    def activity(self, events, selected=None, observed_at=OBSERVED_AT):
        return observations.observed_activity(events, events[-1] if selected is None else selected,
                                               observed_at=observed_at)

    def test_uses_recent_window_not_process_lifetime_or_older_work(self):
        events = [event(120, 900), event(90, 910), event(60, 920), event(30, 927), event(0, 940)]
        self.assertEqual(self.activity(events), {"cpu_seconds": 20, "interval_seconds": 60,
                                                "logged_at": OBSERVED_AT})

    def test_zero_cpu_delta_is_an_observation_without_a_progress_claim(self):
        result = self.activity([event(30, 10), event(0, 10)])
        self.assertEqual(result["cpu_seconds"], 0)
        self.assertEqual(result["interval_seconds"], 30)
        self.assertEqual(set(result), {"cpu_seconds", "interval_seconds", "logged_at"})

    def test_missing_or_invalid_selected_measurements_cannot_show_activity(self):
        for changes in ({"process_pid": None}, {"process_pid": 0}, {"process_pid": True},
                        {"process_pid": 123.0}, {"process_cpu_seconds": None},
                        {"process_cpu_seconds": True}, {"process_cpu_seconds": -1},
                        {"process_cpu_seconds": float("nan")}, {"process_cpu_seconds": float("inf")},
                        {"logged_at": "not a timestamp"}, {"logged_at": "2026-10-13T12:00:00"}):
            with self.subTest(changes=changes):
                self.assertEqual(self.activity([event(30, 10), {**event(0, 20), **changes}]), {})
        self.assertEqual(observations.observed_activity([], None, observed_at=OBSERVED_AT), {})

    def test_two_distinct_recent_observations_are_required(self):
        for events in ([event()], [event(90, 10), event(0, 20)],
                       [event(0, 10), event(0, 20)], [event(), event()]):
            with self.subTest(events=events):
                self.assertEqual(self.activity(events), {})

    def test_identical_duplicate_observation_does_not_inflate_cpu_or_time(self):
        events = [event(30, 10), event(30, 10), event(0, 20), event(0, 20)]
        result = self.activity(events)
        self.assertEqual((result["cpu_seconds"], result["interval_seconds"]), (10, 30))

    def test_stale_future_or_invalid_observation_time_cannot_show_current_activity(self):
        for events in ([event(90, 10), event(61, 20)], [event(30, 10), event(-1, 20)]):
            self.assertEqual(self.activity(events), {})
        for observed in ("bad", "2026-10-13T12:00:00", "2026-10-13T11:59:59+00:00"):
            with self.subTest(observed=observed):
                self.assertEqual(self.activity([event(30, 10), event(0, 20)], observed_at=observed), {})

    def test_stage_step_and_process_changes_break_the_contiguous_epoch(self):
        for middle in (event(20, 15, stage="sql-import"), event(20, 15, step="deduplication"),
                       event(20, 15, pid=999)):
            with self.subTest(middle=middle):
                self.assertEqual(self.activity([event(40, 10), middle, event(0, 20)]), {})
                result = self.activity([event(60, 10), middle, event(10, 19), event(0, 20)])
                self.assertEqual((result["cpu_seconds"], result["interval_seconds"]), (1, 10))

    def test_counter_reset_starts_a_new_epoch_instead_of_negative_or_lifetime_delta(self):
        self.assertEqual(self.activity([event(30, 90), event(0, 10)]), {})
        result = self.activity([event(60, 90), event(30, 10), event(0, 15)])
        self.assertEqual((result["cpu_seconds"], result["interval_seconds"]), (5, 30))

    def test_invalid_missing_or_reversed_intermediate_sample_breaks_comparability(self):
        for middle in ({**event(20, 15), "logged_at": "invalid"},
                       {**event(20, 15), "process_cpu_seconds": None}, event(-5, 15)):
            with self.subTest(middle=middle):
                self.assertEqual(self.activity([event(40, 10), middle, event(0, 20)]), {})

    def test_normal_supervisor_chatter_is_separate_but_supervisor_failure_breaks_epoch(self):
        supervisor = event(15, 9999, stage="update-recover-exact-keys", step="supervise-child", pid=777)
        result = self.activity([event(30, 10), supervisor, event(0, 20)])
        self.assertEqual((result["cpu_seconds"], result["interval_seconds"]), (10, 30))
        self.assertEqual(self.activity([event(30, 10), {**supervisor, "event": "failed"}, event(0, 20)]), {})
        self.assertEqual(self.activity([supervisor, {**supervisor, "logged_at": OBSERVED_AT}]), {})

    def test_selected_sample_must_exist_and_later_stages_are_not_counted(self):
        selected = event(10, 20)
        result = self.activity([event(40, 10), selected, event(0, 500, stage="sql-import")], selected)
        self.assertEqual((result["cpu_seconds"], result["interval_seconds"]), (10, 30))
        self.assertEqual(self.activity([event(40, 10)], selected), {})


class MilestoneTests(unittest.TestCase):
    def test_recent_work_milestones_are_chronological_and_bounded_to_four(self):
        events = [event(age, kind="sql-stage", step=step)
                  for age, step in ((150, "schema"), (120, "source_index"),
                                    (90, "exact_keys"), (60, "deduplication"), (30, "article_merges"))]
        result = observations.recent_milestones(events)
        self.assertEqual([item["step"] for item in result],
                         ["source_index", "exact_keys", "deduplication", "article_merges"])
        self.assertEqual(len(result), 4)

    def test_heartbeats_and_normal_supervisors_are_omitted_but_supervisor_failure_remains(self):
        events = [event(40, stage="update-build", kind="started"), event(30),
                  event(20, stage="update-build", kind="completed"),
                  event(10, stage="update-recover-exact-keys", kind="failed")]
        result = observations.recent_milestones(events)
        self.assertEqual([(item["stage"], item["event"]) for item in result],
                         [("update-recover-exact-keys", "failed")])

    def test_cleanup_completions_do_not_hide_work_failure_or_audit_scan(self):
        events = [event(120, stage="recovery-metadata-audit", kind="completed"),
                  event(100, stage="update-recover-exact-keys", kind="failed")]
        events += [event(age, stage="mysql-shutdown", kind="completed") for age in (80, 60, 40, 20, 0)]
        result = observations.recent_milestones(events)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]["stage"], "recovery-metadata-audit")
        self.assertEqual(result[1]["event"], "failed")
        self.assertEqual(result[0]["note"], observations.AUDIT_SCAN_NOTE)
        self.assertNotIn("passed", result[0]["note"])

    def test_duplicate_or_invalid_milestones_do_not_fill_history(self):
        milestone = event(30, kind="sql-stage")
        result = observations.recent_milestones([
            milestone, milestone.copy(), {**event(0, kind="completed"), "logged_at": "invalid"},
            event(10, kind="range-verified"), event(5, kind="not-a-milestone"),
        ])
        self.assertEqual(len(result), 1)

    def test_milestones_copy_only_display_fields_not_payloads_or_process_identifiers(self):
        result = observations.recent_milestones([event(kind="started", password="never-copy", sql="never-copy")])
        self.assertEqual(set(result[0]), {"stage", "event", "logged_at", "step"})
        self.assertNotIn("never-copy", str(result))
        self.assertNotIn("process_pid", result[0])

    def test_audit_and_rollback_milestones_retain_only_valid_allowlisted_counts(self):
        events = [event(60, stage="recovery-rollback", kind="completed", remaining_rows=0),
                  event(30, stage="recovery-metadata-audit", kind="completed",
                        checked_rows=31_350_000, completed_ranges=32, total_ranges=32)]
        result = observations.recent_milestones(events)
        self.assertEqual(result[0]["remaining_rows"], 0)
        self.assertEqual(result[1]["checked_rows"], 31_350_000)
        self.assertEqual(result[1]["completed_ranges"], 32)
        self.assertEqual(result[1]["total_ranges"], 32)
        self.assertEqual(result[1]["note"], observations.AUDIT_SCAN_NOTE)

    def test_phase_starts_do_not_displace_rollback_and_audit_completion(self):
        events = [event(120, stage="recovery-rollback", kind="started"),
                  event(100, stage="recovery-rollback", kind="completed"),
                  event(90, stage="recovery-metadata-audit", kind="started"),
                  event(60, stage="recovery-metadata-audit", kind="completed"),
                  event(50, kind="started", step=None), event(40, kind="sql-stage")]
        result = observations.recent_milestones(events)
        self.assertIn(("recovery-rollback", "completed"), [(item["stage"], item["event"]) for item in result])
        self.assertIn(("recovery-metadata-audit", "completed"), [(item["stage"], item["event"]) for item in result])

    def test_invalid_milestone_counts_are_not_retained(self):
        for value in (None, "never-copy", True, -1, float("nan"), float("inf"), 10 ** 30):
            with self.subTest(value=value):
                fields = dict.fromkeys(observations.MILESTONE_COUNTERS, value)
                result = observations.recent_milestones([event(kind="completed", **fields)])
                self.assertTrue(set(observations.MILESTONE_COUNTERS).isdisjoint(result[0]))
                self.assertNotIn("never-copy", str(result))


if __name__ == "__main__":
    unittest.main()
