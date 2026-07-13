import json
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.auto_tracking import (  # noqa: E402
    HistoryCounts,
    ResolutionKeyMonitor,
    RoundTracker,
    extract_resolution_keys,
)


class FakeRequest:
    def __init__(self, url, post_data):
        self.url = url
        self.post_data = post_data


class ResolutionKeyParserTests(unittest.TestCase):
    def test_extracts_numeric_entry_keys_from_resolutions_query(self):
        query = (
            "query { resolutions { g21090: entry(key: 21090) { value } "
            "g21091: entry(key: 21091) { value } } }"
        )

        self.assertEqual(extract_resolution_keys(query), {21090, 21091})

    def test_ignores_unrelated_graphql_entry_keys(self):
        query = (
            "query { accounts { entry(key: 999) } "
            "resolutions { g12: entry(key: 12) { value } } }"
        )

        self.assertEqual(extract_resolution_keys({"query": query}), {12})

    def test_accepts_json_batch_payload(self):
        payload = json.dumps([
            {"query": "query { resolutions { g7: entry(key: 7) } }"},
            {"query": "query { resolutions { g8: entry(key: 8) } }"},
        ])

        self.assertEqual(extract_resolution_keys(payload), {7, 8})

    def test_malformed_or_missing_payload_returns_empty_set(self):
        self.assertEqual(extract_resolution_keys("{not json"), set())
        self.assertEqual(extract_resolution_keys({"variables": {}}), set())
        self.assertEqual(extract_resolution_keys(None), set())

    def test_monitor_accepts_only_linera_worker_application_requests(self):
        monitor = ResolutionKeyMonitor()
        query = json.dumps({"query": "query { resolutions { g7: entry(key: 7) } }"})

        monitor.on_request(FakeRequest("https://example.com/applications/x", query))
        monitor.on_request(FakeRequest("https://worker.infra.linera.net/chains/x", query))
        monitor.on_request(
            FakeRequest(
                "https://worker.infra.linera.net/chains/x/applications/y", query
            )
        )

        self.assertEqual(monitor.snapshot(), {7})


class RoundTrackerTests(unittest.TestCase):
    def make_tracker(self, counted=None):
        return RoundTracker(
            {100, 101},
            HistoryCounts(higher=3, lower=3),
            already_counted=set(counted or []),
        )

    def test_baseline_keys_and_active_pair_never_count(self):
        tracker = self.make_tracker()

        added = tracker.observe(
            {100, 101},
            HistoryCounts(1, 1, active_higher=1, active_lower=1),
        )

        self.assertEqual(added, [])

    def test_new_key_without_active_pair_does_not_count(self):
        tracker = self.make_tracker()

        added = tracker.observe({100, 101, 102}, HistoryCounts(1, 1))

        self.assertEqual(added, [])

    def test_active_pair_followed_by_new_resolution_counts_once(self):
        tracker = self.make_tracker()

        tracker.observe(
            {100, 101},
            HistoryCounts(1, 1, active_higher=1, active_lower=1),
        )
        first = tracker.observe(
            {100, 101, 102},
            HistoryCounts(1, 1, active_higher=1, active_lower=1),
        )
        second = tracker.observe(
            {100, 101, 102},
            HistoryCounts(1, 1, active_higher=1, active_lower=1),
        )

        self.assertEqual(first, [102])
        self.assertEqual(second, [])

    def test_duplicate_requests_do_not_double_count(self):
        tracker = self.make_tracker()

        active = HistoryCounts(1, 1, active_higher=1, active_lower=1)
        tracker.observe({100, 101}, active)
        tracker.observe({100, 101, 102}, active)
        added = tracker.observe({100, 101, 102}, active)

        self.assertEqual(added, [])

    def test_two_new_keys_require_active_evidence_in_each_interval(self):
        tracker = self.make_tracker()
        active = HistoryCounts(1, 1, active_higher=1, active_lower=1)

        tracker.observe({100, 101}, active)
        first = tracker.observe({100, 101, 102}, active)
        tracker.observe({100, 101, 102}, active)
        second = tracker.observe({100, 101, 102, 103}, active)

        self.assertEqual(first, [102])
        self.assertEqual(second, [103])

    def test_multiple_unseen_keys_count_only_latest_one(self):
        tracker = self.make_tracker()
        active = HistoryCounts(1, 1, active_higher=1, active_lower=1)

        tracker.observe({100, 101}, active)
        added = tracker.observe({100, 101, 105, 103, 104}, active)

        self.assertEqual(added, [105])

    def test_already_counted_key_is_never_reused(self):
        tracker = self.make_tracker(counted={102})
        active = HistoryCounts(1, 1, active_higher=1, active_lower=1)

        tracker.observe({100, 101, 102}, active)
        added = tracker.observe({100, 101, 102, 103}, active)

        self.assertEqual(added, [103])


if __name__ == "__main__":
    unittest.main()
