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

    def test_baseline_keys_and_rows_never_count(self):
        tracker = self.make_tracker()

        added = tracker.observe({100, 101}, HistoryCounts(3, 3))

        self.assertEqual(added, [])

    def test_new_key_with_only_higher_row_does_not_count(self):
        tracker = self.make_tracker()

        added = tracker.observe({100, 101, 102}, HistoryCounts(4, 3))

        self.assertEqual(added, [])

    def test_new_key_with_higher_and_lower_row_counts_once(self):
        tracker = self.make_tracker()

        first = tracker.observe({102}, HistoryCounts(4, 4))
        second = tracker.observe({102}, HistoryCounts(4, 4))

        self.assertEqual(first, [102])
        self.assertEqual(second, [])

    def test_duplicate_requests_do_not_double_count(self):
        tracker = self.make_tracker()

        tracker.observe({102}, HistoryCounts(4, 4))
        added = tracker.observe({102, 102}, HistoryCounts(5, 5))

        self.assertEqual(added, [])

    def test_two_new_keys_require_two_new_pairs(self):
        tracker = self.make_tracker()

        first = tracker.observe({102, 103}, HistoryCounts(4, 4))
        second = tracker.observe({102, 103}, HistoryCounts(5, 5))

        self.assertEqual(first, [102])
        self.assertEqual(second, [103])

    def test_out_of_order_keys_are_counted_in_numeric_order(self):
        tracker = self.make_tracker()

        added = tracker.observe({105, 103, 104}, HistoryCounts(6, 6))

        self.assertEqual(added, [103, 104, 105])

    def test_already_counted_keys_consume_existing_history_pairs(self):
        tracker = self.make_tracker(counted={102})

        added = tracker.observe({102, 103}, HistoryCounts(5, 5))

        self.assertEqual(added, [103])


if __name__ == "__main__":
    unittest.main()
