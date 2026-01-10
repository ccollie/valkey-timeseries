from collections import defaultdict
import pytest
import statistics
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTSOutliers(ValkeyTimeSeriesTestCaseBase):
    """
    Integration tests for TS.OUTLIERS.

    These tests verify:
      - output shapes for FORMAT simple/full/cleaned
      - DIRECTION filtering
      - basic error handling (unknown option, duplicate METHOD, missing key)
      - SPC EWMA parsing with ALPHA
    """

    def _seed_series(self, key: str) -> None:
        self.client.execute_command("TS.CREATE", key)

        # Mostly flat around 10, with a big positive spike and a big negative dip.
        for t in range(1, 21):
            if t == 7:
                value = 100.0
            elif t == 15:
                value = -50.0
            else:
                value = 10.0
            self.client.execute_command("TS.ADD", key, t, value)

    def _assert_simple_tuple(self, row) -> None:
        # Expected: [timestamp, value, anomaly_direction, anomaly_score]
        self.assertIsInstance(row, list, msg=f"expected list row, got {type(row)}: {row}")
        self.assertEqual(len(row), 4, msg=f"expected 4 elements, got {len(row)}: {row}")

        ts, val, direction, score = row
        self.assertIsInstance(ts, int)
        self.assertIsInstance(val, (int, float))
        self.assertIn(direction, (1, -1), msg=f"unexpected direction: {direction}")
        self.assertIsInstance(score, (int, float))

    def test_outliers_simple_default_zscore_direction_both(self) -> None:
        self._seed_series("s1")

        # Default FORMAT is simple; default DIRECTION is both.
        res = self.client.execute_command(
            "TS.OUTLIERS",
            "s1",
            1,
            20,
            "METHOD",
            "zscore",
            "threshold",
            2.0,
        )

        self.assertIsInstance(res, list)
        self.assertGreater(len(res), 0, msg="expected at least one anomaly in simple output")
        self._assert_simple_tuple(res[0])

        ts = res[0][0]
        self.assertIn(ts, (7, 15), msg=f"expected anomaly timestamp 7 or 15, got {ts}")

    def test_outliers_direction_positive_filters_out_negative(self) -> None:
        self._seed_series("s2")

        res = self.client.execute_command(
            "TS.OUTLIERS",
            "s2",
            1,
            20,
            "DIRECTION",
            "positive",
            "METHOD",
            "zscore",
            "threshold",
            2.0,
        )

        self.assertIsInstance(res, list)
        for row in res:
            self._assert_simple_tuple(row)
            assert row[2] == 1

    def test_outliers_direction_negative_filters_out_positive(self) -> None:
        self._seed_series("s3")

        res = self.client.execute_command(
            "TS.OUTLIERS",
            "s3",
            1,
            20,
            "DIRECTION",
            "negative",
            "METHOD",
            "zscore",
            "threshold",
            2.0,
        )

        self.assertIsInstance(res, list)
        for row in res:
            self._assert_simple_tuple(row)
            assert row[2] == -1

    def test_outliers_format_full_has_expected_keys(self) -> None:
        self._seed_series("s4")

        res = self.client.execute_command(
            "TS.OUTLIERS",
            "s4",
            1,
            20,
            "FORMAT",
            "full",
            "METHOD",
            "zscore",
            "threshold",
            2.0,
        )

        # In this harness, maps are typically returned as dict.
        self.assertIsInstance(res, dict, msg=f"expected dict reply for FORMAT full, got {type(res)}")
        for k in ("method", "threshold", "samples", "scores", "anomalies"):
            self.assertIn(k, res, msg=f"missing key {k}; got keys={sorted(res.keys())}")

        self.assertIsInstance(res["samples"], list)
        self.assertIsInstance(res["scores"], list)
        self.assertIsInstance(res["anomalies"], list)

    def test_outliers_format_cleaned_returns_samples_and_anomalies(self) -> None:
        self._seed_series("s5")

        res = self.client.execute_command(
            "TS.OUTLIERS",
            "s5",
            1,
            20,
            "FORMAT",
            "cleaned",
            "DIRECTION",
            "both",
            "METHOD",
            "zscore",
            "threshold",
            2.0,
        )

        self.assertIsInstance(res, dict, msg=f"expected dict reply for FORMAT cleaned, got {type(res)}")
        self.assertIn("samples", res)
        self.assertIn("anomalies", res)

        self.assertIsInstance(res["samples"], list)
        self.assertIsInstance(res["anomalies"], list)
        self.assertGreater(len(res["anomalies"]), 0, msg="expected anomalies to be non-empty")
        self.assertLess(
            len(res["samples"]),
            20,
            msg="cleaned samples should be fewer than total when direction=both",
        )

    def test_outliers_unknown_option_errors(self) -> None:
        self._seed_series("s6")
        with self.assertRaises(Exception):
            self.client.execute_command("TS.OUTLIERS", "s6", 1, 20, "BOGUS", "x")

    def test_outliers_duplicate_method_errors(self) -> None:
        self._seed_series("s7")
        with self.assertRaises(Exception):
            self.client.execute_command(
                "TS.OUTLIERS",
                "s7",
                1,
                20,
                "METHOD",
                "zscore",
                "threshold",
                2.0,
                "METHOD",
                "zscore",
                "threshold",
                2.0,
            )

    def test_outliers_missing_key_errors(self) -> None:
        with self.assertRaises(Exception):
            self.client.execute_command(
                "TS.OUTLIERS",
                "nope",
                1,
                10,
                "METHOD",
                "zscore",
                "threshold",
                2.0,
            )

    def test_outliers_spc_ewma_with_alpha_parses_and_runs(self) -> None:
        self._seed_series("s8")

        # Sanity: ensure the command runs and returns simple output list.
        res = self.client.execute_command(
            "TS.OUTLIERS",
            "s8",
            1,
            20,
            "METHOD",
            "ewma",
            "ALPHA",
            0.2,
        )
        self.assertIsInstance(res, list)
