import time
from typing import List

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase, CompactionRule


class TestTSCreateRule(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TS.CREATERULE command"""

    def create_test_series(self, key: str, **kwargs) -> None:
        """Helper to create a test time series"""
        args = ["TS.CREATE", key]
        for k, v in kwargs.items():
            args.extend([k.upper(), str(v)])
        self.client.execute_command(*args)

    def add_samples(self, key: str, samples: List[tuple]) -> None:
        """Helper to add multiple samples to a series"""
        for timestamp, value in samples:
            self.client.execute_command("TS.ADD", key, timestamp, value)

    def validate_rule_info(self, source_key: str, expected) -> None:
        # Verify rule was created by checking TS.INFO
        info = self.ts_info(source_key)
        assert "rules" in info
        assert len(info["rules"]) == 1
        rule = info["rules"][0]
        assert expected, rule

    def validate_rules_info(self, source_key: str, expected_rules: List[tuple]) -> None:
        """Helper to validate the rules info for a source key"""
        info = self.ts_info(source_key)
        assert "rules" in info
        rules = info["rules"]
        assert len(rules) == len(expected_rules), f"Expected {len(expected_rules)} rules, got {len(rules)}"
        for i, actual in rules:
            expected = expected_rules[i]
            assert expected == actual, f"Rule {i} mismatch: expected {expected}, got {actual}"

    def test_create_rule_basic_success(self):
        """Test basic successful rule creation"""
        source_key = "test:source1"
        dest_key = "test:dest1"

        # Create source and destination series
        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Create compaction rule
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"  # 60 second buckets
        )
        assert result == b"OK"
        info = self.ts_info(source_key)
        rules = info["rules"]
        assert len(rules) == 1
        expected_rule = CompactionRule(dest_key, 60000,"avg", None)
        assert expected_rule == rules[0]

    def test_create_rule_with_align_timestamp(self):
        """Test rule creation with alignment timestamp"""
        source_key = "test:source2"
        dest_key = "test:dest2"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        align_ts = int(time.time() * 1000)
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "sum", "30000", str(align_ts)
        )
        assert result == b"OK"

        info = self.ts_info(source_key)
        rules = info["rules"]
        assert len(rules) == 1
        expected_rule = CompactionRule(dest_key, 30000, "sum", align_ts)
        assert expected_rule == rules[0]

    def test_create_rule_various_aggregators(self):
        """Test rule creation with different aggregation types"""
        aggregators = [
            "avg", "sum", "min", "max", "count", "first", "last",
            "std.p", "std.s", "var.p", "var.s", "range"
        ]

        for i, agg in enumerate(aggregators):
            source_key = f"test:source_agg_{i}"
            dest_key = f"test:dest_agg_{i}"

            self.create_test_series(source_key)
            self.create_test_series(dest_key)

            result = self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", agg, "60000"
            )
            assert result == b"OK"

            info = self.ts_info(source_key)
            rules = info["rules"]
            assert len(rules) == 1
            expected_rule = CompactionRule(dest_key, 60000, agg, None)
            assert expected_rule == rules[0], f"Failed for aggregator: {agg}"

    def test_disallow_replace_existing(self):
        """Test replacing an existing rule"""
        source_key = "test:source_replace"
        dest_key = "test:dest_replace"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Create initial rule
        self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        with pytest.raises(ResponseError, match="destination series is already the destination of a compaction rule"):
            # Replace with different aggregation
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "sum", "30000"
            )


    def test_create_rule_wrong_arity(self):
        """Test error handling for the wrong number of arguments"""
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.CREATERULE")

        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.CREATERULE", "key1")

        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.CREATERULE", "key1", "key2", "AGGREGATION", "avg")

    def test_create_rule_same_source_dest(self):
        """Test error when source and destination are the same"""
        key = "test:same_key"
        self.create_test_series(key)

        with pytest.raises(ResponseError, match="source and destination key cannot be the same"):
            self.client.execute_command(
                "TS.CREATERULE", key, key,
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_nonexistent_source(self):
        """Test error when the source series doesn't exist"""
        dest_key = "test:dest_nonexist"
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="TSDB: the key does not exist"):
            self.client.execute_command(
                "TS.CREATERULE", "test:nonexistent", dest_key,
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_nonexistent_dest(self):
        """Test error when destination series doesn't exist"""
        source_key = "test:source_nonexist"
        self.create_test_series(source_key)

        with pytest.raises(ResponseError, match="TSDB: the key does not exist"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, "test:nonexistent",
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_source_is_compaction(self):
        """Test error when the source is already a compaction destination"""
        source_key = "test:original"
        middle_key = "test:middle"
        final_key = "test:final"

        self.create_test_series(source_key)
        self.create_test_series(middle_key)
        self.create_test_series(final_key)

        # Create first rule: original -> middle
        self.client.execute_command(
            "TS.CREATERULE", source_key, middle_key,
            "AGGREGATION", "avg", "60000"
        )

        # Try to create rule: middle -> final (should fail)
        with pytest.raises(ResponseError,
                           match="source series is already the destination of a compaction rule"):
            self.client.execute_command(
                "TS.CREATERULE", middle_key, final_key,
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_dest_is_compaction(self):
        """Test error when the destination is already a compaction destination"""
        source1_key = "test:source1_comp"
        source2_key = "test:source2_comp"
        dest_key = "test:dest_comp"

        self.create_test_series(source1_key)
        self.create_test_series(source2_key)
        self.create_test_series(dest_key)

        # Create first rule
        self.client.execute_command(
            "TS.CREATERULE", source1_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        # Try to create second rule to same destination
        with pytest.raises(ResponseError,
                           match="destination series is already the destination of a compaction rule"):
            self.client.execute_command(
                "TS.CREATERULE", source2_key, dest_key,
                "AGGREGATION", "sum", "60000"
            )

    def test_create_rule_dest_has_rules(self):
        """Test error when the destination already has compaction rules"""
        source_key = "test:source_has_rules"
        dest_key = "test:dest_has_rules"
        final_key = "test:final_has_rules"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)
        self.create_test_series(final_key)

        # Create first rule: source -> dest
        self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        # Try to create rule: dest -> final (dest is already the source of rules)
        with pytest.raises(ResponseError,
                           match="source series is already the destination of a compaction rule"):
            self.client.execute_command(
                "TS.CREATERULE", dest_key, final_key,
                "AGGREGATION", "sum", "60000"
            )

    def test_create_rule_invalid_aggregation_keyword(self):
        """Test error with a missing or invalid AGGREGATION keyword"""
        source_key = "test:source_invalid_agg"
        dest_key = "test:dest_invalid_agg"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="missing AGGREGATION keyword"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "INVALID", "avg", "60000"
            )

    def test_create_rule_invalid_aggregation_type(self):
        """Test error with an invalid aggregation type"""
        source_key = "test:source_invalid_type"
        dest_key = "test:dest_invalid_type"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="unknown aggregation type"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "invalid_agg", "60000"
            )

    def test_create_rule_invalid_duration(self):
        """Test error with invalid bucket duration"""
        source_key = "test:source_invalid_dur"
        dest_key = "test:dest_invalid_dur"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="invalid bucket duration"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", "invalid"
            )

        with pytest.raises(ResponseError, match="invalid bucket duration"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", "-1000"
            )

    def test_create_rule_invalid_align_timestamp(self):
        """Test error with invalid alignment timestamp"""
        source_key = "test:source_invalid_align"
        dest_key = "test:dest_invalid_align"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="invalid align timestamp"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", "60000", "invalid_timestamp"
            )

    def test_create_rule_missing_bucket_duration(self):
        """Test error when bucket duration is missing"""
        source_key = "test:source_missing_dur"
        dest_key = "test:dest_missing_dur"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="wrong number of arguments for 'TS.CREATERULE' command"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg"
            )

    def test_create_rule_duration_formats(self):
        """Test various duration formats"""
        duration_formats = [
            ("60000", "milliseconds as integer"),
            ("60s", "seconds"),
            ("1m", "minutes"),
            ("1h", "hours"),
            ("1d", "days"),
        ]

        for i, (duration, desc) in enumerate(duration_formats):
            source_key = f"test:source_dur_{i}"
            dest_key = f"test:dest_dur_{i}"

            self.create_test_series(source_key)
            self.create_test_series(dest_key)

            result = self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", duration
            )
            assert result == b"OK", f"Failed for duration format: {desc}"

    def test_create_rule_functional_verification(self):
        """Test that the rule actually works for aggregation"""
        source_key = "test:source_functional"
        dest_key = "test:dest_functional"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Create rule with 1-second buckets
        self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "1000"
        )

        # Add some test data
        base_ts = int(time.time() * 1000)
        samples = [
            (base_ts, 10),
            (base_ts + 100, 20),
            (base_ts + 200, 30),
            (base_ts + 1100, 40),  # Next bucket
            (base_ts + 1200, 50),
        ]

        for timestamp, value in samples:
            self.client.execute_command("TS.ADD", source_key, timestamp, value)

        # Allow some time for compaction to process
        time.sleep(0.1)

        # Verify aggregated data exists in destination
        dest_range = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_range) > 0, "Expected aggregated data in destination series"

    def test_create_rule_case_insensitive_aggregator(self):
        """Test that aggregator names are case-insensitive"""
        source_key = "test:source_agg_case"
        dest_key = "test:dest_agg_case"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Test uppercase aggregator
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "AVG", "60000"
        )
        assert result == b"OK"

    def test_create_rule_timestamp_formats(self):
        """Test various timestamp formats for alignment"""
        source_key = "test:source_ts_fmt"
        dest_key = "test:dest_ts_fmt"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Unix timestamp in milliseconds
        ts_ms = int(time.time() * 1000)
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000", str(ts_ms)
        )
        assert result == b"OK"