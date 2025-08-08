import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTSJoin(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        """set up any state specific to the execution of the given class (which
        usually contains tests).
        """

        # Create a test series
        self.ts1 = "test_ts1"
        self.ts2 = "test_ts2"

        # Create two time series
        self.client.execute_command("TS.CREATE", self.ts1, "DUPLICATE_POLICY", "last")
        self.client.execute_command("TS.CREATE", self.ts2, "DUPLICATE_POLICY", "last")

        # Base timestamp
        self.now = 1000

        # Add data to the first series
        for i in range(10):
            ts = self.now + i * 1000
            self.client.execute_command("TS.ADD", self.ts1, ts, i * 10)

        # Add data to the second series (some overlapping, some unique)
        for i in range(5, 15):
            ts = self.now + i * 1000
            self.client.execute_command("TS.ADD", self.ts2, ts, i * 5)


    def test_inner_join(self):
        """Test inner join operation"""

        self.setup_data()

        # Inner join should only return matching timestamps
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "INNER"
        )

        # Should have 5 results (timestamps overlap from 5-9)
        assert len(result) == 5

        # Check the structure of the result
        for item in result:
            # Each row should have 3 elements: timestamp, left value, right value
            assert len(item) == 3

            # Get the timestamp and values
            ts, left_val, right_val = item

            # Calculate expected values
            idx = (ts - self.now) // 1000
            expected_left = idx * 10
            expected_right = idx * 5

            # Verify values
            assert float(left_val) == expected_left
            assert float(right_val) == expected_right

    def test_left_join(self):
        """Test left-join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "LEFT"
        )

        # Should have 10 results (all timestamps from left series)
        assert len(result) == 10

        for item in result:
            ts, left_val, right_val = item

            # Calculate the expected left value
            idx = (ts - self.now) // 1000
            expected_left = idx * 10

            # Verify left value
            assert float(left_val) == expected_left

            # For timestamps 0-4, the right value should be None
            if idx < 5:
                assert right_val is None
            else:
                # For timestamps 5-9, the right value should match
                expected_right = idx * 5
                assert float(right_val) == expected_right

    def test_right_join(self):
        """Test right join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "RIGHT"
        )

        # Should have 10 results (all timestamps from the right series)
        assert len(result) == 10

        for item in result:
            ts, left_val, right_val = item

            # Calculate the expected right value
            idx = (ts - self.now) // 1000
            expected_right = idx * 5

            # Verify right value
            assert float(right_val) == expected_right

            # For timestamps 10-14, the left value should be None
            if idx >= 10:
                assert left_val is None
            else:
                # For timestamps 5-9, the left value should match
                expected_left = idx * 10
                assert float(left_val) == expected_left

    def test_full_join(self):
        """Test full join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "FULL"
        )

        # Should have 15 results (all unique timestamps from both series)
        assert len(result) == 15

        for item in result:
            ts, left_val, right_val = item

            # Calculate the index
            idx = (ts - self.now) // 1000

            # Check left value
            if idx < 10:
                expected_left = idx * 10
                assert float(left_val) == expected_left
            else:
                assert left_val is None

            # Check the right value
            if 5 <= idx < 15:
                expected_right = idx * 5
                assert float(right_val) == expected_right
            else:
                assert right_val is None

    def test_anti_join(self):
        """Test anti-join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "ANTI"
        )

        # Should return timestamps in the left that aren't in the right (0-4)
        assert len(result) == 5
        print(result)

        for item in result:
            ts, left_val, right_val = item

            # Calculate expected value
            idx = (ts - self.now) // 1000
            expected_left = idx * 10

            # Verify values
            assert float(left_val) == expected_left
            assert right_val is None
            assert idx < 5  # Only indexes 0-4 should be present

    def test_semi_join(self):
        """Test semi-join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "SEMI"
        )

        # Should return timestamps in the left that are also in the right (5-9)
        assert len(result) == 5
        print(result)

        for item in result:
            ts, left_val, right_val = item

            # Calculate expected value
            idx = (ts - self.now) // 1000
            expected_left = idx * 10

            # Verify values
            assert float(left_val) == expected_left
            assert right_val is None # The right value should be None
            assert 5 <= idx < 10  # Only indexes 5-9 should be present

    def test_asof_join(self):
        """Test as-of join operation"""

        self.setup_data()

        # Create a special series for as-of test with non-matching timestamps
        asof_ts = "test_asof"
        self.client.execute_command("TS.CREATE", asof_ts)

        # Add data with timestamps slightly before regular points
        for i in range(5):
            ts = self.now + i * 1000 - 200  # 200ms before each point
            self.client.execute_command("TS.ADD", asof_ts, ts, i * 100)

        info = self.ts_info(asof_ts)
        print(info)
        samples = self.client.execute_command("TS.RANGE", self.ts1, "-", "+")
        print(f"left: {samples}")
        samples = self.client.execute_command("TS.RANGE", asof_ts, "-", "+")
        print(f"right: {samples}")

        # Test ASOF join with PREVIOUS strategy and tolerance
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "PREVIOUS", "500"  # 500 ms tolerance
        )

        print(f"joined: {result}")

        # All points but the first from the left series
        assert len(result) == 4

        for i, item in enumerate(result):
            ts, left_val, right_info = item

            # For the first 5 points, we should have asof matches
            if i < 5:
                # The right timestamp should be slightly earlier
                assert isinstance(right_info, list)
                right_ts, right_val = right_info
                assert right_ts == self.now + i * 1000 - 200
                assert float(right_val) == i * 100
            else:
                # For points 5-9, no match within tolerance
                assert right_info is None

    def test_join_with_reducer(self):
        """Test join with reducer operation"""

        self.setup_data()

        # Test join with SUM reducer
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "INNER", "REDUCE", "sum"
        )

        # Should have 5 results (only matching timestamps)
        assert len(result) == 5

        for item in result:
            # Each item should be [timestamp, value]
            assert len(item) == 2

            ts, value = item

            # Calculate expected sum
            idx = (ts - self.now) // 1000
            expected_left = idx * 10
            expected_right = idx * 5
            expected_sum = expected_left + expected_right

            # Verify the sum
            assert float(value) == expected_sum

    def test_join_with_aggregation(self):
        """Test join with aggregation"""

        self.setup_data()

        # Add more data points for the aggregation test
        for i in range(10, 20):
            ts = self.now + i * 500  # Create points every 500ms
            self.client.execute_command("TS.ADD", self.ts1, ts, i * 10)
            self.client.execute_command("TS.ADD", self.ts2, ts, i * 5)

        # Test join with aggregation (avg per 2000ms)
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "INNER", "REDUCE", "avg",
            "AGGREGATION", "avg", "2000"
        )

        # Check aggregation results
        assert len(result) > 0

        # Each result should have two elements (timestamp and aggregated value)
        for item in result:
            assert len(item) == 2

    def test_join_with_filter(self):
        """Test join with filters"""

        self.setup_data()

        # Test with value filter
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "INNER", "FILTER_BY_VALUE", "30", "100"
        )

        # Should only include points where the left value is between 30 and 100
        for item in result:
            ts, left_val, right_val = item
            assert 30 <= float(left_val) <= 100

        # Test with a timestamp filter
        specific_ts = self.now + 5000
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "INNER", "FILTER_BY_TS", specific_ts
        )

        # Should only include the specific timestamp
        assert len(result) == 1
        assert result[0][0] == specific_ts

    def test_join_count_limit(self):
        """Test join with count limit"""

        self.setup_data()

        # Test with count limit
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "FULL", "COUNT", "3"
        )

        # Should only return 3 results
        assert len(result) == 3

    def test_error_cases(self):
        """Test error cases"""

        self.setup_data()

        # Test with the same key for both series
        with pytest.raises(ResponseError, match="TSDB: duplicate join keys"):
            self.client.execute_command(f"TS.JOIN {self.ts1} {self.ts1} {self.now} {self.now + 15000}")

        # Test with a non-existent key
        with pytest.raises(ResponseError, match="TSDB: the key does not exist"):
            self.client.execute_command(f"TS.JOIN {self.ts1} nonexistent {self.now} {self.now + 15000}")

        # Test with an invalid join type
        with pytest.raises(ResponseError, match="TSDB: invalid JOIN command argument"):
            self.client.execute_command(f"TS.JOIN {self.ts1} {self.ts2} {self.now} {self.now + 15000} INVALID_TYPE")

        with pytest.raises(ResponseError, match="unknown binary op \"invalid_op\""):
            # Replace with different aggregation
            self.client.execute_command(f"TS.JOIN {self.ts1} {self.ts2} {self.now} {self.now + 15000} INNER REDUCE invalid_op")
        # Test with invalid reducer

    def test_asof_strategies(self):
        """Test different ASOF join strategies"""

        self.setup_data()

        # Create a special series for as-of test
        asof_ts = "test_asof_strategies"
        self.client.execute_command("TS.CREATE", asof_ts)

        # Add sparse data (only at specific points)
        points = [1500, 3500, 6500, 9500]
        for i, ts in enumerate(points):
            self.client.execute_command("TS.ADD", asof_ts, ts, i * 100)

        # Test PREVIOUS (backward) strategy
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "PREVIOUS", "2000"
        )

        # Check some key points
        # At timestamp 2000, should match with 1500
        for item in result:
            ts, left_val, right_info = item
            if ts == 2000:
                assert isinstance(right_info, list)
                right_ts, right_val = right_info
                assert right_ts == 1500

        # Test NEXT (forward) strategy
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "NEXT", "2000"
        )

        # Check some key points
        # At timestamp 2000, should match with 3500
        for item in result:
            ts, left_val, right_info = item
            if ts == 2000:
                assert isinstance(right_info, list)
                right_ts, right_val = right_info
                assert right_ts == 3500

        # Test NEAREST strategy
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "NEAREST", "2000"
        )

        # Check some key points
        # At timestamp 2000, should match with 1500 (nearest)
        for item in result:
            ts, left_val, right_info = item
            if ts == 2000:
                assert isinstance(right_info, list)
                right_ts, right_val = right_info
                assert right_ts == 1500

    def test_all_join_reducers(self):
        """Test different join reducers"""

        self.setup_data()

        reducers = [
            "sum", "avg", "min", "max",
            "eq", "ne", "gt", "lt", "gte", "lte",
            "mul", "div", "mod", "pow", "abs_diff", "sgn_diff"
        ]

        for reducer in reducers:
            result = self.client.execute_command(
                "TS.JOIN", self.ts1, self.ts2,
                self.now + 5000, self.now + 6000,  # Test one point for each
                "INNER", "REDUCE", reducer
            )

            # Should have at least one result
            assert len(result) > 0

            # Each result should be a [timestamp, value] pair
            for item in result:
                assert len(item) == 2