"""TS.QUERY / TS.QUERYRANGE replies in RESP3.

The reply is a map in both protocols. In RESP2 a map is a flat key/value array and a sample value
a bulk string; in RESP3 a map is a map and a value a double. Every other PromQL test reads RESP2,
so these check that the RESP3 shape is what the documentation shows, and that a client reading
either protocol gets the same answer, special values (NaN, +Inf, -Inf) and errors included.
"""
import math

import pytest
from valkey import ResponseError, Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from query_result import QueryResult

# Instant queries are evaluated at 3 s; the samples are at 1, 2 and 3 s.
INSTANT = [
    ("TS.QUERY", "m", "TIME", "3"),
    ("TS.QUERY", "sum by (kind) (m)", "TIME", "3"),
    ("TS.QUERY", "m[3s]", "TIME", "3"),
    ("TS.QUERY", "scalar(sum(m{kind=\"plain\"}))", "TIME", "3"),
    ("TS.QUERY", "1 + 1", "TIME", "3"),
    ("TS.QUERY", "\"text\"", "TIME", "3"),
]
RANGE = [
    ("TS.QUERYRANGE", "m", "START", "1000", "END", "3000", "STEP", "1s"),
    ("TS.QUERYRANGE", "sum(m{kind=\"plain\"})", "START", "1000", "END", "3000", "STEP", "1s"),
    ("TS.QUERYRANGE", "time()", "START", "1000", "END", "3000", "STEP", "1s"),
]


def _key(value):
    return value.decode() if isinstance(value, bytes) else value


def _same(a: float, b: float) -> bool:
    return (math.isnan(a) and math.isnan(b)) or a == b


def _rendered(result: QueryResult):
    """`result` as plain, comparable data: label maps, timestamps and values."""
    if result.is_vector():
        return sorted((sorted(s.metric.items()), s.value.timestamp, repr(s.value.value))
                      for s in result.result)
    if result.is_matrix():
        return sorted((sorted(s.metric.items()), [(p.timestamp, repr(p.value)) for p in s.values])
                      for s in result.result)
    if result.is_scalar():
        return (result.result.timestamp, repr(result.result.value))
    return (result.result.timestamp, result.result.value)


class TestQueryResp3(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        series = [
            ("m:1", ["kind", "plain", "i", "1"], [1.0, 2.0, 3.0]),
            ("m:2", ["kind", "plain", "i", "2"], [10.0, 20.0, 30.0]),
            ("m:nan", ["kind", "special", "i", "nan"], ["NaN", "NaN", "NaN"]),
            ("m:inf", ["kind", "special", "i", "inf"], ["+inf", "+inf", "+inf"]),
            ("m:ninf", ["kind", "special", "i", "ninf"], ["-inf", "-inf", "-inf"]),
        ]
        for key, labels, values in series:
            self.client.execute_command("TS.CREATE", key, "LABELS", "__name__", "m", *labels)
            for ts, value in zip((1000, 2000, 3000), values):
                self.client.execute_command("TS.ADD", key, ts, value)

    def resp3(self) -> Valkey:
        return Valkey(host=self.server.bind_ip, port=self.server.port, protocol=3)

    def test_replies_are_typed_maps_and_doubles(self):
        """RESP3 answers with real maps and doubles, as the command docs show."""
        self.setup_data()
        client = self.resp3()

        vector = client.execute_command("TS.QUERY", "m", "TIME", "3")
        assert isinstance(vector, dict), type(vector)
        assert _key(vector[b"resultType"]) == "vector"
        assert len(vector[b"result"]) == 5
        for entry in vector[b"result"]:
            assert isinstance(entry, dict) and isinstance(entry[b"metric"], dict)
            timestamp, value = entry[b"value"]
            assert timestamp == 3000
            assert isinstance(value, float), (entry, type(value))

        matrix = client.execute_command("TS.QUERYRANGE", "m", "START", "1000", "END", "3000",
                                        "STEP", "1s")
        assert _key(matrix[b"resultType"]) == "matrix"
        for entry in matrix[b"result"]:
            points = entry[b"value"]
            assert [ts for ts, _ in points] == [1000, 2000, 3000]
            assert all(isinstance(v, float) for _, v in points)

        scalar = client.execute_command("TS.QUERY", "1 + 1", "TIME", "3")
        assert _key(scalar[b"resultType"]) == "scalar"
        assert scalar[b"result"][1] == 2.0 and isinstance(scalar[b"result"][1], float)

    @pytest.mark.parametrize("command", INSTANT + RANGE, ids=lambda c: f"{c[0]}:{c[1]}")
    def test_both_protocols_give_the_same_answer(self, command):
        self.setup_data()
        resp2 = QueryResult.from_raw(self.client.execute_command(*command))
        resp3 = QueryResult.from_raw(self.resp3().execute_command(*command))
        assert resp2.result_type == resp3.result_type
        assert _rendered(resp2) == _rendered(resp3)

    def test_special_values_cross_both_protocols(self):
        """NaN and the infinities are doubles in RESP3 and strings in RESP2; both read back
        as the same float."""
        self.setup_data()
        command = ("TS.QUERY", 'm{kind="special"}', "TIME", "3")
        for raw in (self.client.execute_command(*command),
                    self.resp3().execute_command(*command)):
            values = {s.metric["i"]: s.value.value for s in QueryResult.from_raw(raw).result}
            assert math.isnan(values["nan"])
            assert values["inf"] == math.inf
            assert values["ninf"] == -math.inf

    @pytest.mark.parametrize("command", [
        ("TS.QUERY", "sum(", "TIME", "3"),
        ("TS.QUERYRANGE", "m", "START", "1000", "END", "3000", "STEP", "0"),
        ("TS.QUERYRANGE", "m[1m]", "START", "1000", "END", "3000", "STEP", "1s"),
    ], ids=["parse", "zero-step", "range-root"])
    def test_errors_are_the_same_in_both_protocols(self, command):
        self.setup_data()
        errors = []
        for client in (self.client, self.resp3()):
            with pytest.raises(ResponseError) as err:
                client.execute_command(*command)
            errors.append(str(err.value))
        assert errors[0] == errors[1]
