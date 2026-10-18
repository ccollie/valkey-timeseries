import pytest
from valkey import ResponseError, Valkey
from valkeytestframework.conftest import resource_port_tracker

from common import LabelSearchResponse
from valkey_timeseries_test_case import (
    ValkeyTimeSeriesTestCaseBase,
    ValkeyTimeSeriesTestCaseDebugMode,
)


def populate(client, count=1300):
    pipe = client.pipeline(transaction=False)
    for i in range(count):
        pipe.execute_command(
            'TS.CREATE', f'series:{i}', 'LABELS',
            'service', 'api', 'instance', f'host{i:04}', '__name__', f'metric{i:04}',
        )
    pipe.execute()


class TestLabelSearchBackground(ValkeyTimeSeriesTestCaseBase):
    @pytest.mark.parametrize('protocol', [2, 3])
    @pytest.mark.parametrize('command', [
        ('TS.LABELNAMES', 'INCLUDE_METADATA'),
        ('TS.LABELVALUES', 'instance', 'SEARCH', 'host', 'LIMIT', 3),
        ('TS.METRICNAMES', 'SORTBY', 'cardinality', 'DESC', 'LIMIT', 3, 'INCLUDE_METADATA'),
        ('TS.LABELVALUES', 'instance', 'FILTER', 'service=api', 'LIMIT', 3, 'INCLUDE_METADATA'),
    ])
    def test_background_label_search_preserves_database_and_inline_results(self, protocol, command):
        client = Valkey(host=self.server.bind_ip, port=self.server.port,
                        db=1, protocol=protocol, socket_timeout=10)
        try:
            populate(client)
            background = client.execute_command(*command)
            tx = client.pipeline(transaction=True)
            tx.execute_command(*command)
            assert tx.execute() == [background]

            parsed = LabelSearchResponse.parse(background)
            assert len(parsed.results) == 3
            if command[0] == 'TS.LABELNAMES':
                assert {r.value: r.cardinality for r in parsed.results} == {
                    b'__name__': 1300, b'instance': 1300, b'service': 1300,
                }
            else:
                prefix = b'metric' if command[0] == 'TS.METRICNAMES' else b'host'
                values = [r.value for r in parsed.results]
                assert values == [prefix + b'0000', prefix + b'0001', prefix + b'0002']
                assert parsed.has_more

            # A script cannot block its client either.
            assert client.eval('redis.call(unpack(ARGV)); return 1', 0, *command) == 1
            assert client.ping()
        finally:
            client.close()

    def test_background_label_search_keeps_callers_acl_identity(self):
        populate(self.client)
        self.client.execute_command(
            'ACL', 'SETUSER', 'label-reader', 'on', '>password',
            '+ts.labelnames', '+ts.labelvalues', '+ts.metricnames', '~series:*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='label-reader', password='password', socket_timeout=10)
        try:
            for command in [('TS.LABELNAMES',), ('TS.LABELVALUES', 'instance', 'SEARCH', 'host'),
                            ('TS.METRICNAMES', 'SORTBY', 'cardinality')]:
                with pytest.raises(ResponseError, match='insufficient permissions to query labels'):
                    reader.execute_command(*command)
            self.client.execute_command('ACL', 'SETUSER', 'label-reader', '~*')
            result = reader.execute_command('TS.LABELNAMES')
            assert len(LabelSearchResponse.parse(result).results) == 3
        finally:
            reader.close()


class TestLabelSearchBackgroundScheduling(ValkeyTimeSeriesTestCaseDebugMode):
    def test_small_value_limit_stays_inline_and_large_name_scan_uses_lane(self):
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client)
            client.execute_command('TS._DEBUG', 'PANIC_NEXT_ANALYSIS_JOB')
            # These cheap searches must leave the worker-only panic hook untouched.
            result = client.execute_command('TS.LABELVALUES', 'instance', 'LIMIT', 1)
            assert len(LabelSearchResponse.parse(result).results) == 1
            result = client.execute_command('TS.LABELVALUES', 'missing', 'SEARCH', 'host')
            assert not LabelSearchResponse.parse(result).results
            # Names require summing the values' cardinalities, despite LIMIT 1.
            with pytest.raises(ResponseError, match='internal error'):
                client.execute_command('TS.LABELNAMES', 'LIMIT', 1)
            assert client.ping()
            result = client.execute_command('TS.LABELNAMES')
            assert len(LabelSearchResponse.parse(result).results) == 3
        finally:
            client.close()


LANE_CASES = [
    ('TS.LABELNAMES', 'INCLUDE_METADATA'),
    ('TS.LABELVALUES', 'instance', 'SEARCH', 'host', 'LIMIT', 3),
    ('TS.METRICNAMES', 'SORTBY', 'cardinality', 'DESC', 'LIMIT', 3, 'INCLUDE_METADATA'),
    ('TS.LABELVALUES', 'instance', 'FILTER', 'service=api', 'LIMIT', 3, 'INCLUDE_METADATA'),
]


class TestLabelSearchLaneUse(ValkeyTimeSeriesTestCaseDebugMode):
    """Pins which searches leave the main thread: an armed panic hook fires only on the lane."""

    def arm(self, client):
        client.execute_command('TS._DEBUG', 'PANIC_NEXT_ANALYSIS_JOB')

    @pytest.mark.parametrize('command', LANE_CASES)
    def test_large_searches_run_on_the_lane(self, command):
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client)
            self.arm(client)
            with pytest.raises(ResponseError, match='internal error'):
                client.execute_command(*command)
            # The hook is spent: the same search now answers.
            assert LabelSearchResponse.parse(client.execute_command(*command)).results
        finally:
            client.close()

    def test_a_name_search_is_budgeted_like_a_name_scan(self):
        """~900 index entries: over the SEARCH value budget (256), under the name budget (1024)."""
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client, count=300)
            self.arm(client)
            result = client.execute_command('TS.LABELNAMES', 'SEARCH', 'serv', 'inst', 'name')
            assert LabelSearchResponse.parse(result).results
            # Still armed: the search above ran inline. A value search with SEARCH over the
            # same 300 values exceeds its budget and takes the lane.
            with pytest.raises(ResponseError, match='internal error'):
                client.execute_command('TS.LABELVALUES', 'instance', 'SEARCH', 'host')
        finally:
            client.close()

    def test_an_unauthorized_search_is_refused_before_the_lane(self):
        populate(self.client)
        self.client.execute_command(
            'ACL', 'SETUSER', 'narrow-reader', 'on', '>password',
            '+ts.labelnames', '+ts.labelvalues', '+ts.metricnames', '~series:*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='narrow-reader', password='password', socket_timeout=10)
        try:
            self.arm(self.client)
            for command in LANE_CASES:
                with pytest.raises(ResponseError, match='insufficient permissions to query labels'):
                    reader.execute_command(*command)
            # None of them reached the lane: the hook is still armed for the next job.
            with pytest.raises(ResponseError, match='internal error'):
                self.client.execute_command(*LANE_CASES[0])
        finally:
            reader.close()

