"""
Integration tests for the Sharadar data provider's transport and acquisition.

These exercise the full processing pipeline against recorded Sharadar responses instead of the live API: only
the HTTP transport is replaced, while envelope unwrapping, pagination, ticker batching, error translation, rate
limit handling and typing all run through the production code.

Fixtures are pickled recordings, one instrument per table over the same window, each stored as the JSON
envelope the query API answers with.
"""

import datetime
import decimal
import email.message
import http.client
import io
import json
import logging
import pathlib
import random
import ssl
import time
import urllib.error

from types import SimpleNamespace

import pandas
import pyarrow
import pyarrow.compute
import pytest

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.data_providers.sharadar import (
    Sharadar,
    SharadarClient,
    SharadarUnparsableValuesError,
    _split_denominator,
    _split_numerator,
)
from kaxanuk.data_curator.modules.data_column import DataColumn
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import (
    DataProviderAuthenticationError,
    DataProviderAuthorizationError,
    DataProviderFatalError,
    DataProviderMissingKeyError,
    DataProviderParsingError,
    DataProviderRateLimitError,
    DataProviderServerError,
    IdentifierNotFoundError,
    InjectedDependencyError,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataProviderToolkit


FIXTURE_DIR = pathlib.Path(__file__).resolve().parent / 'fixtures'

# Fixture file names follow Sharadar's own documentation of each table. Each one holds a single instrument over
# the same window.
FIXTURE_NAMES = {
    'actions': 'corporate_actions',
    'fundamentals': 'fundamentals',
    'funds': 'fund_prices',
    'stocks': 'stock_prices',
    'tickers': 'tickers_metadata',
}


def load_fixture(table):
    """
    Load the recording of a Sharadar table.

    Each recording is the JSON envelope the query API answers with -- a dict of a row count and a list of
    records -- which preserves every value as JSON carried it, integers included. Feeding these through the
    production parsing is what makes the tests exercise the real typing of the API's responses.
    """
    return pandas.read_pickle(     # noqa: S301  the fixtures are repository content
        FIXTURE_DIR / f'{FIXTURE_NAMES[table]}.pkl'
    )


def as_api_records(envelope):
    """Take the records out of a recorded response envelope."""

    return envelope['data']


STOCKS_ENVELOPE = load_fixture('stocks')
FUNDS_ENVELOPE = load_fixture('funds')
FUNDAMENTALS_ENVELOPE = load_fixture('fundamentals')
ACTIONS_ENVELOPE = load_fixture('actions')
TICKERS_ENVELOPE = load_fixture('tickers')

STOCKS_RECORDS = STOCKS_ENVELOPE['data']
FUNDS_RECORDS = FUNDS_ENVELOPE['data']
FUNDAMENTALS_RECORDS = FUNDAMENTALS_ENVELOPE['data']
ACTIONS_RECORDS = ACTIONS_ENVELOPE['data']
TICKERS_RECORDS = TICKERS_ENVELOPE['data']

# Each fixture records one instrument, so its identity is read off the recording itself
EQUITY_TICKER = STOCKS_RECORDS[0]['ticker']
FUND_TICKER = FUNDS_RECORDS[0]['ticker']

STOCKS_API_RECORDS = as_api_records(STOCKS_ENVELOPE)
FUNDAMENTALS_API_RECORDS = as_api_records(FUNDAMENTALS_ENVELOPE)

STOCK_RECORD = STOCKS_API_RECORDS[0]
FUNDAMENTAL_RECORD = FUNDAMENTALS_API_RECORDS[0]

STOCK_ROWS = len(STOCKS_RECORDS)
# A session predating the split, where the three adjustment bases hold three different numbers. Asserting on a
# later one would pass just as well with the bases swapped, since after the last split they all coincide.
ADJUSTMENT_DIVERGENT_RECORD = next(
    record
    for record in STOCKS_RECORDS
    if record['close'] != record['closeunadj'] != record['closeadj']
)


START_DATE = datetime.date(2022, 1, 1)
END_DATE = datetime.date(2025, 12, 31)


def fixture_table(table, dimension=None):
    """Build the typed table a fixture's response produces, through the production record casting."""
    parsed = SharadarClient.records_to_table(table, as_api_records(load_fixture(table)))

    if dimension is not None:
        parsed = parsed.filter(pyarrow.compute.equal(parsed.column('dimension'), dimension))

    return parsed


def make_configuration(identifiers=(EQUITY_TICKER, FUND_TICKER), period='quarterly'):
    """Build a Configuration covering the fixtures' instruments and window."""

    return Configuration(
        start_date=START_DATE,
        end_date=END_DATE,
        period=period,
        identifiers=tuple(identifiers),
        columns=('m_open', 'm_close'),
    )


def data_block_data(provider, data_block, identifier, period='quarterly'):
    """Ask a provider for one data block's entity, the way the orchestrator does."""

    return provider.get_data_block_data(
        main_identifier=identifier,
        data_block=data_block,
        configuration=make_configuration(period=period),
    )


@pytest.fixture(autouse=True)
def _isolated_toolkit_memoization():
    """
    Give every test the memoization state of a fresh process.

    The toolkit memoizes a data block's column remaps and preprocessors for the process, which is right for a
    run, since a run feeds each data block from a single provider. A test session is the one place where two
    providers share a process, so the memoization is cleared around each test instead of letting whichever
    provider ran first serve its own tags to the next one.
    """
    for cache in (
        DataProviderToolkit._data_block_endpoint_column_remaps,
        DataProviderToolkit._data_block_endpoint_field_preprocessors,
    ):
        cache.clear()

    yield

    for cache in (
        DataProviderToolkit._data_block_endpoint_column_remaps,
        DataProviderToolkit._data_block_endpoint_field_preprocessors,
    ):
        cache.clear()


@pytest.fixture
def provider():
    return Sharadar(api_key='test-api-key')


@pytest.fixture
def initialized_provider(provider, monkeypatch):
    """A provider whose tables come from the recorded endpoint fixtures instead of the live API."""
    monkeypatch.setattr(
        provider,
        '_request_table',
        lambda table, *, identifiers, extra_params=None, filter_dates=True:
            fixture_table(table, dimension=(extra_params or {}).get('dimension')),
    )
    provider.initialize(configuration=make_configuration())

    return provider


@pytest.fixture
def client():
    return SharadarClient(api_key='test-api-key')


def make_http_error(code, payload=None, headers=None):
    """Build an HTTPError carrying a Sharadar style JSON error body."""
    message_headers = email.message.Message()
    for (key, value) in (headers or {}).items():
        message_headers[key] = value

    body = json.dumps(payload).encode('utf-8') if payload is not None else b''

    return urllib.error.HTTPError(
        'https://api.sharadar.com/v1.0/data/stocks',
        code,
        'error',
        message_headers,
        io.BytesIO(body),
    )


class TestBatchTickers:
    """The provider rejects requests carrying more than 30 tickers or a ticker parameter over 200 characters."""

    def test_splits_on_the_ticker_count_limit(self):
        batches = SharadarClient.batch_tickers(['AAPL'] * 100)

        assert [len(batch) for batch in batches] == [30, 30, 30, 10]

    def test_splits_on_the_parameter_length_limit(self):
        batches = SharadarClient.batch_tickers(['ABCDEFGHIJ'] * 30)

        # 10-character tickers hit the 200 character cap well before the 30 ticker cap
        assert all(
            len(','.join(batch)) <= SharadarClient.MAX_TICKER_PARAM_LENGTH
            for batch in batches
        )
        assert sum(len(batch) for batch in batches) == 30
        assert len(batches) > 1

    def test_keeps_every_ticker_exactly_once_and_in_order(self):
        tickers = [f'T{index:04d}' for index in range(97)]

        batches = SharadarClient.batch_tickers(tickers)

        assert [ticker for batch in batches for ticker in batch] == tickers

    def test_exactly_the_ticker_limit_fits_in_one_batch(self):
        batches = SharadarClient.batch_tickers(['AAPL'] * SharadarClient.MAX_TICKERS_PER_REQUEST)

        assert len(batches) == 1

    def test_empty_ticker_list_produces_no_batches(self):
        assert SharadarClient.batch_tickers([]) == []


class TestRecordsToTable:
    """The types come from the values each response carries, the way every other provider's data is typed."""

    def test_integer_columns_arrive_as_the_strings_the_api_sends(self):
        table = SharadarClient.records_to_table('fundamentals', FUNDAMENTALS_API_RECORDS)

        # the API renders every bigint column as a JSON string, and nothing here undoes that
        assert isinstance(FUNDAMENTAL_RECORD['assets'], str)
        assert table.column('assets').type == pyarrow.string()

    def test_date_columns_arrive_as_timestamps(self):
        table = SharadarClient.records_to_table('stocks', [STOCK_RECORD])

        # the endpoint field maps are what turns these into dates, through cast_datetime_to_date
        assert table.column('date').type == pyarrow.timestamp('s')

    def test_whole_numbers_and_decimals_in_one_column_are_not_a_disagreement(self):
        """
        A price that happens to be a whole number is not a malformed value, and its ticker is not skipped.
        """
        records = [
            {'ticker': 'KXA', 'date': '2024-03-02', 'close': 246},
            {'ticker': 'KXB', 'date': '2024-03-01', 'close': 179.66},
        ]

        table = SharadarClient.records_to_table('stocks', records)

        assert table.column('close').to_pylist() == [246.0, 179.66]

    def test_a_column_absent_from_a_page_is_absent_from_its_table(self):
        table = SharadarClient.records_to_table('fundamentals', [{'ticker': EQUITY_TICKER}])

        assert table.column_names == ['ticker']

    def test_empty_records_produce_a_table_with_no_columns(self):
        table = SharadarClient.records_to_table('stocks', [])

        assert table.num_rows == 0
        assert table.num_columns == 0

    def test_columns_no_field_map_reads_are_kept(self):
        table = SharadarClient.records_to_table('stocks', [{**STOCK_RECORD, 'unexpected': 1}])

        assert 'unexpected' in table.column_names

    def test_a_column_whose_json_type_changes_names_the_tickers_holding_it(self):
        """
        PyArrow reads a page in one pass and reports a row number, so whose value it is gets worked out here.
        """
        records = [
            {'ticker': 'KXA', 'date': '2024-03-02', 'close': 179.66},
            {'ticker': 'KXB', 'date': '2024-03-01', 'close': '17O.55'},
        ]

        with pytest.raises(SharadarUnparsableValuesError) as raised:
            SharadarClient.records_to_table('stocks', records)

        assert raised.value.tickers == frozenset({'KXB'})
        assert "KXB close='17O.55'" in str(raised.value)


class TestConcatenatePages:
    """
    Two pages of one query can infer different types, which is what a declared schema would settle up front.
    """

    def test_a_column_null_in_one_page_and_filled_in_another_is_reconciled(self):
        page_without_values = SharadarClient.records_to_table(
            'fundamentals', [{'ticker': 'KXA', 'date': '2024-03-02', 'taxassets': None}]
        )
        page_with_values = SharadarClient.records_to_table(
            'fundamentals', [{'ticker': 'KXB', 'date': '2024-03-01', 'taxassets': '50'}]
        )

        joined = SharadarClient.concatenate_pages([page_without_values, page_with_values])

        assert joined.column('taxassets').to_pylist() == [None, '50']

    def test_a_column_missing_from_one_page_is_filled_with_nulls(self):
        page_with_column = SharadarClient.records_to_table(
            'fundamentals', [{'ticker': 'KXA', 'date': '2024-03-02', 'taxassets': '50'}]
        )
        page_without_column = SharadarClient.records_to_table(
            'fundamentals', [{'ticker': 'KXB', 'date': '2024-03-01'}]
        )

        joined = SharadarClient.concatenate_pages([page_with_column, page_without_column])

        assert joined.column('taxassets').to_pylist() == ['50', None]

    def test_pages_with_no_rows_are_left_out(self):
        page = SharadarClient.records_to_table('stocks', [STOCK_RECORD])

        joined = SharadarClient.concatenate_pages([page, SharadarClient.records_to_table('stocks', [])])

        assert joined.num_rows == 1

    def test_nothing_to_concatenate_produces_a_table_with_no_columns(self):
        assert SharadarClient.concatenate_pages([]).num_columns == 0


class FakeSharadarTable:
    """
    Serves records the way the query API does, for replacing `SharadarClient._request_records`.

    Rows come in descending date order, while rows of the same date come in a different order on every request,
    which is what Sharadar does and what makes paging by position lose rows. It honours ``limit``, ``skip`` and
    ``date.lte``.
    """

    def __init__(self, records, seed=0):
        self.records = records
        self.random = random.Random(seed)     # noqa: S311  shuffles test rows, not cryptographic
        self.requests = []

    def __call__(self, _table, params):
        self.requests.append(params)
        latest_date = params.get('date.lte')
        rows_by_date = {}
        for record in self.records:
            if latest_date is None or record['date'] <= latest_date:
                rows_by_date.setdefault(record['date'], []).append(record)

        ordered_rows = []
        for date in sorted(rows_by_date, reverse=True):
            same_date_rows = list(rows_by_date[date])
            self.random.shuffle(same_date_rows)
            ordered_rows.extend(same_date_rows)

        skip = int(params.get('skip', 0))

        return ordered_rows[skip:skip + int(params['limit'])]


def stock_records(tickers, dates):
    """Build one stock record per ticker and date."""

    return [
        {**STOCK_RECORD, 'ticker': ticker, 'date': date}
        for ticker in tickers
        for date in dates
    ]


def table_keys(table):
    """The (ticker, date) keys of a stocks table, as the ISO strings the API sends."""

    return {
        (ticker, date.date().isoformat())
        for (ticker, date) in zip(table.column('ticker').to_pylist(), table.column('date').to_pylist(), strict=True)
    }


class TestPagination:
    """`count` reports the rows of the current page, so a full page is the only signal that more rows remain."""

    def test_pages_until_a_page_comes_back_short(self, client, monkeypatch):
        client.page_size = 2
        server = FakeSharadarTable(stock_records(['AAPL'], [f'2024-03-{day:02d}' for day in range(1, 6)]))
        monkeypatch.setattr(client, '_request_records', server)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        # each page after the first asks for the rows dated on or before the last date it has seen
        assert [params.get('date.lte') for params in server.requests] == [
            None, '2024-03-04', '2024-03-03', '2024-03-02', '2024-03-01',
        ]
        assert all('skip' not in params for params in server.requests)
        # the order the date paging relies on is asked for rather than assumed
        assert all(params['sort'] == 'date.desc' for params in server.requests)
        assert table.num_rows == 5

    def test_a_short_first_page_issues_a_single_request(self, client, monkeypatch):
        calls = []

        def fake_request_records(_table, params):
            calls.append(params)

            return [STOCK_RECORD]

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        client.fetch_table(table='stocks', tickers=['AAPL'])

        assert len(calls) == 1

    @pytest.mark.parametrize('seed', range(5))
    def test_no_row_is_lost_where_a_page_boundary_splits_a_date(self, client, monkeypatch, seed):
        client.page_size = 7
        records = stock_records(['KXA', 'KXB', 'KXC'], [f'2024-03-{day:02d}' for day in range(1, 11)])
        monkeypatch.setattr(client, '_request_records', FakeSharadarTable(records, seed))

        table = client.fetch_table(table='stocks', tickers=['KXA', 'KXB', 'KXC'])

        # three rows share every date, so a boundary every seven rows falls inside a date on most pages; since
        # Sharadar orders the rows of a date differently on every request, paging by position with `skip` would
        # lose some of them there, which is why the client pages by date
        assert table.num_rows == len(records)
        assert table_keys(table) == {(record['ticker'], record['date']) for record in records}

    def test_a_page_out_of_date_order_is_refused(self, client, monkeypatch):
        client.page_size = 2
        ascending_page = stock_records(['AAPL'], ['2024-03-01', '2024-03-02'])
        monkeypatch.setattr(client, '_request_records', lambda _table, _params: ascending_page)

        with pytest.raises(DataProviderParsingError, match='descending date order'):
            client.fetch_table(table='stocks', tickers=['AAPL'])

    def test_a_date_holding_a_whole_page_is_refused_rather_than_truncated(self, client, monkeypatch):
        client.page_size = 2
        records = stock_records(['KXA', 'KXB', 'KXC'], ['2024-03-01'])
        monkeypatch.setattr(client, '_request_records', FakeSharadarTable(records))

        with pytest.raises(DataProviderFatalError, match='2024-03-01'):
            client.fetch_table(table='stocks', tickers=['KXA', 'KXB', 'KXC'])

    def test_a_table_without_dates_is_paged_by_position(self, client, monkeypatch):
        client.page_size = 2
        requested_params = []
        rows = [
            {'table': 'SEP', 'permaticker': index, 'ticker': f'KX{index}'}
            for index in range(3)
        ]

        def fake_request_records(_table, params):
            requested_params.append(params)
            skip = int(params.get('skip', 0))

            return rows[skip:skip + 2]

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        table = client.fetch_table(table='tickers', tickers=['KX0', 'KX1', 'KX2'])

        assert [params.get('skip') for params in requested_params] == [None, '2']
        assert all('sort' not in params for params in requested_params)
        assert table.num_rows == 3

    def test_rows_repeated_across_pages_are_deduplicated_by_primary_key(self, client, monkeypatch):
        client.page_size = 2
        server = FakeSharadarTable(stock_records(['AAPL'], ['2024-03-01', '2024-03-02', '2024-03-03']))
        monkeypatch.setattr(client, '_request_records', server)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        # the boundary date comes back on both sides of every page break, and is kept once
        received_rows = sum(len(server(None, params)) for params in list(server.requests))
        assert received_rows > table.num_rows
        assert table.num_rows == 3


class TestFetchTable:
    def test_batches_the_tickers_across_requests(self, client, monkeypatch):
        requested_tickers = []

        def fake_request_records(_table, params):
            requested_tickers.append(params['ticker'])

            return []

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        client.fetch_table(table='stocks', tickers=[f'KX{index:03d}' for index in range(65)])

        assert len(requested_tickers) == 3
        assert all(
            len(tickers) <= SharadarClient.MAX_TICKER_PARAM_LENGTH
            for tickers in requested_tickers
        )

    def test_passes_the_date_range_and_extra_params(self, client, monkeypatch):
        captured = {}

        def fake_request_records(_table, params):
            captured.update(params)

            return []

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        client.fetch_table(
            table='fundamentals',
            tickers=['AAPL'],
            start_date='2020-01-01',
            end_date='2024-12-31',
            extra_params={'dimension': 'ARQ'},
        )

        # the range is expressed as the paging's own comparisons: given as `from`, the provider answers a
        # paged query with one row from before the range
        assert captured['date.gte'] == '2020-01-01'
        assert captured['dimension'] == 'ARQ'
        assert captured['format'] == 'json'

    def test_unknown_table_raises_a_fatal_error(self, client):
        with pytest.raises(DataProviderFatalError):
            client.fetch_table(table='not_a_sharadar_table')

    def test_builds_urls_against_the_data_endpoint(self, client):
        url = client._build_url('stocks', {'api_key': 'k', 'format': 'json', 'ticker': 'KXNA,KXNB'})

        assert url.startswith('https://api.sharadar.com/v1.0/data/stocks?')
        assert 'format=json' in url
        assert 'ticker=KXNA,KXNB' in url     # the provider expects unescaped commas


class TestErrorTranslation:
    """Sharadar signals a rejected key, an out-of-plan ticker and a malformed request with different codes."""

    @pytest.mark.parametrize(
        ('status_code', 'payload', 'expected_exception'),
        [
            (401, {'error': 'Unauthorized'}, DataProviderAuthenticationError),
            (403, {'error': 'Exceeds free tier', 'description': 'Please sign up at /pricing.'},
             DataProviderAuthorizationError),
            (400, {'error': 'Too many tickers', 'description': 'ticker accepts at most 30 tickers per request'},
             DataProviderFatalError),
            (429, {'error': 'Rate limited'}, DataProviderRateLimitError),
        ],
    )
    def test_status_codes_map_to_their_exceptions(self, status_code, payload, expected_exception):
        with pytest.raises(expected_exception):
            SharadarClient.raise_for_http_error('stocks', make_http_error(status_code, payload))

    def test_server_errors_are_left_to_be_retried(self):
        assert SharadarClient.raise_for_http_error('stocks', make_http_error(503)) is None

    def test_the_error_payload_reaches_the_message(self):
        with pytest.raises(DataProviderAuthorizationError, match='Exceeds free tier'):
            SharadarClient.raise_for_http_error(
                'stocks',
                make_http_error(403, {'error': 'Exceeds free tier', 'description': 'Please sign up.'}),
            )

    def test_rate_limit_errors_carry_the_retry_after_header(self):
        with pytest.raises(DataProviderRateLimitError) as raised:
            SharadarClient.raise_for_http_error('stocks', make_http_error(429, {}, {'Retry-After': '7'}))

        assert raised.value.retry_after == 7

    def test_an_error_payload_returned_with_a_200_is_still_raised(self, client, monkeypatch):
        monkeypatch.setattr(
            client,
            '_request_with_retries',
            lambda _table, _url: json.dumps({'error': 'Exceeds free tier'}),
        )

        with pytest.raises(DataProviderFatalError):
            client.fetch_table(table='stocks', tickers=['AAPL'])

    def test_a_response_that_is_not_json_raises_a_parsing_error(self, client, monkeypatch):
        monkeypatch.setattr(client, '_request_with_retries', lambda _table, _url: '<html>nope</html>')

        with pytest.raises(DataProviderParsingError):
            client.fetch_table(table='stocks', tickers=['AAPL'])

    def test_an_envelope_without_data_raises_a_parsing_error(self, client, monkeypatch):
        monkeypatch.setattr(client, '_request_with_retries', lambda _table, _url: json.dumps({'count': 0}))

        with pytest.raises(DataProviderParsingError):
            client.fetch_table(table='stocks', tickers=['AAPL'])


class TestRetries:
    def test_transient_failures_are_retried_and_then_succeed(self, client, monkeypatch):
        attempts = []

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)
            if len(attempts) < 3:
                msg = 'temporarily unreachable'

                raise urllib.error.URLError(msg)

            return FakeResponse(json.dumps({'count': 1, 'data': [STOCK_RECORD]}).encode('utf-8'))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert len(attempts) == 3
        assert table.num_rows == 1

    def test_exhausted_retries_raise_a_server_error(self, client, monkeypatch):
        def fake_urlopen(_request, context=None, timeout=None):
            msg = 'unreachable'

            raise urllib.error.URLError(msg)

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        with pytest.raises(DataProviderServerError):
            client.fetch_table(table='stocks', tickers=['AAPL'])

    def test_rate_limited_requests_are_retried_instead_of_aborting_the_run(self, client, monkeypatch):
        attempts = []
        waits = []

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)
            if len(attempts) < 2:

                raise make_http_error(429, {'error': 'Rate limited'}, {'Retry-After': '3'})

            return FakeResponse(json.dumps({'count': 1, 'data': [STOCK_RECORD]}).encode('utf-8'))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', waits.append)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert len(attempts) == 2
        assert table.num_rows == 1
        # the wait honours the provider's Retry-After header, plus the jitter that spreads parallel instances
        assert len(waits) == 1
        assert 3 <= waits[0] <= 3 + SharadarClient.RATE_LIMIT_JITTER_SECONDS

    def test_persistent_rate_limiting_surfaces_as_a_rate_limit_error(self, client, monkeypatch):
        def fake_urlopen(_request, context=None, timeout=None):

            raise make_http_error(429, {'error': 'Rate limited'})

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        with pytest.raises(DataProviderRateLimitError):
            client.fetch_table(table='stocks', tickers=['AAPL'])

    def test_every_request_carries_a_timeout(self, client, monkeypatch):
        """A urlopen without a timeout blocks forever, so a stalled connection would hang the whole run."""
        timeouts = []

        def fake_urlopen(_request, context=None, timeout=None):
            timeouts.append(timeout)

            return FakeResponse(json.dumps({'count': 1, 'data': [STOCK_RECORD]}).encode('utf-8'))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)

        client.fetch_table(table='stocks', tickers=[EQUITY_TICKER])

        assert all(timeout == SharadarClient.REQUEST_TIMEOUT_SECONDS for timeout in timeouts)
        assert len(timeouts) > 0

    def test_a_stalled_connection_surfaces_as_a_retryable_error(self, client, monkeypatch):
        attempts = []

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)
            msg = 'timed out'

            raise TimeoutError(msg)

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        with pytest.raises(DataProviderServerError):
            client.fetch_table(table='stocks', tickers=[EQUITY_TICKER])

        assert len(attempts) == SharadarClient.MAX_CONNECTION_RETRIES

    @pytest.mark.parametrize(
        'read_error',
        [
            ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host'),
            ssl.SSLEOFError(8, 'EOF occurred in violation of protocol'),
            http.client.IncompleteRead(b'{"count": 1, "da'),
        ],
        ids=['connection_reset', 'ssl_eof', 'incomplete_read'],
    )
    def test_a_connection_dropped_while_reading_is_retried(self, client, monkeypatch, read_error):
        """The server can drop a connection after answering, so the error surfaces from reading the body."""
        attempts = []

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)
            if len(attempts) < 2:

                return FailingReadResponse(read_error)

            return FakeResponse(json.dumps({'count': 1, 'data': [STOCK_RECORD]}).encode('utf-8'))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert len(attempts) == 2
        assert table.num_rows == 1

    def test_non_retryable_errors_are_not_retried(self, client, monkeypatch):
        attempts = []

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)

            raise make_http_error(403, {'error': 'Exceeds free tier'})

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        with pytest.raises(DataProviderAuthorizationError):
            client.fetch_table(table='stocks', tickers=['AAPL'])

        assert len(attempts) == 1


class TestRateLimitWaits:
    """
    Sharadar counts requests per API key over a 15 minute window, and every response announces when it resets.

    Retrying a throttled request before then is certain to be throttled again, so the wait runs until the reset,
    however long that is. Capping it at a minute, as for other transient errors, would give up on a run that
    only needed to slow down.
    """

    def test_a_throttled_request_waits_until_the_announced_reset(self, monkeypatch):
        monkeypatch.setattr('time.time', lambda: 1_000_000.0)

        with pytest.raises(DataProviderRateLimitError) as raised:
            SharadarClient.raise_for_http_error(
                'stocks',
                make_http_error(429, {}, {SharadarClient.RATE_LIMIT_RESET_HEADER: '1000600'}),
            )

        assert raised.value.retry_after == 600

    def test_an_explicit_retry_after_takes_precedence_over_the_reset(self):
        headers = {
            'Retry-After': '7',
            SharadarClient.RATE_LIMIT_RESET_HEADER: str(int(time.time()) + 600),
        }

        with pytest.raises(DataProviderRateLimitError) as raised:
            SharadarClient.raise_for_http_error('stocks', make_http_error(429, {}, headers))

        assert raised.value.retry_after == 7

    def test_a_reset_already_past_asks_for_no_wait(self, monkeypatch):
        monkeypatch.setattr('time.time', lambda: 1_000_000.0)

        with pytest.raises(DataProviderRateLimitError) as raised:
            SharadarClient.raise_for_http_error(
                'stocks',
                make_http_error(429, {}, {SharadarClient.RATE_LIMIT_RESET_HEADER: '999990'}),
            )

        assert raised.value.retry_after == 0

    def test_the_wait_for_a_reset_is_not_capped_at_a_minute(self):
        error = DataProviderRateLimitError(429, 'Rate limited', retry_after=600)

        wait_seconds = SharadarClient._retry_wait_seconds(1, error)

        assert wait_seconds > SharadarClient.MAX_RETRY_WAIT_SECONDS
        assert 600 <= wait_seconds <= 600 + SharadarClient.RATE_LIMIT_JITTER_SECONDS

    def test_the_wait_is_bounded_by_the_window(self):
        error = DataProviderRateLimitError(429, 'Rate limited', retry_after=10 * 3600)

        wait_seconds = SharadarClient._retry_wait_seconds(1, error)

        assert wait_seconds <= (
            SharadarClient.MAX_RATE_LIMIT_WAIT_SECONDS + SharadarClient.RATE_LIMIT_JITTER_SECONDS
        )

    def test_a_throttled_response_without_hints_backs_off_from_half_a_minute(self):
        error = DataProviderRateLimitError(429, 'Rate limited')

        first_wait = SharadarClient._retry_wait_seconds(1, error)
        second_wait = SharadarClient._retry_wait_seconds(2, error)

        assert first_wait >= SharadarClient.RATE_LIMIT_BACKOFF_SECONDS
        assert second_wait >= 2 * SharadarClient.RATE_LIMIT_BACKOFF_SECONDS

    def test_other_transient_errors_follow_their_own_schedule(self):
        error = urllib.error.URLError('temporarily unreachable')

        waits = [
            SharadarClient._retry_wait_seconds(attempt_number, error)
            for attempt_number in range(1, SharadarClient.MAX_CONNECTION_RETRIES)
        ]

        assert waits == list(SharadarClient.RETRY_WAIT_SECONDS)

    def test_a_network_outage_is_waited_out_for_minutes_before_giving_up(self, client, monkeypatch):
        """A run over the whole market lasts hours, so an outage of a minute or two must not abort a batch."""
        waits = []

        def fake_urlopen(_request, context=None, timeout=None):
            msg = 'getaddrinfo failed'

            raise urllib.error.URLError(msg)

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', waits.append)

        with pytest.raises(DataProviderServerError):
            client.fetch_table(table='stocks', tickers=['AAPL'])

        assert sum(waits) >= 3 * 60
        assert max(waits) <= SharadarClient.MAX_RETRY_WAIT_SECONDS

    def test_an_outage_shorter_than_the_waits_is_survived(self, client, monkeypatch):
        attempts = []

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)
            if len(attempts) < SharadarClient.MAX_CONNECTION_RETRIES:
                msg = 'getaddrinfo failed'

                raise urllib.error.URLError(msg)

            return FakeResponse(json.dumps({'count': 1, 'data': [STOCK_RECORD]}).encode('utf-8'))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert table.num_rows == 1

    def test_a_run_throttled_mid_way_resumes_after_the_reset(self, client, monkeypatch):
        attempts = []
        waits = []
        reset_timestamp = int(time.time()) + 900

        def fake_urlopen(_request, context=None, timeout=None):
            attempts.append(1)
            if len(attempts) < 2:

                raise make_http_error(
                    429,
                    {'error': 'Rate limited'},
                    {SharadarClient.RATE_LIMIT_RESET_HEADER: str(reset_timestamp)},
                )

            return FakeResponse(json.dumps({'count': 1, 'data': [STOCK_RECORD]}).encode('utf-8'))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)
        monkeypatch.setattr('time.sleep', waits.append)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert table.num_rows == 1
        # the wait covers the rest of the window, which the old one minute ceiling would have cut short
        assert waits[0] >= 890


class TestStreamedAcquisition:
    """
    A run over thousands of identifiers issues thousands of requests, so it has to hold up while doing so.

    Pages that differ in their columns still come together, the progress of every table is logged, and a batch
    that fails says which one it was.
    """

    def test_pages_missing_a_column_still_come_out_typed(self, client, monkeypatch):
        client.page_size = 2
        records = [
            {**STOCK_RECORD, 'date': '2024-03-03'},
            {key: value for (key, value) in STOCK_RECORD.items() if key != 'closeadj'} | {'date': '2024-03-02'},
            {**STOCK_RECORD, 'date': '2024-03-01'},
        ]
        monkeypatch.setattr(client, '_request_records', FakeSharadarTable(records))

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert table.num_rows == 3
        assert table.column('closeadj').type == pyarrow.float64()
        assert table.column('closeadj').null_count == 1

    def test_a_ticker_listed_twice_is_requested_once(self, client, monkeypatch):
        requested_tickers = []

        def fake_request_records(_table, params):
            requested_tickers.append(params['ticker'])

            return []

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        client.fetch_table(table='stocks', tickers=['AAPL', 'MSFT', 'AAPL'])

        assert requested_tickers == ['AAPL,MSFT']

    def test_no_tickers_produce_a_table_with_no_columns(self, client, monkeypatch):
        monkeypatch.setattr(client, '_request_records', lambda _table, _params: pytest.fail('nothing to request'))

        table = client.fetch_table(table='stocks', tickers=[])

        assert table.num_rows == 0
        assert table.num_columns == 0

    def test_progress_is_logged_as_the_batches_complete(self, client, monkeypatch, caplog):
        monkeypatch.setattr(client, '_request_records', lambda _table, _params: [STOCK_RECORD])

        with caplog.at_level(logging.INFO):
            client.fetch_table(table='stocks', tickers=[f'KX{index:03d}' for index in range(65)])

        assert 'fetching 65 tickers in 3 batches' in caplog.text
        assert 'batch 3 of 3 fetched' in caplog.text

    def test_progress_logging_stays_short_for_a_large_universe(self, client, monkeypatch, caplog):
        tickers = [f'KX{index:05d}' for index in range(13549)]
        batch_count = len(SharadarClient.batch_tickers(tickers))
        monkeypatch.setattr(client, '_request_records', lambda _table, _params: [])

        with caplog.at_level(logging.INFO):
            client.fetch_table(table='stocks', tickers=tickers)

        progress_lines = [record for record in caplog.records if 'fetched' in record.getMessage()]
        assert batch_count > 400
        assert 1 < len(progress_lines) <= SharadarClient.PROGRESS_LOG_LINES + 1
        assert f'batch {batch_count} of {batch_count} fetched' in progress_lines[-1].getMessage()

    def test_a_failed_batch_names_the_table_and_its_tickers(self, client, monkeypatch):
        tickers = [f'KX{index:03d}' for index in range(65)]

        def fake_request_records(_table, params):
            if 'KX030' in params['ticker']:

                raise DataProviderServerError(None, 'unreachable')

            return []

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        with pytest.raises(DataProviderServerError) as raised:
            client.fetch_table(table='stocks', tickers=tickers)

        message = str(raised.value)
        assert 'stocks' in message
        assert 'batch 2 of 3' in message
        assert 'KX030' in message
        assert 'unreachable' in message

    def test_a_throttled_batch_is_still_reported_as_throttling(self, client, monkeypatch):
        def fake_request_records(_table, _params):

            raise DataProviderRateLimitError(429, 'Rate limited')

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        with pytest.raises(DataProviderRateLimitError) as raised:
            client.fetch_table(table='stocks', tickers=['AAPL'])

        message = str(raised.value)
        assert 'for tickers AAPL' in message
        assert 'batch' not in message     # a single batch has no number worth reporting
        assert message.count('Data provider error') == 1


class TestDropDuplicateKeys:
    """Rows a request returns more than once, which paging by date can do at a page boundary, are kept once."""

    def test_duplicates_are_found_across_null_key_columns(self):
        # most corporate actions have no counterparty, so their contraticker and contraname keys are null
        action = {
            'date': '2024-03-01',
            'action': 'dividend',
            'ticker': EQUITY_TICKER,
            'name': 'Example Inc',
            'value': 0.25,
            'contraticker': None,
            'contraname': None,
        }
        split = {**action, 'action': 'split', 'value': 4.0}
        table = SharadarClient.records_to_table('actions', [action, split, action])

        deduplicated = SharadarClient.drop_duplicate_keys(table, ('date', 'action', 'ticker', 'name',
                                                                  'contraticker', 'contraname'))

        assert deduplicated.column('action').to_pylist() == ['dividend', 'split']

    def test_a_table_without_duplicates_is_returned_as_is(self):
        table = SharadarClient.records_to_table('stocks', STOCKS_API_RECORDS)

        assert SharadarClient.drop_duplicate_keys(table, ('ticker', 'date')) is table


class FakeResponse:
    """Minimal stand-in for the context-managed object `urlopen` returns."""

    def __init__(self, body):
        self._buffer = io.BytesIO(body)

    def __enter__(self):

        return self

    def __exit__(self, *_arguments):

        return False

    def read(self, *arguments):

        return self._buffer.read(*arguments)


class FailingReadResponse(FakeResponse):
    """Stand-in for a response whose connection the server drops while its body is being read."""

    def __init__(self, read_error):
        super().__init__(b'')
        self._read_error = read_error

    def read(self, *_arguments):

        raise self._read_error


class TestProviderRouting:
    """
    Which table holds an identifier's data is stated by Sharadar's tickers table, not guessed from the ticker.
    """

    def test_an_equity_is_routed_to_the_stocks_table(self, initialized_provider):
        assert initialized_provider._market_data_endpoint(EQUITY_TICKER) is Sharadar.Endpoints.STOCKS

    def test_a_fund_is_routed_to_the_funds_table(self, initialized_provider):
        assert initialized_provider._market_data_endpoint(FUND_TICKER) is Sharadar.Endpoints.FUNDS

    def test_an_identifier_with_no_price_table_is_reported_as_not_found(self, initialized_provider):
        with pytest.raises(IdentifierNotFoundError):
            initialized_provider._market_data_endpoint('NOSUCH')


def fake_universe_table(table, *, identifiers, extra_params=None, filter_dates=True):
    """
    Stand in for `Sharadar._request_table` over a universe of made-up equities.

    Every identifier gets the recorded equity's rows under its own ticker, so the rows returned for a batch are
    exactly the rows of the identifiers asked for.
    """
    recorded = {
        'fundamentals': FUNDAMENTALS_RECORDS,
        'stocks': STOCKS_RECORDS,
        'tickers': TICKERS_RECORDS,
    }[table]
    template_ticker = STOCKS_RECORDS[0]['ticker']

    return SharadarClient.records_to_table(
        table,
        [
            {**record, 'ticker': identifier}
            for identifier in identifiers
            for record in recorded
            if record['ticker'] == template_ticker
        ],
    )


class TestProviderBatchLoading:
    """
    The run's tables are loaded one batch of identifiers at a time, holding a single batch per table.

    The full history of the stocks table runs to 45 million rows, several gigabytes, so holding it whole for the
    length of a run would make the memory of a run grow with its universe.
    """

    IDENTIFIERS = tuple(f'KX{index:03d}' for index in range(65))

    @pytest.fixture
    def universe_provider(self, provider, monkeypatch):
        requested = []

        def recording_request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            requested.append((table, tuple(identifiers)))

            return fake_universe_table(table, identifiers=identifiers)

        monkeypatch.setattr(provider, '_request_table', recording_request_table)
        provider.initialize(configuration=make_configuration(self.IDENTIFIERS))
        provider.requested = requested

        return provider

    def test_each_batch_is_fetched_once_when_identifiers_come_in_order(self, universe_provider):
        for identifier in self.IDENTIFIERS:
            rows = universe_provider._endpoint_rows(Sharadar.Endpoints.STOCKS, identifier)

            assert set(rows.column('ticker').to_pylist()) == {identifier}
            assert rows.num_rows == len(STOCKS_RECORDS)

        stock_requests = [tickers for (table, tickers) in universe_provider.requested if table == 'stocks']
        assert stock_requests == SharadarClient.batch_tickers(self.IDENTIFIERS)

    def test_only_the_current_batch_is_held(self, universe_provider):
        data_block_data(universe_provider, MarketDailyDataBlock, self.IDENTIFIERS[0])
        first_batch = universe_provider._loaded_batches['stocks']

        data_block_data(universe_provider, MarketDailyDataBlock, self.IDENTIFIERS[-1])
        last_batch = universe_provider._loaded_batches['stocks']

        assert first_batch.number == 1
        assert last_batch.number == len(SharadarClient.batch_tickers(self.IDENTIFIERS))
        # the rows held are only those of the batch the last identifier belongs to
        assert set(last_batch.rows.column('ticker').to_pylist()) == set(self.IDENTIFIERS[60:])

    def test_identifiers_asked_out_of_order_still_get_their_own_rows(self, universe_provider):
        for identifier in (self.IDENTIFIERS[0], self.IDENTIFIERS[-1], self.IDENTIFIERS[1]):
            market_data = data_block_data(universe_provider, MarketDailyDataBlock, identifier)

            assert market_data.main_identifier.identifier == identifier
            assert len(market_data.rows) == len(STOCKS_RECORDS)

        # going back to the first batch fetches it again, rather than serving the wrong rows
        stock_requests = [tickers for (table, tickers) in universe_provider.requested if table == 'stocks']
        assert len(stock_requests) == 3

    def test_each_table_holds_its_own_batch(self, universe_provider):
        data_block_data(universe_provider, MarketDailyDataBlock, self.IDENTIFIERS[0])
        data_block_data(universe_provider, FundamentalsDataBlock, self.IDENTIFIERS[-1])

        assert universe_provider._loaded_batches['stocks'].number == 1
        assert universe_provider._loaded_batches['fundamentals'].number == 3

    def test_an_identifier_outside_the_run_requests_nothing(self, universe_provider):
        requests_before = len(universe_provider.requested)

        with pytest.raises(IdentifierNotFoundError):
            data_block_data(universe_provider, MarketDailyDataBlock, 'NOSUCH')

        assert len(universe_provider.requested) == requests_before

    def test_the_metadata_covers_the_run_and_the_prices_only_the_batch_asked_for(self, provider, monkeypatch):
        identifiers = (EQUITY_TICKER, *(f'KX{index:04d}' for index in range(500)))
        requested = []

        def fake_fetch_table(*, table, tickers, start_date=None, end_date=None, extra_params=None):
            requested.append((table, tuple(tickers)))

            return fixture_table(table, dimension=(extra_params or {}).get('dimension'))

        monkeypatch.setattr(provider.client, 'fetch_table', fake_fetch_table)
        provider.initialize(configuration=make_configuration(identifiers))
        market_data = data_block_data(provider, MarketDailyDataBlock, EQUITY_TICKER)

        assert len(market_data.rows) == len(STOCKS_RECORDS)
        # the routing metadata is loaded for the whole run, and the prices only for the batch asked about
        assert requested == [('tickers', identifiers), ('stocks', SharadarClient.batch_tickers(identifiers)[0])]


class TestProviderUndownloadableBatches:
    """
    A batch that stays undownloadable after all its retries costs its own identifiers, not the whole run.

    Its identifiers are reported as not found, which the orchestrator skips over and carries on, and every one
    skipped so far is listed so they can be run again. Errors that would fail every batch alike, such as a
    rejected key, still stop the run, while a table the subscription doesn't cover costs only its identifiers.
    """

    IDENTIFIERS = tuple(f'KX{index:03d}' for index in range(65))

    def make_provider(self, provider, monkeypatch, failing_batch_error):
        """A provider whose second stocks batch fails with the given error, and every other batch works."""
        failing_batch = SharadarClient.batch_tickers(self.IDENTIFIERS)[1]
        requested = []

        def request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            requested.append((table, tuple(identifiers)))
            if table == 'stocks' and tuple(identifiers) == failing_batch:

                raise failing_batch_error

            return fake_universe_table(table, identifiers=identifiers)

        monkeypatch.setattr(provider, '_request_table', request_table)
        provider.initialize(configuration=make_configuration(self.IDENTIFIERS))
        provider.requested = requested

        return (provider, failing_batch)

    def test_the_identifiers_of_a_failed_batch_are_skipped_and_the_rest_carry_on(self, provider, monkeypatch):
        (provider, failing_batch) = self.make_provider(
            provider, monkeypatch, DataProviderServerError(None, 'unreachable'),
        )
        served = []
        skipped = []

        for identifier in self.IDENTIFIERS:
            try:
                rows = provider._endpoint_rows(Sharadar.Endpoints.STOCKS, identifier)
            except IdentifierNotFoundError:
                skipped.append(identifier)
            else:
                assert rows.num_rows == len(STOCKS_RECORDS)
                served.append(identifier)

        assert tuple(skipped) == failing_batch
        assert len(served) == len(self.IDENTIFIERS) - len(failing_batch)

    def test_a_failed_batch_is_not_retried_for_each_of_its_identifiers(self, provider, monkeypatch):
        (provider, failing_batch) = self.make_provider(
            provider, monkeypatch, DataProviderServerError(None, 'unreachable'),
        )

        for identifier in failing_batch:
            with pytest.raises(IdentifierNotFoundError):
                provider._endpoint_rows(Sharadar.Endpoints.STOCKS, identifier)

        # each retry of a batch can take minutes, so its identifiers share the one failure
        assert provider.requested.count(('stocks', failing_batch)) == 1

    def test_the_skipped_identifiers_are_named_so_they_can_be_run_again(self, provider, monkeypatch, caplog):
        (provider, failing_batch) = self.make_provider(
            provider, monkeypatch, DataProviderRateLimitError(429, 'Rate limited'),
        )

        with caplog.at_level(logging.ERROR), pytest.raises(IdentifierNotFoundError) as raised:
            provider._endpoint_rows(Sharadar.Endpoints.STOCKS, failing_batch[0])

        assert 'Rate limited' in str(raised.value)
        assert 'batch 2 of 3' in caplog.text
        assert 'to run again' in caplog.text
        assert all(identifier in caplog.text for identifier in failing_batch)

    def test_the_skipped_identifiers_reach_the_orchestrator_as_not_found(self, provider, monkeypatch):
        (provider, failing_batch) = self.make_provider(
            provider, monkeypatch, DataProviderServerError(None, 'unreachable'),
        )

        with pytest.raises(IdentifierNotFoundError, match='could not be loaded'):
            data_block_data(provider, MarketDailyDataBlock, failing_batch[0])

    def test_a_table_the_subscription_does_not_cover_skips_only_its_identifiers(self, provider, monkeypatch):
        """A 403 is about the plan, like funds on a stocks-only one, so the run carries on without that batch."""
        (provider, failing_batch) = self.make_provider(
            provider, monkeypatch, DataProviderAuthorizationError(403, 'Exceeds your subscription'),
        )

        with pytest.raises(IdentifierNotFoundError, match='no access to Sharadar table stocks'):
            provider._endpoint_rows(Sharadar.Endpoints.STOCKS, failing_batch[0])

        assert provider._endpoint_rows(Sharadar.Endpoints.STOCKS, self.IDENTIFIERS[-1]).num_rows > 0

    def test_a_rejected_key_still_stops_the_run(self, provider, monkeypatch):
        (provider, failing_batch) = self.make_provider(
            provider, monkeypatch, DataProviderAuthenticationError(401, 'Unauthorized'),
        )

        with pytest.raises(DataProviderAuthenticationError):
            provider._endpoint_rows(Sharadar.Endpoints.STOCKS, failing_batch[0])



class TestProviderDataBlockDispatch:
    def test_every_data_block_it_maps_tags_for_can_be_asked_for(self, initialized_provider):
        """
        The blocks the provider maps endpoint tags for are the blocks it serves.

        Those two lists are declared separately -- one in `get_data_block_endpoint_tag_map`, the other in the
        dispatch of `get_data_block_data` -- so this is what keeps a block from being announced and then not
        served, or the other way round.
        """
        for data_block in Sharadar.get_data_block_endpoint_tag_map():
            entity = data_block_data(initialized_provider, data_block, EQUITY_TICKER)

            assert entity is not None

    def test_a_data_block_it_does_not_serve_is_refused_by_name(self, initialized_provider):
        class SentimentDataBlock:
            pass

        with pytest.raises(InjectedDependencyError) as exception_info:
            data_block_data(initialized_provider, SentimentDataBlock, EQUITY_TICKER)

        assert 'SentimentDataBlock' in str(exception_info.value)
        assert 'MarketDailyDataBlock' in str(exception_info.value)


class TestProviderMarketData:
    def test_equity_prices_are_assembled_into_market_data(self, initialized_provider):
        market_data = data_block_data(initialized_provider, MarketDailyDataBlock, EQUITY_TICKER)
        source = ADJUSTMENT_DIVERGENT_RECORD
        row = market_data.rows[source['date']]

        assert len(market_data.rows) == len(STOCKS_RECORDS)
        assert market_data.main_identifier.identifier == EQUITY_TICKER
        # the three closes are three different numbers on this session, so a swap between the adjustment
        # bases would show up here rather than passing on values that happen to coincide
        assert source['close'] != source['closeunadj'] != source['closeadj']
        assert float(row.close) == source['closeunadj']
        assert float(row.close_split_adjusted) == source['close']
        assert float(row.close_dividend_and_split_adjusted) == source['closeadj']
        assert float(row.open_split_adjusted) == source['open']
        assert float(row.high_split_adjusted) == source['high']
        assert float(row.low_split_adjusted) == source['low']
        assert row.volume_split_adjusted == int(source['volume'])

    def test_fund_prices_are_assembled_the_same_way(self, initialized_provider):
        market_data = data_block_data(initialized_provider, MarketDailyDataBlock, FUND_TICKER)

        assert len(market_data.rows) == len(FUNDS_RECORDS)
        assert market_data.main_identifier.identifier == FUND_TICKER

    def test_the_columns_sharadar_does_not_publish_are_left_empty(self, initialized_provider):
        market_data = data_block_data(initialized_provider, MarketDailyDataBlock, EQUITY_TICKER)
        row = market_data.rows[STOCKS_RECORDS[0]['date']]

        assert row.vwap is None
        assert row.open is None                             # only the close is published unadjusted
        assert row.open_dividend_and_split_adjusted is None

    def test_rows_come_out_in_ascending_date_order(self, initialized_provider):
        market_data = data_block_data(initialized_provider, MarketDailyDataBlock, EQUITY_TICKER)
        dates = list(market_data.rows.keys())

        assert dates == sorted(dates)

    def test_an_unknown_identifier_is_reported_as_not_found(self, initialized_provider):
        with pytest.raises(IdentifierNotFoundError):
            data_block_data(initialized_provider, MarketDailyDataBlock, 'NOSUCH')


class TestProviderFundamentalData:
    def test_filings_are_assembled_into_fundamental_data(self, initialized_provider):
        fundamental_data = data_block_data(initialized_provider, FundamentalsDataBlock, EQUITY_TICKER, 'quarterly')

        assert len(fundamental_data.rows) > 0
        row = next(iter(fundamental_data.rows.values()))
        assert row.reported_currency == 'USD'   # joined in from the tickers table
        assert row.fiscal_period in ('Q1', 'Q2', 'Q3', 'Q4')
        assert 1900 < row.fiscal_year < 2200
        assert row.filing_date >= row.period_end_date
        assert row.income_statement.revenues is not None
        assert row.balance_sheet.assets is not None
        assert row.cash_flow.net_cash_from_operating_activities is not None

    def test_the_reported_currency_is_the_one_the_filings_report_in(self, provider, monkeypatch):
        """
        An ADR's tickers rows disagree: its stocks row says the dollars it trades in, its fundamentals row the
        currency it reports in, like Toyota's yen, which is the one its fundamentals are in.
        """
        adr_tickers_records = [
            {**record, 'currency': 'JPY' if record['table'] == 'fundamentals' else 'USD'}
            for record in TICKERS_RECORDS
        ]

        def request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            if table == 'tickers':

                return SharadarClient.records_to_table(table, adr_tickers_records)

            return fixture_table(table, dimension=(extra_params or {}).get('dimension'))

        monkeypatch.setattr(provider, '_request_table', request_table)
        provider.initialize(configuration=make_configuration())

        fundamental_data = data_block_data(provider, FundamentalsDataBlock, EQUITY_TICKER, 'quarterly')
        currencies = {row.reported_currency for row in fundamental_data.rows.values()}

        assert currencies == {'JPY'}

    def test_the_annual_period_asks_for_the_as_reported_annual_dimension(self, provider, monkeypatch):
        requested = {}

        def fake_request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            requested[table] = extra_params

            return fixture_table(table)

        monkeypatch.setattr(provider, '_request_table', fake_request_table)
        provider.initialize(configuration=make_configuration(period='annual'))
        provider._batch_rows('fundamentals', 1)

        assert requested['fundamentals'] == {'dimension': 'ARY'}

    def test_a_period_filed_twice_keeps_only_its_first_filing(self, provider, monkeypatch, caplog):
        """
        Some filers publish a quarter twice with the same figures, Toyota's 6-K and 20-F for instance.

        Point in time, the period became known on its first filing, so the later one is left out and reported.
        """
        as_reported_records = [
            record
            for record in FUNDAMENTALS_RECORDS
            if record['dimension'] == 'ARQ'
        ]
        latest_record = max(as_reported_records, key=lambda record: record['date'])
        refiling_date = (
            datetime.date.fromisoformat(latest_record['date']) + datetime.timedelta(days=20)
        ).isoformat()
        refiled_record = {**latest_record, 'date': refiling_date}

        def request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            if table != 'fundamentals':

                return fixture_table(table)

            return SharadarClient.records_to_table(table, [*as_reported_records, refiled_record])

        monkeypatch.setattr(provider, '_request_table', request_table)
        provider.initialize(configuration=make_configuration())

        with caplog.at_level(logging.WARNING):
            fundamental_data = data_block_data(provider, FundamentalsDataBlock, EQUITY_TICKER, 'quarterly')

        filing_dates = {row.filing_date.isoformat() for row in fundamental_data.rows.values()}
        assert latest_record['date'] in filing_dates
        assert refiling_date not in filing_dates
        assert 'irregular' in caplog.text
        assert refiling_date in caplog.text

    def test_the_fundamentals_are_read_from_before_the_start_date(self, provider, monkeypatch):
        """
        The filing in effect on the first day of a run was filed before it, so the whole history is asked for.
        """
        requested_start_dates = {}

        def fake_fetch_table(*, table, tickers, start_date=None, end_date=None, extra_params=None):
            requested_start_dates[table] = start_date

            return fixture_table(table, dimension=(extra_params or {}).get('dimension'))

        monkeypatch.setattr(provider.client, 'fetch_table', fake_fetch_table)
        provider.initialize(configuration=make_configuration())
        provider._batch_rows('fundamentals', 1)
        provider._batch_rows('stocks', 1)

        # explicit, since a multi-ticker query without a start date only returns Sharadar's last two years
        assert requested_start_dates['fundamentals'] == Sharadar.FUNDAMENTALS_HISTORY_START_DATE.isoformat()
        assert requested_start_dates['stocks'] == START_DATE.isoformat()

    def test_a_filing_from_before_the_start_date_is_kept(self, provider, monkeypatch):
        as_reported_records = [
            record
            for record in FUNDAMENTALS_RECORDS
            if record['dimension'] == 'ARQ'
        ]
        earliest_record = min(as_reported_records, key=lambda record: record['date'])
        earlier_filing_date = (START_DATE - datetime.timedelta(days=40)).isoformat()
        earlier_record = {
            **earliest_record,
            'date': earlier_filing_date,
            'reportperiod': (START_DATE - datetime.timedelta(days=90)).isoformat(),
        }

        def request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            if table != 'fundamentals':

                return fixture_table(table)

            return SharadarClient.records_to_table(table, [earlier_record, *as_reported_records])

        monkeypatch.setattr(provider, '_request_table', request_table)
        provider.initialize(configuration=make_configuration())

        fundamental_data = data_block_data(provider, FundamentalsDataBlock, EQUITY_TICKER, 'quarterly')
        filing_dates = {row.filing_date.isoformat() for row in fundamental_data.rows.values()}

        assert earlier_filing_date in filing_dates

    def test_a_fund_has_no_fundamentals_and_gets_an_empty_entity(self, initialized_provider, caplog):
        with caplog.at_level(logging.WARNING):
            fundamental_data = data_block_data(initialized_provider, FundamentalsDataBlock, FUND_TICKER, 'quarterly')

        assert len(fundamental_data.rows) == 0
        assert 'no fundamental data' in caplog.text
        assert 'ETF' in caplog.text

    def test_an_unknown_identifier_gets_an_empty_entity(self, initialized_provider):
        fundamental_data = data_block_data(initialized_provider, FundamentalsDataBlock, 'NOSUCH', 'quarterly')

        assert len(fundamental_data.rows) == 0


class TestProviderDividendAndSplitData:
    """The recorded equity has a 10 for 1 split on 2024-06-10, with dividends on both sides of it."""

    SPLIT_DATE = '2024-06-10'

    def test_dividends_are_read_off_the_actions_table(self, initialized_provider):
        dividend_data = data_block_data(initialized_provider, DividendsDataBlock, EQUITY_TICKER)
        dividend_rows = [record for record in ACTIONS_RECORDS if record['action'] == 'dividend']

        assert len(dividend_data.rows) == len(dividend_rows)
        row = next(iter(dividend_data.rows.values()))
        # Sharadar publishes no declaration, record or payment date
        assert row.declaration_date is None
        assert row.payment_date is None

    def test_the_split_adjusted_dividend_is_the_one_sharadar_publishes(self, initialized_provider):
        """Before the 10 for 1 split Sharadar publishes 0.005 for a dividend paid as 0.05."""
        dividend_data = data_block_data(initialized_provider, DividendsDataBlock, EQUITY_TICKER)
        row = dividend_data.rows['2024-03-05']

        assert row.dividend_split_adjusted == decimal.Decimal('0.005')

    def test_the_paid_dividend_is_left_empty(self, initialized_provider):
        """
        Sharadar doesn't publish it, and the provider passes on what the provider publishes; deriving it is
        left to custom calculations.
        """
        dividend_data = data_block_data(initialized_provider, DividendsDataBlock, EQUITY_TICKER)

        assert all(row.dividend is None for row in dividend_data.rows.values())

    def test_splits_are_read_off_the_same_table(self, initialized_provider):
        split_data = data_block_data(initialized_provider, SplitsDataBlock, EQUITY_TICKER)
        split_rows = [record for record in ACTIONS_RECORDS if record['action'] == 'split']

        assert len(split_data.rows) == len(split_rows)
        row = split_data.rows[self.SPLIT_DATE]
        assert row.numerator / row.denominator == float(split_rows[0]['value'])

    def test_dividends_and_splits_do_not_leak_into_each_other(self, initialized_provider):
        dividend_data = data_block_data(initialized_provider, DividendsDataBlock, EQUITY_TICKER)
        split_data = data_block_data(initialized_provider, SplitsDataBlock, EQUITY_TICKER)

        assert len(dividend_data.rows) > len(split_data.rows) > 0
        assert set(dividend_data.rows).isdisjoint(set(split_data.rows))


class TestProviderApiKeyValidation:
    """
    The query API cannot tell a valid key from an invalid one, so validation asks the whole-table export URL.

    Sharadar serves a free tier to anyone: a request carrying a wrong key, or none at all, comes back as a
    plain 200 holding either data for the well known tickers or an empty result set for anything the free
    tier does not reach. The export URL is the one that authenticates, so that is what gets asked, and its
    redirect is left unfollowed because only the status code matters.
    """

    def test_a_rejected_key_is_invalid(self, provider, monkeypatch):
        def fake_open(_url, timeout=None):

            raise make_http_error(401, {'error': 'Unauthorized'})

        monkeypatch.setattr(
            'urllib.request.build_opener',
            lambda *_handlers: SimpleNamespace(open=fake_open),
        )

        assert provider.validate_api_key() is False

    def test_a_key_the_export_url_redirects_is_valid(self, provider, monkeypatch):
        def fake_open(_url, timeout=None):

            raise make_http_error(302, headers={'Location': 'https://downloads.example.invalid/actions.zip'})

        monkeypatch.setattr(
            'urllib.request.build_opener',
            lambda *_handlers: SimpleNamespace(open=fake_open),
        )

        assert provider.validate_api_key() is True

    def test_a_status_about_the_subscription_rather_than_the_key_is_still_valid(self, provider, monkeypatch):
        # a plan without export access is a real key all the same
        def fake_open(_url, timeout=None):

            raise make_http_error(403, {'error': 'Not included in your plan'})

        monkeypatch.setattr(
            'urllib.request.build_opener',
            lambda *_handlers: SimpleNamespace(open=fake_open),
        )

        assert provider.validate_api_key() is True

    def test_an_unreachable_provider_is_reported_rather_than_called_invalid(self, provider, monkeypatch):
        def fake_open(_url, timeout=None):
            msg = 'unreachable'

            raise urllib.error.URLError(msg)

        monkeypatch.setattr(
            'urllib.request.build_opener',
            lambda *_handlers: SimpleNamespace(open=fake_open),
        )

        with pytest.raises(DataProviderServerError):
            provider.validate_api_key()

    def test_an_empty_key_is_rejected_on_construction(self):
        with pytest.raises(DataProviderMissingKeyError):
            Sharadar(api_key='')


class TestSplitFractions:
    """
    Sharadar publishes a split as a single rounded ratio, so the fraction behind it is recovered from it.
    """

    @pytest.mark.parametrize(
        ('ratio', 'expected_numerator', 'expected_denominator'),
        [
            (10.0, 10, 1),        # NVDA, ten for one
            (1.5, 3, 2),          # NVDA, three for two
            (0.125, 1, 8),        # GE, one for eight
            (0.05, 1, 20),        # AIG, one for twenty
            (1.33333, 4, 3),      # Citigroup, four for three, sent rounded
            (0.005, 1, 200),      # a one for two hundred reverse split
        ],
    )
    def test_the_fraction_behind_a_ratio_is_recovered(self, ratio, expected_numerator, expected_denominator):
        numerators = _split_numerator(DataColumn.load([ratio])).to_pyarrow().to_pylist()
        denominators = _split_denominator(DataColumn.load([ratio])).to_pyarrow().to_pylist()

        assert (numerators[0], denominators[0]) == (expected_numerator, expected_denominator)

    def test_a_ratio_too_small_for_a_fraction_keeps_the_ratio_it_came_with(self):
        """
        A one for a hundred thousand reverse split has no fraction of this size, and must not come back as
        zero: that is a numerator no price adjustment survives.
        """
        ratio = 1e-05

        numerators = _split_numerator(DataColumn.load([ratio])).to_pyarrow().to_pylist()
        denominators = _split_denominator(DataColumn.load([ratio])).to_pyarrow().to_pylist()

        assert (numerators[0], denominators[0]) == (ratio, 1)

    def test_a_missing_ratio_stays_missing(self):
        ratios = DataColumn.load([None], dtype=pyarrow.float64())

        numerators = _split_numerator(ratios).to_pyarrow().to_pylist()
        denominators = _split_denominator(ratios).to_pyarrow().to_pylist()

        assert numerators == [None]
        assert denominators == [None]

    def test_the_recovered_fraction_reproduces_the_ratio(self, initialized_provider):
        split_data = data_block_data(initialized_provider, SplitsDataBlock, EQUITY_TICKER)
        split_records = [record for record in ACTIONS_RECORDS if record['action'] == 'split']

        for (row, record) in zip(split_data.rows.values(), split_records, strict=True):
            assert float(row.numerator) / float(row.denominator) == pytest.approx(float(record['value']), rel=1e-4)


class TestProviderDimensionFilter:
    """
    Only the as-reported dimensions are point in time, so nothing else may reach the fundamentals block.

    The provider is asked for one dimension and normally answers with just that, but it has been seen
    answering with every dimension of the table, which would mix restated and trailing-twelve-month figures
    into an as-reported series.
    """

    def test_rows_of_other_dimensions_are_dropped(self, provider, monkeypatch, caplog):
        as_reported_records = [
            record
            for record in FUNDAMENTALS_RECORDS
            if record['dimension'] == 'ARQ'
        ]
        other_dimension_records = [
            {**record, 'dimension': 'MRQ'}
            for record in as_reported_records
        ]

        def request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            if table != 'fundamentals':

                return fixture_table(table)

            return SharadarClient.records_to_table(
                table,
                as_reported_records + other_dimension_records,
            )

        monkeypatch.setattr(provider, '_request_table', request_table)
        provider.initialize(configuration=make_configuration())

        with caplog.at_level(logging.WARNING):
            rows = provider._endpoint_rows(Sharadar.Endpoints.FUNDAMENTALS, EQUITY_TICKER)

        assert set(rows.column('dimension').to_pylist()) == {'ARQ'}
        assert 'other dimensions' in caplog.text


class TestProviderUnparsableValues:
    """
    A value of a type its column doesn't hold costs the identifiers holding it, not their batch or the run.

    PyArrow reads a page in a single pass, so one value like '17O.55' among numbers makes the whole page
    unreadable. The identifiers that sent such values are worked out, dropped, and the batch is requested again
    for the rest.
    """

    IDENTIFIERS = tuple(f'KX{index:03d}' for index in range(40))
    BROKEN_IDENTIFIER = 'KX007'

    @pytest.fixture
    def provider_with_a_broken_value(self, provider, monkeypatch):
        """A provider whose stocks page holds an unreadable close for one identifier of the first batch."""
        requested = []

        def request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            requested.append(tuple(identifiers))
            records = [
                {**record, 'ticker': identifier}
                for identifier in identifiers
                for record in (STOCKS_RECORDS if table == 'stocks' else TICKERS_RECORDS)
                if record['ticker'] == STOCKS_RECORDS[0]['ticker']
            ]
            if table == 'stocks' and self.BROKEN_IDENTIFIER in identifiers:
                # a letter O where a zero belongs, in a column every other row sends as a number
                broken_record = next(
                    record
                    for record in records
                    if record['ticker'] == self.BROKEN_IDENTIFIER
                )
                broken_record['close'] = '17O.55'

            return SharadarClient.records_to_table(table, records)

        monkeypatch.setattr(provider, '_request_table', request_table)
        provider.initialize(configuration=make_configuration(self.IDENTIFIERS))
        provider.requested = requested

        return provider

    def test_only_the_identifier_holding_the_value_is_skipped(self, provider_with_a_broken_value):
        served = []
        skipped = []

        for identifier in SharadarClient.batch_tickers(self.IDENTIFIERS)[0]:
            try:
                rows = provider_with_a_broken_value._endpoint_rows(Sharadar.Endpoints.STOCKS, identifier)
            except IdentifierNotFoundError:
                skipped.append(identifier)
            else:
                assert rows.num_rows == len(STOCKS_RECORDS)
                served.append(identifier)

        assert skipped == [self.BROKEN_IDENTIFIER]
        assert len(served) == len(SharadarClient.batch_tickers(self.IDENTIFIERS)[0]) - 1

    def test_the_batch_is_requested_again_without_it(self, provider_with_a_broken_value):
        provider_with_a_broken_value._endpoint_rows(Sharadar.Endpoints.STOCKS, self.IDENTIFIERS[0])

        stock_requests = [
            tickers
            for tickers in provider_with_a_broken_value.requested
            if self.IDENTIFIERS[0] in tickers
        ]
        assert self.BROKEN_IDENTIFIER in stock_requests[-2]
        assert self.BROKEN_IDENTIFIER not in stock_requests[-1]

    def test_the_offending_value_and_identifier_are_reported(self, provider_with_a_broken_value, caplog):
        with caplog.at_level(logging.ERROR):
            provider_with_a_broken_value._endpoint_rows(Sharadar.Endpoints.STOCKS, self.IDENTIFIERS[0])

        assert 'close' in caplog.text
        assert "17O.55" in caplog.text
        assert self.BROKEN_IDENTIFIER in caplog.text
        assert 'to run again' in caplog.text

    def test_the_skipped_identifier_reaches_the_orchestrator_as_not_found(self, provider_with_a_broken_value):
        with pytest.raises(IdentifierNotFoundError, match='could not be loaded'):
            data_block_data(provider_with_a_broken_value, MarketDailyDataBlock, self.BROKEN_IDENTIFIER)
