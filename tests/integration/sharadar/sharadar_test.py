"""
Integration tests for the Sharadar data provider's transport and bulk acquisition paths.

These exercise the full processing pipeline of both paths against recorded Sharadar responses instead of the
live API: only the HTTP transport is replaced, while envelope unwrapping, pagination, ticker batching, error
translation, archive reading and typing all run through the production code.

Fixtures are pickled recordings, one instrument per table over the same window. Each is stored in the shape its own path
delivers: the endpoint ones as the JSON envelope the query API answers with, the bulk ones as a frame of the CSV
export's raw text.
"""

import datetime
import email.message
import io
import json
import logging
import pathlib
import urllib.error
import zipfile

from types import SimpleNamespace

import pandas
import pyarrow
import pyarrow.compute
import pyarrow.csv
import pytest

from kaxanuk.data_curator.data_providers.sharadar import (
    Sharadar,
    SharadarBulkDownloader,
    SharadarClient,
    TABLE_SCHEMAS,
)
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
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataProviderToolkit


FIXTURE_DIR = pathlib.Path(__file__).resolve().parent / 'fixtures'

# Fixture file names follow Sharadar's own documentation of each table, with the bulk export of a table
# recorded alongside it under the same name. Each one holds a single instrument over the same window.
FIXTURE_NAMES = {
    'actions': 'corporate_actions',
    'fundamentals': 'fundamentals',
    'funds': 'fund_prices',
    'stocks': 'stock_prices',
    'tickers': 'tickers_metadata',
}


def load_fixture(table, acquisition_path='endpoint'):
    """
    Load the recording of a Sharadar table taken through one of its two acquisition paths.

    Each recording keeps the shape its own path delivers. The endpoint ones are the JSON envelope the query
    API answers with -- a dict of a row count and a list of records -- which preserves every value as JSON
    carried it, integers included. The bulk ones are a frame of the CSV export's raw text. Feeding these
    through the production parsing is what makes the tests exercise the real degradation of each path.
    """
    suffix = '_bulk' if acquisition_path == 'bulk' else ''

    return pandas.read_pickle(     # noqa: S301  the fixtures are repository content
        FIXTURE_DIR / f'{FIXTURE_NAMES[table]}{suffix}.pkl'
    )


def as_api_records(envelope):
    """Take the records out of a recorded response envelope."""

    return envelope['data']


def as_bulk_csv(frame):
    """Render a bulk fixture back into the CSV its export ships."""

    return frame.to_csv(index=False).encode('utf-8')


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

BULK_STOCKS_FRAME = load_fixture('stocks', 'bulk')
BULK_FUNDAMENTALS_FRAME = load_fixture('fundamentals', 'bulk')
BULK_TICKERS_FRAME = load_fixture('tickers', 'bulk')

# Each fixture records one instrument, so its identity is read off the recording itself
EQUITY_TICKER = STOCKS_RECORDS[0]['ticker']
FUND_TICKER = FUNDS_RECORDS[0]['ticker']

STOCKS_API_RECORDS = as_api_records(STOCKS_ENVELOPE)
FUNDAMENTALS_API_RECORDS = as_api_records(FUNDAMENTALS_ENVELOPE)
STOCK_CSV = as_bulk_csv(BULK_STOCKS_FRAME)

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
BULK_STOCK_ROWS = len(BULK_STOCKS_FRAME)
LATEST_DATE = datetime.date.fromisoformat(max(BULK_STOCKS_FRAME['date']))


START_DATE = datetime.date(2022, 1, 1)
END_DATE = datetime.date(2025, 12, 31)


def fixture_table(table, acquisition_path='endpoint', dimension=None):
    """
    Build the typed table a fixture's acquisition path would produce, through the production parsing.

    The endpoint path goes through the response record casting and the bulk path through the archive reader,
    so a test that swaps the path exercises the same code the provider would run against the live API.
    """
    recorded = load_fixture(table, acquisition_path)
    if acquisition_path == 'bulk':
        parsed = SharadarBulkDownloader.read_archive(table, make_zip_archive(as_bulk_csv(recorded)))
    else:
        parsed = SharadarClient.records_to_table(table, as_api_records(recorded))

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


def make_zip_archive(csv_bytes, name='SHARADAR_SEP.csv'):
    """Build an in-memory zip archive holding a single CSV, like the bulk downloads do."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr(name, csv_bytes)

    return buffer.getvalue()


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
    """The JSON API encodes 64-bit integers as strings, so every response is cast to the published schema."""

    def test_casts_string_encoded_integers(self):
        table = SharadarClient.records_to_table('fundamentals', FUNDAMENTALS_API_RECORDS)

        # the API renders every bigint column as a JSON string, which is what has to be undone
        assert isinstance(FUNDAMENTAL_RECORD['assets'], str)
        assert table.column('assets').type == pyarrow.int64()
        assert table.column('assets')[0].as_py() == int(FUNDAMENTAL_RECORD['assets'])

    def test_casts_dates(self):
        table = SharadarClient.records_to_table('stocks', [STOCK_RECORD])

        # the API sends the date as an ISO string
        assert isinstance(STOCK_RECORD['date'], str)
        assert table.column('date').type == pyarrow.date32()
        assert table.column('date')[0].as_py() == datetime.date.fromisoformat(STOCK_RECORD['date'])

    def test_fills_absent_columns_with_nulls(self):
        table = SharadarClient.records_to_table('fundamentals', [{'ticker': EQUITY_TICKER}])

        assert table.num_columns == len(TABLE_SCHEMAS['fundamentals'])
        assert table.column('revenue')[0].as_py() is None
        assert table.column('revenue').type == pyarrow.int64()

    def test_drops_columns_the_schema_does_not_declare(self):
        table = SharadarClient.records_to_table('stocks', [{**STOCK_RECORD, 'unexpected': 1}])

        assert 'unexpected' not in table.column_names

    def test_empty_records_produce_an_empty_typed_table(self):
        table = SharadarClient.records_to_table('stocks', [])

        assert table.num_rows == 0
        assert table.schema.equals(TABLE_SCHEMAS['stocks'])

    def test_uncastable_value_raises_a_parsing_error(self):
        with pytest.raises(DataProviderParsingError):
            SharadarClient.records_to_table('stocks', [{**STOCK_RECORD, 'volume': 'not a number'}])


class TestPagination:
    """`count` reports the rows of the current page, so a full page is the only signal that more rows remain."""

    def test_pages_until_a_page_comes_back_short(self, client, monkeypatch):
        client.page_size = 2
        requested_params = []

        def fake_request_records(_table, params):
            requested_params.append(params)
            page_index = int(params.get('skip', 0))
            rows = [
                {**STOCK_RECORD, 'date': f'2024-03-{index + 1:02d}'}
                for index in range(page_index, page_index + 2)
            ]

            return rows if page_index < 4 else rows[:1]

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        assert [params.get('skip') for params in requested_params] == [None, '2', '4']
        assert table.num_rows == 5

    def test_a_short_first_page_issues_a_single_request(self, client, monkeypatch):
        calls = []

        def fake_request_records(_table, params):
            calls.append(params)

            return [STOCK_RECORD]

        monkeypatch.setattr(client, '_request_records', fake_request_records)

        client.fetch_table(table='stocks', tickers=['AAPL'])

        assert len(calls) == 1

    def test_rows_repeated_across_pages_are_deduplicated_by_primary_key(self, client, monkeypatch):
        client.page_size = 2
        pages = [
            [STOCK_RECORD, {**STOCK_RECORD, 'date': '2024-03-01'}],
            [{**STOCK_RECORD, 'date': '2024-03-01'}],   # overlaps the previous page
        ]

        monkeypatch.setattr(
            client,
            '_request_records',
            lambda _table, params: pages[0] if 'skip' not in params else pages[1],
        )

        table = client.fetch_table(table='stocks', tickers=['AAPL'])

        # three records came back across the two pages, and the repeated primary key leaves two
        assert table.num_rows == 2
        assert sorted(table.column('date').to_pylist()) == sorted({
            datetime.date.fromisoformat(record['date'])
            for page in pages
            for record in page
        })


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

        assert captured['from'] == '2020-01-01'
        assert captured['to'] == '2024-12-31'
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
        assert waits == [3]     # the wait honours the provider's Retry-After header

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

        assert timeouts == [SharadarClient.REQUEST_TIMEOUT_SECONDS]
        assert all(timeout is not None and timeout > 0 for timeout in timeouts)

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


class TestBulkArchiveReading:
    def test_reads_a_zipped_csv_with_the_published_types(self):
        table = SharadarBulkDownloader.read_archive('stocks', make_zip_archive(STOCK_CSV))

        assert table.num_rows == BULK_STOCK_ROWS
        assert table.column('date').type == pyarrow.date32()
        assert table.column('close').type == pyarrow.float64()

    def test_an_archive_without_a_csv_raises_a_parsing_error(self):
        with pytest.raises(DataProviderParsingError):
            SharadarBulkDownloader.read_archive('stocks', make_zip_archive(b'x', name='readme.txt'))

    def test_a_corrupt_archive_raises_a_parsing_error(self):
        with pytest.raises(DataProviderParsingError):
            SharadarBulkDownloader.read_archive('stocks', b'not a zip file')


class TestBulkFiltering:
    @pytest.fixture
    def bulk_table(self):

        return SharadarBulkDownloader.read_archive('stocks', make_zip_archive(STOCK_CSV))

    def test_filters_by_ticker(self, bulk_table):
        assert SharadarBulkDownloader.filter_table(
            bulk_table,
            tickers=[EQUITY_TICKER],
        ).num_rows == BULK_STOCK_ROWS
        assert SharadarBulkDownloader.filter_table(bulk_table, tickers=['NOSUCH']).num_rows == 0

    def test_filters_by_date_range(self, bulk_table):
        table = SharadarBulkDownloader.filter_table(
            bulk_table,
            start_date=LATEST_DATE,
            end_date=LATEST_DATE,
        )

        assert set(table.column('date').to_pylist()) == {LATEST_DATE}

    def test_without_filters_the_table_is_returned_unchanged(self, bulk_table):
        assert SharadarBulkDownloader.filter_table(bulk_table).num_rows == bulk_table.num_rows


class TestBulkHistorySelection:
    @pytest.mark.parametrize(
        ('start_year', 'expected_history'),
        [
            (2024, '5'),
            (2019, '10'),
            (2005, 'full'),
        ],
    )
    def test_picks_the_smallest_export_covering_the_start_date(self, start_year, expected_history):
        history = SharadarBulkDownloader.select_history(
            datetime.date(start_year, 1, 1),
            'stocks',
            today=datetime.date(2026, 8, 12),
        )

        assert history == expected_history

    def test_tables_published_only_as_full_history_always_use_it(self):
        history = SharadarBulkDownloader.select_history(
            datetime.date(2026, 1, 1),
            'tickers',
            today=datetime.date(2026, 8, 12),
        )

        assert history == 'full'


class TestBulkCache:
    @pytest.fixture
    def downloader(self, tmp_path):

        return SharadarBulkDownloader(api_key='test-api-key', cache_dir=tmp_path)

    def test_a_cold_cache_downloads_and_a_warm_one_does_not(self, downloader, monkeypatch):
        downloads = []

        def fake_download(_table, _history):
            downloads.append(1)

            return make_zip_archive(STOCK_CSV)

        monkeypatch.setattr(downloader, '_download_archive', fake_download)

        first = downloader.load_table(table='stocks', history='5')
        second = downloader.load_table(table='stocks', history='5')

        assert len(downloads) == 1
        assert first.num_rows == second.num_rows == BULK_STOCK_ROWS

    def test_a_stale_cache_is_downloaded_again(self, downloader, monkeypatch):
        downloads = []
        monkeypatch.setattr(
            downloader,
            '_download_archive',
            lambda _table, _history: (downloads.append(1), make_zip_archive(STOCK_CSV))[1],
        )

        downloader.load_table(table='stocks', history='5')
        downloader.cache_max_age_hours = 0
        downloader.load_table(table='stocks', history='5')

        assert len(downloads) == 2

    def test_the_cache_reads_back_only_the_requested_rows(self, downloader, monkeypatch):
        monkeypatch.setattr(downloader, '_download_archive', lambda _t, _h: make_zip_archive(STOCK_CSV))

        table = downloader.load_table(
            table='stocks',
            history='5',
            tickers=[EQUITY_TICKER],
            start_date=LATEST_DATE,
        )

        assert set(table.column('ticker').to_pylist()) == {EQUITY_TICKER}
        assert set(table.column('date').to_pylist()) == {LATEST_DATE}

    def test_an_interrupted_download_leaves_no_cache_entry(self, downloader, monkeypatch):
        def failing_download(_table, _history):

            raise DataProviderServerError(None, 'download failed')

        monkeypatch.setattr(downloader, '_download_archive', failing_download)

        with pytest.raises(DataProviderServerError):
            downloader.load_table(table='stocks', history='5')

        assert not downloader.get_cached_parquet_path('stocks', '5').exists()

    def test_the_download_carries_a_timeout(self, downloader, monkeypatch):
        """Bounds a stalled download without capping how long a legitimately large export may take."""
        timeouts = []

        def fake_urlopen(_request, context=None, timeout=None):
            timeouts.append(timeout)

            return FakeResponse(make_zip_archive(STOCK_CSV))

        monkeypatch.setattr('urllib.request.urlopen', fake_urlopen)

        downloader._download_archive('stocks', '5')

        assert timeouts == [SharadarBulkDownloader.DOWNLOAD_TIMEOUT_SECONDS]

    def test_unknown_tables_and_histories_are_rejected(self, downloader):
        with pytest.raises(DataProviderFatalError):
            downloader._download_archive('not_a_sharadar_table', '5')

        with pytest.raises(DataProviderFatalError):
            downloader._download_archive('stocks', '7')


class TestBulkNamingDifferences:
    """
    The bulk exports are generated from Sharadar's own datasets and carry conventions the query API does not.

    Both are recorded in the fixtures exactly as each path serves them, because a fixture derived from the
    other path would hide precisely the differences these tests exist to pin down.
    """

    def test_the_bulk_fundamentals_filing_date_column_is_renamed(self):
        # the export names the filing date `datekey`, and it is the fundamentals clock sync field
        assert 'datekey' in BULK_FUNDAMENTALS_FRAME.columns
        assert 'date' not in BULK_FUNDAMENTALS_FRAME.columns
        assert 'date' in FUNDAMENTALS_RECORDS[0]

        table = SharadarBulkDownloader.read_archive(
            'fundamentals',
            make_zip_archive(as_bulk_csv(BULK_FUNDAMENTALS_FRAME)),
        )

        assert 'date' in table.column_names
        assert 'datekey' not in table.column_names
        assert table.column('date').type == pyarrow.date32()

    def test_the_bulk_tickers_dataset_codes_are_translated(self):
        # the export identifies the table a ticker belongs to by dataset code, not by the name the API returns
        assert set(BULK_TICKERS_FRAME['table']).isdisjoint({record['table'] for record in TICKERS_RECORDS})

        table = SharadarBulkDownloader.read_archive(
            'tickers',
            make_zip_archive(as_bulk_csv(BULK_TICKERS_FRAME)),
        )

        assert set(table.column('table').to_pylist()) == {record['table'] for record in TICKERS_RECORDS}

    def test_an_unknown_dataset_code_is_left_untouched(self):
        frame = BULK_TICKERS_FRAME.copy()
        frame.loc[frame.index[0], 'table'] = 'SF9'

        table = SharadarBulkDownloader.read_archive(
            'tickers',
            make_zip_archive(as_bulk_csv(frame)),
        )

        assert 'SF9' in table.column('table').to_pylist()


class TestAcquisitionPathEquivalence:
    """
    Both acquisition paths feed the same downstream pipeline, so they must produce identical tables.

    The two sides of each comparison are independent recordings of the same rows, one taken from the query API
    and one from the bulk export, so this is a real comparison rather than two renderings of a single source.
    """

    def test_the_api_and_bulk_paths_produce_identical_stock_tables(self):
        sort_order = [('ticker', 'ascending'), ('date', 'ascending')]
        api_table = SharadarClient.records_to_table('stocks', STOCKS_API_RECORDS)
        bulk_table = SharadarBulkDownloader.read_archive('stocks', make_zip_archive(STOCK_CSV))

        assert api_table.schema.equals(bulk_table.schema)
        assert api_table.num_rows == bulk_table.num_rows
        # the export is not published in date order, so both sides are ordered before comparing
        assert api_table.sort_by(sort_order).equals(bulk_table.sort_by(sort_order))

    def test_the_api_and_bulk_paths_produce_identical_fundamental_tables(self):
        api_table = SharadarClient.records_to_table('fundamentals', FUNDAMENTALS_API_RECORDS)
        bulk_table = SharadarBulkDownloader.read_archive(
            'fundamentals',
            make_zip_archive(as_bulk_csv(BULK_FUNDAMENTALS_FRAME)),
        )
        # the export holds every dimension, while the provider only ever queries one
        bulk_table = bulk_table.filter(
            pyarrow.compute.equal(bulk_table.column('dimension'), 'ARQ')
        )

        assert api_table.schema.equals(bulk_table.schema)
        assert api_table.num_rows == bulk_table.num_rows
        assert api_table.sort_by([('ticker', 'ascending'), ('date', 'ascending')]).equals(
            bulk_table.sort_by([('ticker', 'ascending'), ('date', 'ascending')])
        )


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

    def test_the_run_uses_the_query_api_below_the_bulk_threshold(self, initialized_provider):
        assert initialized_provider._use_bulk is False

    def test_the_run_switches_to_the_bulk_exports_above_the_threshold(self, provider, monkeypatch):
        def fake_request_table(table, *, identifiers, extra_params=None, filter_dates=True):

            return fixture_table(table)

        monkeypatch.setattr(provider, '_request_table', fake_request_table)

        provider.initialize(
            configuration=make_configuration(
                tuple(f'KX{index:04d}' for index in range(Sharadar.BULK_TICKER_THRESHOLD))
            )
        )

        assert provider._use_bulk is True


class TestProviderMarketData:
    def test_equity_prices_are_assembled_into_market_data(self, initialized_provider):
        market_data = initialized_provider.get_market_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        source = ADJUSTMENT_DIVERGENT_RECORD
        row = market_data.daily_rows[source['date']]

        assert len(market_data.daily_rows) == len(STOCKS_RECORDS)
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
        market_data = initialized_provider.get_market_data(
            main_identifier=FUND_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(market_data.daily_rows) == len(FUNDS_RECORDS)
        assert market_data.main_identifier.identifier == FUND_TICKER

    def test_the_columns_sharadar_does_not_publish_are_left_empty(self, initialized_provider):
        market_data = initialized_provider.get_market_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        row = market_data.daily_rows[STOCKS_RECORDS[0]['date']]

        assert row.vwap is None
        assert row.open is None                             # only the close is published unadjusted
        assert row.open_dividend_and_split_adjusted is None

    def test_rows_come_out_in_ascending_date_order(self, initialized_provider):
        market_data = initialized_provider.get_market_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        dates = list(market_data.daily_rows.keys())

        assert dates == sorted(dates)

    def test_an_unknown_identifier_is_reported_as_not_found(self, initialized_provider):
        with pytest.raises(IdentifierNotFoundError):
            initialized_provider.get_market_data(
                main_identifier='NOSUCH',
                start_date=START_DATE,
                end_date=END_DATE,
            )


class TestProviderFundamentalData:
    def test_filings_are_assembled_into_fundamental_data(self, initialized_provider):
        fundamental_data = initialized_provider.get_fundamental_data(
            main_identifier=EQUITY_TICKER,
            period='quarterly',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(fundamental_data.rows) > 0
        row = next(iter(fundamental_data.rows.values()))
        assert row.reported_currency == 'USD'   # joined in from the tickers table
        assert row.fiscal_period in ('Q1', 'Q2', 'Q3', 'Q4')
        assert 1900 < row.fiscal_year < 2200
        assert row.filing_date >= row.period_end_date
        assert row.income_statement.revenues is not None
        assert row.balance_sheet.assets is not None
        assert row.cash_flow.net_cash_from_operating_activities is not None

    def test_the_annual_period_asks_for_the_as_reported_annual_dimension(self, provider, monkeypatch):
        requested = {}

        def fake_request_table(table, *, identifiers, extra_params=None, filter_dates=True):
            requested[table] = extra_params

            return fixture_table(table)

        monkeypatch.setattr(provider, '_request_table', fake_request_table)
        provider.initialize(configuration=make_configuration(period='annual'))
        provider._get_table('fundamentals')

        assert requested['fundamentals'] == {'dimension': 'ARY'}

    def test_a_fund_has_no_fundamentals_and_gets_an_empty_entity(self, initialized_provider, caplog):
        with caplog.at_level(logging.WARNING):
            fundamental_data = initialized_provider.get_fundamental_data(
                main_identifier=FUND_TICKER,
                period='quarterly',
                start_date=START_DATE,
                end_date=END_DATE,
            )

        assert len(fundamental_data.rows) == 0
        assert 'no fundamental data' in caplog.text
        assert 'ETF' in caplog.text

    def test_an_unknown_identifier_gets_an_empty_entity(self, initialized_provider):
        fundamental_data = initialized_provider.get_fundamental_data(
            main_identifier='NOSUCH',
            period='quarterly',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(fundamental_data.rows) == 0


class TestProviderDividendAndSplitData:
    def test_dividends_are_read_off_the_actions_table(self, initialized_provider):
        dividend_data = initialized_provider.get_dividend_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        dividend_rows = [record for record in ACTIONS_RECORDS if record['action'] == 'dividend']

        assert len(dividend_data.rows) == len(dividend_rows)
        row = next(iter(dividend_data.rows.values()))
        assert row.dividend > 0
        # Sharadar publishes no declaration, record or payment date
        assert row.declaration_date is None
        assert row.payment_date is None

    def test_splits_are_read_off_the_same_table(self, initialized_provider):
        split_data = initialized_provider.get_split_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        split_rows = [record for record in ACTIONS_RECORDS if record['action'] == 'split']

        assert len(split_data.rows) == len(split_rows)
        row = next(iter(split_data.rows.values()))
        # Sharadar publishes the split as a single ratio, so the denominator is always one
        assert row.numerator == float(split_rows[0]['value'])
        assert row.denominator == 1.0

    def test_dividends_and_splits_do_not_leak_into_each_other(self, initialized_provider):
        dividend_data = initialized_provider.get_dividend_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        split_data = initialized_provider.get_split_data(
            main_identifier=EQUITY_TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(dividend_data.rows) > len(split_data.rows) > 0
        assert set(dividend_data.rows).isdisjoint(set(split_data.rows))


class TestProviderApiKeyValidation:
    """
    The query API cannot tell a valid key from an invalid one, so validation goes through the bulk endpoint.

    Sharadar serves a free tier to anyone: a request carrying a wrong key, or none at all, comes back as a
    plain 200 holding either data for the well known tickers or an empty result set for anything the free
    tier does not reach. The bulk endpoint is the one that authenticates, so that is what gets asked, and its
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

    def test_a_key_the_bulk_endpoint_redirects_is_valid(self, provider, monkeypatch):
        def fake_open(_url, timeout=None):

            raise make_http_error(302, headers={'Location': 'https://downloads.example.invalid/actions.zip'})

        monkeypatch.setattr(
            'urllib.request.build_opener',
            lambda *_handlers: SimpleNamespace(open=fake_open),
        )

        assert provider.validate_api_key() is True

    def test_a_status_about_the_subscription_rather_than_the_key_is_still_valid(self, provider, monkeypatch):
        # a plan without bulk access is a real key all the same
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


class TestProviderAcquisitionPaths:
    """Both acquisition paths have to produce the same entities, since they feed the same pipeline."""

    def test_the_bulk_path_produces_the_same_market_data_as_the_query_api(self, monkeypatch):
        entities = {}
        for acquisition_path in ('endpoint', 'bulk'):
            provider = Sharadar(api_key='test-api-key')
            monkeypatch.setattr(
                provider,
                '_request_table',
                lambda table, *, identifiers, extra_params=None, filter_dates=True, path=acquisition_path:
                    fixture_table(table, path),
            )
            provider.initialize(configuration=make_configuration())
            entities[acquisition_path] = provider.get_market_data(
                main_identifier=EQUITY_TICKER,
                start_date=START_DATE,
                end_date=END_DATE,
            )

        assert entities['endpoint'].daily_rows.keys() == entities['bulk'].daily_rows.keys()
        assert entities['endpoint'] == entities['bulk']

    def test_the_bulk_path_produces_the_same_fundamental_data_as_the_query_api(self, monkeypatch):
        entities = {}
        for acquisition_path in ('endpoint', 'bulk'):
            provider = Sharadar(api_key='test-api-key')
            monkeypatch.setattr(
                provider,
                '_request_table',
                lambda table, *, identifiers, extra_params=None, filter_dates=True, path=acquisition_path:
                    fixture_table(table, path, dimension=(extra_params or {}).get('dimension')),
            )
            provider.initialize(configuration=make_configuration())
            entities[acquisition_path] = provider.get_fundamental_data(
                main_identifier=EQUITY_TICKER,
                period='quarterly',
                start_date=START_DATE,
                end_date=END_DATE,
            )

        assert len(entities['endpoint'].rows) > 0
        assert entities['endpoint'] == entities['bulk']
