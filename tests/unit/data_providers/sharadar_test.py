import datetime
import email.message
import io
import json
import random
import urllib.error

import pyarrow
import pytest

from kaxanuk.data_curator.data_providers.sharadar import (
    Sharadar,
    SharadarClient,
    _calculate_split_fractions,
)
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import (
    DataProviderAuthenticationError,
    DataProviderAuthorizationError,
    DataProviderRateLimitError,
)
from kaxanuk.data_curator.modules.data_column import DataColumn


CONFIGURATION = Configuration(
    start_date=datetime.date(2022, 1, 1),
    end_date=datetime.date(2025, 12, 31),
    period='quarterly',
    identifiers=('KXNB',),
    columns=('m_close',),
)


class _ShufflingQueryApi:
    """
    Answers like Sharadar's query API: newest date first, with the rows of each date in a different order on
    every request, honouring ``limit`` and the ``date.lte`` the client pages with.
    """

    def __init__(self, records):
        self.records = records
        self.random = random.Random(0)    # noqa: S311  shuffles test rows

    def __call__(self, _table, params):
        latest_date = params.get('date.lte')
        dates = sorted(
            {record['date'] for record in self.records if latest_date is None or record['date'] <= latest_date},
            reverse=True,
        )
        ordered_records = []
        for date in dates:
            date_records = [record for record in self.records if record['date'] == date]
            self.random.shuffle(date_records)
            ordered_records.extend(date_records)

        return ordered_records[:int(params['limit'])]


class _DroppedConnectionResponse(io.BytesIO):
    """A response whose connection the server drops while its body is being read."""

    def read(self, *_arguments):
        raise ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host')


class TestFetchPages:
    def test_fetch_pages_returns_every_row_once_where_a_page_boundary_splits_a_date(self, monkeypatch) -> None:
        """Three rows share every date, so a page of seven rows ends inside a date on most pages."""
        records = [
            {'ticker': ticker, 'date': f'2024-03-{day:02d}'}
            for ticker in ('KXA', 'KXB', 'KXC')
            for day in range(1, 11)
        ]
        client = SharadarClient(api_key='test-api-key', page_size=7)
        monkeypatch.setattr(client, '_request_records', _ShufflingQueryApi(records))

        result = sorted(
            (record['ticker'], record['date'])
            for page in client.fetch_pages(table='stocks', tickers=['KXA', 'KXB', 'KXC'])
            for record in page
        )

        assert result == sorted((record['ticker'], record['date']) for record in records)


class TestPrivateRequestWithRetries:
    def test_a_connection_dropped_while_reading_is_retried(self, monkeypatch) -> None:
        responses = [
            _DroppedConnectionResponse(),
            io.BytesIO(json.dumps({'data': []}).encode('utf-8')),
        ]
        monkeypatch.setattr('urllib.request.urlopen', lambda *_arguments, **_options: responses.pop(0))
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        result = SharadarClient(api_key='test-api-key')._request_with_retries('stocks', 'https://example.test')

        assert result == '{"data": []}'

    def test_a_rejected_key_is_not_retried(self, monkeypatch) -> None:
        attempts = []
        rejection = urllib.error.HTTPError(
            'https://example.test',
            401,
            'Unauthorized',
            email.message.Message(),
            io.BytesIO(b'{}'),
        )

        def reject_key(*_arguments, **_options):
            attempts.append(1)

            raise rejection

        monkeypatch.setattr('urllib.request.urlopen', reject_key)
        monkeypatch.setattr('time.sleep', lambda _seconds: None)

        with pytest.raises(DataProviderAuthenticationError):
            SharadarClient(api_key='bad-key')._request_with_retries('stocks', 'https://example.test')

        assert len(attempts) == 1


class TestPrivateCalculateRetryWaitSeconds:
    def test_a_throttled_request_waits_for_the_announced_reset(self) -> None:
        error = DataProviderRateLimitError(429, 'Rate limited', retry_after=300.0)

        result = SharadarClient._calculate_retry_wait_seconds(1, error)

        assert 300.0 <= result <= 300.0 + SharadarClient.RATE_LIMIT_JITTER_SECONDS


class TestPrivateCalculateSplitFractions:
    @pytest.mark.parametrize(
        ('ratio', 'expected'),
        [
            (1.5, (3.0, 2.0)),
            (1.33333, (4.0, 3.0)),      # Sharadar rounds a four for three split
            (0.125, (1.0, 8.0)),
            (1e-05, (1e-05, 1.0)),      # too small for any fraction: kept as published rather than as 0/1
        ],
    )
    def test_calculate_split_fractions_recovers_the_fraction_behind_a_ratio(self, ratio, expected) -> None:
        result = _calculate_split_fractions(DataColumn.load([ratio]))

        assert result == [expected]


class TestPrivateFindMissingIncomeStatementRows:
    def test_only_a_filing_with_every_income_statement_value_empty_is_found(self) -> None:
        """CMFV's ARQ filings of 2000-01-21, 1999-10-05 and 2001-03-30: complete, empty, and partial."""
        rows = pyarrow.table({
            'FundamentalDataRowBalanceSheet.assets': [3535510, 3918885, 699923],
            'FundamentalDataRowIncomeStatement.revenues': [62000, None, 70478],
            'FundamentalDataRowIncomeStatement.net_income': [-568593, None, None],
        })

        result = Sharadar._find_missing_income_statement_rows(rows)

        assert result.to_pylist() == [False, True, False]


class TestPrivateFindRepeatedFiscalPeriodRows:
    def test_only_the_earlier_periods_of_a_repeated_fiscal_period_are_found(self) -> None:
        """ANLY's ARQ filings from 2001-03-31 to 2002-06-30, across its change of fiscal year end from June."""
        rows = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2001, 3, 31),
                datetime.date(2001, 6, 30),
                datetime.date(2001, 9, 30),
                datetime.date(2001, 12, 31),
                datetime.date(2002, 3, 31),
                datetime.date(2002, 6, 30),
            ],
            'FundamentalDataRow.fiscal_year': [2001, 2001, 2002, 2001, 2002, 2002],
            'FundamentalDataRow.fiscal_period': ['Q3', 'Q4', 'Q1', 'Q4', 'Q1', 'Q2'],
        })

        result = Sharadar._find_repeated_fiscal_period_rows(rows)

        assert result.to_pylist() == [False, True, True, False, False, False]


class TestPrivateSelectRequestedDimensionRows:
    def test_rows_of_another_dimension_are_dropped(self) -> None:
        rows = pyarrow.table({
            'ticker': ['KXNB', 'KXNB'],
            'dimension': ['ARQ', 'MRQ'],
        })

        result = Sharadar._select_requested_dimension_rows('fundamentals', rows, {'dimension': 'ARQ'})

        assert result.column('dimension').to_pylist() == ['ARQ']


class TestPrivateLoadTickerMetadata:
    def test_the_currency_is_the_one_the_filings_report_in(self, monkeypatch) -> None:
        """An ADR trades in dollars but reports in its home currency, which its fundamentals are in."""
        tickers_rows = pyarrow.table({
            'ticker': ['TM', 'TM'],
            'table': ['stocks', 'fundamentals'],
            'currency': ['USD', 'JPY'],
            'category': ['ADR Common Stock', 'ADR Common Stock'],
        })
        provider = Sharadar(api_key='test-api-key')
        provider._identifier_batches = [('TM',)]
        monkeypatch.setattr(provider, '_request_table', lambda *_arguments, **_options: tickers_rows)

        result = provider._load_ticker_metadata()

        assert result['TM']['currency'] == 'JPY'


class TestPrivateRequestTable:
    @pytest.mark.parametrize(
        ('table', 'expected_start_date'),
        [
            ('fundamentals', '1900-01-01'),     # the filing in effect on the first day was filed earlier
            ('stocks', '2022-01-01'),
        ],
    )
    def test_request_table_start_date(self, monkeypatch, table, expected_start_date) -> None:
        requested = {}

        def record_request(**request):
            requested.update(request)

            yield []

        provider = Sharadar(api_key='test-api-key')
        provider._configuration = CONFIGURATION
        monkeypatch.setattr(provider.client, 'fetch_pages', record_request)

        provider._request_table(table, identifiers=['KXNB'])

        assert requested['start_date'] == expected_start_date


class TestPrivateLoadBatchRows:
    def test_a_table_the_plan_does_not_cover_skips_only_its_batch(self, monkeypatch) -> None:
        def deny_access(*_arguments, **_options):
            raise DataProviderAuthorizationError(403, 'Exceeds your subscription')

        provider = Sharadar(api_key='test-api-key')
        provider._configuration = CONFIGURATION
        provider._identifier_batches = [('KXA', 'KXB'), ('KXC',)]
        monkeypatch.setattr(provider, '_request_table', deny_access)

        provider._load_batch_rows('funds', 1)

        assert set(provider._unavailable_identifiers) == {'KXA', 'KXB'}
