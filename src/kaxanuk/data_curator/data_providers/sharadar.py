"""
Sharadar data provider.

Sharadar publishes end of day data for US listed instruments. This
module reads five of its tables and maps them onto the library's four data types:

===================  ==========================================  ==========================
Sharadar table       Holds                                       Feeds
===================  ==========================================  ==========================
``stocks``           daily prices of ~22,000 common stocks       `MarketData`
``funds``            daily prices of ~10,000 ETFs, CEFs and ETNs `MarketData`
``fundamentals``     income, balance and cash flow of a filing   `FundamentalData`
``actions``          dividends and splits                        `DividendData`, `SplitData`
``tickers``          reference metadata for every instrument     routing and reporting currency
===================  ==========================================  ==========================

Three behaviours of the provider shape how the module is built:

* The ``ticker`` parameter accepts at most 30 tickers and 200 characters per request, which puts a run
  covering thousands of identifiers out of reach of per-request acquisition.
* JSON responses encode 64-bit integer columns as strings and the bulk exports are CSV, so neither path yields
  usable types on its own. Both are cast to `TABLE_SCHEMAS`, taken from the SQL DDL Sharadar publishes at its
  ``/v1.0/schema/`` endpoint, which is what makes the two produce identical tables.
"""

import datetime
import enum
import http
import io
import json
import logging
import os
import pathlib
import ssl
import time
import typing
import urllib.error
import urllib.parse
import urllib.request
import zipfile

import pyarrow
import pyarrow.compute
import pyarrow.csv
import pyarrow.parquet

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    DividendDataRow,
    FundamentalData,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    MarketData,
    MarketDataDailyRow,
    MarketInstrumentIdentifier,
    SplitData,
    SplitDataRow,
)
from kaxanuk.data_curator.exceptions import (
    DataProviderApiError,
    DataProviderAuthenticationError,
    DataProviderAuthorizationError,
    DataProviderFatalError,
    DataProviderMissingKeyError,
    DataProviderParsingError,
    DataProviderRateLimitError,
    DataProviderServerError,
    DataProviderToolkitNoDataError,
    IdentifierNotFoundError,
)
from kaxanuk.data_curator.modules.data_column import DataColumn
from kaxanuk.data_curator.services.data_provider_toolkit import (
    DataBlockEndpointTagMap,
    DataProviderToolkit,
    EndpointFieldMap,
    PreprocessedFieldMapping,
)


# The column types below are the ones Sharadar itself declares for each of its tables. It publishes them as SQL
# DDL, with no API key required, at https://api.sharadar.com/v1.0/schema/{table}?format=postgres, where `bigint`
# maps to int64, `double precision` to float64, `date` to date32 and `text` to string. Update them from that
# endpoint whenever Sharadar publishes a schema change; SCHEMA_VERSION tracks which publication they came from.
SCHEMA_VERSION = '2026-08-02'


TABLE_SCHEMAS: dict[str, pyarrow.Schema] = {
    'stocks': pyarrow.schema([
        ('ticker', pyarrow.string()),
        ('date', pyarrow.date32()),
        ('open', pyarrow.float64()),
        ('high', pyarrow.float64()),
        ('low', pyarrow.float64()),
        ('close', pyarrow.float64()),
        ('volume', pyarrow.float64()),
        ('closeadj', pyarrow.float64()),
        ('closeunadj', pyarrow.float64()),
        ('lastupdated', pyarrow.date32()),
    ]),
    'funds': pyarrow.schema([
        ('ticker', pyarrow.string()),
        ('date', pyarrow.date32()),
        ('open', pyarrow.float64()),
        ('high', pyarrow.float64()),
        ('low', pyarrow.float64()),
        ('close', pyarrow.float64()),
        ('volume', pyarrow.float64()),
        ('closeadj', pyarrow.float64()),
        ('closeunadj', pyarrow.float64()),
        ('lastupdated', pyarrow.date32()),
    ]),
    'fundamentals': pyarrow.schema([
        ('ticker', pyarrow.string()),
        ('dimension', pyarrow.string()),
        ('calendardate', pyarrow.date32()),
        ('date', pyarrow.date32()),
        ('reportperiod', pyarrow.date32()),
        ('fiscalperiod', pyarrow.string()),
        ('lastupdated', pyarrow.date32()),
        ('accoci', pyarrow.int64()),
        ('assets', pyarrow.int64()),
        ('assetsavg', pyarrow.int64()),
        ('assetsc', pyarrow.int64()),
        ('assetsnc', pyarrow.int64()),
        ('assetturnover', pyarrow.float64()),
        ('bvps', pyarrow.float64()),
        ('capex', pyarrow.int64()),
        ('cashneq', pyarrow.int64()),
        ('cashnequsd', pyarrow.int64()),
        ('cor', pyarrow.int64()),
        ('consolinc', pyarrow.int64()),
        ('currentratio', pyarrow.float64()),
        ('de', pyarrow.float64()),
        ('debt', pyarrow.int64()),
        ('debtc', pyarrow.int64()),
        ('debtnc', pyarrow.int64()),
        ('debtusd', pyarrow.int64()),
        ('deferredrev', pyarrow.int64()),
        ('depamor', pyarrow.int64()),
        ('deposits', pyarrow.int64()),
        ('divyield', pyarrow.float64()),
        ('dps', pyarrow.float64()),
        ('ebit', pyarrow.int64()),
        ('ebitda', pyarrow.int64()),
        ('ebitdamargin', pyarrow.float64()),
        ('ebitdausd', pyarrow.int64()),
        ('ebitusd', pyarrow.int64()),
        ('ebt', pyarrow.int64()),
        ('eps', pyarrow.float64()),
        ('epsdil', pyarrow.float64()),
        ('epsusd', pyarrow.float64()),
        ('equity', pyarrow.int64()),
        ('equityavg', pyarrow.int64()),
        ('equityusd', pyarrow.int64()),
        ('ev', pyarrow.int64()),
        ('evebit', pyarrow.int64()),
        ('evebitda', pyarrow.float64()),
        ('fcf', pyarrow.int64()),
        ('fcfps', pyarrow.float64()),
        ('fxusd', pyarrow.float64()),
        ('gp', pyarrow.int64()),
        ('grossmargin', pyarrow.float64()),
        ('intangibles', pyarrow.int64()),
        ('intexp', pyarrow.int64()),
        ('invcap', pyarrow.int64()),
        ('invcapavg', pyarrow.int64()),
        ('inventory', pyarrow.int64()),
        ('investments', pyarrow.int64()),
        ('investmentsc', pyarrow.int64()),
        ('investmentsnc', pyarrow.int64()),
        ('liabilities', pyarrow.int64()),
        ('liabilitiesc', pyarrow.int64()),
        ('liabilitiesnc', pyarrow.int64()),
        ('marketcap', pyarrow.int64()),
        ('ncf', pyarrow.int64()),
        ('ncfbus', pyarrow.int64()),
        ('ncfcommon', pyarrow.int64()),
        ('ncfdebt', pyarrow.int64()),
        ('ncfdiv', pyarrow.int64()),
        ('ncff', pyarrow.int64()),
        ('ncfi', pyarrow.int64()),
        ('ncfinv', pyarrow.int64()),
        ('ncfo', pyarrow.int64()),
        ('ncfx', pyarrow.int64()),
        ('netinc', pyarrow.int64()),
        ('netinccmn', pyarrow.int64()),
        ('netinccmnusd', pyarrow.int64()),
        ('netincdis', pyarrow.int64()),
        ('netincnci', pyarrow.int64()),
        ('netmargin', pyarrow.float64()),
        ('opex', pyarrow.int64()),
        ('opinc', pyarrow.int64()),
        ('payables', pyarrow.int64()),
        ('payoutratio', pyarrow.float64()),
        ('pb', pyarrow.float64()),
        ('pe', pyarrow.float64()),
        ('pe1', pyarrow.float64()),
        ('ppnenet', pyarrow.int64()),
        ('prefdivis', pyarrow.int64()),
        ('price', pyarrow.float64()),
        ('ps', pyarrow.float64()),
        ('ps1', pyarrow.float64()),
        ('receivables', pyarrow.int64()),
        ('retearn', pyarrow.int64()),
        ('revenue', pyarrow.int64()),
        ('revenueusd', pyarrow.int64()),
        ('rnd', pyarrow.int64()),
        ('roa', pyarrow.float64()),
        ('roe', pyarrow.float64()),
        ('roic', pyarrow.float64()),
        ('ros', pyarrow.float64()),
        ('sbcomp', pyarrow.int64()),
        ('sgna', pyarrow.int64()),
        ('sharefactor', pyarrow.float64()),
        ('sharesbas', pyarrow.int64()),
        ('shareswa', pyarrow.int64()),
        ('shareswadil', pyarrow.int64()),
        ('sps', pyarrow.float64()),
        ('tangibles', pyarrow.int64()),
        ('taxassets', pyarrow.int64()),
        ('taxexp', pyarrow.int64()),
        ('taxliabilities', pyarrow.int64()),
        ('tbvps', pyarrow.float64()),
        ('workingcapital', pyarrow.int64()),
    ]),
    'actions': pyarrow.schema([
        ('date', pyarrow.date32()),
        ('action', pyarrow.string()),
        ('ticker', pyarrow.string()),
        ('name', pyarrow.string()),
        ('value', pyarrow.float64()),
        ('contraticker', pyarrow.string()),
        ('contraname', pyarrow.string()),
    ]),
    'tickers': pyarrow.schema([
        ('table', pyarrow.string()),
        ('permaticker', pyarrow.int64()),
        ('ticker', pyarrow.string()),
        ('name', pyarrow.string()),
        ('exchange', pyarrow.string()),
        ('isdelisted', pyarrow.string()),
        ('category', pyarrow.string()),
        ('cusips', pyarrow.string()),
        ('siccode', pyarrow.int64()),
        ('sicsector', pyarrow.string()),
        ('sicindustry', pyarrow.string()),
        ('figi', pyarrow.string()),
        ('famaindustry', pyarrow.string()),
        ('sector', pyarrow.string()),
        ('industry', pyarrow.string()),
        ('scalemarketcap', pyarrow.string()),
        ('scalerevenue', pyarrow.string()),
        ('relatedtickers', pyarrow.string()),
        ('currency', pyarrow.string()),
        ('location', pyarrow.string()),
        ('lastupdated', pyarrow.date32()),
        ('firstadded', pyarrow.date32()),
        ('firstpricedate', pyarrow.date32()),
        ('lastpricedate', pyarrow.date32()),
        ('firstquarter', pyarrow.string()),
        ('lastquarter', pyarrow.string()),
        ('secfilings', pyarrow.string()),
        ('companysite', pyarrow.string()),
    ]),
}


# The primary key columns of each table, as declared by Sharadar's DDL.
TABLE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    'stocks': ('ticker', 'date'),
    'funds': ('ticker', 'date'),
    'fundamentals': ('ticker', 'dimension', 'date', 'reportperiod'),
    'actions': ('date', 'action', 'ticker', 'name', 'contraticker', 'contraname'),
    'tickers': ('table', 'permaticker', 'ticker'),
}


type JsonRecord = dict[str, typing.Any]


class SharadarClient:
    """
    Client for the Sharadar query API.

    Handles ticker batching, pagination, retries and typing, and returns PyArrow tables that always conform to
    the schema published by Sharadar for the requested table.

    Parameters
    ----------
    api_key
        The Sharadar API key
    page_size
        The number of rows requested per page; also the threshold that decides whether another page is fetched
    """

    BASE_URL = 'https://api.sharadar.com/v1.0/data'

    # Hard limits enforced by the provider; exceeding either one is answered with an HTTP 400.
    MAX_TICKERS_PER_REQUEST = 30
    MAX_TICKER_PARAM_LENGTH = 200

    MAX_PAGE_SIZE = 10000
    MAX_PAGES_PER_REQUEST = 1000    # safety valve against a pagination loop that never shrinks

    MAX_CONNECTION_RETRIES = 5
    RETRY_BACKOFF_SECONDS = 0.5
    MAX_RETRY_WAIT_SECONDS = 60.0
    # Without an explicit timeout urlopen blocks forever, which would leave a stalled connection hanging the
    # whole run instead of surfacing as the retryable error the loop below is built to handle.
    REQUEST_TIMEOUT_SECONDS = 60.0

    _ssl_context = None

    def __init__(
        self,
        *,
        api_key: str,
        page_size: int = MAX_PAGE_SIZE,
    ):
        self.api_key = api_key
        self.page_size = min(page_size, self.MAX_PAGE_SIZE)

    @classmethod
    def batch_tickers(
        cls,
        tickers: typing.Sequence[str],
    ) -> list[tuple[str, ...]]:
        """
        Split a ticker list into batches the provider will accept.

        A batch is closed as soon as adding the next ticker would exceed either the maximum number of tickers or
        the maximum character length of the ``ticker`` query parameter.

        Parameters
        ----------
        tickers
            The tickers to split

        Returns
        -------
        The batches of tickers, in the order the tickers were given
        """
        batches: list[tuple[str, ...]] = []
        current_batch: list[str] = []
        current_length = 0

        for ticker in tickers:
            # every ticker after the first one also costs the separating comma
            added_length = len(ticker) + (1 if len(current_batch) > 0 else 0)
            if (
                len(current_batch) > 0
                and (
                    len(current_batch) >= cls.MAX_TICKERS_PER_REQUEST
                    or (current_length + added_length) > cls.MAX_TICKER_PARAM_LENGTH
                )
            ):
                batches.append(tuple(current_batch))
                current_batch = []
                current_length = 0
                added_length = len(ticker)

            current_batch.append(ticker)
            current_length += added_length

        if len(current_batch) > 0:
            batches.append(tuple(current_batch))

        return batches

    def fetch_table(
        self,
        *,
        table: str,
        tickers: typing.Sequence[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        extra_params: dict[str, str] | None = None,
    ) -> pyarrow.Table:
        """
        Fetch a full Sharadar table, batching the tickers and paginating as needed.

        Parameters
        ----------
        table
            The name of the Sharadar table
        tickers
            The tickers to fetch, or None to let the provider decide the scope of the query
        start_date
            The earliest date to fetch, in ``YYYY-MM-DD`` format
        end_date
            The latest date to fetch, in ``YYYY-MM-DD`` format
        extra_params
            Any additional query parameters, like the fundamentals ``dimension``

        Returns
        -------
        A table conforming to the schema Sharadar publishes for `table`

        Raises
        ------
        DataProviderFatalError
            When `table` is not one of the tables this client knows the schema of
        """
        if table not in TABLE_SCHEMAS:
            msg = f"Unknown Sharadar table '{table}'"

            raise DataProviderFatalError(None, msg)

        base_params: dict[str, str] = {
            'api_key': self.api_key,
            'format': 'json',
        }
        if start_date is not None:
            base_params['from'] = start_date
        if end_date is not None:
            base_params['to'] = end_date
        if extra_params is not None:
            base_params.update(extra_params)

        ticker_batches: list[tuple[str, ...] | None] = (
            list(self.batch_tickers(tickers))   # type: ignore[arg-type]
            if tickers is not None
            else [None]
        )

        # Pages of a multi-ticker query are not guaranteed to be disjoint, so rows are deduplicated by the
        # primary key Sharadar declares for the table.
        primary_key = TABLE_PRIMARY_KEYS[table]
        seen_keys: set[tuple[typing.Any, ...]] = set()
        records: list[JsonRecord] = []

        for ticker_batch in ticker_batches:
            batch_params = dict(base_params)
            if ticker_batch is not None:
                batch_params['ticker'] = ','.join(ticker_batch)

            for record in self._fetch_paginated(table, batch_params):
                key = tuple(
                    record.get(key_column)
                    for key_column in primary_key
                )
                if key in seen_keys:
                    continue

                seen_keys.add(key)
                records.append(record)

        return self.records_to_table(table, records)

    @classmethod
    def records_to_table(
        cls,
        table: str,
        records: typing.Sequence[JsonRecord],
    ) -> pyarrow.Table:
        """
        Build a schema-conforming PyArrow table out of raw JSON records.

        Columns absent from the records are filled with nulls and columns the schema does not declare are
        dropped, so that the result is directly comparable to a table read from a bulk CSV download.

        Parameters
        ----------
        table
            The name of the Sharadar table the records belong to
        records
            The raw records, as decoded from the response's ``data`` key

        Returns
        -------
        A table conforming to the schema Sharadar publishes for `table`

        Raises
        ------
        DataProviderParsingError
            When a column's values cannot be cast to the type Sharadar declares for it
        """
        schema = TABLE_SCHEMAS[table]
        if len(records) < 1:

            return schema.empty_table()

        raw_table = pyarrow.Table.from_pylist(list(records))
        raw_column_names = set(raw_table.column_names)
        columns = []
        for field in schema:
            if field.name not in raw_column_names:
                columns.append(
                    pyarrow.nulls(raw_table.num_rows, type=field.type)
                )

                continue

            raw_column = raw_table.column(field.name).combine_chunks()
            if raw_column.type == field.type:
                columns.append(raw_column)

                continue

            try:
                # Sharadar's JSON encodes 64-bit integers as strings, so string -> int64 casts are expected here
                columns.append(
                    pyarrow.compute.cast(raw_column, field.type)
                )
            except pyarrow.ArrowInvalid as error:
                msg = (
                    f"Could not cast Sharadar column '{table}.{field.name}'"
                    f" from {raw_column.type} to the published type {field.type}: {error}"
                )

                raise DataProviderParsingError(msg) from error

        return pyarrow.Table.from_arrays(columns, schema=schema)

    def _fetch_paginated(
        self,
        table: str,
        params: dict[str, str],
    ) -> list[JsonRecord]:
        """
        Fetch every page of a single query.

        Sharadar's ``count`` is the number of rows in the returned page, so the only reliable signal that more
        rows remain is a page that came back completely full.

        Parameters
        ----------
        table
            The name of the Sharadar table
        params
            The query parameters, without the pagination ones

        Returns
        -------
        The concatenated records of every page
        """
        records: list[JsonRecord] = []
        skip = 0

        for page_number in range(self.MAX_PAGES_PER_REQUEST):
            page_params = dict(params)
            page_params['limit'] = str(self.page_size)
            if skip > 0:
                page_params['skip'] = str(skip)

            page_records = self._request_records(table, page_params)
            records.extend(page_records)

            if len(page_records) < self.page_size:

                return records

            skip += len(page_records)

            if page_number == (self.MAX_PAGES_PER_REQUEST - 1):
                msg = " ".join([
                    f"Sharadar table {table} returned more than",
                    f"{self.MAX_PAGES_PER_REQUEST * self.page_size} rows for a single query;",
                    "truncating. Narrow the date range or use the bulk download.",
                ])
                logging.getLogger(__name__).warning(msg)

        return records

    def _request_records(
        self,
        table: str,
        params: dict[str, str],
    ) -> list[JsonRecord]:
        """
        Perform a single request and unwrap its response envelope.

        Parameters
        ----------
        table
            The name of the Sharadar table
        params
            The full query parameters, including the pagination ones

        Returns
        -------
        The records held in the response's ``data`` key

        Raises
        ------
        DataProviderParsingError
            When the response is not the JSON envelope the API is documented to return
        """
        url = self._build_url(table, params)
        raw_response = self._request_with_retries(table, url)

        try:
            response = json.loads(raw_response)
        except json.JSONDecodeError as error:
            msg = f"Sharadar table {table} returned a response that is not valid JSON: {error}"

            raise DataProviderParsingError(msg) from error

        if not isinstance(response, dict):
            msg = f"Sharadar table {table} returned a JSON response that is not an envelope object"

            raise DataProviderParsingError(msg)

        if 'error' in response:
            # the API normally signals errors through status codes, but a 200 body can still carry one
            self._raise_for_error_payload(table, None, response)

        data = response.get('data')
        if not isinstance(data, list):
            msg = f"Sharadar table {table} returned an envelope without a 'data' list"

            raise DataProviderParsingError(msg)

        return data

    def _request_with_retries(
        self,
        table: str,
        url: str,
    ) -> str:
        """
        Perform a request, retrying the errors the provider classifies as transient.

        Parameters
        ----------
        table
            The name of the Sharadar table, for error reporting
        url
            The fully assembled request URL

        Returns
        -------
        The raw response body

        Raises
        ------
        DataProviderRateLimitError
            When the provider keeps rate limiting the request after exhausting the retries
        DataProviderServerError
            When the request keeps failing for any other transient reason after exhausting the retries
        """
        last_error: Exception | None = None

        for attempt_number in range(1, self.MAX_CONNECTION_RETRIES + 1):
            try:
                request = urllib.request.Request(url)   # noqa: S310    URL is always built from BASE_URL
                with urllib.request.urlopen(    # noqa: S310
                    request,
                    context=self.load_ssl_context(),
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                ) as response:

                    return response.read().decode('utf-8')

            except urllib.error.HTTPError as error:
                try:
                    self.raise_for_http_error(table, error)     # non-retryable errors leave through here
                except DataProviderRateLimitError as rate_limit_error:
                    # rate limiting is transient: a run covering thousands of identifiers issues enough
                    # requests to hit it, and aborting the whole run over one of them would be wasteful
                    last_error = rate_limit_error
                else:
                    last_error = error
            except (urllib.error.URLError, urllib.error.ContentTooShortError, TimeoutError) as error:
                last_error = error

            if attempt_number < self.MAX_CONNECTION_RETRIES:
                wait_seconds = self._retry_wait_seconds(attempt_number, last_error)
                msg = (
                    f"Sharadar table {table} request failed ({last_error}),"
                    f" retrying in {wait_seconds:.1f}s (attempt {attempt_number}/{self.MAX_CONNECTION_RETRIES})"
                )
                logging.getLogger(__name__).warning(msg)
                time.sleep(wait_seconds)

        if isinstance(last_error, DataProviderRateLimitError):
            # surfaced as is, so the caller can tell throttling apart from an unreachable provider

            raise last_error

        msg = f"Sharadar table {table} request failed after {self.MAX_CONNECTION_RETRIES} attempts: {last_error}"

        raise DataProviderServerError(None, msg)

    @classmethod
    def _retry_wait_seconds(
        cls,
        attempt_number: int,
        error: Exception | None,
    ) -> float:
        """
        Return how long to wait before the next retry.

        Parameters
        ----------
        attempt_number
            The number of the attempt that just failed, starting at 1
        error
            The error that attempt raised

        Returns
        -------
        The number of seconds to wait
        """
        if (
            isinstance(error, DataProviderRateLimitError)
            and error.retry_after is not None
        ):

            return min(error.retry_after, cls.MAX_RETRY_WAIT_SECONDS)

        if (
            isinstance(error, urllib.error.HTTPError)
            and (retry_after := error.headers.get('Retry-After')) is not None
        ):
            try:

                return min(float(retry_after), cls.MAX_RETRY_WAIT_SECONDS)
            except ValueError:
                pass    # the header can also hold a date, which we don't bother parsing

        return min(
            cls.RETRY_BACKOFF_SECONDS * (2 ** (attempt_number - 1)),
            cls.MAX_RETRY_WAIT_SECONDS,
        )

    @classmethod
    def raise_for_http_error(
        cls,
        table: str,
        error: urllib.error.HTTPError,
    ) -> None:
        """
        Translate an HTTP error into the corresponding data provider exception, or return to let it be retried.

        Parameters
        ----------
        table
            The name of the Sharadar table, for error reporting
        error
            The raised HTTP error

        Raises
        ------
        DataProviderApiError
            One of its non-retryable subclasses, when the status code identifies the error as final
        """
        try:
            payload = json.loads(error.read().decode('utf-8'))
        except (ValueError, OSError):
            payload = {}

        if error.code == http.HTTPStatus.TOO_MANY_REQUESTS.value:
            # retryable, but signalled explicitly so callers can slow down instead of just retrying
            retry_after = error.headers.get('Retry-After')
            raise DataProviderRateLimitError(
                error.code,
                cls._error_message(table, payload, error),
                retry_after=float(retry_after) if retry_after is not None and retry_after.isdigit() else None,
            )

        if error.code >= http.HTTPStatus.INTERNAL_SERVER_ERROR.value:

            return  # transient, let the caller retry

        cls._raise_for_error_payload(table, error.code, payload, error)

    @classmethod
    def _raise_for_error_payload(
        cls,
        table: str,
        status_code: int | None,
        payload: dict[str, typing.Any],
        error: urllib.error.HTTPError | None = None,
    ) -> None:
        """
        Raise the exception corresponding to an error payload returned by the provider.

        Parameters
        ----------
        table
            The name of the Sharadar table, for error reporting
        status_code
            The HTTP status code, if the error was signalled by one
        payload
            The decoded JSON error body
        error
            The raised HTTP error, if any

        Raises
        ------
        DataProviderAuthenticationError
            When the API key is missing or rejected
        DataProviderAuthorizationError
            When the key is valid but does not grant access to the requested data
        DataProviderFatalError
            When the request itself is malformed, which always indicates a bug in this client
        """
        message = cls._error_message(table, payload, error)

        if status_code == http.HTTPStatus.UNAUTHORIZED.value:

            raise DataProviderAuthenticationError(status_code, message)

        if status_code == http.HTTPStatus.FORBIDDEN.value:

            raise DataProviderAuthorizationError(status_code, message)

        raise DataProviderFatalError(status_code, message)

    @staticmethod
    def _error_message(
        table: str,
        payload: dict[str, typing.Any],
        error: urllib.error.HTTPError | None = None,
    ) -> str:
        """
        Assemble a readable message out of a Sharadar error payload.

        Parameters
        ----------
        table
            The name of the Sharadar table
        payload
            The decoded JSON error body
        error
            The raised HTTP error, if any

        Returns
        -------
        The assembled message
        """
        details = " ".join(
            str(payload[key])
            for key in ('error', 'description')
            if key in payload
        ).strip()
        if len(details) < 1:
            details = str(error) if error is not None else "no details returned"

        return f"Sharadar table {table}: {details}"

    @classmethod
    def _build_url(
        cls,
        table: str,
        params: dict[str, str],
    ) -> str:
        """
        Assemble the URL of a table query.

        Parameters
        ----------
        table
            The name of the Sharadar table
        params
            The query parameters

        Returns
        -------
        The assembled URL
        """
        query = '&'.join(
            f'{key}={urllib.parse.quote(str(value), safe=",")}'
            for (key, value) in params.items()
        )

        return f'{cls.BASE_URL}/{urllib.parse.quote(table)}?{query}'

    @classmethod
    def load_ssl_context(cls) -> ssl.SSLContext:
        """
        Load the SSL context used for every request.

        Returns
        -------
        The loaded SSL context
        """
        if cls._ssl_context is None:
            cls._ssl_context = ssl.create_default_context()
            cls._ssl_context.check_hostname = True
            cls._ssl_context.verify_mode = ssl.CERT_REQUIRED

        return cls._ssl_context


type BulkHistory = typing.Literal['5', '10', 'full']


# How much history each `years` value covers. Ordered from cheapest to most expensive download.
BULK_HISTORY_YEARS: dict[BulkHistory, int | None] = {
    '5': 5,
    '10': 10,
    'full': None,   # everything Sharadar holds, back to 1998
}

# Tables Sharadar only publishes as a full history export.
FULL_HISTORY_ONLY_TABLES = frozenset({'tickers', 'descriptions'})

# The bulk exports are generated from Sharadar's own datasets, and carry two legacy conventions the query API
# does not: a different name for the fundamentals filing date column, and the dataset codes rather than the
# table names in the tickers export. Both are translated on read, so that a bulk loaded table is
# indistinguishable from the same rows fetched through the API.
BULK_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    'fundamentals': {
        'datekey': 'date',
    },
}
BULK_TICKER_TABLE_CODES: dict[str, str] = {
    'SEP': 'stocks',
    'SF1': 'fundamentals',
    'SF2': 'insiders',
    'SFP': 'funds',
}


class SharadarBulkDownloader:
    """
    Downloads, caches and reads Sharadar's whole-table bulk exports.

    Parameters
    ----------
    api_key
        The Sharadar API key
    cache_dir
        The directory the downloaded tables are cached in
    cache_max_age_hours
        How long a cached table is considered fresh; Sharadar republishes its tables twice a day
    """

    BASE_URL = SharadarClient.BASE_URL

    DEFAULT_CACHE_MAX_AGE_HOURS = 12.0
    DOWNLOAD_CHUNK_BYTES = 1 << 20
    MAX_CONNECTION_RETRIES = 3
    RETRY_BACKOFF_SECONDS = 2.0
    # Applies to each read rather than to the download as a whole, so it bounds a stalled connection without
    # putting a ceiling on how long a legitimately large export may take.
    DOWNLOAD_TIMEOUT_SECONDS = 120.0

    def __init__(
        self,
        *,
        api_key: str,
        cache_dir: pathlib.Path,
        cache_max_age_hours: float = DEFAULT_CACHE_MAX_AGE_HOURS,
    ):
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.cache_max_age_hours = cache_max_age_hours

    @staticmethod
    def select_history(
        start_date: datetime.date,
        table: str,
        *,
        today: datetime.date | None = None,
    ) -> BulkHistory:
        """
        Return the smallest bulk export that still covers a start date.

        Parameters
        ----------
        start_date
            The earliest date the run needs data for
        table
            The name of the Sharadar table
        today
            The date to measure the history against, defaulting to the current UTC date

        Returns
        -------
        The value to pass as the ``years`` query parameter
        """
        if table in FULL_HISTORY_ONLY_TABLES:

            return 'full'

        reference_date = (
            today
            if today is not None
            else datetime.datetime.now(tz=datetime.UTC).date()
        )
        elapsed_years = (reference_date - start_date).days / 365.25

        if elapsed_years <= 5:      # noqa: PLR2004  the bounds are the provider's own export sizes

            return '5'
        elif elapsed_years <= 10:   # noqa: PLR2004

            return '10'

        return 'full'

    def load_table(
        self,
        *,
        table: str,
        history: BulkHistory,
        columns: typing.Sequence[str] | None = None,
        tickers: typing.Collection[str] | None = None,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> pyarrow.Table:
        """
        Return a bulk exported table, downloading and converting it if the cache is cold or stale.

        Parameters
        ----------
        table
            The name of the Sharadar table
        history
            How much history to download
        columns
            The columns to read back, or None for all of them
        tickers
            The tickers to keep, or None to keep every one of them
        start_date
            The earliest date to keep, for tables that have a date column
        end_date
            The latest date to keep, for tables that have a date column

        Returns
        -------
        The filtered table, typed according to the schema Sharadar publishes for it
        """
        parquet_path = self.get_cached_parquet_path(table, history)
        if not self.is_cache_fresh(parquet_path):
            self.refresh_cache(table=table, history=history)

        loaded_table = pyarrow.parquet.read_table(
            parquet_path,
            columns=list(columns) if columns is not None else None,
        )

        return self.filter_table(
            loaded_table,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
        )

    def refresh_cache(
        self,
        *,
        table: str,
        history: BulkHistory,
    ) -> pathlib.Path:
        """
        Download a bulk export and store it in the cache as Parquet.

        Parameters
        ----------
        table
            The name of the Sharadar table
        history
            How much history to download

        Returns
        -------
        The path of the cached Parquet file
        """
        msg = f"Downloading Sharadar bulk export of table {table} ({history} years history)"
        logging.getLogger(__name__).info(msg)

        started_at = time.monotonic()
        archive_bytes = self._download_archive(table, history)
        csv_table = self.read_archive(table, archive_bytes)

        parquet_path = self.get_cached_parquet_path(table, history)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        # written to a temporary name first so an interrupted run never leaves a truncated cache entry behind
        temporary_path = parquet_path.with_suffix('.parquet.partial')
        pyarrow.parquet.write_table(csv_table, temporary_path)
        temporary_path.replace(parquet_path)

        msg = " ".join([
            f"Cached Sharadar table {table} ({csv_table.num_rows} rows,",
            f"{len(archive_bytes) / (1 << 20):.1f} MB downloaded)",
            f"in {time.monotonic() - started_at:.1f}s",
        ])
        logging.getLogger(__name__).info(msg)

        return parquet_path

    def get_cached_parquet_path(
        self,
        table: str,
        history: BulkHistory,
    ) -> pathlib.Path:
        """
        Return the cache path of a bulk exported table.

        Parameters
        ----------
        table
            The name of the Sharadar table
        history
            How much history the export covers

        Returns
        -------
        The path of the cached Parquet file, which may not exist yet
        """
        return self.cache_dir / f'{table}-{history}.parquet'

    def is_cache_fresh(
        self,
        parquet_path: pathlib.Path,
    ) -> bool:
        """
        Return whether a cached table is recent enough to be used as is.

        Parameters
        ----------
        parquet_path
            The path of the cached Parquet file

        Returns
        -------
        Whether the cached file exists and is younger than the configured maximum age
        """
        if not parquet_path.is_file():

            return False

        age_hours = (time.time() - parquet_path.stat().st_mtime) / 3600

        return age_hours < self.cache_max_age_hours

    @classmethod
    def read_archive(
        cls,
        table: str,
        archive_bytes: bytes,
    ) -> pyarrow.Table:
        """
        Read the CSV held in a downloaded zip archive into a typed table.

        The columns are parsed with the types Sharadar publishes for the table, which is what makes a bulk loaded
        table interchangeable with one assembled from the JSON query API.

        Parameters
        ----------
        table
            The name of the Sharadar table
        archive_bytes
            The raw bytes of the downloaded zip archive

        Returns
        -------
        The parsed table, typed according to the schema Sharadar publishes for it

        Raises
        ------
        DataProviderParsingError
            When the archive holds no CSV file, or the CSV cannot be parsed with the published schema
        """
        schema = TABLE_SCHEMAS[table]
        column_renames = BULK_COLUMN_RENAMES.get(table, {})
        # the published types are keyed by the query API's column names, so the legacy ones are added too
        column_types = {field.name: field.type for field in schema}
        column_types.update({
            bulk_name: column_types[api_name]
            for (bulk_name, api_name) in column_renames.items()
        })

        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                csv_names = [
                    name
                    for name in archive.namelist()
                    if name.lower().endswith('.csv')
                ]
                if len(csv_names) < 1:
                    msg = f"Sharadar bulk archive of table {table} holds no CSV file"

                    raise DataProviderParsingError(msg)

                with archive.open(csv_names[0]) as csv_file:
                    parsed_table = pyarrow.csv.read_csv(
                        csv_file,
                        convert_options=pyarrow.csv.ConvertOptions(
                            column_types=column_types,
                        ),
                    )
        except zipfile.BadZipFile as error:
            msg = f"Sharadar bulk download of table {table} is not a valid zip archive: {error}"

            raise DataProviderParsingError(msg) from error
        except pyarrow.ArrowInvalid as error:
            msg = f"Sharadar bulk CSV of table {table} does not match its published schema: {error}"

            raise DataProviderParsingError(msg) from error

        if column_renames:
            parsed_table = parsed_table.rename_columns([
                column_renames.get(column_name, column_name)
                for column_name in parsed_table.column_names
            ])

        if (
            table == 'tickers'
            and 'table' in parsed_table.column_names
        ):
            parsed_table = parsed_table.set_column(
                parsed_table.column_names.index('table'),
                'table',
                cls._translate_ticker_table_codes(parsed_table.column('table')),
            )

        # the CSV column order is not guaranteed, so the table is reordered to the published schema
        return parsed_table.select(
            [
                field.name
                for field in schema
                if field.name in parsed_table.column_names
            ]
        )

    @staticmethod
    def _translate_ticker_table_codes(
        table_column: pyarrow.ChunkedArray,
    ) -> pyarrow.ChunkedArray:
        """
        Translate the dataset codes of the bulk tickers export into the table names the query API returns.

        Codes with no known table name are left untouched, since they identify datasets this provider does not
        consume and dropping them would hide them from anyone debugging a routing decision.

        Parameters
        ----------
        table_column
            The ``table`` column of a bulk loaded tickers table

        Returns
        -------
        The column with every recognised dataset code replaced by its table name
        """
        return pyarrow.chunked_array(
            [
                pyarrow.array(
                    [
                        BULK_TICKER_TABLE_CODES.get(code, code)
                        for code in table_column.to_pylist()
                    ],
                    type=pyarrow.string(),
                )
            ]
        )

    @staticmethod
    def filter_table(
        table_data: pyarrow.Table,
        *,
        tickers: typing.Collection[str] | None = None,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> pyarrow.Table:
        """
        Restrict a bulk table to the tickers and date range a run actually needs.

        Parameters
        ----------
        table_data
            The table to filter
        tickers
            The tickers to keep, or None to keep every one of them
        start_date
            The earliest date to keep
        end_date
            The latest date to keep

        Returns
        -------
        The filtered table
        """
        column_names = set(table_data.column_names)
        mask = None

        if (
            tickers is not None
            and 'ticker' in column_names
        ):
            mask = pyarrow.compute.is_in(
                table_data.column('ticker'),
                value_set=pyarrow.array(sorted(set(tickers)), type=pyarrow.string()),
            )

        if 'date' in column_names:
            date_column = table_data.column('date')
            for (bound, comparison) in (
                (start_date, pyarrow.compute.greater_equal),
                (end_date, pyarrow.compute.less_equal),
            ):
                if bound is None:
                    continue

                bound_mask = comparison(
                    date_column,
                    pyarrow.scalar(bound, type=pyarrow.date32()),
                )
                mask = (
                    bound_mask
                    if mask is None
                    else pyarrow.compute.and_(mask, bound_mask)
                )

        if mask is None:

            return table_data

        return table_data.filter(mask)

    def _download_archive(
        self,
        table: str,
        history: BulkHistory,
    ) -> bytes:
        """
        Download a bulk export, following the redirect to its time limited download URL.

        Parameters
        ----------
        table
            The name of the Sharadar table
        history
            How much history to download

        Returns
        -------
        The raw bytes of the zip archive

        Raises
        ------
        DataProviderFatalError
            When the table or history is not one Sharadar publishes a bulk export of
        DataProviderServerError
            When the download keeps failing after exhausting the retries
        """
        if table not in TABLE_SCHEMAS:
            msg = f"Unknown Sharadar table '{table}'"

            raise DataProviderFatalError(None, msg)

        if history not in BULK_HISTORY_YEARS:
            msg = f"Unknown Sharadar bulk history '{history}'"

            raise DataProviderFatalError(None, msg)

        url = "".join([
            f'{self.BASE_URL}/{urllib.parse.quote(table)}',
            f'?api_key={urllib.parse.quote(self.api_key)}',
            f'&years={history}',
        ])
        last_error: Exception | None = None

        for attempt_number in range(1, self.MAX_CONNECTION_RETRIES + 1):
            try:
                request = urllib.request.Request(url)   # noqa: S310    URL is always built from BASE_URL
                # urlopen follows the redirect to the time limited download URL on its own
                with urllib.request.urlopen(    # noqa: S310
                    request,
                    context=SharadarClient.load_ssl_context(),
                    timeout=self.DOWNLOAD_TIMEOUT_SECONDS,
                ) as response:
                    buffer = io.BytesIO()
                    while chunk := response.read(self.DOWNLOAD_CHUNK_BYTES):
                        buffer.write(chunk)

                    return buffer.getvalue()

            except urllib.error.HTTPError as error:
                # a bulk export the subscription doesn't cover is final, and the caller falls back to the API
                SharadarClient.raise_for_http_error(table, error)
                last_error = error
            except (urllib.error.URLError, urllib.error.ContentTooShortError, TimeoutError) as error:
                last_error = error

            if attempt_number < self.MAX_CONNECTION_RETRIES:
                wait_seconds = self.RETRY_BACKOFF_SECONDS * (2 ** (attempt_number - 1))
                msg = (
                    f"Sharadar bulk download of table {table} failed ({last_error}),"
                    f" retrying in {wait_seconds:.1f}s (attempt {attempt_number}/{self.MAX_CONNECTION_RETRIES})"
                )
                logging.getLogger(__name__).warning(msg)
                time.sleep(wait_seconds)

        msg = (
            f"Sharadar bulk download of table {table} failed after"
            f" {self.MAX_CONNECTION_RETRIES} attempts: {last_error}"
        )

        raise DataProviderServerError(None, msg)


def _extract_fiscal_period(column: DataColumn) -> DataColumn:
    """
    Extract the fiscal period out of Sharadar's fiscal period labels.

    The labels are of the form ``2023-Q4`` for quarterly filings and ``2023-FY`` for annual ones, and their
    period halves are already the values the fundamental data entity accepts.

    Parameters
    ----------
    column
        The column holding the fiscal period labels

    Returns
    -------
    The column holding just the period, like ``Q4`` or ``FY``
    """
    return DataColumn.load(
        pyarrow.compute.utf8_slice_codeunits(column.to_pyarrow(), 5, 7)
    )


def _extract_fiscal_year(column: DataColumn) -> DataColumn:
    """
    Extract the fiscal year out of Sharadar's fiscal period labels.

    Parameters
    ----------
    column
        The column holding the fiscal period labels, of the form ``2023-Q4``

    Returns
    -------
    The column holding the year as an integer
    """
    return DataColumn.load(
        pyarrow.compute.cast(
            pyarrow.compute.utf8_slice_codeunits(column.to_pyarrow(), 0, 4),
            pyarrow.int64(),
        )
    )


def _split_denominator(column: DataColumn) -> DataColumn:
    """
    Return the denominator of a split ratio.

    Sharadar publishes a split as a single ratio, where a value of 4 means four new shares for every old one,
    so the denominator is always one wherever a ratio was published at all.

    Parameters
    ----------
    column
        The column holding the split ratios

    Returns
    -------
    A column holding one wherever `column` holds a ratio, and null wherever it does not
    """
    ratios = column.to_pyarrow()

    return DataColumn.load(
        pyarrow.compute.if_else(
            pyarrow.compute.is_null(ratios),
            pyarrow.scalar(None, type=pyarrow.float64()),
            pyarrow.scalar(1.0, type=pyarrow.float64()),
        )
    )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """
    Redirect handler that lets a redirect surface as an error instead of following it.

    The bulk endpoint answers a recognised API key with a redirect to a download of hundreds of megabytes,
    which is worth avoiding when the status code is the only thing being asked about.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl) -> None:    # noqa: ANN001

        return None


class Sharadar(
    DataProviderInterface,      # this is the interface all data providers have to implement
):
    """
    Sharadar data provider.

    Acquires its data through whichever of the two paths suits the run: for a handful of identifiers the query
    API is cheaper, while past a hundred or so the whole-table exports win by more than an order of magnitude,
    since the API caps every request at 30 tickers and transfers several times more bytes for a subset of the
    data. `initialize` decides which path to use, and every ``get_`` method then only slices what it needs out
    of the tables that path produced.

    Parameters
    ----------
    api_key
        The api key for connecting to the provider
    """

    # the table whose bulk export is asked for when validating the API key; any of them authenticates
    VALIDATION_TABLE = 'actions'
    # Past this many identifiers the bulk exports are downloaded instead of the tickers being requested in
    # batches. Measured break-even is around eighty identifiers; the default leaves room for slower connections.
    BULK_TICKER_THRESHOLD = 100
    CACHE_DIR_ENVIRONMENT_VARIABLE = 'KNDC_SHARADAR_CACHE_DIR'
    DEFAULT_CACHE_DIR = pathlib.Path('Cache') / 'sharadar'

    # the values the tickers table uses to say which table holds a ticker's data
    STOCKS_TABLE = 'stocks'
    FUNDS_TABLE = 'funds'
    FUNDAMENTALS_TABLE = 'fundamentals'

    class Endpoints(enum.StrEnum):
        """
        The endpoints this provider reads.

        Unlike providers that expose one URL per data type, Sharadar serves every table from the same URL with
        the table name in its path, and serves dividends and splits from a single ``actions`` table told apart
        by a query parameter. These are therefore logical identifiers rather than URLs; what they have to be is
        unique keys of the endpoint field maps.
        """

        ACTIONS_DIVIDEND = 'sharadar:actions:dividend'
        ACTIONS_SPLIT = 'sharadar:actions:split'
        FUNDAMENTALS = 'sharadar:fundamentals'
        FUNDS = 'sharadar:funds'
        STOCKS = 'sharadar:stocks'

    # the Sharadar table each endpoint reads from
    _endpoint_tables: typing.Final[dict[str, str]] = {
        Endpoints.ACTIONS_DIVIDEND: 'actions',
        Endpoints.ACTIONS_SPLIT: 'actions',
        Endpoints.FUNDAMENTALS: 'fundamentals',
        Endpoints.FUNDS: 'funds',
        Endpoints.STOCKS: 'stocks',
    }
    # the value of the actions table's `action` column each actions endpoint keeps
    _endpoint_actions: typing.Final[dict[str, str]] = {
        Endpoints.ACTIONS_DIVIDEND: 'dividend',
        Endpoints.ACTIONS_SPLIT: 'split',
    }
    # Sharadar's as-reported dimensions, which are point in time and exclude restatements, matching the
    # filing-date clock the fundamentals data block runs on
    _periods: typing.Final[dict[str, str]] = {
        'annual': 'ARY',
        'quarterly': 'ARQ',
    }

    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.ACTIONS_DIVIDEND: {
            DividendDataRow.ex_dividend_date: 'date',
            DividendDataRow.dividend: 'value',
            # Sharadar publishes no declaration, record or payment date, and no split adjusted dividend
        },
    }

    _split_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.ACTIONS_SPLIT: {
            SplitDataRow.split_date: 'date',
            SplitDataRow.numerator: 'value',
            SplitDataRow.denominator: PreprocessedFieldMapping(
                ['value'],
                [_split_denominator]
            ),
        },
    }

    # the stocks and funds tables share a schema, so equities and funds share this mapping
    _market_data_row_map: typing.Final = {
        MarketDataDailyRow.date: 'date',
        MarketDataDailyRow.close: 'closeunadj',
        MarketDataDailyRow.open_split_adjusted: 'open',
        MarketDataDailyRow.high_split_adjusted: 'high',
        MarketDataDailyRow.low_split_adjusted: 'low',
        MarketDataDailyRow.close_split_adjusted: 'close',
        # Sharadar declares volume as floating point while the entity holds whole units
        MarketDataDailyRow.volume_split_adjusted: 'volume',
        MarketDataDailyRow.close_dividend_and_split_adjusted: 'closeadj',
        # Sharadar publishes no unadjusted or fully adjusted open, high, low or volume, and no vwap
    }
    _market_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCKS: _market_data_row_map,
        Endpoints.FUNDS: _market_data_row_map,
    }

    _fundamental_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.FUNDAMENTALS: {
            FundamentalDataRow.filing_date: 'date',
            FundamentalDataRow.period_end_date: 'reportperiod',
            FundamentalDataRow.fiscal_period: PreprocessedFieldMapping(
                ['fiscalperiod'],
                [_extract_fiscal_period]
            ),
            FundamentalDataRow.fiscal_year: PreprocessedFieldMapping(
                ['fiscalperiod'],
                [_extract_fiscal_year]
            ),
            # the fundamentals table carries no currency, so it is joined in from the tickers table
            FundamentalDataRow.reported_currency: 'currency',
            # Sharadar publishes no filing acceptance timestamp

            FundamentalDataRowIncomeStatement.basic_earnings_per_share: 'eps',
            FundamentalDataRowIncomeStatement.basic_net_income_available_to_common_stockholders: 'netinccmn',
            FundamentalDataRowIncomeStatement.cost_of_revenue: 'cor',
            FundamentalDataRowIncomeStatement.depreciation_and_amortization: 'depamor',
            FundamentalDataRowIncomeStatement.diluted_earnings_per_share: 'epsdil',
            FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax: 'netincdis',
            FundamentalDataRowIncomeStatement.earnings_before_interest_and_tax: 'ebit',
            FundamentalDataRowIncomeStatement.earnings_before_interest_tax_depreciation_and_amortization: 'ebitda',
            FundamentalDataRowIncomeStatement.gross_profit: 'gp',
            FundamentalDataRowIncomeStatement.income_before_tax: 'ebt',
            FundamentalDataRowIncomeStatement.income_tax_expense: 'taxexp',
            FundamentalDataRowIncomeStatement.interest_expense: 'intexp',
            FundamentalDataRowIncomeStatement.net_income: 'netinc',
            FundamentalDataRowIncomeStatement.net_income_deductions: 'prefdivis',
            FundamentalDataRowIncomeStatement.operating_expenses: 'opex',
            FundamentalDataRowIncomeStatement.operating_income: 'opinc',
            FundamentalDataRowIncomeStatement.research_and_development_expense: 'rnd',
            FundamentalDataRowIncomeStatement.revenues: 'revenue',
            FundamentalDataRowIncomeStatement.selling_general_and_administrative_expense: 'sgna',
            FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding: 'shareswa',
            FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding: 'shareswadil',

            FundamentalDataRowBalanceSheet.accumulated_other_comprehensive_income_after_tax: 'accoci',
            FundamentalDataRowBalanceSheet.assets: 'assets',
            FundamentalDataRowBalanceSheet.cash_and_cash_equivalents: 'cashneq',
            FundamentalDataRowBalanceSheet.current_accounts_payable: 'payables',
            FundamentalDataRowBalanceSheet.current_assets: 'assetsc',
            FundamentalDataRowBalanceSheet.current_liabilities: 'liabilitiesc',
            FundamentalDataRowBalanceSheet.current_net_receivables: 'receivables',
            FundamentalDataRowBalanceSheet.deferred_revenue: 'deferredrev',
            FundamentalDataRowBalanceSheet.investments: 'investments',
            FundamentalDataRowBalanceSheet.liabilities: 'liabilities',
            FundamentalDataRowBalanceSheet.longterm_debt: 'debtnc',
            FundamentalDataRowBalanceSheet.longterm_investments: 'investmentsnc',
            FundamentalDataRowBalanceSheet.net_intangible_assets_including_goodwill: 'intangibles',
            FundamentalDataRowBalanceSheet.net_inventory: 'inventory',
            FundamentalDataRowBalanceSheet.net_property_plant_and_equipment: 'ppnenet',
            FundamentalDataRowBalanceSheet.noncurrent_assets: 'assetsnc',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_assets: 'taxassets',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_liabilities: 'taxliabilities',
            FundamentalDataRowBalanceSheet.noncurrent_liabilities: 'liabilitiesnc',
            FundamentalDataRowBalanceSheet.retained_earnings: 'retearn',
            FundamentalDataRowBalanceSheet.shortterm_debt: 'debtc',
            FundamentalDataRowBalanceSheet.shortterm_investments: 'investmentsc',
            FundamentalDataRowBalanceSheet.stockholder_equity: 'equity',
            FundamentalDataRowBalanceSheet.total_debt_including_capital_lease_obligations: 'debt',

            FundamentalDataRowCashFlow.capital_expenditure: 'capex',
            FundamentalDataRowCashFlow.cash_and_cash_equivalents_change: 'ncf',
            FundamentalDataRowCashFlow.cash_exchange_rate_effect: 'ncfx',
            FundamentalDataRowCashFlow.depreciation_and_amortization: 'depamor',
            FundamentalDataRowCashFlow.dividend_payments: 'ncfdiv',
            FundamentalDataRowCashFlow.free_cash_flow: 'fcf',
            FundamentalDataRowCashFlow.net_business_acquisition_payments: 'ncfbus',
            FundamentalDataRowCashFlow.net_cash_from_financing_activities: 'ncff',
            FundamentalDataRowCashFlow.net_cash_from_investing_activities: 'ncfi',
            FundamentalDataRowCashFlow.net_cash_from_operating_activities: 'ncfo',
            FundamentalDataRowCashFlow.net_common_stock_issuance_proceeds: 'ncfcommon',
            FundamentalDataRowCashFlow.net_debt_issuance_proceeds: 'ncfdebt',
            FundamentalDataRowCashFlow.net_income: 'netinc',
            FundamentalDataRowCashFlow.other_investing_activities: 'ncfinv',
            FundamentalDataRowCashFlow.stock_based_compensation: 'sbcomp',
        },
    }

    def __init__(
        self,
        *,
        api_key: str | None,
    ):
        if (
            api_key is None
            or len(api_key) < 1
        ):
            raise DataProviderMissingKeyError

        self.api_key = api_key
        self.client = SharadarClient(api_key=api_key)
        self.bulk_downloader = SharadarBulkDownloader(
            api_key=api_key,
            cache_dir=pathlib.Path(
                os.getenv(self.CACHE_DIR_ENVIRONMENT_VARIABLE)
                or self.DEFAULT_CACHE_DIR
            ),
        )
        self._configuration: Configuration | None = None
        self._use_bulk = False
        self._loaded_tables: dict[str, pyarrow.Table] = {}
        self._ticker_metadata: dict[str, dict[str, typing.Any]] = {}

    @classmethod
    def get_data_block_endpoint_tag_map(cls) -> DataBlockEndpointTagMap:
        return {
            DividendsDataBlock: cls._dividend_data_endpoint_map,
            FundamentalsDataBlock: cls._fundamental_data_endpoint_map,
            MarketDailyDataBlock: cls._market_data_endpoint_map,
            SplitsDataBlock: cls._split_data_endpoint_map,
        }

    def initialize(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        """
        Choose the acquisition path for the run and load the ticker metadata that routes every identifier.

        The remaining tables are loaded lazily, the first time a ``get_`` method asks for one, so a run that
        only wants market data never pays for the fundamentals export.

        Parameters
        ----------
        configuration
            The Configuration entity with all the currently injected settings
        """
        self._configuration = configuration
        self._loaded_tables = {}

        identifiers = tuple(dict.fromkeys(configuration.identifiers))
        self._use_bulk = len(identifiers) >= self.BULK_TICKER_THRESHOLD
        msg = " ".join([
            f"Sharadar acquiring {len(identifiers)} identifiers through",
            "the bulk exports" if self._use_bulk else "the query API",
        ])
        logging.getLogger(__name__).info(msg)

        self._ticker_metadata = self._load_ticker_metadata(identifiers)
        unknown_identifiers = [
            identifier
            for identifier in identifiers
            if identifier not in self._ticker_metadata
        ]
        if len(unknown_identifiers) > 0:
            msg = " ".join([
                "Sharadar has no ticker metadata for the following identifiers,",
                "so they will be reported as not found:",
                ", ".join(unknown_identifiers),
            ])
            logging.getLogger(__name__).warning(msg)

    def get_market_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        """
        Get the market data from Sharadar wrapped in a MarketData entity.

        Equities are served by the stocks table and funds by the funds table, which share a schema; which one
        holds an identifier is stated by the tickers table rather than guessed.

        Parameters
        ----------
        main_identifier
            the stock's ticker
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in

        Returns
        -------
        MarketData

        Raises
        ------
        IdentifierNotFoundError
            When the identifier has no price table, or that table returned no rows for it
        """
        endpoint = self._market_data_endpoint(main_identifier)
        endpoint_rows = self._endpoint_rows(endpoint, main_identifier)
        if endpoint_rows.num_rows == 0:
            msg = f"{main_identifier} market data endpoints returned no data"

            raise IdentifierNotFoundError(msg)

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=MarketDailyDataBlock,
                endpoint_field_map=self._market_data_endpoint_map,
                endpoint_tables={endpoint: endpoint_rows},
            )
        except DataProviderToolkitNoDataError as error:
            msg = f"{main_identifier} market data endpoints returned no data"

            raise IdentifierNotFoundError(msg) from error

        consolidated_market_data = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[],
            predominant_order_descending=False,
        )

        return MarketDailyDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_market_data,
            common_field_data={
                MarketData: {
                    MarketData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

    def get_fundamental_data(
        self,
        *,
        main_identifier: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        """
        Get the fundamental data from Sharadar wrapped in a FundamentalData entity.

        Sharadar serves the income statement, balance sheet and cash flow of a filing as a single row, so no
        reconciliation between endpoints is needed. Instruments with no fundamentals, funds among them, get an
        empty entity rather than an error, since having none is a property of the instrument and not a failure.

        Parameters
        ----------
        main_identifier
            the stock's ticker
        period
            The period identifier
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in

        Returns
        -------
        FundamentalData
        """
        empty_fundamental_data = FundamentalData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )
        metadata = self._ticker_metadata.get(main_identifier)
        if (
            metadata is None
            or self.FUNDAMENTALS_TABLE not in metadata['tables']
        ):
            msg = " ".join([
                f"{main_identifier} has no fundamental data in Sharadar",
                f"(it is categorised as {metadata['category']})" if metadata is not None else "",
            ]).strip()
            logging.getLogger(__name__).warning(msg)

            return empty_fundamental_data

        endpoint = self.Endpoints.FUNDAMENTALS
        endpoint_rows = self._endpoint_rows(endpoint, main_identifier)
        if endpoint_rows.num_rows == 0:
            msg = f"{main_identifier} fundamental data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_fundamental_data

        # the fundamentals table carries no currency of its own, so the tickers table's is joined in
        endpoint_rows = endpoint_rows.append_column(
            'currency',
            pyarrow.array(
                [metadata['currency']] * endpoint_rows.num_rows,
                type=pyarrow.string(),
            )
        )

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=FundamentalsDataBlock,
                endpoint_field_map=self._fundamental_data_endpoint_map,
                endpoint_tables={endpoint: endpoint_rows},
            )
        except DataProviderToolkitNoDataError:
            msg = f"{main_identifier} fundamental data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_fundamental_data

        consolidated_fundamental_table = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[],
            predominant_order_descending=False,
        )

        warning_output_columns_map = {
            FundamentalsDataBlock.get_field_qualified_name(
                FundamentalsDataBlock.clock_sync_field
            ): 'filing_date',
            FundamentalsDataBlock.get_field_qualified_name(
                FundamentalDataRow.period_end_date
            ): 'period_end_date',
            FundamentalsDataBlock.get_field_qualified_name(
                FundamentalDataRow.fiscal_year
            ): 'fiscal_year',
            FundamentalsDataBlock.get_field_qualified_name(
                FundamentalDataRow.fiscal_period
            ): 'fiscal_period',
        }
        irregular_rows_mask = FundamentalsDataBlock.find_consolidated_table_irregular_filing_rows(
            consolidated_table=consolidated_fundamental_table
        )
        if irregular_rows_mask is not None:
            irregular_rows_table = (
                consolidated_fundamental_table
                .select(warning_output_columns_map.keys())
                .filter(irregular_rows_mask)
            )
            discrepancy_output_table = DataProviderToolkit.format_consolidated_discrepancy_table_for_output(
                discrepancy_table=irregular_rows_table,
                output_column_renames=warning_output_columns_map
            )
            msg = "\n".join([
                f"{main_identifier} presents irregular (ammended or late) filings.",
                "Omitting fundamental data for the periods corresponding to the following filings:",
                discrepancy_output_table
            ])
            logging.getLogger(__name__).warning(msg)

            consolidated_fundamental_table = consolidated_fundamental_table.filter(
                pyarrow.compute.invert(irregular_rows_mask)
            )

        if consolidated_fundamental_table.num_rows == 0:

            return empty_fundamental_data

        return FundamentalsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_fundamental_table,
            common_field_data={
                FundamentalData: {
                    FundamentalData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

    def get_dividend_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        """
        Get the dividend data from Sharadar wrapped in a DividendData entity.

        Parameters
        ----------
        main_identifier
            the stock's ticker
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in

        Returns
        -------
        DividendData
        """
        empty_dividend_data = DividendData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )
        endpoint = self.Endpoints.ACTIONS_DIVIDEND
        endpoint_rows = self._endpoint_rows(endpoint, main_identifier)
        if endpoint_rows.num_rows == 0:
            msg = f"{main_identifier} dividend data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_dividend_data

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=DividendsDataBlock,
                endpoint_field_map=self._dividend_data_endpoint_map,
                endpoint_tables={endpoint: endpoint_rows},
            )
        except DataProviderToolkitNoDataError:
            msg = f"{main_identifier} dividend data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_dividend_data

        consolidated_dividend_table = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[],
            predominant_order_descending=False,
        )

        ex_dividend_date_column_name = DividendsDataBlock.get_field_qualified_name(
            DividendsDataBlock.clock_sync_field
        )
        duplicate_rows_mask = DataProviderToolkit.find_duplicate_column_value_rows_mask(
            consolidated_dividend_table,
            ex_dividend_date_column_name,
        )
        if duplicate_rows_mask is not None:
            duplicate_columns_map = {
                DividendsDataBlock.get_field_qualified_name(
                    DividendDataRow.ex_dividend_date
                ): 'ex_dividend_date',
                DividendsDataBlock.get_field_qualified_name(
                    DividendDataRow.dividend
                ): 'dividend',
            }
            duplicate_output_table = DataProviderToolkit.format_consolidated_discrepancy_table_for_output(
                discrepancy_table=(
                    consolidated_dividend_table
                    .select(duplicate_columns_map.keys())
                    .filter(duplicate_rows_mask)
                ),
                output_column_renames=duplicate_columns_map,
            )
            msg = "\n".join([
                f"{main_identifier} dividend data endpoint returned duplicate ex-dividend dates,",
                "omitting its dividend data. Duplicated dividends:",
                duplicate_output_table
            ])
            logging.getLogger(__name__).error(msg)

            return empty_dividend_data

        return DividendsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_dividend_table,
            common_field_data={
                DividendData: {
                    DividendData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

    def get_split_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        """
        Get the split data from Sharadar wrapped in a SplitData entity.

        Sharadar publishes a split as a single ratio, so a four for one split is a value of four against a
        denominator of one.

        Parameters
        ----------
        main_identifier
            the stock's ticker
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in

        Returns
        -------
        SplitData
        """
        empty_split_data = SplitData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )
        endpoint = self.Endpoints.ACTIONS_SPLIT
        endpoint_rows = self._endpoint_rows(endpoint, main_identifier)
        if endpoint_rows.num_rows == 0:
            msg = f"{main_identifier} split data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_split_data

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=SplitsDataBlock,
                endpoint_field_map=self._split_data_endpoint_map,
                endpoint_tables={endpoint: endpoint_rows},
            )
        except DataProviderToolkitNoDataError:
            msg = f"{main_identifier} split data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_split_data

        consolidated_split_table = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[],
            predominant_order_descending=False,
        )

        return SplitsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_split_table,
            common_field_data={
                SplitData: {
                    SplitData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

    def validate_api_key(self) -> bool | None:
        """
        Validate that the API key used to init the class is valid, by making a test request.

        The query API cannot answer this question: it serves a free tier to anyone, so a request with a wrong
        key or no key at all comes back as a plain 200, either with data for the well known tickers or with an
        empty result set for anything the free tier does not reach. Neither is distinguishable from a real
        answer. The bulk endpoint does authenticate, replying 401 to a key it does not recognise and
        redirecting to the download otherwise, so that is what gets asked. The redirect is deliberately not
        followed, since only its status code is of interest here and the file behind it is hundreds of
        megabytes.

        Returns
        -------
        Whether `api_key` is valid
        """
        if (
            self.api_key is None
            or len(self.api_key) < 1
        ):

            return False

        url = "".join([
            f'{SharadarClient.BASE_URL}/{self.VALIDATION_TABLE}',
            f'?api_key={urllib.parse.quote(self.api_key)}',
            '&years=5',
        ])
        opener = urllib.request.build_opener(_NoRedirectHandler)

        try:
            with opener.open(url, timeout=SharadarClient.REQUEST_TIMEOUT_SECONDS) as response:
                # an unredirected success would still mean the key was accepted
                return response.status < http.HTTPStatus.BAD_REQUEST.value

        except urllib.error.HTTPError as error:
            # a redirect means the download was granted, and any status other than an outright rejection is
            # about what the subscription covers rather than about the key itself

            return error.code != http.HTTPStatus.UNAUTHORIZED.value

        except (urllib.error.URLError, TimeoutError) as error:
            msg = f"Could not reach Sharadar to validate the API key: {error}"

            raise DataProviderServerError(None, msg) from error

    def _load_ticker_metadata(
        self,
        identifiers: typing.Sequence[str],
    ) -> dict[str, dict[str, typing.Any]]:
        """
        Load the tickers table rows of the run's identifiers, keyed by ticker.

        The tickers table holds one row per dataset an identifier appears in, which is what says whether its
        prices come from the stocks or the funds table and whether it has fundamentals at all. It also carries
        the reporting currency, which the fundamentals table itself does not.

        Parameters
        ----------
        identifiers
            The identifiers of the run

        Returns
        -------
        Mapping of ticker to its tables, currency and category
        """
        tickers_table = self._request_table('tickers', identifiers=identifiers, filter_dates=False)
        metadata: dict[str, dict[str, typing.Any]] = {}

        for row in tickers_table.select(['ticker', 'table', 'currency', 'category']).to_pylist():
            ticker = row['ticker']
            if ticker not in metadata:
                metadata[ticker] = {
                    'tables': set(),
                    'currency': row['currency'],
                    'category': row['category'],
                }

            metadata[ticker]['tables'].add(row['table'])

        return metadata

    def _market_data_endpoint(
        self,
        main_identifier: str,
    ) -> 'Sharadar.Endpoints':
        """
        Return the endpoint holding an identifier's prices.

        Parameters
        ----------
        main_identifier
            the stock's ticker

        Returns
        -------
        The endpoint to read the identifier's prices from

        Raises
        ------
        IdentifierNotFoundError
            When Sharadar publishes no prices for the identifier at all
        """
        metadata = self._ticker_metadata.get(main_identifier)
        if metadata is not None:
            if self.STOCKS_TABLE in metadata['tables']:

                return self.Endpoints.STOCKS

            if self.FUNDS_TABLE in metadata['tables']:

                return self.Endpoints.FUNDS

        msg = f"{main_identifier} has no price data in Sharadar"

        raise IdentifierNotFoundError(msg)

    def _endpoint_rows(
        self,
        endpoint: 'Sharadar.Endpoints',
        main_identifier: str,
    ) -> pyarrow.Table:
        """
        Return one identifier's rows of an endpoint, in ascending date order.

        The bulk exports are not published in date order, and the data blocks require their clock column
        sorted, so ordering here is what lets both acquisition paths feed the same pipeline.

        Parameters
        ----------
        endpoint
            The endpoint whose rows to return
        main_identifier
            the stock's ticker

        Returns
        -------
        The identifier's rows
        """
        table = self._get_table(self._endpoint_tables[endpoint])
        row_mask = pyarrow.compute.equal(table.column('ticker'), main_identifier)

        endpoint_action = self._endpoint_actions.get(endpoint)
        if endpoint_action is not None:
            # dividends and splits share the actions table, told apart by its action column
            row_mask = pyarrow.compute.and_(
                row_mask,
                pyarrow.compute.equal(table.column('action'), endpoint_action),
            )

        return table.filter(row_mask).sort_by([('date', 'ascending')])

    def _get_table(
        self,
        table: str,
    ) -> pyarrow.Table:
        """
        Return a Sharadar table, loading it the first time it is asked for.

        Parameters
        ----------
        table
            The name of the Sharadar table

        Returns
        -------
        The loaded table
        """
        if table not in self._loaded_tables:
            extra_params = None
            if table == self.FUNDAMENTALS_TABLE:
                extra_params = {'dimension': self._periods[self._configuration.period]}

            self._loaded_tables[table] = self._request_table(
                table,
                identifiers=self._configuration.identifiers,
                extra_params=extra_params,
            )

        return self._loaded_tables[table]

    def _request_table(
        self,
        table: str,
        *,
        identifiers: typing.Sequence[str],
        extra_params: dict[str, str] | None = None,
        filter_dates: bool = True,
    ) -> pyarrow.Table:
        """
        Load a Sharadar table through whichever acquisition path this run chose.

        A bulk export the subscription does not cover is not fatal: the run falls back to the query API, which
        every subscription can reach, at the cost of taking considerably longer.

        Parameters
        ----------
        table
            The name of the Sharadar table
        identifiers
            The identifiers to restrict the table to
        extra_params
            Any additional query parameters, like the fundamentals dimension
        filter_dates
            Whether to restrict the table to the configured date range

        Returns
        -------
        The loaded table
        """
        start_date = self._configuration.start_date if filter_dates else None
        end_date = self._configuration.end_date if filter_dates else None

        if self._use_bulk:
            try:

                return self._request_bulk_table(
                    table,
                    identifiers=identifiers,
                    extra_params=extra_params,
                    start_date=start_date,
                    end_date=end_date,
                )
            except DataProviderApiError as error:
                msg = " ".join([
                    f"Sharadar bulk export of table {table} is not available ({error}),",
                    "falling back to the query API, which will take considerably longer",
                ])
                logging.getLogger(__name__).warning(msg)

        return self.client.fetch_table(
            table=table,
            tickers=identifiers,
            start_date=start_date.strftime('%Y-%m-%d') if start_date is not None else None,
            end_date=end_date.strftime('%Y-%m-%d') if end_date is not None else None,
            extra_params=extra_params,
        )

    def _request_bulk_table(
        self,
        table: str,
        *,
        identifiers: typing.Sequence[str],
        extra_params: dict[str, str] | None,
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> pyarrow.Table:
        """
        Load a Sharadar table from its bulk export.

        The exports carry every dimension of the fundamentals table, while the query API only ever returns the
        one asked for, so the equivalent filtering is applied here.

        Parameters
        ----------
        table
            The name of the Sharadar table
        identifiers
            The identifiers to restrict the table to
        extra_params
            Any additional query parameters, like the fundamentals dimension
        start_date
            The earliest date to keep
        end_date
            The latest date to keep

        Returns
        -------
        The loaded table
        """
        bulk_table = self.bulk_downloader.load_table(
            table=table,
            history=SharadarBulkDownloader.select_history(
                start_date if start_date is not None else self._configuration.start_date,
                table,
            ),
            tickers=identifiers,
            start_date=start_date,
            end_date=end_date,
        )

        dimension = (extra_params or {}).get('dimension')
        if dimension is not None:
            bulk_table = bulk_table.filter(
                pyarrow.compute.equal(bulk_table.column('dimension'), dimension)
            )

        return bulk_table
