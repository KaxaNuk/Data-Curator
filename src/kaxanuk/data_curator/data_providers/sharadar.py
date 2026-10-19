"""
Sharadar data provider.

Reads five Sharadar tables of end of day US data and maps them onto the library's four data types:

===================  ==========================================  ==========================
Sharadar table       Holds                                       Feeds
===================  ==========================================  ==========================
``stocks``           daily prices of ~22,000 common stocks       `MarketData`
``funds``            daily prices of ~10,000 ETFs, CEFs and ETNs `MarketData`
``fundamentals``     income, balance and cash flow of a filing   `FundamentalData`
``actions``          dividends and splits                        `DividendData`, `SplitData`
``tickers``          reference metadata for every instrument     routing and reporting currency
===================  ==========================================  ==========================

Every table is read through the query API, which shapes the module:

* At most 30 tickers per request, so a large run issues thousands of requests.
* Requests are rate limited per key; a throttled request waits for the announced reset instead of failing.
"""

import collections
import datetime
import enum
import fractions
import http
import http.client
import itertools
import json
import logging
import math
import random
import ssl
import time
import typing
import urllib.error
import urllib.parse
import urllib.request

import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.entities import (
    BaseDataEntity,
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
    DataProviderAuthenticationError,
    DataProviderAuthorizationError,
    DataProviderFatalError,
    DataProviderMissingKeyError,
    DataProviderParsingError,
    DataProviderRateLimitError,
    DataProviderServerError,
    DataProviderToolkitError,
    DataProviderToolkitNoDataError,
    IdentifierNotFoundError,
    InjectedDependencyError,
)
from kaxanuk.data_curator.modules.data_column import DataColumn
from kaxanuk.data_curator.services.data_provider_toolkit import (
    DataBlockEndpointTagMap,
    DataProviderFieldPreprocessors,
    DataProviderToolkit,
    EndpointFieldMap,
    PreprocessedFieldMapping,
)


# Primary key columns of each table, per Sharadar's DDL
TABLE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    'stocks': ('ticker', 'date'),
    'funds': ('ticker', 'date'),
    'fundamentals': ('ticker', 'dimension', 'date', 'reportperiod'),
    'actions': ('date', 'action', 'ticker', 'name', 'contraticker', 'contraname'),
    'tickers': ('table', 'permaticker', 'ticker'),
}

TABLE_NAMES: frozenset[str] = frozenset(TABLE_PRIMARY_KEYS)

MAX_SPLIT_FRACTION_DENOMINATOR: typing.Final = 10000
SPLIT_RATIO_ROUNDING_TOLERANCE: typing.Final = 1e-4


type JsonRecord = dict[str, typing.Any]


class SharadarUnparsableValuesError(DataProviderParsingError):
    """
    Raised when a page holds values whose type disagrees with their column, naming the tickers to skip.

    Parameters
    ----------
    message
        The error message, naming the tickers and the columns that disagree
    tickers
        The tickers whose rows hold the offending values
    """

    def __init__(
        self,
        message: str,
        tickers: frozenset[str],
    ) -> None:
        self.tickers = tickers
        super().__init__(message)


class SharadarClient:
    """
    Client for the Sharadar query API: ticker batching, pagination, retries and typing into PyArrow tables.

    Parameters
    ----------
    api_key
        The Sharadar API key
    page_size
        Rows requested per page; a shorter page means there are no more
    """

    BASE_URL = 'https://api.sharadar.com/v1.0/data'

    # Provider limits; exceeding either is answered with an HTTP 400
    MAX_TICKERS_PER_REQUEST = 30
    MAX_TICKER_PARAM_LENGTH = 200

    MAX_PAGE_SIZE = 10000
    MAX_PAGES_PER_REQUEST = 1000
    # Sharadar sorts by one field only, so rows sharing a date come in no fixed order; paging goes by date
    PAGINATION_DATE_COLUMN = 'date'

    # About four minutes in all, enough to ride out a short network outage during a long run
    RETRY_WAIT_SECONDS: tuple[float, ...] = (5.0, 15.0, 30.0, 60.0, 120.0)
    MAX_CONNECTION_RETRIES = len(RETRY_WAIT_SECONDS) + 1
    MAX_RETRY_WAIT_SECONDS = RETRY_WAIT_SECONDS[-1]
    REQUEST_TIMEOUT_SECONDS = 60.0

    # Requests are counted per key over 15 minutes; this header says when the window resets
    RATE_LIMIT_RESET_HEADER = 'X-RateLimit-Reset'
    MAX_RATE_LIMIT_WAIT_SECONDS = 16 * 60.0     # the window plus a minute of clock skew
    RATE_LIMIT_BACKOFF_SECONDS = 30.0           # when no reset is announced
    RATE_LIMIT_JITTER_SECONDS = 5.0             # keeps parallel instances from retrying all at once

    PROGRESS_LOG_LINES = 20         # progress lines per table, besides the first and last batch
    UNPARSABLE_VALUE_SAMPLES = 3    # bad values quoted in an unreadable-values report

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
        Split a ticker list into batches within the provider's ticker count and parameter length limits.

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
            # every ticker after the first also costs a comma
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
        Fetch a Sharadar table, batching the tickers, paginating, and logging progress per batch.

        Parameters
        ----------
        table
            The name of the Sharadar table
        tickers
            The tickers to fetch, or None for no ticker filter
        start_date
            The earliest date to fetch, in ``YYYY-MM-DD`` format
        end_date
            The latest date to fetch, in ``YYYY-MM-DD`` format
        extra_params
            Any additional query parameters, like the fundamentals ``dimension``

        Returns
        -------
        The table's rows, typed from their values

        Raises
        ------
        DataProviderFatalError
            When `table` is not one this client reads
        DataProviderRateLimitError
            When a batch is still throttled after its retries, naming the batch and its tickers
        DataProviderServerError
            When a batch keeps failing after its retries, naming the batch and its tickers
        """
        if table not in TABLE_NAMES:
            msg = f"Unknown Sharadar table '{table}'"

            raise DataProviderFatalError(None, msg)

        base_params: dict[str, str] = {
            'api_key': self.api_key,
            'format': 'json',
        }
        # `from` combined with the paging's `date.lte` returns one row before the range; `date.gte` doesn't
        if start_date is not None:
            base_params[f'{self.PAGINATION_DATE_COLUMN}.gte'] = start_date
        if end_date is not None:
            base_params[f'{self.PAGINATION_DATE_COLUMN}.lte'] = end_date
        if extra_params is not None:
            base_params.update(extra_params)

        # a repeated ticker could land in two batches, which deduplication can't see across
        ticker_batches: list[tuple[str, ...] | None] = (
            list(self.batch_tickers(list(dict.fromkeys(tickers))))
            if tickers is not None
            else [None]
        )
        batch_count = len(ticker_batches)
        progress_step = max(1, math.ceil(batch_count / self.PROGRESS_LOG_LINES))
        if batch_count > 1:
            msg = f"Sharadar table {table}: fetching {len(tickers or []):,} tickers in {batch_count:,} batches"
            logging.getLogger(__name__).info(msg)

        batch_tables: list[pyarrow.Table] = []
        fetched_rows = 0

        for (batch_number, ticker_batch) in enumerate(ticker_batches, start=1):
            batch_params = dict(base_params)
            if ticker_batch is not None:
                batch_params['ticker'] = ','.join(ticker_batch)

            try:
                page_tables = self._fetch_paginated(table, batch_params)
            except (DataProviderRateLimitError, DataProviderServerError) as error:
                msg = " ".join(
                    part
                    for part in (
                        f"Sharadar table {table} failed",
                        f"on batch {batch_number} of {batch_count}" if batch_count > 1 else "",
                        f"for tickers {', '.join(ticker_batch)}" if ticker_batch is not None else "",
                        # the exception adds its own prefix, so only the original detail is carried over
                        f"after exhausting its retries: {str(error).partition(': ')[2] or error}",
                    )
                    if len(part) > 0
                )

                raise type(error)(error.http_code, msg) from error

            # consecutive pages share their boundary date; batches never share a ticker
            batch_table = self.drop_duplicate_keys(
                self.concatenate_pages(page_tables),
                TABLE_PRIMARY_KEYS[table],
            )
            batch_tables.append(batch_table)
            fetched_rows += batch_table.num_rows

            if (
                batch_count > 1
                and (
                    batch_number % progress_step == 0
                    or batch_number == batch_count
                )
            ):
                msg = (
                    f"Sharadar table {table}: batch {batch_number} of {batch_count} fetched"
                    f" ({fetched_rows:,} rows so far)"
                )
                logging.getLogger(__name__).info(msg)

        if len(batch_tables) < 1:

            return pyarrow.table({})

        return self.concatenate_pages(batch_tables)

    @staticmethod
    def drop_duplicate_keys(
        table_data: pyarrow.Table,
        primary_key: typing.Sequence[str],
    ) -> pyarrow.Table:
        """
        Keep the first row of every primary key, in arrival order.

        Parameters
        ----------
        table_data
            The table to deduplicate
        primary_key
            The columns that identify a row

        Returns
        -------
        The table without repeated keys
        """
        if table_data.num_rows < 2:     # noqa: PLR2004  a single row can't repeat itself

            return table_data

        row_number_column = '__row_number'
        first_row_numbers = (
            table_data
            .select(list(primary_key))
            .append_column(row_number_column, pyarrow.array(range(table_data.num_rows), type=pyarrow.int64()))
            .group_by(list(primary_key), use_threads=False)
            .aggregate([(row_number_column, 'min')])
            .column(f'{row_number_column}_min')
        )
        if len(first_row_numbers) == table_data.num_rows:

            return table_data

        return table_data.take(
            first_row_numbers.take(pyarrow.compute.sort_indices(first_row_numbers))
        )

    @classmethod
    def records_to_table(
        cls,
        table: str,
        records: typing.Sequence[JsonRecord],
    ) -> pyarrow.Table:
        """
        Build a PyArrow table from raw JSON records, inferring types like the other providers do.

        Parameters
        ----------
        table
            The name of the Sharadar table the records belong to, for error reporting
        records
            The raw records, as decoded from the response's ``data`` key

        Returns
        -------
        The table the records make up

        Raises
        ------
        SharadarUnparsableValuesError
            When the page can't be read, naming the tickers whose values disagree with their column
        """
        if len(records) < 1:

            return pyarrow.table({})

        try:

            return cls._parse_records(table, records)
        except (DataProviderParsingError, DataProviderToolkitError) as error:
            # PyArrow reports a row number, not a ticker, so the offending tickers are worked out here

            raise cls._unparsable_values_error(table, records) from error

    @classmethod
    def _parse_records(
        cls,
        table: str,
        records: typing.Sequence[JsonRecord],
    ) -> pyarrow.Table:
        """
        Read records into a table through the toolkit call every provider uses.

        Parameters
        ----------
        table
            The name of the Sharadar table, which names the endpoint
        records
            The raw records to read

        Returns
        -------
        The table the records make up
        """
        records_json = json.dumps(records)

        return DataProviderToolkit.create_endpoint_tables_from_json_mapping({
            table: records_json
        })[table]

    @classmethod
    def _unparsable_values_error(
        cls,
        table: str,
        records: typing.Sequence[JsonRecord],
    ) -> SharadarUnparsableValuesError:
        """
        Build the error for an unreadable page, naming the tickers whose values disagree.

        Each ticker is read on its own; a column's type is what most tickers infer, and the tickers inferring
        something else hold the offending values. Slow, but it only runs once a page has failed.

        Parameters
        ----------
        table
            The name of the Sharadar table
        records
            The raw records of the page

        Returns
        -------
        The error to raise
        """
        records_by_ticker: dict[str, list[JsonRecord]] = {}
        for record in records:
            ticker = record.get('ticker', '(no ticker)')
            records_by_ticker.setdefault(ticker, []).append(record)

        unreadable_tickers: dict[str, None] = {}
        ticker_column_families: dict[str, dict[str, str]] = {}
        disagreements: list[str] = []
        for (ticker, ticker_records) in records_by_ticker.items():
            try:
                ticker_table = cls._parse_records(table, ticker_records)
            except (DataProviderParsingError, DataProviderToolkitError):
                # this ticker's own rows already disagree among themselves
                unreadable_tickers[ticker] = None
                odd_value = cls._odd_value_out(ticker_records)
                if (
                    odd_value is not None
                    and len(disagreements) < cls.UNPARSABLE_VALUE_SAMPLES
                ):
                    disagreements.append(f"{ticker} {odd_value}")

                continue

            ticker_column_families[ticker] = {
                field.name: cls._type_family(field.type)
                for field in ticker_table.schema
                if not pyarrow.types.is_null(field.type)
            }

        majority_families = cls._column_majority_families(ticker_column_families)
        for (ticker, column_families) in ticker_column_families.items():
            for (column, column_family) in column_families.items():
                if column_family == majority_families[column]:
                    continue

                unreadable_tickers[ticker] = None
                if len(disagreements) < cls.UNPARSABLE_VALUE_SAMPLES:
                    sample_value = records_by_ticker[ticker][0].get(column)
                    disagreements.append(
                        " ".join([
                            f"{ticker} {column}={sample_value!r} reads as {column_family}",
                            f"where the other tickers send {majority_families[column]}",
                        ])
                    )

        message = " ".join([
            f"Sharadar table {table} returned a page that could not be read in one pass:",
            f"{len(unreadable_tickers)} tickers hold values that disagree with their column.",
            f"Examples: {'; '.join(disagreements)}" if len(disagreements) > 0 else "",
        ]).strip()

        return SharadarUnparsableValuesError(message, frozenset(unreadable_tickers))

    @staticmethod
    def _type_family(
        column_type: pyarrow.DataType,
    ) -> str:
        """
        Return the family a column's inferred type belongs to.

        Compared by family because whole-number prices infer integers where others infer decimals, and both
        are fine.

        Parameters
        ----------
        column_type
            The type inferred for a column

        Returns
        -------
        number, text, timestamp, boolean, or the type itself otherwise
        """
        if pyarrow.types.is_boolean(column_type):

            return 'boolean'

        if (
            pyarrow.types.is_integer(column_type)
            or pyarrow.types.is_floating(column_type)
        ):

            return 'number'

        if (
            pyarrow.types.is_string(column_type)
            or pyarrow.types.is_large_string(column_type)
        ):

            return 'text'

        if pyarrow.types.is_temporal(column_type):

            return 'timestamp'

        return str(column_type)

    @staticmethod
    def _value_family(
        value: typing.Any,
    ) -> str:
        """
        Return the family of a raw JSON value, matching what `_type_family` returns for its column.

        Parameters
        ----------
        value
            The value as JSON carried it

        Returns
        -------
        The name of the family the value belongs to
        """
        if isinstance(value, bool):

            return 'boolean'

        if isinstance(value, (int, float)):

            return 'number'

        if isinstance(value, str):

            return 'text'

        return type(value).__name__

    @classmethod
    def _odd_value_out(
        cls,
        ticker_records: typing.Sequence[JsonRecord],
    ) -> str | None:
        """
        Return the value that disagrees with the rest of its column, among one ticker's records.

        Parameters
        ----------
        ticker_records
            The records of a single ticker

        Returns
        -------
        The column and value that disagree, like ``close='17O.55'``, or None when nothing stands out
        """
        values_by_column: dict[str, list[typing.Any]] = {}
        for record in ticker_records:
            for (column, value) in record.items():
                if value is not None:
                    values_by_column.setdefault(column, []).append(value)

        for (column, values) in values_by_column.items():
            family_counts = collections.Counter(
                cls._value_family(value)
                for value in values
            )
            if len(family_counts) < 2:    # noqa: PLR2004  one family is a column that agrees with itself
                continue

            rarest_family = family_counts.most_common()[-1][0]
            odd_value = next(
                value
                for value in values
                if cls._value_family(value) == rarest_family
            )

            return f"{column}={odd_value!r}"

        return None

    @staticmethod
    def _column_majority_families(
        ticker_column_families: dict[str, dict[str, str]],
    ) -> dict[str, str]:
        """
        Return the type family most tickers infer for each column.

        Parameters
        ----------
        ticker_column_families
            The type family each ticker inferred for each of its columns

        Returns
        -------
        Each column mapped to its majority family
        """
        family_counts: dict[str, collections.Counter] = {}
        for column_families in ticker_column_families.values():
            for (column, column_family) in column_families.items():
                family_counts.setdefault(column, collections.Counter())[column_family] += 1

        return {
            column: counts.most_common(1)[0][0]
            for (column, counts) in family_counts.items()
        }

    @staticmethod
    def concatenate_pages(
        page_tables: typing.Sequence[pyarrow.Table],
    ) -> pyarrow.Table:
        """
        Concatenate the pages of a query, reconciling the types each one inferred.

        A column all null in one page infers null there and a real type elsewhere, and a column may be missing
        from a page altogether.

        Parameters
        ----------
        page_tables
            The tables of each page, in the order they were fetched

        Returns
        -------
        The concatenated table, with every column any page carried
        """
        filled_pages = [
            page_table
            for page_table in page_tables
            if page_table.num_columns > 0
        ]
        if len(filled_pages) < 1:

            return pyarrow.table({})

        return pyarrow.concat_tables(filled_pages, promote_options='permissive')

    def _fetch_paginated(
        self,
        table: str,
        params: dict[str, str],
    ) -> list[pyarrow.Table]:
        """
        Fetch every page of a single query.

        Dated tables are paged by date, descending: rows of the same date come in a different order on every
        request, so paging with ``skip`` lost 11 to 15 rows per 211,138. Each page asks for the rows dated on or
        before the last one received, and the caller drops the repeated ones. A full page means more remain.

        Parameters
        ----------
        table
            The name of the Sharadar table
        params
            The query parameters, without the pagination ones

        Returns
        -------
        One table per page, in fetch order; at least one, even if empty

        Raises
        ------
        DataProviderParsingError
            When a full page of a dated table is not in descending date order
        DataProviderFatalError
            When a single date holds a whole page of rows
        """
        page_tables: list[pyarrow.Table] = []
        pages_by_date = self.PAGINATION_DATE_COLUMN in TABLE_PRIMARY_KEYS[table]
        skip = 0
        latest_date_left: str | None = None

        for page_number in range(self.MAX_PAGES_PER_REQUEST):
            page_params = dict(params)
            page_params['limit'] = str(self.page_size)
            if pages_by_date:
                page_params['sort'] = f'{self.PAGINATION_DATE_COLUMN}.desc'
            if latest_date_left is not None:
                page_params[f'{self.PAGINATION_DATE_COLUMN}.lte'] = latest_date_left
            elif skip > 0:
                page_params['skip'] = str(skip)

            page_records = self._request_records(table, page_params)
            page_tables.append(
                self.records_to_table(table, page_records)
            )

            if len(page_records) < self.page_size:

                return page_tables

            if pages_by_date:
                latest_date_left = self._next_page_latest_date(table, page_records, latest_date_left)
            else:
                skip += len(page_records)

            if page_number == (self.MAX_PAGES_PER_REQUEST - 1):
                msg = " ".join([
                    f"Sharadar table {table} returned more than",
                    f"{self.MAX_PAGES_PER_REQUEST * self.page_size} rows for a single query;",
                    "truncating. Narrow the date range.",
                ])
                logging.getLogger(__name__).warning(msg)

        return page_tables

    @classmethod
    def _next_page_latest_date(
        cls,
        table: str,
        page_records: typing.Sequence[JsonRecord],
        current_latest_date: str | None,
    ) -> str:
        """
        Return the latest date the next page has to ask for.

        Parameters
        ----------
        table
            The name of the Sharadar table, for error reporting
        page_records
            The records of the full page just received
        current_latest_date
            The latest date the current page asked for, or None for the first page

        Returns
        -------
        The page's last date, as Sharadar sends it

        Raises
        ------
        DataProviderParsingError
            When the page is not in descending date order
        DataProviderFatalError
            When the page holds nothing but its starting date
        """
        page_dates = [record.get(cls.PAGINATION_DATE_COLUMN) for record in page_records]
        if (
            any(date is None for date in page_dates)
            or any(earlier < later for (earlier, later) in itertools.pairwise(page_dates))
        ):
            msg = " ".join([
                f"Sharadar table {table} returned a page that is not in descending date order,",
                "so its remaining rows can't be fetched without risking gaps",
            ])

            raise DataProviderParsingError(msg)

        last_date = page_dates[-1]
        if last_date == current_latest_date:
            msg = " ".join([
                f"Sharadar table {table} holds more than {len(page_records)} rows dated {last_date}",
                "for a single query, more than one page can return; request fewer tickers at a time",
            ])

            raise DataProviderFatalError(None, msg)

        return last_date

    def _request_records(
        self,
        table: str,
        params: dict[str, str],
    ) -> list[JsonRecord]:
        """
        Perform a request and unwrap its response envelope.

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
            When the response is not the documented JSON envelope
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
            # a 200 response can still carry an error
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
        Perform a request, retrying transient errors.

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
            When the request is still rate limited after the retries
        DataProviderServerError
            When the request keeps failing after the retries
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
                    last_error = rate_limit_error   # a large run is bound to hit it, so it is waited out
                else:
                    last_error = error
            except (
                http.client.HTTPException,
                OSError,
            ) as error:
                # OSError also covers resets and SSL errors raised while reading the body, not only on connect
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

            raise last_error    # kept as is, so throttling can be told apart from an unreachable provider

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

        A throttled request waits until the rate limit window resets; other errors follow `RETRY_WAIT_SECONDS`.

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
        if isinstance(error, DataProviderRateLimitError):
            wait_seconds = (
                error.retry_after
                if error.retry_after is not None
                else cls.RATE_LIMIT_BACKOFF_SECONDS * (2 ** (attempt_number - 1))
            )
            jitter_seconds = random.uniform(0, cls.RATE_LIMIT_JITTER_SECONDS)     # noqa: S311  not cryptographic

            return min(wait_seconds, cls.MAX_RATE_LIMIT_WAIT_SECONDS) + jitter_seconds

        if (
            isinstance(error, urllib.error.HTTPError)
            and (retry_after := error.headers.get('Retry-After')) is not None
        ):
            try:

                return min(float(retry_after), cls.MAX_RETRY_WAIT_SECONDS)
            except ValueError:
                pass    # the header may hold a date instead, which isn't parsed

        return cls.RETRY_WAIT_SECONDS[
            min(attempt_number, len(cls.RETRY_WAIT_SECONDS)) - 1
        ]

    @classmethod
    def raise_for_http_error(
        cls,
        table: str,
        error: urllib.error.HTTPError,
    ) -> None:
        """
        Translate an HTTP error into its data provider exception, or return to let it be retried.

        Parameters
        ----------
        table
            The name of the Sharadar table, for error reporting
        error
            The raised HTTP error

        Raises
        ------
        DataProviderApiError
            One of its non-retryable subclasses, when the error is final
        """
        try:
            payload = json.loads(error.read().decode('utf-8'))
        except (ValueError, OSError):
            payload = {}

        if error.code == http.HTTPStatus.TOO_MANY_REQUESTS.value:
            raise DataProviderRateLimitError(   # retryable, but raised so the caller waits for the reset
                error.code,
                cls._error_message(table, payload, error),
                retry_after=cls._rate_limit_wait_seconds(error.headers),
            )

        if error.code >= http.HTTPStatus.INTERNAL_SERVER_ERROR.value:

            return  # transient, let the caller retry

        cls._raise_for_error_payload(table, error.code, payload, error)

    @classmethod
    def _rate_limit_wait_seconds(
        cls,
        headers: typing.Mapping[str, str],
    ) -> float | None:
        """
        Return how long a throttled response asks to wait: ``Retry-After`` if given, else until the reset.

        Parameters
        ----------
        headers
            The headers of the throttled response

        Returns
        -------
        The number of seconds to wait, or None when the response doesn't say
        """
        retry_after = headers.get('Retry-After')
        if (
            retry_after is not None
            and retry_after.isdigit()
        ):

            return float(retry_after)

        reset_timestamp = headers.get(cls.RATE_LIMIT_RESET_HEADER)
        if (
            reset_timestamp is not None
            and reset_timestamp.isdigit()
        ):

            return max(float(reset_timestamp) - time.time(), 0.0)

        return None

    @classmethod
    def _raise_for_error_payload(
        cls,
        table: str,
        status_code: int | None,
        payload: dict[str, typing.Any],
        error: urllib.error.HTTPError | None = None,
    ) -> None:
        """
        Raise the exception matching an error payload.

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
            When the key doesn't grant access to the requested data
        DataProviderFatalError
            When the request is malformed, which means a bug in this client
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
        Assemble a readable message from a Sharadar error payload.

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


def _extract_fiscal_period(column: DataColumn) -> DataColumn:
    """
    Extract the period out of Sharadar's fiscal period labels, like ``2023-Q4`` or ``2023-FY``.

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
    Return the denominator of each split ratio.

    Parameters
    ----------
    column
        The column holding the split ratios

    Returns
    -------
    The denominator of each ratio's fraction, null where no ratio was published
    """
    return DataColumn.load([
        fraction[1]
        for fraction in _split_fractions(column)
    ])


def _split_numerator(column: DataColumn) -> DataColumn:
    """
    Return the numerator of each split ratio.

    Parameters
    ----------
    column
        The column holding the split ratios

    Returns
    -------
    The numerator of each ratio's fraction, null where no ratio was published
    """
    return DataColumn.load([
        fraction[0]
        for fraction in _split_fractions(column)
    ])


def _split_fractions(
    column: DataColumn,
) -> list[tuple[float | None, float | None]]:
    """
    Recover the numerator and denominator behind each split ratio.

    Sharadar publishes a split as one rounded ratio (4 is four for one, 0.125 one for eight). The simplest
    fraction that reproduces it also undoes the rounding: 1.33333 comes back as 4/3. A ratio too small for any
    such fraction, like a one for a hundred thousand reverse split, is kept as the ratio over one.

    Parameters
    ----------
    column
        The column holding the split ratios

    Returns
    -------
    The numerator and denominator of each ratio, both null wherever no ratio was published
    """
    ratios = (
        pyarrow.compute.cast(column.to_pyarrow(), pyarrow.float64())
        .to_pylist()
    )
    fractions_found: list[tuple[float | None, float | None]] = []
    for ratio in ratios:
        if (
            ratio is None
            or math.isnan(ratio)
        ):
            fractions_found.append((None, None))

            continue

        simplest_fraction = (
            fractions.Fraction(ratio)
            .limit_denominator(MAX_SPLIT_FRACTION_DENOMINATOR)
        )
        reproduces_ratio = (
            simplest_fraction.numerator > 0
            and math.isclose(float(simplest_fraction), ratio, rel_tol=SPLIT_RATIO_ROUNDING_TOLERANCE)
        )
        fractions_found.append(
            (float(simplest_fraction.numerator), float(simplest_fraction.denominator))
            if reproduces_ratio
            else (ratio, 1.0)
        )

    return fractions_found


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """
    Redirect handler that surfaces a redirect as an error instead of following it.

    The export URL redirects a valid key to a download of hundreds of megabytes, when only the status matters.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl) -> None:    # noqa: ANN001

        return None


class SharadarIdentifierUnavailableError(IdentifierNotFoundError):
    """
    Raised for an identifier whose batch failed after its retries or whose values could not be read.

    An `IdentifierNotFoundError`, so the orchestrator skips the identifier and carries on with the run.
    """


class _LoadedBatch(typing.NamedTuple):
    """
    The rows of one batch of a table, sorted by ticker and date and indexed by ticker; empty if it failed.
    """

    number: int
    rows: pyarrow.Table
    ticker_rows: dict[str, tuple[int, int]]


class Sharadar(
    DataProviderInterface,      # this is the interface all data providers have to implement
):
    """
    Sharadar data provider.

    Loads each table one batch of 30 identifiers at a time, as the run reaches it, and slices each identifier's
    rows out of it. Only one batch per table is held in memory and nothing is written to disk.

    Parameters
    ----------
    api_key
        The api key for connecting to the provider
    """

    # any table authenticates the key; this one is used to validate it
    VALIDATION_TABLE = 'actions'

    # values of the tickers table's `table` column
    STOCKS_TABLE = 'stocks'
    FUNDS_TABLE = 'funds'
    FUNDAMENTALS_TABLE = 'fundamentals'

    # Fundamentals are read from before the run's start date, since the filing in effect on its first day was
    # filed earlier. The date has to be explicit: without one, a multi-ticker query only returns two years.
    FUNDAMENTALS_HISTORY_START_DATE = datetime.date(1900, 1, 1)

    class Endpoints(enum.StrEnum):
        """
        The endpoints this provider reads.

        Logical keys of the field maps rather than URLs: every table shares one URL, and dividends and splits
        both come from the ``actions`` table.
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
    # the `action` value each actions endpoint keeps
    _endpoint_actions: typing.Final[dict[str, str]] = {
        Endpoints.ACTIONS_DIVIDEND: 'dividend',
        Endpoints.ACTIONS_SPLIT: 'split',
    }
    # as-reported dimensions: point in time, without restatements
    _periods: typing.Final[dict[str, str]] = {
        'annual': 'ARY',
        'quarterly': 'ARQ',
    }

    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.ACTIONS_DIVIDEND: {
            DividendDataRow.ex_dividend_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            # Sharadar publishes each dividend adjusted for the splits after it, and nothing else: no paid
            # dividend, nor declaration, record or payment date
            DividendDataRow.dividend_split_adjusted: 'value',
        },
    }

    _split_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.ACTIONS_SPLIT: {
            SplitDataRow.split_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            SplitDataRow.numerator: PreprocessedFieldMapping(
                ['value'],
                [_split_numerator]
            ),
            SplitDataRow.denominator: PreprocessedFieldMapping(
                ['value'],
                [_split_denominator]
            ),
        },
    }

    # the stocks and funds tables share a schema, so equities and funds share this mapping
    _market_data_row_map: typing.Final = {
        MarketDataDailyRow.date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
            ['date'],
            [DataProviderFieldPreprocessors.cast_datetime_to_date]
        ),
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
            FundamentalDataRow.filing_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.period_end_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['reportperiod'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.fiscal_period: PreprocessedFieldMapping(
                ['fiscalperiod'],
                [_extract_fiscal_period]
            ),
            FundamentalDataRow.fiscal_year: PreprocessedFieldMapping(
                ['fiscalperiod'],
                [_extract_fiscal_year]
            ),
            # joined in from the tickers table
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
        self._configuration: Configuration | None = None
        self._identifier_batches: list[tuple[str, ...]] = []
        self._identifier_batch_numbers: dict[str, int] = {}
        self._loaded_batches: dict[str, _LoadedBatch] = {}
        # identifier -> why it could not be loaded, in the order they failed
        self._unavailable_identifiers: dict[str, str] = {}
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
        Load the ticker metadata of the run and split it into batches; the other tables load per batch later.

        Parameters
        ----------
        configuration
            The Configuration entity with all the currently injected settings
        """
        self._configuration = configuration
        self._loaded_batches = {}
        self._unavailable_identifiers = {}

        identifiers = tuple(dict.fromkeys(configuration.identifiers))
        self._identifier_batches = SharadarClient.batch_tickers(identifiers)
        self._identifier_batch_numbers = {
            identifier: batch_number
            for (batch_number, batch) in enumerate(self._identifier_batches, start=1)
            for identifier in batch
        }
        msg = f"Sharadar acquiring {len(identifiers)} identifiers through the query API"
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

    def get_data_block_data(
        self,
        *,
        main_identifier: str,
        data_block: type[BaseDataBlock],
        configuration: Configuration,
    ) -> BaseDataEntity:
        """
        Return one identifier's data for one data block, using the configuration given to `initialize`.

        Parameters
        ----------
        main_identifier
            The security's ticker
        data_block
            The data block class whose data entity to return
        configuration
            The Configuration entity with all the currently injected settings

        Returns
        -------
        The main data entity of `data_block`, holding the data

        Raises
        ------
        InjectedDependencyError
            When asked for a data block Sharadar does not serve
        """
        loaders = {
            DividendsDataBlock: self._dividend_data,
            FundamentalsDataBlock: self._fundamental_data,
            MarketDailyDataBlock: self._market_data,
            SplitsDataBlock: self._split_data,
        }
        loader = loaders.get(data_block)
        if loader is None:
            msg = " ".join([
                f"The Sharadar data provider cannot supply {data_block.__name__}:",
                f"it serves {', '.join(sorted(block.__name__ for block in loaders))}",
            ])

            raise InjectedDependencyError(msg)

        return loader(main_identifier)

    def _market_data(
        self,
        main_identifier: str,
    ) -> MarketData:
        """
        Get the market data wrapped in a MarketData entity, from the stocks or funds table per the tickers table.

        Parameters
        ----------
        main_identifier
            the stock's ticker

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

    def _fundamental_data(
        self,
        main_identifier: str,
    ) -> FundamentalData:
        """
        Get the fundamental data wrapped in a FundamentalData entity.

        Each filing is one row holding all three statements. Instruments without fundamentals, like funds, get an
        empty entity rather than an error.

        Parameters
        ----------
        main_identifier
            the stock's ticker

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

        # the fundamentals table carries no currency, so the tickers table's is joined in
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

    def _dividend_data(
        self,
        main_identifier: str,
    ) -> DividendData:
        """
        Get the dividend data wrapped in a DividendData entity; Sharadar publishes only the split adjusted one.

        Parameters
        ----------
        main_identifier
            the stock's ticker

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
                    DividendDataRow.dividend_split_adjusted
                ): 'dividend_split_adjusted',
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

    def _split_data(
        self,
        main_identifier: str,
    ) -> SplitData:
        """
        Get the split data wrapped in a SplitData entity, the fraction recovered from each ratio.

        Parameters
        ----------
        main_identifier
            the stock's ticker

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
        Validate the API key with a single request.

        The query API answers a wrong key with a free-tier 200, so the export URL is asked instead: it replies
        401 to an unknown key and redirects otherwise. The redirect is not followed, so nothing is downloaded.

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
            # 5 years is the window Sharadar keeps a ready-made export file for, so the request is answered
            # with a redirect to that file instead of streaming the table; any other window streams it
            '&years=5',
        ])
        opener = urllib.request.build_opener(_NoRedirectHandler)

        try:
            with opener.open(url, timeout=SharadarClient.REQUEST_TIMEOUT_SECONDS) as response:
                # an unredirected success would still mean the key was accepted
                return response.status < http.HTTPStatus.BAD_REQUEST.value

        except urllib.error.HTTPError as error:
            # a redirect means the key was accepted; other statuses are about the subscription, not the key

            return error.code != http.HTTPStatus.UNAUTHORIZED.value

        except (urllib.error.URLError, TimeoutError) as error:
            msg = f"Could not reach Sharadar to validate the API key: {error}"

            raise DataProviderServerError(None, msg) from error

    def _load_ticker_metadata(
        self,
        identifiers: typing.Sequence[str],
    ) -> dict[str, dict[str, typing.Any]]:
        """
        Load the tickers table rows of the run's identifiers: price table, fundamentals, currency and category.

        The tickers table holds a row per Sharadar table: the stocks row carries the currency the instrument
        trades in, and the fundamentals row the one its filings report in, which is the one kept here.

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
                    'currency': None,
                    'category': row['category'],
                }

            metadata[ticker]['tables'].add(row['table'])
            if row['table'] == self.FUNDAMENTALS_TABLE:
                # an ADR trades in dollars while reporting in its home currency, like Toyota's yen
                metadata[ticker]['currency'] = row['currency']

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
        Return one identifier's rows of an endpoint, in ascending date order, sliced out of its batch.

        Parameters
        ----------
        endpoint
            The endpoint whose rows to return
        main_identifier
            the stock's ticker

        Returns
        -------
        The identifier's rows

        Raises
        ------
        SharadarIdentifierUnavailableError
            When the identifier's data could not be downloaded or read
        """
        table_name = self._endpoint_tables[endpoint]
        batch_number = self._identifier_batch_numbers.get(main_identifier)
        if batch_number is None:

            return pyarrow.table({})

        batch = self._batch_rows(table_name, batch_number)
        unavailable_reason = self._unavailable_identifiers.get(main_identifier)
        if unavailable_reason is not None:
            msg = f"{main_identifier} could not be loaded from Sharadar: {unavailable_reason}"

            raise SharadarIdentifierUnavailableError(msg)

        (offset, length) = batch.ticker_rows.get(main_identifier, (0, 0))
        rows = batch.rows.slice(offset, length)

        endpoint_action = self._endpoint_actions.get(endpoint)
        if (
            endpoint_action is not None
            and rows.num_rows > 0
        ):
            # dividends and splits share the actions table, told apart by its action column
            rows = rows.filter(
                pyarrow.compute.equal(rows.column('action'), endpoint_action)
            )

        return rows

    def _batch_rows(
        self,
        table: str,
        batch_number: int,
    ) -> '_LoadedBatch':
        """
        Return the rows of a table for one batch of identifiers, loading them if that batch isn't the one held.

        Only one batch per table is held, so memory stays flat whatever the size of the run. The batch is sorted
        by ticker and date, leaving each identifier's rows contiguous and in date order.

        Parameters
        ----------
        table
            The name of the Sharadar table
        batch_number
            The number of the batch, starting at 1

        Returns
        -------
        The loaded batch
        """
        loaded_batch = self._loaded_batches.get(table)
        if (
            loaded_batch is not None
            and loaded_batch.number == batch_number
        ):

            return loaded_batch

        # released first, so two batches of a table are never held at once
        self._loaded_batches.pop(table, None)

        batch_count = len(self._identifier_batches)
        progress_step = max(1, math.ceil(batch_count / SharadarClient.PROGRESS_LOG_LINES))
        if (
            batch_number == 1
            or batch_number % progress_step == 0
            or batch_number == batch_count
        ):
            msg = f"Sharadar table {table}: loading batch {batch_number} of {batch_count}"
            logging.getLogger(__name__).info(msg)

        extra_params = None
        if table == self.FUNDAMENTALS_TABLE:
            extra_params = {'dimension': self._periods[self._configuration.period]}

        batch_identifiers = self._identifier_batches[batch_number - 1]
        try:
            requested_rows = self._request_readable_rows(
                table,
                identifiers=batch_identifiers,
                extra_params=extra_params,
            )
            dimension_rows = self._rows_of_requested_dimension(table, requested_rows, extra_params)
            # an empty batch comes back without columns
            rows = (
                dimension_rows.sort_by([('ticker', 'ascending'), ('date', 'ascending')])
                if dimension_rows.num_rows > 0
                else dimension_rows
            )
        except (
            DataProviderAuthorizationError,
            DataProviderRateLimitError,
            DataProviderServerError,
        ) as error:
            # A rejected key would fail every batch alike, so it still stops the run. A denied table is about what
            # the subscription covers, like funds on a stocks-only plan, so it costs only the identifiers needing it.
            reason = (
                f"the API key has no access to Sharadar table {table}; check the subscription covers it: {error}"
                if isinstance(error, DataProviderAuthorizationError)
                else f"Sharadar table {table} stayed undownloadable after exhausting its retries: {error}"
            )
            self._skip_identifiers(
                batch_identifiers,
                reason=reason,
                report=" ".join([
                    f"Sharadar table {table}: batch {batch_number} of {batch_count} could not be downloaded,",
                    f"skipping its {len(batch_identifiers)} identifiers and carrying on with the run:",
                    str(error),
                ]),
            )

            loaded_batch = _LoadedBatch(
                number=batch_number,
                rows=pyarrow.table({}),
                ticker_rows={},
            )
            self._loaded_batches[table] = loaded_batch

            return loaded_batch

        loaded_batch = _LoadedBatch(
            number=batch_number,
            rows=rows,
            ticker_rows=self._ticker_row_ranges(rows) if rows.num_rows > 0 else {},
        )
        self._loaded_batches[table] = loaded_batch

        return loaded_batch

    @staticmethod
    def _rows_of_requested_dimension(
        table: str,
        rows: pyarrow.Table,
        extra_params: dict[str, str] | None,
    ) -> pyarrow.Table:
        """
        Drop any fundamentals row of a dimension other than the one asked for.

        Sharadar has been seen answering with every dimension, which would mix restated figures into a point in
        time series.

        Parameters
        ----------
        table
            The name of the Sharadar table the rows came from
        rows
            The rows as the provider returned them
        extra_params
            The query parameters, holding the dimension when there is one

        Returns
        -------
        The rows of the requested dimension, or all of them when none was requested
        """
        dimension = (extra_params or {}).get('dimension')
        if (
            dimension is None
            or rows.num_rows < 1
        ):

            return rows

        requested_dimension_rows = rows.filter(
            pyarrow.compute.equal(rows.column('dimension'), dimension)
        )
        if requested_dimension_rows.num_rows < rows.num_rows:
            msg = " ".join([
                f"Sharadar table {table} was asked for dimension {dimension} but answered with",
                f"{rows.num_rows - requested_dimension_rows.num_rows} rows of other dimensions,",
                "which were dropped",
            ])
            logging.getLogger(__name__).warning(msg)

        return requested_dimension_rows

    def _request_readable_rows(
        self,
        table: str,
        *,
        identifiers: typing.Sequence[str],
        extra_params: dict[str, str] | None,
    ) -> pyarrow.Table:
        """
        Request a batch's rows, leaving out the identifiers whose values can't be read.

        A malformed value makes the whole page unreadable, so its identifiers are dropped and the batch is
        requested again without them.

        Parameters
        ----------
        table
            The name of the Sharadar table
        identifiers
            The identifiers to request
        extra_params
            Any additional query parameters, like the fundamentals dimension

        Returns
        -------
        The rows of the readable identifiers, empty if none
        """
        if len(identifiers) < 1:

            return pyarrow.table({})

        try:

            return self._request_table(
                table,
                identifiers=identifiers,
                extra_params=extra_params,
            )
        except SharadarUnparsableValuesError as error:
            self._skip_identifiers(
                sorted(error.tickers),
                reason=str(error),
                report=" ".join([
                    f"Sharadar table {table}: skipping {len(error.tickers)} identifiers whose values could not",
                    f"be read, and carrying on with the rest of their batch. {error}",
                ]),
            )

            return self._request_readable_rows(
                table,
                identifiers=[
                    identifier
                    for identifier in identifiers
                    if identifier not in error.tickers
                ],
                extra_params=extra_params,
            )

    def _skip_identifiers(
        self,
        identifiers: typing.Iterable[str],
        *,
        reason: str,
        report: str,
    ) -> None:
        """
        Record identifiers as unavailable for the run, and log the cumulative list to run again.

        Parameters
        ----------
        identifiers
            The identifiers that could not be loaded
        reason
            Why they could not be loaded, carried into the error each of them raises
        report
            The line describing what just happened, logged above the cumulative list
        """
        self._unavailable_identifiers.update(
            dict.fromkeys(identifiers, reason)
        )
        msg = "\n  ".join([
            report,
            " ".join([
                f"Identifiers skipped so far ({len(self._unavailable_identifiers)}), to run again:",
                ", ".join(self._unavailable_identifiers),
            ]),
        ])
        logging.getLogger(__name__).error(msg)

    @staticmethod
    def _ticker_row_ranges(
        table: pyarrow.Table,
    ) -> dict[str, tuple[int, int]]:
        """
        Map each ticker of a ticker-sorted table to the offset and length of its own stretch of rows.

        Parameters
        ----------
        table
            A table already sorted by ticker

        Returns
        -------
        Each ticker mapped to the (offset, length) of its rows
        """
        # counted rather than looped over, since a batch can hold hundreds of thousands of rows
        counts = pyarrow.compute.value_counts(table.column('ticker'))
        tickers = counts.field('values').to_pylist()
        lengths = counts.field('counts').to_pylist()

        # on a ticker-sorted table the counts come in row order, so their running total is each offset
        offsets = itertools.accumulate(lengths, initial=0)

        return {
            ticker: (offset, length)
            for (ticker, offset, length) in zip(tickers, offsets, lengths, strict=False)
        }

    def _request_table(
        self,
        table: str,
        *,
        identifiers: typing.Sequence[str],
        extra_params: dict[str, str] | None = None,
        filter_dates: bool = True,
    ) -> pyarrow.Table:
        """
        Load a Sharadar table through the query API.

        Parameters
        ----------
        table
            The name of the Sharadar table
        identifiers
            The identifiers to restrict the table to
        extra_params
            Any additional query parameters, like the fundamentals dimension
        filter_dates
            Whether to restrict the table to the configured date range; fundamentals are read from their whole
            history

        Returns
        -------
        The loaded table
        """
        if not filter_dates:
            start_date = None
        elif table == self.FUNDAMENTALS_TABLE:
            start_date = self.FUNDAMENTALS_HISTORY_START_DATE
        else:
            start_date = self._configuration.start_date
        end_date = self._configuration.end_date if filter_dates else None

        return self.client.fetch_table(
            table=table,
            tickers=identifiers,
            start_date=start_date.strftime('%Y-%m-%d') if start_date is not None else None,
            end_date=end_date.strftime('%Y-%m-%d') if end_date is not None else None,
            extra_params=extra_params,
        )
