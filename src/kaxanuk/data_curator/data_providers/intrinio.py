import datetime
import enum
import http
import logging
import types
import typing

import intrinio_sdk
import intrinio_sdk.rest
import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
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
    MainIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    ApiEndpointError,
    DataProviderMissingKeyError,
    DataProviderMultiEndpointCommonDataOrderError,
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderMultiEndpointDuplicateKeysError,
    DataProviderMultiEndpointNullColumnsError,
    DataProviderPaymentError,
    DataProviderToolkitNoDataError,
    DataProviderToolkitRuntimeError,
    IdentifierNotFoundError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services.data_provider_toolkit import (
    DataBlockEndpointTagMap,
    DataProviderFieldPreprocessors,
    DataProviderToolkit,
    EndpointFieldMap,
    PreprocessedFieldMapping,
)


class Intrinio(
    DataProviderInterface,      # this is the interface all data providers have to implement
):
    # frequency requested from the stock prices endpoint
    STOCK_PRICE_FREQUENCY: typing.Final = 'daily'
    # max number of records requested per stock prices endpoint page
    STOCK_PRICE_PAGE_SIZE: typing.Final = 10000

    _intrinio_sdk: types.ModuleType = intrinio_sdk

    class Endpoints(enum.StrEnum):
        ACCOUNT = 'AccountApi.get_account_current_usage'
        COMPANY = 'CompanyApi.get_company_fundamentals'
        FUNDAMENTALS = 'FundamentalsApi.get_fundamental_standardized_financials'
        STOCK_DIVIDENDS = 'SecurityApi.get_security_stock_price_adjustments_dividends'
        STOCK_PRICES = 'SecurityApi.get_security_stock_prices'
        STOCK_SPLITS = 'SecurityApi.get_security_stock_price_adjustments_splits'

    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_DIVIDENDS: {
            DividendDataRow.ex_dividend_date: 'date',
            DividendDataRow.dividend: 'dividend',
        },
    }

    _market_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_PRICES: {
            MarketDataDailyRow.date: 'date',
            MarketDataDailyRow.open_dividend_and_split_adjusted: 'adj_open',
            MarketDataDailyRow.high_dividend_and_split_adjusted: 'adj_high',
            MarketDataDailyRow.low_dividend_and_split_adjusted: 'adj_low',
            MarketDataDailyRow.close_dividend_and_split_adjusted: 'adj_close',
            MarketDataDailyRow.volume_dividend_and_split_adjusted: 'adj_volume',
            MarketDataDailyRow.open: 'open',
            MarketDataDailyRow.high: 'high',
            MarketDataDailyRow.low: 'low',
            MarketDataDailyRow.close: 'close',
            MarketDataDailyRow.volume: 'volume',
        },
    }

    def __init__(
        self,
        *,
        api_key: str | None,
    ):
        """
        Initialize the financial data provider, using its API key.

        Parameters
        ----------
        api_key : str | None
            The api key for connecting to the provider
        """
        if (
            api_key is None
            or len(api_key) < 1
        ):
            raise DataProviderMissingKeyError

        self._intrinio_sdk.ApiClient().configuration.api_key['api_key'] = api_key
        self._intrinio_sdk.ApiClient().allow_retries(True)

    @classmethod
    def get_data_block_endpoint_tag_map(cls) -> DataBlockEndpointTagMap:
        return {
            DividendsDataBlock: cls._dividend_data_endpoint_map,
            FundamentalsDataBlock: cls._fundamental_data_endpoint_map,
            MarketDailyDataBlock: cls._market_data_endpoint_map,
            SplitsDataBlock: cls._split_data_endpoint_map,
        }

    def get_dividend_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        """
        Return the dividend data for `main_identifier`.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        start_date
            The start date of the period whose data we're returning
        end_date
            The end date of the period whose data we're returning

        Returns
        -------
        The DividendData entity containing the data
        """
        dividend_records = self._request_dividends(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        dividend_tag_names = list(
            self._dividend_data_endpoint_map[self.Endpoints.STOCK_DIVIDENDS].values()
        )
        endpoint_tables = {
            self.Endpoints.STOCK_DIVIDENDS: self._create_endpoint_table_from_records(
                records=dividend_records,
                tag_names=dividend_tag_names,
            ),
        }
        empty_dividend_data = DividendData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={},
        )

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=DividendsDataBlock,
                endpoint_field_map=self._dividend_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError:
            msg = f"{main_identifier} dividend data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_dividend_data

        consolidated_dividend_data_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[DividendsDataBlock.clock_sync_field],
            predominant_order_descending=True,
        )
        consolidated_dividend_data = consolidated_dividend_data_descending[::-1]
        dividend_data = DividendsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_dividend_data,
            common_field_data={
                DividendData: {
                    DividendData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return dividend_data  # noqa: RET504

    def get_fundamental_data(
        self,
        *,
        main_identifier: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        """
        Return the fundamental data for `main_identifier`.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        period
            The identifier of the type of period we're using
        start_date
            The start date of the period whose data we're returning
        end_date
            The end date of the period whose data we're returning

        Returns
        -------
        The FundamentalData entity containing the data
        """

    def get_market_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        """
        Return the market data for `main_identifier`.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        start_date
            The start date of the period whose data we're returning
        end_date
            The end date of the period whose data we're returning

        Returns
        -------
        The MarketData entity containing the data

        Raises
        ------
        IdentifierNotFoundError
            When the stock prices endpoint returns no data for the identifier
        """
        stock_price_records = self._request_stock_prices(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        stock_price_tag_names = list(
            self._market_data_endpoint_map[self.Endpoints.STOCK_PRICES].values()
        )
        endpoint_tables = {
            self.Endpoints.STOCK_PRICES: self._create_endpoint_table_from_records(
                records=stock_price_records,
                tag_names=stock_price_tag_names,
            ),
        }

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=MarketDailyDataBlock,
                endpoint_field_map=self._market_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError as error:
            msg = f"{main_identifier} market data endpoints returned no data"

            raise IdentifierNotFoundError(msg) from error

        consolidated_market_data_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[MarketDailyDataBlock.clock_sync_field],
            predominant_order_descending=True,
        )
        consolidated_market_data = consolidated_market_data_descending[::-1]
        market_data = MarketDailyDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_market_data,
            common_field_data={
                MarketData: {
                    MarketData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return market_data  # noqa: RET504

    def get_split_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        """
        Return the split data for `main_identifier`.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        start_date
            The start date of the period whose data we're returning
        end_date
            The end date of the period whose data we're returning

        Returns
        -------
        The SplitData entity containing the data
        """

    def initialize(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        """
        Run any required setup logic here before the identifiers' processing loop.

        Parameters
        ----------
        configuration
            The Configuration entity with all the currently injected settings

        Returns
        -------
        None
        """
        ...

    def validate_api_key(
        self,
    ) -> bool | None:
        """
        Validate that the API key used to init the class is valid.

        Returns
        -------
        Whether `api_key` is valid
        """
        try:
            response = self._intrinio_sdk.AccountApi().get_account_current_usage()
            # @todo check that the response is valid

            return True
        except intrinio_sdk.rest.ApiException as error:
            # @todo log problem to logger

            return False

    @staticmethod
    def _create_endpoint_table_from_records(
        *,
        records: list[typing.Any],
        tag_names: list[str],
    ) -> pyarrow.Table:
        """
        Build a PyArrow table from data provider SDK response records.

        Extracts the given provider tags from each record's attributes into a
        row-oriented mapping and lets PyArrow infer each column's type, so the
        resulting table can feed the shared toolkit remapping pipeline.

        Parameters
        ----------
        records
            The SDK model objects returned by an endpoint
        tag_names
            The provider tag names (SDK attribute names) to extract as columns

        Returns
        -------
        pyarrow.Table
            Table whose columns are named by provider tag, empty when there are
            no records
        """
        row_mappings = [
            {
                tag_name: getattr(record, tag_name)
                for tag_name in tag_names
            }
            for record in records
        ]

        return pyarrow.Table.from_pylist(row_mappings)

    def _request_dividends(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[
        intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment
    ]:
        """
        Download every dividend adjustment page for `main_identifier` in the date range.

        Follows the endpoint's `next_page` cursor until it is exhausted,
        accumulating the dividend adjustment records from all pages. The endpoint
        filters by `start_date` and `end_date` server-side, so no further trimming
        is required.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        start_date
            The first date whose dividend adjustments we're requesting
        end_date
            The last date whose dividend adjustments we're requesting

        Returns
        -------
        The accumulated dividend adjustment records across all pages

        Raises
        ------
        IdentifierNotFoundError
            When the endpoint reports the identifier does not exist
        DataProviderPaymentError
            When the endpoint requires a paid plan for the request
        ApiEndpointError
            When the endpoint returns any other API error
        """
        security_api = self._intrinio_sdk.SecurityApi()
        dividend_records = []
        next_page = ''

        while True:
            try:
                response = security_api.get_security_stock_price_adjustments_dividends(
                    main_identifier,
                    start_date=start_date,
                    end_date=end_date,
                    page_size=self.STOCK_PRICE_PAGE_SIZE,
                    next_page=next_page,
                )
            except intrinio_sdk.rest.ApiException as error:
                if error.status == http.HTTPStatus.NOT_FOUND.value:
                    msg = f"Intrinio dividends endpoint could not find identifier {main_identifier}"

                    raise IdentifierNotFoundError(msg) from error

                if error.status == http.HTTPStatus.PAYMENT_REQUIRED.value:
                    msg = f"Intrinio dividends endpoint requires a paid plan for identifier {main_identifier}"

                    raise DataProviderPaymentError(msg) from error

                msg = " ".join([
                    f"Intrinio dividends endpoint returned HTTP status {error.status}",
                    f"for identifier {main_identifier}: {error.reason}",
                ])

                raise ApiEndpointError(msg) from error

            dividend_records.extend(response.stock_price_adjustments)

            if not response.next_page:
                break

            next_page = response.next_page

        return dividend_records

    def _request_stock_prices(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[
        intrinio_sdk.models.stock_price_summary.StockPriceSummary
    ]:
        """
        Download every stock prices page for `main_identifier` in the date range.

        Follows the endpoint's `next_page` cursor until it is exhausted,
        accumulating the daily stock price records from all pages.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        start_date
            The first date whose prices we're requesting
        end_date
            The last date whose prices we're requesting

        Returns
        -------
        The accumulated stock price records across all pages

        Raises
        ------
        IdentifierNotFoundError
            When the endpoint reports the identifier does not exist
        DataProviderPaymentError
            When the endpoint requires a paid plan for the request
        ApiEndpointError
            When the endpoint returns any other API error
        """
        security_api = self._intrinio_sdk.SecurityApi()
        stock_price_records = []
        next_page = ''

        while True:
            try:
                response = security_api.get_security_stock_prices(
                    main_identifier,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=self.STOCK_PRICE_FREQUENCY,
                    page_size=self.STOCK_PRICE_PAGE_SIZE,
                    next_page=next_page,
                )
            except intrinio_sdk.rest.ApiException as error:
                if error.status == http.HTTPStatus.NOT_FOUND.value:
                    msg = f"Intrinio stock prices endpoint could not find identifier {main_identifier}"

                    raise IdentifierNotFoundError(msg) from error

                if error.status == http.HTTPStatus.PAYMENT_REQUIRED.value:
                    msg = f"Intrinio stock prices endpoint requires a paid plan for identifier {main_identifier}"

                    raise DataProviderPaymentError(msg) from error

                msg = " ".join([
                    f"Intrinio stock prices endpoint returned HTTP status {error.status}",
                    f"for identifier {main_identifier}: {error.reason}",
                ])

                raise ApiEndpointError(msg) from error

            stock_price_records.extend(response.stock_prices)

            if not response.next_page:
                break

            next_page = response.next_page

        return stock_price_records
