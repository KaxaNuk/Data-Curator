import datetime
import enum
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
            DividendDataRow.declaration_date: 'declarationDate',
            DividendDataRow.ex_dividend_date: PreprocessedFieldMapping(  # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            DividendDataRow.record_date: 'recordDate',
            DividendDataRow.payment_date: 'paymentDate',
            DividendDataRow.dividend: 'dividend',
            DividendDataRow.dividend_split_adjusted: 'adjDividend',
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

        self._intrinio_sdk.ApiClient().configuration.api_key['api_key']

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
        """

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
