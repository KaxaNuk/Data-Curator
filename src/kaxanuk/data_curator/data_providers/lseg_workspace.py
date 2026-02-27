"""
LSEG/Refinitiv Workspace data provider implementation.

This module provides integration with the LSEG (London Stock Exchange Group)
Refinitiv Workspace API for fetching market data, fundamental data, dividend
data, and split data for financial instruments.

The implementation uses a bulk-fetch-first strategy with adaptive chunking
to handle API limitations efficiently.
"""

import dataclasses
import datetime
import enum
import logging
import re
import time
import typing
import warnings

import httpx
import pandas
import pyarrow
import pyarrow.compute
import refinitiv.data
from refinitiv.data.errors import RDError

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
    DataProviderMissingKeyError,
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderMultiEndpointCommonDataOrderError,
    DataProviderToolkitNoDataError,
    LSEGAuthenticationError,
    LSEGAuthorizationError,
    LSEGDataNotFoundError,
    LSEGFatalError,
    LSEGGatewayTimeoutError,
    LSEGPlatformOverloadError,
    LSEGProviderError,
    LSEGRateLimitError,
    LSEGServerError,
    LSEGSessionQuotaError,
)
from kaxanuk.data_curator.services.data_provider_toolkit import (
    DataBlockEndpointTagMap,
    DataProviderFieldPreprocessors,
    DataProviderToolkit,
    EndpointFieldMap,
    PreprocessedFieldMapping,
)

logger = logging.getLogger(__name__)

class ColumnNames(enum.StrEnum):
    """
    Constants for LSEG API response column names.

    Centralizes all magic strings for column names to improve maintainability
    and reduce errors from typos.
    """

    # Common columns
    INSTRUMENT = 'Instrument'
    DATE = 'Date'

    # Price columns (unadjusted)
    OPEN_PRICE = 'Open Price'
    HIGH_PRICE = 'High Price'
    LOW_PRICE = 'Low Price'
    CLOSE_PRICE = 'Close Price'
    VOLUME = 'Volume'
    VWAP = 'VWAP'

    # Price columns (split-adjusted suffix)
    OPEN_PRICE_SPLIT = 'Open Price_split'
    HIGH_PRICE_SPLIT = 'High Price_split'
    LOW_PRICE_SPLIT = 'Low Price_split'
    CLOSE_PRICE_SPLIT = 'Close Price_split'

    # Price columns (dividend+split-adjusted suffix)
    OPEN_PRICE_DIV_SPLIT = 'Open Price_div_split'
    HIGH_PRICE_DIV_SPLIT = 'High Price_div_split'
    LOW_PRICE_DIV_SPLIT = 'Low Price_div_split'
    CLOSE_PRICE_DIV_SPLIT = 'Close Price_div_split'

    # Fundamental data columns
    PERIOD_END_DATE = 'Period End Date'
    ORIGINAL_ANNOUNCEMENT = 'Original Announcement Date Time'
    FISCAL_YEAR = 'Fiscal Year'
    FISCAL_PERIOD = 'Fiscal Period'
    CURRENCY = 'Currency'

    # Dividend columns
    DIVIDEND_EX_DATE = 'Dividend Ex Date'
    DIVIDEND_PAY_DATE = 'Dividend Pay Date'
    DIVIDEND_RECORD_DATE = 'Dividend Record Date'
    DIVIDEND_ANNOUNCEMENT_DATE = 'Dividend Announcement Date'
    GROSS_DIVIDEND_AMOUNT = 'Gross Dividend Amount'
    ADJUSTED_GROSS_DIVIDEND = 'Adjusted Gross Dividend Amount'

    # Split columns
    CAPITAL_CHANGE_EX_DATE = 'Capital Change Ex Date'
    TERMS_NEW_SHARES = 'Terms New Shares'
    TERMS_OLD_SHARES = 'Terms Old Shares'

    # Currency field
    CF_CURR = 'CF_CURR'

class LsegWorkspace(DataProviderInterface):
    """
    LSEG/Refinitiv Workspace data provider for financial market data.

    This class implements the DataProviderInterface for fetching market data,
    fundamental data, dividend data, and split data from the LSEG Refinitiv
    Workspace API.

    Data Fetching Strategy
    ----------------------
    Market, dividend, and split data use a three-step bulk pipeline
    (see ``_fetch_bulk_data``):

    1. Attempt a single bulk request for all tickers.
    2. If the bulk request fails or returns incomplete data, partition
       the failed RICs into mini-bulk batches (``MINI_BULK_BATCH_SIZE``,
       default 5, capped at ``MINI_BULK_MAX_BATCH_SIZE`` = 10) and
       retry each batch sequentially.
    3. Any RICs still unresolved after step 2 are retried one final
       time in mini-bulk batches.

    Fundamental data is fetched in field batches ("buckets") to stay
    within API field-count limits.

    Error Handling
    --------------
    - Fatal errors (RDError code 207, PERIOD unrecognized) abort immediately
    - Non-fatal errors trigger retry up to MAX_FETCH_ATTEMPTS times
    - After retries exhausted, unresolved tickers are collected and
      retried through the mini-bulk pipeline described above

    Parameters
    ----------
    api_key : str | None
        Not used for LSEG (authentication via Workspace session).
        Kept for interface compatibility.

    Attributes
    ----------
    cache : TickerCacheType
        Dictionary mapping ticker symbols to their cached data endpoints.

    Examples
    --------
    >>> provider = LsegWorkspace(api_key=None)
    >>> provider.initialize(configuration=config)
    >>> market_data = provider.get_market_data(
    ...     main_identifier='AAPL.OQ',
    ...     start_date=datetime.date(2020, 1, 1),
    ...     end_date=datetime.date(2024, 12, 31)
    ... )
    """

    # ========================================================================
    # Configuration Constants
    # ========================================================================

    #: Maximum number of retry attempts for API requests
    MAX_FETCH_ATTEMPTS: typing.Final[int] = 1

    #: Maximum number of partitioned retry rounds for failed tickers after bulk attempt
    MAX_PARTITION_ROUNDS: typing.Final[int] = 3

    #: Maximum number of fundamental fields per API request
    FUNDAMENTAL_BATCH_SIZE: typing.Final[int] = 30

    #: Default currency when API doesn't return currency info
    DEFAULT_CURRENCY: typing.Final[str] = 'USD'

    #: HTTP 429 Too Many Requests - API rate limit exceeded, retry with backoff
    RATE_LIMIT_ERROR_CODE: typing.Final[int] = 429

    #: HTTP 400 Bad Request - needs sub-classification by message content
    BAD_REQUEST_ERROR_CODE: typing.Final[int] = 400

    #: HTTP 401 Unauthorized - authentication failure, token refresh needed
    AUTHENTICATION_ERROR_CODE: typing.Final[int] = 401

    #: HTTP 403 Forbidden - authorization failure, abort immediately
    AUTHORIZATION_ERROR_CODE: typing.Final[int] = 403

    #: HTTP 503 Service Unavailable - treat like rate limit
    SERVICE_UNAVAILABLE_ERROR_CODE: typing.Final[int] = 503

    #: Error code 207: "Field not recognized" - malformed TR field syntax, retry won't help
    FIELD_NOT_RECOGNIZED_ERROR_CODE: typing.Final[int] = 207

    #: Error code 2504: LSEG gateway timeout - triggers ticker chunking
    GATEWAY_TIMEOUT_ERROR_CODE: typing.Final[int] = 2504

    #: Number of tickers to process through the full pipeline per chunk.
    #: Prevents OOM on large ticker lists by downloading, processing, and
    #: saving each chunk before moving to the next.
    PIPELINE_CHUNK_SIZE: typing.Final[int] = 5

    pipeline_chunk_size: int = PIPELINE_CHUNK_SIZE

    #: Default number of tickers per mini-bulk batch when partitioning a failed bulk request
    MINI_BULK_BATCH_SIZE: typing.Final[int] = 5

    #: Hard cap on the mini-bulk batch size (user-supplied values are clamped to this)
    MINI_BULK_MAX_BATCH_SIZE: typing.Final[int] = 10

    #: Maximum number of tickers per grid cell in fundamental data fallback
    FUNDAMENTAL_GRID_TICKER_SIZE: typing.Final[int] = 3

    #: Maximum number of fields per grid cell in fundamental data fallback
    FUNDAMENTAL_GRID_FIELD_SIZE: typing.Final[int] = 15

    # PERIOD in message: unrecognized period parameter (e.g. invalid Period=FQ0), retry won't help
    FATAL_ERROR_MESSAGES: typing.Final[tuple[str, ...]] = ('PERIOD',)

    # Regex patterns for sub-classifying HTTP 400 errors
    _BACKEND_ERROR_PATTERN: typing.Final[re.Pattern[str]] = re.compile(
        r'backend\s+error', re.IGNORECASE,
    )
    _AUTH_ERROR_PATTERN: typing.Final[re.Pattern[str]] = re.compile(
        r'invalid_grant|invalid_client', re.IGNORECASE,
    )
    _SESSION_QUOTA_PATTERN: typing.Final[re.Pattern[str]] = re.compile(
        r'session\s+quota', re.IGNORECASE,
    )

    class Endpoints(enum.StrEnum):

        FUNDAMENTAL_DATA = "fundamental_data"

        MARKET_DATA_DAILY_UNADJUSTED = "market_data_daily_unadjusted"

        MARKET_DATA_DAILY_SPLIT_ADJUSTED = "market_data_daily_split_adjusted"

        MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED = "market_data_daily_dividend_and_split_adjusted"

        STOCK_DIVIDEND = "stock_dividend"

        STOCK_SPLIT = "stock_split"

    _market_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.MARKET_DATA_DAILY_UNADJUSTED: {
            MarketDataDailyRow.date: ColumnNames.DATE,
            MarketDataDailyRow.open: ColumnNames.OPEN_PRICE,
            MarketDataDailyRow.high: ColumnNames.HIGH_PRICE,
            MarketDataDailyRow.low: ColumnNames.LOW_PRICE,
            MarketDataDailyRow.close: ColumnNames.CLOSE_PRICE,
            MarketDataDailyRow.volume: ColumnNames.VOLUME,
            MarketDataDailyRow.vwap: ColumnNames.VWAP,
        },
        Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED: {
            MarketDataDailyRow.date: ColumnNames.DATE,
            # Note: Split-adjusted prices use Adjusted=1 which only adjusts for splits
            # The column names with _split suffix are added during initialize()
            MarketDataDailyRow.open_split_adjusted: ColumnNames.OPEN_PRICE_SPLIT,
            MarketDataDailyRow.high_split_adjusted: ColumnNames.HIGH_PRICE_SPLIT,
            MarketDataDailyRow.low_split_adjusted: ColumnNames.LOW_PRICE_SPLIT,
            MarketDataDailyRow.close_split_adjusted: ColumnNames.CLOSE_PRICE_SPLIT,
            MarketDataDailyRow.volume_split_adjusted: ColumnNames.VOLUME,
            MarketDataDailyRow.vwap_split_adjusted: ColumnNames.VWAP,
        },
        Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED: {
            MarketDataDailyRow.date: ColumnNames.DATE,
            # Note: Dividend+split adjusted prices are calculated using dividend data
            # The column names with _div_split suffix are added during initialize()
            MarketDataDailyRow.open_dividend_and_split_adjusted: ColumnNames.OPEN_PRICE_DIV_SPLIT,
            MarketDataDailyRow.high_dividend_and_split_adjusted: ColumnNames.HIGH_PRICE_DIV_SPLIT,
            MarketDataDailyRow.low_dividend_and_split_adjusted: ColumnNames.LOW_PRICE_DIV_SPLIT,
            MarketDataDailyRow.close_dividend_and_split_adjusted: ColumnNames.CLOSE_PRICE_DIV_SPLIT,
            MarketDataDailyRow.volume_dividend_and_split_adjusted: ColumnNames.VOLUME,
            MarketDataDailyRow.vwap_dividend_and_split_adjusted: ColumnNames.VWAP,
        },
    }

    # Dividend data endpoint map
    # Maps KaxaNuk entity fields to LSEG API response column names (verified)
    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_DIVIDEND: {
            DividendDataRow.ex_dividend_date: ColumnNames.DIVIDEND_EX_DATE,
            DividendDataRow.payment_date: ColumnNames.DIVIDEND_PAY_DATE,
            DividendDataRow.record_date: ColumnNames.DIVIDEND_RECORD_DATE,
            DividendDataRow.declaration_date: ColumnNames.DIVIDEND_ANNOUNCEMENT_DATE,
            DividendDataRow.dividend: ColumnNames.GROSS_DIVIDEND_AMOUNT,
            DividendDataRow.dividend_split_adjusted: ColumnNames.ADJUSTED_GROSS_DIVIDEND,
        },
    }

    # Split data endpoint map
    # Maps KaxaNuk entity fields to LSEG API response column names (verified)
    _split_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_SPLIT: {
            SplitDataRow.split_date: ColumnNames.CAPITAL_CHANGE_EX_DATE,
            SplitDataRow.numerator: ColumnNames.TERMS_NEW_SHARES,
            SplitDataRow.denominator: ColumnNames.TERMS_OLD_SHARES,
        },
    }

    # Fundamental data endpoint map
    # Maps KaxaNuk entity fields to LSEG API response column names
    # Field mappings derived from KaxaNuk_Tags Excel file
    _fundamental_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.FUNDAMENTAL_DATA: {
            # ================================================================
            # Row-level fields
            # ================================================================
            FundamentalDataRow.filing_date: PreprocessedFieldMapping(
                [ColumnNames.ORIGINAL_ANNOUNCEMENT],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.period_end_date: PreprocessedFieldMapping(
                [ColumnNames.PERIOD_END_DATE],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            # These fields are derived from Period End Date since the LSEG API doesn't provide them
            FundamentalDataRow.fiscal_period: ColumnNames.FISCAL_PERIOD,
            FundamentalDataRow.fiscal_year: ColumnNames.FISCAL_YEAR,
            # FundamentalDataRow.accepted_date:
            #     'PLACEHOLDER_ACCEPTED_DATE',  #@todo: look up the LSEG Workspace field name
            FundamentalDataRow.reported_currency: ColumnNames.CURRENCY,

            # ================================================================
            # Income Statement fields
            # Column names verified against actual LSEG API response
            # ================================================================
            FundamentalDataRowIncomeStatement.basic_earnings_per_share:
                'EPS - Basic - incl Extraordinary Items, Common - Total',
            FundamentalDataRowIncomeStatement.diluted_earnings_per_share:
                'EPS - Diluted - incl Extraordinary Items, Common - Total',
            FundamentalDataRowIncomeStatement.basic_net_income_available_to_common_stockholders:
                'Income Available to Common Shares',
            FundamentalDataRowIncomeStatement.net_income:
                'Net Income after Tax',
            FundamentalDataRowIncomeStatement.net_interest_income:
                'Interest & Dividend Income/(Expense) - Net - Finance',
            FundamentalDataRowIncomeStatement.operating_income:
                'Operating Profit before Non-Recurring Income/Expense',
            FundamentalDataRowIncomeStatement.earnings_before_interest_and_tax:
                'Earnings before Interest & Taxes (EBIT)',
            FundamentalDataRowIncomeStatement.earnings_before_interest_tax_depreciation_and_amortization:
                'Earnings before Interest Taxes Depreciation & Amortization',
            FundamentalDataRowIncomeStatement.revenues:
                'Revenue from Business Activities - Total',
            FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding:
                'Shares used to calculate Basic EPS - Total',
            # Additional Income Statement fields from KaxaNuk_Tags
            FundamentalDataRowIncomeStatement.continuing_operations_income_after_tax:
                'Normalized Net Income from Continuing Operations',
            FundamentalDataRowIncomeStatement.costs_and_expenses:
                'Operating Expenses - Total',
            FundamentalDataRowIncomeStatement.cost_of_revenue:
                'Cost of Operating Revenue',
            FundamentalDataRowIncomeStatement.depreciation_and_amortization:
                'Depreciation & Amortization',
            FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax:
                'Discontinued Operations Net - Total - Income/(Expense)',
            FundamentalDataRowIncomeStatement.general_and_administrative_expense:
                'Selling General & Administrative Expenses',
            FundamentalDataRowIncomeStatement.gross_profit:
                'Gross Revenue from Business Activities - Total',
            FundamentalDataRowIncomeStatement.income_before_tax:
                'Income before Taxes',
            FundamentalDataRowIncomeStatement.income_tax_expense:
                'Excise Tax Expense',
            FundamentalDataRowIncomeStatement.interest_expense:
                'Interest Expense',
            FundamentalDataRowIncomeStatement.interest_income:
                'Interest & Dividend Income - Finance - Total',
            FundamentalDataRowIncomeStatement.net_income_deductions:
                'Earnings Adjustments to Net Income - Other Expense/(Income)',
            FundamentalDataRowIncomeStatement.net_total_other_income:
                'Other Non-Operating Income/(Expense) - Total',
            FundamentalDataRowIncomeStatement.operating_expenses:
                'Operating Expenses',
            FundamentalDataRowIncomeStatement.other_expenses:
                'Other Operating Expense',
            FundamentalDataRowIncomeStatement.other_net_income_adjustments:
                'Adjustments to Net Income - Other',
            FundamentalDataRowIncomeStatement.research_and_development_expense:
                'Research & Development Expense',
            FundamentalDataRowIncomeStatement.selling_and_marketing_expense:
                'Advertising Expense',
            FundamentalDataRowIncomeStatement.selling_general_and_administrative_expense:
                'Selling General & Administrative Expenses - Total',
            FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding:
                'Shares used to calculate Diluted EPS - Total',
            # FundamentalDataRowIncomeStatement.nonoperating_income_excluding_interest:
            #     'PLACEHOLDER_NONOPERATING_INCOME_EXCL_INTEREST',  #@todo: look up the LSEG Workspace field name

            # ================================================================
            # Balance Sheet fields
            # Column names verified against actual LSEG API response
            # ================================================================
            FundamentalDataRowBalanceSheet.assets:
                'Total Assets',
            FundamentalDataRowBalanceSheet.liabilities:
                'Total Liabilities',
            FundamentalDataRowBalanceSheet.stockholder_equity:
                'Common Equity - Total',  # TR.F.ComEqTot
            FundamentalDataRowBalanceSheet.retained_earnings:
                'Retained Earnings - Total',  # TR.F.RetainedEarnTot
            FundamentalDataRowBalanceSheet.current_assets:
                'Total Current Assets',
            FundamentalDataRowBalanceSheet.current_liabilities:
                'Total Current Liabilities',
            FundamentalDataRowBalanceSheet.total_debt_including_capital_lease_obligations:
                'Debt - Total',
            FundamentalDataRowBalanceSheet.net_debt:
                'Net Debt',
            FundamentalDataRowBalanceSheet.longterm_debt:
                'Debt - Long-Term - Total',
            FundamentalDataRowBalanceSheet.preferred_stock_value:
                'Preferred Shareholders Equity',
            # Additional Balance Sheet fields from KaxaNuk_Tags
            FundamentalDataRowBalanceSheet.accumulated_other_comprehensive_income_after_tax:
                'Comprehensive Income - Accumulated - Total',
            FundamentalDataRowBalanceSheet.additional_paid_in_capital:
                'Common Stock - Additional Paid in Capital',
            FundamentalDataRowBalanceSheet.capital_lease_obligations:
                'Capitalized Lease Obligations - Long-Term',
            FundamentalDataRowBalanceSheet.cash_and_cash_equivalents:
                'Cash & Cash Equivalents - Total',
            FundamentalDataRowBalanceSheet.cash_and_shortterm_investments:
                'Cash & Short Term Investments - Total',
            FundamentalDataRowBalanceSheet.common_stock_value:
                'Common Equity - Total',
            FundamentalDataRowBalanceSheet.current_accounts_payable:
                'Trade Accounts Payable & Accruals - Short-Term',
            FundamentalDataRowBalanceSheet.current_accounts_receivable_after_doubtful_accounts:
                'Accounts & Notes Receivable - Trade - Net',
            FundamentalDataRowBalanceSheet.current_accrued_expenses:
                'Accrued Expenses',
            FundamentalDataRowBalanceSheet.current_capital_lease_obligations:
                'Capitalized Leases - Current Portion',
            FundamentalDataRowBalanceSheet.current_net_receivables:
                'Loans & Receivables - Net - Short-Term',
            FundamentalDataRowBalanceSheet.current_tax_payables:
                'Income Taxes - Payable - Short-Term',
            FundamentalDataRowBalanceSheet.deferred_revenue:
                'Deferred Revenue - Long-Term',
            FundamentalDataRowBalanceSheet.goodwill:
                'Goodwill - Net',
            FundamentalDataRowBalanceSheet.investments:
                'Investments - Total',
            FundamentalDataRowBalanceSheet.longterm_investments:
                'Investments - Long-Term',
            FundamentalDataRowBalanceSheet.net_intangible_assets_excluding_goodwill:
                'Intangible Assets - excluding Goodwill - Net - Total',
            FundamentalDataRowBalanceSheet.net_intangible_assets_including_goodwill:
                'Intangible Assets - Total - Net',
            FundamentalDataRowBalanceSheet.net_inventory:
                'Inventories - Total', #@todo: check if the field is TR.F.InvntTot or TR.F.InvntTotToAdjAvgNetOpAssets
            FundamentalDataRowBalanceSheet.net_property_plant_and_equipment:
                'Property Plant & Equipment - Net - Total',
            FundamentalDataRowBalanceSheet.noncontrolling_interest:
                'Minority Interests/Non-Controlling Interests - Total',
            FundamentalDataRowBalanceSheet.noncurrent_assets:
                'Total Non-Current Assets',
            FundamentalDataRowBalanceSheet.noncurrent_capital_lease_obligations:
                'Capital Lease Obligations - Long-Term',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_revenue:
                'Deferred Revenue - Long-Term',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_assets:
                'Deferred Tax - Asset - Long-Term',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_liabilities:
                'Deferred Tax Liabilities - Long-Term',
            FundamentalDataRowBalanceSheet.noncurrent_liabilities:
                'Total Non-Current Liabilities',
            FundamentalDataRowBalanceSheet.other_assets:
                'Other Assets - Total',
            FundamentalDataRowBalanceSheet.other_current_assets:
                'Other Current Assets - Total',
            FundamentalDataRowBalanceSheet.other_current_liabilities:
                'Other Current Liabilities - Total',
            FundamentalDataRowBalanceSheet.other_liabilities:
                'Other Liabilities - Total',
            FundamentalDataRowBalanceSheet.other_noncurrent_assets:
                'Other Non-Current Assets - Total',
            FundamentalDataRowBalanceSheet.other_noncurrent_liabilities:
                'Other Non-Current Liabilities - Total',
            FundamentalDataRowBalanceSheet.other_payables:
                'Other Payables - Total',
            FundamentalDataRowBalanceSheet.other_receivables:
                'Receivables - Other - Total',
            FundamentalDataRowBalanceSheet.other_stockholder_equity:
                'Equity - Other',
            FundamentalDataRowBalanceSheet.prepaid_expenses:
                'Prepaid Expenses - Total',
            FundamentalDataRowBalanceSheet.shortterm_debt:
                'Short-Term Debt - Financial Sector - Total',
            FundamentalDataRowBalanceSheet.shortterm_investments:
                'Short-Term Investments - Total',
            FundamentalDataRowBalanceSheet.total_equity_including_noncontrolling_interest:
                'Total Shareholders Equity',
            FundamentalDataRowBalanceSheet.total_liabilities_and_equity:
                'Total Liabilities & Equity',
            FundamentalDataRowBalanceSheet.total_payables_current_and_noncurrent:
                'Accounts Payable',
            FundamentalDataRowBalanceSheet.treasury_stock_value:
                'Common Shares - Treasury - Total',

            # ================================================================
            # Cash Flow fields
            # Column names verified against actual LSEG API response
            # ================================================================
            FundamentalDataRowCashFlow.free_cash_flow:
                'Non-GAAP Free Cash Flow - Company Reported',
            FundamentalDataRowCashFlow.net_cash_from_operating_activities:
                'Net Cash Flow from Operating Activities',
            FundamentalDataRowCashFlow.common_stock_repurchase:
                'Common Stock Buyback - Net',
            # Additional Cash Flow fields from KaxaNuk_Tags
            FundamentalDataRowCashFlow.accounts_payable_change:
                'Accounts Payable - Increase/(Decrease) - Cash Flow',
            FundamentalDataRowCashFlow.accounts_receivable_change:
                'Accounts Receivables - Decrease/(Increase) - Cash Flow',
            FundamentalDataRowCashFlow.capital_expenditure:
                'Capital Expenditures - Net - Cash Flow',
            FundamentalDataRowCashFlow.cash_and_cash_equivalents_change:
                'Net Change in Cash - Total',
            FundamentalDataRowCashFlow.cash_exchange_rate_effect:
                'Foreign Exchange Effects - Cash Flow',
            FundamentalDataRowCashFlow.common_stock_dividend_payments:
                'Dividends - Common - Cash Paid',
            FundamentalDataRowCashFlow.common_stock_issuance_proceeds:
                'Stock - Common - Issued/Sold - Cash Flow',
            FundamentalDataRowCashFlow.deferred_income_tax:
                'Deferred Inc Taxes & Income Tax Credits - CF - to Reconcile',
            FundamentalDataRowCashFlow.dividend_payments:
                'Dividends Paid - Cash - Total - Cash Flow',
            FundamentalDataRowCashFlow.interest_payments:
                'Interest Paid - Cash',
            FundamentalDataRowCashFlow.inventory_change:
                'Inventories - Decrease/(Increase) - Cash Flow',
            FundamentalDataRowCashFlow.investment_sales_maturities_and_collections_proceeds:
                'Investment Securities - Sold/Matured - Unclassified - CF',
            FundamentalDataRowCashFlow.investments_purchase:
                'Investment Securities - Purchased - Unclassified - Cash Flow',
            FundamentalDataRowCashFlow.net_business_acquisition_payments:
                'Acquisition of Business - Cash Flow',
            FundamentalDataRowCashFlow.net_cash_from_investing_activites:
                'Net Cash Flow from Investing Activities',
            FundamentalDataRowCashFlow.net_cash_from_financing_activities:
                'Net Cash Flow from Financing Activities',
            FundamentalDataRowCashFlow.net_common_stock_issuance_proceeds:
                'Stock - Common - Issuance/(Retirement) - Net - Cash Flow',
            FundamentalDataRowCashFlow.net_debt_issuance_proceeds:
                'Debt - Issued - Long-Term & Short-Term - Cash Flow',
            FundamentalDataRowCashFlow.net_income:
                'Profit/(Loss) - Starting Line - Cash Flow',
            FundamentalDataRowCashFlow.net_income_tax_payments:
                'Income Taxes - Paid/(Reimbursed) - Cash Flow',
            FundamentalDataRowCashFlow.net_longterm_debt_issuance_proceeds:
                'Debt - Issued - Long-Term - Cash Flow',
            FundamentalDataRowCashFlow.net_shortterm_debt_issuance_proceeds:
                'Debt - Issued - Short-Term - Cash Flow',
            FundamentalDataRowCashFlow.net_stock_issuance_proceeds:
                'Stock - Common Preferred & Other - Issued/Sold - Cash Flow',
            FundamentalDataRowCashFlow.other_financing_activities:
                'Other Financing Cash Flow - Increase/(Decrease)',
            FundamentalDataRowCashFlow.other_investing_activities:
                'Other Investing Cash Flow - Decrease/(Increase)',
            FundamentalDataRowCashFlow.other_noncash_items:
                'Other Non-Cash Items & adjustments - CF - to Reconcile',
            FundamentalDataRowCashFlow.other_working_capital:
                'Other Assets & Liabilities - Increase/(Decrease) - Net - CF',
            FundamentalDataRowCashFlow.period_end_cash:
                'Net Cash - Ending Balance',
            FundamentalDataRowCashFlow.period_start_cash:
                'Net Cash - Beginning Balance',
            FundamentalDataRowCashFlow.preferred_stock_dividend_payments:
                'Dividends - Preferred - Cash Paid',
            FundamentalDataRowCashFlow.preferred_stock_issuance_proceeds:
                'Stock - Preferred - Issued/Sold - Cash Flow',
            FundamentalDataRowCashFlow.property_plant_and_equipment_purchase:
                'Property Plant & Equipment - Purchased - Cash Flow',
            FundamentalDataRowCashFlow.stock_based_compensation:
                'Share Based Payments - Cash Flow - to Reconcile',
            FundamentalDataRowCashFlow.working_capital_change:
                'Working Capital - Increase/(Decrease) - Cash Flow',
            FundamentalDataRowCashFlow.depreciation_and_amortization:
                'Depreciation Depletion & Amortization - Cash Flow',  #@todo: look up the LSEG Workspace field name
        },
    }

    TickerCacheType = dict[str, dict['Endpoints', pandas.DataFrame]]

    # Class-level shared cache to avoid redundant initialization
    # when multiple instances are created for the same provider class
    # (e.g. separate market and fundamental provider instances)
    _shared_cache: typing.ClassVar[dict] = {}
    _shared_cache_config_key: typing.ClassVar[tuple | None] = None
    _shared_refetch_key: typing.ClassVar[tuple | None] = None

    @dataclasses.dataclass(frozen=True, slots=True)
    class RawDataBundle:
        """
        Container for all raw data fetched from LSEG API.

        This class holds the raw DataFrames returned by the API before
        per-ticker processing.

        Attributes
        ----------
        market : pandas.DataFrame
            Raw market data for all tickers.
        fundamental : pandas.DataFrame
            Raw fundamental data for all tickers.
        dividend : pandas.DataFrame
            Raw dividend data for all tickers.
        split : pandas.DataFrame
            Raw split data for all tickers.
        currency_map : dict[str, str]
            Mapping of ticker symbols to their reporting currencies.
        """
        #@todo: add validations in post init? I guess so
        market: pandas.DataFrame
        fundamental: pandas.DataFrame
        dividend: pandas.DataFrame
        split: pandas.DataFrame
        currency_map: dict[str, str]

    # Market data fields list
    # Fetches both unadjusted (Adjusted=0) and split-adjusted (Adjusted=1) prices
    # Note: Adjusted=1 only adjusts for splits, NOT dividends
    MARKET_DATA_FIELDS_LIST: typing.Final[list[str]] = [
        # Date field (using close price date)
        r'TR.CLOSEPRICE(SDate={start_date},Frq=D,EDate={end_date}).date',
        # Unadjusted prices (Adjusted=0)
        r'TR.OPENPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=0)',
        r'TR.HIGHPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=0)',
        r'TR.LOWPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=0)',
        r'TR.CLOSEPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=0)',
        # Split-adjusted prices (Adjusted=1) - only split adjusted, NOT dividend adjusted
        r'TR.OPENPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=1)',
        r'TR.HIGHPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=1)',
        r'TR.LOWPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=1)',
        r'TR.CLOSEPRICE(SDate={start_date},Frq=D,EDate={end_date},Adjusted=1)',
        # Volume and VWAP
        r'TR.Volume(SDate={start_date},Frq=D,EDate={end_date})',
        r'TR.TSVWAP(SDate={start_date},Frq=D,EDate={end_date})',
    ]

    # Dividend data field list
    # TR fields for fetching dividend history
    DIVIDEND_FIELDS_LIST: typing.Final[list[str]] = [
        r'TR.DivExDate(SDate={start_date},EDate={end_date})',
        r'TR.DivPayDate(SDate={start_date},EDate={end_date})',
        r'TR.DivRecordDate(SDate={start_date},EDate={end_date})',
        r'TR.DivAnnouncementDate(SDate={start_date},EDate={end_date})',
        r'TR.DivUnadjustedGross(SDate={start_date},EDate={end_date})',
        r'TR.DivAdjustedGross(SDate={start_date},EDate={end_date})',
    ]

    # Split data field list (Corporate Actions)
    # CAEventType=SSP filters for Stock Splits only
    SPLIT_FIELDS_LIST: typing.Final[list[str]] = [
        r'TR.CAExDate(SDate={start_date},EDate={end_date},CAEventType=SSP)',
        r'TR.CATermsNewShares(SDate={start_date},EDate={end_date},CAEventType=SSP)',
        r'TR.CATermsOldShares(SDate={start_date},EDate={end_date},CAEventType=SSP)',
    ]

    # Fundamental data field lists with LSEG TR field codes
    # Parameters are embedded in field names as required by rd.get_data()
    # Field codes derived from KaxaNuk_Tags Excel file
    INCOME_STATEMENT_FIELDS_LIST: typing.Final[list[str]] = [
        # Row-level fields
        r'TR.F.OriginalAnnouncementDate(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.PeriodEndDate(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # r'TR.F.PLACEHOLDER_ACCEPTED_DATE(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # Note: Fiscal Period, Fiscal Year, and Currency are not available via this API
        # Core income statement fields
        r'TR.F.EBIT(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.EBITDA(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.EPSBasicInclExordItemsComTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.EPSDilInclExordItemsComTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IncAvailToComShr(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetIncAfterTax(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IntrDivIncExpnNetFin(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OpProfBefNonRecurIncExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotRevenue(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ShrUsedToCalcBasicEPSTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # Additional income statement fields from KaxaNuk_Tags
        r'TR.F.NormNetIncContOps(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OpExpnTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.CostOfOpRev(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DeprAmort(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DiscOpsNetOfTaxTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.SGA(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.GrossTotRevBizActiv(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IncBefTax(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ExciseTaxExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IntrExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IntrDivIncFinTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.EarnAdjToNetIncOthExpnInc(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthNonOpIncExpnTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OpExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthOpExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AdjToNetIncOth(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.RnD(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AdExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.SGATot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ShrUsedToCalcDilEPSTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # r'TR.F.PLACEHOLDER_NONOP_INC_EXCL_INTEREST(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
    ]

    BALANCE_SHEET_FIELDS_LIST: typing.Final[list[str]] = [
        # Core balance sheet fields
        r'TR.F.TotAssets(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotLiab(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ComEqTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.RetainedEarnTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotCurrAssets(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotCurrLiab(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DebtTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetDebt(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DebtLTTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # TR.F.TotCapital removed - field not available in Refinitiv API
        r'TR.F.PrefShHoldEq(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # Additional balance sheet fields from KaxaNuk_Tags
        r'TR.F.ComprIncAccumTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ComStockShrPrem(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.CapLeaseObligLT(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.CashCashEquivTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.CashSTInvstTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TradeAcctPbleAccrualsST(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AcctNotesRcvblTradeNet(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AccrExpn(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.CapLeaseCurrPort(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.LoansRcvblNetST(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IncTaxPbleST(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DefRevLT(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.GoodwNet(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.InvstTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.InvstLT(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IntangExclGoodwNetTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IntangTotNet(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.InvntTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.PPENetTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.MinIntrNonCtrlIntrTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotNonCurrAssets(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DefTaxAssetLT(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DefTaxLiabLT(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotNonCurrLiab(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthAssetsTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthCurrAssetsTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthCurrLiabTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthLiabTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthNonCurrAssetsTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthNonCurrLiabTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthPbleTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.RcvblOthTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.EqOth(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.PrepaidExpnTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.STDebtFinlSectTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.STInvstTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ShHoldEqParentShHoldTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotShHoldEq(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.TotLiabEq(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AcctPble(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ComShrTrezTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
    ]

    CASH_FLOW_FIELDS_LIST: typing.Final[list[str]] = [
        # Core cash flow fields
        r'TR.F.NonGAAPFreeCashFlow(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetCashFlowOp(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.StockComRepurchRetiredCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        # Additional cash flow fields from KaxaNuk_Tags
        r'TR.F.AcctPbleCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AcctRcvblCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.CAPEXNetCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetChgInCashTot(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.FXEffectsCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DivComCashPaid(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.StockComIssuedSoldCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DefIncTaxIncTaxCreditsCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DivPaidCashTotCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IntrPaidCash(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.InvntCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.InvstSecSoldMaturedCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.InvstSecPurchCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.AcqOfBizCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetCashFlowInvst(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetCashFlowFin(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.StockComNetCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DebtIssuedLTSTCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ProfLossStartingLineCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.IncTaxPaidReimbIndirCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DebtIssuedLTCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DebtIssuedSTCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.StockComPrefOthIssuedSoldCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthFinCashFlow(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthInvstCashFlow(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthNonCashItemsReconcAdjCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.OthAssetsLiabNetCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetCashEndBal(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.NetCashBegBal(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DivPrefCashPaid(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.StockPrefIssuedSoldCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.PPEPurchCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.ShrBasedPaymtCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.WkgCapCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
        r'TR.F.DeprDeplAmortCF(Period=FQ0,SDate={start_date},EDate={end_date},Frq=FQ)',
    ]

    @classmethod
    def _get_market_data_fields(cls, start_date: str, end_date: str) -> list[str]:
        """
        Build the list of Refinitiv fields with date parameters.

        Parameters
        ----------
        start_date
            Start date in 'YYYY-MM-DD' format
        end_date
            End date in 'YYYY-MM-DD' format

        Returns
        -------
        List of formatted Refinitiv field strings
        """
        return [
            field.format(start_date=start_date, end_date=end_date)
            for field in cls.MARKET_DATA_FIELDS_LIST
        ]

    @classmethod
    def _get_fundamental_data_fields(cls, start_date: str, end_date: str) -> list[str]:
        """
        Build the list of Refinitiv fundamental data fields with date parameters.

        Combines income statement, balance sheet, and cash flow fields.

        Parameters
        ----------
        start_date
            Start date in 'YYYY-MM-DD' format
        end_date
            End date in 'YYYY-MM-DD' format

        Returns
        -------
        List of formatted Refinitiv field strings for fundamental data
        """
        all_fields = (
            cls.INCOME_STATEMENT_FIELDS_LIST
            + cls.BALANCE_SHEET_FIELDS_LIST
            + cls.CASH_FLOW_FIELDS_LIST
        )
        return [
            field.format(start_date=start_date, end_date=end_date)
            for field in all_fields
        ]

    @classmethod
    def _get_dividend_fields(cls, start_date: str, end_date: str) -> list[str]:
        """
        Build the list of Refinitiv dividend data fields with date parameters.

        Parameters
        ----------
        start_date
            Start date in 'YYYY-MM-DD' format
        end_date
            End date in 'YYYY-MM-DD' format

        Returns
        -------
        List of formatted Refinitiv field strings for dividend data
        """
        return [
            field.format(start_date=start_date, end_date=end_date)
            for field in cls.DIVIDEND_FIELDS_LIST
        ]

    @classmethod
    def _get_split_fields(cls, start_date: str, end_date: str) -> list[str]:
        """
        Build the list of Refinitiv split data fields with date parameters.

        Parameters
        ----------
        start_date
            Start date in 'YYYY-MM-DD' format
        end_date
            End date in 'YYYY-MM-DD' format

        Returns
        -------
        List of formatted Refinitiv field strings for split data
        """
        return [
            field.format(start_date=start_date, end_date=end_date)
            for field in cls.SPLIT_FIELDS_LIST
        ]

    @staticmethod
    def _calculate_dividend_adjusted_prices(
        market_data: pandas.DataFrame,
        dividend_data: pandas.DataFrame,
    ) -> pandas.DataFrame:
        """
        Calculate dividend-adjusted prices using split-adjusted prices and dividend history.

        The adjustment is applied backwards from the most recent date. For each ex-dividend
        date, prices before that date are multiplied by an adjustment factor.

        Formula
        -------
        Adjusted_Price[t] = Price[t] * Π(1 - Div[i]/Price[i]) for all ex-dates i > t

        The factor (1 - Div/Price) accounts for the price drop on ex-dividend dates.

        Parameters
        ----------
        market_data : pandas.DataFrame
            DataFrame with market data including split-adjusted prices.
            Required columns: Date, Open/High/Low/Close Price_split.
        dividend_data : pandas.DataFrame
            DataFrame with dividend history.
            Required columns: Dividend Ex Date, Adjusted Gross Dividend Amount.

        Returns
        -------
        pandas.DataFrame
            Input DataFrame with additional columns for dividend+split adjusted prices:
            Open/High/Low/Close Price_div_split.

        Notes
        -----
        If dividend_data is empty, the split-adjusted prices are simply copied
        to the dividend+split adjusted columns.
        """
        if market_data.empty:
            return market_data

        # Make a copy to avoid modifying the original
        result = market_data.copy()

        # If no dividend data, dividend+split adjusted = split adjusted
        if dividend_data.empty or ColumnNames.DIVIDEND_EX_DATE not in dividend_data.columns:
            result[ColumnNames.OPEN_PRICE_DIV_SPLIT] = result[ColumnNames.OPEN_PRICE_SPLIT]
            result[ColumnNames.HIGH_PRICE_DIV_SPLIT] = result[ColumnNames.HIGH_PRICE_SPLIT]
            result[ColumnNames.LOW_PRICE_DIV_SPLIT] = result[ColumnNames.LOW_PRICE_SPLIT]
            result[ColumnNames.CLOSE_PRICE_DIV_SPLIT] = result[ColumnNames.CLOSE_PRICE_SPLIT]
            return result

        # Sort market data by date ascending for proper cumulative calculation
        result = result.sort_values(ColumnNames.DATE, ascending=True).reset_index(drop=True)

        # Get ex-dividend dates and split-adjusted amounts
        ex_dates = dividend_data[ColumnNames.DIVIDEND_EX_DATE].dropna().tolist()
        dividends = dividend_data[ColumnNames.ADJUSTED_GROSS_DIVIDEND].dropna().tolist()

        # Initialize adjustment factors to 1.0
        adjustment_factors = pandas.Series(1.0, index=result.index)

        # Calculate cumulative adjustment factor (working backwards from end)
        for ex_date_raw, div_amount in zip(ex_dates, dividends, strict=True):
            if div_amount is None or pandas.isna(div_amount) or div_amount <= 0:
                continue

            # Convert ex_date to comparable type if needed
            ex_date = ex_date_raw.date() if hasattr(ex_date_raw, 'date') else ex_date_raw

            # Find the close price on the day before ex-date for adjustment calculation
            before_ex_mask = result[ColumnNames.DATE] < ex_date
            if not before_ex_mask.any():
                continue

            # Get close price on ex-date (or nearest date before)
            ex_date_mask = result[ColumnNames.DATE] == ex_date
            if ex_date_mask.any():
                close_on_ex = result.loc[ex_date_mask, ColumnNames.CLOSE_PRICE_SPLIT].iloc[0]
            else:
                # Use the last close before ex-date
                last_before_ex_idx = result.loc[before_ex_mask, ColumnNames.DATE].idxmax()
                close_on_ex = result.loc[last_before_ex_idx, ColumnNames.CLOSE_PRICE_SPLIT]

            if close_on_ex is None or pandas.isna(close_on_ex) or close_on_ex <= 0:
                continue

            # Calculate adjustment factor for this dividend
            factor = 1 - (div_amount / close_on_ex)

            # Apply factor to all dates before ex-date
            adjustment_factors[before_ex_mask] *= factor

        # Apply adjustment factors to split-adjusted prices
        result[ColumnNames.OPEN_PRICE_DIV_SPLIT] = (
            result[ColumnNames.OPEN_PRICE_SPLIT] * adjustment_factors
        )
        result[ColumnNames.HIGH_PRICE_DIV_SPLIT] = (
            result[ColumnNames.HIGH_PRICE_SPLIT] * adjustment_factors
        )
        result[ColumnNames.LOW_PRICE_DIV_SPLIT] = (
            result[ColumnNames.LOW_PRICE_SPLIT] * adjustment_factors
        )
        result[ColumnNames.CLOSE_PRICE_DIV_SPLIT] = (
            result[ColumnNames.CLOSE_PRICE_SPLIT] * adjustment_factors
        )

        return result

    def __init__(
        self,
        *,
        api_key: str | None,
    ) -> None:
        """
        Initialize the LSEG Workspace data provider, using its API key.

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

        self.api_key = api_key
        self.cache: LsegWorkspace.TickerCacheType = {}

    def _restart_session(self) -> None:
        """Close the current default Refinitiv session and open a fresh one."""
        try:
            current_session = refinitiv.data.session.get_default()
            if current_session is not None:
                current_session.close()
        except (OSError, RDError):
            logger.debug("Failed to close existing Refinitiv session; proceeding with new session")

        session = refinitiv.data.session.desktop.Definition(
            app_key=self.api_key,
        ).get_session()
        refinitiv.data.session.set_default(session)
        session.open()

        if session is None or str(session.open_state) != 'OpenState.Opened':
            raise LSEGFatalError(
                error_code=None,
                message="Refinitiv session failed to reopen after bulk failure.",
            )
        logger.info("Refinitiv session restarted successfully")

    def initialize(
            self,
            *,
            configuration: Configuration,
    ) -> None:
        """
        Initialize the LSEG data provider by fetching and caching all data types.

        This method orchestrates the bulk data fetching process:
        1. Opens the Refinitiv session (requires Workspace running)
        2. Fetches all data types (market, fundamental, dividend, split, currency)
        3. Processes the raw data for each ticker
        4. Calculates dividend-adjusted prices
        5. Caches the processed data for later retrieval

        Parameters
        ----------
        configuration : Configuration
            The Configuration entity containing:
            - identifiers: List of ticker symbols (RICs) to fetch
            - start_date: Beginning of the date range
            - end_date: End of the date range

        Raises
        ------
        LSEGFatalError
            If unable to connect to Refinitiv Workspace or API returns
            a fatal error (e.g., invalid field syntax).

        Notes
        -----
        All data is cached in memory. For large ticker lists or date ranges,
        consider memory usage implications.
        """
        # Open Refinitiv Desktop session using the api_key (validated in __init__)
        try:
            session = refinitiv.data.session.desktop.Definition(
                app_key=self.api_key,
            ).get_session()
            refinitiv.data.session.set_default(session)
            session.open()
        except KeyboardInterrupt:
            raise LSEGFatalError(
                error_code=None,
                message="Refinitiv session opening was interrupted by user.",
            ) from None
        except Exception as e:
            raise LSEGFatalError(
                error_code=None,
                message=(
                    f"Failed to open Refinitiv Desktop session. "
                    f"Ensure Workspace is running and you are logged in. {e}"
                ),
            ) from e

        # Verify session actually opened (SDK may fail silently)
        if session is None or str(session.open_state) != 'OpenState.Opened':
            raise LSEGFatalError(
                error_code=None,
                message=(
                    "Refinitiv session failed to open. "
                    "Ensure Workspace is running and you are logged in."
                ),
            )
        #@todo: review if it is a logic to fix from our part or refinitiv
        # Suppress known Refinitiv library warnings that don't affect functionality
        warnings.filterwarnings('ignore', category=FutureWarning, module='refinitiv')
        warnings.filterwarnings('ignore', category=RuntimeWarning, message='invalid value encountered in cast')

        # Free memory from previous chunk before downloading the next one
        self.cache.clear()

        tickers = configuration.identifiers
        start_date = configuration.start_date
        end_date = configuration.end_date

        # Check if another instance already fetched data for this exact configuration
        config_key = (tickers, start_date, end_date)
        if (
            LsegWorkspace._shared_cache_config_key == config_key
            and all(ticker in LsegWorkspace._shared_cache for ticker in tickers)
        ):
            self.cache = {
                ticker: LsegWorkspace._shared_cache[ticker]
                for ticker in tickers
            }
            session.close()
            return

        start_time = time.perf_counter()

        logger.info(
            "Starting data initialization for %d tickers (%s to %s)",
            len(tickers),
            start_date,
            end_date,
        )
        #@todo: review why close column sometimes is full of nulls (lseg problem)
        skip_fallback = len(tickers) > self.PIPELINE_CHUNK_SIZE

        # Fetch all raw data
        raw_data = self._fetch_all_raw_data(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            skip_partition_fallback=skip_fallback,
        )

        # Process and cache data for each ticker
        for ticker in tickers:
            logger.debug("Processing data for ticker: %s", ticker)
            self._process_and_cache_ticker_data(
                ticker=ticker,
                raw_data=raw_data
            )

        # Share cache at class level for other instances with same configuration
        LsegWorkspace._shared_cache = dict(self.cache)
        LsegWorkspace._shared_cache_config_key = config_key

        elapsed = time.perf_counter() - start_time
        logger.info(
            "Data initialization complete for %d tickers in %.2f seconds",
            len(tickers),
            elapsed,
        )

        # Close the current default Refinitiv session
        # (may differ from local `session` if _restart_session was called)
        try:
            current_session = refinitiv.data.session.get_default()
            if current_session is not None:
                current_session.close()
        except (OSError, RDError):
            logger.debug("Failed to close Refinitiv session during initialize cleanup")

    def refetch_tickers(
        self,
        *,
        tickers: tuple[str, ...],
        configuration: Configuration,
    ) -> None:
        """
        Re-download and re-cache data for specific tickers.

        Opens a new Refinitiv session, re-fetches all data types for the
        given tickers using the same bulk pipeline as ``initialize()``,
        processes the raw data, and updates both the instance cache and the
        class-level shared cache.

        Parameters
        ----------
        tickers
            The ticker symbols (RICs) to re-fetch data for.
        configuration
            The Configuration entity with date range and other settings.
        """
        if not tickers:
            return

        logger.info(
            "Re-fetching data for %d tickers: %s",
            len(tickers),
            tickers,
        )

        start_date = configuration.start_date
        end_date = configuration.end_date

        # Check if another instance already refetched these exact tickers
        refetch_key = (tickers, start_date, end_date)
        if (
            LsegWorkspace._shared_refetch_key == refetch_key
            and all(ticker in LsegWorkspace._shared_cache for ticker in tickers)
        ):
            for ticker in tickers:
                self.cache[ticker] = LsegWorkspace._shared_cache[ticker]
            logger.info(
                "Re-fetch skipped for %d tickers (already refetched by another instance)",
                len(tickers),
            )
            return

        # Open a new Refinitiv Desktop session
        try:
            session = refinitiv.data.session.desktop.Definition(
                app_key=self.api_key,
            ).get_session()
            refinitiv.data.session.set_default(session)
            session.open()
        except KeyboardInterrupt:
            raise LSEGFatalError(
                error_code=None,
                message="Refinitiv session opening was interrupted by user.",
            ) from None
        except Exception as e:
            raise LSEGFatalError(
                error_code=None,
                message=(
                    f"Failed to open Refinitiv Desktop session for refetch. "
                    f"Ensure Workspace is running and you are logged in. {e}"
                ),
            ) from e

        if session is None or str(session.open_state) != 'OpenState.Opened':
            raise LSEGFatalError(
                error_code=None,
                message=(
                    "Refinitiv session failed to open for refetch. "
                    "Ensure Workspace is running and you are logged in."
                ),
            )

        warnings.filterwarnings('ignore', category=FutureWarning, module='refinitiv')
        warnings.filterwarnings('ignore', category=RuntimeWarning, message='invalid value encountered in cast')

        # Re-fetch all raw data for the failed tickers only
        raw_data = self._fetch_all_raw_data(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
        )

        # Re-process and cache data for each ticker (overwrites old empty entries)
        for ticker in tickers:
            logger.debug("Re-processing data for ticker: %s", ticker)
            self._process_and_cache_ticker_data(
                ticker=ticker,
                raw_data=raw_data,
            )

        # Update the class-level shared cache for these tickers
        for ticker in tickers:
            if ticker in self.cache:
                LsegWorkspace._shared_cache[ticker] = self.cache[ticker]
        LsegWorkspace._shared_refetch_key = refetch_key

        # Close the current default Refinitiv session
        # (may differ from local `session` if _restart_session was called)
        try:
            current_session = refinitiv.data.session.get_default()
            if current_session is not None:
                current_session.close()
        except (OSError, RDError):
            logger.debug("Failed to close Refinitiv session during refetch cleanup")

        logger.info(
            "Re-fetch complete for %d tickers",
            len(tickers),
        )

    def _fetch_all_raw_data(
        self,
        tickers: tuple[str, ...],
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        skip_partition_fallback: bool = False,
    ) -> RawDataBundle:
        """
        Fetch all data types from the LSEG API.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        start_date : datetime.date
            Start date of the requested data range.
        end_date : datetime.date
            End date of the requested data range.
        skip_partition_fallback : bool, optional
            When ``True``, internal fetch methods skip their partition/grid
            fallbacks and re-raise errors so the orchestrator can handle
            chunked fallback at the pipeline level.

        Returns
        -------
        RawDataBundle
            Container with all raw DataFrames and currency mapping.
        """
        # Fetch market data

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        logger.info("Fetching market data for %d tickers", len(tickers))
        market_start = time.perf_counter()
        market_data_fields = self._get_market_data_fields(
            start_date=start_date_str,
            end_date=end_date_str,
        )
        bulk_market_data = self._fetch_bulk_data(
            tickers=tickers,
            fields=market_data_fields,
            session_restart_callback=self._restart_session,
            skip_partition_fallback=skip_partition_fallback,
        )
        bulk_market_data = self._rename_market_data_columns(bulk_market_data=bulk_market_data)
        logger.debug(
            "Market data fetch complete: %d rows in %.2fs",
            len(bulk_market_data) if bulk_market_data is not None else 0,
            time.perf_counter() - market_start
        )

        # Fetch fundamental data in batches (API has field count limits)
        logger.info("Fetching fundamental data for %d tickers", len(tickers))
        fundamental_start = time.perf_counter()
        fundamental_data_fields = self._get_fundamental_data_fields(
            start_date=start_date_str,
            end_date=end_date_str,
        )
        bulk_fundamental_data = self._fetch_fundamental_data_in_batches(
            tickers=tickers,
            fields=fundamental_data_fields,
            batch_size=self.FUNDAMENTAL_BATCH_SIZE,
            session_restart_callback=self._restart_session,
            skip_partition_fallback=skip_partition_fallback,
        )
        logger.debug(
            "Fundamental data fetch complete: %d rows in %.2fs",
            len(bulk_fundamental_data) if bulk_fundamental_data is not None else 0,
            time.perf_counter() - fundamental_start
        )

        # Fetch dividend data
        logger.info("Fetching dividend data for %d tickers", len(tickers))
        dividend_start = time.perf_counter()
        dividend_data_fields = self._get_dividend_fields(
            start_date=start_date_str,
            end_date=end_date_str,
        )
        bulk_dividend_data = self._fetch_bulk_data(
            tickers=tickers,
            fields=dividend_data_fields,
            session_restart_callback=self._restart_session,
            skip_partition_fallback=skip_partition_fallback,
        )
        logger.debug(
            "Dividend data fetch complete: %d rows in %.2fs",
            len(bulk_dividend_data) if bulk_dividend_data is not None else 0,
            time.perf_counter() - dividend_start
        )

        # Fetch split data
        logger.info("Fetching split data for %d tickers", len(tickers))
        split_start = time.perf_counter()
        split_data_fields = self._get_split_fields(
            start_date=start_date_str,
            end_date=end_date_str,
        )
        bulk_split_data = self._fetch_bulk_data(
            tickers=tickers,
            fields=split_data_fields,
            session_restart_callback=self._restart_session,
            skip_partition_fallback=skip_partition_fallback,
        )
        logger.debug(
            "Split data fetch complete: %d rows in %.2fs",
            len(bulk_split_data) if bulk_split_data is not None else 0,
            time.perf_counter() - split_start
        )

        # Fetch currency data
        currency_map = self._fetch_currency_data(tickers)

        return self.RawDataBundle(
            market=bulk_market_data if bulk_market_data is not None else pandas.DataFrame(),
            fundamental=bulk_fundamental_data if bulk_fundamental_data is not None else pandas.DataFrame(),
            dividend=bulk_dividend_data if bulk_dividend_data is not None else pandas.DataFrame(),
            split=bulk_split_data if bulk_split_data is not None else pandas.DataFrame(),
            currency_map=currency_map,
        )

    @classmethod
    def _fetch_currency_data(cls, tickers: tuple[str, ...]) -> dict[str, str]:
        """
        Fetch currency information for each ticker.

        CF_CURR is a static/pricing field that requires refinitiv.data.get_data()
        rather than the fundamental_and_reference API.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch currencies for.

        Returns
        -------
        dict[str, str]
            Mapping of ticker symbols to their reporting currencies.
            Uses DEFAULT_CURRENCY for tickers without currency data.
        """
        # logger.info("Fetching currency data for %d tickers", len(tickers))
        currency_map: dict[str, str] = {}

        try:
            currency_data = refinitiv.data.get_data(
                universe=tickers,
                fields=[ColumnNames.CF_CURR]
            )
            if currency_data is not None and not currency_data.empty:
                for _, row in currency_data.iterrows():
                    instrument = row.get(ColumnNames.INSTRUMENT, '')
                    currency = row.get(ColumnNames.CF_CURR)
                    if instrument and currency and pandas.notna(currency):
                        currency_map[instrument] = str(currency)

            logger.debug("Currency mapping retrieved for %d tickers", len(currency_map))

        except RDError as e:
            logger.warning(
                "Failed to fetch currency data (LSEG error): %s. Using default '%s'.",
                e,
                cls.DEFAULT_CURRENCY
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(
                "Unexpected error fetching currency data: %s. Using default '%s'.",
                e,
                cls.DEFAULT_CURRENCY
            )

        return currency_map

    @staticmethod
    def _rename_market_data_columns(
        bulk_market_data: pandas.DataFrame | None,
    ) -> pandas.DataFrame | None:
        """
        Rename duplicate price columns to distinguish adjustment types.

        The LSEG API returns duplicate column names for different adjustment types:
        - First occurrence: Unadjusted (Adjusted=0)
        - Second occurrence: Split-adjusted (Adjusted=1)

        This method appends '_split' suffix to the second occurrence.

        Parameters
        ----------
        bulk_market_data : pandas.DataFrame | None
            Raw market data DataFrame from the API.

        Returns
        -------
        pandas.DataFrame | None
            DataFrame with renamed columns, or None if input is None/empty.
        """
        if bulk_market_data is None or bulk_market_data.empty:
            return bulk_market_data

        cols = bulk_market_data.columns.tolist()
        new_cols: list[str] = []

        # Track occurrences of each price column
        price_cols = {
            ColumnNames.OPEN_PRICE: 0,
            ColumnNames.HIGH_PRICE: 0,
            ColumnNames.LOW_PRICE: 0,
            ColumnNames.CLOSE_PRICE: 0,
        }

        for col in cols:
            if col in price_cols:
                if price_cols[col] == 0:
                    # First occurrence - unadjusted
                    new_cols.append(col)
                else:
                    # Second occurrence - split-adjusted
                    new_cols.append(f'{col}_split')
                price_cols[col] += 1
            else:
                new_cols.append(col)

        bulk_market_data.columns = new_cols
        return bulk_market_data

    def _process_and_cache_ticker_data(
        self,
        ticker: str,
        raw_data: RawDataBundle,
    ) -> None:
        """
        Process raw data for a single ticker and store in cache.

        Parameters
        ----------
        ticker : str
            The ticker symbol (RIC) to process.
        raw_data : RawDataBundle
            Container with all raw data from the API.
        """
        # Process each data type
        ticker_market_data = self._process_ticker_market_data(ticker=ticker, bulk_data=raw_data.market)
        ticker_dividend_data = self._process_ticker_dividend_data(ticker=ticker, bulk_data=raw_data.dividend)
        ticker_split_data = self._process_ticker_split_data(ticker=ticker, bulk_data=raw_data.split)

        # Calculate dividend-adjusted prices (requires both market and dividend data)
        if not ticker_market_data.empty:
            ticker_market_data = self._calculate_dividend_adjusted_prices(
                market_data=ticker_market_data,
                dividend_data=ticker_dividend_data,
            )
            # Sort back to descending order after dividend adjustment
            ticker_market_data = ticker_market_data.sort_values(
                ColumnNames.DATE,
                ascending=False
            )

        # Process fundamental data (requires currency map)
        ticker_fundamental_data = self._process_ticker_fundamental_data(
            ticker=ticker,
            bulk_data=raw_data.fundamental,
            currency_map=raw_data.currency_map
        )

        # Store in cache
        self.cache[ticker] = {
            self.Endpoints.MARKET_DATA_DAILY_UNADJUSTED: ticker_market_data,
            self.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED: ticker_market_data,
            self.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED: ticker_market_data,
            self.Endpoints.FUNDAMENTAL_DATA: ticker_fundamental_data,
            self.Endpoints.STOCK_DIVIDEND: ticker_dividend_data,
            self.Endpoints.STOCK_SPLIT: ticker_split_data,
        }

    @staticmethod
    def _process_ticker_market_data(
        ticker: str,
        bulk_data: pandas.DataFrame,
    ) -> pandas.DataFrame:
        """
        Process market data for a single ticker.

        Parameters
        ----------
        ticker : str
            The ticker symbol (RIC).
        bulk_data : pandas.DataFrame
            Raw market data for all tickers.

        Returns
        -------
        pandas.DataFrame
            Processed market data for the ticker.
        """
        if bulk_data.empty:
            return pandas.DataFrame()

        ticker_data = bulk_data[
            bulk_data[ColumnNames.INSTRUMENT] == ticker
        ].copy()

        if ticker_data.empty:
            return ticker_data

        # Parse date column (format: DD/MM/YYYY from LSEG)
        ticker_data[ColumnNames.DATE] = pandas.to_datetime(
            ticker_data[ColumnNames.DATE],
            format='%d/%m/%Y',
        ).dt.date
        #@todo: review with the team possible solutions to nulls (lseg problem)
        # Remove invalid rows
        ticker_data = ticker_data.dropna(subset=[ColumnNames.DATE])
        ticker_data = ticker_data.dropna(subset=[ColumnNames.VOLUME])

        # Sort by date descending and remove duplicates
        ticker_data = ticker_data.sort_values(ColumnNames.DATE, ascending=False)

        return ticker_data.drop_duplicates(subset=[ColumnNames.DATE], keep='first')

    @staticmethod
    def _process_ticker_dividend_data(
        ticker: str,
        bulk_data: pandas.DataFrame,
    ) -> pandas.DataFrame:
        """
        Process dividend data for a single ticker.

        Parameters
        ----------
        ticker : str
            The ticker symbol (RIC).
        bulk_data : pandas.DataFrame
            Raw dividend data for all tickers.

        Returns
        -------
        pandas.DataFrame
            Processed dividend data for the ticker.
        """
        if bulk_data.empty:
            return pandas.DataFrame()

        ticker_data = bulk_data[
            bulk_data[ColumnNames.INSTRUMENT] == ticker
        ].copy()

        if ticker_data.empty:
            return ticker_data

        # Parse all dividend date columns
        date_cols = [
            ColumnNames.DIVIDEND_EX_DATE,
            ColumnNames.DIVIDEND_PAY_DATE,
            ColumnNames.DIVIDEND_RECORD_DATE,
            ColumnNames.DIVIDEND_ANNOUNCEMENT_DATE,
        ]
        for date_col in date_cols:
            if date_col in ticker_data.columns:
                ticker_data[date_col] = pandas.to_datetime(
                    ticker_data[date_col],
                    format='ISO8601',
                    errors='coerce',
                ).dt.date
        #@todo: review with the team possible solutions to nulls (lseg problem)
        # Drop rows without ex-dividend date and deduplicate
        if ColumnNames.DIVIDEND_EX_DATE in ticker_data.columns:
            ticker_data = ticker_data.dropna(subset=[ColumnNames.DIVIDEND_EX_DATE])
            ticker_data = ticker_data.sort_values(
                ColumnNames.DIVIDEND_EX_DATE,
                ascending=False
            )
            ticker_data = ticker_data.drop_duplicates(
                subset=[ColumnNames.DIVIDEND_EX_DATE],
                keep='first'
            )

        return ticker_data

    @staticmethod
    def _process_ticker_split_data(
        ticker: str,
        bulk_data: pandas.DataFrame,
    ) -> pandas.DataFrame:
        """
        Process split data for a single ticker.

        Parameters
        ----------
        ticker : str
            The ticker symbol (RIC).
        bulk_data : pandas.DataFrame
            Raw split data for all tickers.

        Returns
        -------
        pandas.DataFrame
            Processed split data for the ticker.
        """
        if bulk_data.empty:
            return pandas.DataFrame()

        ticker_data = bulk_data[
            bulk_data[ColumnNames.INSTRUMENT] == ticker
        ].copy()

        if ticker_data.empty:
            return ticker_data

        # Parse split date column
        if ColumnNames.CAPITAL_CHANGE_EX_DATE in ticker_data.columns:
            ticker_data[ColumnNames.CAPITAL_CHANGE_EX_DATE] = pandas.to_datetime(
                ticker_data[ColumnNames.CAPITAL_CHANGE_EX_DATE],
                format='ISO8601',
                errors='coerce',
            ).dt.date

            ticker_data = ticker_data.dropna(subset=[ColumnNames.CAPITAL_CHANGE_EX_DATE])
            ticker_data = ticker_data.sort_values(
                ColumnNames.CAPITAL_CHANGE_EX_DATE,
                ascending=False
            )
            ticker_data = ticker_data.drop_duplicates(
                subset=[ColumnNames.CAPITAL_CHANGE_EX_DATE],
                keep='first'
            )

        return ticker_data

    @classmethod
    def _process_ticker_fundamental_data(
        cls,
        ticker: str,
        bulk_data: pandas.DataFrame,
        currency_map: dict[str, str],
    ) -> pandas.DataFrame:
        """
        Process fundamental data for a single ticker.

        This method also derives fiscal_year and fiscal_period from Period End Date,
        as these fields are not directly available from the LSEG API.

        Parameters
        ----------
        ticker : str
            The ticker symbol (RIC).
        bulk_data : pandas.DataFrame
            Raw fundamental data for all tickers.
        currency_map : dict[str, str]
            Mapping of tickers to their reporting currencies.

        Returns
        -------
        pandas.DataFrame
            Processed fundamental data for the ticker.
        """
        if bulk_data.empty:
            return pandas.DataFrame()

        ticker_data = bulk_data[
            bulk_data[ColumnNames.INSTRUMENT] == ticker
        ].copy()

        if ticker_data.empty:
            return ticker_data

        # Parse Original Announcement Date Time (ISO 8601 format)
        if ColumnNames.ORIGINAL_ANNOUNCEMENT in ticker_data.columns:
            ticker_data[ColumnNames.ORIGINAL_ANNOUNCEMENT] = pandas.to_datetime(
                ticker_data[ColumnNames.ORIGINAL_ANNOUNCEMENT],
                format='ISO8601',
                errors='coerce',
            ).dt.date

        # Parse Period End Date and derive fiscal fields
        if ColumnNames.PERIOD_END_DATE in ticker_data.columns:
            period_end_dates = pandas.to_datetime(
                ticker_data[ColumnNames.PERIOD_END_DATE],
                format='ISO8601',
                errors='coerce',
            )
            ticker_data[ColumnNames.PERIOD_END_DATE] = period_end_dates.dt.date

            # Drop rows with invalid period_end_date before deriving fiscal fields
            # This prevents None fiscal_period values which cause entity validation errors
            valid_period_mask = period_end_dates.notna()
            ticker_data = ticker_data[valid_period_mask].copy()
            period_end_dates = period_end_dates[valid_period_mask]

            if ticker_data.empty:
                return ticker_data

            # Derive fiscal_year and fiscal_period from Period End Date.
            # Note: This uses the calendar quarter of the period end date, which may not
            # match the company's fiscal quarter for companies with non-standard fiscal years
            # (e.g., Apple's fiscal year ends in September, so its Q4 maps to calendar Q3).
            ticker_data[ColumnNames.FISCAL_YEAR] = period_end_dates.dt.year.astype(int)
            #@todo: review a better solution becuase of semmi anual companies
            ticker_data[ColumnNames.FISCAL_PERIOD] = period_end_dates.dt.quarter.apply(
                lambda q: f'Q{int(q)}'
            )

            # Use currency from CF_CURR field (fetched separately)
            ticker_data[ColumnNames.CURRENCY] = currency_map.get(
                ticker,
                cls.DEFAULT_CURRENCY
            )

        # Deduplicate by Period End Date (the natural quarterly identifier).
        # Sort so that rows WITH a real filing date come first (na_position='last'),
        # then by most recent filing date, so drop_duplicates(keep='first')
        # preserves the best row for each quarter.
        # This avoids dropping quarters that lack Original Announcement Date
        # (common for older data from LSEG).
        if ColumnNames.PERIOD_END_DATE in ticker_data.columns:
            ticker_data = ticker_data.dropna(subset=[ColumnNames.PERIOD_END_DATE])
            ticker_data = ticker_data.sort_values(
                by=[
                    ColumnNames.PERIOD_END_DATE,
                    ColumnNames.ORIGINAL_ANNOUNCEMENT,
                ],
                ascending=[False, False],
                na_position='last',
            )
            ticker_data = ticker_data.drop_duplicates(
                subset=[ColumnNames.PERIOD_END_DATE],
                keep='first',
            )

        return ticker_data

    @classmethod
    def get_data_block_endpoint_tag_map(cls) -> DataBlockEndpointTagMap:
        return {
            DividendsDataBlock: cls._dividend_data_endpoint_map,
            FundamentalsDataBlock: cls._fundamental_data_endpoint_map,
            MarketDailyDataBlock: cls._market_data_endpoint_map,
            SplitsDataBlock: cls._split_data_endpoint_map,
        }

    @classmethod
    def _classify_lseg_error(cls, error: RDError) -> LSEGProviderError:
        """
        Classify an ``RDError`` into a specific ``LSEGProviderError`` subclass.

        The classification determines the appropriate recovery strategy for
        each error type. For HTTP 400 errors, the message is inspected to
        distinguish between backend overload, authentication failures,
        session quota issues, and genuinely fatal bad requests.

        Parameters
        ----------
        error : RDError
            The Refinitiv API error to classify.

        Returns
        -------
        LSEGProviderError
            The appropriate exception subclass for the error.

        Notes
        -----
        Classification priority for error code 400:

        1. "Backend error" in message -> ``LSEGPlatformOverloadError``
        2. "invalid_grant" / "invalid_client" -> ``LSEGAuthenticationError``
        3. "session quota" -> ``LSEGSessionQuotaError``
        4. Other 400 -> ``LSEGFatalError``
        """
        error_code = getattr(error, 'code', None)
        error_message = str(error)

        # Check for fatal PERIOD message pattern first (any error code)
        error_message_upper = error_message.upper()
        for pattern in cls.FATAL_ERROR_MESSAGES:
            if pattern.upper() in error_message_upper:
                return LSEGFatalError(
                    error_code=error_code,
                    message=error_message,
                )

        # Field not recognized (207) — always fatal
        if error_code == cls.FIELD_NOT_RECOGNIZED_ERROR_CODE:
            return LSEGFatalError(
                error_code=error_code,
                message=error_message,
            )

        # Gateway timeout (2504) — triggers ticker chunking
        if error_code == cls.GATEWAY_TIMEOUT_ERROR_CODE:
            return LSEGGatewayTimeoutError(
                http_code=error_code,
                message=error_message,
            )

        # Authentication failure (401)
        if error_code == cls.AUTHENTICATION_ERROR_CODE:
            return LSEGAuthenticationError(
                http_code=error_code,
                message=error_message,
            )

        # Authorization failure (403)
        if error_code == cls.AUTHORIZATION_ERROR_CODE:
            return LSEGAuthorizationError(
                http_code=error_code,
                message=error_message,
            )

        # Rate limit (429) or service unavailable (503)
        if error_code in (cls.RATE_LIMIT_ERROR_CODE, cls.SERVICE_UNAVAILABLE_ERROR_CODE):
            return LSEGRateLimitError(
                http_code=error_code,
                message=error_message,
            )

        # HTTP 400 — sub-classify by message content
        if error_code == cls.BAD_REQUEST_ERROR_CODE:
            if cls._BACKEND_ERROR_PATTERN.search(error_message):
                return LSEGPlatformOverloadError(
                    http_code=error_code,
                    message=error_message,
                )
            if cls._AUTH_ERROR_PATTERN.search(error_message):
                return LSEGAuthenticationError(
                    http_code=error_code,
                    message=error_message,
                )
            if cls._SESSION_QUOTA_PATTERN.search(error_message):
                return LSEGSessionQuotaError(
                    http_code=error_code,
                    message=error_message,
                )
            # Other 400 errors are fatal (bad request parameters)
            return LSEGFatalError(
                error_code=error_code,
                message=error_message,
            )

        # Server error (500, 408) or any unknown code — generic retryable
        return LSEGServerError(
            http_code=error_code,
            message=error_message,
        )

    @staticmethod
    def _is_data_valid(
        data: pandas.DataFrame | None,
        expected_tickers: tuple[str, ...] | None = None
    ) -> bool:
        """
        Validate that the fetched DataFrame contains valid data.

        Parameters
        ----------
        data : pandas.DataFrame | None
            The DataFrame to validate.
        expected_tickers : tuple[str, ...] | None, optional
            Optional tuple of tickers that should be present in the data.
            If provided, at least one ticker must be present.

        Returns
        -------
        bool
            True if the data passes validation checks:
            - Not None or empty
            - Contains 'Instrument' column
            - If expected_tickers provided, at least one is present
        """
        if data is None or data.empty:
            return False

        if ColumnNames.INSTRUMENT not in data.columns:
            return False

        if expected_tickers:
            present_tickers = set(data[ColumnNames.INSTRUMENT].unique())
            # At least some tickers should be present
            if not present_tickers.intersection(set(expected_tickers)):
                return False

        return True

    @staticmethod
    def _validate_response_completeness(
        data: pandas.DataFrame,
        requested_tickers: tuple[str, ...],
    ) -> tuple[pandas.DataFrame, list[str]]:
        """
        Check whether the API response contains data for all requested tickers.

        Compares the set of RICs present in the ``Instrument`` column of
        *data* against *requested_tickers* and returns the list of any
        tickers that are missing from the response.

        Parameters
        ----------
        data : pandas.DataFrame
            The DataFrame returned by the API (must contain an
            ``Instrument`` column).
        requested_tickers : tuple[str, ...]
            The tickers that were sent in the request.

        Returns
        -------
        tuple[pandas.DataFrame, list[str]]
            A two-element tuple of (*data*, *missing_tickers*).
            *missing_tickers* is empty when no truncation is detected.
        """
        if data.empty or ColumnNames.INSTRUMENT not in data.columns:
            return data, list(requested_tickers)

        returned_rics = set(data[ColumnNames.INSTRUMENT].unique())
        requested_set = set(requested_tickers)
        missing = sorted(requested_set - returned_rics)

        if missing:
            logger.warning(
                "Response truncation detected: %d/%d tickers missing: %s",
                len(missing),
                len(requested_tickers),
                ', '.join(missing),
            )

        return data, missing

    @classmethod
    def _fetch_in_mini_bulks(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
        batch_size: int | None = None,
    ) -> tuple[pandas.DataFrame, list[str]]:
        """
        Fetch data by splitting tickers into mini-bulk batches.

        Each batch of up to *effective_batch_size* tickers is fetched via
        ``_fetch_bulk_data_with_retry``.  Batches that fail are **not**
        retried internally; their tickers are accumulated into a
        *failed_rics* list so the caller can decide how to handle them.

        ``LSEGFatalError`` and ``LSEGAuthorizationError`` propagate
        immediately.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields to fetch (with parameters embedded).
        batch_size : int | None, optional
            Number of tickers per mini-bulk batch.  Defaults to
            ``MINI_BULK_BATCH_SIZE`` and is clamped to
            ``MINI_BULK_MAX_BATCH_SIZE``.

        Returns
        -------
        tuple[pandas.DataFrame, list[str]]
            A two-element tuple of (*data*, *failed_rics*).
            *data* is a concatenated DataFrame of all successful batches
            (may be empty).  *failed_rics* contains every ticker that
            could not be fetched.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors.
        LSEGAuthorizationError
            On authorization failures (HTTP 403).
        """
        effective_size = min(
            batch_size or cls.MINI_BULK_BATCH_SIZE,
            cls.MINI_BULK_MAX_BATCH_SIZE,
        )

        ticker_batches = [
            tickers[i:i + effective_size]
            for i in range(0, len(tickers), effective_size)
        ]

        logger.info(
            "Mini-bulk fetch: %d tickers in %d batches of up to %d",
            len(tickers),
            len(ticker_batches),
            effective_size,
        )

        all_results: list[pandas.DataFrame] = []
        failed_rics: list[str] = []

        for batch_idx, batch in enumerate(ticker_batches, 1):
            batch_tuple = tuple(batch)
            logger.info(
                "Mini-bulk batch %d/%d: fetching tickers: %s",
                batch_idx,
                len(ticker_batches),
                list(batch_tuple),
            )
            try:
                batch_data = cls._fetch_bulk_data_with_retry(
                    tickers=batch_tuple,
                    fields=fields,
                )
                if not batch_data.empty:
                    batch_data, batch_missing = cls._validate_response_completeness(
                        data=batch_data,
                        requested_tickers=batch_tuple,
                    )
                    all_results.append(batch_data)
                    failed_rics.extend(batch_missing)
                    logger.debug(
                        "Mini-bulk batch %d/%d successful: %d rows",
                        batch_idx,
                        len(ticker_batches),
                        len(batch_data),
                    )
                else:
                    failed_rics.extend(batch_tuple)
                    logger.debug(
                        "Mini-bulk batch %d/%d returned empty",
                        batch_idx,
                        len(ticker_batches),
                    )

            except (LSEGFatalError, LSEGAuthorizationError):
                raise

            except LSEGProviderError as e:
                failed_rics.extend(batch_tuple)
                logger.warning(
                    "Mini-bulk batch %d/%d failed with %s for %d tickers",
                    batch_idx,
                    len(ticker_batches),
                    type(e).__name__,
                    len(batch_tuple),
                )

        combined = (
            pandas.concat(all_results, ignore_index=True)
            if all_results
            else pandas.DataFrame()
        )
        return combined, failed_rics

    @classmethod
    def _attempt_fetch(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
    ) -> pandas.DataFrame:
        """
        Attempt a single fetch from Refinitiv API.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs).
        fields : list[str]
            List of Refinitiv fields to fetch (with parameters embedded).

        Returns
        -------
        pandas.DataFrame
            DataFrame with fetched data.

        Raises
        ------
        RDError
            On Refinitiv API errors.
        """
        response = refinitiv.data.content.fundamental_and_reference.Definition(
            universe=tickers,
            fields=fields
        ).get_data()
        return response.data.df

    @classmethod
    def _log_retryable_error(
        cls,
        attempt: int,
        error: Exception,
        *,
        is_last_attempt: bool,
        error_code: int | None = None,
    ) -> None:
        """
        Log a retryable fetch error and sleep before the next attempt.

        Parameters
        ----------
        attempt : int
            Current attempt number (1-based).
        error : Exception
            The exception that was caught.
        is_last_attempt : bool
            Whether this is the final retry attempt.
        error_code : int | None, optional
            API error code, if available (e.g. from RDError).
        """
        code_info = f" (code {error_code})" if error_code is not None else ""
        if not is_last_attempt:
            wait_time = 2 ** attempt
            logger.warning(
                "Bulk fetch attempt %d/%d failed%s: %s. Retrying in %ds",
                attempt,
                cls.MAX_FETCH_ATTEMPTS,
                code_info,
                error,
                wait_time,
            )
            time.sleep(wait_time)
        else:
            logger.warning(
                "Bulk fetch attempt %d/%d failed%s: %s",
                attempt,
                cls.MAX_FETCH_ATTEMPTS,
                code_info,
                error,
            )

    @classmethod
    def _fetch_bulk_data_with_retry(
            cls,
            tickers: tuple[str, ...],
            fields: list[str],
    ) -> pandas.DataFrame:
        """
        Fetch bulk data from Refinitiv with retry logic and exponential backoff.

        Attempts to fetch data up to ``MAX_FETCH_ATTEMPTS`` times.  Non-retryable
        errors (``LSEGFatalError``, ``LSEGAuthorizationError``) abort immediately.
        ``LSEGRateLimitError`` uses doubled backoff (``2^(attempt+1)``).
        Other retryable ``LSEGProviderError`` subclasses use standard backoff.

        When all retry attempts are exhausted the **classified exception is
        raised** so that callers can make error-driven fallback decisions.
        Non-LSEG errors (``KeyError``, ``TypeError``, ``AttributeError``) still
        return an empty ``DataFrame`` for backward compatibility.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields to fetch (with parameters embedded).

        Returns
        -------
        pandas.DataFrame
            DataFrame with fetched data, or empty DataFrame if non-LSEG
            errors exhaust all attempts.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors (code 207, bad 400, PERIOD errors).
        LSEGAuthorizationError
            On HTTP 403 authorization failures.
        LSEGProviderError
            On retryable LSEG errors after all retry attempts are exhausted.
        ValueError
            On fatal parameter validation errors.
        """
        last_lseg_error: LSEGProviderError | None = None
        last_error: Exception | None = None
        start_time = time.perf_counter()

        for attempt in range(1, cls.MAX_FETCH_ATTEMPTS + 1):
            is_last_attempt = (attempt == cls.MAX_FETCH_ATTEMPTS)
            try:
                logger.debug(
                    "Bulk fetch attempt %d/%d for %d tickers, %d fields",
                    attempt,
                    cls.MAX_FETCH_ATTEMPTS,
                    len(tickers),
                    len(fields),
                )
                data = cls._attempt_fetch(
                    tickers=tickers,
                    fields=fields,
                )

                if cls._is_data_valid(data=data, expected_tickers=tickers):
                    elapsed = time.perf_counter() - start_time
                    logger.debug(
                        "Bulk fetch successful: %d rows for %d tickers in %.2fs",
                        len(data),
                        len(tickers),
                        elapsed,
                    )
                    return data
                else:
                    last_error = ValueError(
                        f"Attempt {attempt}: fetch returned data that failed validation "
                        f"(empty or missing expected tickers)"
                    )
                    logger.warning(
                        "Bulk fetch attempt %d: validation failed (empty or missing tickers)",
                        attempt,
                    )

            except ValueError as e:
                # Fatal error - parameter validation failed, no point retrying
                logger.error("Fatal validation error: %s", e)
                raise

            except RDError as e:
                classified = cls._classify_lseg_error(error=e)
                last_lseg_error = classified
                last_error = classified

                # Non-retryable errors: abort immediately
                if not classified.retryable:
                    raise classified from e

                # Rate limit: doubled backoff 2^(attempt+1)
                if isinstance(classified, LSEGRateLimitError):
                    if not is_last_attempt:
                        wait_time = 2 ** (attempt + 1)
                        logger.warning(
                            "Rate limit hit (%s), waiting %ds before retry %d/%d",
                            classified.http_code,
                            wait_time,
                            attempt,
                            cls.MAX_FETCH_ATTEMPTS,
                        )
                        time.sleep(wait_time)
                    continue

                # Other retryable LSEG errors: standard backoff
                cls._log_retryable_error(
                    attempt=attempt,
                    error=e,
                    is_last_attempt=is_last_attempt,
                    error_code=classified.http_code,
                )

            except (KeyError, TypeError, AttributeError, httpx.TimeoutException) as e:
                last_error = e
                cls._log_retryable_error(
                    attempt=attempt,
                    error=e,
                    is_last_attempt=is_last_attempt,
                )

        # All attempts failed
        elapsed = time.perf_counter() - start_time

        # If the last error was a classified LSEG error, raise it so callers
        # can make error-driven fallback decisions
        if last_lseg_error is not None:
            logger.error(
                "All %d bulk fetch attempts failed in %.2fs. "
                "Raising %s: %s",
                cls.MAX_FETCH_ATTEMPTS,
                elapsed,
                type(last_lseg_error).__name__,
                last_lseg_error,
            )
            raise last_lseg_error

        # Non-LSEG errors: return empty DataFrame for backward compatibility
        if last_error:
            logger.error(
                "All %d bulk fetch attempts failed in %.2fs. Last error: %s",
                cls.MAX_FETCH_ATTEMPTS,
                elapsed,
                last_error,
            )
        return pandas.DataFrame()

    @classmethod
    def _fetch_with_adaptive_chunking(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
        failed_tickers: list[str] | None = None,
    ) -> pandas.DataFrame:
        """
        Fetch data using adaptive binary-split chunking (Tier 3).

        When bulk fetch fails, this method recursively splits the ticker list
        in half and attempts to fetch each half independently. This continues
        until either:
        - A batch succeeds
        - Individual tickers fail (logged and skipped)

        ``LSEGFatalError`` and ``LSEGAuthorizationError`` propagate immediately.
        All other ``LSEGProviderError`` subclasses are caught and converted to
        empty-DataFrame signals so that splitting can continue.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields to fetch (with parameters embedded).
        failed_tickers : list[str] | None, optional
            Accumulator for tickers that failed even at single-ticker level.
            Used internally during recursion.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing data for all successfully fetched tickers.
            May be empty if all tickers fail.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors (code 207, PERIOD errors).
        LSEGAuthorizationError
            On authorization failures (HTTP 403).
        ValueError
            On fatal parameter validation errors.

        Notes
        -----
        Failed tickers are accumulated in the failed_tickers list for
        diagnostic purposes but do not cause exceptions.
        """
        if failed_tickers is None:
            failed_tickers = []

        if not tickers:
            return pandas.DataFrame()

        # Try to fetch the current batch with retry
        logger.debug("Tier 3: attempting chunked fetch for %d tickers", len(tickers))
        data = pandas.DataFrame()
        try:
            data = cls._fetch_bulk_data_with_retry(
                tickers=tickers,
                fields=fields,
            )
        except (LSEGFatalError, LSEGAuthorizationError):
            raise
        except LSEGProviderError as e:
            # All other LSEG errors: treat as empty for continued splitting
            logger.debug(
                "Tier 3: %s for %d tickers, will split",
                type(e).__name__,
                len(tickers),
            )

        # Non-empty DataFrame signals success
        if not data.empty:
            logger.debug(
                "Tier 3: chunked fetch successful: %d tickers, %d rows",
                len(tickers),
                len(data),
            )
            return data

        # If only one ticker and it failed, log and return empty
        if len(tickers) == 1:
            failed_tickers.append(tickers[0])
            logger.warning("Failed to fetch single ticker: %s", tickers[0])
            return pandas.DataFrame()

        # Split in half and recursively fetch each half
        mid = len(tickers) // 2
        left_tickers = tickers[:mid]
        right_tickers = tickers[mid:]

        logger.debug(
            "Splitting %d tickers into chunks of %d and %d",
            len(tickers),
            len(left_tickers),
            len(right_tickers),
        )

        # Fetch both halves
        left_data = cls._fetch_with_adaptive_chunking(
            tickers=left_tickers,
            fields=fields,
            failed_tickers=failed_tickers,
        )
        right_data = cls._fetch_with_adaptive_chunking(
            tickers=right_tickers,
            fields=fields,
            failed_tickers=failed_tickers,
        )

        # Concatenate results
        results: list[pandas.DataFrame] = []
        if not left_data.empty:
            results.append(left_data)
        if not right_data.empty:
            results.append(right_data)

        if results:
            return pandas.concat(results, ignore_index=True)

        return pandas.DataFrame()

    @classmethod
    def _fetch_bulk_data(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
        mini_bulk_batch_size: int | None = None,
        session_restart_callback: typing.Callable[[], None] | None = None,
        *,
        skip_partition_fallback: bool = False,
    ) -> pandas.DataFrame:
        """
        Fetch bulk data using a persistent multi-round fallback strategy.

        Step 1 — Single bulk request
            Attempt a single bulk request for all tickers.  On success,
            validate response completeness and collect any missing tickers
            as failures for subsequent rounds.

        Step 2 — Partitioned retry rounds (skipped when
        ``skip_partition_fallback=True``)
            Failed tickers enter a retry loop of up to
            ``MAX_PARTITION_ROUNDS`` rounds.  Each round splits the
            remaining failures into mini-bulk batches of
            ``MINI_BULK_BATCH_SIZE`` (default 5, capped at
            ``MINI_BULK_MAX_BATCH_SIZE`` = 10) and fetches each batch
            via ``_fetch_in_mini_bulks``.  An exponential backoff delay
            (``2 ** round`` seconds) is applied between rounds.  The loop
            exits early when no failed tickers remain.

        ``LSEGFatalError`` and ``LSEGAuthorizationError`` propagate
        immediately at any step.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields to fetch (with parameters embedded).
        mini_bulk_batch_size : int | None, optional
            Override the default ``MINI_BULK_BATCH_SIZE`` for partition rounds.
            Clamped to ``MINI_BULK_MAX_BATCH_SIZE``.
        skip_partition_fallback : bool, optional
            When ``True``, skip Step 2 entirely and re-raise the Step 1
            error so the orchestrator can handle chunked fallback at the
            pipeline level.  Defaults to ``False``.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing data for all successfully fetched tickers.
            Returns empty DataFrame if all tickers fail.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors (code 207, PERIOD errors).
        LSEGAuthorizationError
            On authorization failures (HTTP 403).
        LSEGProviderError
            When ``skip_partition_fallback=True`` and Step 1 fails with a
            retryable error.
        ValueError
            On fatal parameter validation errors.
        """
        start_time = time.perf_counter()
        logger.info("Starting bulk data fetch for %d tickers", len(tickers))

        all_results: list[pandas.DataFrame] = []
        failed_rics: list[str] = []
        step1_error: LSEGProviderError | None = None

        # ---- Step 1: single bulk request for ALL tickers ----
        logger.info("Step 1: bulk request for tickers: %s", list(tickers))
        try:
            result = cls._fetch_bulk_data_with_retry(
                tickers=tickers,
                fields=fields,
            )

            if not result.empty:
                result, missing = cls._validate_response_completeness(
                    data=result,
                    requested_tickers=tickers,
                )
                all_results.append(result)
                if missing:
                    logger.info(
                        "Step 1 partial success: %d/%d tickers missing",
                        len(missing),
                        len(tickers),
                    )
                    failed_rics.extend(missing)
            else:
                logger.warning(
                    "Step 1 returned empty DataFrame, "
                    "all %d tickers will be retried in partition rounds",
                    len(tickers),
                )
                failed_rics.extend(tickers)

        except (LSEGFatalError, LSEGAuthorizationError):
            raise

        except LSEGProviderError as e:
            step1_error = e
            logger.warning(
                "Step 1 failed with %s, "
                "all %d tickers will be retried in partition rounds",
                type(e).__name__,
                len(tickers),
            )
            failed_rics.extend(tickers)

        if skip_partition_fallback and failed_rics:
            logger.info(
                "Partition fallback skipped for %d failed tickers "
                "(orchestrator will handle chunked retry)",
                len(failed_rics),
            )
            if step1_error is not None:
                raise step1_error
            if all_results:
                return pandas.concat(all_results, ignore_index=True)
            return pandas.DataFrame()

        if failed_rics and session_restart_callback is not None:
            logger.info("Restarting session before partition rounds")
            try:
                session_restart_callback()
            except (OSError, RDError) as e:
                logger.warning("Session restart failed: %s. Continuing with current session.", e)

        # ---- Step 2: partitioned retry rounds for failed RICs ----
        for round_num in range(1, cls.MAX_PARTITION_ROUNDS + 1):
            if not failed_rics:
                break

            if round_num > 1:
                backoff = 2 ** round_num
                logger.info(
                    "Partition round %d/%d: waiting %ds before retrying tickers: %s",
                    round_num,
                    cls.MAX_PARTITION_ROUNDS,
                    backoff,
                    failed_rics,
                )
                time.sleep(backoff)
            else:
                logger.info(
                    "Partition round %d/%d: retrying tickers: %s",
                    round_num,
                    cls.MAX_PARTITION_ROUNDS,
                    failed_rics,
                )

            round_data, still_failed = cls._fetch_in_mini_bulks(
                tickers=tuple(failed_rics),
                fields=fields,
                batch_size=mini_bulk_batch_size,
            )
            if not round_data.empty:
                all_results.append(round_data)

            recovered = len(failed_rics) - len(still_failed)
            if recovered > 0:
                logger.info(
                    "Partition round %d/%d: recovered %d tickers, %d still failed",
                    round_num,
                    cls.MAX_PARTITION_ROUNDS,
                    recovered,
                    len(still_failed),
                )

            failed_rics = still_failed

        if failed_rics:
            logger.warning(
                "Failed to fetch data for %d tickers after all %d partition rounds: %s",
                len(failed_rics),
                cls.MAX_PARTITION_ROUNDS,
                failed_rics,
            )

        # ---- combine results ----
        elapsed = time.perf_counter() - start_time

        if all_results:
            combined = pandas.concat(all_results, ignore_index=True)
            logger.info(
                "Bulk fetch complete: %d rows in %.2fs",
                len(combined),
                elapsed,
            )
            return combined

        logger.error("No data fetched for any ticker after %.2fs", elapsed)
        return pandas.DataFrame()

    @classmethod
    def _fetch_fundamental_field_batch(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
        session_restart_callback: typing.Callable[[], None] | None = None,
        *,
        skip_partition_fallback: bool = False,
    ) -> pandas.DataFrame:
        """
        Fetch one field batch for fundamental data with grid fallback.

        Tier 1: attempt a single bulk request for all tickers with this
        field batch.  On success, validate completeness and grid-fallback
        only missing tickers.  On failure, grid-fallback all tickers.

        When ``skip_partition_fallback=True``, the grid fallback is skipped
        and Tier 1 errors are re-raised so the orchestrator can handle
        chunked fallback at the pipeline level.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields for this batch.
        skip_partition_fallback : bool, optional
            When ``True``, skip the grid fallback and re-raise Tier 1
            errors.  Defaults to ``False``.

        Returns
        -------
        pandas.DataFrame
            DataFrame with fetched data, or empty DataFrame if everything fails.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors.
        LSEGAuthorizationError
            On authorization failures (HTTP 403).
        LSEGProviderError
            When ``skip_partition_fallback=True`` and Tier 1 fails with a
            retryable error.
        """
        start_time = time.perf_counter()
        logger.info(
            "Fundamental field batch: tickers: %s, %d fields",
            list(tickers),
            len(fields),
        )

        # ---- Tier 1: single bulk request for ALL tickers ----
        try:
            result = cls._fetch_bulk_data_with_retry(
                tickers=tickers,
                fields=fields,
            )

            if not result.empty:
                result, missing = cls._validate_response_completeness(
                    data=result,
                    requested_tickers=tickers,
                )
                if missing and not skip_partition_fallback:
                    logger.info(
                        "Fundamental field batch Tier 1 partial: "
                        "grid-fallback for %d missing tickers",
                        len(missing),
                    )
                    extra = cls._fetch_fundamental_grid(
                        tickers=tuple(missing),
                        fields=fields,
                    )
                    if not extra.empty:
                        result = pandas.concat(
                            [result, extra], ignore_index=True,
                        )

                elapsed = time.perf_counter() - start_time
                logger.info(
                    "Fundamental field batch complete: %d rows in %.2fs",
                    len(result),
                    elapsed,
                )
                return result

            # Empty DataFrame from non-LSEG errors -> grid fallback
            logger.warning(
                "Fundamental field batch Tier 1 returned empty, "
                "falling back to grid",
            )

        except (LSEGFatalError, LSEGAuthorizationError):
            raise

        except LSEGProviderError as e:
            if skip_partition_fallback:
                raise
            logger.warning(
                "Fundamental field batch Tier 1 failed with %s, "
                "falling back to grid for %d tickers",
                type(e).__name__,
                len(tickers),
            )

        if skip_partition_fallback:
            return pandas.DataFrame()

        if session_restart_callback is not None:
            logger.info("Restarting session before fundamental grid fallback")
            try:
                session_restart_callback()
            except (OSError, RDError) as e:
                logger.warning("Session restart failed: %s. Continuing with current session.", e)

        # ---- Grid fallback for all tickers ----
        result = cls._fetch_fundamental_grid(
            tickers=tickers,
            fields=fields,
        )

        elapsed = time.perf_counter() - start_time
        if not result.empty:
            logger.info(
                "Fundamental field batch grid complete: %d rows in %.2fs",
                len(result),
                elapsed,
            )
        else:
            logger.error(
                "Fundamental field batch: no data after %.2fs", elapsed,
            )

        return result

    @classmethod
    def _fetch_fundamental_grid(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
    ) -> pandas.DataFrame:
        """
        Fetch fundamental data using a grid of ticker chunks x field sub-chunks.

        Splits tickers into chunks of ``FUNDAMENTAL_GRID_TICKER_SIZE`` and
        fields into sub-chunks of ``FUNDAMENTAL_GRID_FIELD_SIZE``.  Ensures
        the Period End Date field is present in every sub-chunk for merging.

        For each ticker chunk, fetches all field sub-chunks and merges them
        horizontally on ``[Instrument, Period End Date]``.  Then concatenates
        all ticker chunks vertically.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields to fetch.

        Returns
        -------
        pandas.DataFrame
            DataFrame with fetched data, or empty DataFrame if everything fails.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors.
        LSEGAuthorizationError
            On authorization failures (HTTP 403).
        """
        # Split tickers into chunks
        ticker_chunks = [
            tickers[i:i + cls.FUNDAMENTAL_GRID_TICKER_SIZE]
            for i in range(0, len(tickers), cls.FUNDAMENTAL_GRID_TICKER_SIZE)
        ]

        # Find the Period End Date field (needed in every sub-chunk for merging)
        period_end_date_field = None
        for field in fields:
            if 'PeriodEndDate' in field:
                period_end_date_field = field
                break

        # Split fields into sub-chunks
        field_sub_chunks = [
            fields[i:i + cls.FUNDAMENTAL_GRID_FIELD_SIZE]
            for i in range(0, len(fields), cls.FUNDAMENTAL_GRID_FIELD_SIZE)
        ]

        # Ensure Period End Date is in every sub-chunk
        if period_end_date_field:
            for sub_chunk in field_sub_chunks:
                if period_end_date_field not in sub_chunk:
                    sub_chunk.insert(0, period_end_date_field)

        logger.info(
            "Fundamental grid: %d ticker chunks x %d field sub-chunks "
            "(%d tickers, %d fields)",
            len(ticker_chunks),
            len(field_sub_chunks),
            len(tickers),
            len(fields),
        )

        all_ticker_results: list[pandas.DataFrame] = []

        for tc_idx, ticker_chunk in enumerate(ticker_chunks, 1):
            ticker_tuple = tuple(ticker_chunk)
            logger.info(
                "Grid ticker chunk %d/%d: fetching tickers: %s",
                tc_idx,
                len(ticker_chunks),
                list(ticker_tuple),
            )

            # Fetch all field sub-chunks for this ticker chunk
            sub_chunk_results: list[pandas.DataFrame] = []

            for fc_idx, field_sub_chunk in enumerate(field_sub_chunks, 1):
                logger.debug(
                    "Grid cell [%d/%d tickers, %d/%d fields]: "
                    "%d tickers x %d fields",
                    tc_idx,
                    len(ticker_chunks),
                    fc_idx,
                    len(field_sub_chunks),
                    len(ticker_tuple),
                    len(field_sub_chunk),
                )
                cell_data = cls._fetch_fundamental_grid_cell(
                    tickers=ticker_tuple,
                    fields=field_sub_chunk,
                )
                if not cell_data.empty:
                    sub_chunk_results.append(cell_data)

            if not sub_chunk_results:
                logger.warning(
                    "Grid ticker chunk %d/%d: all field sub-chunks failed",
                    tc_idx,
                    len(ticker_chunks),
                )
                continue

            # Merge field sub-chunks horizontally on [Instrument, Period End Date]
            merged_chunk = sub_chunk_results[0]
            merge_keys = [ColumnNames.INSTRUMENT]
            if ColumnNames.PERIOD_END_DATE in merged_chunk.columns:
                merge_keys.append(ColumnNames.PERIOD_END_DATE)

            for df in sub_chunk_results[1:]:
                new_cols = [
                    col for col in df.columns
                    if col not in merged_chunk.columns
                ]
                if new_cols:
                    available_merge_keys = [
                        k for k in merge_keys if k in df.columns
                    ]
                    merge_cols = available_merge_keys + new_cols
                    merged_chunk = merged_chunk.merge(
                        df[merge_cols],
                        on=available_merge_keys,
                        how='outer',
                    )

            all_ticker_results.append(merged_chunk)

        if all_ticker_results:
            return pandas.concat(all_ticker_results, ignore_index=True)

        return pandas.DataFrame()

    @classmethod
    def _fetch_fundamental_grid_cell(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
    ) -> pandas.DataFrame:
        """
        Fetch a single grid cell (ticker chunk x field sub-chunk).

        Tries ``_fetch_bulk_data_with_retry`` first; on retryable failure
        falls back to ``_fetch_with_adaptive_chunking`` (binary-split RICs).
        Fatal and authorization errors propagate immediately.

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) for this cell.
        fields : list[str]
            List of Refinitiv fields for this cell.

        Returns
        -------
        pandas.DataFrame
            DataFrame with fetched data, or empty DataFrame on failure.

        Raises
        ------
        LSEGFatalError
            On fatal Refinitiv API errors.
        LSEGAuthorizationError
            On authorization failures (HTTP 403).
        """
        try:
            data = cls._fetch_bulk_data_with_retry(
                tickers=tickers,
                fields=fields,
            )
            if not data.empty:
                return data

        except (LSEGFatalError, LSEGAuthorizationError):
            raise

        except LSEGProviderError as e:
            logger.warning(
                "Grid cell failed with %s for %d tickers, "
                "falling back to adaptive chunking",
                type(e).__name__,
                len(tickers),
            )

        # Fallback: adaptive binary-split chunking on RICs
        return cls._fetch_with_adaptive_chunking(
            tickers=tickers,
            fields=fields,
        )

    @classmethod
    def _fetch_fundamental_data_in_batches(
        cls,
        tickers: tuple[str, ...],
        fields: list[str],
        batch_size: int | None = None,
        session_restart_callback: typing.Callable[[], None] | None = None,
        *,
        skip_partition_fallback: bool = False,
    ) -> pandas.DataFrame:
        """
        Fetch fundamental data in field batches to avoid API limits.

        The Refinitiv API limits the number of fields per request.
        This method:
        1. Splits fields into batches of FUNDAMENTAL_BATCH_SIZE
        2. Ensures Period End Date is in every batch (required for merging)
        3. Fetches each batch independently
        4. Merges results on Instrument + Period End Date

        Parameters
        ----------
        tickers : tuple[str, ...]
            Tuple of ticker symbols (RICs) to fetch.
        fields : list[str]
            List of Refinitiv fields to fetch (with parameters embedded).
        batch_size : int | None, optional
            Maximum fields per API call. Defaults to FUNDAMENTAL_BATCH_SIZE.

        Returns
        -------
        pandas.DataFrame
            DataFrame with all requested fields merged together.
            Returns empty DataFrame if all batches fail.

        Notes
        -----
        The Period End Date field is automatically added to each batch
        to enable proper time-series alignment during merge.
        """
        if not fields:
            return pandas.DataFrame()

        effective_batch_size = batch_size or cls.FUNDAMENTAL_BATCH_SIZE

        # Find the Period End Date field - required in every batch for merging
        #@todo: improve this by only looking for Instrument Column
        period_end_date_field = None
        for field in fields:
            if 'PeriodEndDate' in field:
                period_end_date_field = field
                break

        # Split fields into batches
        field_batches = [
            fields[i:i + effective_batch_size]
            for i in range(0, len(fields), effective_batch_size)
        ]

        # Ensure Period End Date is in every batch (needed for merging time-series)
        if period_end_date_field:
            for batch in field_batches:
                if period_end_date_field not in batch:
                    batch.insert(0, period_end_date_field)

        logger.info(
            "Fetching %d fundamental fields in %d batches (max %d fields/batch)",
            len(fields),
            len(field_batches),
            effective_batch_size
        )

        all_results: list[pandas.DataFrame] = []

        for batch_idx, batch_fields in enumerate(field_batches, 1):

            try:
                batch_data = cls._fetch_fundamental_field_batch(
                    tickers=tickers,
                    fields=batch_fields,
                    session_restart_callback=session_restart_callback,
                    skip_partition_fallback=skip_partition_fallback,
                )

                # empty DataFrame signals failure
                if not batch_data.empty:
                    all_results.append(batch_data)
                    logger.info(
                        "Batch %d/%d successful: %d rows",
                        batch_idx,
                        len(field_batches),
                        len(batch_data)
                    )
                else:
                    logger.warning(
                        "Batch %d/%d returned no data",
                        batch_idx,
                        len(field_batches)
                    )

            except (LSEGFatalError, LSEGAuthorizationError):
                # Re-raise fatal/authorization errors - these indicate
                # unrecoverable issues
                raise

            except (
                LSEGProviderError,
                RDError,
                KeyError,
                ValueError,
                TypeError,
                AttributeError,
                httpx.TimeoutException,
            ) as e:
                logger.error(
                    "Batch %d/%d failed: %s",
                    batch_idx,
                    len(field_batches),
                    e,
                )

        if not all_results:
            logger.error("All fundamental data batches failed")
            return pandas.DataFrame()

        # Use Period End Date column for merging time-series batches
        merge_keys = [ColumnNames.INSTRUMENT]
        if ColumnNames.PERIOD_END_DATE in all_results[0].columns:
            merge_keys.append(ColumnNames.PERIOD_END_DATE)

        merged_result = all_results[0]
        for df in all_results[1:]:
            # Get columns to merge (excluding merge keys)
            new_cols = [col for col in df.columns if col not in merged_result.columns]
            if new_cols:
                # Ensure all merge keys exist in the df
                available_merge_keys = [k for k in merge_keys if k in df.columns]
                merge_cols = available_merge_keys + new_cols
                merged_result = merged_result.merge(
                    df[merge_cols],
                    on=available_merge_keys,
                    how='outer'
                )

        return merged_result

    def get_market_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        """
        Retrieve market data from the cache wrapped in a MarketData entity.

        Returns unadjusted, split-adjusted, and dividend+split-adjusted prices
        for the specified ticker and date range.

        Parameters
        ----------
        main_identifier : str
            The stock's ticker in RIC format (e.g., 'AAPL.OQ').
        start_date : datetime.date
            The start date of the requested data range.
        end_date : datetime.date
            The end date of the requested data range.

        Returns
        -------
        MarketData
            Entity containing market price data with rows for each trading day.

        Raises
        ------
        LSEGDataNotFoundError
            If the ticker is not found in the cache or its data is empty.
        """
        try:
            market_raw_data = self.cache[main_identifier][
                self.Endpoints.MARKET_DATA_DAILY_UNADJUSTED
            ]
        except KeyError as e:
            raise LSEGDataNotFoundError(
                tickers=[main_identifier],
                message=f"Market data not found for ticker: {main_identifier}"
            ) from e

        if market_raw_data.empty:
            raise LSEGDataNotFoundError(
                tickers=[main_identifier],
                message=f"Market data is empty for ticker: {main_identifier}"
            )

        endpoint_tables = {
            self.Endpoints.MARKET_DATA_DAILY_UNADJUSTED:
                pyarrow.Table.from_pandas(market_raw_data),
            self.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED:
                pyarrow.Table.from_pandas(market_raw_data),
            self.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED:
                pyarrow.Table.from_pandas(market_raw_data),
        }

        processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
            data_block=MarketDailyDataBlock,
            endpoint_field_map=self._market_data_endpoint_map,
            endpoint_tables=endpoint_tables,
        )
        consolidated_market_data_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[MarketDailyDataBlock.clock_sync_field],
            predominant_order_descending=True
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

    def get_fundamental_data(
        self,
        *,
        main_identifier: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        """
        Retrieve fundamental data from the cache wrapped in a FundamentalData entity.

        Parameters
        ----------
        main_identifier : str
            The stock's ticker in RIC format (e.g., 'AAPL.OQ').
        period : str
            The period identifier ('annual' or 'quarterly').
        start_date : datetime.date
            The start date of the requested data range.
        end_date : datetime.date
            The end date of the requested data range.

        Returns
        -------
        FundamentalData
            Entity containing income statement, balance sheet, and cash flow data.

        Raises
        ------
        LSEGDataNotFoundError
            If the ticker is not found in the cache.
        DataProviderToolkitRuntimeError
            If there's an error in data provider logic.
        """
        try:
            fundamental_raw_data = self.cache[main_identifier][self.Endpoints.FUNDAMENTAL_DATA]
        except KeyError as e:
            raise LSEGDataNotFoundError(
                tickers=[main_identifier],
                message=f"Fundamental data not found for ticker: {main_identifier}"
            ) from e

        # Create empty fundamental data for cases with no data
        empty_fundamental_data = FundamentalData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )

        if fundamental_raw_data.empty:
            logger.warning("%s fundamental data cache is empty", main_identifier)
            return empty_fundamental_data

        # Create endpoint tables from pandas DataFrame
        endpoint_tables = {
            self.Endpoints.FUNDAMENTAL_DATA: pyarrow.Table.from_pandas(fundamental_raw_data)
        }

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=FundamentalsDataBlock,
                endpoint_field_map=self._fundamental_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError:
            logger.warning("%s fundamental data endpoints returned no data", main_identifier)
            return empty_fundamental_data

        try:
            consolidated_fundamental_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=processed_endpoint_tables,
                table_merge_fields=[
                    FundamentalsDataBlock.clock_sync_field,
                    FundamentalDataRow.period_end_date,
                ],
                predominant_order_descending=True
            )
        except DataProviderMultiEndpointCommonDataOrderError:
            logger.error(
                "%s fundamental data endpoints have inconsistent filing_date order, "
                "omitting its fundamental data",
                main_identifier
            )
            return empty_fundamental_data
        except DataProviderMultiEndpointCommonDataDiscrepancyError as error:
            discrepancy_output_table = DataProviderToolkit.format_endpoint_discrepancy_table_for_output(
                data_block=FundamentalsDataBlock,
                discrepancy_table=error.discrepancies_table,
                endpoints_enum=self.Endpoints,
                endpoint_field_map=self._fundamental_data_endpoint_map,
            )
            logger.error(
                "%s fundamental data endpoints present discrepancies between common columns: "
                "%s. Omitting fundamental data for the dates corresponding to the following discrepancies:\n%s",
                main_identifier,
                ', '.join(error.discrepant_columns),
                discrepancy_output_table
            )
            # Fill conflicting rows with None and retry
            no_discrepancy_processed_tables = DataProviderToolkit.clear_discrepant_processed_endpoint_tables_rows(
                discrepancy_table=error.discrepancies_table,
                processed_endpoint_tables=processed_endpoint_tables,
                key_column_names=error.key_column_names,
                preserved_column_names=[
                    FundamentalsDataBlock.get_field_qualified_name(FundamentalsDataBlock.clock_sync_field),
                ]
            )
            consolidated_fundamental_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=no_discrepancy_processed_tables,
                table_merge_fields=[
                    FundamentalsDataBlock.clock_sync_field,
                    FundamentalDataRow.period_end_date,
                ],
                predominant_order_descending=True
            )

        # Reverse to ascending order
        consolidated_fundamental_table = consolidated_fundamental_table_descending[::-1]

        # Filter irregular filing rows (amendments, restatements)
        irregular_rows_mask = FundamentalsDataBlock.find_consolidated_table_irregular_filing_rows(
            consolidated_table=consolidated_fundamental_table
        )
        if irregular_rows_mask is not None:
            regular_rows_mask = pyarrow.compute.invert(irregular_rows_mask)
            consolidated_fundamental_table = consolidated_fundamental_table.filter(regular_rows_mask)

        # Assemble entities
        fundamental_data = FundamentalsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_fundamental_table,
            common_field_data={
                FundamentalData: {
                    FundamentalData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return fundamental_data  # noqa: RET504

    def get_dividend_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        """
        Retrieve dividend data from the cache wrapped in a DividendData entity.

        Parameters
        ----------
        main_identifier : str
            The stock's ticker in RIC format (e.g., 'AAPL.OQ').
        start_date : datetime.date
            The start date of the requested data range.
        end_date : datetime.date
            The end date of the requested data range.

        Returns
        -------
        DividendData
            Entity containing dividend history data with rows for each ex-dividend date.

        Raises
        ------
        LSEGDataNotFoundError
            If the ticker is not found in the cache.
        DataProviderToolkitRuntimeError
            If there's an error in data provider logic.
        """
        try:
            dividend_raw_data = self.cache[main_identifier][self.Endpoints.STOCK_DIVIDEND]
        except KeyError as e:
            raise LSEGDataNotFoundError(
                tickers=[main_identifier],
                message=f"Dividend data not found for ticker: {main_identifier}"
            ) from e

        # Create empty dividend data for cases with no data
        empty_dividend_data = DividendData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )

        if dividend_raw_data.empty:
            # logger.warning("%s dividend data cache is empty", main_identifier)
            return empty_dividend_data

        # Create endpoint tables from pandas DataFrame
        endpoint_tables = {
            self.Endpoints.STOCK_DIVIDEND: pyarrow.Table.from_pandas(dividend_raw_data)
        }

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=DividendsDataBlock,
                endpoint_field_map=self._dividend_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError:
            logger.warning("%s dividend data endpoints returned no data", main_identifier)
            return empty_dividend_data

        consolidated_dividend_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[DividendsDataBlock.clock_sync_field],
            predominant_order_descending=True
        )

        # Reverse to ascending order
        consolidated_dividend_table = consolidated_dividend_table_descending[::-1]

        # Assemble entities
        dividend_data = DividendsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_dividend_table,
            common_field_data={
                DividendData: {
                    DividendData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return dividend_data  # noqa: RET504

    def get_split_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        """
        Retrieve split data from the cache wrapped in a SplitData entity.

        Parameters
        ----------
        main_identifier : str
            The stock's ticker in RIC format (e.g., 'AAPL.OQ').
        start_date : datetime.date
            The start date of the requested data range.
        end_date : datetime.date
            The end date of the requested data range.

        Returns
        -------
        SplitData
            Entity containing stock split history data with rows for each split date.

        Raises
        ------
        LSEGDataNotFoundError
            If the ticker is not found in the cache.
        DataProviderToolkitRuntimeError
            If there's an error in data provider logic.
        """
        try:
            split_raw_data = self.cache[main_identifier][self.Endpoints.STOCK_SPLIT]
        except KeyError as e:
            raise LSEGDataNotFoundError(
                tickers=[main_identifier],
                message=f"Split data not found for ticker: {main_identifier}"
            ) from e

        # Create empty split data for cases with no data
        empty_split_data = SplitData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )

        if split_raw_data.empty:
            # logger.warning("%s split data cache is empty", main_identifier)
            return empty_split_data

        # Create endpoint tables from pandas DataFrame
        endpoint_tables = {
            self.Endpoints.STOCK_SPLIT: pyarrow.Table.from_pandas(split_raw_data)
        }

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=SplitsDataBlock,
                endpoint_field_map=self._split_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError:
            logger.warning("%s split data endpoints returned no data", main_identifier)
            return empty_split_data

        consolidated_split_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[SplitsDataBlock.clock_sync_field],
            predominant_order_descending=True
        )

        # Reverse to ascending order
        consolidated_split_table = consolidated_split_table_descending[::-1]

        # Assemble entities
        split_data = SplitsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_split_table,
            common_field_data={
                SplitData: {
                    SplitData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return split_data  # noqa: RET504

    def validate_api_key(self) -> bool | None:
        """
        Validate that the API key used to init the class is valid.

        LSEG Workspace doesn't use API keys - it uses Refinitiv session authentication.

        Returns
        -------
        None - LSEG doesn't use API keys
        """
        return None
