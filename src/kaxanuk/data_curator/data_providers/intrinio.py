import collections
import concurrent.futures
import dataclasses
import datetime
import enum
import http
import io
import logging
import math
import os
import re
import types
import typing
import urllib.request
import zipfile

import intrinio_sdk
import intrinio_sdk.rest
import pyarrow
import pyarrow.compute
import pyarrow.csv

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
)
from kaxanuk.data_curator.exceptions import (
    ApiEndpointError,
    DataProviderMissingKeyError,
    DataProviderPaymentError,
    DataProviderToolkitNoDataError,
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


# pattern matching a data tag unit that denotes a reporting currency
_CURRENCY_UNIT_PATTERN = re.compile(r"^[a-z]{3}$")

# synthetic tag names for the point-in-time fundamental values the provider derives from a
# period's reported statements (see Intrinio._add_derived_fundamental_values); each is namespaced
# so it can never collide with a real Intrinio tag, and is mapped to its entity field like any other tag
_DERIVED_CASH_AND_SHORTTERM_INVESTMENTS_TAG = 'derived_cash_and_shortterm_investments'
_DERIVED_COSTS_AND_EXPENSES_TAG = 'derived_costs_and_expenses'
_DERIVED_FREE_CASH_FLOW_TAG = 'derived_free_cash_flow'
_DERIVED_NET_DEBT_ISSUANCE_TAG = 'derived_net_debt_issuance'
_DERIVED_NET_INCOME_DEDUCTIONS_TAG = 'derived_net_income_deductions'

# synthetic tag names for the split-only adjusted prices the provider derives from raw prices
# (see Intrinio._build_stock_price_endpoint_table); Intrinio serves raw and fully-adjusted prices
# but not the split-only variant, so these are computed and mapped to their entity fields like any tag
_SPLIT_ADJUSTED_OPEN_TAG = 'split_adjusted_open'
_SPLIT_ADJUSTED_HIGH_TAG = 'split_adjusted_high'
_SPLIT_ADJUSTED_LOW_TAG = 'split_adjusted_low'
_SPLIT_ADJUSTED_CLOSE_TAG = 'split_adjusted_close'
# decimal places kept when rounding away floating-point noise from split-adjusted values
_SPLIT_ADJUSTED_PRICE_DECIMAL_PLACES = 10

# synthetic tag for the split-adjusted dividend the provider derives (see
# Intrinio._build_dividend_endpoint_table); Intrinio serves the raw dividend only
_DERIVED_DIVIDEND_SPLIT_ADJUSTED_TAG = 'derived_dividend_split_adjusted'

# synthetic tags for the point-in-time EBIT and EBITDA the provider derives (see
# Intrinio._add_derived_fundamental_values); Intrinio serves these only in its restated calculations
_DERIVED_EBIT_TAG = 'derived_ebit'
_DERIVED_EBITDA_TAG = 'derived_ebitda'

# synthetic tags for the point-in-time total/net debt and net stock issuance the provider derives
# (see Intrinio._add_derived_fundamental_values); Intrinio serves debt/net-debt only in its restated
# calculations, and does not serve the net equity issuance at all
_DERIVED_TOTAL_DEBT_TAG = 'derived_total_debt'
_DERIVED_NET_DEBT_TAG = 'derived_net_debt'
_DERIVED_NET_COMMON_STOCK_ISSUANCE_TAG = 'derived_net_common_stock_issuance'
_DERIVED_NET_STOCK_ISSUANCE_TAG = 'derived_net_stock_issuance'


@dataclasses.dataclass(frozen=True, slots=True)
class _IntrinioStatementFinancials:
    accepted_date: datetime.datetime | None
    filing_date: datetime.date | None
    fiscal_period: str
    fiscal_year: int | None
    period_end_date: datetime.date
    reported_currency: str | None
    statement_code: str
    values: dict[str, float]


@dataclasses.dataclass(frozen=True, slots=True)
class _IntrinioPeriodRecord:
    reported_currency: str | None
    tag_values: dict[str, typing.Any]


# columns in the Intrinio bulk fundamentals CSVs that carry period metadata rather than line-item tags;
# every other column in a row is a standardized line-item tag whose value feeds the statement record
_BULK_FUNDAMENTAL_METADATA_COLUMNS: typing.Final = frozenset({
    'fundamental_id',
    'company_id',
    'name',
    'cik',
    'ticker',
    'start_date',
    'end_date',
    'months',
    'fiscal_year',
    'fiscal_period',
    'filing_date',
    'updated_date',
    'fundamental_type',
    'currency',
    'reported_currency',
    'pdf_mapping_confidence',
})


@dataclasses.dataclass(frozen=True, slots=True)
class _BulkFundamentalRecord:
    """
    One period's statement parsed from a single Intrinio bulk fundamentals CSV row.

    Carries both the summary-level attributes the selection helpers read
    (`type`, `fiscal_period`, `statement_code`, `fiscal_year`, `filing_date`,
    `updated_date`, `id`) and the statement financials (`period_end_date`,
    `reported_currency`, `accepted_date`, `values`), so one object flows through
    the same `_should_keep_fundamental` / `_select_original_fundamentals` used for
    the live-API summaries and is then converted to an `_IntrinioStatementFinancials`
    for merging. This keeps the bulk path on the exact same downstream pipeline.
    """
    id: str | None
    statement_code: str
    type: str | None
    fiscal_year: int | None
    fiscal_period: str
    filing_date: datetime.date | None
    updated_date: datetime.datetime | None
    period_end_date: datetime.date | None
    reported_currency: str | None
    accepted_date: datetime.datetime | None
    values: dict[str, float]


class Intrinio(
    DataProviderInterface,      # this is the interface all data providers have to implement
):
    # fiscal period whose balance sheet doubles as the fourth-quarter (year-end) balance sheet
    ANNUAL_FISCAL_PERIOD: typing.Final = 'FY'
    # statement code identifying balance sheets in the fundamentals endpoint
    BALANCE_SHEET_STATEMENT_CODE: typing.Final = 'balance_sheet_statement'
    # statement code identifying the derived-metrics statement that carries the adjusted income tags;
    # it is always the latest restated snapshot per period (is_latest, no filing_date, no type)
    CALCULATIONS_STATEMENT_CODE: typing.Final = 'calculations'
    # statement code identifying cash flow statements in the fundamentals endpoint
    CASH_FLOW_STATEMENT_CODE: typing.Final = 'cash_flow_statement'
    # fiscal period Intrinio only reports as calculated statements, never as originally reported ones
    FOURTH_QUARTER_FISCAL_PERIOD: typing.Final = 'Q4'
    # max number of records requested per fundamentals endpoint page
    FUNDAMENTALS_PAGE_SIZE: typing.Final = 10000
    # concurrent standardized-financials requests when fetching a ticker's fundamentals from the API;
    # the SDK retries on rate-limit responses, so this stays comfortably within the per-minute quota
    FUNDAMENTAL_FETCH_CONCURRENCY: typing.Final = 8
    # fundamental types returned by the fundamentals endpoint
    FUNDAMENTAL_TYPE_CALCULATED: typing.Final = 'calculated'
    FUNDAMENTAL_TYPE_REPORTED: typing.Final = 'reported'
    # statement code identifying income statements in the fundamentals endpoint
    INCOME_STATEMENT_CODE: typing.Final = 'income_statement'
    # quarters whose discrete cash flow Intrinio serves only as a calculated statement
    # (a 10-Q's reported cash flow is cumulative year-to-date, not the standalone quarter)
    INTERIM_CASH_FLOW_FISCAL_PERIODS: typing.Final = (
        'Q2',
        'Q3',
    )
    # how many upcoming universe tickers a read-ahead fetches in one go; the curator walks the
    # universe one ticker at a time, so this trades a bounded amount of memory (one batch of a
    # ticker's price/dividend/split records) for removing that many serial round trips
    PREFETCH_BATCH_SIZE: typing.Final = 25
    # concurrent per-ticker requests issued while filling a read-ahead batch; the SDK retries on
    # rate-limit responses, so this stays comfortably within the per-minute quota
    PREFETCH_CONCURRENCY: typing.Final = 8
    # fiscal periods Intrinio reports as originally-filed quarterly statements
    QUARTERLY_REPORTED_FISCAL_PERIODS: typing.Final = (
        'Q1',
        'Q2',
        'Q3',
    )
    # frequency requested from the stock prices endpoint
    STOCK_PRICE_FREQUENCY: typing.Final = 'daily'
    # max number of records requested per stock prices endpoint page
    STOCK_PRICE_PAGE_SIZE: typing.Final = 10000
    # core additive income-statement lines the calculated-Q4 reconciliation guard checks; per-share
    # and weighted-average-share tags are deliberately excluded, they don't sum across quarters
    _RECONCILED_INCOME_TAGS: typing.Final = (
        'totalrevenue',
        'totalgrossprofit',
        'totaloperatingincome',
        'totalpretaxincome',
        'incometaxexpense',
        'netincome',
    )
    # a Q4 reconciliation residue within these tolerances is floating-point noise, not a restatement
    _QUARTER_RECONCILIATION_ABSOLUTE_TOLERANCE: typing.Final = 1.0
    _QUARTER_RECONCILIATION_RELATIVE_TOLERANCE: typing.Final = 1e-6
    # income-statement line fragments that don't sum across quarters (per-share figures and share
    # counts), so the derived fourth quarter can't obtain them by subtracting the interim quarters
    _NON_ADDITIVE_INCOME_TAG_FRAGMENTS: typing.Final = ('eps', 'sharesos', 'pershare')
    # each per-share earnings tag paired with the weighted-average share count it divides by, so the
    # derived fourth quarter's EPS can be recomputed from its own derived net income
    _EARNINGS_PER_SHARE_SHARE_COUNTS: typing.Final = {
        'basiceps': 'weightedavebasicsharesos',
        'dilutedeps': 'weightedavedilutedsharesos',
        'basicdilutedeps': 'weightedavebasicdilutedsharesos',
    }

    _intrinio_sdk: types.ModuleType = intrinio_sdk

    # name fragment identifying the US fundamentals dataset among the account's bulk downloads
    BULK_FUNDAMENTALS_DATASET_NAME_FRAGMENT: typing.Final = 'US Fundamentals'
    # preferred when several US fundamentals datasets exist (the 10+ year one over the 5 year one)
    BULK_FUNDAMENTALS_PREFERRED_DATASET_NAME_FRAGMENT: typing.Final = '10'
    # universe size at or above which downloading the whole-market fundamentals bulk beats per-ticker calls
    BULK_FUNDAMENTALS_MIN_UNIVERSE_SIZE: typing.Final = 10
    # seconds allowed for a single bulk file download before giving up and falling back to the API
    BULK_DOWNLOAD_TIMEOUT_SECONDS: typing.Final = 600
    # concurrent bulk statement-file downloads. Deliberately low and capped by CPU count: each worker
    # holds a compressed archive plus a decompressed CSV block in memory, so parallelism here must not
    # push a modest machine into swapping. Not user-configurable — the safe low default holds for
    # everyone; the fast-machine case isn't worth risking the modest one for.
    BULK_DOWNLOAD_CONCURRENCY: typing.Final = min(2, os.cpu_count() or 1)
    # maps the statement token in a bulk fundamentals file name to the provider's statement code
    _BULK_FILE_STATEMENT_CODES: typing.Final = {
        'INCOME_STATEMENT': INCOME_STATEMENT_CODE,
        'BALANCE_SHEET_STATEMENT': BALANCE_SHEET_STATEMENT_CODE,
        'CASH_FLOW_STATEMENT': CASH_FLOW_STATEMENT_CODE,
        'CALCULATIONS': CALCULATIONS_STATEMENT_CODE,
    }

    # class-level cache of parsed bulk fundamentals, shared across provider instances for one run
    # (the curator builds separate market and fundamental instances), mirroring LsegWorkspace._shared_cache.
    # None until initialize() populates it; stays None when initialize() decides bulk doesn't apply.
    _bulk_fundamentals_cache: typing.ClassVar[dict[str, list] | None] = None
    _bulk_cache_universe_key: typing.ClassVar[tuple[str, ...] | None] = None

    class Endpoints(enum.StrEnum):
        ACCOUNT = 'AccountApi.get_account_current_usage'
        FINANCIALS = 'FundamentalsApi.get_fundamental_standardized_financials'
        FUNDAMENTALS = 'CompanyApi.get_company_fundamentals'
        STOCK_DIVIDENDS = 'SecurityApi.get_security_stock_price_adjustments_dividends'
        STOCK_PRICES = 'SecurityApi.get_security_stock_prices'
        STOCK_SPLITS = 'SecurityApi.get_security_stock_price_adjustments_splits'

    class FundamentalPeriods(enum.StrEnum):
        ANNUAL = 'FY'
        QUARTERLY = 'QTR'

    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_DIVIDENDS: {
            DividendDataRow.ex_dividend_date: 'date',
            DividendDataRow.dividend: 'dividend',
            DividendDataRow.dividend_split_adjusted: _DERIVED_DIVIDEND_SPLIT_ADJUSTED_TAG,
            # declaration_date, record_date and payment_date are intentionally left unmapped:
            # Intrinio's stock price dividend adjustments expose only the ex-date and amount,
            # so those nullable dates stay None (they'd need a different, entitlement-gated endpoint)
        },
    }

    _fundamental_data_endpoint_map : typing.Final[EndpointFieldMap] = {
        Endpoints.FINANCIALS: {
            FundamentalDataRow.accepted_date: 'earnings_disclosed_at',
            FundamentalDataRow.filing_date: 'filing_date',
            FundamentalDataRow.fiscal_period: 'fiscal_period',
            FundamentalDataRow.fiscal_year: 'fiscal_year',
            FundamentalDataRow.period_end_date: 'end_date',

            FundamentalDataRowBalanceSheet.accumulated_other_comprehensive_income_after_tax: 'aoci',
            FundamentalDataRowBalanceSheet.assets: 'totalassets',
            FundamentalDataRowBalanceSheet.capital_lease_obligations: 'capitalleaseobligations',
            FundamentalDataRowBalanceSheet.cash_and_cash_equivalents: 'cashandequivalents',
            FundamentalDataRowBalanceSheet.cash_and_shortterm_investments: _DERIVED_CASH_AND_SHORTTERM_INVESTMENTS_TAG,
            FundamentalDataRowBalanceSheet.common_stock_value: 'commonequity',
            FundamentalDataRowBalanceSheet.current_accounts_payable: 'accountspayable',
            FundamentalDataRowBalanceSheet.current_accounts_receivable_after_doubtful_accounts: 'accountsreceivable',
            FundamentalDataRowBalanceSheet.current_accrued_expenses: 'accruedexpenses',
            FundamentalDataRowBalanceSheet.current_assets: 'totalcurrentassets',
            FundamentalDataRowBalanceSheet.current_liabilities: 'totalcurrentliabilities',
            FundamentalDataRowBalanceSheet.goodwill: 'goodwill',
            FundamentalDataRowBalanceSheet.liabilities: 'totalliabilities',
            FundamentalDataRowBalanceSheet.longterm_debt: 'longtermdebt',
            FundamentalDataRowBalanceSheet.longterm_investments: 'longterminvestments',
            FundamentalDataRowBalanceSheet.net_debt: _DERIVED_NET_DEBT_TAG,
            FundamentalDataRowBalanceSheet.net_intangible_assets_including_goodwill: 'intangibleassets',
            FundamentalDataRowBalanceSheet.net_inventory: 'netinventory',
            FundamentalDataRowBalanceSheet.net_property_plant_and_equipment: 'netppe',
            FundamentalDataRowBalanceSheet.noncontrolling_interest: 'noncontrollinginterests',
            FundamentalDataRowBalanceSheet.noncurrent_assets: 'totalnoncurrentassets',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_revenue: 'noncurrentdeferredrevenue',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_assets: 'noncurrentdeferredtaxassets',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_liabilities: 'noncurrentdeferredtaxliabilities',
            FundamentalDataRowBalanceSheet.noncurrent_liabilities: 'totalnoncurrentliabilities',
            FundamentalDataRowBalanceSheet.other_assets: 'otherassets',
            FundamentalDataRowBalanceSheet.other_current_assets: 'othercurrentassets',
            FundamentalDataRowBalanceSheet.other_current_liabilities: 'othercurrentliabilities',
            FundamentalDataRowBalanceSheet.other_noncurrent_assets: 'othernoncurrentassets',
            FundamentalDataRowBalanceSheet.other_noncurrent_liabilities: 'othernoncurrentliabilities',
            FundamentalDataRowBalanceSheet.preferred_stock_value: 'totalpreferredequity',
            FundamentalDataRowBalanceSheet.prepaid_expenses: 'prepaidexpenses',
            FundamentalDataRowBalanceSheet.retained_earnings: 'retainedearnings',
            FundamentalDataRowBalanceSheet.shortterm_debt: 'shorttermdebt',
            FundamentalDataRowBalanceSheet.shortterm_investments: 'shortterminvestments',
            FundamentalDataRowBalanceSheet.stockholder_equity: 'totalequity',
            FundamentalDataRowBalanceSheet.total_debt_including_capital_lease_obligations: _DERIVED_TOTAL_DEBT_TAG,
            FundamentalDataRowBalanceSheet.total_equity_including_noncontrolling_interest:
                'totalequityandnoncontrollinginterests',
            FundamentalDataRowBalanceSheet.total_liabilities_and_equity: 'totalliabilitiesandequity',
            FundamentalDataRowBalanceSheet.treasury_stock_value: 'treasurystock',

            FundamentalDataRowCashFlow.capital_expenditure: 'purchaseofplantpropertyandequipment',
            FundamentalDataRowCashFlow.cash_and_cash_equivalents_change: 'netchangeincash',
            FundamentalDataRowCashFlow.cash_exchange_rate_effect: 'effectofexchangeratechanges',
            FundamentalDataRowCashFlow.common_stock_issuance_proceeds: 'issuanceofcommonequity',
            FundamentalDataRowCashFlow.common_stock_repurchase: 'repurchaseofcommonequity',
            FundamentalDataRowCashFlow.depreciation_and_amortization: 'depreciationexpense',
            FundamentalDataRowCashFlow.dividend_payments: 'paymentofdividends',
            FundamentalDataRowCashFlow.free_cash_flow: _DERIVED_FREE_CASH_FLOW_TAG,
            FundamentalDataRowCashFlow.interest_payments: 'cashinterestpaid',
            FundamentalDataRowCashFlow.investment_sales_maturities_and_collections_proceeds:
                'saleofinvestments',
            FundamentalDataRowCashFlow.investments_purchase: 'purchaseofinvestments',
            FundamentalDataRowCashFlow.net_business_acquisition_payments: 'acquisitions',
            FundamentalDataRowCashFlow.net_cash_from_operating_activities: 'netcashfromoperatingactivities',
            FundamentalDataRowCashFlow.net_cash_from_investing_activities: 'netcashfrominvestingactivities',
            FundamentalDataRowCashFlow.net_cash_from_financing_activities: 'netcashfromfinancingactivities',
            FundamentalDataRowCashFlow.net_debt_issuance_proceeds: _DERIVED_NET_DEBT_ISSUANCE_TAG,
            FundamentalDataRowCashFlow.net_common_stock_issuance_proceeds: _DERIVED_NET_COMMON_STOCK_ISSUANCE_TAG,
            FundamentalDataRowCashFlow.net_stock_issuance_proceeds: _DERIVED_NET_STOCK_ISSUANCE_TAG,
            FundamentalDataRowCashFlow.net_income: 'netincome',
            FundamentalDataRowCashFlow.net_income_tax_payments: 'cashincometaxespaid',
            FundamentalDataRowCashFlow.other_financing_activities: 'otherfinancingactivitiesnet',
            FundamentalDataRowCashFlow.other_investing_activities: 'otherinvestingactivitiesnet',
            FundamentalDataRowCashFlow.preferred_stock_issuance_proceeds: 'issuanceofpreferredequity',
            FundamentalDataRowCashFlow.property_plant_and_equipment_purchase: 'purchaseofplantpropertyandequipment',
            FundamentalDataRowCashFlow.working_capital_change: 'increasedecreaseinoperatingcapital',

            FundamentalDataRowIncomeStatement.adjusted_basic_and_diluted_earnings_per_share: 'adjbasicdilutedeps',
            FundamentalDataRowIncomeStatement.adjusted_basic_earnings_per_share: 'adjbasiceps',
            FundamentalDataRowIncomeStatement.adjusted_diluted_earnings_per_share: 'adjdilutedeps',
            FundamentalDataRowIncomeStatement.adjusted_weighted_average_basic_and_diluted_shares_outstanding:
                'adjweightedavebasicdilutedsharesos',
            FundamentalDataRowIncomeStatement.adjusted_weighted_average_basic_shares_outstanding:
                'adjweightedavebasicsharesos',
            FundamentalDataRowIncomeStatement.adjusted_weighted_average_diluted_shares_outstanding:
                'adjweightedavedilutedsharesos',
            FundamentalDataRowIncomeStatement.basic_earnings_per_share: 'basiceps',
            FundamentalDataRowIncomeStatement.basic_net_income_available_to_common_stockholders: 'netincometocommon',
            FundamentalDataRowIncomeStatement.continuing_operations_income_after_tax: 'netincomecontinuing',
            FundamentalDataRowIncomeStatement.cost_of_revenue: 'totalcostofrevenue',
            FundamentalDataRowIncomeStatement.costs_and_expenses: _DERIVED_COSTS_AND_EXPENSES_TAG,
            FundamentalDataRowIncomeStatement.depreciation_and_amortization: 'depreciationexpense',
            FundamentalDataRowIncomeStatement.diluted_earnings_per_share: 'dilutedeps',
            FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax: 'netincomediscontinued',
            FundamentalDataRowIncomeStatement.earnings_before_interest_and_tax: _DERIVED_EBIT_TAG,
            FundamentalDataRowIncomeStatement.earnings_before_interest_tax_depreciation_and_amortization:
                _DERIVED_EBITDA_TAG,
            FundamentalDataRowIncomeStatement.gross_profit: 'totalgrossprofit',
            FundamentalDataRowIncomeStatement.income_before_tax: 'totalpretaxincome',
            FundamentalDataRowIncomeStatement.income_tax_expense: 'incometaxexpense',
            FundamentalDataRowIncomeStatement.interest_expense: 'totalinterestexpense',
            FundamentalDataRowIncomeStatement.interest_income: 'totalinterestincome',
            FundamentalDataRowIncomeStatement.net_income: 'netincome',
            FundamentalDataRowIncomeStatement.net_income_deductions: _DERIVED_NET_INCOME_DEDUCTIONS_TAG,
            FundamentalDataRowIncomeStatement.net_interest_income: 'netinterestincome',
            FundamentalDataRowIncomeStatement.net_total_other_income: 'totalotherincome',
            FundamentalDataRowIncomeStatement.operating_expenses: 'totaloperatingexpenses',
            FundamentalDataRowIncomeStatement.operating_income: 'totaloperatingincome',
            FundamentalDataRowIncomeStatement.research_and_development_expense: 'rdexpense',
            FundamentalDataRowIncomeStatement.revenues: 'totalrevenue',
            FundamentalDataRowIncomeStatement.selling_general_and_administrative_expense: 'sgaexpense',
            FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding: 'weightedavebasicsharesos',
            FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding: 'weightedavedilutedsharesos',

            # The adjusted income tags mapped above are served only by Intrinio's `calculations`
            # statement, which the provider now fetches and merges per period. That statement is a
            # single latest snapshot (always is_latest, no filing_date, no reported/restated versions),
            # so its values track the restated vintage; this is acceptable for the adjusted figures,
            # which are computed after the fact and can't be point-in-time regardless of vintage.
            #
            # EBIT, EBITDA, net_debt and total_debt all live only in the restated `calculations`
            # statement, but rather than read them from there (which would inject restated-era data and
            # break point-in-time integrity) they are derived point-in-time in
            # `_add_derived_fundamental_values` (EBIT = pretax income + interest expense - interest
            # income; EBITDA = EBIT + D&A; total_debt = short-term + long-term debt; net_debt =
            # total_debt - cash - short-term investments) and mapped above.
            #
            # depreciation_and_amortization is NOT taken from the restated `depreciationandamortization`;
            # it is filled point-in-time from the reported cash flow's `depreciationexpense` (mapped above).
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
            MarketDataDailyRow.open_split_adjusted: _SPLIT_ADJUSTED_OPEN_TAG,
            MarketDataDailyRow.high_split_adjusted: _SPLIT_ADJUSTED_HIGH_TAG,
            MarketDataDailyRow.low_split_adjusted: _SPLIT_ADJUSTED_LOW_TAG,
            MarketDataDailyRow.close_split_adjusted: _SPLIT_ADJUSTED_CLOSE_TAG,
            # split-only adjusted volume needs no derivation: Intrinio's adj_volume is already
            # adjusted for splits alone, since dividends never change share counts
            MarketDataDailyRow.volume_split_adjusted: 'adj_volume',
            MarketDataDailyRow.open: 'open',
            MarketDataDailyRow.high: 'high',
            MarketDataDailyRow.low: 'low',
            MarketDataDailyRow.close: 'close',
            MarketDataDailyRow.volume: 'volume',
            # vwap, vwap_split_adjusted and vwap_dividend_and_split_adjusted are intentionally left
            # unmapped: Intrinio's stock price endpoint does not expose VWAP, so they stay None
        },
    }

    _split_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_SPLITS: {
            SplitDataRow.split_date: 'date',
            SplitDataRow.numerator: PreprocessedFieldMapping(
                ['split_ratio'],
                [DataProviderFieldPreprocessors.extract_ratio_numerator]
            ),
            SplitDataRow.denominator: PreprocessedFieldMapping(
                ['split_ratio'],
                [DataProviderFieldPreprocessors.extract_ratio_denominator]
            ),
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
        self._intrinio_sdk.ApiClient().allow_retries(setting=True)
        # Per-ticker endpoint records, keyed by (identifier, start date, end date). These serve two
        # purposes: get_dividend_data and get_split_data need the same split adjustments and the
        # curator calls both on this same instance, and `initialize` lets each cache be filled a
        # whole read-ahead batch at a time, so the curator's sequential ticker loop stops paying one
        # serial round trip per ticker. Each cache holds at most one batch, see _resolve_with_read_ahead.
        self._stock_price_cache: dict[
            tuple[str, datetime.date, datetime.date],
            list[intrinio_sdk.models.stock_price_summary.StockPriceSummary],
        ] = {}
        self._dividend_records_cache: dict[
            tuple[str, datetime.date, datetime.date],
            list[intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment],
        ] = {}
        self._split_records_cache: dict[
            tuple[str, datetime.date, datetime.date],
            list[intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment],
        ] = {}
        # the configured universe in curator order, recorded by `initialize`; empty until then, in
        # which case each ticker is resolved on its own with no read-ahead
        self._prefetch_identifiers: tuple[str, ...] = ()

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
        dividend_records = self._resolve_dividends(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        # Split-adjusting a dividend needs the splits that followed its ex-date, which the dividend
        # adjustments themselves don't carry, so the securities' split adjustments are fetched too.
        split_records = self._resolve_splits(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        endpoint_tables = {
            self.Endpoints.STOCK_DIVIDENDS: self._build_dividend_endpoint_table(
                dividend_records=dividend_records,
                split_records=split_records,
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
        period_mode = self.FundamentalPeriods[period.upper()]
        statement_financials = self._collect_statement_financials(
            main_identifier=main_identifier,
            period_mode=period_mode,
        )
        if period_mode is self.FundamentalPeriods.QUARTERLY:
            # Derive the discrete fourth quarter ourselves from the reported statements instead of
            # trusting Intrinio's calculated one (which can be built from restated quarters and break
            # point-in-time integrity), verify the result reconciles, then drop the annual income and
            # cash flow statements (kept only as derivation inputs) so they never become emitted rows
            # nor collide with the fourth quarter at the shared year-end period.
            statement_financials = self._derive_discrete_fourth_quarters(statement_financials)
            self._reconcile_calculated_quarters(
                main_identifier=main_identifier,
                income_statements=[
                    statement
                    for statement in statement_financials
                    if statement.statement_code == self.INCOME_STATEMENT_CODE
                ],
            )
            statement_financials = [
                statement
                for statement in statement_financials
                if not (
                    statement.fiscal_period == self.ANNUAL_FISCAL_PERIOD
                    and statement.statement_code in (
                        self.INCOME_STATEMENT_CODE,
                        self.CASH_FLOW_STATEMENT_CODE,
                    )
                )
            ]

        return self._assemble_fundamental_data(
            main_identifier=main_identifier,
            statement_financials=statement_financials,
        )

    def _collect_statement_financials(
        self,
        *,
        main_identifier: str,
        period_mode: "Intrinio.FundamentalPeriods",
    ) -> list[_IntrinioStatementFinancials]:
        """
        Collect a ticker's selected as-reported statement financials.

        Uses the shared bulk cache when `initialize` populated it and it holds the
        ticker; otherwise falls back to the per-ticker API. Both routes apply the
        same `_should_keep_fundamental` filter and `_select_original_fundamentals`
        selection, so the assembled result is identical.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        period_mode
            Whether annual or quarterly statements were requested

        Returns
        -------
        The selected statement financials for the ticker
        """
        if (
            self._bulk_fundamentals_cache is not None
            and main_identifier in self._bulk_fundamentals_cache
        ):
            return self._bulk_statement_financials(
                main_identifier=main_identifier,
                period_mode=period_mode,
            )

        return self._api_statement_financials(
            main_identifier=main_identifier,
            period_mode=period_mode,
        )

    def _api_statement_financials(
        self,
        *,
        main_identifier: str,
        period_mode: "Intrinio.FundamentalPeriods",
    ) -> list[_IntrinioStatementFinancials]:
        """
        Fetch and build a ticker's selected statement financials from the per-ticker API.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        period_mode
            Whether annual or quarterly statements were requested

        Returns
        -------
        The selected statement financials, empty when the endpoint returns none
        """
        fundamental_summaries = self._request_fundamentals(
            main_identifier=main_identifier,
        )
        filtered_summaries = [
            summary
            for summary in fundamental_summaries
            if self._should_keep_fundamental(
                summary,
                period_mode=period_mode,
            )
        ]
        if not filtered_summaries:
            msg = f"{main_identifier} fundamentals endpoint returned no statements"
            logging.getLogger(__name__).warning(msg)

            return []

        original_summaries = self._select_original_fundamentals(filtered_summaries)
        # the per-period standardized-financials requests are independent, so fetch them
        # concurrently instead of one by one; this is the main cost of the per-ticker path
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.FUNDAMENTAL_FETCH_CONCURRENCY,
        ) as executor:
            standardized_responses = list(
                executor.map(
                    lambda summary: self._request_standardized_financials(
                        fundamental_id=summary.id,
                    ),
                    original_summaries,
                )
            )

        return [
            self._build_statement_financials(standardized_response)
            for standardized_response in standardized_responses
        ]

    def _bulk_statement_financials(
        self,
        *,
        main_identifier: str,
        period_mode: "Intrinio.FundamentalPeriods",
    ) -> list[_IntrinioStatementFinancials]:
        """
        Select a ticker's statement financials from the shared bulk cache.

        Applies the same period/type filter the API path uses. Point-in-time
        statements (reported and calculated) go through `_select_original_fundamentals`
        to keep the as-originally-reported vintage, while the `calculations` statement
        — which the bulk carries in several restated versions — is collapsed to its
        most recent one, matching the single `is_latest` snapshot the API returns.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        period_mode
            Whether annual or quarterly statements were requested

        Returns
        -------
        The selected statement financials for the ticker
        """
        cached_records = self._bulk_fundamentals_cache[main_identifier]
        kept_records = [
            record
            for record in cached_records
            if self._should_keep_fundamental(
                record,
                period_mode=period_mode,
            )
        ]
        point_in_time_records = [
            record
            for record in kept_records
            if record.statement_code != self.CALCULATIONS_STATEMENT_CODE
        ]
        calculations_records = [
            record
            for record in kept_records
            if record.statement_code == self.CALCULATIONS_STATEMENT_CODE
        ]
        selected_records = (
            self._select_original_fundamentals(point_in_time_records)
            + self._select_latest_bulk_calculations(calculations_records)
        )

        return [
            self._bulk_record_to_statement_financials(record)
            for record in selected_records
        ]

    @classmethod
    def _select_latest_bulk_calculations(
        cls,
        calculations_records: list[_BulkFundamentalRecord],
    ) -> list[_BulkFundamentalRecord]:
        """
        Keep only the most recent `calculations` record per period.

        The bulk carries the derived-metrics statement in several restated versions
        per period, whereas the API returns a single `is_latest` one, so the newest
        by update then filing date is kept to match that behaviour.

        Parameters
        ----------
        calculations_records
            The kept `calculations` bulk records

        Returns
        -------
        One `calculations` record per period, the most recent
        """
        latest_by_period: dict[tuple, _BulkFundamentalRecord] = {}
        for record in calculations_records:
            period_key = (record.fiscal_year, record.fiscal_period)
            current_record = latest_by_period.get(period_key)
            if (
                current_record is None
                or cls._bulk_calculations_sort_key(record) > cls._bulk_calculations_sort_key(current_record)
            ):
                latest_by_period[period_key] = record

        return list(latest_by_period.values())

    @staticmethod
    def _bulk_calculations_sort_key(
        record: _BulkFundamentalRecord,
    ) -> tuple:
        """
        Order `calculations` bulk records by recency, ranking missing dates as earliest.

        The presence flags come before each date so a missing date sorts before any
        present one without ever comparing None against a date.
        """
        return (
            record.updated_date is not None,
            record.updated_date,
            record.filing_date is not None,
            record.filing_date,
        )

    @staticmethod
    def _bulk_record_to_statement_financials(
        record: _BulkFundamentalRecord,
    ) -> _IntrinioStatementFinancials:
        """
        Convert a selected bulk record into the statement financials the merge consumes.
        """
        return _IntrinioStatementFinancials(
            accepted_date=record.accepted_date,
            filing_date=record.filing_date,
            fiscal_period=record.fiscal_period,
            fiscal_year=record.fiscal_year,
            period_end_date=record.period_end_date,
            reported_currency=record.reported_currency,
            statement_code=record.statement_code,
            values=record.values,
        )

    def _assemble_fundamental_data(
        self,
        *,
        main_identifier: str,
        statement_financials: list[_IntrinioStatementFinancials],
    ) -> FundamentalData:
        """
        Assemble a FundamentalData entity from selected statement financials.

        Groups the statements by period end date, merges each period (dropping the
        incomplete ones and warning), and runs the resulting records through the
        shared remapping and consolidation pipeline. Shared by the bulk and API
        paths so both produce identical output.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        statement_financials
            The selected statement financials to assemble

        Returns
        -------
        The FundamentalData entity, empty when there's nothing to assemble
        """
        empty_fundamental_data = FundamentalData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={},
        )
        if not statement_financials:
            return empty_fundamental_data

        period_statements = {}
        for statement in statement_financials:
            period_statements.setdefault(
                statement.period_end_date,
                [],
            ).append(statement)

        period_records = []
        incomplete_statements = []
        for statements in period_statements.values():
            period_record = self._merge_period_statements(
                period_statements=statements,
            )
            if period_record is None:
                incomplete_statements.append(statements[0])

                continue

            period_records.append(period_record)

        if incomplete_statements:
            incomplete_descriptions = [
                " ".join([
                    f"{statement.fiscal_year} {statement.fiscal_period}",
                    f"(period ending {statement.period_end_date})",
                ])
                for statement in incomplete_statements
            ]
            msg = "\n".join([
                " ".join([
                    f"{main_identifier} fundamentals endpoint returned incomplete periods",
                    "(missing an income statement or a filing date), omitting the following periods:",
                ]),
                *incomplete_descriptions,
            ])
            logging.getLogger(__name__).warning(msg)

        if not period_records:
            return empty_fundamental_data

        sorted_period_records = sorted(
            period_records,
            key=lambda period_record: (
                period_record.tag_values['filing_date'],
                period_record.tag_values['end_date'],
            ),
        )
        # `pyarrow.Table.from_pylist` infers its columns from the first record alone, so any tag
        # the earliest period omits (a line item it didn't report) would be dropped for every later
        # period too. Normalize each record against the union of all tags, filling the gaps with
        # None, so every reported tag survives as its own column.
        all_tag_names = sorted({
            tag_name
            for period_record in sorted_period_records
            for tag_name in period_record.tag_values
        })
        normalized_period_tag_values = [
            {
                tag_name: period_record.tag_values.get(tag_name)
                for tag_name in all_tag_names
            }
            for period_record in sorted_period_records
        ]
        endpoint_tables = {
            self.Endpoints.FINANCIALS: pyarrow.Table.from_pylist(normalized_period_tag_values),
        }

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=FundamentalsDataBlock,
                endpoint_field_map=self._fundamental_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError:
            msg = f"{main_identifier} fundamental data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_fundamental_data

        consolidated_fundamental_table = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[
                FundamentalsDataBlock.clock_sync_field,
                FundamentalDataRow.period_end_date,
            ],
        )
        # reported_currency has no single provider tag (it lives in each line item's unit), so it can't
        # go through the tag-based field map; append it as its own column aligned to the sorted records
        reported_currency_column = pyarrow.array(
            [
                period_record.reported_currency
                for period_record in sorted_period_records
            ],
            type=pyarrow.string(),
        )
        consolidated_fundamental_table_with_currency = consolidated_fundamental_table.append_column(
            FundamentalsDataBlock.get_field_qualified_name(FundamentalDataRow.reported_currency),
            reported_currency_column,
        )
        fundamental_data = FundamentalsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_fundamental_table_with_currency,
            common_field_data={
                FundamentalData: {
                    FundamentalData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return fundamental_data  # noqa: RET504

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
        stock_price_records = self._resolve_stock_prices(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        endpoint_tables = {
            self.Endpoints.STOCK_PRICES: self._build_stock_price_endpoint_table(
                records=stock_price_records,
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
        split_records = self._resolve_splits(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        split_field_mappings = list(
            self._split_data_endpoint_map[self.Endpoints.STOCK_SPLITS].values()
        )
        endpoint_tables = {
            self.Endpoints.STOCK_SPLITS: self._create_endpoint_table_from_records(
                records=split_records,
                field_mappings=split_field_mappings,
            ),
        }
        empty_split_data = SplitData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={},
        )

        try:
            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=SplitsDataBlock,
                endpoint_field_map=self._split_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
        except DataProviderToolkitNoDataError:
            msg = f"{main_identifier} split data endpoints returned no data"
            logging.getLogger(__name__).warning(msg)

            return empty_split_data

        consolidated_split_data_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[SplitsDataBlock.clock_sync_field],
            predominant_order_descending=True,
        )
        consolidated_split_data = consolidated_split_data_descending[::-1]
        split_data = SplitsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_split_data,
            common_field_data={
                SplitData: {
                    SplitData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return split_data  # noqa: RET504

    def initialize(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        """
        Prime the shared bulk fundamentals cache for the configured universe.

        For a universe of at least `BULK_FUNDAMENTALS_MIN_UNIVERSE_SIZE` identifiers,
        one whole-market fundamentals bulk download is far cheaper than the
        per-ticker two-step API, so it is fetched once here and cached at class level
        (shared across the separate market and fundamental provider instances the
        curator builds). Each `get_fundamental_data` call then reads its ticker's
        statements from that cache instead of calling the API. A smaller universe, a
        missing bulk entitlement, or any download failure leaves the cache unset and
        the provider transparently falls back to the per-ticker API path.

        Parameters
        ----------
        configuration
            The Configuration entity with the universe identifiers and date range

        Returns
        -------
        None
        """
        identifiers = tuple(configuration.identifiers)
        # Record the universe in curator order so the per-ticker endpoints (prices, dividends,
        # splits) can read ahead in batches. This is independent of the fundamentals bulk below,
        # so it is set before any of its early returns.
        self._prefetch_identifiers = identifiers
        universe_key = tuple(sorted(identifiers))
        if (
            self._bulk_fundamentals_cache is not None
            and self._bulk_cache_universe_key == universe_key
        ):
            return

        if len(identifiers) < self.BULK_FUNDAMENTALS_MIN_UNIVERSE_SIZE:
            return

        try:
            bulk_fundamentals_cache = self._download_bulk_fundamentals(
                tickers=frozenset(identifiers),
            )
        except (intrinio_sdk.rest.ApiException, OSError, ValueError) as error:
            msg = " ".join([
                "Intrinio bulk fundamentals unavailable, falling back to the per-ticker API:",
                str(error),
            ])
            logging.getLogger(__name__).warning(msg)

            return

        self._store_bulk_fundamentals_cache(
            bulk_fundamentals_cache=bulk_fundamentals_cache,
            universe_key=universe_key,
        )

    @classmethod
    def _store_bulk_fundamentals_cache(
        cls,
        *,
        bulk_fundamentals_cache: dict[str, list[_BulkFundamentalRecord]],
        universe_key: tuple[str, ...],
    ) -> None:
        """
        Store the parsed bulk fundamentals in the shared class-level cache.

        Parameters
        ----------
        bulk_fundamentals_cache
            The parsed statement records grouped by ticker
        universe_key
            The sorted universe identifiers the cache was built for

        Returns
        -------
        None
        """
        cls._bulk_fundamentals_cache = bulk_fundamentals_cache
        cls._bulk_cache_universe_key = universe_key

    def _download_bulk_fundamentals(
        self,
        *,
        tickers: frozenset[str],
    ) -> dict[str, list[_BulkFundamentalRecord]]:
        """
        Download and parse the US fundamentals bulk into a per-ticker statement cache.

        Resolves the account's active bulk download links, selects the statement
        files of both sector datasets (financial `US_FIN_*` and industrial
        `US_INDU_*`), streams each one, and accumulates the parsed statement records
        for the wanted tickers. The result feeds the same selection and merge
        pipeline the per-ticker API path uses.

        Parameters
        ----------
        tickers
            The universe tickers whose statements to keep

        Returns
        -------
        The parsed statement records grouped by ticker

        Raises
        ------
        ValueError
            When the account exposes no US fundamentals bulk download
        """
        file_links = self._select_fundamentals_bulk_file_links(
            self._request_bulk_download_links()
        )
        if not file_links:
            msg = "Intrinio account exposes no US fundamentals bulk download"

            raise ValueError(msg)

        # The statement files are independent, so download and parse them concurrently instead of one
        # after another. Any file failing propagates out of the map iteration, so initialize() falls
        # back to the per-ticker API exactly as it would have for a sequential failure. The merge runs
        # on this thread, so no shared state is mutated concurrently.
        bulk_fundamentals_cache: dict[str, list[_BulkFundamentalRecord]] = {}
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.BULK_DOWNLOAD_CONCURRENCY,
        ) as executor:
            parsed_files = executor.map(
                lambda file_link: self._download_and_parse_bulk_file(
                    url=file_link[0],
                    statement_code=file_link[1],
                    tickers=tickers,
                ),
                file_links,
            )
            for statement_records_by_ticker in parsed_files:
                for (ticker, records) in statement_records_by_ticker.items():
                    bulk_fundamentals_cache.setdefault(ticker, []).extend(records)

        return bulk_fundamentals_cache

    def _request_bulk_download_links(
        self,
    ) -> typing.Any:
        """
        Fetch the account's active bulk download links.

        Returns
        -------
        The SDK bulk download links response
        """
        bulk_downloads_api = self._intrinio_sdk.BulkDownloadsApi()

        return bulk_downloads_api.get_bulk_download_links()

    @classmethod
    def _select_fundamentals_bulk_file_links(
        cls,
        bulk_download_links: typing.Any,
    ) -> list[tuple[str, str]]:
        """
        Select the US fundamentals statement files and their statement codes.

        Chooses the US fundamentals dataset (preferring the widest history when
        several are offered), then pairs each of its statement files with the
        provider statement code its name denotes; files that aren't one of the
        needed statements are ignored.

        Parameters
        ----------
        bulk_download_links
            The SDK bulk download links response

        Returns
        -------
        Each needed file's download URL paired with its statement code
        """
        datasets = getattr(bulk_download_links, 'bulk_downloads', None) or []
        fundamentals_datasets = [
            dataset
            for dataset in datasets
            if cls.BULK_FUNDAMENTALS_DATASET_NAME_FRAGMENT in (getattr(dataset, 'name', '') or '')
        ]
        if not fundamentals_datasets:
            return []

        preferred_datasets = [
            dataset
            for dataset in fundamentals_datasets
            if cls.BULK_FUNDAMENTALS_PREFERRED_DATASET_NAME_FRAGMENT in (getattr(dataset, 'name', '') or '')
        ]
        selected_dataset = (preferred_datasets or fundamentals_datasets)[0]

        file_links = []
        for link in (getattr(selected_dataset, 'links', None) or []):
            url = getattr(link, 'url', None)
            statement_code = cls._match_bulk_file_statement_code(
                (getattr(link, 'name', '') or '').upper()
            )
            if (
                url
                and statement_code is not None
            ):
                file_links.append((url, statement_code))

        return file_links

    @classmethod
    def _match_bulk_file_statement_code(
        cls,
        file_name: str,
    ) -> str | None:
        """
        Return the statement code a bulk file name denotes, or None if not a needed statement.

        Parameters
        ----------
        file_name
            The bulk file name, upper-cased

        Returns
        -------
        The provider statement code, or None
        """
        for (token, statement_code) in cls._BULK_FILE_STATEMENT_CODES.items():
            if token in file_name:
                return statement_code

        return None

    @classmethod
    def _download_bulk_archive_bytes(
        cls,
        url: str,
    ) -> bytes:
        """
        Download a bulk statement file's archive bytes over HTTPS.

        Parameters
        ----------
        url
            The bulk file download URL

        Returns
        -------
        The downloaded .zip archive's raw bytes

        Raises
        ------
        ValueError
            When the URL isn't HTTPS
        """
        if not url.lower().startswith('https://'):
            msg = f"Refusing to download an Intrinio bulk file from a non-HTTPS URL: {url}"

            raise ValueError(msg)

        with urllib.request.urlopen(url, timeout=cls.BULK_DOWNLOAD_TIMEOUT_SECONDS) as response:  # noqa: S310
            return response.read()

    @classmethod
    def _download_and_parse_bulk_file(
        cls,
        *,
        url: str,
        statement_code: str,
        tickers: frozenset[str],
    ) -> dict[str, list[_BulkFundamentalRecord]]:
        """
        Download one bulk .zip and parse its data CSV (not the `*_KEY.csv`) into records.

        Streams the decompressed statement CSV straight into the PyArrow parser, so
        only the compressed archive and one decompressed CSV block are held in memory
        at a time, keeping the per-file footprint small enough to run several of these
        concurrently.

        Parameters
        ----------
        url
            The bulk file download URL
        statement_code
            The provider statement code this file holds (e.g. income_statement)
        tickers
            The universe tickers whose rows to keep

        Returns
        -------
        The file's parsed statement records grouped by ticker

        Raises
        ------
        ValueError
            When the URL isn't HTTPS or the archive holds no data CSV
        """
        archive = zipfile.ZipFile(io.BytesIO(cls._download_bulk_archive_bytes(url)))
        data_file_names = [
            name
            for name in archive.namelist()
            if not name.endswith('_KEY.csv')
        ]
        if not data_file_names:
            msg = f"Intrinio bulk archive at {url} contained no data CSV"

            raise ValueError(msg)

        with archive.open(data_file_names[0]) as data_stream:
            return cls._parse_bulk_fundamental_statements(
                source=data_stream,
                statement_code=statement_code,
                tickers=tickers,
            )

    def validate_api_key(
        self,
    ) -> bool | None:
        """
        Validate that the API key used to init the class is valid, by making a test request.

        Requests the account's current usage, which requires a valid API key but no particular
        subscription, and treats the account details it returns as proof the key was accepted.

        Returns
        -------
        Whether `api_key` is valid
        """
        account_api = self._intrinio_sdk.AccountApi()

        try:
            response = account_api.get_account_current_usage()
        except intrinio_sdk.rest.ApiException as error:
            msg = " ".join([
                f"Intrinio account usage endpoint returned HTTP status {error.status}",
                f"while validating the API key: {error.reason}",
            ])
            logging.getLogger(__name__).warning(msg)

            return False

        return (
            response is not None
            and response.account is not None
        )

    @staticmethod
    def _add_derived_fundamental_values(
        tag_values: dict[str, typing.Any],
    ) -> None:
        """
        Inject provider-derived, point-in-time fundamental line items into a period's tag values.

        Intrinio's reported statements omit several line items the curator's contract
        defines but that providers like Financial Modeling Prep serve pre-computed. The
        ones that are exact functions of as-reported values are derived here from the
        period's own reported tags, rather than read from Intrinio's `calculations`
        statement (which is always the latest restated snapshot and would break
        point-in-time integrity). Each derived value is stored under a `derived_`
        synthetic tag that `_fundamental_data_endpoint_map` maps to its entity field.

        Flow items (net debt issuance, free cash flow) treat an absent component as a
        zero flow, since a cash flow statement omits a flow line precisely when there
        was no such flow in the period. Stock and total items (cash and short-term
        investments, total costs and expenses, net income deductions) instead require
        every component to be present, so an omitted line yields no value rather than a
        silently understated total.

        Parameters
        ----------
        tag_values
            The period's merged reported tag values, mutated in place
        """
        # Flow items: sum the reported components, treating an absent one as a zero flow,
        # and only emit the derived value when at least one component was reported.
        net_debt_components = [
            tag_values[tag]
            for tag in ('issuanceofdebt', 'repaymentofdebt')
            if tag_values.get(tag) is not None
        ]
        if net_debt_components:
            tag_values[_DERIVED_NET_DEBT_ISSUANCE_TAG] = sum(net_debt_components)

        operating_cash_flow = tag_values.get('netcashfromoperatingactivities')
        if operating_cash_flow is not None:
            # capital expenditure is reported as a negative outflow, so adding it nets it out
            capital_expenditure = tag_values.get('purchaseofplantpropertyandequipment') or 0
            tag_values[_DERIVED_FREE_CASH_FLOW_TAG] = operating_cash_flow + capital_expenditure

        # Stock and total items: derive only when every component is present, so an
        # omitted line never produces a silently understated total.
        cost_of_revenue = tag_values.get('totalcostofrevenue')
        operating_expenses = tag_values.get('totaloperatingexpenses')
        if (
            cost_of_revenue is not None
            and operating_expenses is not None
        ):
            tag_values[_DERIVED_COSTS_AND_EXPENSES_TAG] = cost_of_revenue + operating_expenses

        net_income = tag_values.get('netincome')
        net_income_to_common = tag_values.get('netincometocommon')
        if (
            net_income is not None
            and net_income_to_common is not None
        ):
            tag_values[_DERIVED_NET_INCOME_DEDUCTIONS_TAG] = net_income - net_income_to_common

        cash_and_equivalents = tag_values.get('cashandequivalents')
        shortterm_investments = tag_values.get('shortterminvestments')
        if (
            cash_and_equivalents is not None
            and shortterm_investments is not None
        ):
            tag_values[_DERIVED_CASH_AND_SHORTTERM_INVESTMENTS_TAG] = (
                cash_and_equivalents + shortterm_investments
            )

        # EBIT = pre-tax income + interest expense - interest income (Intrinio's own convention,
        # verified live to the dollar on NVDA and AAPL). Issuers that fold interest into "other
        # income" expose no interest tags, so an absent interest line is treated as a zero flow,
        # which makes EBIT equal pre-tax income exactly as Intrinio computes it. EBITDA = EBIT + the
        # point-in-time D&A taken from the reported cash flow's `depreciationexpense`.
        pretax_income = tag_values.get('totalpretaxincome')
        if pretax_income is not None:
            interest_expense = tag_values.get('totalinterestexpense') or 0
            interest_income = tag_values.get('totalinterestincome') or 0
            ebit = pretax_income + interest_expense - interest_income
            tag_values[_DERIVED_EBIT_TAG] = ebit
            depreciation_and_amortization = tag_values.get('depreciationexpense')
            if depreciation_and_amortization is not None:
                tag_values[_DERIVED_EBITDA_TAG] = ebit + depreciation_and_amortization

        # Total debt = short-term debt + long-term debt (Intrinio already includes capital leases in
        # long-term debt, so they are not added again). Net debt then subtracts cash and short-term
        # investments. Both verified to the dollar vs Intrinio's `debt`/`netdebt` on NVDA and AAPL.
        debt_components = [
            tag_values[tag]
            for tag in ('shorttermdebt', 'longtermdebt')
            if tag_values.get(tag) is not None
        ]
        if debt_components:
            total_debt = sum(debt_components)
            tag_values[_DERIVED_TOTAL_DEBT_TAG] = total_debt
            cash_and_equivalents = tag_values.get('cashandequivalents')
            if cash_and_equivalents is not None:
                shortterm_investments_held = tag_values.get('shortterminvestments') or 0
                tag_values[_DERIVED_NET_DEBT_TAG] = (
                    total_debt - cash_and_equivalents - shortterm_investments_held
                )

        # Net equity issuance = issuance net of repurchase (a repurchase is a negative flow, so summing
        # nets it out); an absent component is treated as a zero flow. The common variant nets only the
        # common lines, the total variant nets preferred in too.
        net_common_stock_components = [
            tag_values[tag]
            for tag in ('issuanceofcommonequity', 'repurchaseofcommonequity')
            if tag_values.get(tag) is not None
        ]
        if net_common_stock_components:
            tag_values[_DERIVED_NET_COMMON_STOCK_ISSUANCE_TAG] = sum(net_common_stock_components)

        net_stock_components = [
            tag_values[tag]
            for tag in (
                'issuanceofcommonequity',
                'issuanceofpreferredequity',
                'repurchaseofcommonequity',
                'repurchaseofpreferredequity',
            )
            if tag_values.get(tag) is not None
        ]
        if net_stock_components:
            tag_values[_DERIVED_NET_STOCK_ISSUANCE_TAG] = sum(net_stock_components)

    @classmethod
    def _build_dividend_endpoint_table(
        cls,
        *,
        dividend_records: list[typing.Any],
        split_records: list[typing.Any],
    ) -> pyarrow.Table:
        """
        Build the dividends endpoint table, adding the split-adjusted dividend column.

        Intrinio's dividend adjustments carry the cash dividend per share but not
        its split-adjusted counterpart, which the curator's contract also defines.
        A dividend split-adjusts the same way a price does: it is scaled by the
        product of the split ratios of every split after its ex-date, expressing it
        in the share basis those later splits produced. The split ratios come from
        the securities' split adjustments, since the dividend adjustments never
        carry them.

        Some issuers (for example ADRs paying a regular plus a special dividend)
        report more than one dividend on a single ex-date; those are summed so each
        ex-date yields one total dividend, since the dividend entity is keyed by
        ex-date. Each date keeps the position of its first occurrence, preserving the
        endpoint's native ordering for the downstream consolidation. Records without a
        dividend amount can't form a valid dividend row and are dropped.

        Parameters
        ----------
        dividend_records
            The dividend adjustment records returned by the endpoint
        split_records
            The split adjustment records used to split-adjust the dividends

        Returns
        -------
        The endpoint table including the raw and split-adjusted dividends
        """
        later_splits = [
            (record.date, record.split_ratio)
            for record in split_records
            if (
                record.split_ratio is not None
                and record.split_ratio != 1.0
            )
        ]
        total_dividend_by_date = {}
        for record in dividend_records:
            if record.dividend is None:
                continue
            total_dividend_by_date[record.date] = (
                total_dividend_by_date.get(record.date, 0.0)
                + record.dividend
            )

        rows = []
        for (date, dividend) in total_dividend_by_date.items():
            cumulative_split_factor = 1.0
            for (split_date, split_ratio) in later_splits:
                if split_date > date:
                    cumulative_split_factor *= split_ratio
            rows.append({
                # rounding clears the floating-point noise summing dividends can introduce
                'date': date,
                'dividend': round(dividend, _SPLIT_ADJUSTED_PRICE_DECIMAL_PLACES),
                _DERIVED_DIVIDEND_SPLIT_ADJUSTED_TAG: cls._scale_by_split_factor(
                    dividend,
                    cumulative_split_factor,
                ),
            })

        return pyarrow.Table.from_pylist(rows)

    @classmethod
    def _parse_bulk_fundamental_statements(
        cls,
        *,
        source: typing.BinaryIO,
        statement_code: str,
        tickers: frozenset[str] | None = None,
    ) -> dict[str, list[_BulkFundamentalRecord]]:
        """
        Parse one Intrinio bulk fundamentals CSV file into per-ticker statement records.

        The file is a wide CSV: the columns in `_BULK_FUNDAMENTAL_METADATA_COLUMNS`
        describe the period and every other column is a standardized line-item tag.
        PyArrow reads and type-converts the whole file in native code, which is far
        cheaper than a Python row-by-row parse of a whole-market file. When `tickers`
        is given, each record batch is filtered to that universe before its rows are
        materialized as Python objects, so the millions of unwanted rows never leave
        native memory. Rows without a ticker are skipped, non-numeric or empty tag
        cells are dropped, and `statement_code` is fixed per file since each file
        holds a single statement type.

        Parameters
        ----------
        source
            A binary file-like object of the decompressed statement CSV
        statement_code
            The provider statement code for this file (e.g. income_statement)
        tickers
            When set, only rows whose ticker is in this set are materialized

        Returns
        -------
        The parsed statement records grouped by ticker
        """
        # PyArrow's streaming reader infers each column's type from only its first block, so a tag
        # column left blank throughout that block would be typed `null` and then fail conversion on a
        # real value further down the whole-market file. Read the header ourselves and force EVERY
        # column to string (the wide file is still tokenized in native code, far cheaper than a Python
        # row-by-row parse), then coerce the tag cells to float in Python for the few universe rows we
        # materialize -- the same result the old row-by-row parse gave, without the fragile per-block
        # type inference.
        header_line = source.readline()
        if not header_line:
            return {}
        column_names = header_line.decode('utf-8-sig').rstrip('\r\n').split(',')
        reader = pyarrow.csv.open_csv(
            source,
            read_options=pyarrow.csv.ReadOptions(column_names=column_names),
            convert_options=pyarrow.csv.ConvertOptions(
                column_types={name: pyarrow.string() for name in column_names},
                # keep empty cells as '' rather than null: null policy would also blank out real
                # symbols that collide with null tokens (tickers like NA or NULL), silently dropping them
                strings_can_be_null=False,
            ),
        )
        ticker_value_set = (
            pyarrow.array(sorted(tickers))
            if tickers is not None
            else None
        )
        records_by_ticker: dict[str, list[_BulkFundamentalRecord]] = {}
        for batch in reader:
            selected_batch = (
                batch.filter(
                    pyarrow.compute.is_in(
                        batch.column(batch.schema.get_field_index('ticker')),
                        value_set=ticker_value_set,
                    )
                )
                if ticker_value_set is not None
                else batch
            )
            for row in selected_batch.to_pylist():
                ticker = row.get('ticker')
                if not ticker:
                    continue

                values = {}
                for (column, raw_value) in row.items():
                    if (
                        raw_value in (None, '')
                        or column in _BULK_FUNDAMENTAL_METADATA_COLUMNS
                    ):
                        continue
                    try:
                        values[column] = float(raw_value)
                    except (TypeError, ValueError):
                        continue

                raw_fiscal_year = row.get('fiscal_year')
                record = _BulkFundamentalRecord(
                    id=row.get('fundamental_id') or None,
                    statement_code=statement_code,
                    type=row.get('fundamental_type') or None,
                    fiscal_year=int(raw_fiscal_year) if raw_fiscal_year else None,
                    fiscal_period=row.get('fiscal_period'),
                    filing_date=cls._parse_bulk_date(row.get('filing_date')),
                    updated_date=cls._parse_bulk_datetime(row.get('updated_date')),
                    period_end_date=cls._parse_bulk_date(row.get('end_date')),
                    reported_currency=cls._resolve_bulk_currency(row),
                    accepted_date=None,
                    values=values,
                )
                records_by_ticker.setdefault(ticker, []).append(record)

        return records_by_ticker

    @staticmethod
    def _parse_bulk_date(
        value: str | None,
    ) -> datetime.date | None:
        """
        Parse a bulk CSV date such as '2021-05-18 00:00:00 +0000' or '2021-05-18' to a date.

        Only the leading ISO date is used, since the bulk timestamps carry no
        time-of-day information the provider needs. Empty values yield None.
        """
        if not value:
            return None

        return datetime.date.fromisoformat(value[:10])

    @staticmethod
    def _parse_bulk_datetime(
        value: str | None,
    ) -> datetime.datetime | None:
        """
        Parse a bulk CSV timestamp to a date-precision datetime, or None when absent.

        Used only to order restated `calculations` vintages, which differ by
        months, so day precision is sufficient and avoids the bulk's mixed
        timezone-suffix formats.
        """
        if not value:
            return None

        return datetime.datetime.fromisoformat(value[:10])

    @staticmethod
    def _resolve_bulk_currency(
        row: dict[str, str],
    ) -> str:
        """
        Resolve a bulk row's reporting currency, defaulting to USD for the US datasets.

        The US bulk datasets report in USD and don't carry a per-row currency
        column; if a future dataset adds one, a valid three-letter code is used.
        """
        currency = (
            row.get('currency')
            or row.get('reported_currency')
            or ''
        ).strip()
        if _CURRENCY_UNIT_PATTERN.fullmatch(currency.lower()):
            return currency.upper()

        return 'USD'

    @staticmethod
    def _build_statement_financials(
        standardized_response: (
            intrinio_sdk.models.api_response_standardized_financials.ApiResponseStandardizedFinancials
        ),
    ) -> _IntrinioStatementFinancials:
        """
        Flatten a single financials response into a statement-level record.

        Pulls the period metadata from the response's `fundamental` object and
        the line-item tag values from its `standardized_financials`, and infers
        the statement's reporting currency from the most common line-item unit
        that looks like a three-letter currency code.

        Parameters
        ----------
        standardized_response
            The response returned by the financials endpoint for one fundamental

        Returns
        -------
        The flattened statement record
        """
        fundamental = standardized_response.fundamental
        standardized_financials = standardized_response.standardized_financials

        raw_filing_date = fundamental.filing_date
        filing_date = (
            raw_filing_date.date() if isinstance(raw_filing_date, datetime.datetime)
            else raw_filing_date
        )
        # Intrinio serves the earnings-disclosure timestamp as a timezone-aware UTC value, but the
        # curator's accepted_date column is timezone-naive like every other provider's, and PyArrow
        # can't serialize a timezone-aware timestamp to CSV without an IANA timezone database (absent
        # on Windows by default). Normalize to UTC before dropping the timezone to keep the instant.
        raw_accepted_date = fundamental.earnings_disclosed_at
        accepted_date = (
            raw_accepted_date.astimezone(datetime.UTC).replace(tzinfo=None)
            if raw_accepted_date is not None and raw_accepted_date.tzinfo is not None
            else raw_accepted_date
        )
        raw_fiscal_year = fundamental.fiscal_year
        fiscal_year = (
            int(raw_fiscal_year) if raw_fiscal_year is not None
            else None
        )
        statement_values = {
            standardized_financial.data_tag.tag: standardized_financial.value
            for standardized_financial in standardized_financials
            if (
                standardized_financial.data_tag is not None
                and standardized_financial.data_tag.tag is not None
            )
        }
        currency_units = [
            standardized_financial.data_tag.unit.upper()
            for standardized_financial in standardized_financials
            if (
                standardized_financial.data_tag is not None
                and standardized_financial.data_tag.unit is not None
                and _CURRENCY_UNIT_PATTERN.fullmatch(standardized_financial.data_tag.unit)
            )
        ]
        reported_currency = (
            collections.Counter(currency_units).most_common(1)[0][0] if currency_units
            else None
        )

        return _IntrinioStatementFinancials(
            accepted_date=accepted_date,
            filing_date=filing_date,
            fiscal_period=fundamental.fiscal_period,
            fiscal_year=fiscal_year,
            period_end_date=fundamental.end_date,
            reported_currency=reported_currency,
            statement_code=fundamental.statement_code,
            values=statement_values,
        )

    @classmethod
    def _build_stock_price_endpoint_table(
        cls,
        *,
        records: list[typing.Any],
    ) -> pyarrow.Table:
        """
        Build the stock prices endpoint table, adding the split-only adjusted price columns.

        Intrinio's stock price endpoint serves raw prices and fully
        (split-and-dividend) adjusted prices, but not the split-only adjusted
        prices the curator's contract also defines. Those are derived here by
        scaling each raw price by the row's cumulative split factor (see
        `_resolve_cumulative_split_factors`) into synthetic `split_adjusted_*`
        columns; the split-adjusted volume needs no derivation, as Intrinio's
        `adj_volume` is already split-only. The synthetic columns are mapped to
        their entity fields by `_market_data_endpoint_map` like any other tag.

        Parameters
        ----------
        records
            The stock price records returned by the endpoint

        Returns
        -------
        The endpoint table with raw, fully-adjusted and split-adjusted columns
        """
        ascending_records = sorted(
            records,
            key=lambda record: record.date,
        )
        split_factors = cls._resolve_cumulative_split_factors(ascending_records)
        rows = [
            {
                'date': record.date,
                'open': record.open,
                'high': record.high,
                'low': record.low,
                'close': record.close,
                'volume': record.volume,
                'adj_open': record.adj_open,
                'adj_high': record.adj_high,
                'adj_low': record.adj_low,
                'adj_close': record.adj_close,
                'adj_volume': record.adj_volume,
                _SPLIT_ADJUSTED_OPEN_TAG: cls._scale_by_split_factor(record.open, split_factor),
                _SPLIT_ADJUSTED_HIGH_TAG: cls._scale_by_split_factor(record.high, split_factor),
                _SPLIT_ADJUSTED_LOW_TAG: cls._scale_by_split_factor(record.low, split_factor),
                _SPLIT_ADJUSTED_CLOSE_TAG: cls._scale_by_split_factor(record.close, split_factor),
            }
            for (record, split_factor) in zip(ascending_records, split_factors, strict=True)
        ]
        # Intrinio serves prices newest-first and the downstream consolidation expects that
        # predominant descending order, so emit the rows most-recent-first.
        rows.reverse()

        return pyarrow.Table.from_pylist(rows)

    @staticmethod
    def _create_endpoint_table_from_records(
        *,
        records: list[typing.Any],
        field_mappings: list[str | PreprocessedFieldMapping],
    ) -> pyarrow.Table:
        """
        Build a PyArrow table from data provider SDK response records.

        Resolves each field mapping to the provider tags it draws from: a plain
        tag name maps to itself, while a `PreprocessedFieldMapping` contributes
        each of its source tags. Those tags are then extracted from every
        record's attributes into a row-oriented mapping, letting PyArrow infer
        each column's type so the resulting table can feed the shared toolkit
        remapping pipeline.

        Parameters
        ----------
        records
            The SDK model objects returned by an endpoint
        field_mappings
            The endpoint's entity-field mappings, each either a provider tag
            name (SDK attribute name) or a `PreprocessedFieldMapping` wrapping
            its source tags

        Returns
        -------
        pyarrow.Table
            Table whose columns are named by provider tag, empty when there are
            no records
        """
        tag_names = []
        for field_mapping in field_mappings:
            if isinstance(field_mapping, PreprocessedFieldMapping):
                tag_names.extend(field_mapping.tags)
            else:
                tag_names.append(field_mapping)

        row_mappings = [
            {
                tag_name: getattr(record, tag_name)
                for tag_name in tag_names
            }
            for record in records
        ]

        return pyarrow.Table.from_pylist(row_mappings)

    @classmethod
    def _derive_discrete_fourth_quarters(
        cls,
        statements: list[_IntrinioStatementFinancials],
    ) -> list[_IntrinioStatementFinancials]:
        """
        Replace Intrinio's calculated fourth quarter with one derived from the reported statements.

        Intrinio's `calculated` fourth quarter can be built from restated quarters,
        silently breaking point-in-time integrity. For every fiscal year that has
        the annual statement, all three interim quarters and Intrinio's fourth
        quarter, the discrete fourth quarter is recomputed here as
        `FY - (Q1 + Q2 + Q3)` from those same as-reported statements (income and
        cash flow), so the whole year stays on one reported basis. Years missing an
        input are left untouched, and the annual statements stay in the list (they
        are inputs the reconciliation guard also reads); `get_fundamental_data`
        drops them before assembly.

        Parameters
        ----------
        statements
            The ticker's selected statement financials

        Returns
        -------
        The statements with each derivable fourth quarter replaced
        """
        income_by_key: dict[tuple[int, str], _IntrinioStatementFinancials] = {}
        cash_flow_by_key: dict[tuple[int, str], _IntrinioStatementFinancials] = {}
        for statement in statements:
            if statement.fiscal_year is None:
                continue
            key = (statement.fiscal_year, statement.fiscal_period)
            if statement.statement_code == cls.INCOME_STATEMENT_CODE:
                income_by_key[key] = statement
            elif statement.statement_code == cls.CASH_FLOW_STATEMENT_CODE:
                cash_flow_by_key[key] = statement

        derived_income: dict[int, _IntrinioStatementFinancials] = {}
        derived_cash_flow: dict[int, _IntrinioStatementFinancials] = {}
        fiscal_years = {
            fiscal_year
            for (fiscal_year, _period) in (*income_by_key, *cash_flow_by_key)
        }
        for fiscal_year in fiscal_years:
            income_fourth_quarter = cls._derive_fourth_quarter_income(fiscal_year, income_by_key)
            if income_fourth_quarter is not None:
                derived_income[fiscal_year] = income_fourth_quarter
            cash_flow_fourth_quarter = cls._derive_fourth_quarter_cash_flow(fiscal_year, cash_flow_by_key)
            if cash_flow_fourth_quarter is not None:
                derived_cash_flow[fiscal_year] = cash_flow_fourth_quarter

        derived_statements = []
        for statement in statements:
            is_fourth_quarter = statement.fiscal_period == cls.FOURTH_QUARTER_FISCAL_PERIOD
            if (
                is_fourth_quarter
                and statement.statement_code == cls.INCOME_STATEMENT_CODE
                and statement.fiscal_year in derived_income
            ):
                derived_statements.append(derived_income[statement.fiscal_year])
            elif (
                is_fourth_quarter
                and statement.statement_code == cls.CASH_FLOW_STATEMENT_CODE
                and statement.fiscal_year in derived_cash_flow
            ):
                derived_statements.append(derived_cash_flow[statement.fiscal_year])
            else:
                derived_statements.append(statement)

        return derived_statements

    @classmethod
    def _derive_fourth_quarter_income(
        cls,
        fiscal_year: int,
        income_by_key: dict[tuple[int, str], _IntrinioStatementFinancials],
    ) -> _IntrinioStatementFinancials | None:
        """
        Build the discrete fourth-quarter income statement for a year, or None if it can't be derived.

        The additive dollar lines are `FY - (Q1 + Q2 + Q3)`. Share counts and
        per-share dividends are kept from Intrinio's fourth quarter (a count is
        unaffected by a revenue restatement), and each per-share earnings figure is
        recomputed from the derived net income over its kept share count.

        Parameters
        ----------
        fiscal_year
            The fiscal year to derive
        income_by_key
            The year/period-keyed income statements

        Returns
        -------
        The derived fourth-quarter income statement, or None
        """
        annual = income_by_key.get((fiscal_year, cls.ANNUAL_FISCAL_PERIOD))
        fourth_quarter = income_by_key.get((fiscal_year, cls.FOURTH_QUARTER_FISCAL_PERIOD))
        interim = [
            income_by_key.get((fiscal_year, period))
            for period in cls.QUARTERLY_REPORTED_FISCAL_PERIODS
        ]
        if (
            annual is None
            or fourth_quarter is None
            or any(quarter is None for quarter in interim)
        ):
            return None

        derived_values = cls._subtract_reported_quarters(
            annual_values=annual.values,
            interim_value_maps=[quarter.values for quarter in interim],
        )
        # a share count or a per-share dividend isn't restated, so keep Intrinio's fourth-quarter value
        for (tag, value) in fourth_quarter.values.items():
            if (
                'eps' not in tag
                and ('sharesos' in tag or 'pershare' in tag)
            ):
                derived_values[tag] = value
        # recompute per-share earnings from the derived net income and the kept share counts
        net_income = derived_values.get('netincometocommon', derived_values.get('netincome'))
        if net_income is not None:
            for (earnings_tag, share_count_tag) in cls._EARNINGS_PER_SHARE_SHARE_COUNTS.items():
                share_count = derived_values.get(share_count_tag)
                if share_count:
                    derived_values[earnings_tag] = net_income / share_count

        return dataclasses.replace(fourth_quarter, values=derived_values)

    @classmethod
    def _derive_fourth_quarter_cash_flow(
        cls,
        fiscal_year: int,
        cash_flow_by_key: dict[tuple[int, str], _IntrinioStatementFinancials],
    ) -> _IntrinioStatementFinancials | None:
        """
        Build the discrete fourth-quarter cash flow for a year, or None if it can't be derived.

        Every cash flow line is a flow, so the discrete quarter is simply
        `FY - (Q1 + Q2 + Q3)`; the interim quarters are already the discrete ones
        (Q1 reported, Q2 and Q3 the calculated standalone quarters).

        Parameters
        ----------
        fiscal_year
            The fiscal year to derive
        cash_flow_by_key
            The year/period-keyed cash flow statements

        Returns
        -------
        The derived fourth-quarter cash flow statement, or None
        """
        annual = cash_flow_by_key.get((fiscal_year, cls.ANNUAL_FISCAL_PERIOD))
        fourth_quarter = cash_flow_by_key.get((fiscal_year, cls.FOURTH_QUARTER_FISCAL_PERIOD))
        interim = [
            cash_flow_by_key.get((fiscal_year, period))
            for period in cls.QUARTERLY_REPORTED_FISCAL_PERIODS
        ]
        if (
            annual is None
            or fourth_quarter is None
            or any(quarter is None for quarter in interim)
        ):
            return None

        derived_values = cls._subtract_reported_quarters(
            annual_values=annual.values,
            interim_value_maps=[quarter.values for quarter in interim],
        )

        return dataclasses.replace(fourth_quarter, values=derived_values)

    @classmethod
    def _subtract_reported_quarters(
        cls,
        *,
        annual_values: dict[str, float],
        interim_value_maps: list[dict[str, float]],
    ) -> dict[str, float]:
        """
        Return each additive line's annual value minus the three interim quarters.

        A line is skipped when it isn't additive (a per-share or share-count line)
        or when the annual value or any interim value is missing, so a partial line
        never yields a wrong discrete quarter.

        Parameters
        ----------
        annual_values
            The annual statement's tag values
        interim_value_maps
            The three interim quarters' tag values

        Returns
        -------
        The derived discrete fourth-quarter values
        """
        derived_values = {}
        for (tag, annual_value) in annual_values.items():
            if any(fragment in tag for fragment in cls._NON_ADDITIVE_INCOME_TAG_FRAGMENTS):
                continue
            if annual_value is None:
                continue
            interim_values = [value_map.get(tag) for value_map in interim_value_maps]
            if any(value is None for value in interim_values):
                continue
            derived_values[tag] = annual_value - sum(interim_values)

        return derived_values

    @classmethod
    def _reconcile_calculated_quarters(
        cls,
        *,
        main_identifier: str,
        income_statements: list[_IntrinioStatementFinancials],
    ) -> None:
        """
        Warn when Intrinio's calculated fourth quarter doesn't reconcile with the reported quarters.

        Intrinio serves the year-end quarter as a `calculated` statement equal to
        `FY - (Q1 + Q2 + Q3)`, but doesn't reveal whether it subtracted the
        as-reported or the restated quarters. The provider selects the reported
        quarters, so a Q4 built from restated ones would silently break the
        point-in-time contract, and nothing in the metadata would show it. For
        every fiscal year that carries all three interim quarters, the calculated
        fourth quarter and the annual income statement, this recomputes
        `FY - (Q1 + Q2 + Q3)` from those same records and compares it — per core
        additive line — to the calculated Q4, logging a warning (never raising)
        on any material mismatch so the discrepancy surfaces instead of passing
        silently. Non-additive per-share and share-count lines are not checked.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        income_statements
            The ticker's income statements, including the annual (FY) one

        Returns
        -------
        None
        """
        income_by_year: dict[int, dict[str, _IntrinioStatementFinancials]] = {}
        for statement in income_statements:
            if (
                statement.statement_code != cls.INCOME_STATEMENT_CODE
                or statement.fiscal_year is None
            ):
                continue
            income_by_year.setdefault(statement.fiscal_year, {})[statement.fiscal_period] = statement

        interim_periods = cls.QUARTERLY_REPORTED_FISCAL_PERIODS
        required_periods = (
            cls.ANNUAL_FISCAL_PERIOD,
            *interim_periods,
            cls.FOURTH_QUARTER_FISCAL_PERIOD,
        )
        for (fiscal_year, periods_by_name) in sorted(income_by_year.items()):
            if any(period not in periods_by_name for period in required_periods):
                continue

            annual_values = periods_by_name[cls.ANNUAL_FISCAL_PERIOD].values
            interim_value_maps = [
                periods_by_name[period].values
                for period in interim_periods
            ]
            fourth_quarter_values = periods_by_name[cls.FOURTH_QUARTER_FISCAL_PERIOD].values

            discrepancies = []
            for tag in cls._RECONCILED_INCOME_TAGS:
                present_values = [
                    annual_values.get(tag),
                    fourth_quarter_values.get(tag),
                    *(interim_values.get(tag) for interim_values in interim_value_maps),
                ]
                if any(value is None for value in present_values):
                    continue

                expected_fourth_quarter = annual_values[tag] - sum(
                    interim_values[tag]
                    for interim_values in interim_value_maps
                )
                actual_fourth_quarter = fourth_quarter_values[tag]
                if not math.isclose(
                    expected_fourth_quarter,
                    actual_fourth_quarter,
                    rel_tol=cls._QUARTER_RECONCILIATION_RELATIVE_TOLERANCE,
                    abs_tol=cls._QUARTER_RECONCILIATION_ABSOLUTE_TOLERANCE,
                ):
                    discrepancies.append(
                        f"{tag} expected {expected_fourth_quarter:,.0f} got {actual_fourth_quarter:,.0f}"
                    )

            if discrepancies:
                msg = " ".join([
                    f"{main_identifier} {fiscal_year} calculated fourth quarter does not reconcile with",
                    "the reported quarters (FY - (Q1+Q2+Q3)), it may include restated data:",
                    "; ".join(discrepancies),
                ])
                logging.getLogger(__name__).warning(msg)

    @classmethod
    def _merge_period_statements(
        cls,
        *,
        period_statements: list[_IntrinioStatementFinancials],
    ) -> _IntrinioPeriodRecord | None:
        """
        Merge every statement sharing a period end date into one period record.

        Intrinio splits each period across separate income, balance sheet and
        cash flow fundamentals that share a `period_end_date` but not always a
        `filing_date` (the year-end balance sheet carries the filing date that
        the calculated fourth-quarter flow statements lack). The period's
        metadata and reporting currency are therefore taken from its income
        statement, while the filing date is coalesced across its point-in-time
        statements. Periods without an income statement or without any filing
        date can't form a valid row and are dropped.

        The derived-metrics `calculations` statement, when present, is merged in
        at a lower priority than the point-in-time statements: its values fill
        only the tags the point-in-time statements don't already provide, so a
        restated calculations value can never overwrite an as-reported one. It
        also carries no filing date, so it never contributes to the coalesced
        filing date above.

        Parameters
        ----------
        period_statements
            All statement records sharing the same period end date

        Returns
        -------
        The merged period record, or None when the period is incomplete
        """
        calculation_statements = [
            statement
            for statement in period_statements
            if statement.statement_code == cls.CALCULATIONS_STATEMENT_CODE
        ]
        point_in_time_statements = [
            statement
            for statement in period_statements
            if statement.statement_code != cls.CALCULATIONS_STATEMENT_CODE
        ]
        income_statements = [
            statement
            for statement in point_in_time_statements
            if statement.statement_code == cls.INCOME_STATEMENT_CODE
        ]

        if not income_statements:
            return None

        income_statement = income_statements[0]
        available_filing_dates = [
            statement.filing_date
            for statement in point_in_time_statements
            if statement.filing_date is not None
        ]

        if not available_filing_dates:
            return None

        tag_values = {
            'end_date': income_statement.period_end_date,
            'filing_date': max(available_filing_dates),
            'fiscal_period': income_statement.fiscal_period,
            'fiscal_year': income_statement.fiscal_year,
        }
        if income_statement.accepted_date is not None:
            tag_values['earnings_disclosed_at'] = income_statement.accepted_date

        # Merge the calculations statement first, then let the point-in-time statements
        # overwrite any tag they share, so restated values never displace as-reported ones.
        tag_values.update({
            tag: value
            for statement in calculation_statements
            for (tag, value) in statement.values.items()
            if value is not None
        })
        tag_values.update({
            tag: value
            for statement in point_in_time_statements
            for (tag, value) in statement.values.items()
            if value is not None
        })
        cls._add_derived_fundamental_values(tag_values)

        return _IntrinioPeriodRecord(
            reported_currency=income_statement.reported_currency,
            tag_values=tag_values,
        )

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

    def _request_fundamentals(
        self,
        *,
        main_identifier: str,
    ) -> list[
        intrinio_sdk.models.fundamental_summary.FundamentalSummary
    ]:
        """
        Download every fundamentals page for `main_identifier`.

        Follows the endpoint's `next_page` cursor until it is exhausted,
        accumulating the fundamental summaries from all pages. No fiscal period
        type is requested, so the endpoint returns every fundamental (annual and
        quarterly, across all statement types) for later filtering; this is the
        only way to obtain both the calculated fourth-quarter flow statements and
        the year-end balance sheet that completes them.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider

        Returns
        -------
        The accumulated fundamental summary records across all pages

        Raises
        ------
        IdentifierNotFoundError
            When the endpoint reports the identifier does not exist
        DataProviderPaymentError
            When the endpoint requires a paid plan for the request
        ApiEndpointError
            When the endpoint returns any other API error
        """
        company_api = self._intrinio_sdk.CompanyApi()
        fundamental_records = []
        next_page = ''

        while True:
            try:
                response = company_api.get_company_fundamentals(
                    main_identifier,
                    latest_only=False,
                    page_size=self.FUNDAMENTALS_PAGE_SIZE,
                    next_page=next_page,
                )
            except intrinio_sdk.rest.ApiException as error:
                if error.status == http.HTTPStatus.NOT_FOUND.value:
                    msg = f"Intrinio fundamentals endpoint could not find identifier {main_identifier}"

                    raise IdentifierNotFoundError(msg) from error

                if error.status == http.HTTPStatus.PAYMENT_REQUIRED.value:
                    msg = f"Intrinio fundamentals endpoint requires a paid plan for identifier {main_identifier}"

                    raise DataProviderPaymentError(msg) from error

                msg = " ".join([
                    f"Intrinio fundamentals endpoint returned HTTP status {error.status}",
                    f"for identifier {main_identifier}: {error.reason}",
                ])

                raise ApiEndpointError(msg) from error

            fundamental_records.extend(response.fundamentals)

            if not response.next_page:
                break

            next_page = response.next_page

        return fundamental_records

    def _resolve_splits(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[
        intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment
    ]:
        """
        Return the security's split adjustments, requesting them once per ticker and date range.

        The dividend and the split data methods both need the same split
        adjustments — dividends are split-adjusted by the splits that followed
        each ex-date — and the curator calls both on the same provider instance.
        Without this, every ticker's splits would be downloaded twice, which is
        pure waste on a large universe. The cache is keyed by the date range as
        well as the identifier, since a wider range yields more split records.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        start_date
            The start date of the period whose splits we're returning
        end_date
            The end date of the period whose splits we're returning

        Returns
        -------
        The security's split adjustment records for the range
        """
        return self._resolve_with_read_ahead(
            cache=self._split_records_cache,
            request=self._request_splits,
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )

    def _resolve_stock_prices(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[
        intrinio_sdk.models.stock_price_summary.StockPriceSummary
    ]:
        """
        Return the security's stock prices, reading ahead over the upcoming universe tickers.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        start_date
            The start date of the period whose prices we're returning
        end_date
            The end date of the period whose prices we're returning

        Returns
        -------
        The security's stock price records for the range
        """
        return self._resolve_with_read_ahead(
            cache=self._stock_price_cache,
            request=self._request_stock_prices,
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )

    def _resolve_dividends(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[
        intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment
    ]:
        """
        Return the security's dividend adjustments, reading ahead over the upcoming tickers.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider
        start_date
            The start date of the period whose dividends we're returning
        end_date
            The end date of the period whose dividends we're returning

        Returns
        -------
        The security's dividend adjustment records for the range
        """
        return self._resolve_with_read_ahead(
            cache=self._dividend_records_cache,
            request=self._request_dividends,
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )

    def _resolve_with_read_ahead(
        self,
        *,
        cache: dict[tuple[str, datetime.date, datetime.date], list],
        request: collections.abc.Callable[..., list],
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list:
        """
        Serve one ticker's records from `cache`, filling a whole read-ahead batch on a miss.

        Intrinio serves these endpoints one ticker at a time while the curator walks
        the universe sequentially, so resolving each ticker on its own spends the
        whole run waiting on serial round trips. On a cache miss the upcoming batch
        of universe tickers is fetched concurrently instead, and the curator's
        following iterations are then served from memory.

        The cache is reset per batch, so its footprint stays bounded no matter how
        large the universe or how deep the date range. A ticker whose read-ahead
        failed is deliberately left uncached and requested again on its own, so its
        error reaches the caller instead of being silently cached as missing data.

        Parameters
        ----------
        cache
            The per-endpoint record cache to read from and fill
        request
            The endpoint request to call for a single identifier
        main_identifier
            The security's main identifier used by the data provider
        start_date
            The start date of the period whose records we're returning
        end_date
            The end date of the period whose records we're returning

        Returns
        -------
        The identifier's records for the range
        """
        cache_key = (main_identifier, start_date, end_date)
        if cache_key in cache:
            return cache[cache_key]

        batch_identifiers = self._read_ahead_batch(main_identifier=main_identifier)
        if len(batch_identifiers) > 1:
            cache.clear()
            self._fill_read_ahead_batch(
                cache=cache,
                request=request,
                identifiers=batch_identifiers,
                start_date=start_date,
                end_date=end_date,
            )

        if cache_key not in cache:
            cache[cache_key] = request(
                main_identifier=main_identifier,
                start_date=start_date,
                end_date=end_date,
            )

        return cache[cache_key]

    def _read_ahead_batch(
        self,
        *,
        main_identifier: str,
    ) -> tuple[str, ...]:
        """
        Return the identifier plus the universe tickers the curator will ask for next.

        Falls back to the identifier alone when no universe was recorded (`initialize`
        wasn't called) or when it isn't part of it, which keeps the provider usable
        standalone, just without any read-ahead.

        Parameters
        ----------
        main_identifier
            The security's main identifier used by the data provider

        Returns
        -------
        The identifiers to fetch together
        """
        if main_identifier not in self._prefetch_identifiers:
            return (main_identifier,)

        batch_start = self._prefetch_identifiers.index(main_identifier)

        return self._prefetch_identifiers[batch_start : batch_start + self.PREFETCH_BATCH_SIZE]

    def _fill_read_ahead_batch(
        self,
        *,
        cache: dict[tuple[str, datetime.date, datetime.date], list],
        request: collections.abc.Callable[..., list],
        identifiers: tuple[str, ...],
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> None:
        """
        Fetch every identifier in the batch concurrently and cache whichever succeeded.

        Parameters
        ----------
        cache
            The per-endpoint record cache to fill
        request
            The endpoint request to call for a single identifier
        identifiers
            The batch's identifiers
        start_date
            The start date of the period whose records we're fetching
        end_date
            The end date of the period whose records we're fetching

        Returns
        -------
        None
        """
        def fetch_records(
            identifier: str,
        ) -> tuple[str, list | None]:
            try:
                return (
                    identifier,
                    request(
                        main_identifier=identifier,
                        start_date=start_date,
                        end_date=end_date,
                    ),
                )
            except Exception:   # noqa: BLE001
                # One ticker failing must not fail the ticker actually being resolved, nor the rest
                # of its batch. None marks it as not cached, so it is requested again on its own
                # turn and raises there, exactly as it would have without any read-ahead.
                return (identifier, None)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.PREFETCH_CONCURRENCY,
        ) as executor:
            for (identifier, records) in executor.map(fetch_records, identifiers):
                if records is not None:
                    cache[(identifier, start_date, end_date)] = records

    def _request_splits(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[
        intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment
    ]:
        """
        Download every split adjustment page for `main_identifier` in the date range.

        Follows the endpoint's `next_page` cursor until it is exhausted,
        accumulating the split adjustment records from all pages. The endpoint
        filters by `start_date` and `end_date` server-side, so no further
        trimming is required.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        start_date
            The first date whose split adjustments we're requesting
        end_date
            The last date whose split adjustments we're requesting

        Returns
        -------
        The accumulated split adjustment records across all pages

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
        split_records = []
        next_page = ''

        while True:
            try:
                response = security_api.get_security_stock_price_adjustments_splits(
                    main_identifier,
                    start_date=start_date,
                    end_date=end_date,
                    page_size=self.STOCK_PRICE_PAGE_SIZE,
                    next_page=next_page,
                )
            except intrinio_sdk.rest.ApiException as error:
                if error.status == http.HTTPStatus.NOT_FOUND.value:
                    msg = f"Intrinio splits endpoint could not find identifier {main_identifier}"

                    raise IdentifierNotFoundError(msg) from error

                if error.status == http.HTTPStatus.PAYMENT_REQUIRED.value:
                    msg = f"Intrinio splits endpoint requires a paid plan for identifier {main_identifier}"

                    raise DataProviderPaymentError(msg) from error

                msg = " ".join([
                    f"Intrinio splits endpoint returned HTTP status {error.status}",
                    f"for identifier {main_identifier}: {error.reason}",
                ])

                raise ApiEndpointError(msg) from error

            split_records.extend(response.stock_price_adjustments)

            if not response.next_page:
                break

            next_page = response.next_page

        return split_records

    def _request_standardized_financials(
        self,
        *,
        fundamental_id: str,
    ) -> intrinio_sdk.models.api_response_standardized_financials.ApiResponseStandardizedFinancials:
        """
        Download the standardized financials for a single fundamental.

        The endpoint returns every standardized line item for the fundamental in
        a single response, so no pagination is required. The identifier is passed
        positionally because the SDK names it `id`.

        Parameters
        ----------
        fundamental_id
            The Intrinio identifier of the fundamental whose financials we're requesting

        Returns
        -------
        The standardized financials response for the fundamental

        Raises
        ------
        IdentifierNotFoundError
            When the endpoint reports the fundamental does not exist
        DataProviderPaymentError
            When the endpoint requires a paid plan for the request
        ApiEndpointError
            When the endpoint returns any other API error
        """
        fundamentals_api = self._intrinio_sdk.FundamentalsApi()

        try:
            response = fundamentals_api.get_fundamental_standardized_financials(fundamental_id)
        except intrinio_sdk.rest.ApiException as error:
            if error.status == http.HTTPStatus.NOT_FOUND.value:
                msg = f"Intrinio financials endpoint could not find fundamental {fundamental_id}"

                raise IdentifierNotFoundError(msg) from error

            if error.status == http.HTTPStatus.PAYMENT_REQUIRED.value:
                msg = f"Intrinio financials endpoint requires a paid plan for fundamental {fundamental_id}"

                raise DataProviderPaymentError(msg) from error

            msg = " ".join([
                f"Intrinio financials endpoint returned HTTP status {error.status}",
                f"for fundamental {fundamental_id}: {error.reason}",
            ])

            raise ApiEndpointError(msg) from error

        return response

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

    @staticmethod
    def _resolve_cumulative_split_factors(
        ascending_records: list[typing.Any],
    ) -> list[float]:
        """
        Resolve each row's split-only cumulative adjustment factor from its split ratios.

        A split adjusts every price *before* its ex-date, so a date's cumulative
        factor is the product of the split ratios of all later splits. Intrinio
        tags each row with the split ratio effective on that date (1.0 when no
        split occurs), so the factors are accumulated by walking the series from
        newest to oldest: each row takes the running product of the splits already
        seen (all strictly later than it), and a split row then folds its own ratio
        in for the older rows. Multiplying a raw price by this factor yields its
        split-only adjusted value; the exact ratios keep that value clean, unlike
        the rounded `adj_volume` from which the same factor could be recovered.

        Parameters
        ----------
        ascending_records
            The stock price records, ordered ascending by date

        Returns
        -------
        The cumulative split factor for each record, in the same order
        """
        factors = [1.0] * len(ascending_records)
        running_factor = 1.0
        for index in range(len(ascending_records) - 1, -1, -1):
            factors[index] = running_factor
            split_ratio = ascending_records[index].split_ratio
            if (
                split_ratio is not None
                and split_ratio != 1.0
            ):
                running_factor *= split_ratio

        return factors

    @classmethod
    def _resolve_selection_date(
        cls,
        summary: intrinio_sdk.models.fundamental_summary.FundamentalSummary,
    ) -> "datetime.date | datetime.datetime | None":
        """
        Resolve the date used to order a summary's vintage during original selection.

        Point-in-time statements are ranked by their filing date, but the
        `calculations` statement has no filing date, so its latest-restated
        snapshot is ranked by `updated_date` instead. Both are only ever
        compared against summaries of the same statement code, so the differing
        date granularities never mix.

        Parameters
        ----------
        summary
            The fundamental summary whose selection date we're resolving

        Returns
        -------
        The summary's filing date, or its update date for calculations statements
        """
        if summary.statement_code == cls.CALCULATIONS_STATEMENT_CODE:

            return summary.updated_date

        return summary.filing_date

    @staticmethod
    def _scale_by_split_factor(
        value: float | None,
        split_factor: float,
    ) -> float | None:
        """
        Scale a raw value by a cumulative split factor, preserving nulls.

        Used for both prices and per-share dividends, which split-adjust the same way.

        Parameters
        ----------
        value
            The raw value to scale, or None
        split_factor
            The cumulative split factor to apply

        Returns
        -------
        The split-adjusted value, or None when the raw value is None
        """
        if value is None:
            return None

        # Scaling by the split factor introduces floating-point noise (e.g. 100 * 0.1); round it
        # away at a precision far finer than any real value so the stored result stays clean.
        return round(value * split_factor, _SPLIT_ADJUSTED_PRICE_DECIMAL_PLACES)

    @classmethod
    def _select_original_fundamentals(
        cls,
        summaries: list[
            intrinio_sdk.models.fundamental_summary.FundamentalSummary
        ],
    ) -> list[
        intrinio_sdk.models.fundamental_summary.FundamentalSummary
    ]:
        """
        Keep only the as-originally-reported statement for each period.

        An untyped fundamentals request can return the same statement more than
        once when a later filing re-presents a prior period (for example a 10-Q
        that restates the previous fiscal year as a comparative, which Intrinio
        still labels `reported`). Those re-presentations share the original
        statement's period but carry a later filing date and recast values,
        which would both duplicate the clock-sync filing date and mix vintages.
        For each period the earliest statement is kept, as it is the closest to
        the original point-in-time report.

        The point-in-time statements are ordered by filing date, but the
        `calculations` statement has no filing date, so its vintage is instead
        ordered by `updated_date`; each such selection date is resolved by
        `_resolve_selection_date`.

        Parameters
        ----------
        summaries
            The already-filtered fundamental summaries

        Returns
        -------
        The earliest summary for each distinct period
        """
        earliest_by_period = {}
        for summary in summaries:
            period_key = (
                summary.statement_code,
                summary.fiscal_year,
                summary.fiscal_period,
                summary.type,
            )
            current_summary = earliest_by_period.get(period_key)
            summary_selection_date = cls._resolve_selection_date(summary)
            current_selection_date = (
                cls._resolve_selection_date(current_summary) if current_summary is not None
                else None
            )
            if (
                current_summary is None
                or (
                    summary_selection_date is not None
                    and (
                        current_selection_date is None
                        or summary_selection_date < current_selection_date
                    )
                )
            ):
                earliest_by_period[period_key] = summary

        return list(earliest_by_period.values())

    @classmethod
    def _should_keep_fundamental(
        cls,
        summary: intrinio_sdk.models.fundamental_summary.FundamentalSummary,
        *,
        period_mode: "Intrinio.FundamentalPeriods",
    ) -> bool:
        """
        Decide whether a fundamental summary belongs in the requested period's dataset.

        An untyped fundamentals request returns every fiscal period Intrinio
        tracks, including the cumulative `YTD` and trailing `TTM` variants we
        never want. For annual data we keep only the originally-reported annual
        statements. For quarterly data we keep the originally-reported first
        three quarters, the calculated fourth-quarter flow statements, the
        calculated second- and third-quarter cash flow statements (Intrinio
        serves those standalone quarters only as calculated, since a 10-Q's
        reported cash flow is cumulative year-to-date), and the year-end
        (annual) balance sheet, which doubles as the fourth quarter's balance
        sheet and carries the filing date its calculated flows lack.

        Alongside those, and for whichever fiscal periods the requested mode
        covers, we keep the derived-metrics `calculations` statement. It is the
        only statement that carries the adjusted income tags, and unlike the
        others it has no `type` and no `filing_date`; it is always the latest
        restated snapshot, which is acceptable for adjusted figures since those
        are computed after the fact regardless of vintage.

        Parameters
        ----------
        summary
            The fundamental summary to evaluate
        period_mode
            Whether the caller requested annual or quarterly data

        Returns
        -------
        Whether the summary should be kept
        """
        if period_mode is cls.FundamentalPeriods.ANNUAL:

            return (
                (
                    summary.type == cls.FUNDAMENTAL_TYPE_REPORTED
                    and summary.fiscal_period == cls.ANNUAL_FISCAL_PERIOD
                )
                or (
                    summary.statement_code == cls.CALCULATIONS_STATEMENT_CODE
                    and summary.fiscal_period == cls.ANNUAL_FISCAL_PERIOD
                )
            )

        return (
            (
                summary.type == cls.FUNDAMENTAL_TYPE_REPORTED
                and summary.fiscal_period in cls.QUARTERLY_REPORTED_FISCAL_PERIODS
            )
            or (
                summary.type == cls.FUNDAMENTAL_TYPE_CALCULATED
                and summary.fiscal_period == cls.FOURTH_QUARTER_FISCAL_PERIOD
            )
            or (
                summary.type == cls.FUNDAMENTAL_TYPE_REPORTED
                and summary.fiscal_period == cls.ANNUAL_FISCAL_PERIOD
                and summary.statement_code == cls.BALANCE_SHEET_STATEMENT_CODE
            )
            or (
                # the annual income and cash flow statements are kept only as inputs for deriving the
                # discrete fourth quarter (FY - (Q1+Q2+Q3)) and for the reconciliation guard; they are
                # dropped before assembly so they never become emitted rows (see get_fundamental_data)
                summary.type == cls.FUNDAMENTAL_TYPE_REPORTED
                and summary.fiscal_period == cls.ANNUAL_FISCAL_PERIOD
                and summary.statement_code in (
                    cls.INCOME_STATEMENT_CODE,
                    cls.CASH_FLOW_STATEMENT_CODE,
                )
            )
            or (
                summary.type == cls.FUNDAMENTAL_TYPE_CALCULATED
                and summary.statement_code == cls.CASH_FLOW_STATEMENT_CODE
                and summary.fiscal_period in cls.INTERIM_CASH_FLOW_FISCAL_PERIODS
            )
            or (
                summary.statement_code == cls.CALCULATIONS_STATEMENT_CODE
                and (
                    summary.fiscal_period in cls.QUARTERLY_REPORTED_FISCAL_PERIODS
                    or summary.fiscal_period == cls.FOURTH_QUARTER_FISCAL_PERIOD
                )
            )
        )
