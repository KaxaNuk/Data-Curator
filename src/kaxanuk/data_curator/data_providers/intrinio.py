import collections
import dataclasses
import datetime
import enum
import http
import logging
import re
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


class Intrinio(
    DataProviderInterface,      # this is the interface all data providers have to implement
):
    # fiscal period whose balance sheet doubles as the fourth-quarter (year-end) balance sheet
    ANNUAL_FISCAL_PERIOD: typing.Final = 'FY'
    # statement code identifying balance sheets in the fundamentals endpoint
    BALANCE_SHEET_STATEMENT_CODE: typing.Final = 'balance_sheet_statement'
    # fiscal period Intrinio only reports as calculated statements, never as originally reported ones
    FOURTH_QUARTER_FISCAL_PERIOD: typing.Final = 'Q4'
    # max number of records requested per fundamentals endpoint page
    FUNDAMENTALS_PAGE_SIZE: typing.Final = 10000
    # fundamental types returned by the fundamentals endpoint
    FUNDAMENTAL_TYPE_CALCULATED: typing.Final = 'calculated'
    FUNDAMENTAL_TYPE_REPORTED: typing.Final = 'reported'
    # statement code identifying income statements in the fundamentals endpoint
    INCOME_STATEMENT_CODE: typing.Final = 'income_statement'
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

    _intrinio_sdk: types.ModuleType = intrinio_sdk

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
            FundamentalDataRowBalanceSheet.total_equity_including_noncontrolling_interest:
                'totalequityandnoncontrollinginterests',
            FundamentalDataRowBalanceSheet.total_liabilities_and_equity: 'totalliabilitiesandequity',
            FundamentalDataRowBalanceSheet.treasury_stock_value: 'treasurystock',

            FundamentalDataRowCashFlow.cash_and_cash_equivalents_change: 'netchangeincash',
            FundamentalDataRowCashFlow.cash_exchange_rate_effect: 'effectofexchangeratechanges',
            FundamentalDataRowCashFlow.common_stock_issuance_proceeds: 'issuanceofcommonequity',
            FundamentalDataRowCashFlow.common_stock_repurchase: 'repurchaseofcommonequity',
            FundamentalDataRowCashFlow.dividend_payments: 'paymentofdividends',
            FundamentalDataRowCashFlow.interest_payments: 'cashinterestpaid',
            FundamentalDataRowCashFlow.investment_sales_maturities_and_collections_proceeds:
                'saleofinvestments',
            FundamentalDataRowCashFlow.investments_purchase: 'purchaseofinvestments',
            FundamentalDataRowCashFlow.net_business_acquisition_payments: 'acquisitions',
            FundamentalDataRowCashFlow.net_cash_from_operating_activities: 'netcashfromoperatingactivities',
            FundamentalDataRowCashFlow.net_cash_from_investing_activities: 'netcashfrominvestingactivities',
            FundamentalDataRowCashFlow.net_cash_from_financing_activities: 'netcashfromfinancingactivities',
            FundamentalDataRowCashFlow.net_debt_issuance_proceeds: 'issuanceofdebt',
            FundamentalDataRowCashFlow.net_income: 'netincome',
            FundamentalDataRowCashFlow.net_income_tax_payments: 'cashincometaxespaid',
            FundamentalDataRowCashFlow.other_financing_activities: 'otherfinancingactivitiesnet',
            FundamentalDataRowCashFlow.other_investing_activities: 'otherinvestingactivitiesnet',
            FundamentalDataRowCashFlow.preferred_stock_issuance_proceeds: 'issuanceofpreferredequity',
            FundamentalDataRowCashFlow.property_plant_and_equipment_purchase: 'purchaseofplantpropertyandequipment',

            FundamentalDataRowIncomeStatement.basic_earnings_per_share: 'basiceps',
            FundamentalDataRowIncomeStatement.basic_net_income_available_to_common_stockholders: 'netincometocommon',
            FundamentalDataRowIncomeStatement.continuing_operations_income_after_tax: 'netincomecontinuing',
            FundamentalDataRowIncomeStatement.cost_of_revenue: 'totalcostofrevenue',
            FundamentalDataRowIncomeStatement.diluted_earnings_per_share: 'dilutedeps',
            FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax: 'netincomediscontinued',
            FundamentalDataRowIncomeStatement.gross_profit: 'totalgrossprofit',
            FundamentalDataRowIncomeStatement.income_before_tax: 'totalpretaxincome',
            FundamentalDataRowIncomeStatement.income_tax_expense: 'incometaxexpense',
            FundamentalDataRowIncomeStatement.interest_expense: 'totalinterestexpense',
            FundamentalDataRowIncomeStatement.interest_income: 'totalinterestincome',
            FundamentalDataRowIncomeStatement.net_income: 'netincome',
            FundamentalDataRowIncomeStatement.net_interest_income: 'netinterestincome',
            FundamentalDataRowIncomeStatement.net_total_other_income: 'totalotherincome',
            FundamentalDataRowIncomeStatement.operating_expenses: 'totaloperatingexpenses',
            FundamentalDataRowIncomeStatement.operating_income: 'totaloperatingincome',
            FundamentalDataRowIncomeStatement.research_and_development_expense: 'rdexpense',
            FundamentalDataRowIncomeStatement.revenues: 'totalrevenue',
            FundamentalDataRowIncomeStatement.selling_general_and_administrative_expense: 'sgaexpense',
            FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding: 'weightedavebasicsharesos',
            FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding: 'weightedavedilutedsharesos',

            # The tags below are served only by Intrinio's `calculations` statement, not by the
            # income/balance sheet/cash flow statements. That statement is a single latest snapshot per
            # period (always is_latest, no filing_date, no reported/restated versions), so its values
            # track the restated vintage. Mapping them would inject restated-era data into our
            # as-originally-reported rows and break point-in-time integrity, so they are left unmapped:
            # FundamentalDataRowBalanceSheet.net_debt: 'netdebt',
            # FundamentalDataRowBalanceSheet.total_debt_including_capital_lease_obligations: 'debt',
            # FundamentalDataRowIncomeStatement.depreciation_and_amortization: 'depreciationandamortization',
            # FundamentalDataRowIncomeStatement.earnings_before_interest_and_tax: 'ebit',
            # FundamentalDataRowIncomeStatement.earnings_before_interest_tax_depreciation_and_amortization: 'ebitda',
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
        dividend_field_mappings = list(
            self._dividend_data_endpoint_map[self.Endpoints.STOCK_DIVIDENDS].values()
        )
        endpoint_tables = {
            self.Endpoints.STOCK_DIVIDENDS: self._create_endpoint_table_from_records(
                records=dividend_records,
                field_mappings=dividend_field_mappings,
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
        empty_fundamental_data = FundamentalData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={},
        )

        if not filtered_summaries:
            msg = f"{main_identifier} fundamentals endpoint returned no statements"
            logging.getLogger(__name__).warning(msg)

            return empty_fundamental_data

        original_summaries = self._select_original_fundamentals(filtered_summaries)
        statement_financials = [
            self._build_statement_financials(
                self._request_standardized_financials(
                    fundamental_id=summary.id,
                )
            )
            for summary in original_summaries
        ]
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
        endpoint_tables = {
            self.Endpoints.FINANCIALS: pyarrow.Table.from_pylist([
                period_record.tag_values
                for period_record in sorted_period_records
            ]),
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
        stock_price_records = self._request_stock_prices(
            main_identifier=main_identifier,
            start_date=start_date,
            end_date=end_date,
        )
        stock_price_field_mappings = list(
            self._market_data_endpoint_map[self.Endpoints.STOCK_PRICES].values()
        )
        endpoint_tables = {
            self.Endpoints.STOCK_PRICES: self._create_endpoint_table_from_records(
                records=stock_price_records,
                field_mappings=stock_price_field_mappings,
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
        split_records = self._request_splits(
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
        Run any required setup logic here before the identifiers' processing loop.

        Parameters
        ----------
        configuration
            The Configuration entity with all the currently injected settings

        Returns
        -------
        None
        """

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
            accepted_date=fundamental.earnings_disclosed_at,
            filing_date=filing_date,
            fiscal_period=fundamental.fiscal_period,
            fiscal_year=fiscal_year,
            period_end_date=fundamental.end_date,
            reported_currency=reported_currency,
            statement_code=fundamental.statement_code,
            values=statement_values,
        )

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
        statement, while the filing date is coalesced across all its statements.
        Periods without an income statement or without any filing date can't
        form a valid row and are dropped.

        Parameters
        ----------
        period_statements
            All statement records sharing the same period end date

        Returns
        -------
        The merged period record, or None when the period is incomplete
        """
        income_statements = [
            statement
            for statement in period_statements
            if statement.statement_code == cls.INCOME_STATEMENT_CODE
        ]

        if not income_statements:
            return None

        income_statement = income_statements[0]
        available_filing_dates = [
            statement.filing_date
            for statement in period_statements
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

        tag_values.update({
            tag: value
            for statement in period_statements
            for (tag, value) in statement.values.items()
            if value is not None
        })

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
        For each period the earliest-filed statement is kept, as it is the
        original point-in-time report.

        Parameters
        ----------
        summaries
            The already-filtered fundamental summaries

        Returns
        -------
        The earliest-filed summary for each distinct period
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
            if (
                current_summary is None
                or (
                    summary.filing_date is not None
                    and (
                        current_summary.filing_date is None
                        or summary.filing_date < current_summary.filing_date
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
        three quarters, the calculated fourth-quarter flow statements, and the
        year-end (annual) balance sheet, which doubles as the fourth quarter's
        balance sheet and carries the filing date its calculated flows lack.

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
                summary.type == cls.FUNDAMENTAL_TYPE_REPORTED
                and summary.fiscal_period == cls.ANNUAL_FISCAL_PERIOD
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
        )
