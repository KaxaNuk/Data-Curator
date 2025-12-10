import datetime
import enum
import logging
import typing

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
    DataProviderPaymentError,
    DataProviderToolkitNoDataError,
    DataProviderToolkitRuntimeError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services.data_provider_toolkit import (
    DataBlockEndpointTagMap,
    DataProviderFieldPreprocessors,
    DataProviderToolkit,
    EndpointFieldMap,
    PreprocessedFieldMapping,
)


class FinancialModelingPrep(
    DataProviderInterface,      # this is the interface all data providers have to implement
):
    CONNECTION_VALIDATION_TICKER = 'AAPL'   # will be used to validate we can connect
    MAX_RECORDS_DOWNLOAD_LIMIT = 1000
    MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT = 5
    # @todo: add logic to determine number of statements to retrieve based on initial date
    FILING_DATE_FIELD_NAME = "filing_date"
    PERIOD_END_DATE_PROVIDER_FIELD_NAME = 'date'

    class Endpoints(enum.StrEnum):
        BALANCE_SHEET_STATEMENT = 'https://financialmodelingprep.com/stable/balance-sheet-statement'
        CASH_FLOW_STATEMENT = 'https://financialmodelingprep.com/stable/cash-flow-statement'
        INCOME_STATEMENT = 'https://financialmodelingprep.com/stable/income-statement'
        MARKET_DATA_DAILY_UNADJUSTED = (
            'https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted'
        )
        MARKET_DATA_DAILY_SPLIT_ADJUSTED = (
            'https://financialmodelingprep.com/stable/historical-price-eod/full'
        )
        MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED = (
            'https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted'
        )
        SEARCH_TICKER = 'https://financialmodelingprep.com/stable/search-symbol'
        STOCK_DIVIDEND = 'https://financialmodelingprep.com/stable/dividends'
        STOCK_SPLIT = 'https://financialmodelingprep.com/stable/splits'

    _is_paid_account_plan = None

    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_DIVIDEND: {
            DividendDataRow.declaration_date: 'declarationDate',
            DividendDataRow.ex_dividend_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            DividendDataRow.record_date: 'recordDate',
            DividendDataRow.payment_date: 'paymentDate',
            DividendDataRow.dividend: 'dividend',
            DividendDataRow.dividend_split_adjusted: 'adjDividend',
        },
    }

    _fundamental_data_endpoint_map : typing.Final[EndpointFieldMap] = {
        Endpoints.BALANCE_SHEET_STATEMENT: {
            FundamentalDataRow.accepted_date: 'acceptedDate',
            FundamentalDataRow.filing_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['filingDate'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.fiscal_period: 'period',
            FundamentalDataRow.fiscal_year: 'fiscalYear',
            FundamentalDataRow.period_end_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.reported_currency: 'reportedCurrency',
            FundamentalDataRowBalanceSheet.accumulated_other_comprehensive_income_after_tax:
                'accumulatedOtherComprehensiveIncomeLoss',
            FundamentalDataRowBalanceSheet.additional_paid_in_capital: 'additionalPaidInCapital',
            FundamentalDataRowBalanceSheet.assets: 'totalAssets',
            FundamentalDataRowBalanceSheet.capital_lease_obligations: 'capitalLeaseObligations',
            FundamentalDataRowBalanceSheet.cash_and_cash_equivalents: 'cashAndCashEquivalents',
            FundamentalDataRowBalanceSheet.cash_and_shortterm_investments: 'cashAndShortTermInvestments',
            FundamentalDataRowBalanceSheet.common_stock_value: 'commonStock',
            FundamentalDataRowBalanceSheet.current_accounts_payable: 'accountPayables',
            FundamentalDataRowBalanceSheet.current_accounts_receivable_after_doubtful_accounts: 'accountsReceivables',
            FundamentalDataRowBalanceSheet.current_accrued_expenses: 'accruedExpenses',
            FundamentalDataRowBalanceSheet.current_assets: 'totalCurrentAssets',
            FundamentalDataRowBalanceSheet.current_capital_lease_obligations: 'capitalLeaseObligationsCurrent',
            FundamentalDataRowBalanceSheet.current_liabilities: 'totalCurrentLiabilities',
            FundamentalDataRowBalanceSheet.current_net_receivables: 'netReceivables',
            FundamentalDataRowBalanceSheet.current_tax_payables: 'taxPayables',
            FundamentalDataRowBalanceSheet.deferred_revenue: 'deferredRevenue',
            FundamentalDataRowBalanceSheet.goodwill: 'goodwill',
            FundamentalDataRowBalanceSheet.investments: 'totalInvestments',
            FundamentalDataRowBalanceSheet.liabilities: 'totalLiabilities',
            FundamentalDataRowBalanceSheet.longterm_debt: 'longTermDebt',
            FundamentalDataRowBalanceSheet.longterm_investments: 'longTermInvestments',
            FundamentalDataRowBalanceSheet.net_debt: 'netDebt',
            FundamentalDataRowBalanceSheet.net_intangible_assets_excluding_goodwill: 'intangibleAssets',
            FundamentalDataRowBalanceSheet.net_intangible_assets_including_goodwill: 'goodwillAndIntangibleAssets',
            FundamentalDataRowBalanceSheet.net_inventory: 'inventory',
            FundamentalDataRowBalanceSheet.net_property_plant_and_equipment: 'propertyPlantEquipmentNet',
            FundamentalDataRowBalanceSheet.noncontrolling_interest: 'minorityInterest',
            FundamentalDataRowBalanceSheet.noncurrent_assets: 'totalNonCurrentAssets',
            FundamentalDataRowBalanceSheet.noncurrent_capital_lease_obligations: 'capitalLeaseObligationsNonCurrent',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_revenue: 'deferredRevenueNonCurrent',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_assets: 'taxAssets',
            FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_liabilities: 'deferredTaxLiabilitiesNonCurrent',
            FundamentalDataRowBalanceSheet.noncurrent_liabilities: 'totalNonCurrentLiabilities',
            FundamentalDataRowBalanceSheet.other_assets: 'otherAssets',
            FundamentalDataRowBalanceSheet.other_current_assets: 'otherCurrentAssets',
            FundamentalDataRowBalanceSheet.other_current_liabilities: 'otherCurrentLiabilities',
            FundamentalDataRowBalanceSheet.other_liabilities: 'otherLiabilities',
            FundamentalDataRowBalanceSheet.other_noncurrent_assets: 'otherNonCurrentAssets',
            FundamentalDataRowBalanceSheet.other_noncurrent_liabilities: 'otherNonCurrentLiabilities',
            FundamentalDataRowBalanceSheet.other_payables: 'otherPayables',
            FundamentalDataRowBalanceSheet.other_receivables: 'otherReceivables',
            FundamentalDataRowBalanceSheet.other_stockholder_equity: 'otherTotalStockholdersEquity',
            FundamentalDataRowBalanceSheet.preferred_stock_value: 'preferredStock',
            FundamentalDataRowBalanceSheet.prepaid_expenses: 'prepaids',
            FundamentalDataRowBalanceSheet.retained_earnings: 'retainedEarnings',
            FundamentalDataRowBalanceSheet.shortterm_debt: 'shortTermDebt',
            FundamentalDataRowBalanceSheet.shortterm_investments: 'shortTermInvestments',
            FundamentalDataRowBalanceSheet.stockholder_equity: 'totalStockholdersEquity',
            FundamentalDataRowBalanceSheet.total_debt_including_capital_lease_obligations: 'totalDebt',
            FundamentalDataRowBalanceSheet.total_equity_including_noncontrolling_interest: 'totalEquity',
            FundamentalDataRowBalanceSheet.total_liabilities_and_equity: 'totalLiabilitiesAndTotalEquity',
            FundamentalDataRowBalanceSheet.total_payables_current_and_noncurrent: 'totalPayables',
            FundamentalDataRowBalanceSheet.treasury_stock_value: 'treasuryStock',
        },
        Endpoints.CASH_FLOW_STATEMENT: {
            FundamentalDataRow.accepted_date: 'acceptedDate',
            FundamentalDataRow.filing_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['filingDate'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.fiscal_period: 'period',
            FundamentalDataRow.fiscal_year: 'fiscalYear',
            FundamentalDataRow.period_end_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.reported_currency: 'reportedCurrency',
            FundamentalDataRowCashFlow.accounts_payable_change: 'accountsPayables',
            FundamentalDataRowCashFlow.accounts_receivable_change: 'accountsReceivables',
            FundamentalDataRowCashFlow.capital_expenditure: 'capitalExpenditure',
            FundamentalDataRowCashFlow.cash_and_cash_equivalents_change: 'netChangeInCash',
            FundamentalDataRowCashFlow.cash_exchange_rate_effect: 'effectOfForexChangesOnCash',
            FundamentalDataRowCashFlow.common_stock_dividend_payments: 'commonDividendsPaid',
            FundamentalDataRowCashFlow.common_stock_issuance_proceeds: 'commonStockIssuance',
            FundamentalDataRowCashFlow.common_stock_repurchase: 'commonStockRepurchased',
            FundamentalDataRowCashFlow.deferred_income_tax: 'deferredIncomeTax',
            FundamentalDataRowCashFlow.depreciation_and_amortization: 'depreciationAndAmortization',
            FundamentalDataRowCashFlow.dividend_payments: 'netDividendsPaid',
            FundamentalDataRowCashFlow.free_cash_flow: 'freeCashFlow',
            FundamentalDataRowCashFlow.interest_payments: 'interestPaid',
            FundamentalDataRowCashFlow.inventory_change: 'inventory',
            FundamentalDataRowCashFlow.investment_sales_maturities_and_collections_proceeds:
                'salesMaturitiesOfInvestments',
            FundamentalDataRowCashFlow.investments_purchase: 'purchasesOfInvestments',
            FundamentalDataRowCashFlow.net_business_acquisition_payments: 'acquisitionsNet',
            FundamentalDataRowCashFlow.net_cash_from_operating_activities: 'netCashProvidedByOperatingActivities',
            FundamentalDataRowCashFlow.net_cash_from_investing_activites: 'netCashProvidedByInvestingActivities',
            FundamentalDataRowCashFlow.net_cash_from_financing_activities: 'netCashProvidedByFinancingActivities',
            FundamentalDataRowCashFlow.net_common_stock_issuance_proceeds: 'netCommonStockIssuance',
            FundamentalDataRowCashFlow.net_debt_issuance_proceeds: 'netDebtIssuance',
            FundamentalDataRowCashFlow.net_income: 'netIncome',
            FundamentalDataRowCashFlow.net_income_tax_payments: 'incomeTaxesPaid',
            FundamentalDataRowCashFlow.net_longterm_debt_issuance_proceeds: 'longTermNetDebtIssuance',
            FundamentalDataRowCashFlow.net_shortterm_debt_issuance_proceeds: 'shortTermNetDebtIssuance',
            FundamentalDataRowCashFlow.net_stock_issuance_proceeds: 'netStockIssuance',
            FundamentalDataRowCashFlow.other_financing_activities: 'otherFinancingActivities',
            FundamentalDataRowCashFlow.other_investing_activities: 'otherInvestingActivities',
            FundamentalDataRowCashFlow.other_noncash_items: 'otherNonCashItems',
            FundamentalDataRowCashFlow.other_working_capital: 'otherWorkingCapital',
            FundamentalDataRowCashFlow.period_end_cash: 'cashAtEndOfPeriod',
            FundamentalDataRowCashFlow.period_start_cash: 'cashAtBeginningOfPeriod',
            FundamentalDataRowCashFlow.preferred_stock_dividend_payments: 'preferredDividendsPaid',
            FundamentalDataRowCashFlow.preferred_stock_issuance_proceeds: 'netPreferredStockIssuance',
            FundamentalDataRowCashFlow.property_plant_and_equipment_purchase: 'investmentsInPropertyPlantAndEquipment',
            FundamentalDataRowCashFlow.stock_based_compensation: 'stockBasedCompensation',
            FundamentalDataRowCashFlow.working_capital_change: 'changeInWorkingCapital',
        },
        Endpoints.INCOME_STATEMENT: {
            FundamentalDataRow.accepted_date: 'acceptedDate',
            FundamentalDataRow.filing_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['filingDate'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.fiscal_period: 'period',
            FundamentalDataRow.fiscal_year: 'fiscalYear',
            FundamentalDataRow.period_end_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            FundamentalDataRow.reported_currency: 'reportedCurrency',
            FundamentalDataRowIncomeStatement.basic_earnings_per_share: 'eps',
            FundamentalDataRowIncomeStatement.basic_net_income_available_to_common_stockholders: 'bottomLineNetIncome',
            FundamentalDataRowIncomeStatement.continuing_operations_income_after_tax:
                'netIncomeFromContinuingOperations',
            FundamentalDataRowIncomeStatement.costs_and_expenses: 'costAndExpenses',
            FundamentalDataRowIncomeStatement.cost_of_revenue: 'costOfRevenue',
            FundamentalDataRowIncomeStatement.depreciation_and_amortization: 'depreciationAndAmortization',
            FundamentalDataRowIncomeStatement.diluted_earnings_per_share: 'epsDiluted',
            FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax:
                'netIncomeFromDiscontinuedOperations',
            FundamentalDataRowIncomeStatement.earnings_before_interest_and_tax: 'ebit',
            FundamentalDataRowIncomeStatement.earnings_before_interest_tax_depreciation_and_amortization: 'ebitda',
            FundamentalDataRowIncomeStatement.general_and_administrative_expense: 'generalAndAdministrativeExpenses',
            FundamentalDataRowIncomeStatement.gross_profit: 'grossProfit',
            FundamentalDataRowIncomeStatement.income_before_tax: 'incomeBeforeTax',
            FundamentalDataRowIncomeStatement.income_tax_expense: 'incomeTaxExpense',
            FundamentalDataRowIncomeStatement.interest_expense: 'interestExpense',
            FundamentalDataRowIncomeStatement.interest_income: 'interestIncome',
            FundamentalDataRowIncomeStatement.net_income: 'netIncome',
            FundamentalDataRowIncomeStatement.net_income_deductions: 'netIncomeDeductions',
            FundamentalDataRowIncomeStatement.net_interest_income: 'netInterestIncome',
            FundamentalDataRowIncomeStatement.net_total_other_income: 'totalOtherIncomeExpensesNet',
            FundamentalDataRowIncomeStatement.nonoperating_income_excluding_interest:
                'nonOperatingIncomeExcludingInterest',
            FundamentalDataRowIncomeStatement.operating_expenses: 'operatingExpenses',
            FundamentalDataRowIncomeStatement.operating_income: 'operatingIncome',
            FundamentalDataRowIncomeStatement.other_expenses: 'otherExpenses',
            FundamentalDataRowIncomeStatement.other_net_income_adjustments: 'otherAdjustmentsToNetIncome',
            FundamentalDataRowIncomeStatement.research_and_development_expense: 'researchAndDevelopmentExpenses',
            FundamentalDataRowIncomeStatement.revenues: 'revenue',
            FundamentalDataRowIncomeStatement.selling_and_marketing_expense: 'sellingAndMarketingExpenses',
            FundamentalDataRowIncomeStatement.selling_general_and_administrative_expense:
                'sellingGeneralAndAdministrativeExpenses',
            FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding: 'weightedAverageShsOut',
            FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding: 'weightedAverageShsOutDil',
        },
    }

    _market_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED: {
            MarketDataDailyRow.date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            MarketDataDailyRow.open_dividend_and_split_adjusted: 'adjOpen',
            MarketDataDailyRow.high_dividend_and_split_adjusted: 'adjHigh',
            MarketDataDailyRow.low_dividend_and_split_adjusted: 'adjLow',
            MarketDataDailyRow.close_dividend_and_split_adjusted: 'adjClose',
            MarketDataDailyRow.volume_dividend_and_split_adjusted: 'volume',
        },
        Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED: {
            MarketDataDailyRow.date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            MarketDataDailyRow.open_split_adjusted: 'open',
            MarketDataDailyRow.high_split_adjusted: 'high',
            MarketDataDailyRow.low_split_adjusted: 'low',
            MarketDataDailyRow.close_split_adjusted: 'close',
            MarketDataDailyRow.volume_split_adjusted: 'volume',
            MarketDataDailyRow.vwap_split_adjusted: 'vwap',
        },
        Endpoints.MARKET_DATA_DAILY_UNADJUSTED: {
            MarketDataDailyRow.date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            MarketDataDailyRow.open: 'adjOpen',
            MarketDataDailyRow.high: 'adjHigh',
            MarketDataDailyRow.low: 'adjLow',
            MarketDataDailyRow.close: 'adjClose',
            MarketDataDailyRow.volume: 'volume',
        },
    }

    _split_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.STOCK_SPLIT: {
            SplitDataRow.split_date: PreprocessedFieldMapping(   # compensate pyarrow casting issues
                ['date'],
                [DataProviderFieldPreprocessors.cast_datetime_to_date]
            ),
            SplitDataRow.numerator: 'numerator',
            SplitDataRow.denominator: 'denominator',
        },
    }

    # @todo: make enum
    _periods: typing.Final = {
        'annual': 'annual',
        'quarterly': 'quarter'
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

        self.api_key = api_key

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
        Get the dividend data from the FMP web service wrapped in a DividendData entity.

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

        Raises
        ------
        ConnectionError
        """
        if self._get_paid_account_status() is False:
            max_records_download_limit = self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT
        else:
            max_records_download_limit = self.MAX_RECORDS_DOWNLOAD_LIMIT

        endpoint_id = self.Endpoints.STOCK_DIVIDEND.name

        try:
            # Attempt to download the data, possibly with a paid account download limit
            dividend_raw_data = self._request_data(
                endpoint_id,
                self.Endpoints.STOCK_DIVIDEND,
                main_identifier,
                {
                    'apikey': self.api_key,
                    'limit': max_records_download_limit,
                    'symbol': main_identifier,
                }
            )
        except DataProviderPaymentError as error:
            if self._get_paid_account_status() is None:
                # Attempt to download the data with a free account download limit
                dividend_raw_data = self._request_data(
                    endpoint_id,
                    self.Endpoints.STOCK_DIVIDEND,
                    main_identifier,
                    {
                        'apikey': self.api_key,
                        'limit': self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT,
                        'symbol': main_identifier,
                    }
                )
                # If the download actually completed this time, looks like we're using a free account
                self._set_paid_account_status(is_paid_account_plan=False)
            else:
                raise error

        endpoint_tables = DataProviderToolkit.create_endpoint_tables_from_json_mapping({
            self.Endpoints.STOCK_DIVIDEND:
                dividend_raw_data
        })

        empty_dividend_data = DividendData(
            main_identifier=MainIdentifier(main_identifier),
            rows={}
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

        # @todo trim based on start and end dates
        try:
            consolidated_dividend_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=processed_endpoint_tables,
                table_merge_fields=[],
                predominant_order_descending=True
            )
        except DataProviderToolkitRuntimeError:
            # reraise as problem is with data provider logic

            raise

        consolidated_dividend_table = consolidated_dividend_table_descending[::-1]

        dividend_data = DividendsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_dividend_table,
            common_field_data={
                DividendData: {
                    DividendData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return dividend_data    # noqa: RET504

    def get_fundamental_data(
        self,
        *,
        main_identifier: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        """
        Get the FUNDAMENTAL data from the FMP web service wrapped in a fundamentalData entity.

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

        Raises
        ------
        ConnectionError

        """
        if self._get_paid_account_status() is False:
            max_records_download_limit = self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT
        else:
            max_records_download_limit = self.MAX_RECORDS_DOWNLOAD_LIMIT

        income_endpoint_id = self.Endpoints.INCOME_STATEMENT.name

        try:
            # Attempt to download the data, possibly with a paid account download limit
            fundamental_income_raw_data = self._request_data(
                income_endpoint_id,
                self.Endpoints.INCOME_STATEMENT,
                main_identifier,
                {
                    'apikey': self.api_key,
                    'limit': max_records_download_limit,
                    'period': self._periods[period],
                    'symbol': main_identifier,
                }
            )
        except DataProviderPaymentError as error:
            if self._get_paid_account_status() is None:
                # Attempt to download the data with a free account download limit
                fundamental_income_raw_data = self._request_data(
                    income_endpoint_id,
                    self.Endpoints.INCOME_STATEMENT,
                    main_identifier,
                    {
                        'apikey': self.api_key,
                        'limit': self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT,
                        'period': self._periods[period],
                        'symbol': main_identifier,
                    }
                )
                # If the download actually completed this time, looks like we're using a free account
                self._set_paid_account_status(is_paid_account_plan=False)
                max_records_download_limit = self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT
            else:
                raise error

        balance_endpoint_id = self.Endpoints.BALANCE_SHEET_STATEMENT.name
        fundamental_balance_sheet_raw_data = self._request_data(
            balance_endpoint_id,
            self.Endpoints.BALANCE_SHEET_STATEMENT,
            main_identifier,
            {
                'apikey': self.api_key,
                'limit': max_records_download_limit,
                'period': self._periods[period],
                'symbol': main_identifier,
            }
        )
        cashflow_endpoint_id = self.Endpoints.CASH_FLOW_STATEMENT.name
        fundamental_cash_flow_raw_data = self._request_data(
            cashflow_endpoint_id,
            self.Endpoints.CASH_FLOW_STATEMENT,
            main_identifier,
            {
                'apikey': self.api_key,
                'limit': max_records_download_limit,
                'period': self._periods[period],
                'symbol': main_identifier,
            }
        )

        endpoint_tables = DataProviderToolkit.create_endpoint_tables_from_json_mapping({
            self.Endpoints.BALANCE_SHEET_STATEMENT:
                fundamental_balance_sheet_raw_data,
            self.Endpoints.CASH_FLOW_STATEMENT:
                fundamental_cash_flow_raw_data,
            self.Endpoints.INCOME_STATEMENT:
                fundamental_income_raw_data,
        })

        empty_fundamental_data = FundamentalData(
            main_identifier=MarketInstrumentIdentifier(main_identifier),
            rows={}
        )
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
        except DataProviderToolkitRuntimeError:
            # reraise as problem is with data provider logic

            raise

        # @todo put this try catch logic in a context manager ??
        # @todo trim based on start and end dates
        try:
            consolidated_fundamental_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=processed_endpoint_tables,
                table_merge_fields=[
                    FundamentalsDataBlock.clock_sync_field,
                    FundamentalDataRow.period_end_date,
                ],
                predominant_order_descending=True
            )
        except DataProviderToolkitRuntimeError:
            # reraise as problem is with data provider logic

            raise

        except DataProviderMultiEndpointCommonDataOrderError:
            msg = " ".join([
                f"{main_identifier} fundamental data endpoints have inconsistent filing_date order for common data,",
                "omitting its fundamental data"
            ])
            logging.getLogger(__name__).error(msg)

            return empty_fundamental_data

        except DataProviderMultiEndpointCommonDataDiscrepancyError as error:
            discrepancy_output_table = DataProviderToolkit.format_endpoint_discrepancy_table_for_output(
                data_block=FundamentalsDataBlock,
                discrepancy_table=error.discrepancies_table,
                endpoints_enum=self.Endpoints,
                endpoint_field_map=self._fundamental_data_endpoint_map,
            )
            msg = "\n".join([
                f"{main_identifier} fundamental data endpoints present discrepancies between common columns:",
                ", ".join(error.discrepant_columns),
                "Omitting fundamental data for the dates corresponding to the following discrepancies:",
                discrepancy_output_table
            ])
            logging.getLogger(__name__).error(msg)

            # fill the conflicting rows with None and retry
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

        consolidated_fundamental_table = consolidated_fundamental_table_descending[::-1]
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

        # @todo filter rows where filing date is before period end date

        # catch empty income statement rows
        missing_income_statement_rows_mask = DataProviderToolkit.find_common_table_missing_rows_mask(
            consolidated_fundamental_table.select([
                    FundamentalsDataBlock.get_field_qualified_name(
                        FundamentalsDataBlock.clock_sync_field
                    )
                ]
            ),
            processed_endpoint_tables[self.Endpoints.INCOME_STATEMENT].select([
                FundamentalsDataBlock.get_field_qualified_name(
                    FundamentalsDataBlock.clock_sync_field
                )
            ]),
        )
        if missing_income_statement_rows_mask is not None:
            missing_income_statement_rows_table = (
                consolidated_fundamental_table
                .select(warning_output_columns_map.keys())
                .filter(missing_income_statement_rows_mask)
            )
            discrepancy_output_table = DataProviderToolkit.format_consolidated_discrepancy_table_for_output(
                discrepancy_table=missing_income_statement_rows_table,
                output_column_renames=warning_output_columns_map
            )
            msg = "\n".join([
                f"{main_identifier} has balance sheet or cash flow statements with no corresponding income statement.",
                "Omitting fundamental data for the periods corresponding to the following filings:",
                discrepancy_output_table
            ])
            logging.getLogger(__name__).error(msg)

            full_income_rows_mask = pyarrow.compute.invert(missing_income_statement_rows_mask)
            full_income_fundamental_table = consolidated_fundamental_table.filter(full_income_rows_mask)
        else:
            full_income_fundamental_table = consolidated_fundamental_table

        # filter ammendments
        irregular_rows_mask = FundamentalsDataBlock.find_consolidated_table_irregular_filing_rows(
            consolidated_table=full_income_fundamental_table
        )
        if irregular_rows_mask is not None:
            irregular_rows_table = (
                full_income_fundamental_table
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

            regular_rows_mask = pyarrow.compute.invert(irregular_rows_mask)
            regularized_fundamental_table = full_income_fundamental_table.filter(regular_rows_mask)
        else:
            regularized_fundamental_table = full_income_fundamental_table

        fundamental_data = FundamentalsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=regularized_fundamental_table,
            common_field_data={
                FundamentalData: {
                    FundamentalData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return fundamental_data     # noqa: RET504

    def get_market_data(
            self,
            *,
            main_identifier: str,
            start_date: datetime.date,
            end_date: datetime.date,
    ) -> MarketData:
        """
        Get the market data from the FMP web service wrapped in a MarketData entity.

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
        ConnectionError
        """
        market_raw_unadjusted_data = self._request_data(
            self.Endpoints.MARKET_DATA_DAILY_UNADJUSTED.name,
            self.Endpoints.MARKET_DATA_DAILY_UNADJUSTED,
            main_identifier,
            {
                'apikey': self.api_key,
                'symbol': main_identifier,
                'from': start_date.strftime("%Y-%m-%d"),
                'to': end_date.strftime("%Y-%m-%d"),
            },
        )
        market_raw_split_adjusted_data = self._request_data(
            self.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED.name,
            self.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED,
            main_identifier,
            {
                'apikey': self.api_key,
                'symbol': main_identifier,
                'from': start_date.strftime("%Y-%m-%d"),
                'to': end_date.strftime("%Y-%m-%d"),
            },
        )
        market_raw_dividend_and_split_adjusted_data = self._request_data(
            self.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED.name,
            self.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED,
            main_identifier,
            {
                'apikey': self.api_key,
                'symbol': main_identifier,
                'from': start_date.strftime("%Y-%m-%d"),
                'to': end_date.strftime("%Y-%m-%d"),
            },
        )
        endpoint_tables = DataProviderToolkit.create_endpoint_tables_from_json_mapping({
            self.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED:
                market_raw_dividend_and_split_adjusted_data,
            self.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED:
                market_raw_split_adjusted_data,
            self.Endpoints.MARKET_DATA_DAILY_UNADJUSTED:
                market_raw_unadjusted_data,
        })

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

    def get_split_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        """
        Get the split data from the FMP web service wrapped in a SplitData entity.

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

        Raises
        ------
        ConnectionError
        """
        if self._get_paid_account_status() is False:
            max_records_download_limit = self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT
        else:
            max_records_download_limit = self.MAX_RECORDS_DOWNLOAD_LIMIT

        endpoint_id = self.Endpoints.STOCK_SPLIT.name

        try:
            # Attempt to download the data, possibly with a paid account download limit
            split_raw_data = self._request_data(
                endpoint_id,
                self.Endpoints.STOCK_SPLIT,
                main_identifier,
                {
                    'apikey': self.api_key,
                    'limit': max_records_download_limit,
                    'symbol': main_identifier,
                }
            )
        except DataProviderPaymentError as error:
            if self._get_paid_account_status() is None:
                # Attempt to download the data with a free account download limit
                split_raw_data = self._request_data(
                    endpoint_id,
                    self.Endpoints.STOCK_SPLIT,
                    main_identifier,
                    {
                        'apikey': self.api_key,
                        'limit': self.MAX_FREE_ACCOUNT_RECORDS_DOWNLOAD_LIMIT,
                        'symbol': main_identifier,
                    }
                )
                # If the download actually completed this time, looks like we're using a free account
                self._set_paid_account_status(is_paid_account_plan=False)
            else:
                raise error

        endpoint_tables = DataProviderToolkit.create_endpoint_tables_from_json_mapping({
            self.Endpoints.STOCK_SPLIT:
                split_raw_data
        })

        empty_split_data = SplitData(
                main_identifier=MainIdentifier(main_identifier),
                rows={}
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

        # @todo trim based on start and end dates
        try:
            consolidated_split_table_descending = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=processed_endpoint_tables,
                table_merge_fields=[],
                predominant_order_descending=True
            )
        except DataProviderToolkitRuntimeError:
            # reraise as problem is with data provider logic

            raise

        consolidated_split_table = consolidated_split_table_descending[::-1]

        split_data = SplitsDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_split_table,
            common_field_data={
                SplitData: {
                    SplitData.main_identifier: MarketInstrumentIdentifier(main_identifier),
                }
            }
        )

        return split_data   # noqa: RET504

    def initialize(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        pass

    def validate_api_key(
        self,
    ) -> bool | None:
        """
        Validate that the API key used to init the class is valid, by making a test request.

        Returns
        -------
        Whether `api_key` is valid
        """
        if (
            self.api_key is None
            or len(self.api_key) < 1
        ):
            return False

        endpoint_id = self.Endpoints.SEARCH_TICKER.name
        test_data = self._request_data(
            endpoint_id,
            self.Endpoints.SEARCH_TICKER,
            self.CONNECTION_VALIDATION_TICKER,
            {
                'apikey': self.api_key,
                'query': self.CONNECTION_VALIDATION_TICKER,
                'limit': 1
            }
        )

        # @todo: logic to check if we actually got valid data
        # @todo: throw exception for connection errors unrelated to api key

        return test_data is not None

    @classmethod
    def _get_paid_account_status(
        cls,
    ) -> bool | None:
        """
        Get the account paid plan status of the FMP account.

        Returns
        -------
        Whether the account is a paid account plan or not
        """
        return cls._is_paid_account_plan

    @classmethod
    def _set_paid_account_status(
        cls,
        *,
        is_paid_account_plan: bool,
    ) -> None:
        """
        Set the account paid plan status of the FMP account.

        Parameters
        ----------
        is_paid_account_plan
            Whether the account is a paid account plan or not
        """
        cls._is_paid_account_plan = is_paid_account_plan
        if not cls._is_paid_account_plan:
            msg = "Free FMP account plan limitation: fundamental data is only available for the most recent periods."
            logging.getLogger(__name__).warning(msg)
