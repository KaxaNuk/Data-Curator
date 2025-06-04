import collections
import datetime
import enum
import json
import logging
import types

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
    SplitData,
    SplitDataRow,
    MainIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    DataProviderMissingKeyError,
    DividendDataEmptyError,
    DividendDataRowError,
    EntityFieldTypeError,
    EntityProcessingError,
    EntityTypeError,
    EntityValueError,
    FundamentalDataNoIncomeError,
    FundamentalDataNonChronologicalStatementWithoutOriginalDateError,
    FundamentalDataUnsortedRowDatesError,
    MarketDataEmptyError,
    MarketDataRowError,
    SplitDataEmptyError,
    SplitDataRowError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services import entity_helper



class FinancialModelingPrep(DataProviderInterface):
    CONNECTION_VALIDATION_TICKER = 'AAPL'   # will be used to validate we can connect
    MAX_RECORDS_DOWNLOAD_LIMIT = 1000
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


    _fields_dividend_data_rows = types.MappingProxyType({
        'declaration_date': 'declarationDate',
        'ex_dividend_date': 'date',
        'record_date': 'recordDate',
        'payment_date': 'paymentDate',
        'dividend': 'dividend',
        'dividend_split_adjusted': 'adjDividend',
    })
    _fields_fundamental_balance_sheet_data_rows = types.MappingProxyType({
        'filing_date': 'fillingDate',
        'accumulated_other_comprehensive_income_after_tax': 'accumulatedOtherComprehensiveIncomeLoss',
        'additional_paid_in_capital': 'additionalPaidInCapital',
        'assets': 'totalAssets',
        'capital_lease_obligations': 'capitalLeaseObligations',
        'cash_and_cash_equivalents': 'cashAndCashEquivalents',
        'cash_and_shortterm_investments': 'cashAndShortTermInvestments',
        'common_stock_value': 'commonStock',
        'current_accounts_payable': 'accountPayables',
        'current_accounts_receivable_after_doubtful_accounts': 'accountsReceivables',
        'current_accrued_expenses': 'accruedExpenses',
        'current_assets': 'totalCurrentAssets',
        'current_capital_lease_obligations': 'capitalLeaseObligationsCurrent',
        'current_liabilities': 'totalCurrentLiabilities',
        'current_net_receivables': 'netReceivables',
        'current_tax_payables': 'taxPayables',
        'deferred_revenue': 'deferredRevenue',
        'goodwill': 'goodwill',
        'investments': 'totalInvestments',
        'liabilities': 'totalLiabilities',
        'longterm_debt': 'longTermDebt',
        'longterm_investments': 'longTermInvestments',
        'net_debt': 'netDebt',
        'net_intangible_assets_excluding_goodwill': 'intangibleAssets',
        'net_intangible_assets_including_goodwill': 'goodwillAndIntangibleAssets',
        'net_inventory': 'inventory',
        'net_property_plant_and_equipment': 'propertyPlantEquipmentNet',
        'noncontrolling_interest': 'minorityInterest',
        'noncurrent_assets': 'totalNonCurrentAssets',
        'noncurrent_capital_lease_obligations': 'capitalLeaseObligationsNonCurrent',
        'noncurrent_deferred_revenue': 'deferredRevenueNonCurrent',
        'noncurrent_deferred_tax_assets': 'taxAssets',
        'noncurrent_deferred_tax_liabilities': 'deferredTaxLiabilitiesNonCurrent',
        'noncurrent_liabilities': 'totalNonCurrentLiabilities',
        'other_assets': 'otherAssets',
        'other_current_assets': 'otherCurrentAssets',
        'other_current_liabilities': 'otherCurrentLiabilities',
        'other_liabilities': 'otherLiabilities',
        'other_noncurrent_assets': 'otherNonCurrentAssets',
        'other_noncurrent_liabilities': 'otherNonCurrentLiabilities',
        'other_payables': 'otherPayables',
        'other_receivables': 'otherReceivables',
        'other_stockholder_equity': 'otherTotalStockholdersEquity',
        'preferred_stock_value': 'preferredStock',
        'prepaid_expenses': 'prepaids',
        'retained_earnings': 'retainedEarnings',
        'shortterm_debt': 'shortTermDebt',
        'shortterm_investments': 'shortTermInvestments',
        'stockholder_equity': 'totalStockholdersEquity',
        'total_debt_including_capital_lease_obligations': 'totalDebt',
        'total_equity_including_noncontrolling_interest': 'totalEquity',
        'total_liabilities_and_equity': 'totalLiabilitiesAndTotalEquity',
        'total_payables_current_and_noncurrent': 'totalPayables',
        'treasury_stock_value': 'treasuryStock',
    })
    _fields_fundamental_cash_flow_data_rows = types.MappingProxyType({
        'accounts_payable_change': 'accountsPayables',
        'accounts_receivable_change': 'accountsReceivables',
        'capital_expenditure': 'capitalExpenditure',
        'cash_and_cash_equivalents_change': 'netChangeInCash',
        'cash_exchange_rate_effect': 'effectOfForexChangesOnCash',
        'common_stock_dividend_payments': 'commonDividendsPaid',
        'common_stock_issuance_proceeds': 'commonStockIssuance',
        'common_stock_repurchase': 'commonStockRepurchased',
        'deferred_income_tax': 'deferredIncomeTax',
        'depreciation_and_amortization': 'depreciationAndAmortization',
        'dividend_payments': 'netDividendsPaid',
        'free_cash_flow': 'freeCashFlow',
        'interest_payments': 'interestPaid',
        'inventory_change': 'inventory',
        'investment_sales_maturities_and_collections_proceeds': 'salesMaturitiesOfInvestments',
        'investments_purchase': 'purchasesOfInvestments',
        'net_business_acquisition_payments': 'acquisitionsNet',
        'net_cash_from_operating_activities': 'netCashProvidedByOperatingActivities',
        'net_cash_from_investing_activites': 'netCashProvidedByInvestingActivities',
        'net_cash_from_financing_activities': 'netCashProvidedByFinancingActivities',
        'net_common_stock_issuance_proceeds': 'netCommonStockIssuance',
        'net_debt_issuance_proceeds': 'netDebtIssuance',
        'net_income': 'netIncome',
        'net_income_tax_payments': 'incomeTaxesPaid',
        'net_longterm_debt_issuance_proceeds': 'longTermNetDebtIssuance',
        'net_shortterm_debt_issuance_proceeds': 'shortTermNetDebtIssuance',
        'net_stock_issuance_proceeds': 'netStockIssuance',
        'other_financing_activities': 'otherFinancingActivities',
        'other_investing_activities': 'otherInvestingActivities',
        'other_noncash_items': 'otherNonCashItems',
        'other_working_capital': 'otherWorkingCapital',
        'period_end_cash': 'cashAtEndOfPeriod',
        'period_start_cash': 'cashAtBeginningOfPeriod',
        'preferred_stock_dividend_payments': 'preferredDividendsPaid',
        'preferred_stock_issuance_proceeds': 'netPreferredStockIssuance',
        'property_plant_and_equipment_purchase': 'investmentsInPropertyPlantAndEquipment',
        'stock_based_compensation': 'stockBasedCompensation',
        'working_capital_change': 'changeInWorkingCapital',
    })
    _fields_fundamental_common_data_rows = types.MappingProxyType({
        'accepted_date': 'acceptedDate',
        'filing_date': 'filingDate',
        'fiscal_period': 'period',
        'fiscal_year': 'fiscalYear',
        'period_end_date': 'date',
        'reported_currency': 'reportedCurrency',
    })
    _fields_fundamental_income_data_rows = types.MappingProxyType({
        'basic_earnings_per_share': 'eps',
        'basic_net_income_available_to_common_stockholders': 'bottomLineNetIncome',
        'continuing_operations_income_after_tax': 'netIncomeFromContinuingOperations',
        'costs_and_expenses': 'costAndExpenses',
        'cost_of_revenue': 'costOfRevenue',
        'depreciation_and_amortization': 'depreciationAndAmortization',
        'diluted_earnings_per_share': 'epsDiluted',
        'discontinued_operations_income_after_tax': 'netIncomeFromDiscontinuedOperations',
        'earnings_before_interest_and_tax': 'ebit',
        'earnings_before_interest_tax_depreciation_and_amortization': 'ebitda',
        'general_and_administrative_expense': 'generalAndAdministrativeExpenses',
        'gross_profit': 'grossProfit',
        'income_before_tax': 'incomeBeforeTax',
        'income_tax_expense': 'incomeTaxExpense',
        'interest_expense': 'interestExpense',
        'interest_income': 'interestIncome',
        'net_income': 'netIncome',
        'net_income_deductions': 'netIncomeDeductions',
        'net_interest_income': 'netInterestIncome',
        'net_total_other_income': 'totalOtherIncomeExpensesNet',
        'nonoperating_income_excluding_interest': 'nonOperatingIncomeExcludingInterest',
        'operating_expenses': 'operatingExpenses',
        'operating_income': 'operatingIncome',
        'other_expenses': 'otherExpenses',
        'other_net_income_adjustments': 'otherAdjustmentsToNetIncome',
        'research_and_development_expense': 'researchAndDevelopmentExpenses',
        'revenues': 'revenue',
        'selling_and_marketing_expense': 'sellingAndMarketingExpenses',
        'selling_general_and_administrative_expense': 'sellingGeneralAndAdministrativeExpenses',
        'weighted_average_basic_shares_outstanding': 'weightedAverageShsOut',
        'weighted_average_diluted_shares_outstanding': 'weightedAverageShsOutDil',
    })
    _fields_market_data_daily_rows_dividend_and_split_adjusted = types.MappingProxyType({
        'date': 'date',
        'open': None,
        'high': None,
        'low': None,
        'close': None,
        'volume': None,
        'vwap': None,
        'open_split_adjusted': None,
        'high_split_adjusted': None,
        'low_split_adjusted': None,
        'close_split_adjusted': None,
        'volume_split_adjusted': None,
        'vwap_split_adjusted': None,
        'open_dividend_and_split_adjusted': 'adjOpen',
        'high_dividend_and_split_adjusted': 'adjHigh',
        'low_dividend_and_split_adjusted': 'adjLow',
        'close_dividend_and_split_adjusted': 'adjClose',
        'volume_dividend_and_split_adjusted': 'volume',
        'vwap_dividend_and_split_adjusted': None,
    })
    _fields_market_data_daily_rows_split_adjusted = types.MappingProxyType({
        'date': 'date',
        'open': None,
        'high': None,
        'low': None,
        'close': None,
        'volume': None,
        'vwap': None,
        'open_split_adjusted': 'open',
        'high_split_adjusted': 'high',
        'low_split_adjusted': 'low',
        'close_split_adjusted': 'close',
        'volume_split_adjusted': 'volume',
        'vwap_split_adjusted': 'vwap',
        'open_dividend_and_split_adjusted': None,
        'high_dividend_and_split_adjusted': None,
        'low_dividend_and_split_adjusted': None,
        'close_dividend_and_split_adjusted': None,
        'volume_dividend_and_split_adjusted': None,
        'vwap_dividend_and_split_adjusted': None,
    })
    _fields_market_data_daily_rows_unadjusted = types.MappingProxyType({
        'date': 'date',
        'open': 'adjOpen',
        'high': 'adjHigh',
        'low': 'adjLow',
        'close': 'adjClose',
        'volume': 'volume',
        'vwap': None,
        'open_split_adjusted': None,
        'high_split_adjusted': None,
        'low_split_adjusted': None,
        'close_split_adjusted': None,
        'volume_split_adjusted': None,
        'vwap_split_adjusted': None,
        'open_dividend_and_split_adjusted': None,
        'high_dividend_and_split_adjusted': None,
        'low_dividend_and_split_adjusted': None,
        'close_dividend_and_split_adjusted': None,
        'volume_dividend_and_split_adjusted': None,
        'vwap_dividend_and_split_adjusted': None,
    })
    _fields_split_data_rows = types.MappingProxyType({
        'split_date': 'date',
        'numerator': 'numerator',
        'denominator': 'denominator',
    })
    _periods = types.MappingProxyType({
        'annual': 'annual',
        'quarterly': 'quarter'
    })

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
        endpoint_id = self.Endpoints.STOCK_DIVIDEND.name
        dividend_raw_data = self._request_data(
            endpoint_id,
            self.Endpoints.STOCK_DIVIDEND,
            main_identifier,
            {
                'apikey': self.api_key,
                'limit': self.MAX_RECORDS_DOWNLOAD_LIMIT,
                'symbol': main_identifier,
            }
        )

        return self._create_dividend_data_from_raw_stock_response(
            main_identifier,
            start_date,
            end_date,
            dividend_raw_data,
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
        balance_endpoint_id = self.Endpoints.BALANCE_SHEET_STATEMENT.name
        fundamental_balance_sheet_raw_data = self._request_data(
            balance_endpoint_id,
            self.Endpoints.BALANCE_SHEET_STATEMENT,
            main_identifier,
            {
                'apikey': self.api_key,
                'limit': self.MAX_RECORDS_DOWNLOAD_LIMIT,
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
                'limit': self.MAX_RECORDS_DOWNLOAD_LIMIT,
                'period': self._periods[period],
                'symbol': main_identifier,
            }
        )
        income_endpoint_id = self.Endpoints.INCOME_STATEMENT.name
        fundamental_income_raw_data = self._request_data(
            income_endpoint_id,
            self.Endpoints.INCOME_STATEMENT,
            main_identifier,
            {
                'apikey': self.api_key,
                'limit': self.MAX_RECORDS_DOWNLOAD_LIMIT,
                'period': self._periods[period],
                'symbol': main_identifier,
            }
        )

        return self._create_fundamental_data_from_raw_stock_response(
            main_identifier,
            start_date,
            end_date,
            fundamental_balance_sheet_raw_data,
            fundamental_cash_flow_raw_data,
            fundamental_income_raw_data
        )

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

        return self._create_market_data_from_raw_stock_response(
            main_identifier,
            market_raw_unadjusted_data,
            market_raw_split_adjusted_data,
            market_raw_dividend_and_split_adjusted_data,
        )

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
        endpoint_id = self.Endpoints.STOCK_SPLIT.name
        split_raw_data = self._request_data(
            endpoint_id,
            self.Endpoints.STOCK_SPLIT,
            main_identifier,
            {
                'apikey': self.api_key,
                'limit': self.MAX_RECORDS_DOWNLOAD_LIMIT,
                'symbol': main_identifier,
            }
        )

        return self._create_split_data_from_raw_stock_response(
            main_identifier,
            start_date,
            end_date,
            split_raw_data,
        )

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
        Validate that the API key used to init the class is valid, by making a test request

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
    def _create_dividend_data_from_raw_stock_response(
        cls,
        ticker: str,
        start_date,
        end_date,
        raw_dividend_response: str,
    ) -> DividendData:
        """
        Populate a DividendData entity from the web service raw data.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in
        raw_dividend_response
            The raw dividend response data

        Returns
        -------
        DividendData

        Raises
        ------
        EntityProcessingError
        """
        dividend_data_rows = {}
        try:
            if raw_dividend_response is None:
                raise DividendDataEmptyError("No data returned by dividend data endpoint")

            raw_dividend_data = json.loads(raw_dividend_response)

            dividend_data = sorted(raw_dividend_data, key=lambda x: x['date'])
            for dividend_row in dividend_data:
                all_dates = [
                    dividend_row['declarationDate'],
                    dividend_row['date'],
                    dividend_row['recordDate'],
                    dividend_row['paymentDate'],
                ]
                non_empty_dates = list(
                    filter(
                        lambda x: x != '',
                        all_dates
                    )
                )
                # check if the available dates lie outside of the selected period
                if (
                    dividend_row['date'] == ''
                    or len(non_empty_dates) == 0
                    or non_empty_dates[-1] < str(start_date)
                    or non_empty_dates[0] > str(end_date)
                ):
                    continue

                date = datetime.date.fromisoformat(dividend_row['date'])
                try:
                    fields = entity_helper.convert_data_row_into_entity_fields(
                        dividend_row,
                        cls._fields_dividend_data_rows,
                        DividendDataRow
                    )
                    dividend_data_rows[dividend_row['date']] = DividendDataRow(
                        **fields
                    )
                except (
                    EntityFieldTypeError,
                    EntityTypeError,
                    EntityValueError,
                ) as error:
                    msg = f"date: {date}"
                    raise DividendDataRowError(msg) from error

            dividend_data = DividendData(
                main_identifier=MainIdentifier(ticker),
                rows=dividend_data_rows
            )
        except (
            DividendDataEmptyError,
            DividendDataRowError
        ):
            msg = f"{ticker} has no dividend data obtained for the selected period, omitting its dividend data"
            logging.getLogger(__name__).warning(msg)
            dividend_data = DividendData(
                main_identifier=MainIdentifier(ticker),
                rows={}
            )

        return dividend_data

    @classmethod
    def _create_fundamental_data_from_raw_stock_response(
        cls,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
        fundamental_balance_sheet_raw_data: str,
        fundamental_cash_flow_raw_data: str,
        fundamental_income_raw_data: str
    ) -> FundamentalData:
        """
        Populate a FundamentalData entity from the web service raw data.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in
        fundamental_balance_sheet_raw_data
            The raw balance sheet response data
        fundamental_cash_flow_raw_data
            The raw cash flow response data
        fundamental_income_raw_data
            The raw income response data

        Returns
        -------
        FundamentalData

        Raises
        ------
        EntityProcessingError
        """
        try:
            fundamental_data_rows = {}
            balance_sheet_data = {
                i[cls._fields_fundamental_common_data_rows[cls.FILING_DATE_FIELD_NAME]]: i
                for i in json.loads(fundamental_balance_sheet_raw_data)
            }
            cash_flow_data = {
                i[cls._fields_fundamental_common_data_rows[cls.FILING_DATE_FIELD_NAME]]: i
                for i in json.loads(fundamental_cash_flow_raw_data)
            }
            income_data = {
                i[cls._fields_fundamental_common_data_rows[cls.FILING_DATE_FIELD_NAME]]: i
                for i in json.loads(fundamental_income_raw_data)
            }

            # Reminder: FMP returns fundamentals ordered from new to old
            descending_dates = income_data.keys()
            if len(descending_dates) < 1:
                raise FundamentalDataNoIncomeError

            # find unordered dates (usually amendments)
            unordered_dates = cls._find_unordered_dates(
                list(descending_dates),
                descending_order=True
            )

            first_date = cls._find_first_date_before_start_date(
                list(descending_dates),
                str(start_date)
            )
            last_date = str(end_date)

            date_indexes = reversed(descending_dates)

            for date_index in date_indexes:
                if (
                    date_index < first_date
                    or (
                        date_index > last_date
                        and (       # amendments with original date after last_date
                            cls.PERIOD_END_DATE_PROVIDER_FIELD_NAME not in income_data[date_index]
                            or income_data[date_index][cls.PERIOD_END_DATE_PROVIDER_FIELD_NAME] > last_date
                        )
                    )
                ):
                    continue

                if date_index in unordered_dates:
                    # possibly amended statement; try to insert blank data into original date
                    if cls.PERIOD_END_DATE_PROVIDER_FIELD_NAME not in income_data[date_index]:
                        raise FundamentalDataNonChronologicalStatementWithoutOriginalDateError
                    else:
                        current_period_end_date = income_data[date_index][cls.PERIOD_END_DATE_PROVIDER_FIELD_NAME]
                        if (
                            start_date
                            <= datetime.date.fromisoformat(current_period_end_date)
                            <= end_date
                        ):
                            logging.getLogger(__name__).warning(
                                " ".join([
                                    f"{ticker} has late or amended filing date for {date_index}, omitting",
                                    f"fundamental data corresponding to: {current_period_end_date}"
                                ])
                            )
                        fundamental_data_rows[current_period_end_date] = None

                        continue

                income_row = income_data[date_index]

                try:
                    if date_index not in balance_sheet_data:
                        fundamental_row_balance_sheet = None
                    else:
                        balance_sheet_fields = entity_helper.convert_data_row_into_entity_fields(
                            balance_sheet_data[date_index],
                            dict(cls._fields_fundamental_balance_sheet_data_rows),
                            FundamentalDataRowBalanceSheet
                        )
                        fundamental_row_balance_sheet = FundamentalDataRowBalanceSheet(
                            **balance_sheet_fields
                        )

                    if date_index not in cash_flow_data:
                        fundamental_row_cash_flow = None
                    else:
                        cash_flow_fields = entity_helper.convert_data_row_into_entity_fields(
                            cash_flow_data[date_index],
                            dict(cls._fields_fundamental_cash_flow_data_rows),
                            FundamentalDataRowCashFlow
                        )
                        fundamental_row_cash_flow = FundamentalDataRowCashFlow(
                            **cash_flow_fields
                        )

                    income_fields = entity_helper.convert_data_row_into_entity_fields(
                        income_row,
                        dict(cls._fields_fundamental_income_data_rows),
                        FundamentalDataRowIncomeStatement
                    )
                except EntityFieldTypeError as error:
                    fundamental_data_rows[date_index] = None
                    msg = f"{ticker} row processing error: {error}; omitting fundamental data for date: {date_index}"
                    logging.getLogger(__name__).warning(msg)

                    continue

                if (
                    fundamental_row_balance_sheet is None
                    and fundamental_row_cash_flow is None
                ):
                    logging.getLogger(__name__).warning(
                        " ".join([
                            f"{ticker} missing Balance Sheet and Cash Flow data for date: {date_index}"
                        ])
                    )
                elif fundamental_row_balance_sheet is None:
                    logging.getLogger(__name__).warning(
                        " ".join([
                            f"{ticker} missing Balance Sheet data for date: {date_index}"
                        ])
                    )
                elif fundamental_row_cash_flow is None:
                    logging.getLogger(__name__).warning(
                        " ".join([
                            f"{ticker} missing Cash Flow data for date: {date_index}"
                        ])
                    )

                try:
                    fundamental_data_rows[date_index] = FundamentalDataRow(
                        accepted_date=datetime.datetime.strptime(   # noqa: DTZ007
                            income_row[cls._fields_fundamental_common_data_rows["accepted_date"]],
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        filing_date=datetime.date.fromisoformat(
                            income_row[cls._fields_fundamental_common_data_rows[cls.FILING_DATE_FIELD_NAME]],
                        ),
                        fiscal_period=str(
                            income_row[cls._fields_fundamental_common_data_rows["fiscal_period"]]
                        ),
                        fiscal_year=int(
                            income_row[cls._fields_fundamental_common_data_rows["fiscal_year"]]
                        ),
                        period_end_date=datetime.date.fromisoformat(
                            income_row[cls.PERIOD_END_DATE_PROVIDER_FIELD_NAME],
                        ),
                        reported_currency=income_row[cls._fields_fundamental_common_data_rows["reported_currency"]],
                        balance_sheet=fundamental_row_balance_sheet,
                        cash_flow=fundamental_row_cash_flow,
                        income_statement=FundamentalDataRowIncomeStatement(
                            **income_fields
                        ),
                    )
                except (
                    EntityTypeError,
                    EntityValueError,
                ) as error:
                    msg = f"Fundamental data processing error for date {date_index}"
                    raise EntityProcessingError(msg) from error

            fundamental_data = FundamentalData(
                main_identifier=MainIdentifier(ticker),
                rows=fundamental_data_rows
            )
        except FundamentalDataNoIncomeError:
            msg = f"{ticker} has no income data obtained for the selected period, omitting its fundamental data"
            logging.getLogger(__name__).warning(msg)
            fundamental_data = FundamentalData(
                main_identifier=MainIdentifier(ticker),
                rows={}
            )
        except FundamentalDataNonChronologicalStatementWithoutOriginalDateError:
            msg = ", ".join([
                f"{ticker} has possibly amended statements without their original dates",
                "omitting its fundamental data"
            ])
            logging.getLogger(__name__).warning(msg)
            fundamental_data = FundamentalData(
                main_identifier=MainIdentifier(ticker),
                rows={}
            )
        except (
            FundamentalDataUnsortedRowDatesError,
            EntityTypeError,
            EntityValueError,
        ) as error:
            raise EntityProcessingError("Fundamental data processing error") from error

        return fundamental_data

    @classmethod
    def _create_market_data_from_raw_stock_response(
        cls,
        ticker: str,
        raw_unadjusted_response: str,
        raw_split_adjusted_response: str,
        raw_dividend_and_split_adjusted_response: str,
    ) -> MarketData:
        """
        Populate a MarketData entity from the web service raw data.

        Parameters
        ----------
        ticker
            the stock's ticker
        raw_unadjusted_response
            The raw unadjusted market response data
        raw_split_adjusted_response
            The raw split adjusted market response data
        raw_dividend_and_split_adjusted_response
            The raw dividend and split adjusted market response data

        Returns
        -------
        MarketData

        Raises
        ------
        EntityProcessingError
        """
        market_data_rows = {}
        try:
            if (
                raw_unadjusted_response is None
                or raw_unadjusted_response == '[]'
            ):
                raise MarketDataEmptyError("No data returned by unadjusted market data endpoint")
            if (
                raw_split_adjusted_response is None
                or raw_split_adjusted_response == '[]'
            ):
                raise MarketDataEmptyError("No data returned by split adjusted market data endpoint")
            if (
                raw_dividend_and_split_adjusted_response is None
                or raw_dividend_and_split_adjusted_response == '[]'
            ):
                raise MarketDataEmptyError("No data returned by dividend and split adjusted market data endpoint")

            unadjusted_data_list = json.loads(raw_unadjusted_response)
            unadjusted_data_by_date = {
                i[cls._fields_market_data_daily_rows_unadjusted['date']]: i
                for i in unadjusted_data_list
            }
            split_adjusted_data_list = json.loads(raw_split_adjusted_response)
            split_adjusted_data_by_date = {
                i[cls._fields_market_data_daily_rows_split_adjusted['date']]: i
                for i in split_adjusted_data_list
            }
            dividend_and_split_adjusted_data_list = json.loads(raw_dividend_and_split_adjusted_response)
            dividend_and_split_adjusted_data_by_date = {
                i[cls._fields_market_data_daily_rows_dividend_and_split_adjusted['date']]: i
                for i in dividend_and_split_adjusted_data_list
            }

            # stock_data = sorted(raw_stock_data, key=lambda x: x['date'])
            raw_dates = sorted(
                unadjusted_data_by_date.keys()
            )
            field_equivalences_unadjusted = dict(cls._fields_market_data_daily_rows_unadjusted)
            field_equivalences_split_adjusted = dict(cls._fields_market_data_daily_rows_split_adjusted)
            field_equivalences_dividend_and_split_adjusted = dict(
                cls._fields_market_data_daily_rows_dividend_and_split_adjusted
            )
            min_date = None
            max_date = None
            for raw_date in raw_dates:
                date = datetime.date.fromisoformat(raw_date)
                try:
                    unadjusted_fields = entity_helper.convert_data_row_into_entity_fields(
                        unadjusted_data_by_date[raw_date],
                        field_equivalences_unadjusted,
                        MarketDataDailyRow
                    )
                    split_adjusted_fields = entity_helper.convert_data_row_into_entity_fields(
                        split_adjusted_data_by_date[raw_date],
                        field_equivalences_split_adjusted,
                        MarketDataDailyRow
                    )
                    dividend_and_split_adjusted_fields = entity_helper.convert_data_row_into_entity_fields(
                        dividend_and_split_adjusted_data_by_date[raw_date],
                        field_equivalences_dividend_and_split_adjusted,
                        MarketDataDailyRow
                    )
                    nonempty_split_adjusted_fields = {
                        k: v
                        for k, v in split_adjusted_fields.items()
                        if v is not None
                    }
                    nonempty_dividend_and_split_adjusted_fields = {
                        k: v
                        for k, v in dividend_and_split_adjusted_fields.items()
                        if v is not None
                    }
                    fields_chain = collections.ChainMap(
                        nonempty_dividend_and_split_adjusted_fields,
                        nonempty_split_adjusted_fields,
                        unadjusted_fields,
                    )
                    market_data_rows[raw_date] = MarketDataDailyRow(
                        **fields_chain
                    )
                except (
                    EntityFieldTypeError,
                    EntityTypeError,
                    EntityValueError,
                ) as error:
                    msg = f"date: {date}"
                    raise MarketDataRowError(msg) from error

                if (
                    min_date is None
                    or date < min_date
                ):
                    min_date = date
                if (
                    max_date is None
                    or date > max_date
                ):
                    max_date = date

            market_data = MarketData(
                start_date=min_date,
                end_date=max_date,
                main_identifier=MainIdentifier(ticker),
                daily_rows=market_data_rows
            )
        except (
            MarketDataEmptyError,
            MarketDataRowError
        ) as error:
            raise EntityProcessingError("Market data processing error") from error

        return market_data

    @classmethod
    def _create_split_data_from_raw_stock_response(
        cls,
        ticker: str,
        start_date,
        end_date,
        raw_split_response: str,
    ) -> SplitData:
        """
        Populate a SplitData entity from the web service raw data.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
            The first date we're interested in
        end_date
            The last date we're interested in
        raw_split_response
            The raw split response data

        Returns
        -------
        SplitData

        Raises
        ------
        EntityProcessingError
        """
        split_data_rows = {}
        try:
            if raw_split_response is None:
                raise SplitDataEmptyError("No data returned by split data endpoint")

            raw_split_data = json.loads(raw_split_response)

            split_data = sorted(raw_split_data, key=lambda x: x['date'])
            for split_row in split_data:
                # check if the available dates lie outside of the selected period
                if (
                    split_row['date'] == ''
                    or split_row['date'] < str(start_date)
                    or split_row['date'] > str(end_date)
                ):
                    continue

                date = datetime.date.fromisoformat(split_row['date'])
                try:
                    fields = entity_helper.convert_data_row_into_entity_fields(
                        split_row,
                        cls._fields_split_data_rows,
                        SplitDataRow
                    )
                    split_data_rows[split_row['date']] = SplitDataRow(
                        **fields
                    )
                except (
                    EntityFieldTypeError,
                    EntityTypeError,
                    EntityValueError,
                ) as error:
                    msg = f"date: {date}"
                    raise SplitDataRowError(msg) from error

            split_data = SplitData(
                main_identifier=MainIdentifier(ticker),
                rows=split_data_rows
            )
        except (
            SplitDataEmptyError,
            SplitDataRowError
        ):
            msg = f"{ticker} has no split data obtained for the selected period, omitting its split data"
            logging.getLogger(__name__).warning(msg)
            split_data = SplitData(
                main_identifier=MainIdentifier(ticker),
                rows={}
            )

        return split_data
