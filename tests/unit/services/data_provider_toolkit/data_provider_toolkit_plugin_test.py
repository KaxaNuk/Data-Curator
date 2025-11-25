import datetime
import enum
import pathlib

import pyarrow
import pytest

from kaxanuk.data_curator.exceptions import (
    DataProviderIncorrectMappingTypeError,
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderMultiEndpointCommonDataOrderError,
    DataProviderToolkitRuntimeError,
)
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.entities.fundamental_data_row import (
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataProviderToolkit

from .fixtures import (
    endpoint_maps,
    # entity_columns,
    entity_tables,
)


@pytest.fixture()
def endpoints_fundamental_example():
    class Endpoints(enum.StrEnum):
        BALANCE_SHEET_STATEMENT = 'balance-sheet-statement'
        CASH_FLOW_STATEMENT = 'cash-flow-statement'
        INCOME_STATEMENT = 'income-statement'

    return Endpoints


@pytest.fixture()
def fundamental_endpoint_field_map_example(endpoints_fundamental_example):
    return {
        endpoints_fundamental_example.BALANCE_SHEET_STATEMENT: {
            FundamentalDataRow.accepted_date: 'acceptedDate',
            FundamentalDataRow.filing_date: 'filingDate',
            FundamentalDataRow.fiscal_period: 'period',
            FundamentalDataRow.fiscal_year: 'fiscalYear',
            FundamentalDataRow.period_end_date: 'date',
            FundamentalDataRow.reported_currency: 'reportedCurrency',
            FundamentalDataRowBalanceSheet.accumulated_other_comprehensive_income_after_tax: 'accumulatedOtherComprehensiveIncomeLoss',
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
        endpoints_fundamental_example.CASH_FLOW_STATEMENT: {
            FundamentalDataRow.accepted_date: 'acceptedDate',
            FundamentalDataRow.filing_date: 'filingDate',
            FundamentalDataRow.fiscal_period: 'period',
            FundamentalDataRow.fiscal_year: 'fiscalYear',
            FundamentalDataRow.period_end_date: 'date',
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
            FundamentalDataRowCashFlow.investment_sales_maturities_and_collections_proceeds: 'salesMaturitiesOfInvestments',
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
        endpoints_fundamental_example.INCOME_STATEMENT: {
            FundamentalDataRow.accepted_date: 'acceptedDate',
            FundamentalDataRow.filing_date: 'filingDate',
            FundamentalDataRow.fiscal_period: 'period',
            FundamentalDataRow.fiscal_year: 'fiscalYear',
            FundamentalDataRow.period_end_date: 'date',
            FundamentalDataRow.reported_currency: 'reportedCurrency',
            FundamentalDataRowIncomeStatement.basic_earnings_per_share: 'eps',
            FundamentalDataRowIncomeStatement.basic_net_income_available_to_common_stockholders: 'bottomLineNetIncome',
            FundamentalDataRowIncomeStatement.continuing_operations_income_after_tax: 'netIncomeFromContinuingOperations',
            FundamentalDataRowIncomeStatement.costs_and_expenses: 'costAndExpenses',
            FundamentalDataRowIncomeStatement.cost_of_revenue: 'costOfRevenue',
            FundamentalDataRowIncomeStatement.depreciation_and_amortization: 'depreciationAndAmortization',
            FundamentalDataRowIncomeStatement.diluted_earnings_per_share: 'epsDiluted',
            FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax: 'netIncomeFromDiscontinuedOperations',
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
            FundamentalDataRowIncomeStatement.nonoperating_income_excluding_interest: 'nonOperatingIncomeExcludingInterest',
            FundamentalDataRowIncomeStatement.operating_expenses: 'operatingExpenses',
            FundamentalDataRowIncomeStatement.operating_income: 'operatingIncome',
            FundamentalDataRowIncomeStatement.other_expenses: 'otherExpenses',
            FundamentalDataRowIncomeStatement.other_net_income_adjustments: 'otherAdjustmentsToNetIncome',
            FundamentalDataRowIncomeStatement.research_and_development_expense: 'researchAndDevelopmentExpenses',
            FundamentalDataRowIncomeStatement.revenues: 'revenue',
            FundamentalDataRowIncomeStatement.selling_and_marketing_expense: 'sellingAndMarketingExpenses',
            FundamentalDataRowIncomeStatement.selling_general_and_administrative_expense: 'sellingGeneralAndAdministrativeExpenses',
            FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding: 'weightedAverageShsOut',
            FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding: 'weightedAverageShsOutDil',
        },
    }


def check_if_table_preserves_row_order(
    merged_table: pyarrow.Table,
    original_table: pyarrow.Table,
) -> bool:
    """
        Check if merged_table preserves the row order of original_table,
        ignoring rows that are not in the original table.

        Args:
            original_table: The original PyArrow table
            merged_table: The merged PyArrow table

        Returns:
            True if row order is preserved, False otherwise
        """
    # Get all column names for comparison
    columns = original_table.column_names

    # Track the last found index in merged table
    last_found_index = -1

    # Iterate through each row in the original table
    for i in range(original_table.num_rows):
        # Create a filter to find this row in the merged table
        # We'll compare all columns to ensure we find the exact row
        filter_mask = None

        for col_name in columns:
            original_value = original_table[col_name][i]

            # Handle null values specially
            if original_value.is_valid:
                col_comparison = pyarrow.compute.equal(merged_table[col_name], original_value)
            else:
                col_comparison = pyarrow.compute.is_null(merged_table[col_name])

            if filter_mask is None:
                filter_mask = col_comparison
            else:
                filter_mask = pyarrow.compute.and_(filter_mask, col_comparison)

        # Find indices where the row matches
        matching_indices = pyarrow.compute.filter(
            pyarrow.array(range(merged_table.num_rows)),
            filter_mask
        )

        if len(matching_indices) == 0:
            # Row from original table not found in merged table
            return False

        # Get the first (should be only) matching index
        current_index = matching_indices[0].as_py()

        # Check if this index comes after the previous one
        if current_index <= last_found_index:
            return False

        last_found_index = current_index

    return True


def reverse_pyarrow_array(array: pyarrow.Array):
    indices = pyarrow.array(reversed(range(len(array))))
    return array.take(indices)


class TestTestCheckIfTablePreservesRowOrder:
    def test_check_if_table_preserves_row_order(self):
        test1 = check_if_table_preserves_row_order(
            entity_tables.COMPOUND_KEY_MERGED_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
        )
        test2 = check_if_table_preserves_row_order(
            entity_tables.COMPOUND_KEY_MERGED_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
        )

        assert (
            test1
            and test2
        )


class TestTestReversePyarrowArray:
    def test_reverse_pyarrow_array(self):
        array = pyarrow.array([3, 1, 5, 7])
        expected = pyarrow.array([7, 5, 1, 3])
        result = reverse_pyarrow_array(
            array
        )

        assert result.equals(expected)


class TestPrivateCalculateEndpointColumnRemaps:
    def test_calculate_endpoint_column_remaps(self):
        result = DataProviderToolkit._calculate_endpoint_column_remaps(
            endpoint_maps.ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS

        assert result == expected


class TestPrivateCalculateEndpointFieldPreprocessors:
    def test_calculate_endpoint_field_preprocessors(self):
        result = DataProviderToolkit._calculate_endpoint_field_preprocessors(
            endpoint_maps.ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS

        assert result == expected


class TestPrivateClearTableRowsByPrimaryKey:
    def test_clear_table_rows_by_single_primary_key(self):
        table = pyarrow.table({
            'primary_key': [1, 2, 3, 4],
            'other_column': [1, 2, 3, 4]
        })
        primary_keys_table = pyarrow.table({
            'primary_key': [1, 3],
        })
        result = DataProviderToolkit._clear_table_rows_by_primary_key(
            table,
            primary_keys_table,
            ['primary_key']
        )
        expected = pyarrow.table({
            'primary_key': [1, 2, 3, 4],
            'other_column': [None, 2, None, 4]
        })

        assert result.equals(expected)

    def test_clear_table_rows_by_multiple_primary_keys_with_none(self):
        table = pyarrow.table({
            'primary_key1': [
                datetime.date.fromisoformat('2021-01-03'),  # to clear
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-04'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
            ],
            'primary_key2': [1, None, 1, 3, 1, 4],
            'other_column1': [1, 2, 3, 4, 5, 6],
            'other_column2': [7, 7, None, None, 9, 6],
        })
        primary_keys_table = pyarrow.table({
            'primary_key1': [
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-05'),
                datetime.date.fromisoformat('2021-01-05'),
            ],
            'primary_key2': [1, 3, 1, 4],
        })
        result = DataProviderToolkit._clear_table_rows_by_primary_key(
            table,
            primary_keys_table,
            ['primary_key1', 'primary_key2']
        )
        expected = pyarrow.table({
            'primary_key1': [
                datetime.date.fromisoformat('2021-01-03'),  # to clear
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-04'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
            ],
            'primary_key2': [1, None, 1, 3, 1, 4],
            'other_column1': [None, 2, 3, None, None, None],
            'other_column2': [None, 7, None, None, None, None],
        })

        assert result.equals(expected)

    def test_clear_table_rows_by_primary_keys_fails_on_discrepant_columns(self):
        with pytest.raises(ValueError):
            table = pyarrow.table({
                'primary_key': [1, 2, 3, 4],
                'other_column': [1, 2, 3, 4]
            })
            primary_keys_table = pyarrow.table({
                'missing_primary_key': [1, 3],
            })
            DataProviderToolkit._clear_table_rows_by_primary_key(
                table,
                primary_keys_table,
                ['primary_key']
            )


# @todo no longer private
class TestPrivateConsolidateProcessedEndpointTables:
    def test_consolidate_processed_endpoint_tables(self):
        result = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_CONSISTENT,
            table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
        )
        expected = entity_tables.CONSOLIDATED_TABLE

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_reversed(self):
        result = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_CONSISTENT_REVERSED,
            table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date],
            predominant_order_descending=True
        )
        expected = entity_tables.CONSOLIDATED_TABLE_REVERSED

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_single(self):
        result = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_SINGLE,
            table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
        )
        expected = entity_tables.ENDPOINT_TABLES_SINGLE[
            entity_tables.Endpoints.BALANCE_SHEET_STATEMENT
        ]

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_inconsistent_tables(self):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_INCONSISTENT,
                table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
            )

    def test_consolidate_processed_endpoint_tables_inconsistent_tables_with_none(self):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_INCONSISTENT_WITH_NONE,
                table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
            )

    # def test_comprehensive_consolidation_with_descending_shifted_endpoints(
    #     self,
    #     endpoints_fundamental_example,
    #     fundamental_endpoint_field_map_example
    # ):
    #     base_dir = pathlib.Path(__file__).parent
    #     cashflow_path = f'{base_dir}/fixtures/fundamental_shifted_cashflow.json'
    #     balance_path = f'{base_dir}/fixtures/fundamental_shifted_balance.json'
    #     income_path = f'{base_dir}/fixtures/fundamental_shifted_income.json'
    #     fundamental_income_raw_data = pathlib.Path(income_path).read_text()
    #     fundamental_balance_sheet_raw_data = pathlib.Path(balance_path).read_text()
    #     fundamental_cash_flow_raw_data = pathlib.Path(cashflow_path).read_text()
    #
    #     endpoint_tables = DataProviderToolkit.create_endpoint_tables_from_json_mapping({
    #         endpoints_fundamental_example.BALANCE_SHEET_STATEMENT:
    #             fundamental_balance_sheet_raw_data,
    #         endpoints_fundamental_example.CASH_FLOW_STATEMENT:
    #             fundamental_cash_flow_raw_data,
    #         endpoints_fundamental_example.INCOME_STATEMENT:
    #             fundamental_income_raw_data,
    #     })
    #
    #     endpoint_column_remaps = DataProviderToolkit._calculate_endpoint_column_remaps(
    #         fundamental_endpoint_field_map_example
    #     )
    #
    #     # transform table columns per tag to columns per entity.field$tag
    #     remapped_endpoint_tables = DataProviderToolkit._remap_endpoint_table_columns(
    #         endpoint_column_remaps,
    #         endpoint_tables,
    #     )
    #
    #     # fuse all tables into one
    #     consolidated_table = DataProviderToolkit._consolidate_processed_endpoint_tables(
    #         remapped_endpoint_tables,
    #         [
    #             FundamentalDataRow.period_end_date,
    #         ],
    #         predominant_order_descending=True
    #     )
    #     assert True


class TestPrivateConvertTableToPandasPreservingTimeData:
    def test_convert_table_to_pandas_preserving_time_data(self):
        period_end_date = [
            datetime.date(1989, 1, 31),
            datetime.date(1988, 1, 31),
            datetime.date(1987, 1, 31),
            datetime.date(1986, 1, 31),
        ]
        balance_sheet_accepted_date = [
            datetime.datetime(1989, 1, 31, 0, 0, 0),
            datetime.datetime(1988, 1, 31, 0, 0, 0),
            datetime.datetime(1987, 1, 31, 0, 0, 0),
            datetime.datetime(1986, 1, 31, 0, 0, 0),
        ]
        income_statement_accepted_date = [
            datetime.datetime(1989, 1, 30, 19, 0, 0),
            datetime.datetime(1988, 1, 30, 19, 0, 0),
            datetime.datetime(1987, 1, 30, 19, 0, 0),
            datetime.datetime(1986, 1, 30, 19, 0, 0),
        ]
        table = pyarrow.table({
            'period_end_date': period_end_date,
            'balance-sheet-statement.accepted_date': balance_sheet_accepted_date,
            'income-statement.accepted_date': income_statement_accepted_date
        })
        dataframe = DataProviderToolkit._convert_table_to_pandas_preserving_time_data(
            table
        )
        dataframe_printed = dataframe.to_csv(sep='|', index=False)
        expected = "\n".join([
            'period_end_date|balance-sheet-statement.accepted_date|income-statement.accepted_date',
            '1989-01-31|1989-01-31 00:00:00|1989-01-30 19:00:00',
            '1988-01-31|1988-01-31 00:00:00|1988-01-30 19:00:00',
            '1987-01-31|1987-01-31 00:00:00|1987-01-30 19:00:00',
            '1986-01-31|1986-01-31 00:00:00|1986-01-30 19:00:00',
            ''
        ])

        assert dataframe_printed == expected


class TestPrivateCreateTableFromJsonString:
    def test_create_table_from_json_string(self):
        base_dir = pathlib.Path(__file__).parent
        relative_path = f'{base_dir}/fixtures/market_daily.json'
        json = pathlib.Path(relative_path).read_text()
        result = DataProviderToolkit._create_table_from_json_string(json)
        expected = pyarrow.table({
            'symbol': pyarrow.array(['NVDA', 'NVDA']),
            'date': pyarrow.array(
                [
                    datetime.datetime.fromisoformat('2025-11-14'),
                    datetime.datetime.fromisoformat('2025-11-13')
                ],
                type=pyarrow.timestamp('s')
            ),
            'adjOpen': pyarrow.array([182.86, 191.05]),
            'adjHigh': pyarrow.array([190.68, 191.44]),
            'adjLow': pyarrow.array([180.58, 183.85]),
            'adjClose': pyarrow.array([189.42, 186.86]),
            'volume': pyarrow.array([130626834, 206750700])
            })

        assert result.equals(expected)


class TestPrivateMergeArraySubsetsPreservingOrder:
    def test_merge_single_column_keys(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            pyarrow.table({
                'date':
                    entity_tables.filing_dates,
            }),
            pyarrow.table({
                'date':
                    entity_tables.filing_dates_subset,
            }),
            pyarrow.table({
                'date':
                    entity_tables.filing_dates_shifted
            }),
        ])
        expected = pyarrow.table({
            'date': entity_tables.filing_dates_all
        })

        assert result.equals(expected)

    def test_merge_single_column_keys_descending(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order(
            [
                pyarrow.table({
                    'date':
                        reverse_pyarrow_array(entity_tables.filing_dates),
                }),
                pyarrow.table({
                    'date':
                        reverse_pyarrow_array(entity_tables.filing_dates_subset),
                }),
                pyarrow.table({
                    'date':
                        reverse_pyarrow_array(entity_tables.filing_dates_shifted)
                }),
            ],
            predominant_order_descending=True
        )
        expected = pyarrow.table({
            'date': reverse_pyarrow_array(entity_tables.filing_dates_all)
        })

        assert result.equals(expected)

    def test_merge_compound_column_keys(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_SUBSET1_TABLE,
            entity_tables.COMPOUND_KEY_SUBSET2_TABLE,
            entity_tables.COMPOUND_KEY_SUBSET3_TABLE,
        ])
        expected = entity_tables.COMPOUND_KEY_MERGED_TABLE

        assert result.equals(expected)

    def test_merge_single_subset(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_SUBSET1_TABLE
        ])
        expected = entity_tables.COMPOUND_KEY_SUBSET1_TABLE

        assert result.equals(expected)

    def test_merge_single_column_inconsistent_order_fails(self):
        with pytest.raises(DataProviderMultiEndpointCommonDataOrderError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates_inconsistent,
                }),
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates_subset,
                }),
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates_shifted
                }),
            ])

    def test_merge_key_tables_with_different_column_names_fails(self):
        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates,
                }),
                pyarrow.table({
                    'other_date':
                        entity_tables.filing_dates_subset,
                })
            ])

    def test_merge_key_tables_with_different_column_numbers_fails(self):
        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates,
                }),
                entity_tables.COMPOUND_KEY_SUBSET1_TABLE,
            ])

    def test_merge_key_tables_with_duplicate_rows_fails(self):
        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                entity_tables.COMPOUND_KEY_MERGED_TABLE,
                entity_tables.COMPOUND_KEY_MERGED_TABLE_DUPLICATE_ROWS
            ])

    def test_merge_nondeterministic_order_subsets_preserves_subset_order(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
        ])

        assert (
            check_if_table_preserves_row_order(
                result,
                entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
            )
            and check_if_table_preserves_row_order(
                result,
                entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
            )
        )


class TestPrivateProcessRemappedEndpointTables:
    def test_process_remapped_endpoint_tables(self):
        result = DataProviderToolkit._process_remapped_endpoint_tables(
            endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS,
            endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_FIELD
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PROCESSED

        assert (
            result[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT]
            )
            and result[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT]
            )
        )


class TestPrivateRemapEndpointTableColumns:
    def test_remap_endpoint_table_columns(self):
        result = DataProviderToolkit._remap_endpoint_table_columns(
            endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS,
            endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_TAG
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_FIELD

        assert (
            result[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT]
            )
            and result[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT]
            )
        )
