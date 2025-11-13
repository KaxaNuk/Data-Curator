import datetime
import enum
import typing

import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.entities import (
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    FundamentalDataRow,
)
from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.services.data_provider_toolkit import (
    EndpointFieldMap,
    PreprocessedFieldMapping,
)


class Endpoints(enum.StrEnum):
    BALANCE_SHEET_STATEMENT = '/balance-sheet-statement'
    CASH_FLOW_STATEMENT = '/cash-flow-statement'
    INCOME_STATEMENT = '/income-statement'

fundamental_data_endpoint_map : typing.Final[EndpointFieldMap] = {
    Endpoints.BALANCE_SHEET_STATEMENT: {
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
    Endpoints.CASH_FLOW_STATEMENT: {
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
    Endpoints.INCOME_STATEMENT: {
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

expected_fundamental_data_relation_map = {
    FundamentalDataRow: {
        FundamentalDataRow.balance_sheet: FundamentalDataRowBalanceSheet,
        FundamentalDataRow.cash_flow: FundamentalDataRowCashFlow,
        FundamentalDataRow.income_statement: FundamentalDataRowIncomeStatement,
    },
    FundamentalDataRowBalanceSheet: {},
    FundamentalDataRowCashFlow: {},
    FundamentalDataRowIncomeStatement: {},
}

assets_preprocessed_field_mapping =  PreprocessedFieldMapping(
    ['totalCurrentAssets', 'totalNonCurrentAssets'],
    [lambda current_assets, noncurrent_assets: current_assets + noncurrent_assets]
)
net_income_preprocessed_field_mapping = PreprocessedFieldMapping(
    ['netIncome'],
    [
        (lambda net_income: net_income * 100),
        (lambda net_income: net_income * 10)
    ]
)

ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS = {
    Endpoints.BALANCE_SHEET_STATEMENT: {
        FundamentalDataRow.period_end_date: 'date',
        FundamentalDataRowBalanceSheet.current_assets: 'totalCurrentAssets',
        FundamentalDataRowBalanceSheet.assets: assets_preprocessed_field_mapping
    },
    Endpoints.CASH_FLOW_STATEMENT: {
        FundamentalDataRow.period_end_date: 'date',
        FundamentalDataRowCashFlow.net_income: net_income_preprocessed_field_mapping
    },
}

EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS = {
    Endpoints.BALANCE_SHEET_STATEMENT: {
        FundamentalDataRowBalanceSheet.assets: assets_preprocessed_field_mapping
    },
    Endpoints.CASH_FLOW_STATEMENT: {
        FundamentalDataRowCashFlow.net_income: net_income_preprocessed_field_mapping
    },
}

EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS = {
    Endpoints.BALANCE_SHEET_STATEMENT: {
        'date': ['FundamentalDataRow.period_end_date'],
        'totalCurrentAssets': [
            'FundamentalDataRowBalanceSheet.current_assets',
            'FundamentalDataRowBalanceSheet.assets$totalCurrentAssets',
        ],
        'totalNonCurrentAssets': [
            'FundamentalDataRowBalanceSheet.assets$totalNonCurrentAssets',
        ],
    },
    Endpoints.CASH_FLOW_STATEMENT: {
        'date': ['FundamentalDataRow.period_end_date'],
        'netIncome': ['FundamentalDataRowCashFlow.net_income$netIncome'],
    }
}

date_array = pyarrow.array([
    datetime.date(2023, 1, 2) + datetime.timedelta(days=i)
    for i in range(10)
])
total_current_assets_array = pyarrow.array([1, 1, 2, 2, 3, 3, 4, 4, 5, 5])
total_noncurrent_assets_array = pyarrow.array([1, 1, 2, 2, 3, 3, 2, 2, 1, 1])
net_income_array = pyarrow.array([100, 150, 200, 220, 230, 230, 210, 230, 300, 300])


EXAMPLE_TABLE_COLUMNS_PER_TAG_CASHFLOW = pyarrow.table({
    'date': date_array,
    'netIncome': net_income_array,
})
EXAMPLE_ENDPOINT_TABLES_PER_TAG = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'date': date_array,
        'totalCurrentAssets': total_current_assets_array,
        'totalNonCurrentAssets': total_noncurrent_assets_array,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'date': date_array,
        'netIncome': net_income_array,
    }),
}
EXAMPLE_ENDPOINT_TABLES_PER_FIELD = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'FundamentalDataRow.period_end_date': date_array,
        'FundamentalDataRowBalanceSheet.current_assets': total_current_assets_array,
        'FundamentalDataRowBalanceSheet.assets$totalCurrentAssets': total_current_assets_array,
        'FundamentalDataRowBalanceSheet.assets$totalNonCurrentAssets': total_noncurrent_assets_array,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'FundamentalDataRow.period_end_date': date_array,
        'FundamentalDataRowCashFlow.net_income$netIncome': net_income_array,
    }),
}
summed_assets_array = pyarrow.compute.add_checked(total_current_assets_array, total_noncurrent_assets_array)
multiplied_net_income_array = pyarrow.compute.multiply_checked(net_income_array, 1000)
EXAMPLE_ENDPOINT_TABLES_PROCESSED = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'FundamentalDataRow.period_end_date': date_array,
        'FundamentalDataRowBalanceSheet.current_assets': total_current_assets_array,
        'FundamentalDataRowBalanceSheet.assets': summed_assets_array,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'FundamentalDataRow.period_end_date': date_array,
        'FundamentalDataRowCashFlow.net_income': multiplied_net_income_array,
    }),
}
