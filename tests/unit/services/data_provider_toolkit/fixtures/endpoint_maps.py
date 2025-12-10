import datetime
import enum

import pyarrow.compute

from kaxanuk.data_curator.entities import (
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRow,
)
from kaxanuk.data_curator.services.data_provider_toolkit import (
    PreprocessedFieldMapping,
)


class Endpoints(enum.StrEnum):
    BALANCE_SHEET_STATEMENT = '/balance-sheet-statement'
    CASH_FLOW_STATEMENT = '/cash-flow-statement'
    INCOME_STATEMENT = '/income-statement'


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
