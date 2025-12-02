import dataclasses
import decimal
import datetime
import enum

import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.entities.base_data_entity import BaseDataEntity


@dataclasses.dataclass(frozen=True, slots=True)
class MiniFundamentalRowBalanceSheet(BaseDataEntity):
    assets: decimal.Decimal | None
    cash_and_cash_equivalents: decimal.Decimal | None

@dataclasses.dataclass(frozen=True, slots=True)
class MiniFundamentalRowCashFlow(BaseDataEntity):
    net_income: decimal.Decimal | None
    net_income_tax_payments: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class MiniFundamentalRowIncomeStatement(BaseDataEntity):
    cost_of_revenue: decimal.Decimal | None
    net_income: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class MiniFundamentalRow(BaseDataEntity):
    accepted_date: datetime.datetime | None
    balance_sheet: MiniFundamentalRowBalanceSheet | None
    cash_flow: MiniFundamentalRowCashFlow | None
    income_statement: MiniFundamentalRowIncomeStatement | None
    filing_date: datetime.date


class Endpoints(enum.StrEnum):
    BALANCE_SHEET_STATEMENT = '/balance-sheet-statement'
    CASH_FLOW_STATEMENT = '/cash-flow-statement'
    INCOME_STATEMENT = '/income-statement'


filing_dates = pyarrow.array([
    # 2023-01-02 to 2023-01-05
    datetime.date(2023, 1, 2),
    datetime.date(2023, 1, 3),
    datetime.date(2023, 1, 4),
    datetime.date(2023, 1, 5),
    # 2023-01-08 to 2023-01-11
    datetime.date(2023, 1, 8),
    datetime.date(2023, 1, 9),
    datetime.date(2023, 1, 10),
    datetime.date(2023, 1, 11),
    # 2023-01-14 to 2023-01-17
    datetime.date(2023, 1, 14),
    datetime.date(2023, 1, 16),
    datetime.date(2023, 1, 15),
    datetime.date(2023, 1, 17),
])
filing_dates_reversed = filing_dates.take(
    pyarrow.array(range(len(filing_dates) - 1, -1, -1))
)
filing_dates_subset = pyarrow.array([
    # 2023-01-02 to 2023-01-05
    None,
    None,
    None,
    datetime.date(2023, 1, 5),
    # 2023-01-08 to 2023-01-11
    datetime.date(2023, 1, 8),
    None,
    datetime.date(2023, 1, 10),
    datetime.date(2023, 1, 11),
    # 2023-01-14 to 2023-01-17
    datetime.date(2023, 1, 14),
    datetime.date(2023, 1, 16),
    datetime.date(2023, 1, 15),
    datetime.date(2023, 1, 17),
])
filing_dates_subset_reversed = filing_dates_subset.take(
    pyarrow.array(range(len(filing_dates_subset) - 1, -1, -1))
)
filing_dates_shifted = pyarrow.array([
    # 2023-01-03 to 2023-01-06
    datetime.date(2023, 1, 3),
    datetime.date(2023, 1, 4),
    datetime.date(2023, 1, 5),
    datetime.date(2023, 1, 6),
    # 2023-01-09 to 2023-01-12
    datetime.date(2023, 1, 9),
    datetime.date(2023, 1, 10),
    datetime.date(2023, 1, 11),
    datetime.date(2023, 1, 12),
    # 2023-01-15 to 2023-01-18
    datetime.date(2023, 1, 16),
    datetime.date(2023, 1, 15),
    datetime.date(2023, 1, 17),
    datetime.date(2023, 1, 18),
])
filing_dates_shifted_reversed = filing_dates_shifted.take(
    pyarrow.array(range(len(filing_dates_shifted) - 1, -1, -1))
)
filing_dates_all = pyarrow.array([
    datetime.date(2023, 1, 2),
    datetime.date(2023, 1, 3),
    datetime.date(2023, 1, 4),
    datetime.date(2023, 1, 5),
    datetime.date(2023, 1, 6),

    datetime.date(2023, 1, 8),
    datetime.date(2023, 1, 9),
    datetime.date(2023, 1, 10),
    datetime.date(2023, 1, 11),
    datetime.date(2023, 1, 12),

    datetime.date(2023, 1, 14),
    datetime.date(2023, 1, 16),
    datetime.date(2023, 1, 15),
    datetime.date(2023, 1, 17),
    datetime.date(2023, 1, 18),
])
filing_dates_all_reversed = filing_dates_all.take(
    pyarrow.array(range(len(filing_dates_all) - 1, -1, -1))
)
filing_dates_inconsistent = pyarrow.array([
    # 2023-01-02 to 2023-01-05
    datetime.date(2023, 1, 2),
    datetime.date(2023, 1, 3),
    datetime.date(2023, 1, 10),
    datetime.date(2023, 1, 5),
    # 2023-01-08 to 2023-01-11
    datetime.date(2023, 1, 8),
    datetime.date(2023, 1, 9),
    datetime.date(2023, 1, 4),
    datetime.date(2023, 1, 11),
    # 2023-01-14 to 2023-01-17
    datetime.date(2023, 1, 14),
    datetime.date(2023, 1, 16),
    datetime.date(2023, 1, 15),
    datetime.date(2023, 1, 17),
])

accepted_dates = pyarrow.array([
    datetime.datetime(2023, 1, 15, 9, 30, 0),
    datetime.datetime(2023, 2, 14, 10, 0, 0),
    datetime.datetime(2023, 3, 20, 14, 15, 30),
    datetime.datetime(2023, 4, 10, 8, 45, 0),

    datetime.datetime(2023, 5, 5, 16, 20, 0),
    datetime.datetime(2023, 6, 18, 11, 30, 0),
    datetime.datetime(2023, 7, 22, 13, 0, 0),
    datetime.datetime(2023, 8, 8, 15, 45, 0),

    datetime.datetime(2023, 9, 30, 12, 10, 0),
    datetime.datetime(2023, 10, 12, 9, 0, 0),
    datetime.datetime(2023, 11, 25, 17, 30, 0),
    datetime.datetime(2023, 12, 30, 23, 59, 59),
])
accepted_dates_reversed = accepted_dates.take(
    pyarrow.array(range(len(accepted_dates) - 1, -1, -1))
)
accepted_dates_inconsistent = pyarrow.array([
    datetime.datetime(2023, 1, 15, 9, 30, 0),
    datetime.datetime(2023, 2, 14, 10, 0, 0),
    datetime.datetime(2023, 2, 20, 14, 15, 30),
    datetime.datetime(2023, 4, 10, 8, 45, 0),

    datetime.datetime(2023, 5, 5, 16, 20, 0),
    datetime.datetime(2023, 6, 18, 11, 30, 0),
    datetime.datetime(2023, 7, 22, 13, 0, 0),
    datetime.datetime(2023, 8, 8, 15, 45, 0),   # inconsistency

    datetime.datetime(2023, 9, 30, 12, 10, 0),
    datetime.datetime(2023, 10, 12, 9, 0, 0),
    datetime.datetime(2023, 11, 25, 17, 30, 0),
    datetime.datetime(2023, 12, 30, 23, 59, 59),
])
accepted_dates_inconsistent_none = pyarrow.array([
    datetime.datetime(2023, 1, 15, 9, 30, 0),
    datetime.datetime(2023, 2, 14, 10, 0, 0),
    datetime.datetime(2023, 3, 20, 14, 15, 30),
    datetime.datetime(2023, 4, 10, 8, 45, 0),

    datetime.datetime(2023, 5, 5, 16, 20, 0),
    datetime.datetime(2023, 6, 18, 11, 30, 0),
    datetime.datetime(2023, 7, 22, 13, 0, 0),
    None,

    datetime.datetime(2023, 9, 30, 12, 10, 0),
    datetime.datetime(2023, 10, 12, 9, 0, 0),
    datetime.datetime(2023, 11, 25, 17, 30, 0),
    datetime.datetime(2023, 12, 30, 23, 59, 59),
])
accepted_dates_subset = pyarrow.array([
    None,
    None,
    None,
    datetime.datetime(2023, 4, 10, 8, 45, 0),

    datetime.datetime(2023, 5, 5, 16, 20, 0),
    None,
    datetime.datetime(2023, 7, 22, 13, 0, 0),
    datetime.datetime(2023, 8, 8, 15, 45, 0),

    datetime.datetime(2023, 9, 30, 12, 10, 0),
    datetime.datetime(2023, 10, 12, 9, 0, 0),
    datetime.datetime(2023, 11, 25, 17, 30, 0),
    datetime.datetime(2023, 12, 30, 23, 59, 59),
])
accepted_dates_subset_reversed = accepted_dates_subset.take(
    pyarrow.array(range(len(accepted_dates_subset) - 1, -1, -1))
)
accepted_dates_shifted = pyarrow.array([
    datetime.datetime(2023, 2, 14, 10, 0, 0),
    datetime.datetime(2023, 3, 20, 14, 15, 30),
    datetime.datetime(2023, 4, 10, 8, 45, 0),
    datetime.datetime(2023, 4, 12, 8, 45, 0),

    datetime.datetime(2023, 6, 18, 11, 30, 0),
    datetime.datetime(2023, 7, 22, 13, 0, 0),
    datetime.datetime(2023, 8, 8, 15, 45, 0),
    datetime.datetime(2023, 8, 10, 16, 20, 0),

    datetime.datetime(2023, 10, 12, 9, 0, 0),
    datetime.datetime(2023, 11, 25, 17, 30, 0),
    datetime.datetime(2023, 12, 30, 23, 59, 59),
    datetime.datetime(2023, 12, 31, 12, 10, 0),
])
accepted_dates_shifted_reversed = accepted_dates_shifted.take(
    pyarrow.array(range(len(accepted_dates_shifted) - 1, -1, -1))
)
accepted_dates_all = pyarrow.array([
    datetime.datetime(2023, 1, 15, 9, 30, 0),
    datetime.datetime(2023, 2, 14, 10, 0, 0),
    datetime.datetime(2023, 3, 20, 14, 15, 30),
    datetime.datetime(2023, 4, 10, 8, 45, 0),
    datetime.datetime(2023, 4, 12, 8, 45, 0),

    datetime.datetime(2023, 5, 5, 16, 20, 0),
    datetime.datetime(2023, 6, 18, 11, 30, 0),
    datetime.datetime(2023, 7, 22, 13, 0, 0),
    datetime.datetime(2023, 8, 8, 15, 45, 0),
    datetime.datetime(2023, 8, 10, 16, 20, 0),

    datetime.datetime(2023, 9, 30, 12, 10, 0),
    datetime.datetime(2023, 10, 12, 9, 0, 0),
    datetime.datetime(2023, 11, 25, 17, 30, 0),
    datetime.datetime(2023, 12, 30, 23, 59, 59),
    datetime.datetime(2023, 12, 31, 12, 10, 0),
])
accepted_dates_all_reversed = accepted_dates_all.take(
    pyarrow.array(range(len(accepted_dates_all) - 1, -1, -1))
)
assets_subset = pyarrow.array([
    None,
    None,
    None,
    125000,

    126000,
    None,
    125000,
    220000,

    250000,
    240000,
    320000,
    840000,
])
assets_subset_reversed = assets_subset.take(
    pyarrow.array(range(len(assets_subset) - 1, -1, -1))
)
assets_all = pyarrow.array([
    None,
    None,
    None,
    125000,
    None,

    126000,
    None,
    125000,
    220000,
    None,

    250000,
    240000,
    320000,
    840000,
    None,
])
assets_all_reversed = assets_all.take(
    pyarrow.array(range(len(assets_all) - 1, -1, -1))
)
cash_and_cash_equivalents_subset = pyarrow.array([
    None,
    None,
    None,
    12500,

    24000,
    None,
    32000,
    22000,

    25000,
    12600,
    12500,
    84000,
])
cash_and_cash_equivalents_subset_reversed = cash_and_cash_equivalents_subset.take(
    pyarrow.array(range(len(cash_and_cash_equivalents_subset) - 1, -1, -1))
)
cash_and_cash_equivalents_all = pyarrow.array([
    None,
    None,
    None,
    12500,
    None,

    24000,
    None,
    32000,
    22000,
    None,

    25000,
    12600,
    12500,
    84000,
    None,
])
cash_and_cash_equivalents_all_reversed = cash_and_cash_equivalents_all.take(
    pyarrow.array(range(len(cash_and_cash_equivalents_all) - 1, -1, -1))
)
net_income = pyarrow.array([
    56000,
    15000,
    12000,
    -20000,

    20000,
    65000,
    250000,
    12000,

    56000,
    125000,
    6000,
    620000,
])
net_income_reversed = net_income.take(
    pyarrow.array(range(len(net_income) - 1, -1, -1))
)
net_income_shifted = pyarrow.array([
    15000,
    12000,
    -20000,
    -15000,

    65000,
    250000,
    12000,
    45000,

    125000,
    6000,
    620000,
    105000,
])
net_income_shifted_reversed = net_income_shifted.take(
    pyarrow.array(range(len(net_income_shifted) - 1, -1, -1))
)
net_income_all = pyarrow.array([
    56000,
    15000,
    12000,
    -20000,
    -15000,

    20000,
    65000,
    250000,
    12000,
    45000,

    56000,
    125000,
    6000,
    620000,
    105000,
])
net_income_all_reversed = net_income_all.take(
    pyarrow.array(range(len(net_income_all) - 1, -1, -1))
)
net_income_tax_payments_shifted = pyarrow.array([
    1500,
    1200,
    200,
    150,

    6500,
    25000,
    1200,
    4500,

    12500,
    600,
    62000,
    10500,
])
net_income_tax_payments_shifted_reversed = net_income_tax_payments_shifted.take(
    pyarrow.array(range(len(net_income_tax_payments_shifted) - 1, -1, -1))
)
net_income_tax_payments_all = pyarrow.array([
    None,
    1500,
    1200,
    200,
    150,

    None,
    6500,
    25000,
    1200,
    4500,

    None,
    12500,
    600,
    62000,
    10500,
])
net_income_tax_payments_all_reversed = net_income_tax_payments_all.take(
    pyarrow.array(range(len(net_income_tax_payments_all) - 1, -1, -1))
)

cost_of_revenue = pyarrow.array([
    2000,
    1000,
    2000,
    1000,

    2000,
    1000,
    2000,
    1000,

    2000,
    1000,
    2000,
    1000,
])
cost_of_revenue_reversed = cost_of_revenue.take(
    pyarrow.array(range(len(cost_of_revenue) - 1, -1, -1))
)
cost_of_revenue_all = pyarrow.array([
    2000,
    1000,
    2000,
    1000,
    None,

    2000,
    1000,
    2000,
    1000,
    None,

    2000,
    1000,
    2000,
    1000,
    None,
])
cost_of_revenue_all_reversed = cost_of_revenue_all.take(
    pyarrow.array(range(len(cost_of_revenue_all) - 1, -1, -1))
)

custom_order_subset1 = pyarrow.array([
    7,
    4,
    6,
    9,
])
custom_order_subset2 = pyarrow.array([
    1,
    4,
    5,
    9,
])
custom_order_subset3 = pyarrow.array([
    7,
    1,
    6,
    5,
])
custom_order_merge = pyarrow.array([
    7,
    1,
    4,
    6,
    5,
    9,
])

COMPOUND_KEY_SUBSET1_TABLE = pyarrow.table({
    'key1': pyarrow.array([
        datetime.date.fromisoformat('2023-01-02'),
        datetime.date.fromisoformat('2023-01-01'),
        # datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-03'),
        # datetime.date.fromisoformat('2023-01-05'),
        datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        21,
        21,
        # None,
        21,
        # 24,
        31
    ]),
})
COMPOUND_KEY_SUBSET2_TABLE = pyarrow.table({
    'key1': pyarrow.array([
        # datetime.date.fromisoformat('2023-01-02'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-01'),
        # datetime.date.fromisoformat('2023-01-03'),
        datetime.date.fromisoformat('2023-01-05'),
        datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        # 21,
        21,
        None,
        # 21,
        24,
        31
    ]),
})
COMPOUND_KEY_SUBSET3_TABLE = pyarrow.table({
    'key1': pyarrow.array([
        datetime.date.fromisoformat('2023-01-02'),
        # datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-03'),
        datetime.date.fromisoformat('2023-01-05'),
        # datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        21,
        # 21,
        None,
        21,
        24,
        # 31
    ]),
})
COMPOUND_KEY_MERGED_TABLE = pyarrow.table({
    'key1': pyarrow.array([
        datetime.date.fromisoformat('2023-01-02'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-03'),
        datetime.date.fromisoformat('2023-01-05'),
        datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        21,
        21,
        None,
        21,
        24,
        31
    ]),
})
COMPOUND_KEY_MERGED_TABLE_DUPLICATE_ROWS = pyarrow.table({
    'key1': pyarrow.array([
        datetime.date.fromisoformat('2023-01-02'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-03'),
        datetime.date.fromisoformat('2023-01-05'),
        datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        21,
        21,
        21,
        21,
        24,
        31
    ]),
})

COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE = pyarrow.table({
    'key1': pyarrow.array([
        datetime.date.fromisoformat('2023-01-02'),
        datetime.date.fromisoformat('2023-01-01'),
        # datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-03'),
        # datetime.date.fromisoformat('2023-01-05'),
        datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        21,
        21,
        # None,
        21,
        # 24,
        31
    ]),
})
COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE = pyarrow.table({
    'key1': pyarrow.array([
        # datetime.date.fromisoformat('2023-01-02'),
        datetime.date.fromisoformat('2023-01-01'),
        datetime.date.fromisoformat('2023-01-01'),
        # datetime.date.fromisoformat('2023-01-03'),
        datetime.date.fromisoformat('2023-01-05'),
        # datetime.date.fromisoformat('2023-01-05'),
    ]),
    'key2': pyarrow.array([
        # 21,
        21,
        None,
        # 21,
        24,
        # 31
    ]),
})

ENDPOINT_TABLES_CONSISTENT = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_subset,
        'MiniFundamentalRow.accepted_date': accepted_dates_subset,
        'MiniFundamentalRowBalanceSheet.assets': assets_subset,
        'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_subset,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_shifted,
        'MiniFundamentalRow.accepted_date': accepted_dates_shifted,
        'MiniFundamentalRowCashFlow.net_income': net_income_shifted,
        'MiniFundamentalRowCashFlow.net_income_tax_payments': net_income_tax_payments_shifted,
    }),
    Endpoints.INCOME_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates,
        'MiniFundamentalRow.accepted_date': accepted_dates,
        'MiniFundamentalRowIncomeStatement.cost_of_revenue': cost_of_revenue,
        'MiniFundamentalRowIncomeStatement.net_income': net_income,
    }),
}

ENDPOINT_TABLES_CONSISTENT_REVERSED = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_subset_reversed,
        'MiniFundamentalRow.accepted_date': accepted_dates_subset_reversed,
        'MiniFundamentalRowBalanceSheet.assets': assets_subset_reversed,
        'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_subset_reversed,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_shifted_reversed,
        'MiniFundamentalRow.accepted_date': accepted_dates_shifted_reversed,
        'MiniFundamentalRowCashFlow.net_income': net_income_shifted_reversed,
        'MiniFundamentalRowCashFlow.net_income_tax_payments': net_income_tax_payments_shifted_reversed,
    }),
    Endpoints.INCOME_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_reversed,
        'MiniFundamentalRow.accepted_date': accepted_dates_reversed,
        'MiniFundamentalRowIncomeStatement.cost_of_revenue': cost_of_revenue_reversed,
        'MiniFundamentalRowIncomeStatement.net_income': net_income_reversed,
    }),
}

ENDPOINT_TABLES_INCONSISTENT = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_subset,
        'MiniFundamentalRow.accepted_date': accepted_dates_subset,
        'MiniFundamentalRowBalanceSheet.assets': assets_subset,
        'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_subset,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_shifted,
        'MiniFundamentalRow.accepted_date': accepted_dates_shifted,
        'MiniFundamentalRowCashFlow.net_income': net_income_shifted,
        'MiniFundamentalRowCashFlow.net_income_tax_payments': net_income_tax_payments_shifted,
    }),
    Endpoints.INCOME_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates,
        'MiniFundamentalRow.accepted_date': accepted_dates_inconsistent,
        'MiniFundamentalRowIncomeStatement.cost_of_revenue': cost_of_revenue,
        'MiniFundamentalRowIncomeStatement.net_income': net_income,
    }),
}
ENDPOINT_TABLES_INCONSISTENT_WITH_NONE = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_subset,
        'MiniFundamentalRow.accepted_date': accepted_dates_subset,
        'MiniFundamentalRowBalanceSheet.assets': assets_subset,
        'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_subset,
    }),
    Endpoints.CASH_FLOW_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_shifted,
        'MiniFundamentalRow.accepted_date': accepted_dates_shifted,
        'MiniFundamentalRowCashFlow.net_income': net_income_shifted,
        'MiniFundamentalRowCashFlow.net_income_tax_payments': net_income_tax_payments_shifted,
    }),
    Endpoints.INCOME_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates,
        'MiniFundamentalRow.accepted_date': accepted_dates_inconsistent_none,
        'MiniFundamentalRowIncomeStatement.cost_of_revenue': cost_of_revenue,
        'MiniFundamentalRowIncomeStatement.net_income': net_income,
    }),
}
ENDPOINT_TABLES_SINGLE = {
    Endpoints.BALANCE_SHEET_STATEMENT: pyarrow.table({
        'MiniFundamentalRow.filing_date': filing_dates_subset,
        'MiniFundamentalRow.accepted_date': accepted_dates_subset,
        'MiniFundamentalRowBalanceSheet.assets': assets_subset,
        'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_subset,
    }),
}

CONSOLIDATED_TABLE = pyarrow.table({
    'MiniFundamentalRow.filing_date': filing_dates_all,
    'MiniFundamentalRow.accepted_date': accepted_dates_all,
    'MiniFundamentalRowBalanceSheet.assets': assets_all,
    'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_all,
    'MiniFundamentalRowCashFlow.net_income': net_income_all,
    'MiniFundamentalRowCashFlow.net_income_tax_payments': net_income_tax_payments_all,
    'MiniFundamentalRowIncomeStatement.cost_of_revenue': cost_of_revenue_all,
    'MiniFundamentalRowIncomeStatement.net_income': net_income_all,
})
CONSOLIDATED_TABLE_REVERSED = pyarrow.table({
    'MiniFundamentalRow.filing_date': filing_dates_all_reversed,
    'MiniFundamentalRow.accepted_date': accepted_dates_all_reversed,
    'MiniFundamentalRowBalanceSheet.assets': assets_all_reversed,
    'MiniFundamentalRowBalanceSheet.cash_and_cash_equivalents': cash_and_cash_equivalents_all_reversed,
    'MiniFundamentalRowCashFlow.net_income': net_income_all_reversed,
    'MiniFundamentalRowCashFlow.net_income_tax_payments': net_income_tax_payments_all_reversed,
    'MiniFundamentalRowIncomeStatement.cost_of_revenue': cost_of_revenue_all_reversed,
    'MiniFundamentalRowIncomeStatement.net_income': net_income_all_reversed,
})
NON_ENTITY_FIELD_KEYS_TABLE = pyarrow.table({
    'MiniFundamentalRow.filing_date': filing_dates_all,
    'accepted_date': accepted_dates_all,
})
INEXISTENT_ENTITY_KEYS_TABLE = pyarrow.table({
    'InexistentEntity.filing_date': filing_dates_all,
    'InexistentEntity.accepted_date': accepted_dates_all,
})
INEXISTENT_ENTITY_FIELD_KEYS_TABLE = pyarrow.table({
    'MiniFundamentalRow.filing_date': filing_dates_all,
    'MiniFundamentalRow.inexistent_date': accepted_dates_all,
})

ENTITY_TABLES = {
    MiniFundamentalRow: pyarrow.table({
        'filing_date': filing_dates_all,
        'accepted_date': accepted_dates_all,
    }),
    MiniFundamentalRowBalanceSheet: pyarrow.table({
        'assets': assets_all,
        'cash_and_cash_equivalents': cash_and_cash_equivalents_all,
    }),
    MiniFundamentalRowCashFlow: pyarrow.table({
        'net_income': net_income_all,
        'net_income_tax_payments': net_income_tax_payments_all,
    }),
    MiniFundamentalRowIncomeStatement: pyarrow.table({
        'cost_of_revenue': cost_of_revenue_all,
        'net_income': net_income_all,
    }),
}
