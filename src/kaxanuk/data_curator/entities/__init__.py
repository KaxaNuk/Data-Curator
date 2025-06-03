"""
Package containing all our domain building blocks, i.e. Entities, Aggregates, Aggregate Roots and Value Objects.

The "entities" module name is a misnomer as it doesn't contain just entities, but it'll have to do for now, as it's
easier to understand and remember than "building_blocks".
"""

# make these classes part of the public API of the base namespace
from kaxanuk.data_curator.entities.configuration import Configuration
from kaxanuk.data_curator.entities.dividend_data import DividendData
from kaxanuk.data_curator.entities.dividend_data_row import DividendDataRow
from kaxanuk.data_curator.entities.fundamental_data import FundamentalData
from kaxanuk.data_curator.entities.fundamental_data_row import FundamentalDataRow
from kaxanuk.data_curator.entities.fundamental_data_row_balance_sheet import FundamentalDataRowBalanceSheet
from kaxanuk.data_curator.entities.fundamental_data_row_cash_flow import FundamentalDataRowCashFlow
from kaxanuk.data_curator.entities.fundamental_data_row_income_statement import FundamentalDataRowIncomeStatement
from kaxanuk.data_curator.entities.market_data import MarketData
from kaxanuk.data_curator.entities.market_data_daily_row import MarketDataDailyRow
from kaxanuk.data_curator.entities.split_data import SplitData
from kaxanuk.data_curator.entities.split_data import SplitDataRow
from kaxanuk.data_curator.entities.main_identifier import MainIdentifier


__all__ = [
    'Configuration',
    'DividendData',
    'DividendDataRow',
    'FundamentalData',
    'FundamentalDataRow',
    'FundamentalDataRowBalanceSheet',
    'FundamentalDataRowCashFlow',
    'FundamentalDataRowIncomeStatement',
    'MainIdentifier',
    'MarketData',
    'MarketDataDailyRow',
    'SplitData',
    'SplitDataRow',
]
