"""
Package containing all our domain building blocks, i.e. Entities, Aggregates, Aggregate Roots and Value Objects.

The "entities" module name is a misnomer as it doesn't contain just entities, but it'll have to do for now, as it's
easier to understand and remember than "building_blocks".
"""

__all__ = [
    'BaseDataEntity',
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
    'MarketInstrumentIdentifier',
    'SplitData',
    'SplitDataRow',
]


# make these classes part of the public API of the base namespace
from kaxanuk.data_curator.entities.base_data_entity import BaseDataEntity
from kaxanuk.data_curator.entities.configuration import Configuration
from kaxanuk.data_curator.entities.main_identifier import MainIdentifier
from kaxanuk.data_curator.entities.market_instrument_identifier import MarketInstrumentIdentifier

# DEPRECATED: the following block-owned entities now live in their respective data block packages.
# They are re-exported here only for backward compatibility, so that existing
# `from kaxanuk.data_curator.entities import ...` imports keep working. Prefer importing them from
# their data block package (e.g. kaxanuk.data_curator.data_blocks.dividends.dividend_data) instead.
from kaxanuk.data_curator.data_blocks.dividends.dividend_data import DividendData
from kaxanuk.data_curator.data_blocks.dividends.dividend_data_row import DividendDataRow
from kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data import FundamentalData
from kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row import FundamentalDataRow
from kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row_balance_sheet import (
    FundamentalDataRowBalanceSheet
)
from kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row_cash_flow import FundamentalDataRowCashFlow
from kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row_income_statement import (
    FundamentalDataRowIncomeStatement
)
from kaxanuk.data_curator.data_blocks.market_daily.market_data import MarketData
from kaxanuk.data_curator.data_blocks.market_daily.market_data_daily_row import MarketDataDailyRow
from kaxanuk.data_curator.data_blocks.splits.split_data import SplitData
from kaxanuk.data_curator.data_blocks.splits.split_data_row import SplitDataRow
