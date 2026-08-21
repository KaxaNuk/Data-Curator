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


# @todo remove everything below, including its imports, once we stop redirecting the block-owned entities
#
# DEPRECATED: the following block-owned entities now live in their respective data block packages.
# They are redirected here only for backward compatibility, so that existing
# `from kaxanuk.data_curator.entities import ...` imports keep working. Prefer importing them from
# their data block package (e.g. kaxanuk.data_curator.data_blocks.dividends) instead.
#
# Every data block package imports BaseDataBlock, which imports this package's BaseDataEntity, so importing
# these entities at module level would make that circular. They get redirected on first access instead.
import importlib
import typing

_DEPRECATED_ENTITY_MODULE_NAMES : typing.Final = {
    'DividendData': 'kaxanuk.data_curator.data_blocks.dividends.dividend_data',
    'DividendDataRow': 'kaxanuk.data_curator.data_blocks.dividends.dividend_data_row',
    'FundamentalData': 'kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data',
    'FundamentalDataRow': 'kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row',
    'FundamentalDataRowBalanceSheet':
        'kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row_balance_sheet',
    'FundamentalDataRowCashFlow': 'kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row_cash_flow',
    'FundamentalDataRowIncomeStatement':
        'kaxanuk.data_curator.data_blocks.fundamentals.fundamental_data_row_income_statement',
    'MarketData': 'kaxanuk.data_curator.data_blocks.market_daily.market_data',
    'MarketDataDailyRow': 'kaxanuk.data_curator.data_blocks.market_daily.market_data_daily_row',
    'SplitData': 'kaxanuk.data_curator.data_blocks.splits.split_data',
    'SplitDataRow': 'kaxanuk.data_curator.data_blocks.splits.split_data_row',
}

if typing.TYPE_CHECKING:
    # the redirected entities are only imported here for static analysis, as they resolve on first access
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


def __getattr__(name: str) -> type:
    """
    Redirect the deprecated block-owned entities to their data block packages, on first access.

    The resolved entity gets cached in this package's namespace, so it only gets redirected once.

    Parameters
    ----------
    name
        The name of the package attribute being accessed

    Returns
    -------
    The entity class of that name, from the data block package that owns it

    Raises
    ------
    AttributeError
    """
    if name not in _DEPRECATED_ENTITY_MODULE_NAMES:
        msg = f"module {__name__!r} has no attribute {name!r}"

        raise AttributeError(msg)

    entity = getattr(
        importlib.import_module(
            _DEPRECATED_ENTITY_MODULE_NAMES[name]
        ),
        name
    )
    globals()[name] = entity

    return entity
