"""
Data blocks subclassing the built-in data blocks, for the ColumnBuilder tests.

Each one only maps its prefixes to its entities, plus the downscale consolidation methods of any new fields when its
parent data block lists every field.
"""

import dataclasses
import decimal
import typing

from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.data_blocks.dividends import (
    DividendDataRow,
    DividendsDataBlock,
)
from kaxanuk.data_curator.data_blocks.fundamentals import (
    FundamentalDataRowBalanceSheet,
    FundamentalsDataBlock,
)
from kaxanuk.data_curator.data_blocks.market_daily import (
    MarketDailyDataBlock,
    MarketDataDailyRow,
)


@dataclasses.dataclass(frozen=True, slots=True)
class ExtendedBalanceSheet(FundamentalDataRowBalanceSheet):
    crypto_assets: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExtendedDividendDataRow(DividendDataRow):
    withholding_tax_rate: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExtendedMarketDailyRow(MarketDataDailyRow):
    short_interest: decimal.Decimal | None


class AlternativeMarketDailyDataBlock(MarketDailyDataBlock):
    """
    Second source of market data, sharing its entities and main entity with MarketDailyDataBlock under its own prefix.
    """

    prefix_entity_map: typing.ClassVar = {
        'mx': MarketDataDailyRow,
    }


class ExtendedDividendsDataBlock(DividendsDataBlock):
    downscale_consolidation_methods: typing.ClassVar = {
        **DividendsDataBlock.downscale_consolidation_methods,
        ExtendedDividendDataRow.withholding_tax_rate: clock_sync_methods.ConsolidationMethods.last,
    }
    prefix_entity_map: typing.ClassVar = {
        'd': ExtendedDividendDataRow,
    }


class ExtendedFundamentalsDataBlock(FundamentalsDataBlock):
    prefix_entity_map: typing.ClassVar = {
        **FundamentalsDataBlock.prefix_entity_map,
        'fbs': ExtendedBalanceSheet,
    }


class ExtendedMarketDailyDataBlock(MarketDailyDataBlock):
    downscale_consolidation_methods: typing.ClassVar = {
        **MarketDailyDataBlock.downscale_consolidation_methods,
        ExtendedMarketDailyRow.short_interest: clock_sync_methods.ConsolidationMethods.last,
    }
    prefix_entity_map: typing.ClassVar = {
        'm': ExtendedMarketDailyRow,
    }
