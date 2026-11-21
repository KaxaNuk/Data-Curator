"""
Data blocks for the ColumnBuilder tests, mostly subclassing the built-in data blocks.

Each subclass only maps its prefixes to its entities, plus the downscale consolidation methods of any new fields when
its parent data block lists every field. The intraday market data block is a new one, for an intraday master clock.
"""

import dataclasses
import datetime
import decimal
import typing

from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
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
from kaxanuk.data_curator.entities import (
    BaseDataEntity,
    ClockSpec,
    MainIdentifier,
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


@dataclasses.dataclass(frozen=True, slots=True)
class IntradayMarketDataRow(BaseDataEntity):
    # the local time of the identifier's exchange, without a time zone:
    timestamp: datetime.datetime
    close: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class IntradayMarketData(BaseDataEntity):
    main_identifier: MainIdentifier
    clock_spec: ClockSpec
    rows: dict[str, IntradayMarketDataRow]


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


class IntradayMarketDataBlock(BaseDataBlock):
    """
    Intraday market data, with its rows keyed by their local timestamps, for acting as an intraday master clock.
    """

    clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
    clock_spec_field = IntradayMarketData.clock_spec
    clock_sync_field = IntradayMarketDataRow.timestamp
    clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_period_end
    downscale_consolidation_methods: typing.ClassVar = {
        IntradayMarketDataRow: clock_sync_methods.ConsolidationMethods.last,
    }
    grouping_identifier_field = IntradayMarketData.main_identifier
    main_entity = IntradayMarketData
    prefix_entity_map: typing.ClassVar = {
        'mi': IntradayMarketDataRow,
    }
    rows_field = IntradayMarketData.rows
