__all__ = [
    'MarketDailyDataBlock',
    'MarketData',
    'MarketDataDailyRow',
]


import datetime
import typing

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    ConsolidationMethodMap,
    FieldValueToEntityMap,
    PrefixEntityMap,
)
from .market_data import MarketData
from .market_data_daily_row import MarketDataDailyRow
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
)
from kaxanuk.data_curator.modules.clock_sync_methods import (
    ConsolidationMethods,
    DownscaleMethods,
    UpscaleMethods,
)


class MarketDailyDataBlock(BaseDataBlock):
    clock_downscale_method = DownscaleMethods.calendar_periods
    clock_spec_field = MarketData.clock_spec
    clock_sync_field = MarketDataDailyRow.date
    # each daily bar is only known once its day ends:
    clock_upscale_method = UpscaleMethods.infill_from_period_end
    # every field is listed, so any new field needs its own method:
    # (the volume-weighted average prices can't be consolidated without their volumes)
    downscale_consolidation_methods: typing.ClassVar[ConsolidationMethodMap] = {
        MarketDataDailyRow.date: ConsolidationMethods.last,
        MarketDataDailyRow.open: ConsolidationMethods.first,
        MarketDataDailyRow.high: ConsolidationMethods.max,
        MarketDataDailyRow.low: ConsolidationMethods.min,
        MarketDataDailyRow.close: ConsolidationMethods.last,
        MarketDataDailyRow.volume: ConsolidationMethods.sum,
        MarketDataDailyRow.vwap: ConsolidationMethods.null,
        MarketDataDailyRow.open_split_adjusted: ConsolidationMethods.first,
        MarketDataDailyRow.high_split_adjusted: ConsolidationMethods.max,
        MarketDataDailyRow.low_split_adjusted: ConsolidationMethods.min,
        MarketDataDailyRow.close_split_adjusted: ConsolidationMethods.last,
        MarketDataDailyRow.volume_split_adjusted: ConsolidationMethods.sum,
        MarketDataDailyRow.vwap_split_adjusted: ConsolidationMethods.null,
        MarketDataDailyRow.open_dividend_and_split_adjusted: ConsolidationMethods.first,
        MarketDataDailyRow.high_dividend_and_split_adjusted: ConsolidationMethods.max,
        MarketDataDailyRow.low_dividend_and_split_adjusted: ConsolidationMethods.min,
        MarketDataDailyRow.close_dividend_and_split_adjusted: ConsolidationMethods.last,
        MarketDataDailyRow.volume_dividend_and_split_adjusted: ConsolidationMethods.sum,
        MarketDataDailyRow.vwap_dividend_and_split_adjusted: ConsolidationMethods.null,
    }
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = MarketData.main_identifier
    main_entity = MarketData
    prefix_entity_map: typing.ClassVar[PrefixEntityMap] = {
        'm': MarketDataDailyRow,
    }
    rows_field = MarketData.rows

    @classmethod
    def assemble_entities_from_consolidated_table(
        cls,
        *,
        consolidated_table: ConsolidatedFieldsTable,
        common_field_data: FieldValueToEntityMap,
    ) -> MarketData:
        common_market_fields = common_field_data[MarketData]
        identifier = common_market_fields[MarketData.main_identifier]

        if not cls.validate_column_sorted_without_duplicates(
            consolidated_table[
                cls.get_field_qualified_name(cls.clock_sync_field)
            ]
        ):
            msg = f"Market data unordered or duplicate dates received for {identifier.identifier}"

            raise EntityProcessingError(msg)

        try:
            daily_rows = cls.pack_rows_entities_from_consolidated_table(
                consolidated_table
            )
        except DataBlockEntityPackingError as error:
            msg = "Market data processing error"

            raise EntityProcessingError(msg) from error

        try:
            if not daily_rows:
                msg = f"No rows could be processed by the {cls.__name__} data block for {identifier.identifier}"

                raise DataBlockEmptyError(msg)

            # The dates come from the row keys, as the rows without any data get packed as None, which MarketData
            # then rejects along with any other row of the wrong type
            first_date = next(iter(daily_rows))
            last_date = next(reversed(daily_rows))
            data_entity = MarketData(
                start_date=datetime.date.fromisoformat(first_date),
                end_date=datetime.date.fromisoformat(last_date),
                main_identifier=common_market_fields[MarketData.main_identifier],
                clock_spec=common_market_fields[MarketData.clock_spec],
                rows=typing.cast(
                    dict[str, MarketDataDailyRow],
                    daily_rows,
                ),
            )
        except (
            DataBlockEmptyError,
            EntityValueError
        ) as error:
            msg = f"Market data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity
