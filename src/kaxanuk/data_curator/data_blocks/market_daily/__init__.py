__all__ = [
    'MarketDailyDataBlock',
]


import logging
import typing

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    FieldValueToEntityMap,
)
from kaxanuk.data_curator.entities import (
    MarketData,
    MarketDataDailyRow,
    # MarketInstrumentIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
)


class MarketDailyDataBlock(BaseDataBlock):
    clock_sync_field = MarketDataDailyRow.date
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = MarketData.main_identifier
    main_entity = MarketData
    prefix_entity_map: typing.Final = {
        'm': MarketDataDailyRow,
    }

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

        # A date whose every mapped field is null packs as None, which providers do serve:
        # Intrinio returns empty rows for dates before a security started trading, for one.
        # MarketData admits no empty daily row, and those dates carry nothing to output anyway,
        # so drop them rather than fail the whole security over them.
        empty_dates = [
            date
            for (date, row) in daily_rows.items()
            if row is None
        ]
        if empty_dates:
            daily_rows = {
                date: row
                for (date, row) in daily_rows.items()
                if row is not None
            }
            logging.getLogger(__name__).warning(
                "%s market data endpoints returned %d empty dates, dropping them: %s to %s",
                identifier.identifier,
                len(empty_dates),
                empty_dates[0],
                empty_dates[-1],
            )

        try:
            if not daily_rows:
                msg = f"No rows could be processed by the {cls.__name__} data block for {identifier.identifier}"

                raise DataBlockEmptyError(msg)

            first_date = next(iter(daily_rows))
            last_date = next(reversed(daily_rows))
            data_entity = MarketData(
                start_date=daily_rows[first_date].date,
                end_date=daily_rows[last_date].date,
                main_identifier=common_market_fields[MarketData.main_identifier],
                daily_rows=daily_rows,
            )
        except (
            DataBlockEmptyError,
            EntityValueError
        ) as error:
            msg = f"Market data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity
