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


__all__ = [
    'MarketDailyDataBlock',
]
