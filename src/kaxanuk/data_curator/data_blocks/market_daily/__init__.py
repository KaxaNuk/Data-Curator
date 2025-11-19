import dataclasses
import typing


# from kaxanuk.data_curator.data_blocks import base_data_block
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
    prefix_to_entity_map = {
        'm': MarketDataDailyRow,
    }

    @classmethod
    def assemble_entities_from_consolidated_table(
        cls,
        *,
        # entity_tables: EntityBuildingTables # @todo receive consolidated table instead
        consolidated_table: ConsolidatedFieldsTable,
        common_field_data: FieldValueToEntityMap,
    ) -> MarketData:
        # @todo throw error if not sorted by date asc

        # package row entities
        # entity_tables = cls.create_entity_tables_from_consolidated_table(consolidated_table)
        # package into MArketData
        # error handling
        # return
        # market_daily_table = entity_tables[MarketDataDailyRow]
        common_market_fields = common_field_data[MarketData]
        identifier = common_market_fields[MarketData.main_identifier]
        # market_daily_column_names = set(market_daily_table.column_names)
        # row_field_names = {field.name for field in dataclasses.fields(MarketDataDailyRow)}
        # missing_field_names = row_field_names - market_daily_column_names
        # daily_rows = {}

        try:
            daily_rows = cls.pack_rows_entities_from_consolidated_table(
                consolidated_table
            )
        except DataBlockEntityPackingError as error:
            raise EntityProcessingError("Market data processing error") from error

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
