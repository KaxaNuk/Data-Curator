import typing

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    FieldValueToEntityMap,
)
from kaxanuk.data_curator.entities import (
    SplitData,
    SplitDataRow,
    # MarketInstrumentIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
)


class SplitsDataBlock(BaseDataBlock):
    clock_sync_field = SplitDataRow.split_date
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = SplitData.main_identifier
    main_entity = SplitData
    prefix_entity_map: typing.Final = {
        's': SplitDataRow,
    }

    @classmethod
    def assemble_entities_from_consolidated_table(
        cls,
        *,
        consolidated_table: ConsolidatedFieldsTable,
        common_field_data: FieldValueToEntityMap,
    ) -> SplitData:
        common_split_fields = common_field_data[SplitData]
        identifier = common_split_fields[SplitData.main_identifier]

        if not cls.validate_column_sorted_without_duplicates(
            consolidated_table[
                cls.get_field_qualified_name(cls.clock_sync_field)
            ]
        ):
            msg = f"Split data unordered or duplicate dates received for {identifier.identifier}"

            raise EntityProcessingError(msg)

        try:
            split_rows = cls.pack_rows_entities_from_consolidated_table(
                consolidated_table
            )
        except DataBlockEntityPackingError as error:
            msg = "Split data processing error"

            raise EntityProcessingError(msg) from error

        try:
            if not split_rows:
                msg = f"No rows could be processed by the {cls.__name__} data block for {identifier.identifier}"

                raise DataBlockEmptyError(msg)

            data_entity = SplitData(
                main_identifier=common_split_fields[SplitData.main_identifier],
                rows=split_rows,
            )
        except (
            DataBlockEmptyError,
            EntityValueError
        ) as error:
            msg = f"Split data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity


__all__ = [
    'SplitsDataBlock',
]
