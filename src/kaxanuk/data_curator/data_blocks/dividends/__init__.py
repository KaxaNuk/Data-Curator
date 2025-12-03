import typing

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    FieldValueToEntityMap,
)
from kaxanuk.data_curator.entities import (
    DividendData,
    DividendDataRow,
    # MarketInstrumentIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
)


class DividendsDataBlock(BaseDataBlock):
    clock_sync_field = DividendDataRow.ex_dividend_date
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = DividendData.main_identifier
    main_entity = DividendData
    prefix_entity_map: typing.Final = {
        'd': DividendDataRow,
    }

    @classmethod
    def assemble_entities_from_consolidated_table(
        cls,
        *,
        consolidated_table: ConsolidatedFieldsTable,
        common_field_data: FieldValueToEntityMap,
    ) -> DividendData:
        common_dividend_fields = common_field_data[DividendData]
        identifier = common_dividend_fields[DividendData.main_identifier]

        if not cls.validate_column_sorted_without_duplicates(
            consolidated_table[
                cls.get_field_qualified_name(cls.clock_sync_field)
            ]
        ):
            msg = f"Dividend data unordered or duplicate dates received for {identifier.identifier}"

            raise EntityProcessingError(msg)

        try:
            dividend_rows = cls.pack_rows_entities_from_consolidated_table(
                consolidated_table
            )
        except DataBlockEntityPackingError as error:
            msg = "Dividend data processing error"

            raise EntityProcessingError(msg) from error

        try:
            if not dividend_rows:
                msg = f"No rows could be processed by the {cls.__name__} data block for {identifier.identifier}"

                raise DataBlockEmptyError(msg)

            data_entity = DividendData(
                main_identifier=common_dividend_fields[DividendData.main_identifier],
                rows=dividend_rows,
            )
        except (
            DataBlockEmptyError,
            EntityValueError
        ) as error:
            msg = f"Dividend data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity


__all__ = [
    'DividendsDataBlock',
]
