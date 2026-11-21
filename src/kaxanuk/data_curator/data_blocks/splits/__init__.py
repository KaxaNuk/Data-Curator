__all__ = [
    'SplitData',
    'SplitDataRow',
    'SplitsDataBlock',
]


import typing

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    ConsolidationMethodMap,
    FieldValueToEntityMap,
    PrefixEntityMap,
)
from .split_data import SplitData
from .split_data_row import SplitDataRow
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
)
from kaxanuk.data_curator.modules.clock_sync_methods import (
    ConsolidationMethods,
    DownscaleMethods,
)


class SplitsDataBlock(BaseDataBlock):
    # each split takes effect from the master clock row in progress, or the next one if it falls between them:
    clock_downscale_method = DownscaleMethods.group_by_next_period_end
    clock_spec_field = SplitData.clock_spec
    clock_sync_field = SplitDataRow.split_date
    # the split dates are instants, which never get upscaled:
    clock_upscale_method = None
    dated_factor_date_fields = (
        SplitDataRow.split_date,
    )
    dated_factor_value_fields = (
        SplitDataRow.numerator,
        SplitDataRow.denominator,
    )
    # every field is listed, so any new field needs its own method:
    # (several splits in the same period compound)
    downscale_consolidation_methods: typing.ClassVar[ConsolidationMethodMap] = {
        SplitDataRow.split_date: ConsolidationMethods.last,
        SplitDataRow.numerator: ConsolidationMethods.product,
        SplitDataRow.denominator: ConsolidationMethods.product,
    }
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = SplitData.main_identifier
    main_entity = SplitData
    prefix_entity_map: typing.ClassVar[PrefixEntityMap] = {
        's': SplitDataRow,
    }
    rows_field = SplitData.rows

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
                clock_spec=common_split_fields[SplitData.clock_spec],
                rows=typing.cast(
                    dict[str, SplitDataRow],
                    split_rows,
                ),
            )
        except (
            DataBlockEmptyError,
            EntityValueError
        ) as error:
            msg = f"Split data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity
