import typing

import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    FieldValueToEntityMap,
)
from kaxanuk.data_curator.entities import (
    FundamentalData,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    # MarketInstrumentIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
    FundamentalDataUnsortedRowDatesError,
)


class FundamentalsDataBlock(BaseDataBlock):
    clock_sync_field = FundamentalDataRow.filing_date   # @todo check if accepted_date should be used instead
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = FundamentalData.main_identifier
    main_entity = FundamentalData
    # main_prefix = 'f'
    prefix_entity_map: typing.Final = {
        'f': FundamentalDataRow,
        'fbs': FundamentalDataRowBalanceSheet,
        'fcf': FundamentalDataRowCashFlow,
        'fis': FundamentalDataRowIncomeStatement,
    }

    @classmethod
    def assemble_entities_from_consolidated_table(
        cls,
        *,
        consolidated_table: ConsolidatedFieldsTable,
        common_field_data: FieldValueToEntityMap,
    ) -> FundamentalData:
        common_market_fields = common_field_data[FundamentalData]
        identifier = common_market_fields[FundamentalData.main_identifier]

        if not cls.validate_column_sorted_without_duplicates(
            consolidated_table[
                cls.get_field_qualified_name(cls.clock_sync_field)
            ]
        ):
            msg = f"Fundamental data unordered or duplicate dates received for {identifier.identifier}"

            raise EntityProcessingError(msg)

        try:
            period_rows = cls.pack_rows_entities_from_consolidated_table(
                consolidated_table
            )
        except DataBlockEntityPackingError as error:
            msg = "Fundametal data processing error"

            raise EntityProcessingError(msg) from error

        try:
            if not period_rows:
                msg = f"No rows could be processed by the {cls.__name__} data block for {identifier.identifier}"

                raise DataBlockEmptyError(msg)

            data_entity = FundamentalData(
                main_identifier=common_market_fields[FundamentalData.main_identifier],
                rows=period_rows,
            )
        except (
            DataBlockEmptyError,
            EntityValueError,
            FundamentalDataUnsortedRowDatesError,
        ) as error:
            msg = f"Fundamental data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity

    @classmethod
    def find_consolidated_table_irregular_filing_rows(
        cls,
        /,
        consolidated_table: ConsolidatedFieldsTable,
    ) -> pyarrow.BooleanArray | None:
        mask = cls._find_irregular_filing_rows(
            consolidated_table=consolidated_table,
            filing_date_column_name=cls.get_field_qualified_name(FundamentalDataRow.filing_date),
            period_end_date_column_name=cls.get_field_qualified_name(FundamentalDataRow.period_end_date),
        )

        if pyarrow.compute.any(mask).as_py():
            return mask
        else:
            return None

    @staticmethod
    def _find_irregular_filing_rows(
        consolidated_table: ConsolidatedFieldsTable,
        filing_date_column_name: str,
        period_end_date_column_name: str,
    ) -> pyarrow.BooleanArray:

        # get sort indices for period end date
        sort_indices = pyarrow.compute.sort_indices(
            consolidated_table,
            [
                (period_end_date_column_name, 'descending')
            ]
        )
        sorted_table = consolidated_table.take(sort_indices)

        # Set filing dates to null where period end date is null
        period_end_dates = sorted_table[period_end_date_column_name]
        filing_dates = sorted_table[filing_date_column_name]
        filing_dates_masked = pyarrow.compute.if_else(
            pyarrow.compute.is_null(period_end_dates),
            pyarrow.compute.cast(None, filing_dates.type),
            filing_dates
        )
        filing_date_ints = filing_dates_masked.cast(pyarrow.int32())

        # most irregularities are where the cumulative_min is different from the current value
        filing_date_cumulative_min = pyarrow.compute.cumulative_min(filing_date_ints)
        irregular_min_dates_mask = pyarrow.compute.not_equal(
            filing_date_cumulative_min,
            filing_date_ints
        )

        # duplicate values after the first occurrence are also irregular
        posterior_duplicates_mask = FundamentalsDataBlock._calculate_array_posterior_duplicates_mask(
            filing_date_ints
        )
        mask_with_nones = pyarrow.compute.or_kleene(
            irregular_min_dates_mask,
            posterior_duplicates_mask
        )

        # Fill None values with False
        mask_filled = pyarrow.compute.fill_null(
            mask_with_nones,
            fill_value=False
        )

        resort_order = pyarrow.compute.sort_indices(sort_indices)
        reordered_mask = mask_filled.take(resort_order)

        return reordered_mask.combine_chunks()

    @staticmethod
    def _calculate_array_posterior_duplicates_mask(array: pyarrow.Array) -> pyarrow.BooleanArray:
        # Get value counts for all values in the array
        value_counts = pyarrow.compute.value_counts(array)

        # Filter to only values that appear more than once using PyArrow compute
        duplicate_entries = pyarrow.compute.filter(
            value_counts,
            pyarrow.compute.greater(
                pyarrow.compute.struct_field(value_counts, [1]),  # 'counts' field is at index 1
                1
            )
        )

        if len(duplicate_entries) == 0:
            return pyarrow.array(
                [False] * len(array),
                type=pyarrow.bool_()
            )

        # Build set of duplicate values
        duplicate_values = set()
        for i in range(len(duplicate_entries)):
            struct_val = duplicate_entries[i].as_py()
            duplicate_values.add(struct_val['values'])

        # Build mask, marking duplicates after first occurrence
        seen = set()
        mask = []

        for value in array:
            value_primitive = value.as_py()
            if (
                value_primitive is not None
                and value_primitive in duplicate_values
                and value_primitive in seen
            ):
                mask.append(True)
            else:
                mask.append(False)
                seen.add(value_primitive)

        return pyarrow.array(
            mask,
            type=pyarrow.bool_()
        )


__all__ = [
    'FundamentalsDataBlock',
]
