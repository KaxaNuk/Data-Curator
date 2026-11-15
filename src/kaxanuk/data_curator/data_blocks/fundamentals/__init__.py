__all__ = [
    'FundamentalData',
    'FundamentalDataRow',
    'FundamentalDataRowBalanceSheet',
    'FundamentalDataRowCashFlow',
    'FundamentalDataRowIncomeStatement',
    'FundamentalsDataBlock',
]


import datetime
import itertools
import typing

import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    ConsolidationMethodMap,
    FieldValueToEntityMap,
    PrefixEntityMap,
)
from .fundamental_data import FundamentalData
from .fundamental_data_row import FundamentalDataRow
from .fundamental_data_row_balance_sheet import FundamentalDataRowBalanceSheet
from .fundamental_data_row_cash_flow import FundamentalDataRowCashFlow
from .fundamental_data_row_income_statement import FundamentalDataRowIncomeStatement
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
    FundamentalDataUnsortedRowDatesError,
)
from kaxanuk.data_curator.modules.clock_sync_methods import (
    ConsolidationMethods,
    DownscaleMethods,
    UpscaleMethods,
)


class FundamentalsDataBlock(BaseDataBlock):
    clock_downscale_method = DownscaleMethods.calendar_periods
    clock_spec_field = FundamentalData.clock_spec
    clock_sync_field = FundamentalDataRow.filing_date   # @todo check if accepted_date should be used instead
    # each filing is known from its filing date onward:
    clock_upscale_method = UpscaleMethods.infill_from_timestamp
    # the balance sheet holds balances at the period's end, while the other statements hold the period's totals:
    downscale_consolidation_methods: typing.ClassVar[ConsolidationMethodMap] = {
        FundamentalDataRow: ConsolidationMethods.last,
        FundamentalDataRowBalanceSheet: ConsolidationMethods.last,
        FundamentalDataRowCashFlow: ConsolidationMethods.sum,
        # the cash balances at the period's start and end:
        FundamentalDataRowCashFlow.period_start_cash: ConsolidationMethods.first,
        FundamentalDataRowCashFlow.period_end_cash: ConsolidationMethods.last,
        FundamentalDataRowIncomeStatement: ConsolidationMethods.sum,
        # the earnings per share can't be consolidated without their net incomes and share counts:
        FundamentalDataRowIncomeStatement.basic_earnings_per_share: ConsolidationMethods.null,
        FundamentalDataRowIncomeStatement.diluted_earnings_per_share: ConsolidationMethods.null,
        # the share counts are averages over the period:
        FundamentalDataRowIncomeStatement.weighted_average_basic_shares_outstanding: ConsolidationMethods.mean,
        FundamentalDataRowIncomeStatement.weighted_average_diluted_shares_outstanding: ConsolidationMethods.mean,
    }
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = FundamentalData.main_identifier
    main_entity = FundamentalData
    # main_prefix = 'f'
    prefix_entity_map: typing.ClassVar[PrefixEntityMap] = {
        'f': FundamentalDataRow,
        'fbs': FundamentalDataRowBalanceSheet,
        'fcf': FundamentalDataRowCashFlow,
        'fis': FundamentalDataRowIncomeStatement,
    }
    rows_field = FundamentalData.rows

    # the number of months spanned by the rows of companies reporting quarterly and half-yearly:
    _QUARTERLY_ROW_SPAN_MONTHS = 3
    _HALF_YEARLY_ROW_SPAN_MONTHS = 6
    # the months a row's gap from the previous row can deviate from its span, like for 31 January period end dates:
    _ROW_SPAN_TOLERANCE_MONTHS = 1

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
                clock_spec=common_market_fields[FundamentalData.clock_spec],
                rows=typing.cast(
                    dict[str, FundamentalDataRow | None],
                    period_rows,
                ),
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

    @classmethod
    def find_consolidated_table_row_span_months(
        cls,
        /,
        consolidated_table: ConsolidatedFieldsTable,
    ) -> int | None:
        """
        Find the number of months spanned by each quarterly row of the latest reporting frequency in the table.

        Companies reporting half-yearly get quarterly rows spanning 6 months instead of 3. The spans come from the
        gaps between the rows' period end dates, as data providers can label half-yearly rows as quarters.

        Parameters
        ----------
        consolidated_table
            The consolidated table of quarterly fundamental data rows

        Returns
        -------
        The quarterly or half-yearly number of months, or None if no gap between the rows matches either
        """
        period_end_dates = consolidated_table[
            cls.get_field_qualified_name(FundamentalDataRow.period_end_date)
        ]
        sort_indices = pyarrow.compute.sort_indices(period_end_dates)
        row_spans_months = cls._calculate_row_spans_months(
            period_end_dates.take(sort_indices).to_pylist()
        )

        return cls._find_latest_row_span_months(row_spans_months)

    @classmethod
    def find_consolidated_table_superseded_frequency_rows(
        cls,
        /,
        consolidated_table: ConsolidatedFieldsTable,
    ) -> pyarrow.BooleanArray | None:
        """
        Find the quarterly rows reported before the company changed its reporting frequency to the latest one.

        Each data entity has a single clock spec, so its rows can't mix the spans of different reporting frequencies.

        Parameters
        ----------
        consolidated_table
            The consolidated table of quarterly fundamental data rows

        Returns
        -------
        The mask of the rows reported with a superseded frequency, or None if all the rows have the latest one
        """
        period_end_dates = consolidated_table[
            cls.get_field_qualified_name(FundamentalDataRow.period_end_date)
        ]
        sort_indices = pyarrow.compute.sort_indices(period_end_dates)
        row_spans_months = cls._calculate_row_spans_months(
            period_end_dates.take(sort_indices).to_pylist()
        )
        latest_frequency_start = cls._find_latest_frequency_start(row_spans_months)
        if latest_frequency_start == 0:

            return None

        sorted_superseded_mask = pyarrow.array(
            [
                position < latest_frequency_start
                for position in range(len(row_spans_months))
            ],
            type=pyarrow.bool_(),
        )
        # the mask follows the period end date order, so it gets taken back to the table's row order:
        resort_order = pyarrow.compute.sort_indices(sort_indices)

        return sorted_superseded_mask.take(resort_order)

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

    @classmethod
    def _find_latest_frequency_start(
        cls,
        row_spans_months: list[int | None],
    ) -> int:
        """
        Find the position of the first row of the latest reporting frequency.

        Parameters
        ----------
        row_spans_months
            The number of months spanned by each row in period end date order, as calculated by
            `_calculate_row_spans_months`

        Returns
        -------
        The position of the first row of the latest reporting frequency, 0 if all the rows have it
        """
        latest_row_span_months = cls._find_latest_row_span_months(row_spans_months)
        if latest_row_span_months == cls._HALF_YEARLY_ROW_SPAN_MONTHS:
            # the half-yearly reporting starts after the latest quarterly row
            superseded_end_positions = [
                position
                for (position, row_span_months) in enumerate(row_spans_months)
                if row_span_months == cls._QUARTERLY_ROW_SPAN_MONTHS
            ]
        elif latest_row_span_months == cls._QUARTERLY_ROW_SPAN_MONTHS:
            # the quarterly reporting starts after the latest pair of consecutive half-yearly rows
            measured_positions = [
                position
                for (position, row_span_months) in enumerate(row_spans_months)
                if row_span_months is not None
            ]
            superseded_end_positions = [
                later_position
                for (earlier_position, later_position) in itertools.pairwise(measured_positions)
                if (
                    row_spans_months[earlier_position] == cls._HALF_YEARLY_ROW_SPAN_MONTHS
                    and row_spans_months[later_position] == cls._HALF_YEARLY_ROW_SPAN_MONTHS
                )
            ]
        else:
            superseded_end_positions = []

        if not superseded_end_positions:

            return 0

        return superseded_end_positions[-1] + 1

    @classmethod
    def _find_latest_row_span_months(
        cls,
        row_spans_months: list[int | None],
    ) -> int | None:
        """
        Find the number of months spanned by the rows of the latest reporting frequency.

        A single half-yearly gap after quarterly ones is a missing quarter, so half-yearly reporting needs either two
        consecutive half-yearly gaps, or a half-yearly gap as the only one.

        Parameters
        ----------
        row_spans_months
            The number of months spanned by each row in period end date order, as calculated by
            `_calculate_row_spans_months`

        Returns
        -------
        The quarterly or half-yearly number of months, or None if no row span is known
        """
        measured_spans_months = [
            row_span_months
            for row_span_months in row_spans_months
            if row_span_months is not None
        ]
        if not measured_spans_months:

            return None

        if (
            measured_spans_months[-1] == cls._HALF_YEARLY_ROW_SPAN_MONTHS
            and (
                len(measured_spans_months) == 1
                or measured_spans_months[-2] == cls._HALF_YEARLY_ROW_SPAN_MONTHS
            )
        ):

            return cls._HALF_YEARLY_ROW_SPAN_MONTHS

        return cls._QUARTERLY_ROW_SPAN_MONTHS

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

    @classmethod
    def _calculate_row_spans_months(
        cls,
        sorted_period_end_dates: list[datetime.date | None],
    ) -> list[int | None]:
        """
        Calculate the number of months spanned by each row, from the gap between its period end date and the previous.

        Parameters
        ----------
        sorted_period_end_dates
            The period end dates of the rows, in ascending order

        Returns
        -------
        The quarterly or half-yearly number of months spanned by each row, with None for the first row and for the
        rows whose gaps match neither, like the ones following missing rows
        """
        row_spans_months: list[int | None] = []
        for (position, period_end_date) in enumerate(sorted_period_end_dates):
            previous_period_end_date = (
                sorted_period_end_dates[position - 1]
                if position > 0
                else None
            )
            if (
                period_end_date is None
                or previous_period_end_date is None
            ):
                row_spans_months.append(None)

                continue

            gap_months = (
                (period_end_date.year - previous_period_end_date.year) * 12
                + period_end_date.month
                - previous_period_end_date.month
            )
            row_spans_months.append(
                next(
                    (
                        row_span_months
                        for row_span_months in (
                            cls._QUARTERLY_ROW_SPAN_MONTHS,
                            cls._HALF_YEARLY_ROW_SPAN_MONTHS,
                        )
                        if abs(gap_months - row_span_months) <= cls._ROW_SPAN_TOLERANCE_MONTHS
                    ),
                    None,
                )
            )

        return row_spans_months
