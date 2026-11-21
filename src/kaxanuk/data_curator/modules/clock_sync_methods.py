"""
Standard methods for syncing the clock chain columns to their master clock.

The upscale and downscale methods match the rows of a reference clock to the rows of its master clock, while the
consolidation methods consolidate the reference array rows belonging to each master clock row into a single row.
"""
import pyarrow

from kaxanuk.data_curator.exceptions import DataColumnSyncError
from kaxanuk.data_curator.modules.data_column import (
    BEFORE_MASTER_CLOCK_INDEX,
    DataColumn,
)


class ConsolidationMethods:
    """
    Methods for consolidating the reference array rows belonging to each master clock row into a single row.

    Each method except `infill_last` and `null` uses the PyArrow grouped aggregation with its same name, and the
    master clock rows without any reference array rows get null. Only `infill_last` uses the reference array rows
    before the first master clock row, which the downscale methods mark with BEFORE_MASTER_CLOCK_INDEX.
    """

    @staticmethod
    def first(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into their first row.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The first reference array row belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='first',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def infill_last(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate each master clock row's reference array rows into their last row, infilling the rows without any.

        For the data holding states that last until they change, like ratings, as opposed to `last`. Each master clock
        row gets the last reference array row belonging to it, or else to the latest earlier master clock row with any,
        even when that row's value is null, so a newer null replaces an older value. The reference array rows before
        the first master clock row, marked with BEFORE_MASTER_CLOCK_INDEX, hold the state of the master clock rows
        before any other reference array row.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The last reference array row belonging to each master clock row or to the latest earlier one with any, or
        null for the master clock rows before all the reference array rows
        """
        if pyarrow.types.is_null(reference_array.type):
            return pyarrow.nulls(master_row_count)

        rows_table = pyarrow.Table.from_arrays(
            [
                master_row_indices,
                reference_array,
            ],
            names=[
                'master_row',
                'value',
            ],
        )
        # The reference array rows after the master clock don't belong to any master clock row, while the ones before
        # it keep their own group, holding the state of the first master clock rows
        assigned_rows_table = rows_table.filter(
            pyarrow.compute.is_valid(
                rows_table['master_row']
            )
        )
        # The ordered aggregations require disabling threads, and keeping the nulls lets a newer null replace an older
        # value
        consolidated_table = assigned_rows_table.group_by(
            'master_row',
            use_threads=False,
        ).aggregate([
            (
                'value',
                'last',
                pyarrow.compute.ScalarAggregateOptions(skip_nulls=False),
            ),
        ])
        master_rows = pyarrow.array(
            range(master_row_count),
            type=pyarrow.int64(),
        )
        latest_consolidated_rows = _match_latest_reference_rows(
            master_timestamps=master_rows,
            reference_timestamps=consolidated_table['master_row'].combine_chunks(),
        )
        infilled_values = pyarrow.compute.take(
            consolidated_table['value_last'],
            latest_consolidated_rows,
        )

        return infilled_values.combine_chunks()

    @staticmethod
    def last(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into their last row.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The last reference array row belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='last',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def max(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into their maximum value.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The maximum value of the reference array rows belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='max',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def mean(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into their mean value.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The mean value of the reference array rows belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='mean',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def min(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into their minimum value.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The minimum value of the reference array rows belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='min',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def null(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into null.

        For the data without any meaningful consolidation, like volume-weighted average prices without their volumes.
        The nulls keep the reference array's type, so the column keeps the same type at any scale.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to, unused but part of the interface
        master_row_count
            The number of master clock rows

        Returns
        -------
        A null for each master clock row, with the reference array's type
        """
        return pyarrow.nulls(
            master_row_count,
            type=reference_array.type,
        )

    @staticmethod
    def product(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into the product of their values.

        For the factors that compound, like the numerators and denominators of several stock splits in the same period.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The product of the values of the reference array rows belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='product',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def sum(
        reference_array: pyarrow.Array,
        master_row_indices: pyarrow.Int64Array,
        master_row_count: int,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row into the sum of their values.

        Parameters
        ----------
        reference_array
            The data at the reference clock's scale
        master_row_indices
            The index of the master clock row each reference array row belongs to
        master_row_count
            The number of master clock rows

        Returns
        -------
        The sum of the values of the reference array rows belonging to each master clock row
        """
        return ConsolidationMethods._aggregate(
            aggregation='sum',
            master_row_count=master_row_count,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )

    @staticmethod
    def _aggregate(
        *,
        aggregation: str,
        master_row_count: int,
        master_row_indices: pyarrow.Int64Array,
        reference_array: pyarrow.Array,
    ) -> pyarrow.Array:
        """
        Consolidate the reference array rows belonging to each master clock row with a PyArrow grouped aggregation.

        Parameters
        ----------
        aggregation
            The name of the PyArrow grouped aggregation
        master_row_count
            The number of master clock rows
        master_row_indices
            The index of the master clock row each reference array row belongs to
        reference_array
            The data at the reference clock's scale

        Returns
        -------
        The aggregated value of the reference array rows belonging to each master clock row, or null for the master
        clock rows without any
        """
        if pyarrow.types.is_null(reference_array.type):
            return pyarrow.nulls(master_row_count)

        rows_table = pyarrow.Table.from_arrays(
            [
                master_row_indices,
                reference_array,
            ],
            names=[
                'master_row',
                'value',
            ],
        )
        # The reference array rows outside all the master clock rows don't belong to any of them, while the ones before
        # the master clock get their own group, which the master clock rows ignore
        assigned_rows_table = rows_table.filter(
            pyarrow.compute.is_valid(
                rows_table['master_row']
            )
        )
        # The ordered aggregations, like first and last, require disabling threads
        consolidated_table = assigned_rows_table.group_by(
            'master_row',
            use_threads=False,
        ).aggregate([
            (
                'value',
                aggregation,
            ),
        ])
        master_rows = pyarrow.array(
            range(master_row_count),
            type=pyarrow.int64(),
        )
        consolidated_rows = pyarrow.compute.index_in(
            master_rows,
            value_set=consolidated_table['master_row'],
        )
        consolidated_values = pyarrow.compute.take(
            consolidated_table[f'value_{aggregation}'],
            consolidated_rows,
        )

        return consolidated_values.combine_chunks()


class DownscaleMethods:
    """
    Methods for matching each reference clock row to the master clock row it belongs to.

    The periods are calculated by flooring the timestamps of both clocks to the master clock's reference period and
    periods per row, so each master clock row can be labeled with any timestamp inside its period, like its start, its
    end, or its first or last trading day. The reference clock rows before the first master clock row get
    BEFORE_MASTER_CLOCK_INDEX, so the consolidation methods holding states can start from them.
    """

    @staticmethod
    def group_by_calendar_period(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each reference clock row to the master clock row whose calendar period contains it.

        The reference clock rows between the master clock periods, like weekend ticks with a master clock of trading
        days, don't belong to any master clock row, as opposed to `group_by_next_period_end`.

        Parameters
        ----------
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column

        Returns
        -------
        The index of the master clock row each reference clock row belongs to, BEFORE_MASTER_CLOCK_INDEX for the
        reference clock rows before the first master clock row, or null for the rest of the reference clock rows
        outside all the master clock periods, as well as for the ones with null timestamps

        Raises
        ------
        DataColumnSyncError
            If several master clock rows fall in the same period
        """
        master_periods = _calculate_master_periods(master_clock_column)
        reference_periods = _calculate_reference_periods(
            master_clock_column=master_clock_column,
            reference_clock_column=reference_clock_column,
        )
        master_row_indices = pyarrow.compute.index_in(
            reference_periods,
            value_set=master_periods,
        )

        return _mark_rows_before_master_clock(
            master_periods=master_periods,
            master_row_indices=pyarrow.compute.cast(
                master_row_indices,
                pyarrow.int64(),
            ),
            reference_periods=reference_periods,
        )

    @staticmethod
    def group_by_next_period_end(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each reference clock row to the first master clock row whose period ends after it.

        For the reference clocks whose rows take effect from the master clock row in progress, or else the next one,
        like the dates of dividends or splits, as opposed to `group_by_calendar_period`. Each reference clock row
        matches the master clock row whose period contains it, or when it falls between the master clock periods, like
        a payment date on a weekend with a master clock of trading days, the next master clock row. So with an intraday
        master clock, the rows dated without a time, which start at midnight, match the first master clock row of their
        date.

        Parameters
        ----------
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column

        Returns
        -------
        The index of the master clock row each reference clock row belongs to, BEFORE_MASTER_CLOCK_INDEX for the
        reference clock rows before the first master clock row, or null for the ones after the last master clock row's
        period, as well as for the ones with null timestamps

        Raises
        ------
        DataColumnSyncError
            If several master clock rows fall in the same period
        """
        master_periods = _calculate_master_periods(master_clock_column)
        reference_periods = _calculate_reference_periods(
            master_clock_column=master_clock_column,
            reference_clock_column=reference_clock_column,
        )
        reference_row_count = len(reference_periods)

        # Placing the reference periods before the master periods keeps them first among equal periods through the
        # stable sort, so each reference period also matches the master period equal to it, while the null periods
        # get sorted at the end, after all the master periods
        combined_periods = pyarrow.concat_arrays([
            reference_periods,
            master_periods,
        ])
        combined_master_rows = pyarrow.concat_arrays([
            pyarrow.nulls(
                reference_row_count,
                type=pyarrow.int64(),
            ),
            pyarrow.array(
                range(len(master_periods)),
                type=pyarrow.int64(),
            ),
        ])
        sorted_positions = pyarrow.compute.sort_indices(combined_periods)
        sorted_master_rows = pyarrow.compute.take(
            combined_master_rows,
            sorted_positions,
        )

        # Each reference period gets the earliest master period sorted after it, and sorting the sorted positions
        # inverts the sort, getting back the reference periods in their own order
        next_master_rows = pyarrow.compute.fill_null_backward(sorted_master_rows)
        unsorted_positions = pyarrow.compute.sort_indices(sorted_positions)
        reference_next_master_rows = pyarrow.compute.take(
            next_master_rows,
            unsorted_positions.slice(0, reference_row_count),
        )

        return _mark_rows_before_master_clock(
            master_periods=master_periods,
            master_row_indices=reference_next_master_rows,
            reference_periods=reference_periods,
        )


class UpscaleMethods:
    """
    Methods for matching each master clock row to at most one reference clock row.
    """

    @staticmethod
    def match_same_period(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each master clock row to the last reference clock row in the same master clock period.

        The periods are calculated by flooring the timestamps of both clocks to the master clock's reference period
        and periods per row, so with a daily master clock each master clock row gets matched to the reference clock
        row with the same date. When several reference clock rows fall in the same period, the last one gets matched.

        Parameters
        ----------
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column

        Returns
        -------
        The index of the matching reference clock row for each master clock row, or null for the master clock rows
        without a match
        """
        reference_timestamps = _cast_to_master_clock_type(
            master_clock_column=master_clock_column,
            reference_clock_column=reference_clock_column,
        )
        master_periods = pyarrow.compute.floor_temporal(
            master_clock_column.reference_array,
            multiple=master_clock_column.periods_per_row,
            unit=master_clock_column.reference_period,
        )
        reference_periods = pyarrow.compute.floor_temporal(
            reference_timestamps,
            multiple=master_clock_column.periods_per_row,
            unit=master_clock_column.reference_period,
        )

        # index_in matches the first of any duplicate reference periods, so matching against the reversed reference
        # periods gets the last one instead
        reference_row_count = len(reference_periods)
        reversed_reference_rows = pyarrow.array(
            range(reference_row_count - 1, -1, -1),
            type=pyarrow.int64(),
        )
        reversed_reference_periods = pyarrow.compute.take(
            reference_periods,
            reversed_reference_rows,
        )
        reversed_matches = pyarrow.compute.index_in(
            master_periods,
            value_set=reversed_reference_periods,
        )
        last_reference_row = pyarrow.scalar(
            reference_row_count - 1,
            type=pyarrow.int64(),
        )

        return pyarrow.compute.subtract(
            last_reference_row,
            pyarrow.compute.cast(
                reversed_matches,
                pyarrow.int64(),
            ),
        )

    @staticmethod
    def infill_from_period_end(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each master clock row to the latest reference clock row whose period ends at or before the master row's.

        For the reference clocks whose rows hold the data of a whole period, which is only known once the period ends,
        like daily or weekly price bars, as opposed to `infill_from_timestamp`. The end of each row's period gets
        calculated by ceiling its timestamp to its own clock's reference period and periods per row, so the rows can be
        labeled with any timestamp inside their period, like its start or its last trading day. With a daily master
        clock, each row of a daily reference clock matches the master clock row of its same date, while each row of a
        weekly one only matches from the master clock rows of the following week onward.

        Each reference clock row stays matched to all the following master clock rows until the next reference clock
        row, without any limit on how long. When several reference clock rows' periods end at the same time, the last
        one gets matched. The reference clock rows don't need to be sorted, and the ones with null timestamps never get
        matched.

        Parameters
        ----------
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column

        Returns
        -------
        The index of the matching reference clock row for each master clock row, or null for the master clock rows
        whose periods end before those of all the reference clock rows
        """
        reference_timestamps = _cast_to_master_clock_type(
            master_clock_column=master_clock_column,
            reference_clock_column=reference_clock_column,
        )

        # Ceiling strictly gets the start of the next period even for the timestamps already on a period start
        master_period_ends = pyarrow.compute.ceil_temporal(
            master_clock_column.reference_array,
            multiple=master_clock_column.periods_per_row,
            unit=master_clock_column.reference_period,
            ceil_is_strictly_greater=True,
        )
        reference_period_ends = pyarrow.compute.ceil_temporal(
            reference_timestamps,
            multiple=reference_clock_column.periods_per_row,
            unit=reference_clock_column.reference_period,
            ceil_is_strictly_greater=True,
        )

        return _match_latest_reference_rows(
            master_timestamps=master_period_ends,
            reference_timestamps=reference_period_ends,
        )

    @staticmethod
    def infill_from_timestamp(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each master clock row to the latest reference clock row at or before it.

        For the reference clocks whose rows hold data known from their timestamps onward, like financial statements
        labeled by their filing dates, as opposed to `infill_from_period_end`.

        Each reference clock row stays matched to all the following master clock rows until the next reference clock
        row, without any limit on how long. When several reference clock rows share the same timestamp, the last one
        gets matched. The reference clock rows don't need to be sorted, and the ones with null timestamps never get
        matched.

        Parameters
        ----------
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column

        Returns
        -------
        The index of the matching reference clock row for each master clock row, or null for the master clock rows
        before all the reference clock rows
        """
        reference_timestamps = _cast_to_master_clock_type(
            master_clock_column=master_clock_column,
            reference_clock_column=reference_clock_column,
        )

        return _match_latest_reference_rows(
            master_timestamps=master_clock_column.reference_array,
            reference_timestamps=reference_timestamps,
        )


def _calculate_master_periods(master_clock_column: DataColumn) -> pyarrow.Array:
    """
    Return the period of each master clock row, by flooring its timestamp to its reference period and periods per row.

    Parameters
    ----------
    master_clock_column
        The master clock column

    Returns
    -------
    The start of each master clock row's period, from earliest to latest

    Raises
    ------
    DataColumnSyncError
        If several master clock rows fall in the same period
    """
    master_periods = pyarrow.compute.floor_temporal(
        master_clock_column.reference_array,
        multiple=master_clock_column.periods_per_row,
        unit=master_clock_column.reference_period,
    )
    distinct_master_period_count = pyarrow.compute.count_distinct(master_periods).as_py()

    if distinct_master_period_count < len(master_periods):
        msg = " ".join([
            "DataColumn master clock has several rows in the same period of",
            f"{master_clock_column.periods_per_row} {master_clock_column.reference_period}",
        ])

        raise DataColumnSyncError(msg)

    return master_periods


def _calculate_reference_periods(
    *,
    master_clock_column: DataColumn,
    reference_clock_column: DataColumn,
) -> pyarrow.Array:
    """
    Return the master clock period of each reference clock row, by flooring its timestamp to the master's periods.

    Parameters
    ----------
    master_clock_column
        The master clock column
    reference_clock_column
        The reference clock column

    Returns
    -------
    The start of the master clock period each reference clock row falls in, or null for the rows with null timestamps
    """
    reference_timestamps = _cast_to_master_clock_type(
        master_clock_column=master_clock_column,
        reference_clock_column=reference_clock_column,
    )

    return pyarrow.compute.floor_temporal(
        reference_timestamps,
        multiple=master_clock_column.periods_per_row,
        unit=master_clock_column.reference_period,
    )


def _cast_to_master_clock_type(
    *,
    master_clock_column: DataColumn,
    reference_clock_column: DataColumn,
) -> pyarrow.Array:
    """
    Return the reference clock's timestamps cast to the master clock's type, so the timestamps can be compared.

    This also turns the null arrays of reference clocks with no timestamps into null timestamps.

    Parameters
    ----------
    master_clock_column
        The master clock column
    reference_clock_column
        The reference clock column

    Returns
    -------
    The reference clock's timestamps, with the same type as the master clock's
    """
    master_type = master_clock_column.reference_array.type
    reference_timestamps = reference_clock_column.reference_array

    if reference_timestamps.type == master_type:
        return reference_timestamps

    return pyarrow.compute.cast(
        reference_timestamps,
        master_type,
    )


def _mark_rows_before_master_clock(
    *,
    master_periods: pyarrow.Array,
    master_row_indices: pyarrow.Int64Array,
    reference_periods: pyarrow.Array,
) -> pyarrow.Int64Array:
    """
    Return the master row indices of the reference clock rows, with BEFORE_MASTER_CLOCK_INDEX for the rows before it.

    Parameters
    ----------
    master_periods
        The start of each master clock row's period, from earliest to latest
    master_row_indices
        The index of the master clock row each reference clock row belongs to, or null for the rows belonging to none
    reference_periods
        The start of the master clock period each reference clock row falls in

    Returns
    -------
    The master row indices, with BEFORE_MASTER_CLOCK_INDEX for the reference clock rows whose period is before the
    first master clock row's
    """
    if len(master_periods) == 0:
        # Without any master clock rows, no reference clock row comes before them

        return master_row_indices

    # The rows with null periods stay null, as comparing them gives null
    is_before_master_clock = pyarrow.compute.less(
        reference_periods,
        master_periods[0],
    )

    return pyarrow.compute.if_else(
        is_before_master_clock,
        pyarrow.scalar(
            BEFORE_MASTER_CLOCK_INDEX,
            type=pyarrow.int64(),
        ),
        master_row_indices,
    )


def _match_latest_reference_rows(
    *,
    master_timestamps: pyarrow.Array,
    reference_timestamps: pyarrow.Array,
) -> pyarrow.Int64Array:
    """
    Match each master timestamp to the latest reference timestamp at or before it.

    When several reference timestamps are equal, the last one gets matched. The reference timestamps don't need to be
    sorted, and the null ones never get matched.

    Parameters
    ----------
    master_timestamps
        The master clock's timestamps, or the timestamps derived from them, sorted from earliest to latest, with
        repetitions allowed
    reference_timestamps
        The reference clock's timestamps, or the timestamps derived from them, with the same type as the master ones

    Returns
    -------
    The index of the matching reference timestamp for each master timestamp, or null for the master timestamps before
    all the reference timestamps
    """
    reference_row_count = len(reference_timestamps)

    # Placing the reference timestamps before the master timestamps keeps them first among equal timestamps through
    # the stable sort, so each reference timestamp also matches the master timestamp equal to it, while the null
    # timestamps get sorted at the end, after all the master timestamps
    combined_timestamps = pyarrow.concat_arrays([
        reference_timestamps,
        master_timestamps,
    ])
    combined_reference_rows = pyarrow.concat_arrays([
        pyarrow.array(
            range(reference_row_count),
            type=pyarrow.int64(),
        ),
        pyarrow.nulls(
            len(master_timestamps),
            type=pyarrow.int64(),
        ),
    ])
    sorted_positions = pyarrow.compute.sort_indices(combined_timestamps)
    sorted_reference_rows = pyarrow.compute.take(
        combined_reference_rows,
        sorted_positions,
    )

    # Each master timestamp gets the latest reference timestamp sorted before it, and as the master timestamps are
    # already sorted, they keep their order through the stable sort
    infilled_reference_rows = pyarrow.compute.fill_null_forward(sorted_reference_rows)
    master_row_mask = pyarrow.compute.greater_equal(
        sorted_positions,
        reference_row_count,
    )

    return pyarrow.compute.filter(
        infilled_reference_rows,
        master_row_mask,
    )
