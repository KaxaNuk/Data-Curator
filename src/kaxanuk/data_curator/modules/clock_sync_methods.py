"""
Standard methods for syncing the clock chain columns to their master clock.

The upscale and downscale methods match the rows of a reference clock to the rows of its master clock, while the
consolidation methods consolidate the reference array rows belonging to each master clock row into a single row.
"""
import pyarrow

from kaxanuk.data_curator.exceptions import DataColumnSyncError
from kaxanuk.data_curator.modules.data_column import DataColumn


class ConsolidationMethods:
    """
    Methods for consolidating the reference array rows belonging to each master clock row into a single row.

    Each method uses the PyArrow grouped aggregation with its same name, and the master clock rows without any
    reference array rows get null.
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
        # The reference array rows outside all the master clock rows don't belong to any of them
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
    """

    @staticmethod
    def calendar_periods(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each reference clock row to the master clock row whose calendar period contains it.

        The periods are calculated by flooring the timestamps of both clocks to the master clock's reference period
        and periods per row, so each master clock row can be labeled with any timestamp inside its period, like its
        start, its end, or its first or last trading day. The reference clock rows outside all the master clock
        periods, like weekend ticks with a master clock of trading days, don't belong to any master clock row.

        Parameters
        ----------
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column

        Returns
        -------
        The index of the master clock row each reference clock row belongs to, or null for the reference clock rows
        outside all the master clock periods

        Raises
        ------
        DataColumnSyncError
            If several master clock rows fall in the same period
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
        distinct_master_period_count = pyarrow.compute.count_distinct(master_periods).as_py()

        if distinct_master_period_count < len(master_periods):
            msg = " ".join([
                "DataColumn master clock has several rows in the same period of",
                f"{master_clock_column.periods_per_row} {master_clock_column.reference_period}",
            ])

            raise DataColumnSyncError(msg)

        reference_periods = pyarrow.compute.floor_temporal(
            reference_timestamps,
            multiple=master_clock_column.periods_per_row,
            unit=master_clock_column.reference_period,
        )
        master_row_indices = pyarrow.compute.index_in(
            reference_periods,
            value_set=master_periods,
        )

        return pyarrow.compute.cast(
            master_row_indices,
            pyarrow.int64(),
        )


class UpscaleMethods:
    """
    Methods for matching each master clock row to at most one reference clock row.
    """

    @staticmethod
    def exact_match(
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
    def infill(
        master_clock_column: DataColumn,
        reference_clock_column: DataColumn,
    ) -> pyarrow.Int64Array:
        """
        Match each master clock row to the latest reference clock row at or before it.

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
        master_timestamps = master_clock_column.reference_array
        reference_row_count = len(reference_timestamps)

        # Placing the reference timestamps before the master timestamps keeps them first among equal timestamps
        # through the stable sort, so each reference clock row also matches the master clock row with its timestamp,
        # while the null timestamps get sorted at the end, after all the master clock rows
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

        # Each master clock row gets the latest reference clock row sorted before it, and as the master clock is
        # strictly increasing, its rows keep their order after the sort
        infilled_reference_rows = pyarrow.compute.fill_null_forward(sorted_reference_rows)
        master_row_mask = pyarrow.compute.greater_equal(
            sorted_positions,
            reference_row_count,
        )

        return pyarrow.compute.filter(
            infilled_reference_rows,
            master_row_mask,
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
