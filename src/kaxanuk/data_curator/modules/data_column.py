import decimal
import enum
import sys
import typing

import pandas
import pyarrow

from kaxanuk.data_curator.exceptions import (
    DataColumnParameterError,
    DataColumnSyncError,
)


class ReferencePeriods(enum.StrEnum):
    """
    The base periods for expressing how much time each clock row spans, together with the clock's periods per row.

    Each clock uses the largest period that can express its frequency, like MONTH with 3 periods per row for
    quarterly data. Periods up to WEEK have fixed lengths, while MONTH and YEAR depend on the calendar. The values
    match PyArrow's temporal units, so they can be passed directly to functions like `pyarrow.compute.floor_temporal`.
    """

    SECOND = 'second'
    MINUTE = 'minute'
    HOUR = 'hour'
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'


# The reference periods with calendar-dependent lengths, by their number of months
CALENDAR_PERIOD_MONTHS = {
    ReferencePeriods.MONTH: 1,
    ReferencePeriods.YEAR: 12,
}
# The reference periods with fixed lengths, by their number of seconds
FIXED_PERIOD_SECONDS = {
    ReferencePeriods.SECOND: 1,
    ReferencePeriods.MINUTE: 60,
    ReferencePeriods.HOUR: 3_600,
    ReferencePeriods.DAY: 86_400,
    ReferencePeriods.WEEK: 604_800,
}
LONGEST_MONTH_DAYS = 31
SHORTEST_MONTH_DAYS = 28

# Receives a reference array, the index of the master clock row each of its rows belongs to, and the number of master
# clock rows, and returns the reference array consolidated into one row per master clock row
type ConsolidationMethod = typing.Callable[
    [
        pyarrow.Array,
        pyarrow.Int64Array,
        int,
    ],
    pyarrow.Array,
]
# Receives the master clock column and a reference clock column, and returns the index of the master clock row each
# reference clock row belongs to, or null for the reference clock rows outside all the master clock rows
type DownscaleMethod = typing.Callable[
    [
        DataColumn,
        DataColumn,
    ],
    pyarrow.Int64Array,
]
# The values other than DataColumns that can be operands of DataColumn operations, with bool included in int
type OperationScalar = int | float | decimal.Decimal | str | pyarrow.Scalar
# Receives the master clock column and a reference clock column, and returns the index of the reference clock row
# matching each master clock row, or null for the master clock rows without a match
type UpscaleMethod = typing.Callable[
    [
        DataColumn,
        DataColumn,
    ],
    pyarrow.Int64Array,
]


class DataColumn:
    """
    Columnar data structure for holding and combining PyArrow-based time series data.

    DataColumns should normally be created with `DataColumn.load()` instead of the constructor, as it accepts any
    iterable data, including existing DataColumns to derive new ones from.

    Operator overloading allows element-wise arithmetic operations with other columns and scalars, as well as
    element-wise comparisons between columns.

    The master and reference clock columns allow syncing data across different time scales. The master clock defines
    the time scale all columns get synced to, while each data column points to the reference clock matching its rows,
    which is either the master clock itself or a reference clock synced to it. The original data is kept at the
    reference clock's scale in `reference_array`, while `array` exposes it synced to the master clock's scale.

    Parameters
    ----------
    array
        The PyArrow array containing the data at the reference scale
    clock_downscale_method
        Only for clock columns: the method for matching each of this clock's rows to the master clock row it belongs
        to, used when this clock's rows span less time than the master clock's. Reference clocks require it then.
    clock_master
        Only for clock columns, as setting it turns this column into a clock, based on the type:
        - True: This column is the master clock, which all the other columns get synced to
        - DataColumn: This column is a reference clock, pointing to the master clock column it gets synced to
        - None: This column is not a clock
    clock_periods_per_row
        Only for clock columns: the number of reference periods spanned by each array row.
    clock_reference_period
        Only for clock columns: the base reference period of the array rows.
        Used for matching clocks between time scales, and for rolling window functions
    clock_upscale_method
        Only for clock columns: the method for matching each master clock row to at most one of this clock's rows,
        used when this clock's rows span at least as long as the master clock's. Reference clocks require it then.
    consolidation_method
        The method for consolidating the reference array rows belonging to each master clock row, used when the
        reference clock's rows span less time than the master clock's. Reference clocks require it then, while
        for non-clock columns None means using their reference clock's method.
    reference_clock_column
        Only for non-clock columns: the clock column whose rows match this column's array rows, either the master
        clock or a reference clock. Leave as None for a column with no clock at all.

    Attributes
    ----------
    array
        The data synced to the master clock's scale
    clock_chain_parameters
        The clock chain parameters exactly as received by the constructor
    consolidation_method
        The method for consolidating the reference array rows belonging to each master clock row, falling back to
        the reference clock's method
    downscale_method
        The method for matching each reference clock row to the master clock row it belongs to, taken from the
        reference clock
    master_clock_column
        The master clock column, which is this column itself if it's the master clock, or None if there's no clock
    periods_per_row
        The number of reference periods spanned by each reference array row, taken from the reference clock
    reference_array
        The data at the reference clock's scale
    reference_clock_column
        The reference clock column, which is this column itself if it's a clock, or None if there's no clock
    reference_period
        The base reference period of the reference array, taken from the reference clock
    sync_direction
        The direction in which the reference clock's rows get synced to the master clock's rows, taken from the
        reference clock, or None if there's nothing to sync
    sync_indices
        The row indices matching the reference clock's rows to the master clock's rows, taken from the reference
        clock, or None if there's nothing to sync
    type
        The PyArrow data type of the synced data
    upscale_method
        The method for matching each master clock row to at most one reference clock row, taken from the reference
        clock
    """

    MAX_FLOAT_EPSILON_UNITS_DISCREPANCY = 128   # max epsilon units discrepancy when comparing floats approximately

    class ClockChainParameters(typing.TypedDict, total=False):
        """
        The clock chain keyword parameters of the DataColumn constructor, all of them optional.

        Used for overriding an existing DataColumn's parameters when loading it into a new one with
        `DataColumn.load()`. See the DataColumn class for the description of each parameter.
        """

        clock_downscale_method: DownscaleMethod | None
        clock_master: 'DataColumn | typing.Literal[True] | None'
        clock_periods_per_row: int | None
        clock_reference_period: ReferencePeriods | None
        clock_upscale_method: UpscaleMethod | None
        consolidation_method: ConsolidationMethod | None
        reference_clock_column: 'DataColumn | None'

    class SyncDirections(enum.StrEnum):
        """
        The directions in which a reference clock's rows can get synced to its master clock's rows.

        DOWNSCALE consolidates the multiple reference clock rows belonging to each master clock row, as the
        reference clock rows span less time. UPSCALE matches each master clock row to at most one reference clock
        row, as the reference clock rows span at least as long.
        """

        DOWNSCALE = 'downscale'
        UPSCALE = 'upscale'

    def __init__(
        self,
        array: pyarrow.Array,
        /,
        *,
        clock_downscale_method: DownscaleMethod | None = None,
        clock_master: 'DataColumn | typing.Literal[True] | None' = None,
        clock_periods_per_row: int | None = None,
        clock_reference_period: ReferencePeriods | None = None,
        clock_upscale_method: UpscaleMethod | None = None,
        consolidation_method: ConsolidationMethod | None = None,
        reference_clock_column: 'DataColumn | None' = None,
    ):
        self._validate_clock_parameters(
            array=array,
            clock_downscale_method=clock_downscale_method,
            clock_master=clock_master,
            clock_periods_per_row=clock_periods_per_row,
            clock_reference_period=clock_reference_period,
            clock_upscale_method=clock_upscale_method,
            consolidation_method=consolidation_method,
            reference_clock_column=reference_clock_column,
        )
        self._validate_clock_array(
            array=array,
            clock_master=clock_master,
        )

        # Only reference clocks get synced to their master clock, and the previous validations already guarantee
        # that both clocks have their periods set, so the None checks only narrow the types.
        sync_direction: DataColumn.SyncDirections | None
        if (
            isinstance(clock_master, DataColumn)
            and clock_master.periods_per_row is not None
            and clock_master.reference_period is not None
            and clock_periods_per_row is not None
            and clock_reference_period is not None
        ):
            sync_direction = self._calculate_sync_direction(
                master_period=clock_master.reference_period,
                master_periods_per_row=clock_master.periods_per_row,
                reference_period=clock_reference_period,
                reference_periods_per_row=clock_periods_per_row,
            )
        else:
            sync_direction = None

        self._validate_sync_methods(
            clock_downscale_method=clock_downscale_method,
            clock_upscale_method=clock_upscale_method,
            consolidation_method=consolidation_method,
            sync_direction=sync_direction,
        )

        self._clock_downscale_method = clock_downscale_method
        self._clock_master = clock_master
        self._clock_periods_per_row = clock_periods_per_row
        self._clock_reference_period = clock_reference_period
        self._clock_upscale_method = clock_upscale_method
        self._consolidation_method = consolidation_method
        self._reference_array = array
        self._reference_clock_column = reference_clock_column
        self._sync_direction = sync_direction

        # These get calculated on first access and then cached, as they can be expensive to calculate
        self._master_synced_array: pyarrow.Array | None = None
        self._sync_indices: pyarrow.Int64Array | None = None

    @property
    def array(self) -> pyarrow.Array:
        """
        Return the underlying pyarrow.Array, synced to the master clock's scale.

        The synced array gets calculated on first access, and then cached.

        Returns
        -------
        The data synced to the master clock's scale
        """
        if self._master_synced_array is None:
            self._master_synced_array = self._calculate_master_synced_array(
                consolidation_method=self.consolidation_method,
                master_clock_column=self.master_clock_column,
                reference_array=self._reference_array,
                sync_direction=self.sync_direction,
                sync_indices=self.sync_indices,
            )

        return self._master_synced_array

    @property
    def clock_chain_parameters(self) -> 'DataColumn.ClockChainParameters':
        """
        Return the clock chain parameters exactly as received by the constructor.

        Unlike the properties resolving them through the clock columns, these allow creating a new DataColumn with the
        same parameters.

        Returns
        -------
        The clock chain keyword parameters received by the constructor
        """
        return {
            'clock_downscale_method': self._clock_downscale_method,
            'clock_master': self._clock_master,
            'clock_periods_per_row': self._clock_periods_per_row,
            'clock_reference_period': self._clock_reference_period,
            'clock_upscale_method': self._clock_upscale_method,
            'consolidation_method': self._consolidation_method,
            'reference_clock_column': self._reference_clock_column,
        }

    @property
    def consolidation_method(self) -> ConsolidationMethod | None:
        """
        Return the method for consolidating the reference array rows belonging to each master clock row.

        Non-clock columns without their own method get it from their reference clock column.

        Returns
        -------
        The consolidation method, or None if neither this column nor its reference clock have one
        """
        if (
            self._consolidation_method is None
            and self._reference_clock_column is not None
        ):
            return self._reference_clock_column.consolidation_method

        return self._consolidation_method

    @property
    def downscale_method(self) -> DownscaleMethod | None:
        """
        Return the method for matching each reference clock row to the master clock row it belongs to.

        Non-clock columns get it from their reference clock column.

        Returns
        -------
        The downscale method, or None if the reference clock doesn't have one or there's no clock
        """
        if self._reference_clock_column is not None:
            return self._reference_clock_column.downscale_method

        return self._clock_downscale_method

    @property
    def master_clock_column(self) -> typing.Union['DataColumn', None]:
        """
        Return the master clock column that this column gets synced to.

        Non-clock columns get it from their reference clock column.

        Returns
        -------
        The master clock column, which is this column itself if it's the master clock, or None if there's no clock
        """
        if self._clock_master is True:
            return self

        if self._clock_master is not None:
            return self._clock_master

        if self._reference_clock_column is not None:
            return self._reference_clock_column.master_clock_column

        return None

    @property
    def periods_per_row(self) -> int | None:
        """
        Return the number of reference periods spanned by each row of the reference array.

        Non-clock columns get it from their reference clock column.

        Returns
        -------
        The number of reference periods spanned by each row, or None if there's no clock
        """
        if self._reference_clock_column is not None:
            return self._reference_clock_column.periods_per_row

        return self._clock_periods_per_row

    @property
    def reference_array(self) -> pyarrow.Array:
        """
        Return the underlying pyarrow.Array at the reference clock's scale, without syncing it to the master clock.

        Returns
        -------
        The data at the reference clock's scale
        """
        return self._reference_array

    @property
    def reference_clock_column(self) -> typing.Union['DataColumn', None]:
        """
        Return the reference clock column whose rows match this column's reference array rows.

        Returns
        -------
        The reference clock column, which is this column itself if it's a clock, or None if there's no clock
        """
        if self._clock_master is not None:
            return self

        return self._reference_clock_column

    @property
    def reference_period(self) -> ReferencePeriods | None:
        """
        Return the base reference period of the reference array.

        Non-clock columns get it from their reference clock column.

        Returns
        -------
        The base reference period, or None if there's no clock
        """
        if self._reference_clock_column is not None:
            return self._reference_clock_column.reference_period

        return self._clock_reference_period

    @property
    def sync_direction(self) -> 'DataColumn.SyncDirections | None':
        """
        Return the direction in which the reference clock's rows get synced to the master clock's rows.

        Non-clock columns get it from their reference clock column.

        Returns
        -------
        The sync direction, or None for the master clock and its columns, as well as for columns with no clock
        """
        if self._reference_clock_column is not None:
            return self._reference_clock_column.sync_direction

        return self._sync_direction

    @property
    def sync_indices(self) -> pyarrow.Int64Array | None:
        """
        Return the row indices matching the reference clock's rows to the master clock's rows.

        When upscaling there's one index per master clock row, pointing to its matching reference clock row, while
        when downscaling there's one index per reference clock row, pointing to the master clock row it belongs to.
        Reference clocks calculate them on first access and then cache them, while non-clock columns get them from
        their reference clock column.

        Returns
        -------
        The sync indices, or None for the master clock and its columns, as well as for columns with no clock
        """
        if self._reference_clock_column is not None:
            return self._reference_clock_column.sync_indices

        if (
            self._sync_indices is None
            and isinstance(self._clock_master, DataColumn)
            and self._sync_direction is not None
        ):
            self._sync_indices = self._calculate_sync_indices(
                clock_downscale_method=self._clock_downscale_method,
                clock_upscale_method=self._clock_upscale_method,
                master_clock_column=self._clock_master,
                reference_clock_column=self,
                sync_direction=self._sync_direction,
            )

        return self._sync_indices

    @property
    def type(self) -> pyarrow.DataType:
        """
        Return the underlying native pyarrow.array object type.

        Returns
        -------
        pyarrow.DataType
        """
        return self.array.type

    @property
    def upscale_method(self) -> UpscaleMethod | None:
        """
        Return the method for matching each master clock row to at most one reference clock row.

        Non-clock columns get it from their reference clock column.

        Returns
        -------
        The upscale method, or None if the reference clock doesn't have one or there's no clock
        """
        if self._reference_clock_column is not None:
            return self._reference_clock_column.upscale_method

        return self._clock_upscale_method

    def __add__(
        self,
        addend: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise addition operator for DataColumn objects.

        Parameters
        ----------
        addend
            The other object to add to the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(addend)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            addend,
        )
        augend_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        addend_values = self._get_operation_array(
            addend,
            operation_reference_clock,
        )

        try:
            result = pyarrow.compute.add_checked(
                augend_values,
                addend_values
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.add_checked(
                    augend_values.cast(
                        pyarrow.float64()
                    ),
                    addend_values
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            augend_values,
            addend_values
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    # noinspection PyShadowingBuiltins
    def __arrow_array__(
        self,
        type: pyarrow.DataType = None   # pragma: no cover
    ) -> pyarrow.Array:
        """
        Implement the __arrow_array__ PyArrow extension protocol.

        Parameters
        ----------
        type
            The data type to use for the returned PyArrow Array.
            If not specified, it will pass through the current array as is.

        Returns
        -------
        The current DataColumn's underlying PyArrow array, cast to the specified type.
        """
        return pyarrow.array(self.array, type=type)

    # The element-wise comparison returns a DataColumn instead of object's bool, like NumPy and pandas do
    def __eq__(  # type: ignore[override]
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise equality comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            other,
        )
        own_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        other_values = self._get_operation_array(
            other,
            operation_reference_clock,
        )
        result = pyarrow.compute.equal(
            own_values,
            other_values
        )

        return DataColumn(
            result,
            reference_clock_column=operation_reference_clock,
        )

    def __floordiv__(
        self,
        divisor: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise floor division operator for DataColumn objects.

        Parameters
        ----------
        divisor
            The other object to divide the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(divisor)

        if null_result is not None:
            return null_result

        quotient = self / divisor

        return DataColumn(
            pyarrow.compute.floor(quotient.reference_array),
            reference_clock_column=quotient.reference_clock_column,
        )

    def __hash__(self) -> int:
        """
        Implement the `hash` function for DataColumn objects.

        This allows DataColumn objects to be used as keys in dictionaries or be added to sets.
        The hash is based on the type and contents of the array.

        Returns
        -------
        The hash value of the DataColumn object's internal PyArrow array.
        """
        hashable_tuple = (
            self.array.type,
            tuple(
                self.array.to_pylist()
            )
        )

        return hash(hashable_tuple)

    def __ge__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'greater than or equal' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            other,
        )
        own_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        other_values = self._get_operation_array(
            other,
            operation_reference_clock,
        )
        result = pyarrow.compute.greater_equal(
            own_values,
            other_values
        )

        return DataColumn(
            result,
            reference_clock_column=operation_reference_clock,
        )

    def __getitem__(
        self,
        items: slice | int
    ) -> 'DataColumn':
        """
        Implement index retrieval and slicing of the underlying pyarrow.Array.

        Parameters
        ----------
        items
            The slice or index to retrieve.

        Returns
        -------
        The result as a new DataColumn
        """
        if isinstance(items, slice):
            return DataColumn(
                self.array[items]
            )
        else:
            return DataColumn.load([self.array[items]])

    def __gt__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'greater than' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            other,
        )
        own_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        other_values = self._get_operation_array(
            other,
            operation_reference_clock,
        )
        result = pyarrow.compute.greater(
            own_values,
            other_values
        )

        return DataColumn(
            result,
            reference_clock_column=operation_reference_clock,
        )

    def __le__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'less than or equal' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            other,
        )
        own_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        other_values = self._get_operation_array(
            other,
            operation_reference_clock,
        )
        result = pyarrow.compute.less_equal(
            own_values,
            other_values
        )

        return DataColumn(
            result,
            reference_clock_column=operation_reference_clock,
        )

    def __lt__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'less than' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            other,
        )
        own_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        other_values = self._get_operation_array(
            other,
            operation_reference_clock,
        )
        result = pyarrow.compute.less(
            own_values,
            other_values
        )

        return DataColumn(
            result,
            reference_clock_column=operation_reference_clock,
        )

    def __len__(self) -> int:
        """
        Return the length of the underlying pyarrow.Array.

        Returns
        -------
        The length of the underlying pyarrow.Array
        """
        return len(self.array)

    def __mod__(
        self,
        divisor: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise modulo operator for DataColumn objects.

        Parameters
        ----------
        divisor
            The other object to divide the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        return (
            self
            - (
                (self // divisor)
                * divisor
            )
        )

    def __mul__(
        self,
        multiplier: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise multiplication operator for DataColumn objects.

        Any row involving null returns null.

        Parameters
        ----------
        multiplier
            The other object to multiply the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(multiplier)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            multiplier,
        )
        multiplicand_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        multiplier_values = self._get_operation_array(
            multiplier,
            operation_reference_clock,
        )

        try:
            result = pyarrow.compute.multiply_checked(
                multiplicand_values,
                multiplier_values
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.multiply_checked(
                    multiplicand_values.cast(
                        pyarrow.float64()
                    ),
                    multiplier_values
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            multiplicand_values,
            multiplier_values
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    # The element-wise comparison returns a DataColumn instead of object's bool, like NumPy and pandas do
    def __ne__(  # type: ignore[override]
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise inequality comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            other,
        )
        own_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        other_values = self._get_operation_array(
            other,
            operation_reference_clock,
        )
        result = pyarrow.compute.not_equal(
            own_values,
            other_values
        )

        return DataColumn(
            result,
            reference_clock_column=operation_reference_clock,
        )

    def __neg__(self) -> 'DataColumn':
        """
        Negate the entire contents of the DataColumn.

        Returns
        -------
        A new DataColumn
        """
        return DataColumn(
            pyarrow.compute.negate_checked(self.reference_array),
            reference_clock_column=self.reference_clock_column,
        )

    def __pos__(self):
        """
        Do nothing. Placeholder for possible future functionality.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def __radd__(
        self,
        augend: typing.Union[int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise addition operator for DataColumn objects.

        Parameters
        ----------
        augend : DataColumn | Int | Float | pyarrow.Scalar
            The other object to which the current object will be added

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(augend)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            augend,
            self,
        )
        augend_values = self._get_operation_array(
            augend,
            operation_reference_clock,
        )
        addend_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )

        try:
            result = pyarrow.compute.add_checked(
                augend_values,
                addend_values
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.add_checked(
                    augend_values,
                    addend_values.cast(
                        pyarrow.float64()
                    )
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            addend_values,
            augend_values
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    def __rfloordiv__(
        self,
        dividend: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise floor division operator for DataColumn objects.

        Parameters
        ----------
        dividend
            The other object to divide by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(dividend)

        if null_result is not None:
            return null_result

        quotient = self.__rtruediv__(dividend)

        return DataColumn(
            pyarrow.compute.floor(quotient.reference_array),
            reference_clock_column=quotient.reference_clock_column,
        )

    def __rmod__(
        self,
        dividend: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise modulo operator for DataColumn objects.

        Parameters
        ----------
        dividend
            The other object to divide by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        return (
            dividend
            - (
                (dividend // self)
                * self
            )
        )


    def __rmul__(
        self,
        multiplicand: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise multiplication operator for DataColumn objects.

        Any row involving null returns null.

        Parameters
        ----------
        multiplicand
            The other object to multiply by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(multiplicand)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            multiplicand,
            self,
        )
        multiplicand_values = self._get_operation_array(
            multiplicand,
            operation_reference_clock,
        )
        multiplier_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )

        try:
            result = pyarrow.compute.multiply_checked(
                multiplicand_values,
                multiplier_values
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.multiply_checked(
                    multiplicand_values,
                    multiplier_values.cast(
                        pyarrow.float64()
                    )
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            multiplier_values,
            multiplicand_values
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    def __rsub__(
        self,
        minuend: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise subtraction operator for DataColumn objects.

        Parameters
        ----------
        minuend
            The other object to subtract from the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(minuend)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            minuend,
            self,
        )
        minuend_values = self._get_operation_array(
            minuend,
            operation_reference_clock,
        )
        subtrahend_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )

        try:
            result = pyarrow.compute.subtract_checked(
                minuend_values,
                subtrahend_values
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.subtract_checked(
                    minuend_values,
                    subtrahend_values.cast(
                        pyarrow.float64()
                    )
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            subtrahend_values,
            minuend_values
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    def __rtruediv__(
        self,
        dividend: typing.Union[int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise division operator for DataColumn objects.

        Parameters
        ----------
        dividend
            The other object to divide by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it, which also covers a None dividend
        null_result = self._return_null_column_on_null_operand(dividend)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            dividend,
            self,
        )
        dividend_values = self._get_operation_array(
            dividend,
            operation_reference_clock,
        )
        divisor_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )

        # dividing by integers returns floats, as in Python's own division
        if (
            pyarrow.types.is_decimal(divisor_values.type)
            or pyarrow.types.is_integer(divisor_values.type)
        ):
            divisor = pyarrow.compute.cast(
                divisor_values,
                pyarrow.float64()
            )
        else:
            divisor = divisor_values

        divisor_mask = self._mask_zeroes(divisor)
        null_mask = self._mask_dual_array_nulls(
            divisor,
            dividend_values,
        )

        if pyarrow.compute.any(divisor_mask).as_py():
            clean_divisor = pyarrow.compute.if_else(
                divisor_mask,
                pyarrow.scalar(None, type=divisor_values.type),
                divisor
            )
        else:
            clean_divisor = divisor

        result = pyarrow.compute.divide_checked(
            dividend_values,
            clean_divisor
        )

        if null_mask is not None:
            final_mask = pyarrow.compute.or_kleene(
                null_mask,
                divisor_mask
            )
        else:
            final_mask = divisor_mask

        masked_result = self._replace_array_mask_with_nones(
            result,
            final_mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    def __sub__(
        self,
        subtrahend: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise subtraction operator for DataColumn objects.

        Parameters
        ----------
        subtrahend
            Other object to subtract from the current object.

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(subtrahend)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            subtrahend,
        )
        minuend_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )
        subtrahend_values = self._get_operation_array(
            subtrahend,
            operation_reference_clock,
        )

        try:
            result = pyarrow.compute.subtract_checked(
                minuend_values,
                subtrahend_values
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.subtract_checked(
                    minuend_values.cast(
                        pyarrow.float64()
                    ),
                    subtrahend_values
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            minuend_values,
            subtrahend_values
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    def __truediv__(
        self,
        divisor: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise division operator for DataColumn objects.

        Divisions involving decimals return a float column, as any initial precision of the involved decimals gets lost
        during the division.

        Parameters
        ----------
        divisor
            The other object to divide the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(divisor)

        if null_result is not None:
            return null_result

        operation_reference_clock = self._calculate_operation_reference_clock(
            self,
            divisor,
        )
        dividend_values = self._get_operation_array(
            self,
            operation_reference_clock,
        )

        if isinstance(divisor, DataColumn):
            divisor_array = self._get_operation_array(
                divisor,
                operation_reference_clock,
            )

            if pyarrow.types.is_decimal(divisor_array.type):
                divisor_values = divisor_array.cast(
                    pyarrow.float64()
                )
            else:
                divisor_values = divisor_array

            divisor_mask = self._mask_zeroes(divisor_values)
        else:
            divisor_values = divisor
            divisor_mask = None

        null_mask = self._mask_dual_array_nulls(
            dividend_values,
            divisor_values
        )

        if (
            null_mask is not None
            and divisor_mask is not None
        ):
            mask = pyarrow.compute.or_kleene(null_mask, divisor_mask)
        elif divisor_mask is not None:
            mask = divisor_mask
        else:
            mask = None

        if (
            divisor_mask is not None
            and pyarrow.compute.any(divisor_mask).as_py()
        ):
            masked_divisor = pyarrow.compute.if_else(
                divisor_mask,
                pyarrow.scalar(None, type=divisor_values.type),
                divisor_values
            )
        else:
            masked_divisor = divisor_values

        # dividing integers returns floats, as in Python's own division
        if (
            pyarrow.types.is_decimal(dividend_values.type)
            or pyarrow.types.is_integer(dividend_values.type)
        ):
            dividend = dividend_values.cast(
                pyarrow.float64()
            )
        else:
            dividend = dividend_values

        result = pyarrow.compute.divide_checked(
            dividend,
            masked_divisor
        )

        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(
            masked_result,
            reference_clock_column=operation_reference_clock,
        )

    @classmethod
    def boolean_and(
        cls,
        *columns: typing.Union['DataColumn', pyarrow.Scalar, bool],
        allow_null_comparisons: bool = False
    ) -> 'DataColumn':
        """
        Perform a logical AND comparison on multiple DataColumns.

        Parameters
        ----------
        *columns
            The columns to be combined with boolean AND logic.
        allow_null_comparisons
            Whether to allow null comparisons with Kleene logic. Default is False, which outputs null on any row
            containing any null value.

        Returns
        -------
        A new DataColumn containing the result of the logical AND comparison.
        """
        if len(columns) < 1:
            msg = "DataColumn.boolean_and() requires at least one parameter"

            raise DataColumnParameterError(msg)

        operation_reference_clock = cls._calculate_operation_reference_clock(*columns)
        recasted_columns = [
            cls._get_operation_array(
                column,
                operation_reference_clock,
            ).cast(
                pyarrow.bool_()
            )
                if isinstance(column, DataColumn)
                else column
            for column
            in columns
        ]

        if allow_null_comparisons:
            and_function = pyarrow.compute.and_kleene
        else:
            and_function = pyarrow.compute.and_

        result = recasted_columns[0]
        if len(recasted_columns) > 1:
            for column in recasted_columns[1:]:
                result = and_function(result, column)

        # Combining only scalars returns a scalar, which can't be loaded into a DataColumn
        if not isinstance(result, pyarrow.Array):
            msg = "DataColumn.boolean_and() requires at least one DataColumn parameter"

            raise DataColumnParameterError(msg)

        return DataColumn.load(
            result,
            reference_clock_column=operation_reference_clock,
        )

    @classmethod
    def boolean_or(
        cls,
        *columns: typing.Union['DataColumn', pyarrow.Scalar, bool],
        allow_null_comparisons: bool = False
    ) -> 'DataColumn':
        """
        Perform a logical OR comparison on multiple DataColumns.

        Parameters
        ----------
        *columns
            The columns to be combined with boolean OR logic.
        allow_null_comparisons
            Whether to allow null comparisons with Kleene logic. Default is False, which outputs null on any row
            containing any null value.

        Returns
        -------
        A new DataColumn containing the result of the logical OR comparison.
        """
        if len(columns) < 1:
            msg = "DataColumn.boolean_or() requires at least one parameter"

            raise DataColumnParameterError(msg)

        operation_reference_clock = cls._calculate_operation_reference_clock(*columns)
        recasted_columns = [
            cls._get_operation_array(
                column,
                operation_reference_clock,
            ).cast(
                pyarrow.bool_()
            )
                if isinstance(column, DataColumn)
                else column
            for column
            in columns
        ]

        if allow_null_comparisons:
            or_function = pyarrow.compute.or_kleene
        else:
            or_function = pyarrow.compute.or_

        result = recasted_columns[0]
        if len(recasted_columns) > 1:
            for column in recasted_columns[1:]:
                result = or_function(result, column)

        # Combining only scalars returns a scalar, which can't be loaded into a DataColumn
        if not isinstance(result, pyarrow.Array):
            msg = "DataColumn.boolean_or() requires at least one DataColumn parameter"

            raise DataColumnParameterError(msg)

        return DataColumn.load(
            result,
            reference_clock_column=operation_reference_clock,
        )

    @classmethod
    def concatenate(
        cls,
        *columns: typing.Union['DataColumn', pyarrow.Scalar, str],
        null_replacement: str = "",
        separator: str = "",
    ) -> typing.Union['DataColumn', pyarrow.Scalar]:
        """
        Concatenate DataColumns into one DataColumn.

        Parameters
        ----------
        *columns : 'DataColumn' | pyarrow.Scalar | str
            The columns to be concatenated. Each column can be either a 'DataColumn' object, a pyarrow.Scalar,
            or a string.

        null_replacement : str, optional
            The value to be used as replacement for null values in the concatenated result.
            Defaults to an empty string.

        separator : str, optional
            The separator to be used between concatenated values.
            Defaults to an empty string.

        Returns
        -------
        DataColumn | pyarrow.Scalar
            A new DataColumn containing the concatenated rows of the input columns, or a pyarrow.Scalar if all
            columns were strings or scalars.
        """
        operation_reference_clock = cls._calculate_operation_reference_clock(*columns)
        recasted_columns = (
            cls._get_operation_array(
                column,
                operation_reference_clock,
            ).cast(
                pyarrow.string()
            )
                if isinstance(column, DataColumn)
                else column
            for column
            in columns
        )
        concatenation = pyarrow.compute.binary_join_element_wise(
            *recasted_columns,
            separator,
            null_handling='replace',
            null_replacement=null_replacement
        )

        if isinstance(concatenation, pyarrow.Array):
            return DataColumn.load(
                concatenation,
                reference_clock_column=operation_reference_clock,
            )
        else:
            return concatenation

    @classmethod
    def equal(
        cls,
        column1: 'DataColumn',
        column2: 'DataColumn',
        /,
        *,
        approximate_floats: bool = False,
        equal_nulls: bool = False,
    ) -> 'DataColumn':
        """
        Compare two DataColumns element-wise.

        Parameters
        ----------
        column1
            The first column to compare.
        column2
            The second column to compare.
        equal_nulls
            Specifies whether null values should be considered equal. Default is False.
        approximate_floats
            Specifies whether floating-point value equality should compensate for rounding errors.
            Default is False.

        Returns
        -------
        A DataColumn containing a pyarrow.BooleanArray indicating element-wise equality between the two columns.
        """
        operation_reference_clock = cls._calculate_operation_reference_clock(
            column1,
            column2,
        )
        values1 = cls._get_operation_array(
            column1,
            operation_reference_clock,
        )
        values2 = cls._get_operation_array(
            column2,
            operation_reference_clock,
        )

        if (
            pyarrow.types.is_null(values1.type)
            and pyarrow.types.is_null(values2.type)
        ):
            if equal_nulls:
                return DataColumn(
                    values1.is_null(),  # all True
                    reference_clock_column=operation_reference_clock,
                )
            else:
                return DataColumn(
                    values1.is_valid(),  # all False
                    reference_clock_column=operation_reference_clock,
                )

        if approximate_floats:
            # The intermediate columns have no clock, as their values are already at the operation's scale
            aligned_column1 = DataColumn(values1)
            aligned_column2 = DataColumn(values2)

            # based on https://stackoverflow.com/a/32334103/5220723
            difference = pyarrow.compute.abs_checked(
                (aligned_column1 - aligned_column2).reference_array
            )
            norm = (
                DataColumn(
                    pyarrow.compute.abs_checked(values1)
                )
                + DataColumn(
                    pyarrow.compute.abs_checked(values2)
                )
            )
            base_equality = pyarrow.compute.less_equal(
                difference,
                (
                    norm
                    * (
                        cls.MAX_FLOAT_EPSILON_UNITS_DISCREPANCY
                        * sys.float_info.epsilon
                    )
                ).reference_array
            )
        else:
            base_equality = pyarrow.compute.equal(
                values1,
                values2
            )

        if (
            not equal_nulls
            or (
                values1.null_count < 1
                and values2.null_count < 1
            )
        ):
            return DataColumn(
                base_equality,
                reference_clock_column=operation_reference_clock,
            )

        else:
            nulls1 = pyarrow.compute.is_null(values1, nan_is_null=True)
            nulls2 = pyarrow.compute.is_null(values2, nan_is_null=True)
            result = pyarrow.compute.if_else(
                pyarrow.compute.is_null(base_equality, nan_is_null=True),
                pyarrow.compute.and_(nulls1, nulls2),
                base_equality
            )

            return DataColumn(
                result,
                reference_clock_column=operation_reference_clock,
            )

    @classmethod
    def fully_equal(
        cls,
        column1: 'DataColumn',
        column2: 'DataColumn',
        /,
        *,
        approximate_floats: bool = False,
        equal_nulls: bool = False,
        skip_nulls: bool = False
    ) -> bool | None:
        """
        Check if two DataColumns are fully equal.

        Parameters
        ----------
        column1
            The first column to compare.

        column2
            The second column to compare.

        approximate_floats: bool, optional
            Whether to consider floats as approximately equal. If True, floating-point comparison will use tolerance.
            If not specified, the default value is False.

        equal_nulls: bool, optional
            Whether to consider null values as equal. If True, null values will be treated as equal.
            If not specified, the default value is False.

        skip_nulls: bool | None, optional
            Whether to skip null values during comparison. If True, null values will be ignored.
            If not specified, the default value is False.

        Returns
        -------
        bool
            Returns None if equal_nulls is False and there are Nones, True if both columns are equal, False otherwise.
        """
        element_wise_equalities = cls.equal(
            column1,
            column2,
            approximate_floats=approximate_floats,
            equal_nulls=equal_nulls,
        )

        # The equalities get checked at the scale they were compared at, instead of synced to the master clock
        return (
            pyarrow.compute.all(
                element_wise_equalities.reference_array,
                skip_nulls=skip_nulls
            )
            .as_py()
        )

    def is_null(self) -> bool:
        """
        Check if the underlying pyarrow.Array is NullArray.

        Returns
        -------
        bool :
            whether or not the underlying pyarrow.Array is a NullArray
        """
        return self.array.type == pyarrow.null()

    @classmethod
    def load(
        cls,
        data: 'typing.Iterable | DataColumn',
        *,
        dtype: pyarrow.DataType = None,
        **clock_chain_parameters: 'typing.Unpack[DataColumn.ClockChainParameters]',
    ) -> 'DataColumn':
        """
        Wrap data (pyarrow.Array, pandas.Series, Iterable, DataColumn) in a new DataColumn object.

        When `data` is a DataColumn, the new column keeps its reference array and all its clock chain parameters
        except the ones passed in `clock_chain_parameters`, and if neither `dtype` nor any clock chain parameters are
        passed, the same DataColumn is returned.

        Parameters
        ----------
        data
            the data to be wrapped
        dtype
            the type of the underlying pyarrow.Array
        **clock_chain_parameters
            Any of the clock chain keyword parameters of the DataColumn constructor, as documented in the DataColumn
            class and listed in DataColumn.ClockChainParameters

        Returns
        -------
        DataColumn
        """
        if not isinstance(data, DataColumn):
            return DataColumn(
                pyarrow.array(
                    data,
                    from_pandas=(
                        isinstance(data, pandas.Series)
                        and data.hasnans
                    ),
                    type=dtype
                ),
                **clock_chain_parameters,
            )

        if (
            dtype is None
            and not clock_chain_parameters
        ):
            return data

        if dtype is None:
            reference_array = data.reference_array
        else:
            reference_array = pyarrow.array(
                data.reference_array,
                type=dtype
            )

        merged_clock_chain_parameters = data.clock_chain_parameters | clock_chain_parameters

        return DataColumn(
            reference_array,
            **merged_clock_chain_parameters,
        )

    def to_master_scale(self) -> 'DataColumn':
        """
        Return this column's data synced to the master clock's scale, as a column pointing to the master clock.

        Useful for operating on columns at the master clock's scale, like consolidating each operand with its own
        consolidation method before combining them, instead of combining them at their reference clock's scale.

        Returns
        -------
        The column synced to the master clock's scale, or this same column if it's already at that scale
        """
        master_clock_column = self.master_clock_column

        if (
            master_clock_column is None
            or self.reference_clock_column is master_clock_column
        ):
            # The column has no clock, or it's the master clock itself or one of its columns

            return self

        return DataColumn(
            self.array,
            reference_clock_column=master_clock_column,
        )

    def to_pandas(self) -> pandas.Series:
        """
        Force pandas to use PyArrow in the backend by means of ArrowExtensionArray.

        Cf. https://pandas.pydata.org/docs/user_guide/pyarrow.html

        Returns
        -------
        pandas.Series
        """
        return pandas.Series(
            # @todo remove the mypy ignore comment below once pandas has fixed its own stubs
            pandas.arrays.ArrowExtensionArray(self.array)   # type: ignore[attr-defined]
        )

    def to_pyarrow(self) -> pyarrow.Array:
        """
        Return the underlying native pyarrow.array object.

        Returns
        -------
        pyarrow.Array
        """
        return self.array

    @staticmethod
    def _calculate_master_synced_array(
        *,
        consolidation_method: ConsolidationMethod | None,
        master_clock_column: 'DataColumn | None',
        reference_array: pyarrow.Array,
        sync_direction: 'DataColumn.SyncDirections | None',
        sync_indices: pyarrow.Int64Array | None,
    ) -> pyarrow.Array:
        """
        Calculate the reference array synced to the master clock's scale.

        Upscaling takes the reference array row matching each master clock row, while downscaling consolidates the
        reference array rows belonging to each master clock row through the consolidation method.

        Parameters
        ----------
        consolidation_method
            The method for consolidating the reference array rows belonging to each master clock row
        master_clock_column
            The master clock column, or None if there's no clock
        reference_array
            The data at the reference clock's scale
        sync_direction
            The direction in which the reference clock's rows get synced to the master clock's rows, or None if
            there's nothing to sync
        sync_indices
            The row indices matching the reference clock's rows to the master clock's rows, or None if there's
            nothing to sync

        Returns
        -------
        The data synced to the master clock's scale

        Raises
        ------
        DataColumnSyncError
            If the consolidation method doesn't return a pyarrow.Array with one row per master clock row
        """
        if (
            sync_direction is None
            or sync_indices is None
            or master_clock_column is None
        ):
            # The reference array already matches the master clock's scale, or there's no clock at all

            return reference_array

        if sync_direction is DataColumn.SyncDirections.UPSCALE:
            return pyarrow.compute.take(
                reference_array,
                sync_indices
            )

        # The constructor validations guarantee that a downscaled column has a consolidation method, so this check
        # only narrows the type
        if consolidation_method is None:
            msg = "DataColumn can't be downscaled without a consolidation_method"

            raise DataColumnSyncError(msg)

        master_row_count = len(master_clock_column.reference_array)
        master_synced_array = consolidation_method(
            reference_array,
            sync_indices,
            master_row_count,
        )

        if (
            not isinstance(master_synced_array, pyarrow.Array)
            or len(master_synced_array) != master_row_count
        ):
            msg = " ".join([
                "DataColumn consolidation_method must return a pyarrow.Array",
                f"with one row for each of the {master_row_count} master clock rows",
            ])

            raise DataColumnSyncError(msg)

        return master_synced_array

    @staticmethod
    def _calculate_operation_reference_clock(
        *operands: 'DataColumn | OperationScalar',
    ) -> 'DataColumn | None':
        """
        Calculate the reference clock of an operation's result, which also sets the scale of the operation.

        When all the operands with a clock share the same reference clock, they get operated at its scale, keeping
        it as the result's reference clock. Otherwise, all of them get operated at the master clock's scale, which
        is also the scale of any operands with no clock, so their length must match the master clock's.

        Parameters
        ----------
        *operands
            The operands of the operation, where the ones that aren't a DataColumn get ignored

        Returns
        -------
        The reference clock of the operation's result, or None if none of the operands have a clock

        Raises
        ------
        DataColumnSyncError
            If the operands are synced to different master clocks, or if an operand with no clock doesn't match the
            master clock's length
        """
        clocked_columns = [
            operand
            for operand in operands
            if (
                isinstance(operand, DataColumn)
                and operand.reference_clock_column is not None
            )
        ]

        if not clocked_columns:
            return None

        master_clock_column = clocked_columns[0].master_clock_column
        has_single_master_clock = all(
            column.master_clock_column is master_clock_column
            for column in clocked_columns
        )

        if not has_single_master_clock:
            msg = "DataColumn operands must be synced to the same master clock"

            raise DataColumnSyncError(msg)

        clockless_columns = [
            operand
            for operand in operands
            if (
                isinstance(operand, DataColumn)
                and operand.reference_clock_column is None
            )
        ]
        reference_clock_column = clocked_columns[0].reference_clock_column
        has_single_reference_clock = all(
            column.reference_clock_column is reference_clock_column
            for column in clocked_columns
        )

        if (
            has_single_reference_clock
            and not clockless_columns
        ):
            return reference_clock_column

        # The operands with no clock can only be operated at the master clock's scale if their lengths match it
        master_row_count = len(clocked_columns[0])
        has_master_clock_length = all(
            len(column) == master_row_count
            for column in clockless_columns
        )

        if not has_master_clock_length:
            msg = " ".join([
                "DataColumn operands with no clock must match the length",
                f"{master_row_count} of the master clock of the columns they're operated with",
            ])

            raise DataColumnSyncError(msg)

        return master_clock_column

    @staticmethod
    def _calculate_span_seconds(
        *,
        month_days: int,
        period: ReferencePeriods,
        periods_per_row: int,
    ) -> int:
        """
        Calculate the number of seconds spanned by each row of a clock.

        Parameters
        ----------
        month_days
            The number of days to count for each month, as calendar periods have no fixed length
        period
            The base reference period of the clock rows
        periods_per_row
            The number of reference periods spanned by each clock row

        Returns
        -------
        The number of seconds spanned by each clock row
        """
        if period in FIXED_PERIOD_SECONDS:
            return periods_per_row * FIXED_PERIOD_SECONDS[period]

        span_months = periods_per_row * CALENDAR_PERIOD_MONTHS[period]
        month_seconds = month_days * FIXED_PERIOD_SECONDS[ReferencePeriods.DAY]

        return span_months * month_seconds

    @classmethod
    def _calculate_sync_direction(
        cls,
        *,
        master_period: ReferencePeriods,
        master_periods_per_row: int,
        reference_period: ReferencePeriods,
        reference_periods_per_row: int,
    ) -> 'DataColumn.SyncDirections':
        """
        Calculate the direction in which a reference clock's rows get synced to its master clock's rows.

        Spans of calendar periods are compared exactly by their number of months, while all other spans are compared
        by their number of seconds, with calendar periods counting anything between their shortest and longest
        possible month lengths. Equal spans get upscaled, as each master clock row matches at most one reference
        clock row.

        Parameters
        ----------
        master_period
            The base reference period of the master clock rows
        master_periods_per_row
            The number of reference periods spanned by each master clock row
        reference_period
            The base reference period of the reference clock rows
        reference_periods_per_row
            The number of reference periods spanned by each reference clock row

        Returns
        -------
        DOWNSCALE if the reference clock rows span less time than the master clock rows, otherwise UPSCALE

        Raises
        ------
        DataColumnParameterError
            If the spans can't be ordered, as which one is longer depends on the months they cover
        """
        if (
            master_period in CALENDAR_PERIOD_MONTHS
            and reference_period in CALENDAR_PERIOD_MONTHS
        ):
            master_months = master_periods_per_row * CALENDAR_PERIOD_MONTHS[master_period]
            reference_months = reference_periods_per_row * CALENDAR_PERIOD_MONTHS[reference_period]

            if reference_months < master_months:
                return DataColumn.SyncDirections.DOWNSCALE

            return DataColumn.SyncDirections.UPSCALE

        master_longest_seconds = cls._calculate_span_seconds(
            month_days=LONGEST_MONTH_DAYS,
            period=master_period,
            periods_per_row=master_periods_per_row,
        )
        master_shortest_seconds = cls._calculate_span_seconds(
            month_days=SHORTEST_MONTH_DAYS,
            period=master_period,
            periods_per_row=master_periods_per_row,
        )
        reference_longest_seconds = cls._calculate_span_seconds(
            month_days=LONGEST_MONTH_DAYS,
            period=reference_period,
            periods_per_row=reference_periods_per_row,
        )
        reference_shortest_seconds = cls._calculate_span_seconds(
            month_days=SHORTEST_MONTH_DAYS,
            period=reference_period,
            periods_per_row=reference_periods_per_row,
        )

        if reference_longest_seconds < master_shortest_seconds:
            return DataColumn.SyncDirections.DOWNSCALE

        if reference_shortest_seconds >= master_longest_seconds:
            return DataColumn.SyncDirections.UPSCALE

        msg = " ".join([
            f"DataColumn reference clock span of {reference_periods_per_row} {reference_period}",
            f"can't be ordered against its master clock span of {master_periods_per_row} {master_period},",
            "as which one is longer depends on the months they cover",
        ])

        raise DataColumnParameterError(msg)

    @staticmethod
    def _calculate_sync_indices(
        *,
        clock_downscale_method: DownscaleMethod | None,
        clock_upscale_method: UpscaleMethod | None,
        master_clock_column: 'DataColumn',
        reference_clock_column: 'DataColumn',
        sync_direction: 'DataColumn.SyncDirections',
    ) -> pyarrow.Int64Array:
        """
        Calculate the row indices matching a reference clock's rows to its master clock's rows.

        Parameters
        ----------
        clock_downscale_method
            The reference clock's method for matching its rows to the master clock rows
        clock_upscale_method
            The reference clock's method for matching the master clock rows to its rows
        master_clock_column
            The master clock column
        reference_clock_column
            The reference clock column
        sync_direction
            The direction in which the reference clock's rows get synced to the master clock's rows

        Returns
        -------
        One index per master clock row when upscaling, or one index per reference clock row when downscaling

        Raises
        ------
        DataColumnSyncError
            If the sync method doesn't return an integer pyarrow.Array with the expected number of rows
        """
        if sync_direction is DataColumn.SyncDirections.UPSCALE:
            sync_method_name = 'clock_upscale_method'
            sync_method = clock_upscale_method
            expected_row_count = len(master_clock_column.reference_array)
        else:
            sync_method_name = 'clock_downscale_method'
            sync_method = clock_downscale_method
            expected_row_count = len(reference_clock_column.reference_array)

        # The constructor validations guarantee that a reference clock has the method for its sync direction, so
        # this check only narrows the type
        if sync_method is None:
            msg = f"DataColumn reference clock can't be synced without a {sync_method_name}"

            raise DataColumnSyncError(msg)

        sync_indices = sync_method(
            master_clock_column,
            reference_clock_column,
        )

        if (
            not isinstance(sync_indices, pyarrow.Array)
            or not pyarrow.types.is_integer(sync_indices.type)
            or len(sync_indices) != expected_row_count
        ):
            msg = " ".join([
                f"DataColumn {sync_method_name} must return an integer pyarrow.Array",
                f"with {expected_row_count} rows",
            ])

            raise DataColumnSyncError(msg)

        return sync_indices

    @typing.overload
    @staticmethod
    def _get_operation_array(
        operand: 'DataColumn',
        operation_reference_clock: 'DataColumn | None',
    ) -> pyarrow.Array:
        ...

    @typing.overload
    @staticmethod
    def _get_operation_array(
        operand: OperationScalar,
        operation_reference_clock: 'DataColumn | None',
    ) -> OperationScalar:
        ...

    @staticmethod
    def _get_operation_array(
        operand: 'DataColumn | OperationScalar',
        operation_reference_clock: 'DataColumn | None',
    ) -> pyarrow.Array | OperationScalar:
        """
        Get an operand's values at the scale of an operation.

        Parameters
        ----------
        operand
            The operand of the operation, which gets returned as is if it isn't a DataColumn
        operation_reference_clock
            The reference clock of the operation's result, as calculated by `_calculate_operation_reference_clock()`

        Returns
        -------
        The operand's reference array if it's already at the operation's scale, its array synced to the master
        clock's scale otherwise, or the operand itself if it isn't a DataColumn
        """
        if not isinstance(operand, DataColumn):
            return operand

        if operand.reference_clock_column is operation_reference_clock:
            return operand.reference_array

        return operand.array

    @staticmethod
    def _mask_dual_array_nulls(
        array1: pyarrow.Array,
        array2: pyarrow.Array | OperationScalar
    ) -> pyarrow.BooleanArray | None:
        """
        Create a mask array with true on the rows where any of the 2 input arrays are null or nan.

        Parameters
        ----------
        array1
        array2

        Returns
        -------
        The mask array, or None if neither of the arrays has nulls
        """
        # @todo: fix this horrible internal API method
        if (
            array1.null_count > 0
            and (
                isinstance(array2, pyarrow.Array)
                and array2.null_count > 0
            )
        ):
            mask1 = pyarrow.compute.is_null(array1, nan_is_null=True)
            mask2 = pyarrow.compute.is_null(array2, nan_is_null=True)
            mask = pyarrow.compute.or_kleene(mask1, mask2)
        elif array1.null_count > 0:
            mask = pyarrow.compute.is_null(array1, nan_is_null=True)
        elif (
            isinstance(array2, pyarrow.Array)
            and array2.null_count > 0
        ):
            mask = pyarrow.compute.is_null(array2, nan_is_null=True)
        else:
            mask = None
        return mask

    @staticmethod
    def _mask_zeroes(array: pyarrow.Array) -> pyarrow.BooleanArray:
        """
        Create a mask array with true on the rows where the array is 0.

        Parameters
        ----------
        array : pyarrow.Array

        Returns
        -------
        pyarrow.BooleanArray
        """
        try:
            result = pyarrow.compute.equal(array, 0)
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.equal(
                    array.cast(
                        pyarrow.float64()
                    ),
                    0
                )
            else:
                raise   # pragma: no cover

        return result

    @staticmethod
    def _replace_array_mask_with_nones(
        array: pyarrow.Array,
        mask: pyarrow.BooleanArray | None,
    ) -> pyarrow.Array:
        """
        Replace the values in the array with None where the mask is True.

        Parameters
        ----------
        array
            The array to be modified.
        mask
            The mask indicating which values to replace with None.

        Returns
        -------
        The modified array with None values where the mask is True.
        """
        if mask is None:
            return array

        return pyarrow.compute.if_else(
            mask,
            pyarrow.scalar(None, type=array.type),
            array
        )

    def _return_null_column_on_null_operand(
        self,
        operand: 'DataColumn | int | float | pyarrow.Scalar'
    ) -> typing.Union['DataColumn', None]:
        """
        Return a null column if this column or the operand is a null column, or the operand is a null scalar.

        The checks use the reference arrays, so they don't require syncing the columns to the master clock.

        Parameters
        ----------
        operand

        Returns
        -------
        The null column, at this column's scale for a null scalar operand, or None if there are no null operands
        """
        if (
            self.reference_array.type == pyarrow.null()
        ):
            return self
        elif (
            isinstance(operand, DataColumn)
            and operand.reference_array.type == pyarrow.null()
        ):
            return operand
        elif (
            not isinstance(operand, DataColumn)
            and (
                operand is None
                or getattr(operand, 'type', None) == pyarrow.null()
            )
        ):
            return DataColumn(
                pyarrow.nulls(
                    len(self.reference_array)
                ),
                reference_clock_column=self.reference_clock_column,
            )
        else:
            return None

    @staticmethod
    def _validate_clock_array(
        *,
        array: pyarrow.Array,
        clock_master: 'DataColumn | typing.Literal[True] | None',
    ) -> None:
        """
        Validate the timestamps array of a clock column.

        All clocks require a date or timestamp array, although reference clocks also accept a null array, for when
        they have no timestamps at all. The master clock's timestamps can't have nulls and must be strictly
        increasing, as all the other columns get synced to its rows in order.

        Parameters
        ----------
        array
            The array at the reference scale, as received by the constructor
        clock_master
            The clock's master clock configuration, as received by the constructor

        Raises
        ------
        DataColumnParameterError
        """
        if clock_master is None:
            return

        is_temporal = (
            pyarrow.types.is_date(array.type)
            or pyarrow.types.is_timestamp(array.type)
        )

        if (
            clock_master is True
            and not is_temporal
        ):
            msg = "DataColumn master clock requires a date or timestamp array"

            raise DataColumnParameterError(msg)

        if (
            clock_master is not True
            and not is_temporal
            and not pyarrow.types.is_null(array.type)
        ):
            msg = "DataColumn reference clock requires a date, timestamp, or null array"

            raise DataColumnParameterError(msg)

        if (
            clock_master is True
            and array.null_count > 0
        ):
            msg = "DataColumn master clock can't have null timestamps"

            raise DataColumnParameterError(msg)

        # Comparing each timestamp with the previous one requires at least 2 of them
        if (
            clock_master is True
            and len(array) > 1
        ):
            is_strictly_increasing = pyarrow.compute.all(
                pyarrow.compute.greater(
                    array.slice(1),
                    array.slice(0, len(array) - 1),
                )
            ).as_py()

            if not is_strictly_increasing:
                msg = "DataColumn master clock timestamps must be strictly increasing"

                raise DataColumnParameterError(msg)

    @staticmethod
    def _validate_clock_parameters(
        *,
        array: pyarrow.Array,
        clock_downscale_method: DownscaleMethod | None,
        clock_master: 'DataColumn | typing.Literal[True] | None',
        clock_periods_per_row: int | None,
        clock_reference_period: ReferencePeriods | None,
        clock_upscale_method: UpscaleMethod | None,
        consolidation_method: ConsolidationMethod | None,
        reference_clock_column: 'DataColumn | None',
    ) -> None:
        """
        Validate the consistency of the clock parameters of a DataColumn.

        Parameters
        ----------
        array
            The array at the reference scale, as received by the constructor
        clock_downscale_method
            The clock's method for matching its rows to the master clock rows, as received by the constructor
        clock_master
            The clock's master clock configuration, as received by the constructor
        clock_periods_per_row
            The number of reference periods spanned by each clock row, as received by the constructor
        clock_reference_period
            The base reference period of the clock rows, as received by the constructor
        clock_upscale_method
            The clock's method for matching the master clock rows to its rows, as received by the constructor
        consolidation_method
            The method for consolidating the reference array rows belonging to each master clock row, as received by
            the constructor
        reference_clock_column
            The reference clock column of a non-clock column, as received by the constructor

        Raises
        ------
        DataColumnParameterError
        """
        # The clock parameters must always be checked by identity, never by truthiness or equality, as a
        # DataColumn's truthiness depends on its length, and its equality operator works element-wise.
        if (
            clock_master is not None
            and clock_master is not True
            and not isinstance(clock_master, DataColumn)
        ):
            msg = "DataColumn clock_master must be either True, a DataColumn, or None"

            raise DataColumnParameterError(msg)

        if (
            reference_clock_column is not None
            and not isinstance(reference_clock_column, DataColumn)
        ):
            msg = "DataColumn reference_clock_column must be either a DataColumn or None"

            raise DataColumnParameterError(msg)

        if (
            clock_downscale_method is not None
            and not callable(clock_downscale_method)
        ):
            msg = "DataColumn clock_downscale_method must be either a callable or None"

            raise DataColumnParameterError(msg)

        if (
            clock_upscale_method is not None
            and not callable(clock_upscale_method)
        ):
            msg = "DataColumn clock_upscale_method must be either a callable or None"

            raise DataColumnParameterError(msg)

        if (
            consolidation_method is not None
            and not callable(consolidation_method)
        ):
            msg = "DataColumn consolidation_method must be either a callable or None"

            raise DataColumnParameterError(msg)

        if (
            clock_master is not None
            and reference_clock_column is not None
        ):
            msg = "DataColumn acting as a clock can't set a reference_clock_column, as it's its own reference clock"

            raise DataColumnParameterError(msg)

        # Only the master clock resolves to itself as its own master clock column
        if (
            isinstance(clock_master, DataColumn)
            and clock_master.master_clock_column is not clock_master
        ):
            msg = "DataColumn clock_master must point to the column acting as the master clock"

            raise DataColumnParameterError(msg)

        # Only clock columns resolve to themselves as their own reference clock column
        if (
            isinstance(reference_clock_column, DataColumn)
            and reference_clock_column.reference_clock_column is not reference_clock_column
        ):
            msg = "DataColumn reference_clock_column must point to a column acting as a clock"

            raise DataColumnParameterError(msg)

        if (
            isinstance(reference_clock_column, DataColumn)
            and len(array) != len(reference_clock_column.reference_array)
        ):
            msg = " ".join([
                f"DataColumn array length {len(array)} doesn't match the length",
                f"{len(reference_clock_column.reference_array)} of its reference clock's reference array",
            ])

            raise DataColumnParameterError(msg)

        if (
            clock_master is not None
            and (
                clock_reference_period is None
                or clock_periods_per_row is None
            )
        ):
            msg = "DataColumn acting as a clock requires both clock_reference_period and clock_periods_per_row"

            raise DataColumnParameterError(msg)

        if (
            clock_master is None
            and (
                clock_reference_period is not None
                or clock_periods_per_row is not None
            )
        ):
            msg = "Only a DataColumn acting as a clock can set clock_reference_period and clock_periods_per_row"

            raise DataColumnParameterError(msg)

        if (
            clock_master is None
            and (
                clock_downscale_method is not None
                or clock_upscale_method is not None
            )
        ):
            msg = "Only a DataColumn acting as a clock can set clock_downscale_method and clock_upscale_method"

            raise DataColumnParameterError(msg)

        if (
            clock_reference_period is not None
            and not isinstance(clock_reference_period, ReferencePeriods)
        ):
            msg = "DataColumn clock_reference_period must be a ReferencePeriods member"

            raise DataColumnParameterError(msg)

        # bool is a subclass of int, so it needs to be explicitly excluded
        if (
            clock_periods_per_row is not None
            and (
                isinstance(clock_periods_per_row, bool)
                or not isinstance(clock_periods_per_row, int)
                or clock_periods_per_row < 1
            )
        ):
            msg = "DataColumn clock_periods_per_row must be a positive integer"

            raise DataColumnParameterError(msg)

    @staticmethod
    def _validate_sync_methods(
        *,
        clock_downscale_method: DownscaleMethod | None,
        clock_upscale_method: UpscaleMethod | None,
        consolidation_method: ConsolidationMethod | None,
        sync_direction: 'DataColumn.SyncDirections | None',
    ) -> None:
        """
        Validate that a reference clock has the methods required for syncing its rows to its master clock's rows.

        Parameters
        ----------
        clock_downscale_method
            The clock's method for matching its rows to the master clock rows, as received by the constructor
        clock_upscale_method
            The clock's method for matching the master clock rows to its rows, as received by the constructor
        consolidation_method
            The method for consolidating the reference array rows belonging to each master clock row, as received by
            the constructor
        sync_direction
            The direction in which the clock's rows get synced to its master clock's rows, or None if the column isn't
            a reference clock

        Raises
        ------
        DataColumnParameterError
        """
        if (
            sync_direction is DataColumn.SyncDirections.UPSCALE
            and clock_upscale_method is None
        ):
            msg = " ".join([
                "DataColumn reference clock requires a clock_upscale_method,",
                "as its rows span at least as long as its master clock's rows",
            ])

            raise DataColumnParameterError(msg)

        if (
            sync_direction is DataColumn.SyncDirections.DOWNSCALE
            and clock_downscale_method is None
        ):
            msg = " ".join([
                "DataColumn reference clock requires a clock_downscale_method,",
                "as its rows span less time than its master clock's rows",
            ])

            raise DataColumnParameterError(msg)

        # The reference clock's own consolidation method also serves as the default for the columns pointing to it
        if (
            sync_direction is DataColumn.SyncDirections.DOWNSCALE
            and consolidation_method is None
        ):
            msg = " ".join([
                "DataColumn reference clock requires a consolidation_method,",
                "as its rows span less time than its master clock's rows",
            ])

            raise DataColumnParameterError(msg)

