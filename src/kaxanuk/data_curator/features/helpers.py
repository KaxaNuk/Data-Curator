"""
Helper functions for calculating the features that combine many rows, like moving averages or rolling sums.

The helpers compute on the rows of their columns' own clock, like the daily rows of market data even on an intraday
master clock, and return a column on that same clock, which the clock chain then syncs to the master clock. So a
21-day window always spans 21 rows of daily data, while the columns without a clock get computed as they are.
"""

import datetime
import typing
import warnings

import pandas
import pyarrow
import pyarrow.compute

from kaxanuk.data_curator.entities import ClockSpec
from kaxanuk.data_curator.exceptions import CalculationHelperError
from kaxanuk.data_curator.modules.data_column import (
    CALENDAR_PERIOD_MONTHS,
    FIXED_PERIOD_SECONDS,
    DataColumn,
)


# the most digits a PyArrow decimal can hold, which a decimal sum can exceed without PyArrow detecting it:
DECIMAL_MAX_PRECISION = 38
MARKET_DAYS_PER_YEAR = 252


def annualized_volatility(
    *,
    column: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the annualized volatility of a column, with a standard deviation rolling over a number of its rows.

    The rows are those of the column's own clock, like the daily rows of market data, and the standard deviation gets
    annualized by the square root of MARKET_DAYS_PER_YEAR.

    Parameters
    ----------
    column
        The column containing the data for which the annualized volatility needs to be calculated.
    days
        The number of rows to use for the rolling standard deviation calculation.

    Returns
    -------
    A new DataColumn object containing the annualized volatility values, on the column's clock.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn, or days is not a positive integer.
    """
    _validate_column(
        column,
        function_name='annualized_volatility',
        parameter_name='column',
    )
    _validate_row_count(
        days,
        function_name='annualized_volatility',
        parameter_name='days',
    )

    value_series = pandas.Series(
        get_float_values(
            column,
            operation_clock_column=column.reference_clock_column,
        ).to_numpy(zero_copy_only=False)
    )
    rolling_standard_deviations = (
        value_series
        .rolling(days)
        .std()
    )
    volatilities = (
        rolling_standard_deviations
        * (MARKET_DAYS_PER_YEAR ** 0.5)
    )

    return DataColumn.load(
        pyarrow.array(
            volatilities,
            from_pandas=True,
        ),
        reference_clock_column=column.reference_clock_column,
    )


def chaikin_money_flow(
    *,
    high: DataColumn,
    low: DataColumn,
    close: DataColumn,
    volume: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the Chaikin Money Flow (CMF) indicator over a number of rows.

    The CMF divides the money flow volume summed over the window by the volume summed over it, with each row's money
    flow volume being its volume times its money flow multiplier: ((close - low) - (high - close)) / (high - low). A
    window gets null when it isn't complete, when any of its rows has a null value or no price range, or when its
    volume sums to zero. The columns get computed on their shared clock's rows, or on the master clock's if they
    don't share one.

    Parameters
    ----------
    high
        High prices.
    low
        Low prices.
    close
        Close prices.
    volume
        Trading volume.
    days
        Number of rows for the rolling calculation.

    Returns
    -------
    The CMF values as a DataColumn, on the columns' shared clock.

    Raises
    ------
    CalculationHelperError
        If high, low, close or volume is not a DataColumn or they're synced to different master clocks, or days is not
        a positive integer.
    """
    price_columns = {
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    }
    for (parameter_name, price_column) in price_columns.items():
        _validate_column(
            price_column,
            function_name='chaikin_money_flow',
            parameter_name=parameter_name,
        )

    _validate_row_count(
        days,
        function_name='chaikin_money_flow',
        parameter_name='days',
    )

    operation_clock_column = find_operation_clock(
        list(price_columns.values())
    )
    high_values = get_float_values(
        high,
        operation_clock_column=operation_clock_column,
    )
    low_values = get_float_values(
        low,
        operation_clock_column=operation_clock_column,
    )
    close_values = get_float_values(
        close,
        operation_clock_column=operation_clock_column,
    )
    volume_values = get_float_values(
        volume,
        operation_clock_column=operation_clock_column,
    )

    # A row without any price range divides by zero, so every window holding it sums to NaN, and ends up null
    money_flow_multipliers = pyarrow.compute.divide(
        pyarrow.compute.subtract(
            pyarrow.compute.subtract(
                close_values,
                low_values,
            ),
            pyarrow.compute.subtract(
                high_values,
                close_values,
            ),
        ),
        pyarrow.compute.subtract(
            high_values,
            low_values,
        ),
    )
    money_flow_volumes = pyarrow.compute.multiply(
        money_flow_multipliers,
        volume_values,
    )

    # PyArrow can only sum the windows as differences between cumulative sums, which lose the digits of each window's
    # sum smaller than the precision of the growing cumulative sums, while pandas compensates its rolling sums' errors
    money_flow_volume_sums = (
        pandas.Series(
            money_flow_volumes.to_numpy(zero_copy_only=False),
            dtype='float64',
        )
        .rolling(days)
        .sum()
    )
    volume_sums = (
        pandas.Series(
            volume_values.to_numpy(zero_copy_only=False),
            dtype='float64',
        )
        .rolling(days)
        .sum()
    )

    # A window whose volume sums to zero divides by zero, so it's infinite or NaN, and ends up null
    return replace_infinite_with_none(
        DataColumn.load(
            pyarrow.array(
                money_flow_volume_sums / volume_sums,
                from_pandas=True,
            ),
            reference_clock_column=operation_clock_column,
        )
    )


def exponential_moving_average(
    *,
    column: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the Exponential Moving Average (EMA) over a number of rows, with a smoothing factor of 2 / (days + 1).

    Each run of rows without nulls starts its EMA from the simple moving average of its first rows, so the EMA restarts
    after every null, staying null until the run has enough rows.

    Parameters
    ----------
    column
        Data for which the EMA is calculated.
    days
        The span for the EMA. It specifies that the "center of mass" of the EMA's weights is roughly at the same point
        as an SMA of the same length in rows.

    Returns
    -------
    EMA values as a DataColumn, on the column's clock.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn, or days is not a positive integer.
    """
    _validate_column(
        column,
        function_name='exponential_moving_average',
        parameter_name='column',
    )
    _validate_row_count(
        days,
        function_name='exponential_moving_average',
        parameter_name='days',
    )

    return replace_infinite_with_none(
        exponential_smoothing(
            column=column,
            days=days,
            smoothing_factor=2 / (days + 1),
        )
    )


def exponential_smoothing(
    *,
    column: DataColumn,
    days: int,
    smoothing_factor: float,
) -> DataColumn:
    """
    Smooth a column exponentially, starting each run of rows without nulls from the simple average of its first rows.

    Each run's smoothed value starts at its row number `days`, as the simple average of its first `days` rows, and
    then each following row updates it to smoothing_factor * value + (1 - smoothing_factor) * previous smoothed value,
    so the smoothing restarts after every null, staying null until the run has enough rows. The exponential moving
    average uses a smoothing factor of 2 / (days + 1), while Wilder's smoothing, like in the RSI, uses 1 / days.

    Parameters
    ----------
    column
        Data to smooth.
    days
        The number of rows whose simple average starts each run's smoothing.
    smoothing_factor
        The weight of each row's value in its smoothed value, greater than 0 and at most 1.

    Returns
    -------
    The smoothed values as a DataColumn, on the column's clock, with null for the rows of each run before its first
    smoothed value, and for the null rows.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn, days is not a positive integer, or smoothing_factor is not a number greater than
        0 and at most 1.
    """
    _validate_column(
        column,
        function_name='exponential_smoothing',
        parameter_name='column',
    )
    _validate_row_count(
        days,
        function_name='exponential_smoothing',
        parameter_name='days',
    )
    # bool is an int subclass, but not a valid smoothing factor
    if (
        not isinstance(smoothing_factor, (int, float))
        or isinstance(smoothing_factor, bool)
        or not 0 < smoothing_factor <= 1
    ):
        msg = " ".join([
            "features.helpers.exponential_smoothing() smoothing_factor parameter must be a number",
            "greater than 0 and at most 1",
        ])

        raise CalculationHelperError(msg)

    values = get_float_values(
        column,
        operation_clock_column=column.reference_clock_column,
    )
    value_series = pandas.Series(
        values.to_numpy(zero_copy_only=False),
        dtype='float64',
    )
    smoothed_series = pandas.Series(
        index=value_series.index,
        dtype='float64',
    )
    for value_run in _find_runs_without_nulls(values):
        if len(value_run) < days:
            continue

        seed_position = value_run.start + days - 1
        seeded_values = value_series.iloc[seed_position:value_run.stop].copy()
        seeded_values.iloc[0] = value_series.iloc[value_run.start:seed_position + 1].mean()
        run_smoothed_values = (
            seeded_values
            .ewm(
                alpha=smoothing_factor,
                adjust=False,
            )
            .mean()
        )
        smoothed_series.iloc[seed_position:value_run.stop] = run_smoothed_values.to_numpy()

    return DataColumn.load(
        pyarrow.array(
            smoothed_series,
            from_pandas=True,
        ),
        reference_clock_column=column.reference_clock_column,
    )


def find_operation_clock(columns: list[DataColumn]) -> DataColumn | None:
    """
    Return the clock whose rows to compute several columns on together, like with your own pandas or pyarrow code.

    Columns sharing a reference clock get computed on its own rows, like the daily rows of market data even on an
    intraday master clock, while columns with different clocks get computed on their master clock's rows. Get each
    column's values on the clock's rows with get_float_values, and wrap the results with
    DataColumn.load(results, reference_clock_column=operation_clock_column), so the clock chain syncs them to the
    master clock.

    Parameters
    ----------
    columns
        The columns to compute on together.

    Returns
    -------
    The reference clock shared by all the columns, or else their master clock, or None if none of them has a clock.

    Raises
    ------
    CalculationHelperError
        If columns is not a non-empty list of DataColumn objects, or they're synced to different master clocks.
    """
    if (
        not isinstance(columns, list)
        or len(columns) < 1
    ):
        msg = "features.helpers.find_operation_clock() columns parameter must be a non-empty list of DataColumn objects"

        raise CalculationHelperError(msg)

    for column in columns:
        _validate_column(
            column,
            function_name='find_operation_clock',
            parameter_name='columns',
        )

    first_reference_clock_column = columns[0].reference_clock_column
    has_shared_reference_clock = all(
        column.reference_clock_column is first_reference_clock_column
        for column in columns
    )

    if has_shared_reference_clock:
        return first_reference_clock_column

    master_clock_columns = [
        column.master_clock_column
        for column in columns
        if column.master_clock_column is not None
    ]
    first_master_clock_column = master_clock_columns[0]
    has_shared_master_clock = all(
        master_clock_column is first_master_clock_column
        for master_clock_column in master_clock_columns
    )

    if not has_shared_master_clock:
        msg = "features.helpers.find_operation_clock() columns must be synced to the same master clock"

        raise CalculationHelperError(msg)

    return first_master_clock_column


def get_float_values(
    column: DataColumn,
    *,
    operation_clock_column: DataColumn | None,
) -> pyarrow.Array:
    """
    Return a column's values on the rows of the clock to compute on, as floats.

    Parameters
    ----------
    column
        The column whose values to return.
    operation_clock_column
        The clock to compute on, like the one find_operation_clock returns, which has to be either the column's own
        reference clock, or its master clock.

    Returns
    -------
    The column's values on the clock's rows, as a float64 pyarrow.Array.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn, or operation_clock_column is neither its reference clock nor its master clock.
    """
    _validate_column(
        column,
        function_name='get_float_values',
        parameter_name='column',
    )

    # The columns without a clock get computed as they are, on the rows of the other columns' clock
    is_valid_operation_clock = (
        operation_clock_column is column.reference_clock_column
        or operation_clock_column is column.master_clock_column
        or column.master_clock_column is None
    )

    if not is_valid_operation_clock:
        msg = " ".join([
            "features.helpers.get_float_values() operation_clock_column parameter must be the column's reference clock",
            "or master clock",
        ])

        raise CalculationHelperError(msg)

    values = (
        column.reference_array
        if column.reference_clock_column is operation_clock_column
        else column.array
    )

    return pyarrow.compute.cast(
        values,
        pyarrow.float64(),
    )


def indexed_rolling_window_operation(
    *,
    key_column: DataColumn,
    value_column: DataColumn,
    operation_function: typing.Callable,
    window_length: int
) -> DataColumn:
    """
    Apply a rolling window operation on data corresponding to the unique keys, repeating the values on duplicates.

    Deprecated, as rolling_sum sums the rows of each period across their own clock, without needing any keys.

    Useful for rolling windows on period data across periods, with each period having the same key and thus
    the same data.

    Each key must occupy a single contiguous block of rows: once the rows of a key end, that same key must
    not appear again further down the column.

    Parameters
    ----------
    key_column
        Keys on which we base the rolling window.
    value_column
        Values to be used for the rolling window.
    operation_function
        Function to apply on each rolling window.
    window_length
        Length of each rolling window.

    Returns
    -------
    Column with the resulting values for each key in the same order as key_column.

    Raises
    ------
    CalculationHelperError
        If key_column or value_column is not a DataColumn, operation_function is not callable, window_length is not
        a positive integer, or any key reappears in a separate later block of rows.
    """
    warnings.warn(
        " ".join([
            "features.helpers.indexed_rolling_window_operation() is deprecated",
            "and will be removed in a future version, use features.helpers.rolling_sum() instead",
        ]),
        DeprecationWarning,
        stacklevel=2,
    )

    if not isinstance(key_column, DataColumn):
        msg = "features.helpers.indexed_rolling_window_operation() key_column parameter must be a DataColumn object"

        raise CalculationHelperError(msg)

    if not isinstance(value_column, DataColumn):
        msg = "features.helpers.indexed_rolling_window_operation() value_column parameter must be a DataColumn object"

        raise CalculationHelperError(msg)

    if not callable(operation_function):
        msg = "features.helpers.indexed_rolling_window_operation() operation_function parameter must be a callable"

        raise CalculationHelperError(msg)

    if (
        not isinstance(window_length, int)
        or window_length <= 0
    ):
        msg = "features.helpers.indexed_rolling_window_operation() window_length parameter must be a positive integer"

        raise CalculationHelperError(msg)

    shifted_key_arrays = [
        pyarrow.array(
            [None],
            type=key_column.type
        ),
        key_column[:-1].to_pyarrow(),
    ]
    adjacent_equal_keys = DataColumn.equal(
        DataColumn.load(
            pyarrow.concat_arrays(shifted_key_arrays)
        ),
        key_column,
        equal_nulls=True
    )
    adjacent_unique_keys = pyarrow.compute.invert(
        adjacent_equal_keys.to_pyarrow()
    )
    unique_positions = pyarrow.compute.indices_nonzero(adjacent_unique_keys)

    # Pass the adjacently unique rows to an indexed pandas.Series
    key_series = (
        pyarrow.compute.array_take(
            key_column.to_pyarrow(),
            unique_positions
        )
        .to_pandas()
    )
    value_series = (
        pyarrow.compute.array_take(
            value_column.to_pyarrow(),
            unique_positions
        )
        .to_pandas()
    )
    keys_values = pandas.Series(
        data=value_series.values,
        index=key_series
    )
    rolling_applied = (
        keys_values
            .rolling(window_length)
            .apply(operation_function, raw=True)
    )
    try:
        result = key_column.to_pandas().map(
            rolling_applied.drop(
                ['', None, float('nan')],
                errors='ignore'
            ),
            na_action='ignore'
        )
    except pandas.errors.InvalidIndexError as error:
        repeated_keys = (
            key_series[key_series.duplicated()]
                .unique()
                .tolist()
        )
        msg = ''.join([
            "features.helpers.indexed_rolling_window_operation() requires each key_column key to occupy a single",
            " contiguous block of rows, but the following keys reappear in a separate later block: ",
            ', '.join(
                str(repeated_key)
                for repeated_key in repeated_keys
            ),
        ])

        raise CalculationHelperError(msg) from error

    return DataColumn.load(result)


def log_returns(column: DataColumn) -> DataColumn:
    """
    Calculate the logarithmic returns between each row of a column and its previous row.

    Parameters
    ----------
    column
        Price series for which to compute log returns.

    Returns
    -------
    Log returns series on the column's clock, with null for the first row, as well as for the rows following a null
    or a zero.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn.
    """
    _validate_column(
        column,
        function_name='log_returns',
        parameter_name='column',
    )

    values = get_float_values(
        column,
        operation_clock_column=column.reference_clock_column,
    )
    previous_values = pyarrow.concat_arrays([
        pyarrow.nulls(
            1,
            type=values.type,
        ),
        values,
    ]).slice(0, len(values))
    returns = pyarrow.compute.divide(
        values,
        _replace_array_zeros_with_null(previous_values),
    )

    return DataColumn.load(
        pyarrow.compute.ln(returns),
        reference_clock_column=column.reference_clock_column,
    )


def relative_strength_index(
    *,
    column: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the Relative Strength Index (RSI) over a number of rows, with Wilder's smoothing.

    Each row's change from its previous row counts as a gain when positive, or as a loss when negative, and each run of
    changes without nulls starts its average gain and loss from the simple averages of its first changes, which then
    get smoothed with a factor of 1 / days. The RSI is then 100 - 100 / (1 + average gain / average loss), or null when
    the average loss is zero, and it restarts after every null.

    Parameters
    ----------
    column
        Price data for RSI calculation.
    days
        Number of rows for the RSI calculation.

    Returns
    -------
    The RSI values as a DataColumn, on the column's clock.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn, or days is not a positive integer.
    """
    _validate_column(
        column,
        function_name='relative_strength_index',
        parameter_name='column',
    )
    _validate_row_count(
        days,
        function_name='relative_strength_index',
        parameter_name='days',
    )

    # The first row has no change, as it has no previous row
    changes = pyarrow.compute.pairwise_diff(
        get_float_values(
            column,
            operation_clock_column=column.reference_clock_column,
        )
    )
    zero = pyarrow.scalar(0.0)
    gains = pyarrow.compute.max_element_wise(
        changes,
        zero,
        skip_nulls=False,
    )
    losses = pyarrow.compute.max_element_wise(
        pyarrow.compute.negate(changes),
        zero,
        skip_nulls=False,
    )
    average_gains = exponential_smoothing(
        column=DataColumn.load(
            gains,
            reference_clock_column=column.reference_clock_column,
        ),
        days=days,
        smoothing_factor=1 / days,
    ).reference_array
    average_losses = exponential_smoothing(
        column=DataColumn.load(
            losses,
            reference_clock_column=column.reference_clock_column,
        ),
        days=days,
        smoothing_factor=1 / days,
    ).reference_array

    # Without any losses the RSI is null, as dividing by them would give 100 instead
    relative_strengths = pyarrow.compute.divide(
        average_gains,
        _replace_array_zeros_with_null(average_losses),
    )
    relative_strength_indexes = pyarrow.compute.subtract(
        100.0,
        pyarrow.compute.divide(
            100.0,
            pyarrow.compute.add(
                1.0,
                relative_strengths,
            ),
        ),
    )

    return replace_infinite_with_none(
        DataColumn.load(
            relative_strength_indexes,
            reference_clock_column=column.reference_clock_column,
        )
    )


def replace_infinite_with_none(column: DataColumn) -> DataColumn:
    """
    Replace -inf and inf values in a DataColumn with None.

    Parameters
    ----------
    column
        The column containing the calculated results.

    Returns
    -------
    The same column if it has no infinite values, or else a new DataColumn object with them replaced by None, on the
    column's clock.
    """
    values = column.reference_array
    is_finite = pyarrow.compute.is_finite(values)

    if pyarrow.compute.all(is_finite).as_py():
        return column

    return DataColumn.load(
        pyarrow.compute.if_else(
            is_finite,
            values,
            pyarrow.scalar(
                None,
                type=values.type,
            ),
        ),
        reference_clock_column=column.reference_clock_column,
    )


def rolling_sum(
    column: DataColumn,
    window: ClockSpec,
) -> DataColumn:
    """
    Sum a column's rows over a trailing window of time, like the last twelve months.

    When the rows of the column's clock span periods that fit the window a whole number of times, like the quarterly,
    half-yearly or annual rows of financial statements in a window of 12 months, each row sums with the rows before
    it that complete the window, on the column's own clock. Otherwise, like for the dates of dividends or the daily rows
    of market data in a window of 12 months, each master clock row sums the column's rows timestamped after one window
    before it, up to and including its own timestamp, on the master clock, so the rows stop counting once they get
    older than the window, and the master clock rows with no rows in their window sum to zero, like all of them when
    the column has no rows at all, like a company that never paid dividends.

    The sums are exact, keeping the type of the column's values, like the scale of its decimals. The windows with any
    null value sum to null, as do the incomplete windows at the start, all the windows of the rows that span longer
    than the window itself, and all the windows of a column whose rows have no data, like those of a data block that
    got no entity.

    Parameters
    ----------
    column
        The column whose rows get summed, which needs a clock
    window
        The time spanned by each window, like ClockSpec(reference_period=ReferencePeriods.MONTH, periods_per_row=12)

    Returns
    -------
    The sum of each window, on the column's clock when its rows fit the window, or else on the master clock

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn with a clock, window is not a ClockSpec spanning some time, or a decimal sum
        exceeds the digits that PyArrow decimals can hold
    """
    _validate_column(
        column,
        function_name='rolling_sum',
        parameter_name='column',
    )

    if (
        not isinstance(window, ClockSpec)
        or window.periods_per_row < 1
    ):
        msg = "features.helpers.rolling_sum() window parameter must be a ClockSpec with at least 1 period per row"

        raise CalculationHelperError(msg)

    reference_clock_column = column.reference_clock_column
    master_clock_column = column.master_clock_column

    if (
        reference_clock_column is None
        or master_clock_column is None
    ):
        msg = "features.helpers.rolling_sum() column parameter needs a clock, to know the time spanned by its rows"

        raise CalculationHelperError(msg)

    values = column.reference_array
    window_row_count = _calculate_window_row_count(
        clock_column=reference_clock_column,
        window=window,
    )

    if window_row_count is not None:
        row_window_sums = _sum_row_windows(
            values,
            row_count=window_row_count,
        )
        _validate_decimal_sums(row_window_sums)

        return DataColumn.load(
            row_window_sums,
            reference_clock_column=reference_clock_column,
        )

    time_window_sums = _sum_time_windows(
        master_clock_column=master_clock_column,
        reference_clock_column=reference_clock_column,
        values=values,
        window=window,
    )
    _validate_decimal_sums(time_window_sums)

    return DataColumn.load(
        time_window_sums,
        reference_clock_column=master_clock_column,
    )


def simple_moving_average(
    column: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the simple moving average over a number of rows.

    Parameters
    ----------
    column
        Price series for which to compute the moving average.
    days
        Number of rows to include in the average.

    Returns
    -------
    Simple moving average series on the column's clock, with null for the incomplete windows at the start, and for
    the windows with any null value.

    Raises
    ------
    CalculationHelperError
        If column is not a DataColumn, or days is not a positive integer.
    """
    _validate_column(
        column,
        function_name='simple_moving_average',
        parameter_name='column',
    )
    _validate_row_count(
        days,
        function_name='simple_moving_average',
        parameter_name='days',
    )

    values = get_float_values(
        column,
        operation_clock_column=column.reference_clock_column,
    )

    # PyArrow can only sum the windows as differences between cumulative sums, which lose the digits of each window's
    # sum smaller than the precision of the growing cumulative sums, while pandas compensates its rolling sums' errors
    window_sums = (
        pandas.Series(
            values.to_numpy(zero_copy_only=False),
            dtype='float64',
        )
        .rolling(days)
        .sum()
    )

    return DataColumn.load(
        pyarrow.array(
            window_sums / days,
            from_pandas=True,
        ),
        reference_clock_column=column.reference_clock_column,
    )


def _calculate_window_row_count(
    *,
    clock_column: DataColumn,
    window: ClockSpec,
) -> int | None:
    """
    Return the number of a clock's rows spanning a window, when the rows' periods fit it a whole number of times.

    The calendar periods get compared by their number of months, and the fixed periods by their number of seconds, so
    the days, which have no fixed number per month, never fit a window of months.

    Parameters
    ----------
    clock_column
        The clock whose rows would get counted
    window
        The time spanned by the window

    Returns
    -------
    The number of rows spanning the window, 0 when each row spans longer than the window, or None when the rows are
    instants, or their periods don't fit the window a whole number of times
    """
    periods_per_row = clock_column.periods_per_row
    reference_period = clock_column.reference_period

    if (
        periods_per_row is None
        or reference_period is None
        or periods_per_row == 0
    ):
        return None

    if (
        reference_period in CALENDAR_PERIOD_MONTHS
        and window.reference_period in CALENDAR_PERIOD_MONTHS
    ):
        row_length = periods_per_row * CALENDAR_PERIOD_MONTHS[reference_period]
        window_length = window.periods_per_row * CALENDAR_PERIOD_MONTHS[window.reference_period]
    elif (
        reference_period in FIXED_PERIOD_SECONDS
        and window.reference_period in FIXED_PERIOD_SECONDS
    ):
        row_length = periods_per_row * FIXED_PERIOD_SECONDS[reference_period]
        window_length = window.periods_per_row * FIXED_PERIOD_SECONDS[window.reference_period]
    else:
        return None

    if row_length > window_length:
        return 0

    if window_length % row_length != 0:
        return None

    return window_length // row_length


def _count_boundaries_before(
    *,
    boundaries: pyarrow.Array,
    timestamps: pyarrow.Array,
) -> pyarrow.Int64Array:
    """
    Count the boundaries before each timestamp, which is also the position of the first boundary at or after it.

    Parameters
    ----------
    boundaries
        The timestamps to count, sorted from earliest to latest, like a master clock's timestamps
    timestamps
        The timestamps to count the boundaries before, with the same type as the boundaries, in any order

    Returns
    -------
    The number of boundaries before each timestamp, or null for the null timestamps
    """
    timestamp_count = len(timestamps)

    # Placing the timestamps before the boundaries keeps them first among equal values through the stable sort, so a
    # boundary equal to a timestamp doesn't count as before it, while the null timestamps get sorted at the end
    combined_values = pyarrow.concat_arrays([
        timestamps,
        boundaries,
    ])
    combined_boundary_counts = pyarrow.array(
        [
            *([0] * timestamp_count),
            *([1] * len(boundaries)),
        ],
        type=pyarrow.int64(),
    )
    sorted_positions = pyarrow.compute.sort_indices(combined_values)
    sorted_boundary_counts = pyarrow.compute.cumulative_sum(
        pyarrow.compute.take(
            combined_boundary_counts,
            sorted_positions,
        )
    )

    # Sorting the sorted positions inverts the sort, getting back the timestamps in their own order
    unsorted_positions = pyarrow.compute.sort_indices(sorted_positions)
    timestamp_boundary_counts = pyarrow.compute.take(
        sorted_boundary_counts,
        unsorted_positions.slice(0, timestamp_count),
    )

    return pyarrow.compute.if_else(
        pyarrow.compute.is_null(timestamps),
        pyarrow.scalar(
            None,
            type=pyarrow.int64(),
        ),
        timestamp_boundary_counts,
    )


def _find_runs_without_nulls(values: pyarrow.Array) -> list[range]:
    """
    Return the positions of each run of consecutive rows without nulls or NaNs.

    Parameters
    ----------
    values
        The values whose runs to find

    Returns
    -------
    The range of positions of each run, from first to last
    """
    null_positions = pyarrow.compute.indices_nonzero(
        pyarrow.compute.is_null(
            values,
            nan_is_null=True,
        )
    ).to_pylist()
    run_starts = [
        0,
        *(
            null_position + 1
            for null_position in null_positions
        ),
    ]
    run_stops = [
        *null_positions,
        len(values),
    ]

    return [
        range(run_start, run_stop)
        for (run_start, run_stop) in zip(
            run_starts,
            run_stops,
            strict=True
        )
        if run_stop > run_start
    ]


def _replace_array_zeros_with_null(values: pyarrow.Array) -> pyarrow.Array:
    """
    Replace an array's zero values with nulls, like for the divisors of a PyArrow division, which gives inf or NaN.

    Parameters
    ----------
    values
        The values to replace in

    Returns
    -------
    The values, with null instead of each zero
    """
    return pyarrow.compute.if_else(
        pyarrow.compute.equal(
            values,
            0,
        ),
        pyarrow.scalar(
            None,
            type=values.type,
        ),
        values,
    )


def _shift_timestamps(
    timestamps: pyarrow.Array,
    window: ClockSpec,
) -> pyarrow.Array:
    """
    Return each timestamp shifted forward by the time a window spans.

    The calendar windows get shifted by their number of months, like a date one year later, keeping the day of the
    month whenever it exists, and else moving it back to the month's last day.

    Parameters
    ----------
    timestamps
        The dates or timestamps to shift
    window
        The time spanned by the window

    Returns
    -------
    The shifted timestamps, with the same type, or null for the null timestamps
    """
    if window.reference_period in CALENDAR_PERIOD_MONTHS:
        window_months = window.periods_per_row * CALENDAR_PERIOD_MONTHS[window.reference_period]
        shifted_series = (
            pandas.to_datetime(timestamps.to_pandas())
            + pandas.DateOffset(months=window_months)
        )
        shifted_timestamps = pyarrow.array(
            shifted_series,
            from_pandas=True,
        )
    else:
        window_seconds = window.periods_per_row * FIXED_PERIOD_SECONDS[window.reference_period]
        shifted_timestamps = pyarrow.compute.add(
            timestamps,
            pyarrow.scalar(
                datetime.timedelta(seconds=window_seconds)
            ),
        )

    return pyarrow.compute.cast(
        shifted_timestamps,
        timestamps.type,
    )


def _sum_row_windows(
    values: pyarrow.Array,
    *,
    row_count: int,
) -> pyarrow.Array:
    """
    Return the exact sum of each trailing window of rows, by summing each window's rows as a separate group.

    Unlike the differences of cumulative sums, the grouped sums keep every digit, and keep the scale of the decimals
    with their full precision of 38 digits, so they can't raise PyArrow's decimal precision errors.

    Parameters
    ----------
    values
        The values to sum
    row_count
        The number of rows in each window, with 0 making every window null

    Returns
    -------
    The sum of each window ending at each row, or null for the incomplete windows at the start, and for the windows
    with any null value, like all of them for values without any data
    """
    total_row_count = len(values)

    if (
        pyarrow.types.is_null(values.type)
        or row_count < 1
        or row_count > total_row_count
    ):
        return pyarrow.nulls(
            total_row_count,
            type=values.type,
        )

    # Each window gets one copy of each of its rows, with the row before it in each copy's window ending one row later
    window_rows = pyarrow.concat_arrays([
        pyarrow.array(
            range(row_offset, total_row_count),
            type=pyarrow.int64(),
        )
        for row_offset in range(row_count)
    ])
    window_value_rows = pyarrow.concat_arrays([
        pyarrow.array(
            range(total_row_count - row_offset),
            type=pyarrow.int64(),
        )
        for row_offset in range(row_count)
    ])
    window_sums_table = pyarrow.table({
        'window_row': window_rows,
        'value': pyarrow.compute.take(
            values,
            window_value_rows,
        ),
    }).group_by(
        'window_row',
        use_threads=False,
    ).aggregate([
        (
            'value',
            'sum',
            pyarrow.compute.ScalarAggregateOptions(skip_nulls=False),
        ),
        (
            'value',
            'count',
            pyarrow.compute.CountOptions(mode='all'),
        ),
    ])
    complete_window_sums_table = window_sums_table.filter(
        pyarrow.compute.equal(
            window_sums_table['value_count'],
            row_count,
        )
    )
    complete_window_positions = pyarrow.compute.index_in(
        pyarrow.array(
            range(total_row_count),
            type=pyarrow.int64(),
        ),
        value_set=complete_window_sums_table['window_row'],
    )

    return pyarrow.compute.take(
        complete_window_sums_table['value_sum'],
        complete_window_positions,
    ).combine_chunks()


def _sum_time_windows(
    *,
    master_clock_column: DataColumn,
    reference_clock_column: DataColumn,
    values: pyarrow.Array,
    window: ClockSpec,
) -> pyarrow.Array:
    """
    Return the exact sum of the reference rows in the window of time up to each master clock row.

    Each master clock row's window holds the reference rows timestamped after one window before it, up to and
    including its own timestamp, so each reference row counts from the first master clock row at or after it, until
    one window after its timestamp. The reference rows with null timestamps don't count in any window.

    Parameters
    ----------
    master_clock_column
        The master clock whose rows each get a window
    reference_clock_column
        The clock whose rows hold the values to sum
    values
        The values of the reference clock's rows
    window
        The time spanned by each window

    Returns
    -------
    The sum of each master clock row's window, zero for the windows without any reference rows, as nothing happened
    in them, or null for the windows with any null value, like all of them for values without any data
    """
    master_timestamps = master_clock_column.reference_array
    master_row_count = len(master_timestamps)

    if pyarrow.types.is_null(values.type):
        if len(values) > 0:
            return pyarrow.nulls(master_row_count)

        # Without any reference rows, nothing happened in any window
        return pyarrow.array(
            [0] * master_row_count,
            type=pyarrow.int64(),
        )

    reference_timestamps = pyarrow.compute.cast(
        reference_clock_column.reference_array,
        master_timestamps.type,
    )
    first_master_rows = _count_boundaries_before(
        boundaries=master_timestamps,
        timestamps=reference_timestamps,
    ).to_pylist()
    expired_master_rows = _count_boundaries_before(
        boundaries=master_timestamps,
        timestamps=_shift_timestamps(
            reference_timestamps,
            window,
        ),
    ).to_pylist()

    window_rows: list[int] = []
    window_value_rows: list[int] = []
    for (value_row, first_master_row) in enumerate(first_master_rows):
        if first_master_row is None:
            continue

        window_master_rows = range(
            first_master_row,
            expired_master_rows[value_row],
        )
        window_rows.extend(window_master_rows)
        window_value_rows.extend([value_row] * len(window_master_rows))

    window_sums_table = pyarrow.table({
        'window_row': pyarrow.array(
            window_rows,
            type=pyarrow.int64(),
        ),
        'value': pyarrow.compute.take(
            values,
            pyarrow.array(
                window_value_rows,
                type=pyarrow.int64(),
            ),
        ),
    }).group_by(
        'window_row',
        use_threads=False,
    ).aggregate([
        (
            'value',
            'sum',
            pyarrow.compute.ScalarAggregateOptions(skip_nulls=False),
        ),
    ])
    window_sums = window_sums_table['value_sum'].combine_chunks()
    window_positions = pyarrow.compute.index_in(
        pyarrow.array(
            range(master_row_count),
            type=pyarrow.int64(),
        ),
        value_set=window_sums_table['window_row'],
    )

    # The master clock rows without any reference rows in their window don't get a group
    return pyarrow.compute.if_else(
        pyarrow.compute.is_null(window_positions),
        pyarrow.scalar(0).cast(window_sums.type),
        pyarrow.compute.take(
            window_sums,
            window_positions,
        ),
    )


def _validate_column(
    column: object,
    *,
    function_name: str,
    parameter_name: str,
) -> None:
    """
    Validate that a helper's column parameter is a DataColumn.

    Parameters
    ----------
    column
        The value passed as the column
    function_name
        The name of the helper, for the error message
    parameter_name
        The name of the helper's parameter, for the error message

    Raises
    ------
    CalculationHelperError
    """
    if not isinstance(column, DataColumn):
        msg = f"features.helpers.{function_name}() {parameter_name} parameter must be a DataColumn object"

        raise CalculationHelperError(msg)


def _validate_decimal_sums(sums: pyarrow.Array) -> None:
    """
    Validate that no decimal sum exceeds the digits that PyArrow decimals can hold, which PyArrow doesn't detect.

    Parameters
    ----------
    sums
        The sums to validate

    Raises
    ------
    CalculationHelperError
        If a decimal sum has more digits than DECIMAL_MAX_PRECISION
    """
    if not pyarrow.types.is_decimal(sums.type):
        return

    largest_sum = pyarrow.compute.max(
        pyarrow.compute.abs(
            pyarrow.compute.cast(
                sums,
                pyarrow.float64(),
            )
        )
    ).as_py()

    if (
        largest_sum is not None
        and largest_sum >= 10.0 ** (DECIMAL_MAX_PRECISION - sums.type.scale)
    ):
        msg = f"features.helpers.rolling_sum() sums exceed the {DECIMAL_MAX_PRECISION} digits of PyArrow decimals"

        raise CalculationHelperError(msg)


def _validate_row_count(
    row_count: object,
    *,
    function_name: str,
    parameter_name: str,
) -> None:
    """
    Validate that a helper's number of rows is a positive integer.

    Parameters
    ----------
    row_count
        The value passed as the number of rows
    function_name
        The name of the helper, for the error message
    parameter_name
        The name of the helper's parameter, for the error message

    Raises
    ------
    CalculationHelperError
    """
    # bool is an int subclass, but not a valid number of rows
    if (
        not isinstance(row_count, int)
        or isinstance(row_count, bool)
        or row_count <= 0
    ):
        msg = f"features.helpers.{function_name}() {parameter_name} parameter must be a positive integer"

        raise CalculationHelperError(msg)
