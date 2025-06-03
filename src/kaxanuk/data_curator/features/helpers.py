import typing
import pyarrow.compute
import pandas
import pyarrow
from decimal import Decimal
from kaxanuk.data_curator.exceptions import CalculationHelperError
from kaxanuk.data_curator.modules.data_column import DataColumn

MARKET_DAYS_PER_YEAR = 252


def annualized_volatility(
    *,
    column: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the annualized volatility of column with a standard deviation rolling window of days.

    Parameters
    ----------
    column
        The column containing the data for which the annualized volatility needs to be calculated.
    days
        The number of days to use for the rolling standard deviation calculation.

    Returns
    -------
    A new DataColumn object containing the annualized volatility values.
    """
    if not isinstance(column, DataColumn):
        msg = "features.helpers.annualized_volatility() column parameter must be a DataColumn object"
        raise CalculationHelperError(msg)

    if (
        not isinstance(days, int)
        or days <= 0
    ):
        msg = "features.helpers.annualized_volatility() days parameter must be a positive integer"
        raise CalculationHelperError(msg)

    rolling_std = (
        column
            .to_pandas()
            .rolling(days)
            .std()
    )
    volatility = (
        rolling_std
        * (MARKET_DAYS_PER_YEAR ** 0.5)
    )

    return DataColumn.load(volatility)


def chaikin_money_flow(
    *,
    high: DataColumn,
    low: DataColumn,
    close: DataColumn,
    volume: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the Chaikin Money Flow (CMF) indicator over a specified period.

    The CMF measures the volume-weighted average of accumulation and distribution over a given number of days.

    Parameters
    ----------
    high : DataColumn
        High prices.
    low : DataColumn
        Low prices.
    close : DataColumn
        Close prices.
    volume : DataColumn
        Trading volume.
    days : int
        Number of days for the rolling calculation.

    Returns
    -------
    DataColumn
        The CMF values as a DataColumn.
    """
    if not all(isinstance(col, DataColumn) for col in (high, low, close, volume)):
        raise CalculationHelperError("All input parameters must be DataColumn objects")
    if not isinstance(days, int) or days <= 0:
        raise CalculationHelperError("days parameter must be a positive integer")

    high_list = high.to_pyarrow().to_pylist()
    low_list = low.to_pyarrow().to_pylist()
    close_list = close.to_pyarrow().to_pylist()
    vol_list = volume.to_pyarrow().to_pylist()

    mfv_list = []
    for (h_val, low_val, c_val, v_val) in zip(high_list, low_list, close_list, vol_list, strict=False):
        if None in (h_val, low_val, c_val, v_val):
            mfv_list.append(None)
        else:
            (h_d, low_d, c_d, v_d) = (Decimal(str(x)) for x in (h_val, low_val, c_val, v_val))
            if h_d == low_d:
                mfv_list.append(None)
            else:
                mfm = ((c_d - low_d) - (h_d - c_d)) / (h_d - low_d)
                mfv_list.append(mfm * v_d)

    cmf_list = []
    window_mfv = []
    window_vol = []

    for (mfv, v) in zip(mfv_list, vol_list, strict=False):
        window_mfv.append(mfv)
        window_vol.append(Decimal(str(v)) if v is not None else None)

        if len(window_mfv) > days:
            window_mfv.pop(0)
            window_vol.pop(0)

        if (
            len(window_mfv) < days
            or any(x is None for x in window_mfv)
            or any(x is None for x in window_vol)
        ):
            cmf_list.append(None)
        else:
            total_mfv = sum(window_mfv)
            total_vol = sum(window_vol)
            cmf = (total_mfv / total_vol) if total_vol != 0 else None
            cmf_list.append(float(cmf) if cmf is not None else None)

    arrow_array = pyarrow.array(cmf_list, from_pandas=True)
    finite_mask = pyarrow.compute.is_finite(arrow_array)
    if not pyarrow.compute.all(finite_mask).as_py():
        arrow_array = pyarrow.compute.if_else(
            finite_mask,
            arrow_array,
            pyarrow.scalar(None, type=arrow_array.type)
        )
    return DataColumn.load(arrow_array)


def exponential_moving_average(
    *,
    column: DataColumn,
    days: int
) -> DataColumn:
    """
    Calculate the Exponential Moving Average (EMA) over a specified period.

    Based on a smoothing factor of 2/(days+1). Resets the calculation on missing data.

    Parameters
    ----------
    column : DataColumn
        Data for which the EMA is calculated.
    days : int
        Number of days for the EMA.

    Returns
    -------
    DataColumn
        EMA values as a DataColumn.
    """
    if not isinstance(column, DataColumn):
        raise CalculationHelperError("column parameter must be a DataColumn object")
    if not isinstance(days, int) or days <= 0:
        raise CalculationHelperError("days parameter must be a positive integer")

    data = column.to_pyarrow().to_pylist()
    smoothing_factor = Decimal('2') / Decimal(days + 1)

    ema_values = []
    window_values = []
    current_ema = None

    for value in data:
        if value is None:
            ema_values.append(None)
            window_values = []
            current_ema = None
        else:
            value_decimal = Decimal(str(value))
            window_values.append(value_decimal)

            if len(window_values) < days:
                ema_values.append(None)
            elif len(window_values) == days:
                current_ema = sum(window_values) / Decimal(days)
                ema_values.append(float(current_ema))
            else:
                current_ema = (
                    value_decimal * smoothing_factor
                    + current_ema * (Decimal('1') - smoothing_factor)
                )
                ema_values.append(float(current_ema))

    arrow_array = pyarrow.array(ema_values, from_pandas=True)
    finite_mask = pyarrow.compute.is_finite(arrow_array)
    if not pyarrow.compute.all(finite_mask).as_py():
        arrow_array = pyarrow.compute.if_else(
            finite_mask,
            arrow_array,
            pyarrow.scalar(None, type=arrow_array.type)
        )
    return DataColumn.load(arrow_array)


def indexed_rolling_window_operation(
    *,
    key_column: DataColumn,
    value_column: DataColumn,
    operation_function: typing.Callable,
    window_length: int
) -> DataColumn:
    """
    Apply a rolling window operation on data corresponding to the unique keys, repeating the values on duplicates.

    Useful for rolling windows on period data across periods, with each period having the same key and thus
    the same data.

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
    """
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
    result = key_column.to_pandas().map(
        rolling_applied.drop(
            ['', None, float('nan')],
            errors='ignore'
        ),
        na_action='ignore'
    )

    return DataColumn.load(result)


def log_returns(column: DataColumn) -> DataColumn:
    """
    Calculate the logarithmic returns for a series of prices.

    Parameters
    ----------
    column : DataColumn
        Price series for which to compute log returns.

    Returns
    -------
    DataColumn
        Log returns series, with None for the first element.
    """
    if not isinstance(column, DataColumn):
        raise CalculationHelperError("log_returns() requires a DataColumn input")

    shifted_ratio = column[1:] / column[:-1]
    ln_array = pyarrow.compute.ln(shifted_ratio.to_pyarrow())
    output_array = pyarrow.concat_arrays([
        pyarrow.array([None], type=ln_array.type),
        ln_array
    ])

    return DataColumn.load(output_array)


def replace_infinite_with_none(column) -> DataColumn:
    """
    Replace -inf and inf values in a DataColumn with None.

    Parameters
    ----------
    column : DataColumn
        The column containing the calculated results.

    Returns
    -------
    DataColumn
        A new DataColumn object with -inf and inf values replaced by None.
    """
    column = column.to_pyarrow()
    finite_mask = pyarrow.compute.is_finite(column)
    if pyarrow.compute.all(finite_mask).as_py():
        return column
    result = pyarrow.compute.if_else(finite_mask, column, pyarrow.scalar(None))
    return DataColumn.load(result)


def relative_strength_index(*, column: DataColumn, days: int) -> DataColumn:
    """
    Calculate the Relative Strength Index (RSI) over a specified period.

    The RSI is a momentum oscillator that measures the speed and change of price movements.

    Parameters
    ----------
    column : DataColumn
        Price data for RSI calculation.
    days : int
        Number of days for the RSI calculation.

    Returns
    -------
    DataColumn
        The RSI values as a DataColumn.
    """
    if not isinstance(column, DataColumn):
        msg = "features.helpers.relative_strength_index() column parameter must be a DataColumn object"
        raise CalculationHelperError(msg)
    if not isinstance(days, int) or days <= 0:
        msg = "features.helpers.relative_strength_index() days parameter must be a positive integer"
        raise CalculationHelperError(msg)

    # Extract raw list, including None
    data = column.to_pyarrow().to_pylist()
    n = len(data)

    # Compute gains/losses
    gain_list = [None] * n
    loss_list = [None] * n
    for i in range(1, n):
        prev, curr = data[i-1], data[i]
        if prev is None or curr is None:
            gain_list[i] = None
            loss_list[i] = None
        else:
            diff = curr - prev
            gain_list[i] = diff if diff > 0 else 0
            loss_list[i] = (-diff) if diff < 0 else 0

    rsi_list = [None] * n
    current_gain = None
    current_loss = None
    window_gain = []
    window_loss = []

    for i in range(n):
        gain_val, loss_val = gain_list[i], loss_list[i]
        if gain_val is None or loss_val is None:
            window_gain = []
            window_loss = []
            current_gain = None
            current_loss = None
            rsi_list[i] = None
            continue

        if current_gain is None:
            window_gain.append(Decimal(gain_val))
            window_loss.append(Decimal(loss_val))
            if len(window_gain) < days:
                rsi_list[i] = None
                continue
            current_gain = sum(window_gain) / Decimal(days)
            current_loss = sum(window_loss) / Decimal(days)
        else:
            current_gain = (current_gain * (days - 1) + Decimal(gain_val)) / Decimal(days)
            current_loss = (current_loss * (days - 1) + Decimal(loss_val)) / Decimal(days)

        if current_loss == 0:
            rsi_list[i] = None
        else:
            rs = current_gain / current_loss
            rsi_list[i] = float(Decimal('100') - (Decimal('100') / (Decimal('1') + rs)))

    arrow_array = pyarrow.array(rsi_list, from_pandas=True)
    finite_mask = pyarrow.compute.is_finite(arrow_array)
    if not pyarrow.compute.all(finite_mask).as_py():
        arrow_array = pyarrow.compute.if_else(
            finite_mask,
            arrow_array,
            pyarrow.scalar(None, type=arrow_array.type)
        )
    return DataColumn.load(arrow_array)


def simple_moving_average(column: DataColumn, days: int) -> DataColumn:
    """
    Calculate the simple moving average over a given window.

    Parameters
    ----------
    column : DataColumn
        Price series for which to compute the moving average.
    days : int
        Number of periods to include in the average.

    Returns
    -------
    DataColumn
        Simple moving average series, with None for initial elements until window is reached.
    """
    if not isinstance(column, DataColumn):
        raise CalculationHelperError("simple_moving_average() requires a DataColumn input")
    if not isinstance(days, int) or days <= 0:
        raise CalculationHelperError("simple_moving_average() requires a positive integer window")

    series = column.to_pandas().rolling(window=days).mean()
    return DataColumn.load(series)
