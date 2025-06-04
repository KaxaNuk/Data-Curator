import pandas
import pyarrow

from kaxanuk.data_curator import DataColumn


def test_load_changing_dtype():
    data = [1, 2, 3, None]
    int_column = DataColumn.load(
        pyarrow.array(data, type=pyarrow.int32())
    )
    float_column = DataColumn.load(
        pyarrow.array(data, type=pyarrow.float64())
    )
    recast_column = DataColumn.load(int_column, dtype=pyarrow.float64())

    assert DataColumn.fully_equal(
        float_column,
        recast_column,
        equal_nulls=True
    )


def test_load_from_pandas_with_nans():
    serie = pandas.Series([2.1, 5.2, float('nan'), 14])
    column = DataColumn.load([2.1, 5.2, float('nan'), 14])

    assert DataColumn.fully_equal(
        DataColumn.load(serie),
        column,
        equal_nulls=True
    )


def test_load_is_idempotent():
    column = DataColumn.load([1, 2, 3, None])

    assert column == DataColumn.load(column)


def test_to_pandas():
    data = [1, 2, 3, 4, 5]
    column = DataColumn.load(data)
    series = pandas.Series(data, dtype='int64[pyarrow]')
    column_series = column.to_pandas()

    pandas.testing.assert_series_equal(column_series, series)


def test_to_pyarrow():
    data = pyarrow.array([1, 2, 3, 4, 5])
    column = DataColumn(data)
    result = column.to_pyarrow()

    assert result == data
