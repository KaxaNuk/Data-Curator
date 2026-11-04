import pandas
import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods

from .fixtures import clocks


@pytest.fixture
def daily_master_clock():
    return clocks.create_daily_master_clock()


@pytest.fixture
def hourly_reference_clock(daily_master_clock):
    return clocks.create_hourly_reference_clock(daily_master_clock)


@pytest.fixture
def monthly_reference_clock(daily_master_clock):
    return clocks.create_monthly_reference_clock(daily_master_clock)


class TestToMasterScale:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])
        result = column.to_master_scale()

        assert result is column

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        result = daily_master_clock.to_master_scale()

        assert result is daily_master_clock

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        result = column.to_master_scale()

        assert result is column

    def test_column_on_reference_clock_moves_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        result = column.to_master_scale()

        assert result.reference_clock_column is daily_master_clock

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        result = column.to_master_scale()
        expected = pyarrow.array([10, 20, 20])

        assert result.reference_array.equals(expected)

    def test_column_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        result = column.to_master_scale()
        expected = pyarrow.array([2, 3, 4])

        assert result.reference_array.equals(expected)

    def test_column_on_downscaling_clock_with_own_consolidation_method(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
            reference_clock_column=hourly_reference_clock,
        )
        result = column.to_master_scale()
        expected = pyarrow.array([5, 3, 4])

        assert result.reference_array.equals(expected)

    def test_reference_clock_becomes_column_on_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = monthly_reference_clock.to_master_scale()

        assert result.reference_clock_column is daily_master_clock


class TestToPandas:
    def test_int_column_to_pyarrow_backed_series(self):
        data = [1, 2, 3, 4, 5]
        column = DataColumn.load(data)
        series = pandas.Series(data, dtype='int64[pyarrow]')
        column_series = column.to_pandas()

        pandas.testing.assert_series_equal(column_series, series)

    def test_column_on_reference_clock_at_master_scale(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        series = pandas.Series(
            [10, 20, 20],
            dtype='int64[pyarrow]',
        )
        column_series = column.to_pandas()

        pandas.testing.assert_series_equal(column_series, series)


class TestToPyarrow:
    def test_returns_wrapped_array(self):
        data = pyarrow.array([1, 2, 3, 4, 5])
        column = DataColumn(data)
        result = column.to_pyarrow()

        assert result == data

    def test_column_on_reference_clock_at_master_scale(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        result = column.to_pyarrow()
        expected = pyarrow.array([10, 20, 20])

        assert result.equals(expected)
