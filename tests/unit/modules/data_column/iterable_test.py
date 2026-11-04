from decimal import Decimal

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn

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


@pytest.fixture
def sample_list_data():
    data = ['item1', 'item2', None, 'item4', 'item5']

    return data


class TestDunderArrowArray:
    def test_default_type(self):
        actual_result = DataColumn(pyarrow.array([1, 2, 3])).__arrow_array__()
        expected_result = pyarrow.array([1, 2, 3])

        assert (
            pyarrow.compute.all(
                pyarrow.compute.equal(actual_result, expected_result)
            )
            .as_py()
        )

    def test_specific_type(self):
        actual_result = DataColumn(pyarrow.array([1, 2, 3])).__arrow_array__(type=pyarrow.float64())
        expected_result = pyarrow.array([1, 2, 3], type=pyarrow.float64())

        assert (
            pyarrow.compute.all(
                pyarrow.compute.equal(actual_result, expected_result)
            )
            .as_py()
        )

    def test_column_on_reference_clock_at_master_scale(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column.__arrow_array__()
        expected = pyarrow.array([10, 21, 21])

        assert result.equals(expected)


class TestDunderGetitem:
    def test_slice_start(
        self,
        sample_list_data,
    ):
        column = DataColumn.load(sample_list_data)
        result = column[1:]
        expected = DataColumn.load(sample_list_data[1:])

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_slice_stop(
        self,
        sample_list_data,
    ):
        column = DataColumn.load(sample_list_data)
        result = column[:3]
        expected = DataColumn.load(sample_list_data[:3])

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_slice_step(
        self,
        sample_list_data,
    ):
        column = DataColumn.load(sample_list_data)
        result = column[::2]
        expected = DataColumn.load(sample_list_data[::2])

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_int_parameter(
        self,
        sample_list_data,
    ):
        column = DataColumn.load(sample_list_data)
        result = column[4]
        expected = DataColumn.load([sample_list_data[4]])

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_slice_of_column_on_reference_clock_at_master_scale(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column[1:3]
        expected = pyarrow.array([21, 21])

        assert result.reference_array.equals(expected)

    def test_slice_of_column_on_clock_without_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        result = column[0:2]

        assert result.reference_clock_column is None

    def test_int_parameter_on_reference_clock_at_master_scale(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column[2]
        expected = pyarrow.array([21])

        assert result.reference_array.equals(expected)


class TestDunderLen:
    def test_empty_pyarrow_array(self):
        array = pyarrow.array([])
        dc = DataColumn(array)

        assert len(dc) == 0

    def test_null_array(self):
        array = pyarrow.array([None, None], type=pyarrow.null())
        dc = DataColumn(array)

        assert len(dc) == 2

    def test_float_array_with_nan(self):
        array = pyarrow.array([1.1, 2.2, float('nan')])
        dc = DataColumn(array)

        assert len(dc) == 3

    def test_decimal_array_with_none(self):
        array = pyarrow.array([Decimal('1.1'), Decimal('2.2'), None])
        dc = DataColumn(array)

        assert len(dc) == 3

    def test_column_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )

        assert len(column) == 3

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )

        assert len(column) == 3
