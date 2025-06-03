from decimal import Decimal

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn


@pytest.fixture
def sample_list_data():
    data = ['item1', 'item2', None, 'item4', 'item5']

    return data


def test_arrow_array_default_type():
    actual_result = DataColumn(pyarrow.array([1, 2, 3])).__arrow_array__()
    expected_result = pyarrow.array([1, 2, 3])

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(actual_result, expected_result)
        )
        .as_py()
    )


def test_arrow_array_specific_type():
    actual_result = DataColumn(pyarrow.array([1, 2, 3])).__arrow_array__(type=pyarrow.float64())
    expected_result = pyarrow.array([1, 2, 3], type=pyarrow.float64())

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(actual_result, expected_result)
        )
        .as_py()
    )


def test_get_item_slice_start(sample_list_data):
    column = DataColumn.load(sample_list_data)
    result = column[1:]
    expected = DataColumn.load(sample_list_data[1:])

    assert DataColumn.fully_equal(
        result,
        expected,
        equal_nulls=True
    )


def test_get_item_slice_stop(sample_list_data):
    column = DataColumn.load(sample_list_data)
    result = column[:3]
    expected = DataColumn.load(sample_list_data[:3])

    assert DataColumn.fully_equal(
        result,
        expected,
        equal_nulls=True
    )


def test_get_item_slice_step(sample_list_data):
    column = DataColumn.load(sample_list_data)
    result = column[::2]
    expected = DataColumn.load(sample_list_data[::2])

    assert DataColumn.fully_equal(
        result,
        expected,
        equal_nulls=True
    )


def test_get_item_int_parameter(sample_list_data):
    column = DataColumn.load(sample_list_data)
    result = column[4]
    expected = DataColumn.load([sample_list_data[4]])

    assert DataColumn.fully_equal(
        result,
        expected,
        equal_nulls=True
    )


def test_len_with_empty_pyarrow_array():
    array = pyarrow.array([])
    dc = DataColumn(array)

    assert len(dc) == 0


def test_len_with_null_array():
    array = pyarrow.array([None, None], type=pyarrow.null())
    dc = DataColumn(array)

    assert len(dc) == 2


def test_len_with_float_array_with_nan():
    array = pyarrow.array([1.1, 2.2, float('nan')])
    dc = DataColumn(array)

    assert len(dc) == 3


def test_len_with_decimal_array_with_none():
    array = pyarrow.array([Decimal('1.1'), Decimal('2.2'), None])
    dc = DataColumn(array)

    assert len(dc) == 3
