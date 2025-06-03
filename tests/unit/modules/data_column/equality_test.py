from decimal import Decimal

import pytest
import pyarrow

from kaxanuk.data_curator import DataColumn


@pytest.fixture(scope="module")
def example_column_equality_data():
    return {
        'column_ints_without_none': DataColumn(pyarrow.array([1, 2, 3, 4, 5])),
        'column_ints_without_none_duplicate': DataColumn(pyarrow.array([1, 2, 3, 4, 5])),
        'column_ints_without_none_changed': DataColumn(pyarrow.array([1, 3, 2, 4, 5])),
        'column_ints_without_none_as_float': DataColumn(pyarrow.array([1.0, 2.0, 3.0, 4.0, 5.0])),
        'column_ints_without_none_as_decimal': DataColumn(pyarrow.array([
            Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5')
        ])),
        'column_ints_without_none_as_decimal_float': DataColumn(pyarrow.array([
            Decimal('1.0'), Decimal('2.0'), Decimal('3.0'), Decimal('4.0'), Decimal('5.0')
        ])),
        'column_ints_with_none': DataColumn(pyarrow.array([1, 2, 3, None, 5])),
        'column_ints_with_none_duplicate': DataColumn(pyarrow.array([1, 2, 3, None, 5])),
        'column_ints_with_none_changed': DataColumn(pyarrow.array([1, 3, 2, None, 5])),
        'column_ints_with_none_as_float': DataColumn(pyarrow.array([1.0, 2.0, 3.0, None, 5.0])),
        'column_ints_with_nan_as_float': DataColumn(pyarrow.array([1.0, 2.0, 3.0, float('nan'), 5.0])),
        'column_ints_with_none_as_decimal': DataColumn(pyarrow.array([
            Decimal('1'), Decimal('2'), Decimal('3'), None, Decimal('5')
        ])),
        'column_ints_with_none_as_decimal_float': DataColumn(pyarrow.array([
            Decimal('1.0'), Decimal('2.0'), Decimal('3.0'), None, Decimal('5.0')
        ])),
        'column_float_error_without_none': DataColumn(pyarrow.array([1.0, 2.0, ((0.1 + 0.2) * 10), 4.0, 5.0])),
        'column_float_error_with_none': DataColumn(pyarrow.array([1.0, 2.0, ((0.1 + 0.2) * 10), None, 5.0])),
        'column_float_error_with_nan': DataColumn(pyarrow.array([1.0, 2.0, ((0.1 + 0.2) * 10), float('nan'), 5.0])),
        'result_all_equal': DataColumn(pyarrow.array([True, True, True, True, True])),
        'result_only_none_unequal': DataColumn(pyarrow.array([True, True, True, False, True])),
        'result_only_changed_unequal': DataColumn(pyarrow.array([True, False, False, True, True])),
        'result_changed_none_unequal': DataColumn(pyarrow.array([True, False, False, False, True])),
        'result_only_approximation_unequal': DataColumn(pyarrow.array([True, True, False, True, True])),
        'result_approximation_none_unequal': DataColumn(pyarrow.array([True, True, False, False, True])),
    }


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', 'result_only_none_unequal'),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', 'result_only_none_unequal'),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', 'result_only_none_unequal'),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', 'result_only_none_unequal'),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', 'result_only_none_unequal'),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', 'result_only_changed_unequal'),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', 'result_only_none_unequal'),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', 'result_changed_none_unequal'),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', 'result_only_approximation_unequal'),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', 'result_approximation_none_unequal'),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', 'result_approximation_none_unequal'),
    ]
)
def test_equal_default_kwargs(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.equal with all default kwargs (expecting approximate_floats=False, equal_nulls=False)
    calculated_result = DataColumn.equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2]
    )

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                calculated_result.to_pyarrow(),
                example_column_equality_data[expected_result].to_pyarrow()
            )
        )
        == pyarrow.scalar(True)
    )


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', 'result_only_none_unequal'),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', 'result_only_none_unequal'),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', 'result_only_none_unequal'),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', 'result_only_none_unequal'),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', 'result_only_none_unequal'),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', 'result_only_changed_unequal'),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', 'result_only_none_unequal'),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', 'result_changed_none_unequal'),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', 'result_only_approximation_unequal'),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', 'result_approximation_none_unequal'),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', 'result_approximation_none_unequal'),
    ]
)
def test_equal_approximate_floats_false_equal_nulls_false(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.equal with explicit approximate_floats=False, equal_nulls=False
    calculated_result = DataColumn.equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=False,
        equal_nulls=False
    )

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                calculated_result.to_pyarrow(),
                example_column_equality_data[expected_result].to_pyarrow()
            )
        )
        == pyarrow.scalar(True)
    )


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', 'result_only_none_unequal'),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', 'result_only_none_unequal'),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', 'result_only_none_unequal'),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', 'result_only_none_unequal'),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', 'result_only_none_unequal'),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', 'result_only_changed_unequal'),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', 'result_only_none_unequal'),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', 'result_changed_none_unequal'),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', 'result_all_equal'),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', 'result_only_none_unequal'),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', 'result_only_none_unequal'),
    ]
)
def test_equal_approximate_floats_true_equal_nulls_false(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.equal with approximate_floats=True, equal_nulls=False
    calculated_result = DataColumn.equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=True,
        equal_nulls=False
    )

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                calculated_result.to_pyarrow(),
                example_column_equality_data[expected_result].to_pyarrow()
            )
        )
        == pyarrow.scalar(True)
    )


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', 'result_all_equal'),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', 'result_only_changed_unequal'),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', 'result_only_none_unequal'),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', 'result_only_changed_unequal'),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', 'result_only_approximation_unequal'),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', 'result_only_approximation_unequal'),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', 'result_only_approximation_unequal'),
    ]
)
def test_equal_approximate_floats_false_equal_nulls_true(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.equal with approximate_floats=False, equal_nulls=True
    calculated_result = DataColumn.equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=False,
        equal_nulls=True
    )

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                calculated_result.to_pyarrow(),
                example_column_equality_data[expected_result].to_pyarrow()
            )
        )
        == pyarrow.scalar(True)
    )


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', 'result_all_equal'),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', 'result_all_equal'),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', 'result_all_equal'),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', 'result_all_equal'),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', 'result_all_equal'),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', 'result_only_changed_unequal'),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', 'result_only_none_unequal'),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', 'result_only_changed_unequal'),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', 'result_all_equal'),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', 'result_all_equal'),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', 'result_all_equal'),
    ]
)
def test_equal_approximate_floats_true_equal_nulls_true(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.equal with approximate_floats=False, equal_nulls=True
    calculated_result = DataColumn.equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=True,
        equal_nulls=True
    )
    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                calculated_result.to_pyarrow(),
                example_column_equality_data[expected_result].to_pyarrow()
            )
        )
        == pyarrow.scalar(True)
    )


def test_equal_none_columns_with_equal_nulls_false():
    column1 = DataColumn.load([None])
    column2 = DataColumn.load([None])
    result = DataColumn.equal(
        column1,
        column2
    )
    expected_result = DataColumn.load([False])

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                result.to_pyarrow(),
                expected_result.to_pyarrow()
            )
        )
        == pyarrow.scalar(True)
    )


def test_equal_none_columns_with_equal_nulls_true():
    column1 = DataColumn.load([None])
    column2 = DataColumn.load([None])
    result = DataColumn.equal(
        column1,
        column2,
        equal_nulls=True
    )
    expected_result = DataColumn.load([True])

    assert (
        pyarrow.compute.all(
            pyarrow.compute.equal(
                result.to_pyarrow(),
                expected_result.to_pyarrow(),
            )
        )
        == pyarrow.scalar(True)
    )


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', None),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', None),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', None),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', None),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', None),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', None),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', False),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', False),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', False),
    ]
)
def test_fully_equal_default_kwargs(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with all default kwargs
    # (expecting approximate_floats=False, equal_nulls=False, skip_nulls=False)
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2]
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', None),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', None),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', None),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', None),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', None),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', None),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', False),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', False),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', False),
    ]
)
def test_fully_equal_approximate_floats_false_equal_nulls_false_skip_nulls_false(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with explicit approximate_floats=False, equal_nulls=False, skip_nulls=False
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=False,
        equal_nulls=False,
        skip_nulls=False
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', None),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', None),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', None),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', None),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', None),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', None),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', True),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', None),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', None),
    ]
)
def test_fully_equal_approximate_floats_true_equal_nulls_false_skip_nulls_false(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with approximate_floats=True, equal_nulls=False, skip_nulls=False
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=True,
        equal_nulls=False,
        skip_nulls=False
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', True),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', True),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', True),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', True),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', True),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', False),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', False),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', False),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', False),
    ]
)
def test_fully_equal_approximate_floats_false_equal_nulls_true_skip_nulls_false(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with explicit approximate_floats=False, equal_nulls=True, skip_nulls=False
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=False,
        equal_nulls=True,
        skip_nulls=False
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', True),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', True),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', True),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', True),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', True),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', False),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', True),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', True),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', True),
    ]
)
def test_fully_equal_approximate_floats_true_equal_nulls_true_skip_nulls_false(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with explicit approximate_floats=True, equal_nulls=True, skip_nulls=False
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=True,
        equal_nulls=True,
        skip_nulls=False
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', True),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', True),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', True),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', True),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', True),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', True),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', False),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', False),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', False),
    ]
)
def test_fully_equal_approximate_floats_false_equal_nulls_false_skip_nulls_true(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with explicit approximate_floats=False, equal_nulls=False, skip_nulls=True
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=False,
        equal_nulls=False,
        skip_nulls=True
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', True),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', True),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', True),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', True),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', True),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', True),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', True),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', True),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', True),
    ]
)
def test_fully_equal_approximate_floats_true_equal_nulls_false_skip_nulls_true(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with approximate_floats=True, equal_nulls=False, skip_nulls=True
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=True,
        equal_nulls=False,
        skip_nulls=True
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', True),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', True),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', True),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', True),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', True),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', False),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', False),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', False),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', False),
    ]
)
def test_fully_equal_approximate_floats_false_equal_nulls_true_skip_nulls_true(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with explicit approximate_floats=False, equal_nulls=True, skip_nulls=True
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=False,
        equal_nulls=True,
        skip_nulls=True
    )

    assert result is expected_result


@pytest.mark.parametrize(
    ('column1', 'column2', 'expected_result'),
    [
        # Test case: Equal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_duplicate', True),
        # Test case: Equal int columns as float without None
        ('column_ints_without_none', 'column_ints_without_none_as_float', True),
        # Test case: Equal int columns as decimal without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal', True),
        # Test case: Equal int columns as decimal float without None
        ('column_ints_without_none', 'column_ints_without_none_as_decimal_float', True),
        # Test case: Equal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_duplicate', True),
        # Test case: Equal int columns as float with None
        ('column_ints_with_none', 'column_ints_with_none_as_float', True),
        # Test case: Equal int columns as decimal with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal', True),
        # Test case: Equal int columns as decimal float with None
        ('column_ints_with_none', 'column_ints_with_none_as_decimal_float', True),
        # Test case: Equal int columns as float with NaN
        ('column_ints_with_none', 'column_ints_with_nan_as_float', True),
        # Test case: Unequal int columns without None
        ('column_ints_without_none', 'column_ints_without_none_changed', False),
        # Test case: Unequal int columns with mixed None
        ('column_ints_with_none', 'column_ints_without_none', False),
        # Test case: Unequal int columns with None
        ('column_ints_with_none', 'column_ints_with_none_changed', False),
        # Test case: Approximately equal int float columns without None
        ('column_ints_without_none', 'column_float_error_without_none', True),
        # Test case: Approximately equal int float columns with None
        ('column_ints_with_none', 'column_float_error_with_none', True),
        # Test case: Approcimately equal int float columns with NaN
        ('column_ints_with_none', 'column_float_error_with_nan', True),
    ]
)
def test_fully_equal_approximate_floats_true_equal_nulls_true_skip_nulls_true(
    example_column_equality_data,
    column1, column2, expected_result
):
    # Test case: Datacolumn.all_equal with explicit approximate_floats=True, equal_nulls=True, skip_nulls=True
    result = DataColumn.fully_equal(
        example_column_equality_data[column1],
        example_column_equality_data[column2],
        approximate_floats=True,
        equal_nulls=True,
        skip_nulls=True
    )

    assert result is expected_result


def test_is_null_with_null_array():
    test_array = pyarrow.array([None, None], type=pyarrow.null())
    data_column = DataColumn(test_array)
    result = data_column.is_null()

    assert result is True


def test_is_null_with_int_array():
    test_array = pyarrow.array([2, 5])
    data_column = DataColumn(test_array)
    result = data_column.is_null()
    assert result is False


def test_type():
    data_type = pyarrow.int32()
    column = DataColumn(
        pyarrow.array(
            [1, 2, 3],
            type=data_type
        )
    )

    assert column.type == data_type
