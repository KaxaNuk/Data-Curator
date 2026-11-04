from decimal import Decimal

import pytest
import pyarrow

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.exceptions import DataColumnSyncError

from .fixtures import clocks


@pytest.fixture
def daily_master_clock():
    return clocks.create_daily_master_clock()


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


@pytest.fixture
def hourly_reference_clock(daily_master_clock):
    return clocks.create_hourly_reference_clock(daily_master_clock)


@pytest.fixture
def monthly_reference_clock(daily_master_clock):
    return clocks.create_monthly_reference_clock(daily_master_clock)


@pytest.fixture
def other_daily_master_clock():
    return clocks.create_daily_master_clock()


class TestEqual:
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
    def test_default_kwargs(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_false_equal_nulls_false(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_true_equal_nulls_false(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_false_equal_nulls_true(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_true_equal_nulls_true(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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

    def test_none_columns_with_equal_nulls_false(self):
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

    def test_none_columns_with_equal_nulls_true(self):
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

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.equal(
            DataColumn.load(
                [1, 2],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [1, 3],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.equal(
            DataColumn.load(
                [1, 2],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [1, 3],
                reference_clock_column=monthly_reference_clock,
            ),
        )
        expected = pyarrow.array([True, False])

        assert result.reference_array.equals(expected)

    def test_columns_with_nulls_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.equal(
            DataColumn.load(
                [1, None],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [1, None],
                reference_clock_column=monthly_reference_clock,
            ),
            equal_nulls=True,
        )

        assert result.reference_clock_column is monthly_reference_clock

    @pytest.mark.parametrize(
        'equal_nulls',
        [
            False,
            True,
        ]
    )
    def test_null_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
        equal_nulls,
    ):
        result = DataColumn.equal(
            DataColumn(
                pyarrow.nulls(2),
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn(
                pyarrow.nulls(2),
                reference_clock_column=monthly_reference_clock,
            ),
            equal_nulls=equal_nulls,
        )

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.equal(
            DataColumn.load(
                [1, 2],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [1, 2, 3],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.equal(
            DataColumn.load(
                [1, 2],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [1, 2, 3],
                reference_clock_column=daily_master_clock,
            ),
        )
        expected = pyarrow.array([True, True, False])

        assert result.reference_array.equals(expected)

    def test_approximate_floats_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.equal(
            DataColumn.load(
                [0.1 + 0.2, 2.0],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [0.3, 2.0, 3.0],
                reference_clock_column=daily_master_clock,
            ),
            approximate_floats=True,
        )
        expected = pyarrow.array([True, True, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn.equal(
                DataColumn.load(
                    [1, 2, 3],
                    reference_clock_column=daily_master_clock,
                ),
                DataColumn.load(
                    [1, 2, 3],
                    reference_clock_column=other_daily_master_clock,
                ),
            )


class TestFullyEqual:
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
    def test_default_kwargs(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_false_equal_nulls_false_skip_nulls_false(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_true_equal_nulls_false_skip_nulls_false(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_false_equal_nulls_true_skip_nulls_false(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_true_equal_nulls_true_skip_nulls_false(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_false_equal_nulls_false_skip_nulls_true(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_true_equal_nulls_false_skip_nulls_true(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_false_equal_nulls_true_skip_nulls_true(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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
    def test_approximate_floats_true_equal_nulls_true_skip_nulls_true(
        self,
        example_column_equality_data,
        column1,
        column2,
        expected_result,
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

    def test_columns_on_same_reference_clock_compared_at_master_scale(
        self,
        hourly_reference_clock,
    ):
        # The columns differ at their reference clock's scale, but consolidate into the same master clock values
        result = DataColumn.fully_equal(
            DataColumn.load(
                [5, 2, 3, 4],
                reference_clock_column=hourly_reference_clock,
            ),
            DataColumn.load(
                [1, 2, 3, 4],
                reference_clock_column=hourly_reference_clock,
            ),
        )

        assert result is True

    def test_same_reference_values_with_different_consolidation_methods(
        self,
        hourly_reference_clock,
    ):
        # The columns match at their reference clock's scale, but consolidate into different master clock values
        result = DataColumn.fully_equal(
            DataColumn.load(
                [5, 2, 3, 4],
                consolidation_method=clock_sync_methods.ConsolidationMethods.last,
                reference_clock_column=hourly_reference_clock,
            ),
            DataColumn.load(
                [5, 2, 3, 4],
                consolidation_method=clock_sync_methods.ConsolidationMethods.max,
                reference_clock_column=hourly_reference_clock,
            ),
        )

        assert result is False

    def test_columns_on_different_clocks_compared_at_master_scale(
        self,
        daily_master_clock,
        hourly_reference_clock,
    ):
        result = DataColumn.fully_equal(
            DataColumn.load(
                [5, 2, 3, 4],
                reference_clock_column=hourly_reference_clock,
            ),
            DataColumn.load(
                [2, 3, 4],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result is True

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn.fully_equal(
                DataColumn.load(
                    [1, 2, 3],
                    reference_clock_column=daily_master_clock,
                ),
                DataColumn.load(
                    [1, 2, 3],
                    reference_clock_column=other_daily_master_clock,
                ),
            )


class TestIsNull:
    def test_null_array(self):
        test_array = pyarrow.array([None, None], type=pyarrow.null())
        data_column = DataColumn(test_array)
        result = data_column.is_null()

        assert result is True

    def test_int_array(self):
        test_array = pyarrow.array([2, 5])
        data_column = DataColumn(test_array)
        result = data_column.is_null()
        assert result is False

    def test_null_array_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn(
            pyarrow.nulls(2),
            reference_clock_column=monthly_reference_clock,
        )
        result = column.is_null()

        assert result is True

    def test_null_array_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn(
            pyarrow.nulls(4),
            reference_clock_column=hourly_reference_clock,
        )
        result = column.is_null()

        assert result is True

    def test_int_array_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        result = column.is_null()

        assert result is False
