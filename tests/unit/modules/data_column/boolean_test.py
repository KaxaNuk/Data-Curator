import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.exceptions import (
    DataColumnParameterError,
    DataColumnSyncError,
)

from .fixtures import clocks


@pytest.fixture
def daily_master_clock():
    return clocks.create_daily_master_clock()


@pytest.fixture
def monthly_reference_clock(daily_master_clock):
    return clocks.create_monthly_reference_clock(daily_master_clock)


@pytest.fixture
def other_daily_master_clock():
    return clocks.create_daily_master_clock()


class TestBooleanAnd:
    def test_one_column_disallowing_null_comparisons(self):
        column = DataColumn.load(
            [1, None, 0]
        )
        result = DataColumn.boolean_and(column)
        expected = DataColumn.load(
            [True, None, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_one_column_allowing_null_comparisons(self):
        column = DataColumn.load(
            [1, None, 0]
        )
        result = DataColumn.boolean_and(
            column,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True, None, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_two_columns_disallowing_null_comparisons(self):
        column1 = DataColumn.load(
            [True,  False, True, False, None, False, None]
        )
        column2 = DataColumn.load(
            [False, False, True, True,  True, None,  None]
        )
        result = DataColumn.boolean_and(
            column1,
            column2
        )
        expected = DataColumn.load(
            [False, False, True, False,  None, None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_two_columns_allowing_null_comparisons(self):
        column1 = DataColumn.load(
            [True,  False, True, False, None, False, None]
        )
        column2 = DataColumn.load(
            [False, False, True, True,  True, None,  None]
        )
        result = DataColumn.boolean_and(
            column1,
            column2,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [False, False, True, False, None, False,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_three_columns_disallowing_null_comparisons(self):
        column1 = DataColumn.load(
            [True,  False, True, False, None, False, None]
        )
        column2 = DataColumn.load(
            [False, False, True, True,  True, None,  None]
        )
        column3 = DataColumn.load(
            [True,  False,  True, False, None, False, None]
        )
        result = DataColumn.boolean_and(
            column1,
            column2,
            column3
        )
        expected = DataColumn.load(
            [False, False,  True, False, None, None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_no_columns(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn.boolean_and()

    def test_only_scalars(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn.boolean_and(
                True,
                False,
            )

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_and(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [True, True],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_and(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [True, True],
                reference_clock_column=monthly_reference_clock,
            ),
        )
        expected = pyarrow.array([True, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_and(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [True, True, False],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_and(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [True, True, False],
                reference_clock_column=daily_master_clock,
            ),
        )
        expected = pyarrow.array([True, False, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn.boolean_and(
                DataColumn.load(
                    [True, False, True],
                    reference_clock_column=daily_master_clock,
                ),
                DataColumn.load(
                    [True, True, False],
                    reference_clock_column=other_daily_master_clock,
                ),
            )

    def test_column_against_bool_disallowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = True
        result = DataColumn.boolean_and(
            column,
            scalar
        )
        expected = DataColumn.load(
            [True,  False,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_column_against_bool_allowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = True
        result = DataColumn.boolean_and(
            column,
            scalar,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True,  False,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_column_against_scalar_disallowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = pyarrow.scalar(
            True,
            type=pyarrow.bool_()
        )
        result = DataColumn.boolean_and(
            column,
            scalar
        )
        expected = DataColumn.load(
            [True,  False, None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_column_against_scalar_allowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = pyarrow.scalar(
            True,
            type=pyarrow.bool_()
        )
        result = DataColumn.boolean_and(
            column,
            scalar,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True,  False, None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )


class TestBooleanOr:
    def test_one_column_disallowing_null_comparisons(self):
        column = DataColumn.load(
            [1, None, 0]
        )
        result = DataColumn.boolean_or(column)
        expected = DataColumn.load(
            [True, None, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_one_column_allowing_null_comparisons(self):
        column = DataColumn.load(
            [1, None, 0]
        )
        result = DataColumn.boolean_or(
            column,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True, None, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_two_columns_disallowing_null_comparisons(self):
        column1 = DataColumn.load(
            [True,  False, True, False, None, False, None]
        )
        column2 = DataColumn.load(
            [False, False, True, True,  True, None,  None]
        )
        result = DataColumn.boolean_or(
            column1,
            column2
        )
        expected = DataColumn.load(
            [True,  False, True, True,  None, None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_two_columns_allowing_null_comparisons(self):
        column1 = DataColumn.load(
            [True,  False, True, False, None, False, None]
        )
        column2 = DataColumn.load(
            [False, False, True, True,  True, None,  None]
        )
        result = DataColumn.boolean_or(
            column1,
            column2,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True,  False, True, True,  True, None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_three_columns_disallowing_null_comparisons(self):
        column1 = DataColumn.load(
            [True,  False, True, False, None, False, None]
        )
        column2 = DataColumn.load(
            [False, False, True, True,  True, None,  None]
        )
        column3 = DataColumn.load(
            [True,  False,  True, False, None, False, None]
        )
        result = DataColumn.boolean_or(
            column1,
            column2,
            column3
        )
        expected = DataColumn.load(
            [True,  False,  True, True,  None, None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_no_columns(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn.boolean_or()

    def test_only_scalars(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn.boolean_or(
                True,
                False,
            )

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_or(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [False, False],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_or(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [False, False],
                reference_clock_column=monthly_reference_clock,
            ),
        )
        expected = pyarrow.array([True, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_or(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [False, False, True],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.boolean_or(
            DataColumn.load(
                [True, False],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [False, False, True],
                reference_clock_column=daily_master_clock,
            ),
        )
        expected = pyarrow.array([True, False, True])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn.boolean_or(
                DataColumn.load(
                    [True, False, True],
                    reference_clock_column=daily_master_clock,
                ),
                DataColumn.load(
                    [True, True, False],
                    reference_clock_column=other_daily_master_clock,
                ),
            )

    def test_column_against_bool_disallowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = True
        result = DataColumn.boolean_or(
            column,
            scalar
        )
        expected = DataColumn.load(
            [True,  True,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_column_against_bool_allowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = True
        result = DataColumn.boolean_or(
            column,
            scalar,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True,  True,  True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_column_against_scalar_disallowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = pyarrow.scalar(
            True,
            type=pyarrow.bool_()
        )
        result = DataColumn.boolean_or(
            column,
            scalar
        )
        expected = DataColumn.load(
            [True,  True,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_column_against_scalar_allowing_null_comparisons(self):
        column = DataColumn.load(
            [True,  False, None]
        )
        scalar = pyarrow.scalar(
            True,
            type=pyarrow.bool_()
        )
        result = DataColumn.boolean_or(
            column,
            scalar,
            allow_null_comparisons=True
        )
        expected = DataColumn.load(
            [True,  True,  True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )
