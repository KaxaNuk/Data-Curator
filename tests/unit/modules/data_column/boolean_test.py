import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.exceptions import DataColumnParameterError


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
