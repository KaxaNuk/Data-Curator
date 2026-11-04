import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.exceptions import DataColumnSyncError

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
def other_daily_master_clock():
    return clocks.create_daily_master_clock()


class TestDunderEq:
    def test_equal_integer_columns(self):
        column1 = DataColumn.load(
            [1,     2,      3,      4]
        )
        column2 = DataColumn.load(
            [1,     2,      3,      4]
        )
        result = column1 == column2
        expected = DataColumn.load(
            [True,  True,   True,   True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_unequal_integer_columns(self):
        column1 = DataColumn.load(
            [1,     2,      3,      4]
        )
        column2 = DataColumn.load(
            [1,     2,      5,      4]
        )
        result = column1 == column2
        expected = DataColumn.load(
            [True,  True,   False,   True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_mixed_nulls(self):
        column1 = DataColumn.load(
            [1,     None,   None,      4]
        )
        column2 = DataColumn.load(
            [1,     2,      None,   4]
        )
        result = column1 == column2
        expected = DataColumn.load(
            [True,  None,   None,   True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_scalar(self):
        column = DataColumn.load(
            [5, 10, 5, None]
        )
        scalar = 5
        result = column == scalar
        expected = DataColumn.load(
            [True, False, True, None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_null_column(self):
        int_column = DataColumn.load(
            [3, 2, 1]
        )
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = (int_column == null_column)

        assert DataColumn.is_null(result)

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 == column2

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 == column2
        expected = pyarrow.array([True, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 == column2

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 == column2
        expected = pyarrow.array([True, False, False])

        assert result.reference_array.equals(expected)

    def test_column_on_reference_clock_and_scalar_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column == 10

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column1 = DataColumn.load(
            [10, 21, 30],
            reference_clock_column=daily_master_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column1 == column2  # noqa: B015  the comparison raises before returning


class TestDunderHash:
    def test_equal_integer_columns_with_nulls(self):
        base_data = [1, 2, None, 3, 4]
        column1 = DataColumn.load(
            base_data
        )
        column2 = DataColumn.load(
            base_data
        )
        result1 = hash(column1)
        result2 = hash(column2)

        assert result1 == result2

    def test_different_integer_columns_with_nulls(self):
        data1 = [1, 2, None, 3, 4]
        data2 = [1, None, 2, 3, 4]
        column1 = DataColumn.load(
            data1
        )
        column2 = DataColumn.load(
            data2
        )
        result1 = hash(column1)
        result2 = hash(column2)

        assert result1 != result2

    def test_column_on_clock_hashed_at_master_scale(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        master_scale_column = DataColumn.load(
            [2, 3, 4]
        )

        assert hash(column) == hash(master_scale_column)


class TestDunderGe:
    def test_mixed_integer_columns_with_none(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None,   4]
        )
        column2 = DataColumn.load(
            [1,     2,      3,      None,   None]
        )
        result = column1 >= column2
        expected = DataColumn.load(
            [True,  True,   False,  None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_mixed_integer_columns_with_none_against_scalar(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None,   4]
        )
        scalar = 3
        result = column1 >= scalar
        expected = DataColumn.load(
            [False,  True,   False,  None,  True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_null_column(self):
        int_column = DataColumn.load(
            [3, 2, 1]
        )
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column >= null_column

        assert DataColumn.is_null(result)

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 >= column2

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 >= column2
        expected = pyarrow.array([True, True])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 >= column2

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 >= column2
        expected = pyarrow.array([True, True, False])

        assert result.reference_array.equals(expected)

    def test_column_on_reference_clock_and_scalar_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column >= 10

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column1 = DataColumn.load(
            [10, 21, 30],
            reference_clock_column=daily_master_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column1 >= column2  # noqa: B015  the comparison raises before returning


class TestDunderGt:
    def test_mixed_integer_columns_with_none(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None,   4]
        )
        column2 = DataColumn.load(
            [1,     2,      3,      None,   None]
        )
        result = column1 > column2
        expected = DataColumn.load(
            [False, True,   False,  None,  None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_mixed_integer_columns_with_none_against_scalar(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None,   4]
        )
        scalar = 3
        result = column1 > scalar
        expected = DataColumn.load(
            [False,  False,  False,  None,  True]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_null_column(self):
        int_column = DataColumn.load(
            [3, 2, 1]
        )
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column > null_column

        assert DataColumn.is_null(result)

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 > column2

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 > column2
        expected = pyarrow.array([False, True])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 > column2

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 > column2
        expected = pyarrow.array([False, True, False])

        assert result.reference_array.equals(expected)

    def test_column_on_reference_clock_and_scalar_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column > 10

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column1 = DataColumn.load(
            [10, 21, 30],
            reference_clock_column=daily_master_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column1 > column2  # noqa: B015  the comparison raises before returning


class TestDunderLe:
    def test_mixed_integer_columns_with_none(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None, 4]
        )
        column2 = DataColumn.load(
            [1,     2,      3,      None, None]
        )
        result = column1 <= column2
        expected = DataColumn.load(
            [True,  False,  True,   None, None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_mixed_integer_columns_with_none_against_scalar(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None,   4]
        )
        scalar = 3
        result = column1 <= scalar
        expected = DataColumn.load(
            [True,  True,   True,   None,   False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_null_column(self):
        int_column = DataColumn.load(
            [3, 2, 1]
        )
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column <= null_column

        assert DataColumn.is_null(result)

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 <= column2

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 <= column2
        expected = pyarrow.array([True, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 <= column2

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 <= column2
        expected = pyarrow.array([True, False, True])

        assert result.reference_array.equals(expected)

    def test_column_on_reference_clock_and_scalar_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column <= 10

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column1 = DataColumn.load(
            [10, 21, 30],
            reference_clock_column=daily_master_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column1 <= column2  # noqa: B015  the comparison raises before returning


class TestDunderLt:
    def test_mixed_integer_columns_with_none(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None, 4]
        )
        column2 = DataColumn.load(
            [1,     2,      3,      None, None]
        )
        result = column1 < column2
        expected = DataColumn.load(
            [False, False,  True,   None, None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_mixed_integer_columns_with_none_against_scalar(self):
        column1 = DataColumn.load(
            [1,     3,      2,      None,   4]
        )
        scalar = 3
        result = column1 < scalar
        expected = DataColumn.load(
            [True,  False,  True,   None,   False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_null_column(self):
        int_column = DataColumn.load(
            [3, 2, 1]
        )
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column < null_column

        assert DataColumn.is_null(result)

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 < column2

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 < column2
        expected = pyarrow.array([False, False])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 < column2

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 < column2
        expected = pyarrow.array([False, False, True])

        assert result.reference_array.equals(expected)

    def test_column_on_reference_clock_and_scalar_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column < 10

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column1 = DataColumn.load(
            [10, 21, 30],
            reference_clock_column=daily_master_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column1 < column2  # noqa: B015  the comparison raises before returning


class TestDunderNe:
    def test_equal_integer_columns(self):
        column1 = DataColumn.load(
            [1, 2, 3, 4]
        )
        column2 = DataColumn.load(
            [1, 2, 3, 4]
        )
        result = column1 != column2
        expected = DataColumn.load(
            [False, False, False, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_unequal_integer_columns(self):
        column1 = DataColumn.load(
            [1, 2, 3, 4]
        )
        column2 = DataColumn.load(
            [1, 2, 5, 4]
        )
        result = column1 != column2
        expected = DataColumn.load(
            [False, False, True, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_mixed_nulls(self):
        column1 = DataColumn.load(
            [1, None, None, 4]
        )
        column2 = DataColumn.load(
            [1, 2, None, 4]
        )
        result = column1 != column2
        expected = DataColumn.load(
            [False, None, None, False]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_scalar(self):
        column = DataColumn.load(
            [5, 10, 5, None]
        )
        scalar = 5
        result = column != scalar
        expected = DataColumn.load(
            [False, True, False, None]
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_against_null_column(self):
        int_column = DataColumn.load(
            [3, 2, 1]
        )
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column != null_column

        assert DataColumn.is_null(result)

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 != column2

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5],
            reference_clock_column=monthly_reference_clock,
        )
        result = column1 != column2
        expected = pyarrow.array([False, True])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 != column2

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column1 = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=daily_master_clock,
        )
        result = column1 != column2
        expected = pyarrow.array([False, True, True])

        assert result.reference_array.equals(expected)

    def test_column_on_reference_clock_and_scalar_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 21],
            reference_clock_column=monthly_reference_clock,
        )
        result = column != 10

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column1 = DataColumn.load(
            [10, 21, 30],
            reference_clock_column=daily_master_clock,
        )
        column2 = DataColumn.load(
            [10, 5, 30],
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column1 != column2  # noqa: B015  the comparison raises before returning

