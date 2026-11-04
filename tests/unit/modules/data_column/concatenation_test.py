import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.exceptions import DataColumnSyncError

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


class TestConcatenate:
    @pytest.mark.parametrize(
        ('first_operand', 'second_operand', 'expected_list'),
        [
            # String column without nulls, then a string
            (
                DataColumn.load(["Hello", "Hey", "Hi"]),
                " World",
                ["Hello World", "Hey World", "Hi World"],
            ),
            # String column with a null, then a string
            (
                DataColumn.load(["Hello", None, "Hi"]),
                " World",
                ["Hello World", " World", "Hi World"],
            ),
            # String, then a string column with a null
            (
                "Hello ",
                DataColumn.load(["Hello", None, "Hi"]),
                ["Hello Hello", "Hello ", "Hello Hi"],
            ),
            # Int column without nulls, then a string
            (
                DataColumn.load([1, 2, 3]),
                " Cats",
                ["1 Cats", "2 Cats", "3 Cats"],
            ),
            # Int column with a null, then a string
            (
                DataColumn.load([1, None, 3]),
                " Cats",
                ["1 Cats", " Cats", "3 Cats"],
            ),
            # Float column without nulls, then a string
            (
                DataColumn.load([1.1, 2.4, 3.6]),
                " Dogs",
                ["1.1 Dogs", "2.4 Dogs", "3.6 Dogs"],
            ),
            # Float column with a null, then a string
            (
                DataColumn.load([1.4, None, 3.6]),
                " Dogs",
                ["1.4 Dogs", " Dogs", "3.6 Dogs"],
            ),
            # Float column with a NaN, then a string
            (
                DataColumn.load([1.1, float('nan'), 3.6]),
                " Dogs",
                ["1.1 Dogs", "nan Dogs", "3.6 Dogs"],
            ),
        ]
    )
    def test_column_and_string(
        self,
        first_operand,
        second_operand,
        expected_list,
    ):
        result = DataColumn.concatenate(
            first_operand,
            second_operand,
        )
        expected = DataColumn.load(expected_list)

        assert DataColumn.fully_equal(
            result,
            expected,
        )

    def test_two_strings(self):
        result = DataColumn.concatenate(
            "Hello ",
            "World",
        )

        assert result.as_py() == "Hello World"

    @pytest.mark.parametrize(
        ('first_operand', 'second_operand', 'expected_list'),
        [
            # Both string columns without nulls
            (
                DataColumn.load(["Hello", "Hey", "Hi"]),
                DataColumn.load([" World", "! How are you", "?"]),
                ["Hello World", "Hey! How are you", "Hi?"],
            ),
            # String column without nulls, then a string column with a null
            (
                DataColumn.load(["Hello", "Hey", "Hi"]),
                DataColumn.load([" World", None, "?"]),
                ["Hello World", "Hey", "Hi?"],
            ),
            # String column with a null, then a string column without nulls
            (
                DataColumn.load(["Hello", None, "Hi"]),
                DataColumn.load([" World", "! How are you", "?"]),
                ["Hello World", "! How are you", "Hi?"],
            ),
            # Both string columns with a null in the same row
            (
                DataColumn.load(["Hello", None, "Hi"]),
                DataColumn.load([" World", None, "?"]),
                ["Hello World", "", "Hi?"],
            ),
        ]
    )
    def test_two_columns(
        self,
        first_operand,
        second_operand,
        expected_list,
    ):
        result = DataColumn.concatenate(
            first_operand,
            second_operand,
        )
        expected = DataColumn.load(expected_list)

        assert DataColumn.fully_equal(
            result,
            expected,
        )

    def test_column_on_reference_clock_and_string_keep_its_clock(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.concatenate(
            DataColumn.load(
                ['a', 'b'],
                reference_clock_column=monthly_reference_clock,
            ),
            '!',
        )

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_keep_it(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.concatenate(
            DataColumn.load(
                ['a', 'b'],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                ['x', 'y'],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result.reference_clock_column is monthly_reference_clock

    def test_columns_on_same_reference_clock_at_its_scale(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn.concatenate(
            DataColumn.load(
                ['a', 'b'],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                ['x', 'y'],
                reference_clock_column=monthly_reference_clock,
            ),
        )
        expected = pyarrow.array(['ax', 'by'])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_clocks_move_to_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.concatenate(
            DataColumn.load(
                ['a', 'b'],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                ['x', 'y', 'z'],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result.reference_clock_column is daily_master_clock

    def test_columns_on_different_clocks_at_master_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn.concatenate(
            DataColumn.load(
                ['a', 'b'],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                ['x', 'y', 'z'],
                reference_clock_column=daily_master_clock,
            ),
        )
        expected = pyarrow.array(['ax', 'by', 'bz'])

        assert result.reference_array.equals(expected)

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn.concatenate(
                DataColumn.load(
                    ['a', 'b', 'c'],
                    reference_clock_column=daily_master_clock,
                ),
                DataColumn.load(
                    ['x', 'y', 'z'],
                    reference_clock_column=other_daily_master_clock,
                ),
            )
