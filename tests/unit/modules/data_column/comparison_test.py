import pyarrow

from kaxanuk.data_curator import DataColumn


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

    def test_equality_with_mixed_nulls(self):
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

    def test_equality_with_scalar(self):
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

    def test_equality_with_null_column(self):
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


class TestDunderHash:
    def test_hashing_equal_integer_columns_with_nulls(self):
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

    def test_hashing_different_integer_columns_with_nulls(self):
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

    def test_inequality_with_mixed_nulls(self):
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

    def test_inequality_with_scalar(self):
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

    def test_inequality_with_null_column(self):
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

