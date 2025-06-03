from decimal import Decimal

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn


@pytest.fixture(scope="module")
def example_decimal_arithmetic_operations_with_nones_and_zeroes():
    return {
        'column1': [
            Decimal('3.45'),
            None,
            Decimal('718.24'),
            Decimal('0.00512'),
            Decimal('94'),
            Decimal('0'),
            Decimal('15.248'),
        ],
        'column2': [
            Decimal('1.2'),
            Decimal('14'),
            Decimal('0'),
            Decimal('4'),
            None,
            Decimal('7.4'),
            Decimal('2'),
        ],
        'addition': [
            Decimal('4.65'),
            None,
            Decimal('718.24'),
            Decimal('4.00512'),
            None,
            Decimal('7.4'),
            Decimal('17.248'),
        ],
        'subtraction': [
            Decimal('2.25'),
            None,
            Decimal('718.24'),
            Decimal('-3.99488'),
            None,
            Decimal('-7.4'),
            Decimal('13.248'),
        ],
        'multiplication': [
            Decimal('4.14'),
            None,
            Decimal('0'),
            Decimal('0.02048'),
            None,
            Decimal('0'),
            Decimal('30.496'),
        ],
        # divisions involving decimals should return floats
        'division': [
            2.875,
            None,
            None,
            0.00128,
            None,
            0,
            7.624,
        ],
        'column2floordiv3': [
            Decimal('0'),
            Decimal('4'),
            Decimal('0'),
            Decimal('1'),
            None,
            Decimal('2'),
            Decimal('0'),
        ],
        '3floordivcolumn2': [
            Decimal('2'),
            Decimal('0'),
            None,
            Decimal('0'),
            None,
            Decimal('0'),
            Decimal('1'),
        ],
        'column2modulo3': [
            Decimal('1.2'),
            Decimal('2'),
            Decimal('0'),
            Decimal('1'),
            None,
            Decimal('1.4'),
            Decimal('2'),
        ],
        # divisions involving decimals should return floats
        '3modulocolumn2': [
            0.6,
            3,
            None,
            3,
            None,
            3,
            1,
        ],
    }

@pytest.fixture(scope="module")
def example_float_arithmetic_operations_with_nones_and_zeroes():
    return {
        'column1': [
            3.45,
            None,
            718.24,
            0.00512,
            94,
            0,
            15.248,
        ],
        'column2': [
            1.2,
            14,
            0,
            4,
            None,
            7.4,
            2,
        ],
        'addition': [
            4.65,
            None,
            718.24,
            4.00512,
            None,
            7.4,
            17.248,
        ],
        'subtraction': [
            2.25,
            None,
            718.24,
            -3.99488,
            None,
            -7.4,
            13.248,
        ],
        'multiplication': [
            4.14,
            None,
            0,
            0.02048,
            None,
            0,
            30.496,
        ],
        'division': [
            2.875,
            None,
            None,
            0.00128,
            None,
            0,
            7.624,
        ],
    }


# noinspection DuplicatedCode
class TestDunderAbs:
    ...


# noinspection DuplicatedCode
class TestDunderAdd:
    def test_add_decimal_columns_with_nones(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['addition']
        )
        result = column1 + column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_add_decimal_columns_with_excessive_decimals(self):
        column1 = DataColumn.load(
            pyarrow.array(
                [Decimal('43578457147')],
                type=pyarrow.decimal128(11, 0)
            )
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('16540223841.00000000000000000000000000')],
                type=pyarrow.decimal128(38, 26)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('60118680988.00000000000000000')],
                type=pyarrow.decimal128(30, 12)
            )
        )
        result = column1 + column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,    # float
            Decimal('7.5'), # decimal.Decimal
            pyarrow.scalar(5.11)    # pyarrow.Scalar
        ]
    )
    def test_decimal_column_add_scalar(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = column1 + scalar_array
        result = column1 + scalar

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_add_decimal_and_float_columns(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        example_float_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['addition']
        )
        result = column1 + column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_add_null_column(self):
        int_column = DataColumn.load([3, 2, 1])
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column + null_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderCeil:
    ...


# noinspection DuplicatedCode
class TestDunderFloor:
    ...


# noinspection DuplicatedCode
class TestDunderFloordiv:
    def test_scalar_array_floordiv_decimal_column_with_nones(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        length = len(column)
        scalar = 3
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['3floordivcolumn2']
        )
        result = scalar_array // column

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_floordiv_null_column(self):
        int_column = DataColumn.load([3, 2, 1])
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column // null_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderMod:
    def test_modulo_decimal_columns_with_nones(
            self,
            example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2modulo3']
        )
        length = len(column)
        scalar = 3
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        result = column % scalar_array

        assert DataColumn.fully_equal(
            expected,
            result,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_scalar_modulo_decimal_column_with_nones(
            self,
            example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2modulo3']
        )
        scalar = 3
        result = column % scalar

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )


# noinspection DuplicatedCode
class TestDunderMul:
    def test_multiply_decimal_columns_with_nones(
            self,
            example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['multiplication']
        )
        result = column1 * column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_multiply_decimal_columns_with_excessive_decimals(self):
        column1 = DataColumn.load(
            pyarrow.array(
                [Decimal('1.00000000000000000')],
                type=pyarrow.decimal128(33, 17)
            )
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('1.12E-13')],
                type=pyarrow.decimal128(15, 15)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('1.12E-13')],
                type=pyarrow.decimal128(15, 15)
            )
        )
        result = column1 * column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,    # float
            Decimal('7.5'), # decimal.Decimal
            pyarrow.scalar(5.11)    # pyarrow.Scalar
        ]
    )
    def test_decimal_column_multiply_scalar(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = column1 * scalar_array
        result = column1 * scalar

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_data_column_multiply_decimal_and_float_columns(
            self,
            example_decimal_arithmetic_operations_with_nones_and_zeroes,
            example_float_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['multiplication']
        )
        result = column1 * column2

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_data_column_multiply_by_null_column(self):
        int_column = DataColumn.load([3, 2, 1])
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column * null_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderNeg:
    @pytest.mark.parametrize(
        ('base_list', 'negated_list'),
        [
            (
                [-2, 0, 1, None, 3],
                [2, 0, -1, None, -3]
            ),
            (
                [-2.7, 0, 1.5, None, 31564.23541],
                [2.7, 0, -1.5, None, -31564.23541]
            ),
            (
                [Decimal('-2.7'), Decimal('0'), Decimal('1.5'), None, Decimal('31564.23541')],
                [Decimal('2.7'), Decimal('0'), -Decimal('1.5'), None, -Decimal('31564.23541')]
            ),
        ]
    )
    def test_column_negation(self, base_list, negated_list):
        column = DataColumn.load(base_list)
        result = -column
        expected = DataColumn.load(negated_list)

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )


# noinspection DuplicatedCode
class TestDunderPos:
    def test_pos_not_implemented(self):
        column = DataColumn.load([1, 2, 3])
        with pytest.raises(NotImplementedError):
            return +column


# noinspection DuplicatedCode
class TestDunderPow:
    ...


# noinspection DuplicatedCode
class TestDunderRadd:
    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,    # float
            Decimal('7.5'), # decimal.Decimal
            pyarrow.scalar(5.11)    # pyarrow.Scalar
        ]
    )
    def test_scalar_add_decimal_column(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = scalar_array + column1
        result = scalar + column1

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_add_decimal_columns_with_excessive_decimals(self):
        column1 = pyarrow.scalar(
            Decimal('43578457147'),
            type=pyarrow.decimal128(11, 0)
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('16540223841.00000000000000000000000000')],
                type=pyarrow.decimal128(38, 26)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('60118680988.00000000000000000')],
                type=pyarrow.decimal128(30, 12)
            )
        )
        result = column1 + column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_add_null(self):
        int_column = DataColumn.load([3, 2, 1])
        null_value = pyarrow.scalar(
            None,
            type=pyarrow.null()
        )
        result = null_value + int_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderRfloordiv:
    def test_scalar_array_floordiv_decimal_column_with_nones(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        scalar = 3
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['3floordivcolumn2']
        )
        result = scalar // column

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_floordiv_null_column(self):
        null_scalar = pyarrow.scalar(
            None,
            pyarrow.null()
        )
        int_column = DataColumn.load([3, 2, 1])
        result = null_scalar // int_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderRmod:
    def test_modulo_decimal_column_with_nones_reflected(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['3modulocolumn2']
        )
        scalar = 3
        length = len(column)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        result = scalar_array % column

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_scalar_modulo_decimal_column_with_nones_reflected(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['3modulocolumn2']
        )
        scalar = 3
        result = scalar % column

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )


# noinspection DuplicatedCode
class TestDunderRmul:
    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,  # float
            Decimal('7.5'),  # decimal.Decimal
            pyarrow.scalar(5.11)  # pyarrow.Scalar
        ]
    )
    def test_scalar_multiply_decimal_column(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = scalar_array * column1
        result = scalar * column1

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_multiply_decimal_columns_with_excessive_decimals(self):
        column1 = pyarrow.scalar(
            Decimal('1.00000000000000000'),
            type=pyarrow.decimal128(33, 17)
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('1.12E-13')],
                type=pyarrow.decimal128(15, 15)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('1.12E-13')],
                type=pyarrow.decimal128(15, 15)
            )
        )
        result = column1 * column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_multiply_null(self):
        int_column = DataColumn.load([3, 2, 1])
        null_value = pyarrow.scalar(
            None,
            type=pyarrow.null()
        )
        result = null_value * int_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderRound:
    ...


# noinspection DuplicatedCode
class TestDunderRpow:
    ...


# noinspection DuplicatedCode
class TestDunderRsub:
    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,  # float
            Decimal('7.5'),  # decimal.Decimal
            pyarrow.scalar(5.11)  # pyarrow.Scalar
        ]
    )
    def test_scalar_subtract_decimal_column(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = scalar_array - column1
        result = scalar - column1

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_subtract_decimal_columns_with_excessive_decimals(self):
        column1 = pyarrow.scalar(
            Decimal('43578457147'),
            type=pyarrow.decimal128(11, 0)
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('16540223841.00000000000000000000000000')],
                type=pyarrow.decimal128(38, 26)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('27038233306.00000000000000000')],
                type=pyarrow.decimal128(30, 12)
            )
        )
        result = column1 - column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_subtract_null(self):
        int_column = DataColumn.load([3, 2, 1])
        null_value = pyarrow.scalar(
            None,
            type=pyarrow.null()
        )
        result = null_value - int_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderRtruediv:
    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,  # float
            Decimal('7.5'),  # decimal.Decimal
            pyarrow.scalar(5.11)  # pyarrow.Scalar
        ]
    )
    def test_scalar_divide_decimal_column_with_nones_and_zeroes(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = scalar_array / column1
        result = scalar / column1

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_scalar_divide_decimal_column_without_nones_and_zeroes(
        self,
    ):
        scalar = 4
        column1 = DataColumn.load(
            [1, 2, 4]
        )
        expected = DataColumn.load(
            [4, 2, 1]
        )
        result = scalar / column1

        assert DataColumn.fully_equal(
            result,
            expected,
        )

    def test_divide_decimal_columns_with_excessive_decimals(self):
        column1 = pyarrow.scalar(
            Decimal('321199999.99999988079071044921875'),
            type=pyarrow.decimal128(35, 23)
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('582880000')],
                type=pyarrow.decimal128(9, 0)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [0.5510568213011252],
                type=pyarrow.float64()
            )
        )
        result = column1 / column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_divide_null(self):
        int_column = DataColumn.load([3, 2, 1])
        null_value = pyarrow.scalar(
            None,
            type=pyarrow.null()
        )
        result = null_value / int_column

        assert DataColumn.is_null(
            result
        )

    def test_decimal_divided_by_int_returns_float(self):
        scalar = Decimal('1.5')
        column = DataColumn.load(
            [1, 2, 3]
        )
        result = scalar / column

        assert pyarrow.types.is_floating(result.type)

    def test_int_divided_by_decimal_returns_float(self):
        scalar = 5
        column = DataColumn.load(
            [Decimal('1.5'), Decimal('2.5'), Decimal('3.5')]
        )
        result = scalar / column

        assert pyarrow.types.is_floating(result.type)


# noinspection DuplicatedCode
class TestDunderSub:
    def test_subtract_decimal_columns_with_nones(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['subtraction']
        )
        result = column1 - column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    def test_subtract_decimal_columns_with_excessive_decimals(self):
        column1 = DataColumn.load(
            pyarrow.array(
                [Decimal('43578457147')],
                type=pyarrow.decimal128(11, 0)
            )
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('16540223841.00000000000000000000000000')],
                type=pyarrow.decimal128(38, 26)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('27038233306.00000000000000000')],
                type=pyarrow.decimal128(30, 12)
            )
        )
        result = column1 - column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,  # float
            Decimal('7.5'),  # decimal.Decimal
            pyarrow.scalar(5.11)  # pyarrow.Scalar
        ]
    )
    def test_decimal_column_subtract_scalar(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = column1 - scalar_array
        result = column1 - scalar

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_data_column_subtraction_between_decimal_and_float_columns(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        example_float_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['subtraction']
        )
        result = column1 - column2

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_data_column_subtraction_with_null_column(self):
        int_column = DataColumn.load([3, 2, 1])
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column - null_column

        assert DataColumn.is_null(
            result
        )


# noinspection DuplicatedCode
class TestDunderTruediv:
    def test_divide_decimal_columns_with_nones_and_zeroes(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['division']
        )
        result = column1 / column2

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_divide_decimal_columns_with_excessive_decimals(self):
        column1 = DataColumn.load(
            pyarrow.array(
                [Decimal('321199999.99999988079071044921875')],
                type=pyarrow.decimal128(35, 23)
            )
        )
        column2 = DataColumn.load(
            pyarrow.array(
                [Decimal('582880000')],
                type=pyarrow.decimal128(9, 0)
            )
        )
        expected = DataColumn.load(
            pyarrow.array(
                [Decimal('0.5510568213011252415432172132')],
                type=pyarrow.decimal128(30, 29)
            )
        )
        result = column1 / column2

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True
        )

    @pytest.mark.parametrize(
        'scalar',
        [
            3,  # int
            2.1,    # float
            Decimal('7.5'), # decimal.Decimal
            pyarrow.scalar(5.11)    # pyarrow.Scalar
        ]
    )
    def test_decimal_column_divide_scalar(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        scalar
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        length = len(column1)
        scalar_array = DataColumn.load(
            [scalar] * length
        )
        expected = column1 / scalar_array
        result = column1 / scalar

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_int_divided_by_decimal_scalar_returns_float(self):
        column = DataColumn.load(
            [1, 2, 3]
        )
        scalar = Decimal('1.5')
        result = column / scalar

        assert pyarrow.types.is_floating(result.type)

    def test_data_column_division_between_decimal_and_float_columns(
        self,
        example_decimal_arithmetic_operations_with_nones_and_zeroes,
        example_float_arithmetic_operations_with_nones_and_zeroes
    ):
        column1 = DataColumn.load(
            example_decimal_arithmetic_operations_with_nones_and_zeroes['column1']
        )
        column2 = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['column2']
        )
        expected = DataColumn.load(
            example_float_arithmetic_operations_with_nones_and_zeroes['division']
        )
        result = column1 / column2

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True
        )

    def test_data_column_divide_null_column(self):
        int_column = DataColumn.load([3, 2, 1])
        null_column = DataColumn(
            pyarrow.array(
                [None, None, None],
                pyarrow.null()
            )
        )
        result = int_column / null_column

        assert DataColumn.is_null(
            result
        )

    def test_decimal_divided_by_int_returns_float(self):
        column1 = DataColumn.load(
            [Decimal('1.5'), Decimal('2.5'), Decimal('3.5')]
        )
        column2 = DataColumn.load(
            [1, 2, 3]
        )
        result = column1 / column2

        assert pyarrow.types.is_floating(result.type)

    def test_int_divided_by_decimal_returns_float(self):
        column1 = DataColumn.load(
            [1, 2, 3]
        )
        column2 = DataColumn.load(
            [Decimal('1.5'), Decimal('2.5'), Decimal('3.5')]
        )
        result = column1 / column2

        assert pyarrow.types.is_floating(result.type)


# noinspection DuplicatedCode
class TestDunderTrunc:
    ...
