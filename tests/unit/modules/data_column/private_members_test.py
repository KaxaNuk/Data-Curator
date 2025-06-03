from decimal import Decimal

import pyarrow

from kaxanuk.data_curator import DataColumn


class TestPrivateMaskDualArrayNulls:
    def test_mask_dual_array_nulls_both_with_nulls(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'),    None,   Decimal('3.3'), Decimal('4.1'), None,           None,   Decimal('5.5')]
        )
        float_array = pyarrow.array(
            [None,              2.2,    None,           4.4,            float('nan'),   6.6,    float('nan')]
        )
        result = DataColumn._mask_dual_array_nulls(decimal_array, float_array)
        expected = pyarrow.array(
            [True,              True,   True,           False,          True,           True,   True]
        )

        assert result.equals(expected)

    def test_mask_dual_array_nulls_second_with_nulls(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'),    Decimal('2'),   Decimal('3.3'), Decimal('4.1')]
        )
        float_array = pyarrow.array(
            [None,              2.2,            None,           4.4]
        )
        result = DataColumn._mask_dual_array_nulls(decimal_array, float_array)
        expected = pyarrow.array(
            [True,              False,          True,           False]
        )

        assert result.equals(expected)


class TestPrivateMaskZeroes:
    def test_mask_zeroes_decimal(self):
        array = pyarrow.array(
            [Decimal('0'),  Decimal('1'),   Decimal('2'),   Decimal('0'),   Decimal('3'),   Decimal('0')]
        )
        mask = DataColumn._mask_zeroes(
            array.cast(
                pyarrow.float64()
            )
        )
        expected = [True,   False,          False,          True,           False,          True]

        assert mask.tolist() == expected

    def test_mask_zeroes_float(self):
        array = pyarrow.array(
            [0.0,   1.0,    2.0,    0.0,    3.0,    0.0,    0.0000000000001],
            type=pyarrow.float64()
        )
        mask = DataColumn._mask_zeroes(array)
        expected = [True, False, False, True,   False,  True,   False]

        assert mask.tolist() == expected

    def test_mask_zeroes_int(self):
        array = pyarrow.array(
            [0,     1,      2,      0,   3,     0],
            type=pyarrow.int64()
        )
        mask = DataColumn._mask_zeroes(array)
        expected = [True, False, False, True, False, True]

        assert mask.tolist() == expected

    def test_mask_zeroes_with_excessive_decimals(self):
        column1 = pyarrow.array(
            [Decimal('0')],
            type=pyarrow.decimal128(33, 23)
        )
        mask = DataColumn._mask_zeroes(column1)
        result = [True]

        assert DataColumn.fully_equal(
            DataColumn.load(mask),
            DataColumn.load(result),
            equal_nulls=True
        )


# noinspection DuplicatedCode
class TestPrivateReturnNullColumnOnNullOperand:
    def test_return_null_column_on_null_operand_with_null_self_decimal_operand(self):
        null_array = pyarrow.array(
            [None,      None,           None, None],
            type=pyarrow.null()
        )
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        null_column = DataColumn(null_array)
        decimal_column = DataColumn(decimal_array)
        output = null_column._return_null_column_on_null_operand(decimal_column)

        assert output.array.type == pyarrow.null()
        assert len(output.array) == len(null_column.array)

    def test_return_null_column_on_null_operand_with_decimal_self_null_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        null_array = pyarrow.array(
            [None,      None,           None, None],
            type=pyarrow.null()
        )
        decimal_column = DataColumn(decimal_array)
        null_column = DataColumn(null_array)
        output = decimal_column._return_null_column_on_null_operand(null_column)

        assert output.array.type == pyarrow.null()
        assert len(output.array) == len(decimal_column.array)

    def test_return_null_column_on_null_operand_with_null_self_null_operand(self):
        null_array = pyarrow.array(
            [None, None, None, None],
            type=pyarrow.null()
        )
        null_column = DataColumn(null_array)
        null_array = DataColumn(null_array)
        output = null_column._return_null_column_on_null_operand(null_array)

        assert output.array.type == pyarrow.null()
        assert len(output.array) == len(null_column.array)

    def test_return_null_column_on_null_operand_with_decimal_self_none_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        decimal_column = DataColumn(decimal_array)
        output = decimal_column._return_null_column_on_null_operand(None)

        assert output.array.type == pyarrow.null()
        assert len(output.array) == len(decimal_column.array)

    def test_return_null_column_on_null_operand_with_decimal_self_null_scalar_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        null_scalar = pyarrow.scalar(
            None,
            type=pyarrow.null()
        )
        decimal_column = DataColumn(decimal_array)
        output = decimal_column._return_null_column_on_null_operand(null_scalar)

        assert output.array.type == pyarrow.null()
        assert len(output.array) == len(decimal_column.array)


    def test_return_null_column_on_null_operand_with_decimal_self_float_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'),    None,   Decimal('3.3'), Decimal('4.1'), None,           None,   Decimal('5.5')]
        )
        float_array = pyarrow.array(
            [None,              2.2,    None,           4.4,            float('nan'),   6.6,    float('nan')]
        )
        decimal_column = DataColumn(decimal_array)
        float_column = DataColumn(float_array)
        result = decimal_column._return_null_column_on_null_operand(float_column)

        assert result is None

    def test_return_null_column_on_null_operand_with_decimal_self_scalar_int_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), None, Decimal('3.3'), Decimal('4.1'), None, None, Decimal('5.5')]
        )
        decimal_column = DataColumn(decimal_array)
        result = decimal_column._return_null_column_on_null_operand(2)

        assert result is None


class TestPrivateReplaceArrayMaskWithNones:
    def test_replace_array_mask_with_nones_mixed(self):
        array = pyarrow.array(
            [1,     2,      3,      4,      5]
        )
        mask = pyarrow.array(
            [True, False,   True,   False,  True],
        )
        # noinspection PyTypeChecker
        result = DataColumn._replace_array_mask_with_nones(array, mask)
        expected = pyarrow.array(
            [None, 2,       None,   4,      None]
        )

        assert result.equals(expected)

    def test_replace_array_mask_with_nones_with_existing_nulls(self):
        array = pyarrow.array(
            [1,     None, 3,    None,   5]
        )
        mask = pyarrow.array(
            [False, True, True, False,  True]
        )
        # noinspection PyTypeChecker
        result = DataColumn._replace_array_mask_with_nones(array, mask)
        expected = pyarrow.array(
            [1,     None, None, None,   None]
        )

        assert result.equals(expected)

