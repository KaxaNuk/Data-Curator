import decimal
import sys
import typing

import pandas
import pyarrow

from kaxanuk.data_curator.exceptions import DataColumnParameterError


class DataColumn:
    MAX_FLOAT_EPSILON_UNITS_DISCREPANCY = 128   # max epsilon units discrepancy when comparing floats approximately
    def __init__(self, array: pyarrow.Array, /):
        """
        Wrap a pyarrow.Array in this DataColumn.

        Parameters
        ----------
        array
        """
        self.array = array

    def __add__(
        self,
        addend: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise addition operator for DataColumn objects

        Parameters
        ----------
        addend
            The other object to add to the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(addend)

        if null_result is not None:
            return null_result

        if isinstance(addend, DataColumn):
            addend = addend.array

        try:
            result = pyarrow.compute.add_checked(
                self.array,
                addend
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.add_checked(
                    self.array.cast(
                        pyarrow.float64()
                    ),
                    addend
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            self.array,
            addend
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    # noinspection PyShadowingBuiltins
    def __arrow_array__(
        self,
        type: pyarrow.DataType = None   # pragma: no cover
    ) -> pyarrow.Array:
        """
        Implement the __arrow_array__ PyArrow extension protocol.

        Parameters
        ----------
        type
            The data type to use for the returned PyArrow Array.
            If not specified, it will pass through the current array as is.

        Returns
        -------
        The current DataColumn's underlying PyArrow array, cast to the specified type.
        """
        return pyarrow.array(self.array, type=type)

    def __eq__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise equality comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        if isinstance(other, DataColumn):
            other = other.array

        result = pyarrow.compute.equal(self.array, other)

        return DataColumn(result)

    def __floordiv__(
        self,
        divisor: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise floor division operator for DataColumn objects

        Parameters
        ----------
        divisor
            The other object to divide the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(divisor)

        if null_result is not None:
            return null_result

        return DataColumn(
            pyarrow.compute.floor(
                (self / divisor).array
            )
        )

    def __hash__(self) -> int:
        """
        Implement the `hash` function for DataColumn objects.

        This allows DataColumn objects to be used as keys in dictionaries or be added to sets.
        The hash is based on the type and contents of the array.

        Returns
        -------
        The hash value of the DataColumn object's internal PyArrow array.
        """
        hashable_tuple = (
            self.array.type,
            tuple(
                self.array.to_pylist()
            )
        )

        return hash(hashable_tuple)

    def __ge__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'greater than or equal' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        if isinstance(other, DataColumn):
            other = other.array

        result = pyarrow.compute.greater_equal(self.array, other)

        return DataColumn(result)

    def __getitem__(
        self,
        items: slice | int
    ) -> 'DataColumn':
        """
        Implement index retrieval and slicing of the underlying pyarrow.Array.

        Parameters
        ----------
        items
            The slice or index to retrieve.

        Returns
        -------
        The result as a new DataColumn
        """
        if isinstance(items, slice):
            return DataColumn(
                self.array[items]
            )
        else:
            return DataColumn.load([self.array[items]])

    def __gt__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'greater than' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        if isinstance(other, DataColumn):
            other = other.array

        result = pyarrow.compute.greater(self.array, other)

        return DataColumn(result)

    def __le__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'less than or equal' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        if isinstance(other, DataColumn):
            other = other.array

        result = pyarrow.compute.less_equal(self.array, other)

        return DataColumn(result)

    def __lt__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise 'less than' comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        if isinstance(other, DataColumn):
            other = other.array

        result = pyarrow.compute.less(self.array, other)

        return DataColumn(result)

    def __len__(self) -> int:
        """
        Return the length of the underlying pyarrow.Array.

        Returns
        -------
        The length of the underlying pyarrow.Array
        """
        return len(self.array)

    def __mod__(
        self,
        divisor: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise modulo operator for DataColumn objects

        Parameters
        ----------
        divisor
            The other object to divide the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        return (
            self
            - (
                (self // divisor)
                * divisor
            )
        )

    def __mul__(
        self,
        multiplier: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise multiplication operator for DataColumn objects

        Any row involving null returns null.

        Parameters
        ----------
        multiplier
            The other object to multiply the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(multiplier)

        if null_result is not None:
            return null_result

        if isinstance(multiplier, DataColumn):
            multiplier = multiplier.array

        try:
            result = pyarrow.compute.multiply_checked(
                self.array,
                multiplier
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.multiply_checked(
                    self.array.cast(
                        pyarrow.float64()
                    ),
                    multiplier
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            self.array,
            multiplier
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    def __ne__(
        self,
        other: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise inequality comparison for DataColumn objects.

        Parameters
        ----------
        other
            The other object to compare with the current object

        Returns
        -------
        The result as a new DataColumn with boolean values
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(other)

        if null_result is not None:
            return null_result

        if isinstance(other, DataColumn):
            other = other.array

        result = pyarrow.compute.not_equal(self.array, other)

        return DataColumn(result)

    def __neg__(self) -> 'DataColumn':
        """
        Negate the entire contents of the DataColumn

        Returns
        -------
        A new DataColumn
        """
        return DataColumn(
            pyarrow.compute.negate_checked(self.array)
        )

    def __pos__(self):
        """
        Do nothing. Placeholder for possible future functionality

        Raises
        -------
        NotImplementedError
        """
        raise NotImplementedError

    def __radd__(
        self,
        augend: typing.Union[int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise addition operator for DataColumn objects

        Parameters
        ----------
        augend : DataColumn | Int | Float | pyarrow.Scalar
            The other object to which the current object will be added

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(augend)

        if null_result is not None:
            return null_result

        try:
            result = pyarrow.compute.add_checked(
                augend,
                self.array
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.add_checked(
                    augend,
                    self.array.cast(
                        pyarrow.float64()
                    )
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            self.array,
            augend
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    def __rfloordiv__(
        self,
        dividend: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise floor division operator for DataColumn objects

        Parameters
        ----------
        dividend
            The other object to divide by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(dividend)

        if null_result is not None:
            return null_result

        division_array = self.__rtruediv__(dividend).array
        result = pyarrow.compute.floor(division_array)

        return DataColumn(result)

    def __rmod__(
        self,
        dividend: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise modulo operator for DataColumn objects

        Parameters
        ----------
        dividend
            The other object to divide by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        return (
            dividend
            - (
                (dividend // self)
                * self
            )
        )


    def __rmul__(
        self,
        multiplicand: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise multiplication operator for DataColumn objects

        Any row involving null returns null.

        Parameters
        ----------
        multiplicand
            The other object to multiply by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(multiplicand)

        if null_result is not None:
            return null_result

        try:
            result = pyarrow.compute.multiply_checked(
                multiplicand,
                self.array
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.multiply_checked(
                    multiplicand,
                    self.array.cast(
                        pyarrow.float64()
                    )
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(
            self.array,
            multiplicand
        )
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    def __rsub__(
        self,
        minuend: typing.Union[int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement reflected element-wise subtraction operator for DataColumn objects

        Parameters
        ----------
        minuend
            The other object to subtract from the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(minuend)

        if null_result is not None:
            return null_result

        try:
            result = pyarrow.compute.subtract_checked(
                minuend,
                self.array
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.subtract_checked(
                    minuend,
                    self.array.cast(
                        pyarrow.float64()
                    )
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(self.array, minuend)
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    def __rtruediv__(
        self,
        dividend: typing.Union[int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise division operator for DataColumn objects

        Parameters
        ----------
        dividend
            The other object to divide by the current object

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the dividends is a null column or has value of 0, return it
        null_result = self._return_null_column_on_null_operand(dividend)

        if (
            null_result is not None
            or dividend is None
        ):
            return null_result

        if (
            pyarrow.types.is_decimal(self.type)
            or (
                pyarrow.types.is_integer(self.type)
                and isinstance(dividend, decimal.Decimal)
            )
        ):
            divisor = pyarrow.compute.cast(
                self.array,
                pyarrow.float64()
            )
        else:
            divisor = self.array

        divisor_mask = self._mask_zeroes(divisor)
        null_mask = self._mask_dual_array_nulls(
            divisor,
            dividend,
        )

        if pyarrow.compute.any(divisor_mask).as_py():
            clean_divisor = pyarrow.compute.if_else(
                divisor_mask,
                pyarrow.scalar(None, type=self.type),
                divisor
            )
        else:
            clean_divisor = divisor

        result = pyarrow.compute.divide_checked(
            dividend,
            clean_divisor
        )

        if null_mask is not None:
            final_mask = pyarrow.compute.or_kleene(
                null_mask,
                divisor_mask
            )
        else:
            final_mask = divisor_mask

        masked_result = self._replace_array_mask_with_nones(
            result,
            final_mask
        )

        return DataColumn(masked_result)

    def __sub__(
        self,
        subtrahend: typing.Union['DataColumn', int, float, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise subtraction operator for DataColumn objects.

        Parameters
        ----------
        subtrahend
            Other object to subtract from the current object.

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(subtrahend)

        if null_result is not None:
            return null_result

        if isinstance(subtrahend, DataColumn):
            subtrahend = subtrahend.array

        try:
            result = pyarrow.compute.subtract_checked(
                self.array,
                subtrahend
            )
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.subtract_checked(
                    self.array.cast(
                        pyarrow.float64()
                    ),
                    subtrahend
                )
            else:
                raise   # pragma: no cover

        mask = self._mask_dual_array_nulls(self.array, subtrahend)
        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    def __truediv__(
        self,
        divisor: typing.Union['DataColumn', int, float, decimal.Decimal, pyarrow.Scalar],
        /
    ) -> 'DataColumn':
        """
        Implement element-wise division operator for DataColumn objects.

        Divisions involving decimals return a float column, as any initial precision of the involved decimals gets lost
        during the division.

        Parameters
        ----------
        divisor
            The other object to divide the current object by

        Returns
        -------
        The result as a new DataColumn
        """
        # If any of the operands is a null column, return it
        null_result = self._return_null_column_on_null_operand(divisor)

        if null_result is not None:
            return null_result

        if isinstance(divisor, DataColumn):
            if pyarrow.types.is_decimal(divisor.type):
                divisor = divisor.array.cast(
                    pyarrow.float64()
                )
            else:
                divisor = divisor.array

            divisor_mask = self._mask_zeroes(divisor)
        else:
            divisor_mask = None

        null_mask = self._mask_dual_array_nulls(
            self.array,
            divisor
        )

        if (
            null_mask is not None
            and divisor_mask is not None
        ):
            mask = pyarrow.compute.or_kleene(null_mask, divisor_mask)
        elif divisor_mask is not None:
            mask = divisor_mask
        else:
            mask = None

        if (
            divisor_mask is not None
            and pyarrow.compute.any(divisor_mask).as_py()
        ):
            masked_divisor = pyarrow.compute.if_else(
                divisor_mask,
                pyarrow.scalar(None, type=divisor.type),
                divisor
            )
        else:
            masked_divisor = divisor

        if (
            pyarrow.types.is_decimal(self.type)
            or (
                pyarrow.types.is_integer(self.type)
                and isinstance(divisor, decimal.Decimal)
            )
        ):
            dividend = self.array.cast(
                pyarrow.float64()
            )
        else:
            dividend = self.array

        result = pyarrow.compute.divide_checked(
            dividend,
            masked_divisor
        )

        masked_result = self._replace_array_mask_with_nones(
            result,
            mask
        )

        return DataColumn(masked_result)

    @classmethod
    def boolean_and(
        cls,
        *columns: typing.Union['DataColumn', pyarrow.Scalar, bool],
        allow_null_comparisons: bool = False
    ) -> 'DataColumn':
        """
        Perform a logical AND comparison on multiple DataColumns.

        Parameters
        ----------
        *columns
            The columns to be combined with boolean AND logic.
        allow_null_comparisons
            Whether to allow null comparisons with Kleene logic. Default is False, which outputs null on any row
            containing any null value.

        Returns
        -------
        A new DataColumn containing the result of the logical AND comparison.
        """
        if len(columns) < 1:
            raise DataColumnParameterError(
                "DataColumn.boolean_and() requires at least one parameter"
            )

        recasted_columns = [
            column.to_pyarrow().cast(
                pyarrow.bool_()
            )
                if isinstance(column, DataColumn)
                else column
            for column
            in columns
        ]

        if allow_null_comparisons:
            and_function = pyarrow.compute.and_kleene
        else:
            and_function = pyarrow.compute.and_

        result = recasted_columns[0]
        if len(recasted_columns) > 1:
            for column in recasted_columns[1:]:
                result = and_function(result, column)

        return DataColumn.load(result)

    @classmethod
    def boolean_or(
        cls,
        *columns: typing.Union['DataColumn', pyarrow.Scalar, bool],
        allow_null_comparisons: bool = False
    ) -> 'DataColumn':
        """
        Perform a logical OR comparison on multiple DataColumns.

        Parameters
        ----------
        *columns
            The columns to be combined with boolean OR logic.
        allow_null_comparisons
            Whether to allow null comparisons with Kleene logic. Default is False, which outputs null on any row
            containing any null value.

        Returns
        -------
        A new DataColumn containing the result of the logical OR comparison.
        """
        if len(columns) < 1:
            raise DataColumnParameterError(
                "DataColumn.boolean_or() requires at least one parameter"
            )

        recasted_columns = [
            column.to_pyarrow().cast(
                pyarrow.bool_()
            )
                if isinstance(column, DataColumn)
                else column
            for column
            in columns
        ]

        if allow_null_comparisons:
            or_function = pyarrow.compute.or_kleene
        else:
            or_function = pyarrow.compute.or_

        result = recasted_columns[0]
        if len(recasted_columns) > 1:
            for column in recasted_columns[1:]:
                result = or_function(result, column)

        return DataColumn.load(result)

    @classmethod
    def concatenate(
        cls,
        *columns: typing.Union['DataColumn', pyarrow.Scalar, str],
        null_replacement: str = "",
        separator: str = "",
    ) -> typing.Union['DataColumn', pyarrow.Scalar]:
        """
        Concatenate DataColumns into one DataColumn.

        Parameters
        ----------
        *columns : 'DataColumn' | pyarrow.Scalar | str
            The columns to be concatenated. Each column can be either a 'DataColumn' object, a pyarrow.Scalar,
            or a string.

        null_replacement : str, optional
            The value to be used as replacement for null values in the concatenated result.
            Defaults to an empty string.

        separator : str, optional
            The separator to be used between concatenated values.
            Defaults to an empty string.

        Returns
        -------
        DataColumn | pyarrow.Scalar
            A new DataColumn containing the concatenated rows of the input columns, or a pyarrow.Scalar if all
            columns were strings or scalars.
        """
        recasted_columns = (
            column.to_pyarrow().cast(pyarrow.string()) if isinstance(column, DataColumn)
                else column
            for column
            in columns
        )
        concatenation = pyarrow.compute.binary_join_element_wise(
            *recasted_columns,
            separator,
            null_handling='replace',
            null_replacement=null_replacement
        )

        if isinstance(concatenation, pyarrow.Array):
            return DataColumn.load(concatenation)
        else:
            return concatenation

    @classmethod
    def equal(
        cls,
        column1: 'DataColumn',
        column2: 'DataColumn',
        /,
        *,
        approximate_floats: bool = False,
        equal_nulls: bool = False,
    ) -> 'DataColumn':
        """
        Compare two DataColumns element-wise.

        Parameters
        ----------
        column1
            The first column to compare.
        column2
            The second column to compare.
        equal_nulls
            Specifies whether null values should be considered equal. Default is False.
        approximate_floats
            Specifies whether floating-point value equality should compensate for rounding errors.
            Default is False.

        Returns
        -------
        A DataColumn containing a pyarrow.BooleanArray indicating element-wise equality between the two columns.
        """
        if (
            column1.is_null()
            and column2.is_null()
        ):
            if equal_nulls:
                return DataColumn.load(
                    column1.to_pyarrow().is_null()  # all True
                )
            else:
                return DataColumn.load(
                    column1.to_pyarrow().is_valid()  # all False
                )

        if approximate_floats:
            # based on https://stackoverflow.com/a/32334103/5220723
            difference = pyarrow.compute.abs_checked(
                (column1 - column2).to_pyarrow()
            )
            norm = (
                DataColumn.load(
                    pyarrow.compute.abs_checked(column1.to_pyarrow())
                )
                + DataColumn.load(
                    pyarrow.compute.abs_checked(column2.to_pyarrow())
                )
            )
            base_equality = pyarrow.compute.less_equal(
                difference,
                (
                    norm
                    * (
                        cls.MAX_FLOAT_EPSILON_UNITS_DISCREPANCY
                        * sys.float_info.epsilon
                    )
                ).to_pyarrow()
            )
        else:
            base_equality = pyarrow.compute.equal(
                column1.to_pyarrow(),
                column2.to_pyarrow()
            )

        if (
            not equal_nulls
            or (
                column1.to_pyarrow().null_count < 1
                and column2.to_pyarrow().null_count < 1
            )
        ):
            return DataColumn.load(base_equality)

        else:
            nulls1 = pyarrow.compute.is_null(column1.to_pyarrow(), nan_is_null=True)
            nulls2 = pyarrow.compute.is_null(column2.to_pyarrow(), nan_is_null=True)
            result = pyarrow.compute.if_else(
                pyarrow.compute.is_null(base_equality, nan_is_null=True),
                pyarrow.compute.and_(nulls1, nulls2),
                base_equality
            )

            return DataColumn.load(result)

    @classmethod
    def fully_equal(
        cls,
        column1: 'DataColumn',
        column2: 'DataColumn',
        /,
        *,
        approximate_floats: bool = False,
        equal_nulls: bool = False,
        skip_nulls: bool = False
    ) -> bool | None:
        """
        Check if two DataColumns are fully equal.

        Parameters
        ----------
        column1
            The first column to compare.

        column2
            The second column to compare.

        approximate_floats: bool, optional
            Whether to consider floats as approximately equal. If True, floating-point comparison will use tolerance.
            If not specified, the default value is False.

        equal_nulls: bool, optional
            Whether to consider null values as equal. If True, null values will be treated as equal.
            If not specified, the default value is False.

        skip_nulls: bool | None, optional
            Whether to skip null values during comparison. If True, null values will be ignored.
            If not specified, the default value is False.

        Returns
        -------
        bool
            Returns None if equal_nulls is False and there are Nones, True if both columns are equal, False otherwise.
        """
        element_wise_equalities = cls.equal(
            column1,
            column2,
            approximate_floats=approximate_floats,
            equal_nulls=equal_nulls,
        )

        return (
            pyarrow.compute.all(
                element_wise_equalities.to_pyarrow(),
                skip_nulls=skip_nulls
            )
            .as_py()
        )

    def is_null(self) -> bool:
        """
        Check if the underlying pyarrow.Array is NullArray.

        Returns
        -------
        bool :
            whether or not the underlying pyarrow.Array is a NullArray
        """
        return self.array.type == pyarrow.null()

    @classmethod
    def load(
        cls,
        data: typing.Iterable | 'DataColumn',
        dtype=None
    ) -> 'DataColumn':
        """
        Wrap data (pyarrow.Array, pandas.Series, Iterable) in a new DataColumn object.

        Parameters
        ----------
        data : typing.Iterable | 'DataColumn'
            the data to be wrapped
        dtype : pyarroy.DataType
            the type of the underlying pyarrow.Array

        Returns
        -------
        DataColumn
        """
        if (
            isinstance(data, DataColumn)
            and dtype is None
        ):
            return data

        return DataColumn(
            pyarrow.array(
                data,
                from_pandas=(
                    isinstance(data, pandas.Series)
                    and data.hasnans
                ),
                type=dtype
            )
        )

    def to_pandas(self) -> pandas.Series:
        """
        Force pandas to use PyArrow in the backend by means of ArrowExtensionArray.

        Cf. https://pandas.pydata.org/docs/user_guide/pyarrow.html

        Returns
        -------
        pandas.Series
        """
        return pandas.Series(
            # @todo remove the mypy ignore comment below once pandas has fixed its own stubs
            pandas.arrays.ArrowExtensionArray(self.array)   # type: ignore[attr-defined]
        )

    def to_pyarrow(self) -> pyarrow.Array:
        """
        Return the underlying native pyarrow.array object.

        Returns
        -------
        pyarrow.Array
        """
        return self.array

    @property
    def type(self) -> pyarrow.DataType:
        """
        Return the underlying native pyarrow.array object type.

        Returns
        -------
        pyarrow.DataType
        """
        return self.array.type

    @staticmethod
    def _mask_dual_array_nulls(
        array1: pyarrow.Array,
        array2: pyarrow.Array | typing.Any
    ) -> pyarrow.BooleanArray:
        """
        Create a mask array with true on the rows where any of the 2 input arrays are null or nan.

        Parameters
        ----------
        array1
        array2

        Returns
        -------
        pyarrow.BooleanArray
        """
        # @todo: fix this horrible internal API method
        if (
            array1.null_count > 0
            and (
                isinstance(array2, pyarrow.Array)
                and array2.null_count > 0
            )
        ):
            mask1 = pyarrow.compute.is_null(array1, nan_is_null=True)
            mask2 = pyarrow.compute.is_null(array2, nan_is_null=True)
            mask = pyarrow.compute.or_kleene(mask1, mask2)
        elif array1.null_count > 0:
            mask = pyarrow.compute.is_null(array1, nan_is_null=True)
        elif (
            isinstance(array2, pyarrow.Array)
            and array2.null_count > 0
        ):
            mask = pyarrow.compute.is_null(array2, nan_is_null=True)
        else:
            mask = None
        return mask

    @staticmethod
    def _mask_zeroes(array: pyarrow.Array) -> pyarrow.BooleanArray:
        """
        Create a mask array with true on the rows where the array is 0.

        Parameters
        ----------
        array : pyarrow.Array

        Returns
        -------
        pyarrow.BooleanArray
        """
        try:
            result = pyarrow.compute.equal(array, 0)
        except pyarrow.lib.ArrowInvalid as error:
            if 'Decimal precision out of range' in str(error):
                result = pyarrow.compute.equal(
                    array.cast(
                        pyarrow.float64()
                    ),
                    0
                )
            else:
                raise   # pragma: no cover

        return result

    @staticmethod
    def _replace_array_mask_with_nones(
        array: pyarrow.Array,
        mask: pyarrow.BooleanArray | None,
    ) -> pyarrow.Array:
        """
        Replace the values in the array with None where the mask is True.

        Parameters
        ----------
        array
            The array to be modified.
        mask
            The mask indicating which values to replace with None.

        Returns
        -------
        The modified array with None values where the mask is True.
        """
        if mask is None:
            return array

        return pyarrow.compute.if_else(
            mask,
            pyarrow.scalar(None, type=array.type),
            array
        )

    def _return_null_column_on_null_operand(
        self,
        operand
    ) -> typing.Union['DataColumn', None]:
        """
        Return a null column if self.array is a null column or the operand is a null column or scalar, None otherwise.

        Parameters
        ----------
        operand : DataColumn | Int | Float | pyarrow.Scalar

        Returns
        -------
        DataColumn | None
        """
        if (
            self.array.type == pyarrow.null()
        ):
            return self
        elif (
            isinstance(operand, DataColumn)
            and operand.array.type == pyarrow.null()
        ):
            return operand
        elif (
            not isinstance(operand, DataColumn)
            and (
                operand is None
                or getattr(operand, 'type', None) == pyarrow.null()
            )
        ):
            return DataColumn.load(
                pyarrow.array(
                    [None] * len(self)
                )
            )
        else:
            return None
