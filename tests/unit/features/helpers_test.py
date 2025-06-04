from decimal import Decimal

import pytest
from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.exceptions import CalculationHelperError
from kaxanuk.data_curator.features import helpers


@pytest.fixture(scope="module")
def example_rolling_window_operations():
    return {
        'key_column': DataColumn.load([
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            'A1',
            'A1',
            'A1',
            'A2',
            'A2',
            'A2',
            'A3',
            'A3',
            'A3',
            None,
            None,
            None,
            'A4',
            'A4',
            'A4',
            'A5',
            'A5',
            'A5',
            'A6',
            'A6',
            'A6',
            'A7',
            'A7',
            'A7',
        ]),
        'value_column': DataColumn.load([
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Decimal('5'),
            Decimal('5'),
            Decimal('5'),
            Decimal('6'),
            Decimal('6'),
            Decimal('6'),
            Decimal('5'),
            Decimal('5'),
            Decimal('5'),
            None,
            None,
            None,
            Decimal('2'),
            Decimal('2'),
            Decimal('2'),
            Decimal('5'),
            Decimal('5'),
            Decimal('5'),
            Decimal('6'),
            Decimal('6'),
            Decimal('6'),
            Decimal('7'),
            Decimal('7'),
            Decimal('7'),
        ]),
        'sum_window_2': DataColumn.load([
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            None,
            None,
            None,
            None,
            None,
            None,
            Decimal('7'),
            Decimal('7'),
            Decimal('7'),
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            Decimal('13'),
            Decimal('13'),
            Decimal('13'),
        ])
    }
@pytest.fixture(scope="module")
def example_infinite_operations():
    return{
        "column": DataColumn.load([
            None,
            None,
            float('-inf'),
            None,
            float('-inf'),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            float('-inf'),
            float('-inf'),
            float('-inf'),
            None,
            None,
            None,
            float('-inf'),
            float('-inf'),
            None,
            float('-inf'),
            float('-inf'),
        ]),
        "expected_result": DataColumn.load([
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ])
    }

def test_replace_infinite_with_none_test(example_infinite_operations):
    column = example_infinite_operations["column"]
    expected_result = example_infinite_operations["expected_result"]
    assert DataColumn.fully_equal(
        helpers.replace_infinite_with_none(column),
        expected_result, equal_nulls=True
    )

def test_replace_infinite_with_none_all_finite():
    finite_values = [1, 2, 3, 4]
    column = DataColumn.load(finite_values)
    result = helpers.replace_infinite_with_none(column)
    expected = column.to_pyarrow()
    assert result.equals(expected)


class TestIndexedRollingWindowOperation:
    def test_sum_window_2(self, example_rolling_window_operations):
        key_column = example_rolling_window_operations['key_column']
        value_column = example_rolling_window_operations['value_column']
        expected_result = example_rolling_window_operations['sum_window_2']

        assert DataColumn.fully_equal(
            helpers.indexed_rolling_window_operation(
                key_column=key_column,
                value_column=value_column,
                operation_function=sum,
                window_length=2
            ),
            expected_result,
            equal_nulls=True
        )

    # noinspection PyTypeChecker
    def test_incorrect_param_key_column(self, example_rolling_window_operations):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column={'some_key': 'some_value'},
                value_column=example_rolling_window_operations['value_column'],
                operation_function=sum,
                window_length=2
            )

    # noinspection PyTypeChecker
    def test_incorrect_param_value_column(self, example_rolling_window_operations):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column=example_rolling_window_operations['key_column'],
                value_column={'some_key': 'some_value'},
                operation_function=sum,
                window_length=2
            )

    # noinspection PyTypeChecker
    def test_incorrect_param_operation_function(self, example_rolling_window_operations):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column=example_rolling_window_operations['key_column'],
                value_column=example_rolling_window_operations['value_column'],
                operation_function="some string that's not a function",
                window_length=2
            )

    # noinspection PyTypeChecker
    def test_incorrect_param_window_lngth(self, example_rolling_window_operations):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column=example_rolling_window_operations['key_column'],
                value_column=example_rolling_window_operations['value_column'],
                operation_function=sum,
                window_length="some string that's not an int"
            )

@pytest.mark.parametrize(("column", "days", "expected_msg"), [
    # Case: 'column' is not a DataColumn
    ("not_a_DataColumn", 5, "features.helpers.annualized_volatility() column parameter must be a DataColumn object"),
    # Cases: 'days' is not a positive integer
    (DataColumn.load([Decimal('1'), Decimal('2'), Decimal('3')]), 0,
     "features.helpers.annualized_volatility() days parameter must be a positive integer"),
    (DataColumn.load([Decimal('1'), Decimal('2'), Decimal('3')]), -1,
     "features.helpers.annualized_volatility() days parameter must be a positive integer"),
    (DataColumn.load([Decimal('1'), Decimal('2'), Decimal('3')]), "5",
     "features.helpers.annualized_volatility() days parameter must be a positive integer")
])


def test_annualized_volatility_invalid_parameters(column, days, expected_msg):
    with pytest.raises(CalculationHelperError) as exc_info:
        helpers.annualized_volatility(column=column, days=days)
    assert expected_msg in str(exc_info.value)

@pytest.mark.parametrize(("column", "days", "expected_msg"), [
    # Case: 'column' is not a DataColumn
    ("not_a_DataColumn", 10, "column parameter must be a DataColumn object"),
    # Case: 'days' is not an integer (e.g., a string)
    (DataColumn.load([1, 2, 3, 4]), "10", "days parameter must be a positive integer"),
    # Case: 'days' is not positive (zero)
    (DataColumn.load([1, 2, 3, 4]), 0, "days parameter must be a positive integer"),
])

def test_exponential_moving_average_invalid_parameters(column, days, expected_msg):
    with pytest.raises(CalculationHelperError) as exc_info:
        helpers.exponential_moving_average(column=column, days=days)
    assert expected_msg in str(exc_info.value)


