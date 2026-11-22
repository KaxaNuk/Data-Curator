import datetime
import math
from decimal import Decimal

import pyarrow
import pytest
from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import ReferencePeriods
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.entities import ClockSpec
from kaxanuk.data_curator.exceptions import CalculationHelperError
from kaxanuk.data_curator.features import helpers


@pytest.fixture
def daily_master_clock():
    # The trading days from Monday 2024-01-01 to Friday 2024-01-05, without Wednesday 2024-01-03
    return DataColumn.load(
        [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 4),
            datetime.date(2024, 1, 5),
        ],
        clock_master=True,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.DAY,
    )


@pytest.fixture
def dividend_clock(daily_master_clock):
    # Three ex-dividend dates: before the master clock, on the missing Wednesday, and on Thursday
    return DataColumn.load(
        [
            datetime.date(2023, 12, 31),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
        ],
        clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
        clock_master=daily_master_clock,
        clock_periods_per_row=0,
        clock_reference_period=ReferencePeriods.DAY,
        consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
    )


@pytest.fixture
def quarterly_clock(daily_master_clock):
    # The filing dates of six quarterly financial statements
    return DataColumn.load(
        [
            datetime.date(2022, 2, 1),
            datetime.date(2022, 5, 1),
            datetime.date(2022, 8, 1),
            datetime.date(2022, 11, 1),
            datetime.date(2023, 2, 1),
            datetime.date(2023, 5, 1),
        ],
        clock_master=daily_master_clock,
        clock_periods_per_row=3,
        clock_reference_period=ReferencePeriods.MONTH,
        clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
    )


@pytest.fixture
def twelve_months():
    return ClockSpec(
        reference_period=ReferencePeriods.MONTH,
        periods_per_row=12,
    )


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


class TestReplaceInfiniteWithNone:
    def test_with_infinite_values(
        self,
        example_infinite_operations,
    ):
        column = example_infinite_operations["column"]
        expected_result = example_infinite_operations["expected_result"]
        assert DataColumn.fully_equal(
            helpers.replace_infinite_with_none(column),
            expected_result,
            equal_nulls=True
        )

    def test_all_finite(self):
        finite_values = [1, 2, 3, 4]
        column = DataColumn.load(finite_values)
        result = helpers.replace_infinite_with_none(column)
        assert DataColumn.fully_equal(
            result,
            column,
        )

    def test_on_reference_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [1.0, float('inf'), 3.0, 4.0, 5.0, 6.0],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.replace_infinite_with_none(column)

        assert result.reference_clock_column is quarterly_clock


class TestIndexedRollingWindowOperation:
    def test_deprecated(self, example_rolling_window_operations):
        with pytest.warns(
            DeprecationWarning,
            match='indexed_rolling_window_operation',
        ):
            helpers.indexed_rolling_window_operation(
                key_column=example_rolling_window_operations['key_column'],
                value_column=example_rolling_window_operations['value_column'],
                operation_function=sum,
                window_length=2
            )

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
    def test_incorrect_param_window_length(self, example_rolling_window_operations):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column=example_rolling_window_operations['key_column'],
                value_column=example_rolling_window_operations['value_column'],
                operation_function=sum,
                window_length="some string that's not an int"
            )

    def test_key_repeated_in_separate_later_block(self):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column=DataColumn.load(['a', 'a', 'b', 'b', 'a', 'c']),
                value_column=DataColumn.load([1.0, 1.0, 2.0, 2.0, 3.0, 4.0]),
                operation_function=sum,
                window_length=2
            )

    def test_multiple_keys_repeated_in_separate_later_blocks(self):
        with pytest.raises(CalculationHelperError):
            helpers.indexed_rolling_window_operation(
                key_column=DataColumn.load(['a', 'b', 'a', 'b', 'c']),
                value_column=DataColumn.load([1.0, 2.0, 1.0, 2.0, 3.0]),
                operation_function=sum,
                window_length=2
            )


class TestAnnualizedVolatility:
    @pytest.mark.parametrize(("column", "days", "expected_msg"), [
        # Case: 'column' is not a DataColumn
        (
            "not_a_DataColumn",
            5,
            "features.helpers.annualized_volatility() column parameter must be a DataColumn object",
        ),
        # Cases: 'days' is not a positive integer
        (DataColumn.load([Decimal('1'), Decimal('2'), Decimal('3')]), 0,
         "features.helpers.annualized_volatility() days parameter must be a positive integer"),
        (DataColumn.load([Decimal('1'), Decimal('2'), Decimal('3')]), -1,
         "features.helpers.annualized_volatility() days parameter must be a positive integer"),
        (DataColumn.load([Decimal('1'), Decimal('2'), Decimal('3')]), "5",
         "features.helpers.annualized_volatility() days parameter must be a positive integer")
    ])
    def test_invalid_parameters(
        self,
        column,
        days,
        expected_msg,
    ):
        with pytest.raises(CalculationHelperError) as exc_info:
            helpers.annualized_volatility(column=column, days=days)
        assert expected_msg in str(exc_info.value)


class TestExponentialMovingAverage:
    # @todo: don't check for hardcode error message texts
    @pytest.mark.parametrize(("column", "days", "expected_msg"), [
        # Case: 'column' is not a DataColumn
        ("not_a_DataColumn", 10, "column parameter must be a DataColumn object"),
        # Case: 'days' is not an integer (e.g., a string)
        (DataColumn.load([1, 2, 3, 4]), "10", "days parameter must be a positive integer"),
        # Case: 'days' is not positive (zero)
        (DataColumn.load([1, 2, 3, 4]), 0, "days parameter must be a positive integer"),
    ])
    def test_invalid_parameters(
        self,
        column,
        days,
        expected_msg,
    ):
        with pytest.raises(CalculationHelperError) as exc_info:
            helpers.exponential_moving_average(column=column, days=days)
        assert expected_msg in str(exc_info.value)

    def test_seeded_with_simple_moving_average(self):
        # The smoothing factor of 2 days is 2 / 3, after starting from the average of the first 2 rows
        result = helpers.exponential_moving_average(
            column=DataColumn.load([1.0, 2.0, 3.0, 4.0]),
            days=2,
        )
        expected = DataColumn.load([None, 1.5, 2.5, 3.5])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_restarts_after_null(self):
        result = helpers.exponential_moving_average(
            column=DataColumn.load([1.0, 2.0, None, 3.0, 5.0, 7.0]),
            days=2,
        )
        expected = DataColumn.load([None, 1.5, None, None, 4.0, 6.0])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_on_reference_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.exponential_moving_average(
            column=column,
            days=2,
        )

        assert result.reference_clock_column is quarterly_clock


class TestChaikinMoneyFlow:
    def test_window_sums(self):
        # The money flow volumes are 0, 20 and -30, for volumes of 10, 20 and 30
        result = helpers.chaikin_money_flow(
            high=DataColumn.load([3.0, 4.0, 5.0]),
            low=DataColumn.load([1.0, 2.0, 3.0]),
            close=DataColumn.load([2.0, 4.0, 3.0]),
            volume=DataColumn.load([10, 20, 30]),
            days=2,
        )
        expected = DataColumn.load([None, 20 / 30, -10 / 50])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_row_without_price_range(self):
        # The second row has no price range, so neither window holding it has a money flow
        result = helpers.chaikin_money_flow(
            high=DataColumn.load([3.0, 4.0, 5.0]),
            low=DataColumn.load([1.0, 4.0, 3.0]),
            close=DataColumn.load([2.0, 4.0, 3.0]),
            volume=DataColumn.load([10, 20, 30]),
            days=2,
        )
        expected = DataColumn.load(
            pyarrow.nulls(
                3,
                type=pyarrow.float64(),
            )
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_on_shared_reference_clock(
        self,
        quarterly_clock,
    ):
        prices = DataColumn.load(
            [3.0, 4.0, 5.0, 4.0, 3.0, 4.0],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.chaikin_money_flow(
            high=prices + 1,
            low=prices - 1,
            close=prices,
            volume=DataColumn.load(
                [10, 20, 30, 40, 50, 60],
                reference_clock_column=quarterly_clock,
            ),
            days=2,
        )

        assert result.reference_clock_column is quarterly_clock

    def test_on_different_reference_clocks(
        self,
        daily_master_clock,
        quarterly_clock,
    ):
        prices = DataColumn.load(
            [3.0, 4.0, 5.0, 4.0],
            reference_clock_column=daily_master_clock,
        )
        result = helpers.chaikin_money_flow(
            high=prices + 1,
            low=prices - 1,
            close=prices,
            volume=DataColumn.load(
                [10, 20, 30, 40, 50, 60],
                reference_clock_column=quarterly_clock,
            ),
            days=2,
        )

        assert result.reference_clock_column is daily_master_clock

    def test_on_different_master_clocks(
        self,
        daily_master_clock,
    ):
        other_master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 4),
                datetime.date(2024, 1, 5),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        prices = DataColumn.load(
            [3.0, 4.0, 5.0, 4.0],
            reference_clock_column=daily_master_clock,
        )

        with pytest.raises(CalculationHelperError):
            helpers.chaikin_money_flow(
                high=prices + 1,
                low=prices - 1,
                close=prices,
                volume=DataColumn.load(
                    [10, 20, 30, 40],
                    reference_clock_column=other_master_clock,
                ),
                days=2,
            )


class TestLogReturns:
    def test_returns(self):
        result = helpers.log_returns(
            DataColumn.load([1.0, math.e, None, 1.0])
        )
        expected = DataColumn.load([None, 1.0, None, None])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_previous_row_zero(self):
        result = helpers.log_returns(
            DataColumn.load([1.0, 0.0, 2.0])
        )
        expected = DataColumn.load([None, float('-inf'), None])

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_on_reference_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.log_returns(column)

        assert result.reference_clock_column is quarterly_clock


class TestRelativeStrengthIndex:
    def test_wilder_smoothing(self):
        # The gains are 1, 0 and 2, and the losses 0, 1 and 0, with average gains of 0.5 and 1.25, and average losses
        # of 0.5 and 0.25, for a smoothing factor of 1 / 2
        result = helpers.relative_strength_index(
            column=DataColumn.load([1.0, 2.0, 1.0, 3.0]),
            days=2,
        )
        expected = DataColumn.load([None, None, 50.0, 100 - 100 / 6])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_without_losses(self):
        result = helpers.relative_strength_index(
            column=DataColumn.load([1.0, 2.0, 3.0]),
            days=2,
        )
        expected = DataColumn.load(
            pyarrow.nulls(
                3,
                type=pyarrow.float64(),
            )
        )

        assert DataColumn.fully_equal(
            result,
            expected,
            equal_nulls=True,
        )

    def test_on_reference_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [1.0, 2.0, 1.0, 3.0, 2.0, 4.0],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.relative_strength_index(
            column=column,
            days=2,
        )

        assert result.reference_clock_column is quarterly_clock


class TestRollingSum:
    def test_quarterly_rows(
        self,
        quarterly_clock,
        twelve_months,
    ):
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_array.to_pylist() == [None, None, None, Decimal('10'), Decimal('14'), Decimal('18')]

    def test_half_yearly_rows(
        self,
        daily_master_clock,
        twelve_months,
    ):
        half_yearly_clock = DataColumn.load(
            [
                datetime.date(2022, 8, 1),
                datetime.date(2023, 2, 1),
                datetime.date(2023, 8, 1),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=6,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
        )
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3')],
            reference_clock_column=half_yearly_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_array.to_pylist() == [None, Decimal('3'), Decimal('5')]

    def test_annual_rows(
        self,
        daily_master_clock,
        twelve_months,
    ):
        annual_clock = DataColumn.load(
            [
                datetime.date(2022, 2, 1),
                datetime.date(2023, 2, 1),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.YEAR,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
        )
        column = DataColumn.load(
            [Decimal('1'), Decimal('2')],
            reference_clock_column=annual_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_array.to_pylist() == [Decimal('1'), Decimal('2')]

    def test_rows_longer_than_window(
        self,
        daily_master_clock,
    ):
        annual_clock = DataColumn.load(
            [
                datetime.date(2022, 2, 1),
                datetime.date(2023, 2, 1),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.YEAR,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
        )
        column = DataColumn.load(
            [Decimal('1'), Decimal('2')],
            reference_clock_column=annual_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=ClockSpec(
                reference_period=ReferencePeriods.MONTH,
                periods_per_row=6,
            ),
        )

        assert result.reference_array.to_pylist() == [None, None]

    def test_null_in_window(
        self,
        quarterly_clock,
        twelve_months,
    ):
        column = DataColumn.load(
            [Decimal('1'), None, Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_array.to_pylist() == [None, None, None, None, None, Decimal('18')]

    def test_exact_decimal_sums(
        self,
        quarterly_clock,
    ):
        # The same sum in floats would be 0.30000000000000004
        column = DataColumn.load(
            [Decimal('0.1'), Decimal('0.1'), Decimal('0.1'), Decimal('0.1'), Decimal('0.1'), Decimal('0.1')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=ClockSpec(
                reference_period=ReferencePeriods.MONTH,
                periods_per_row=9,
            ),
        )

        assert result.reference_array.to_pylist()[2] == Decimal('0.3')

    def test_on_column_clock(
        self,
        quarterly_clock,
        twelve_months,
    ):
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_clock_column is quarterly_clock

    def test_instants_in_time_windows(
        self,
        daily_master_clock,
        dividend_clock,
    ):
        # Each master clock row sums the dividends after two days before it, up to its own date
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('4')],
            reference_clock_column=dividend_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=ClockSpec(
                reference_period=ReferencePeriods.DAY,
                periods_per_row=2,
            ),
        )

        assert result.reference_array.to_pylist() == [Decimal('1'), Decimal('0'), Decimal('6'), Decimal('4')]

    def test_instants_on_master_clock(
        self,
        daily_master_clock,
        dividend_clock,
        twelve_months,
    ):
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('4')],
            reference_clock_column=dividend_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_clock_column is daily_master_clock

    def test_calendar_month_windows(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 15),
                datetime.date(2024, 2, 14),
                datetime.date(2024, 2, 15),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        event_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 15),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        column = DataColumn.load(
            [Decimal('5')],
            reference_clock_column=event_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=ClockSpec(
                reference_period=ReferencePeriods.MONTH,
                periods_per_row=1,
            ),
        )

        # The dividend stops counting one month after it, on 2024-02-15
        assert result.reference_array.to_pylist() == [Decimal('5'), Decimal('5'), Decimal('0')]

    def test_instant_without_timestamp(
        self,
        daily_master_clock,
    ):
        # The second dividend has no date, so it doesn't count in any window
        undated_dividend_clock = DataColumn.load(
            [
                datetime.date(2023, 12, 31),
                None,
                datetime.date(2024, 1, 4),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=daily_master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('4')],
            reference_clock_column=undated_dividend_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=ClockSpec(
                reference_period=ReferencePeriods.DAY,
                periods_per_row=2,
            ),
        )

        assert result.reference_array.to_pylist() == [Decimal('1'), Decimal('0'), Decimal('4'), Decimal('4')]

    def test_null_instant_in_window(
        self,
        dividend_clock,
    ):
        column = DataColumn.load(
            [Decimal('1'), None, Decimal('4')],
            reference_clock_column=dividend_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=ClockSpec(
                reference_period=ReferencePeriods.DAY,
                periods_per_row=2,
            ),
        )

        assert result.reference_array.to_pylist() == [Decimal('1'), Decimal('0'), None, Decimal('4')]

    def test_column_without_rows(
        self,
        daily_master_clock,
        twelve_months,
    ):
        # A company that never paid dividends paid none in the last twelve months
        empty_dividend_clock = DataColumn(
            pyarrow.array(
                [],
                type=pyarrow.date32(),
            ),
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=daily_master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        column = DataColumn.load(
            [],
            reference_clock_column=empty_dividend_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_array.to_pylist() == [0, 0, 0, 0]

    def test_column_without_data(
        self,
        daily_master_clock,
        twelve_months,
    ):
        # The columns of a data block that got no entity have no data at all
        column = DataColumn(
            pyarrow.nulls(4),
            reference_clock_column=daily_master_clock,
        )
        result = helpers.rolling_sum(
            column,
            window=twelve_months,
        )

        assert result.reference_array.to_pylist() == [None, None, None, None]

    def test_sums_exceeding_decimal_precision(
        self,
        quarterly_clock,
        twelve_months,
    ):
        column = DataColumn(
            pyarrow.array(
                [Decimal(6) * Decimal(10) ** 37] * 6,
                type=pyarrow.decimal128(38, 0),
            ),
            reference_clock_column=quarterly_clock,
        )

        with pytest.raises(CalculationHelperError):
            helpers.rolling_sum(
                column,
                window=twelve_months,
            )

    def test_column_without_clock(
        self,
        twelve_months,
    ):
        with pytest.raises(CalculationHelperError):
            helpers.rolling_sum(
                DataColumn.load([Decimal('1'), Decimal('2')]),
                window=twelve_months,
            )

    def test_window_without_periods(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )

        with pytest.raises(CalculationHelperError):
            helpers.rolling_sum(
                column,
                window=ClockSpec(
                    reference_period=ReferencePeriods.MONTH,
                    periods_per_row=0,
                ),
            )


class TestSimpleMovingAverage:
    def test_window_averages(self):
        result = helpers.simple_moving_average(
            DataColumn.load([1.0, 2.0, 3.0, 4.0]),
            2,
        )
        expected = DataColumn.load([None, 1.5, 2.5, 3.5])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_null_in_window(self):
        result = helpers.simple_moving_average(
            DataColumn.load([1.0, None, 3.0, 4.0]),
            2,
        )
        expected = DataColumn.load([None, None, None, 3.5])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_on_reference_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.simple_moving_average(column, 2)

        assert result.reference_clock_column is quarterly_clock

    def test_bool_days(self):
        with pytest.raises(CalculationHelperError):
            helpers.simple_moving_average(
                DataColumn.load([1.0, 2.0]),
                True,
            )


class TestExponentialSmoothing:
    def test_wilder_smoothing_factor(self):
        # After starting from the average of the first 2 rows, each row weighs half of its smoothed value
        result = helpers.exponential_smoothing(
            column=DataColumn.load([1.0, 2.0, 3.0, 4.0]),
            days=2,
            smoothing_factor=1 / 2,
        )
        expected = DataColumn.load([None, 1.5, 2.25, 3.125])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    def test_restarts_after_null(self):
        result = helpers.exponential_smoothing(
            column=DataColumn.load([1.0, 2.0, None, 3.0, 5.0]),
            days=2,
            smoothing_factor=1 / 2,
        )
        expected = DataColumn.load([None, 1.5, None, None, 4.0])

        assert DataColumn.fully_equal(
            result,
            expected,
            approximate_floats=True,
            equal_nulls=True,
        )

    @pytest.mark.parametrize(
        'smoothing_factor',
        [
            0,
            1.5,
            # A bool, as bool is a subclass of int
            True,
            '0.5',
        ]
    )
    def test_invalid_smoothing_factor(
        self,
        smoothing_factor,
    ):
        with pytest.raises(CalculationHelperError):
            helpers.exponential_smoothing(
                column=DataColumn.load([1.0, 2.0, 3.0]),
                days=2,
                smoothing_factor=smoothing_factor,
            )

    def test_on_reference_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.exponential_smoothing(
            column=column,
            days=2,
            smoothing_factor=1 / 2,
        )

        assert result.reference_clock_column is quarterly_clock


class TestFindOperationClock:
    def test_shared_reference_clock(
        self,
        quarterly_clock,
    ):
        columns = [
            DataColumn.load(
                [1, 2, 3, 4, 5, 6],
                reference_clock_column=quarterly_clock,
            ),
            DataColumn.load(
                [6, 5, 4, 3, 2, 1],
                reference_clock_column=quarterly_clock,
            ),
        ]
        result = helpers.find_operation_clock(columns)

        assert result is quarterly_clock

    def test_different_reference_clocks(
        self,
        daily_master_clock,
        quarterly_clock,
    ):
        columns = [
            DataColumn.load(
                [1, 2, 3, 4],
                reference_clock_column=daily_master_clock,
            ),
            DataColumn.load(
                [1, 2, 3, 4, 5, 6],
                reference_clock_column=quarterly_clock,
            ),
        ]
        result = helpers.find_operation_clock(columns)

        assert result is daily_master_clock

    def test_without_clocks(self):
        result = helpers.find_operation_clock([
            DataColumn.load([1, 2]),
            DataColumn.load([3, 4]),
        ])

        assert result is None

    def test_different_master_clocks(
        self,
        daily_master_clock,
    ):
        other_master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 1),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        columns = [
            DataColumn.load(
                [1, 2, 3, 4],
                reference_clock_column=daily_master_clock,
            ),
            DataColumn.load(
                [1],
                reference_clock_column=other_master_clock,
            ),
        ]

        with pytest.raises(CalculationHelperError):
            helpers.find_operation_clock(columns)

    @pytest.mark.parametrize(
        'columns',
        [
            [],
            (DataColumn.load([1, 2]),),
            ['not_a_DataColumn'],
        ]
    )
    def test_invalid_columns(
        self,
        columns,
    ):
        with pytest.raises(CalculationHelperError):
            helpers.find_operation_clock(columns)


class TestGetFloatValues:
    def test_values_on_own_clock(
        self,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [Decimal('1.5'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.get_float_values(
            column,
            operation_clock_column=quarterly_clock,
        )
        expected = pyarrow.array([1.5, 2.0, 3.0, 4.0, 5.0, 6.0])

        assert result.equals(expected)

    def test_values_on_master_clock(
        self,
        daily_master_clock,
        quarterly_clock,
    ):
        # All the master clock dates come after the last filing date, so they all hold its value
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )
        result = helpers.get_float_values(
            column,
            operation_clock_column=daily_master_clock,
        )
        expected = pyarrow.array([6.0, 6.0, 6.0, 6.0])

        assert result.equals(expected)

    def test_clock_neither_reference_nor_master(
        self,
        dividend_clock,
        quarterly_clock,
    ):
        column = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5'), Decimal('6')],
            reference_clock_column=quarterly_clock,
        )

        with pytest.raises(CalculationHelperError):
            helpers.get_float_values(
                column,
                operation_clock_column=dividend_clock,
            )

    def test_not_a_column(
        self,
        quarterly_clock,
    ):
        with pytest.raises(CalculationHelperError):
            helpers.get_float_values(
                [1.0, 2.0],
                operation_clock_column=quarterly_clock,
            )


class TestPrivateCalculateWindowRowCount:
    @pytest.mark.parametrize(
        (
            'reference_period',
            'periods_per_row',
            'expected',
        ),
        [
            (ReferencePeriods.MONTH, 3, 4),
            (ReferencePeriods.MONTH, 6, 2),
            (ReferencePeriods.YEAR, 1, 1),
            (ReferencePeriods.MONTH, 5, None),
            (ReferencePeriods.YEAR, 2, 0),
            # Days have no fixed number per month
            (ReferencePeriods.DAY, 1, None),
            # Instants don't span any time
            (ReferencePeriods.DAY, 0, None),
        ]
    )
    def test_rows_in_twelve_months(
        self,
        daily_master_clock,
        twelve_months,
        reference_period,
        periods_per_row,
        expected,
    ):
        clock = DataColumn.load(
            [
                datetime.date(2024, 1, 2),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=daily_master_clock,
            clock_periods_per_row=periods_per_row,
            clock_reference_period=reference_period,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        result = helpers._calculate_window_row_count(
            clock_column=clock,
            window=twelve_months,
        )

        assert result == expected

    def test_hours_in_a_day(self):
        master_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 2, 9),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
        )
        result = helpers._calculate_window_row_count(
            clock_column=master_clock,
            window=ClockSpec(
                reference_period=ReferencePeriods.DAY,
                periods_per_row=1,
            ),
        )

        assert result == 24


class TestPrivateCountBoundariesBefore:
    def test_boundaries_before_each_timestamp(self):
        result = helpers._count_boundaries_before(
            boundaries=pyarrow.array([
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 4),
            ]),
            timestamps=pyarrow.array([
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 3),
                datetime.date(2023, 12, 31),
                None,
                datetime.date(2024, 1, 5),
            ]),
        )
        expected = pyarrow.array(
            [1, 2, 0, None, 3],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)


class TestPrivateFindRunsWithoutNulls:
    def test_runs_between_nulls(self):
        result = helpers._find_runs_without_nulls(
            pyarrow.array([1.0, None, 2.0, float('nan'), None, 3.0, 4.0])
        )

        assert result == [range(1), range(2, 3), range(5, 7)]


class TestPrivateShiftTimestamps:
    def test_calendar_months(self):
        result = helpers._shift_timestamps(
            pyarrow.array([
                datetime.date(2024, 2, 29),
                None,
            ]),
            ClockSpec(
                reference_period=ReferencePeriods.YEAR,
                periods_per_row=1,
            ),
        )
        expected = pyarrow.array([
            datetime.date(2025, 2, 28),
            None,
        ])

        assert result.equals(expected)

    def test_fixed_periods(self):
        result = helpers._shift_timestamps(
            pyarrow.array([
                datetime.datetime(2024, 1, 2, 9, 30),
            ]),
            ClockSpec(
                reference_period=ReferencePeriods.HOUR,
                periods_per_row=2,
            ),
        )
        expected = pyarrow.array([
            datetime.datetime(2024, 1, 2, 11, 30),
        ])

        assert result.equals(expected)


class TestPrivateSumRowWindows:
    def test_float_values(self):
        result = helpers._sum_row_windows(
            pyarrow.array([0.5, 1.0, 2.0]),
            row_count=2,
        )
        expected = pyarrow.array([None, 1.5, 3.0])

        assert result.equals(expected)

    def test_more_rows_than_values(self):
        result = helpers._sum_row_windows(
            pyarrow.array([1, 2]),
            row_count=3,
        )
        expected = pyarrow.array(
            [None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)


class TestPrivateValidateDecimalSums:
    def test_float_sums(self):
        result = helpers._validate_decimal_sums(
            pyarrow.array([1e300])
        )

        assert result is None

    def test_decimal_sums_within_precision(self):
        result = helpers._validate_decimal_sums(
            pyarrow.array(
                [Decimal('1.5')],
                type=pyarrow.decimal128(38, 1),
            )
        )

        assert result is None
