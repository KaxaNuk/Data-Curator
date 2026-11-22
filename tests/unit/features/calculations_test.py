import datetime
from decimal import Decimal
import math
import pyarrow
import pytest
import pyarrow.compute
from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import ReferencePeriods
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.features import calculations

from .fixtures import example_columns
#data_column_debugger defined in tests/conftest.py


@pytest.fixture(scope="module")
def example_average_daily_traded_value():
    return example_columns.load_example_columns('average_daily_traded_value.csv')


@pytest.fixture(scope="module")
def example_book_to_price():
    return example_columns.load_example_columns('book_to_price.csv')


@pytest.fixture(scope="module")
def example_chakin_money_flow_calculation():
    return example_columns.load_example_columns('chakin_money_flow.csv')


@pytest.fixture
def example_daily_master_clock():
    # Four trading days, from January 2024 to January 2025
    return DataColumn.load(
        [
            datetime.date(2024, 1, 2),
            datetime.date(2024, 6, 3),
            datetime.date(2024, 12, 2),
            datetime.date(2025, 1, 3),
        ],
        clock_master=True,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.DAY,
    )


@pytest.fixture
def example_dividend_clock(example_daily_master_clock):
    # The ex-dividend dates of two dividends, the first one before the master clock starts
    return DataColumn.load(
        [
            datetime.date(2023, 12, 1),
            datetime.date(2024, 6, 3),
        ],
        clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
        clock_master=example_daily_master_clock,
        clock_periods_per_row=0,
        clock_reference_period=ReferencePeriods.DAY,
        consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
    )


@pytest.fixture(scope="module")
def example_earnings_to_price():
    return example_columns.load_example_columns('earnings_to_price.csv')


@pytest.fixture
def example_fundamental_annual_clock(example_daily_master_clock):
    # The filing dates of two annual financial statements
    return DataColumn.load(
        [
            datetime.date(2023, 2, 1),
            datetime.date(2024, 2, 1),
        ],
        clock_master=example_daily_master_clock,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.YEAR,
        clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
    )


@pytest.fixture
def example_fundamental_half_yearly_clock(example_daily_master_clock):
    # The filing dates of three half-yearly financial statements
    return DataColumn.load(
        [
            datetime.date(2023, 2, 1),
            datetime.date(2023, 8, 1),
            datetime.date(2024, 2, 1),
        ],
        clock_master=example_daily_master_clock,
        clock_periods_per_row=6,
        clock_reference_period=ReferencePeriods.MONTH,
        clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
    )


@pytest.fixture
def example_fundamental_quarterly_clock(example_daily_master_clock):
    # The filing dates of five quarterly financial statements
    return DataColumn.load(
        [
            datetime.date(2023, 2, 1),
            datetime.date(2023, 5, 1),
            datetime.date(2023, 8, 1),
            datetime.date(2023, 11, 1),
            datetime.date(2024, 2, 1),
        ],
        clock_master=example_daily_master_clock,
        clock_periods_per_row=3,
        clock_reference_period=ReferencePeriods.MONTH,
        clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
    )


@pytest.fixture(scope="module")
def example_last_twelve_months_revenue_per_share():
    return example_columns.load_example_columns('last_twelve_months_revenue_per_share.csv')


@pytest.fixture(scope="module")
def example_log_returns_and_annualized_volatility():
    return example_columns.load_example_columns('log_returns_annualized_volatility.csv')


@pytest.fixture(scope="module")
def example_logarithmic_difference_high_low():
    return example_columns.load_example_columns('logarithmic_difference_high_low.csv')


@pytest.fixture(scope="module")
def example_market_cap():
    return example_columns.load_example_columns('market_cap.csv')


@pytest.fixture(scope="module")
def example_moving_average():
    return example_columns.load_example_columns('moving_average.csv')


@pytest.fixture(scope="module")
def example_moving_average_convergence_divergence():
    return example_columns.load_example_columns('moving_average_convergence_divergence.csv')


@pytest.fixture(scope="module")
def example_relative_strength_index():
    return example_columns.load_example_columns('relative_strength_index.csv')


@pytest.fixture(scope="module")
def example_sales_to_price():
    return example_columns.load_example_columns('sales_to_price.csv')


class TestCAnnualizedVolatility5dLogReturnsDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_log_returns_and_annualized_volatility,
    ):
        log_returns = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_log_returns_adjusted_close']
        )
        result = calculations.c_annualized_volatility_5d_log_returns_dividend_and_split_adjusted(log_returns)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_annualized_volatility_5d']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCAnnualizedVolatility21dLogReturnsDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_log_returns_and_annualized_volatility,
    ):
        log_returns = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_log_returns_adjusted_close']
        )
        result = calculations.c_annualized_volatility_21d_log_returns_dividend_and_split_adjusted(log_returns)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_annualized_volatility_21d']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCAnnualizedVolatility63dLogReturnsDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_log_returns_and_annualized_volatility,
    ):
        log_returns = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_log_returns_adjusted_close']
        )
        result = calculations.c_annualized_volatility_63d_log_returns_dividend_and_split_adjusted(
            log_returns
        )
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_annualized_volatility_63d']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCAnnualizedVolatility252dLogReturnsDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_log_returns_and_annualized_volatility,
    ):
        log_returns = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_log_returns_adjusted_close']
        )
        result = calculations.c_annualized_volatility_252d_log_returns_dividend_and_split_adjusted(
            log_returns
        )
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_annualized_volatility_252d']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCDailyTradedValue:
    def test_example_data(
        self,
        example_average_daily_traded_value,
    ):
        # @todo: need to test with all the adjusted vwap and volume combinations
        vwap = DataColumn.load(
            example_average_daily_traded_value['m_vwap']
        )
        volume = DataColumn.load(
            example_average_daily_traded_value['m_volume']
        )
        result = calculations.c_daily_traded_value(
            vwap,
            volume,
            vwap,
            volume,
            vwap,
            volume,
        )
        expected_result = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCDailyTradedValueSma5d:
    def test_example_data(
        self,
        example_average_daily_traded_value,
    ):
        average_daily_traded_value = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value']
        )
        result = calculations.c_daily_traded_value_sma_5d(average_daily_traded_value)
        expected_result = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value_5d']
        )
        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCDailyTradedValueSma21d:
    def test_example_data(
        self,
        example_average_daily_traded_value,
    ):
        average_daily_traded_value = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value']
        )
        result = calculations.c_daily_traded_value_sma_21d(average_daily_traded_value)
        expected_result = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value_21d']
        )
        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCDailyTradedValueSma63d:
    def test_example_data(
        self,
        example_average_daily_traded_value,
    ):
        average_daily_traded_value = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value']
        )
        result = calculations.c_daily_traded_value_sma_63d(average_daily_traded_value)
        expected_result = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value_63d']
        )
        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCDailyTradedValueSma252d:
    def test_example_data(
        self,
        example_average_daily_traded_value,
    ):
        average_daily_traded_value = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value']
        )
        result = calculations.c_daily_traded_value_sma_252d(average_daily_traded_value)
        expected_result = DataColumn.load(
            example_average_daily_traded_value['c_average_daily_traded_value_252d']
        )
        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCBookToPrice:
    def test_example_data(
        self,
        example_book_to_price,
    ):
        total_assets = DataColumn.load(
            example_book_to_price['fbs_total_assets']
        )
        total_liabilities = DataColumn.load(
            example_book_to_price['fbs_total_liabilities']
        )
        preferred_stock = DataColumn.load(
            example_book_to_price['fbs_preferred_stock']
        )
        market_cap = DataColumn.load(
            example_book_to_price['c_market_cap']
        )
        result = calculations.c_book_to_price(total_assets, total_liabilities, preferred_stock,
                                                   market_cap)
        expected_result = DataColumn.load(
            example_book_to_price['c_book_to_price']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCBookValuePerShare:
    def test_with_nulls_and_zeroes(self):
        fbs_total_assets = DataColumn.load(
            [
                None,
                Decimal('10'),
                Decimal('0'),
                Decimal('7'),
                Decimal('5'),
                Decimal('8.3'),
            ]
        )
        fbs_total_liabilities = DataColumn.load(
            [
                None,
                Decimal('4'),
                Decimal('3'),
                Decimal('0'),
                Decimal('1'),
                Decimal('2.1'),
            ]
        )
        fbs_preferred_stock = DataColumn.load(
            [
                None,
                Decimal('2'),
                Decimal('1'),
                Decimal('3'),
                Decimal('0'),
                Decimal('3.4')
            ]
        )
        fi_weighted_average_shares_outstanding = DataColumn.load(
            [
                None,
                Decimal('0'),
                Decimal('2'),
                Decimal('2'),
                Decimal('4'),
                Decimal('2'),
            ]
        )
        expected_result = DataColumn.load(
            [
                None,
                None,
                Decimal('-2'),
                Decimal('2'),
                Decimal('1'),
                Decimal('1.4'),
            ]
        )

        assert DataColumn.fully_equal(
            calculations.c_book_value_per_share(
                fbs_total_assets,
                fbs_total_liabilities,
                fbs_preferred_stock,
                fi_weighted_average_shares_outstanding
            ),
            expected_result,
            equal_nulls=True
        )


class TestCChaikinMoneyFlow21dDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_chakin_money_flow_calculation,
    ):
        high = DataColumn.load(
            example_chakin_money_flow_calculation['c_adjusted_high']
        )
        low = DataColumn.load(
            example_chakin_money_flow_calculation['c_adjusted_low']
        )
        volume = DataColumn.load(
            example_chakin_money_flow_calculation['m_volume']
        )
        close = DataColumn.load(
            example_chakin_money_flow_calculation['m_adjusted_close']
        )
        result = calculations.c_chaikin_money_flow_21d_dividend_and_split_adjusted(high, low, close, volume)

        result_values = result.to_pyarrow().to_pylist()
        expected_values = DataColumn.load(
            example_chakin_money_flow_calculation['c_chaikin_money_flow_21d']
        ).to_pyarrow().to_pylist()

        # The float arithmetic differs from the exact expected values around their 12th significant digit, where the
        # money flows of a window nearly cancel out
        assert all(
            (
                math.isclose(
                    result_value,
                    float(expected_value),
                    rel_tol=1e-9,
                )
                if (
                    result_value is not None
                    and expected_value is not None
                )
                else result_value is expected_value
            )
            for (result_value, expected_value) in zip(result_values, expected_values, strict=True)
        )


class TestCChaikinMoneyFlow21dSplitAdjusted:
    def test_example_data(
        self,
        example_chakin_money_flow_calculation,
    ):
        high = DataColumn.load(
            example_chakin_money_flow_calculation['c_adjusted_high']
        )
        low = DataColumn.load(
            example_chakin_money_flow_calculation['c_adjusted_low']
        )
        volume = DataColumn.load(
            example_chakin_money_flow_calculation['m_volume']
        )
        close = DataColumn.load(
            example_chakin_money_flow_calculation['m_adjusted_close']
        )
        result = calculations.c_chaikin_money_flow_21d_split_adjusted(high, low, close, volume)

        result_values = result.to_pyarrow().to_pylist()
        expected_values = DataColumn.load(
            example_chakin_money_flow_calculation['c_chaikin_money_flow_21d']
        ).to_pyarrow().to_pylist()

        # The float arithmetic differs from the exact expected values around their 12th significant digit, where the
        # money flows of a window nearly cancel out
        assert all(
            (
                math.isclose(
                    result_value,
                    float(expected_value),
                    rel_tol=1e-9,
                )
                if (
                    result_value is not None
                    and expected_value is not None
                )
                else result_value is expected_value
            )
            for (result_value, expected_value) in zip(result_values, expected_values, strict=True)
        )


class TestCEarningsPerShare:
    def test_with_nulls_and_zero_shares(self):
        fi_net_income = DataColumn.load(
            [
                None,
                Decimal('10'),
                Decimal('14.4'),
                Decimal('666666'),
                Decimal('2')
            ]
        )
        fi_weighted_average_shares_outstanding = DataColumn.load(
            [
                None,
                Decimal('5'),
                Decimal('7.2'),
                Decimal('333333'),
                Decimal('0')
            ]
        )
        expected_result = DataColumn.load(
            [
                None,
                Decimal('2'),
                Decimal('2'),
                Decimal('2'),
                None
            ]
        )

        assert DataColumn.fully_equal(
            calculations.c_earnings_per_share(
                fi_net_income,
                fi_weighted_average_shares_outstanding
            ),
            expected_result,
            equal_nulls=True
        )


class TestCEarningsToPrice:
    def test_example_data(
        self,
        example_earnings_to_price,
    ):
        market_cap = DataColumn.load(
            example_earnings_to_price['c_market_cap']
        )
        earnings = DataColumn.load(
            example_earnings_to_price['c_last_twelve_months_net_income']
        )
        result = calculations.c_earnings_to_price(earnings, market_cap)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_earnings_to_price['c_earnings_to_price']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage5dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):

        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_5d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_5d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage5dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):

        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_5d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_5d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage21dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_21d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_21d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage21dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_21d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_21d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage63dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_63d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_63d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage63dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_63d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_63d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage252dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_252d_close_dividend_and_split_adjusted(
            adjusted_close
        )
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_252d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCExponentialMovingAverage252dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_exponential_moving_average_252d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_exponential_moving_average_252d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCLastTwelveMonthsDividendsSplitAdjusted:
    def test_dividends_by_ex_dividend_date(
        self,
        example_dividend_clock,
    ):
        # Each row sums the dividends that went ex-dividend after the same date a year before, up to its own date
        dividends = DataColumn.load(
            [Decimal('0.25'), Decimal('0.30')],
            reference_clock_column=example_dividend_clock,
        )
        result = calculations.c_last_twelve_months_dividends_split_adjusted(dividends)

        assert result.to_pyarrow().to_pylist() == [Decimal('0.25'), Decimal('0.55'), Decimal('0.30'), Decimal('0.30')]

    def test_without_dividends(
        self,
        example_daily_master_clock,
    ):
        empty_dividend_clock = DataColumn(
            pyarrow.array(
                [],
                type=pyarrow.date32(),
            ),
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=example_daily_master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        dividends = DataColumn.load(
            [],
            reference_clock_column=empty_dividend_clock,
        )
        result = calculations.c_last_twelve_months_dividends_split_adjusted(dividends)

        assert result.to_pyarrow().to_pylist() == [0, 0, 0, 0]


class TestCLastTwelveMonthsNetIncome:
    def test_quarterly_statements(
        self,
        example_fundamental_quarterly_clock,
    ):
        net_income = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5')],
            reference_clock_column=example_fundamental_quarterly_clock,
        )
        result = calculations.c_last_twelve_months_net_income(net_income)

        assert result.reference_array.to_pylist() == [None, None, None, Decimal('10'), Decimal('14')]

    def test_half_yearly_statements(
        self,
        example_fundamental_half_yearly_clock,
    ):
        net_income = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3')],
            reference_clock_column=example_fundamental_half_yearly_clock,
        )
        result = calculations.c_last_twelve_months_net_income(net_income)

        assert result.reference_array.to_pylist() == [None, Decimal('3'), Decimal('5')]

    def test_annual_statements(
        self,
        example_fundamental_annual_clock,
    ):
        net_income = DataColumn.load(
            [Decimal('1'), Decimal('2')],
            reference_clock_column=example_fundamental_annual_clock,
        )
        result = calculations.c_last_twelve_months_net_income(net_income)

        assert result.reference_array.to_pylist() == [Decimal('1'), Decimal('2')]

    def test_null_statement_value(
        self,
        example_fundamental_quarterly_clock,
    ):
        # Both windows of four quarters hold the second quarter
        net_income = DataColumn.load(
            [Decimal('1'), None, Decimal('3'), Decimal('4'), Decimal('5')],
            reference_clock_column=example_fundamental_quarterly_clock,
        )
        result = calculations.c_last_twelve_months_net_income(net_income)

        assert result.reference_array.to_pylist() == [None, None, None, None, None]

    def test_on_statements_clock(
        self,
        example_fundamental_quarterly_clock,
    ):
        net_income = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5')],
            reference_clock_column=example_fundamental_quarterly_clock,
        )
        result = calculations.c_last_twelve_months_net_income(net_income)

        assert result.reference_clock_column is example_fundamental_quarterly_clock


class TestCLastTwelveMonthsRevenue:
    def test_quarterly_statements(
        self,
        example_fundamental_quarterly_clock,
    ):
        revenues = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5')],
            reference_clock_column=example_fundamental_quarterly_clock,
        )
        result = calculations.c_last_twelve_months_revenue(revenues)

        assert result.reference_array.to_pylist() == [None, None, None, Decimal('10'), Decimal('14')]

    def test_half_yearly_statements(
        self,
        example_fundamental_half_yearly_clock,
    ):
        revenues = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3')],
            reference_clock_column=example_fundamental_half_yearly_clock,
        )
        result = calculations.c_last_twelve_months_revenue(revenues)

        assert result.reference_array.to_pylist() == [None, Decimal('3'), Decimal('5')]

    def test_annual_statements(
        self,
        example_fundamental_annual_clock,
    ):
        revenues = DataColumn.load(
            [Decimal('1'), Decimal('2')],
            reference_clock_column=example_fundamental_annual_clock,
        )
        result = calculations.c_last_twelve_months_revenue(revenues)

        assert result.reference_array.to_pylist() == [Decimal('1'), Decimal('2')]

    def test_null_statement_value(
        self,
        example_fundamental_quarterly_clock,
    ):
        # Both windows of four quarters hold the second quarter
        revenues = DataColumn.load(
            [Decimal('1'), None, Decimal('3'), Decimal('4'), Decimal('5')],
            reference_clock_column=example_fundamental_quarterly_clock,
        )
        result = calculations.c_last_twelve_months_revenue(revenues)

        assert result.reference_array.to_pylist() == [None, None, None, None, None]

    def test_on_statements_clock(
        self,
        example_fundamental_quarterly_clock,
    ):
        revenues = DataColumn.load(
            [Decimal('1'), Decimal('2'), Decimal('3'), Decimal('4'), Decimal('5')],
            reference_clock_column=example_fundamental_quarterly_clock,
        )
        result = calculations.c_last_twelve_months_revenue(revenues)

        assert result.reference_clock_column is example_fundamental_quarterly_clock


class TestCLastTwelveMonthsRevenuePerShare:
    def test_example_data(
        self,
        example_last_twelve_months_revenue_per_share,
    ):
        weighted_average_shares_outstanding = DataColumn.load(
            example_last_twelve_months_revenue_per_share['fi_weighted_average_shares_outstanding']
        )
        revenue = DataColumn.load(
            example_last_twelve_months_revenue_per_share['c_last_twelve_months_revenue']
        )
        result = calculations.c_last_twelve_months_revenue_per_share(revenue, weighted_average_shares_outstanding)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_last_twelve_months_revenue_per_share['c_last_twelve_months_revenue_per_share']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCLogReturnsDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_log_returns_and_annualized_volatility,
    ):
        adjusted_close = DataColumn.load(
            example_log_returns_and_annualized_volatility['m_adjusted_close']
        )
        result = calculations.c_log_returns_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_log_returns_and_annualized_volatility['c_log_returns_adjusted_close']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCLogDifferenceHighToLow:
    def test_example_data(
        self,
        example_logarithmic_difference_high_low,
    ):
        high = DataColumn.load(
            example_logarithmic_difference_high_low['m_high']
        )
        low = DataColumn.load(
            example_logarithmic_difference_high_low['m_low']
        )
        result = calculations.c_log_difference_high_to_low(high, low)
        expected_result = DataColumn.load(
            example_logarithmic_difference_high_low['c_logarithmic_difference_high_low']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCMarketCap:
    def test_example_data(
        self,
        example_market_cap,
    ):
        close = DataColumn.load(
            example_market_cap['m_close']
        )
        shares_outstanding = DataColumn.load(
            example_market_cap['fi_weighted_average_shares_outstanding']
        )
        result = calculations.c_market_cap(close, shares_outstanding)
        expected_result = DataColumn.load(
            example_market_cap['c_market_cap']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCMacd26d12dDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average_convergence_divergence,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average_convergence_divergence['m_adjusted_close']
        )
        result = calculations.c_macd_26d_12d_dividend_and_split_adjusted(adjusted_close)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=10
            )
        )

        expected_result = DataColumn.load(
            example_moving_average_convergence_divergence['c_moving_average_convergence_divergence_26_12']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=10
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCMacd26d12dSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average_convergence_divergence,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average_convergence_divergence['m_adjusted_close']
        )
        result = calculations.c_macd_26d_12d_split_adjusted(adjusted_close)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=10
            )
        )

        expected_result = DataColumn.load(
            example_moving_average_convergence_divergence['c_moving_average_convergence_divergence_26_12']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=10
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCMacdSignal9dDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average_convergence_divergence,
    ):

        macd = DataColumn.load(
            example_moving_average_convergence_divergence['c_moving_average_convergence_divergence_26_12']
        )
        result = calculations.c_macd_signal_9d_dividend_and_split_adjusted(
            macd
        )
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=10
            )
        )

        expected_result = DataColumn.load(
            example_moving_average_convergence_divergence['c_moving_average_convergence_divergence_signal_9']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=10
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCMacdSignal9dSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average_convergence_divergence,
    ):

        macd = DataColumn.load(
            example_moving_average_convergence_divergence['c_moving_average_convergence_divergence_26_12']
        )
        result = calculations.c_macd_signal_9d_split_adjusted(macd)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=10
            )
        )

        expected_result = DataColumn.load(
            example_moving_average_convergence_divergence['c_moving_average_convergence_divergence_signal_9']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=10
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCRsi14dDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_relative_strength_index,
    ):
        close = DataColumn.load(
            example_relative_strength_index['m_adjusted_close']
        )
        result = calculations.c_rsi_14d_dividend_and_split_adjusted(close)

        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )

        expected_result = DataColumn.load(
            example_relative_strength_index['c_relative_strength_index_14d']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCRsi14dSplitAdjusted:
    def test_example_data(
        self,
        example_relative_strength_index,
    ):
        close = DataColumn.load(
            example_relative_strength_index['m_adjusted_close']
        )
        result = calculations.c_rsi_14d_split_adjusted(close)

        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )

        expected_result = DataColumn.load(
            example_relative_strength_index['c_relative_strength_index_14d']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSalesToPrice:
    def test_example_data(
        self,
        example_sales_to_price,
    ):
        market_cap = DataColumn.load(
            example_sales_to_price['c_market_cap']
        )
        revenue = DataColumn.load(
            example_sales_to_price['c_last_twelve_months_revenue']
        )
        result = calculations.c_sales_to_price(revenue, market_cap)
        rounded_result = DataColumn.load(
            pyarrow.compute.round(
                result.to_pyarrow(),
                ndigits=16
            )
        )
        expected_result = DataColumn.load(
            example_sales_to_price['c_sales_to_price']
        )
        expected_rounded_result = DataColumn.load(
            pyarrow.compute.round(
                expected_result.to_pyarrow(),
                ndigits=16
            )
        )

        assert DataColumn.fully_equal(
            rounded_result,
            expected_rounded_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage5dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_5d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_5d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage5dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_5d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_5d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage21dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_21d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_21d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage21dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_21d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_21d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage63dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_63d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_63d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage63dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_63d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_63d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage252dCloseDividendAndSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_252d_close_dividend_and_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_252d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )


class TestCSimpleMovingAverage252dCloseSplitAdjusted:
    def test_example_data(
        self,
        example_moving_average,
    ):
        adjusted_close = DataColumn.load(
            example_moving_average['m_adjusted_close']
        )
        result = calculations.c_simple_moving_average_252d_close_split_adjusted(adjusted_close)
        expected_result = DataColumn.load(
            example_moving_average['c_simple_moving_average_252d']
        )

        assert DataColumn.fully_equal(
            result,
            expected_result,
            approximate_floats=True,
            equal_nulls=True,
        )
