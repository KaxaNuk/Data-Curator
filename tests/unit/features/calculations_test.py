from decimal import Decimal
import types
import pyarrow
import pytest
import pyarrow.compute
from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.features import calculations
from kaxanuk.data_curator.exceptions import CalculationError
#data_column_debugger defined in tests/conftest.py


@pytest.fixture
def example_configuration_period_annual():
    return types.SimpleNamespace(period='annual')


@pytest.fixture
def example_configuration_period_quarterly():
    return types.SimpleNamespace(period='quarterly')


def test_annualized_volatility_5d(
    example_log_returns_and_annualized_volatility
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


def test_annualized_volatility_21d(example_log_returns_and_annualized_volatility):
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


def test_annualized_volatility_63d(example_log_returns_and_annualized_volatility):
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


def test_annualized_volatility_252d(example_log_returns_and_annualized_volatility):
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


def test_daily_traded_value(example_average_daily_traded_value):
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


def test_daily_traded_value_sma_5d(example_average_daily_traded_value):
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


def test_daily_traded_value_sma_21d(example_average_daily_traded_value):
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


def test_daily_traded_value_sma_63d(example_average_daily_traded_value):
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


def test_daily_traded_value_sma_252d(example_average_daily_traded_value):
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


def test_book_to_price(example_book_to_price):
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


def test_book_value_per_share():
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


def test_chaikin_money_flow_21d_dividend_and_split_adjusted(example_chakin_money_flow_calculation):
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

    rounded_result = DataColumn.load(
        pyarrow.compute.round(
            result.to_pyarrow(),
            ndigits=11
        )
    )

    expected_result = DataColumn.load(
        example_chakin_money_flow_calculation['c_chaikin_money_flow_21d']
    )
    expected_rounded_result = DataColumn.load(
        pyarrow.compute.round(
            expected_result.to_pyarrow(),
            ndigits=11
        )
    )

    assert DataColumn.fully_equal(
        rounded_result,
        expected_rounded_result,
        approximate_floats=True,
        equal_nulls=True,
    )


def test_chaikin_money_flow_21d_split_adjusted(example_chakin_money_flow_calculation):
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

    rounded_result = DataColumn.load(
        pyarrow.compute.round(
            result.to_pyarrow(),
            ndigits=11
        )
    )

    expected_result = DataColumn.load(
        example_chakin_money_flow_calculation['c_chaikin_money_flow_21d']
    )
    expected_rounded_result = DataColumn.load(
        pyarrow.compute.round(
            expected_result.to_pyarrow(),
            ndigits=11
        )
    )

    assert DataColumn.fully_equal(
        rounded_result,
        expected_rounded_result,
        approximate_floats=True,
        equal_nulls=True,
    )


def test_earnings_per_share():
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


def test_earnings_to_price(example_earnings_to_price):
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


def test_ema_5d_dividend_and_split_adjusted_close(example_moving_average):

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


def test_ema_5d_split_adjusted_close(example_moving_average):

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


def test_ema_21d_dividend_and_split_adjusted_close(example_moving_average):
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


def test_ema_21d_split_adjusted_close(example_moving_average):
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


def test_ema_63d_dividend_and_split_adjusted_close(example_moving_average):
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


def test_ema_63d_split_adjusted_close(example_moving_average):
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


def test_ema_252d_dividend_and_split_adjusted_close(example_moving_average):
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


def test_ema_252d_split_adjusted_close(example_moving_average):
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


def test_last_twelve_months_net_income_annual(
    example_configuration_period_annual
):
    fi_net_income_annual = DataColumn.load(
        [
            None,
            None,
            Decimal('3'),
            Decimal('5'),
            Decimal('7'),
            Decimal('11'),
            Decimal('13'),
            Decimal('2'),
        ]
    )
    f_fiscal_year_annual = DataColumn.load(
        [
            None,
            None,
            '2001',
            '2002',
            '2003',
            '2004',
            '2005',
            '2006',
        ]
    )
    f_fiscal_period_annual = DataColumn.load(
        [
            None,
            None,
            'FY',
            'FY',
            'FY',
            'FY',
            'FY',
            'FY',
        ]
    )
    assert DataColumn.fully_equal(
        calculations.c_last_twelve_months_net_income(
            fi_net_income_annual,
            f_fiscal_year_annual,
            f_fiscal_period_annual,
            example_configuration_period_annual
        ),
        fi_net_income_annual,
        equal_nulls=True
    )


def test_last_twelve_months_net_income_invalid_period():
    fi_net_income_valid = DataColumn.load([Decimal('5'), Decimal('7'), Decimal('9')])
    f_fiscal_year_valid = DataColumn.load(['2001', '2002', '2003'])
    f_fiscal_period_valid = DataColumn.load(['FY', 'FY', 'FY'])

    class DummyConfig:
        period = 'monthly'

    with pytest.raises(CalculationError) as exc_info:
        calculations.c_last_twelve_months_net_income(
            fi_net_income_valid,
            f_fiscal_year_valid,
            f_fiscal_period_valid,
            DummyConfig()
        )
    assert "unexpected period type: monthly" in str(exc_info.value)


@pytest.mark.parametrize("config_fixture", ["annual", "quarterly"])
def test_last_twelve_months_net_income_null_fiscal_period(
    example_configuration_period_annual,
    example_configuration_period_quarterly,
    config_fixture
):
    config = (example_configuration_period_annual
              if config_fixture == "annual"
              else example_configuration_period_quarterly)

    net_income_valid = DataColumn.load([Decimal('5'), Decimal('7'), Decimal('9')])
    fiscal_year_valid = DataColumn.load(['2001', '2002', '2003'])
    fiscal_period_null = DataColumn.load([None, None, None])

    result = calculations.c_last_twelve_months_net_income(
        net_income_valid,
        fiscal_year_valid,
        fiscal_period_null,
        config
    )
    assert DataColumn.fully_equal(result, fiscal_period_null, equal_nulls=True)


@pytest.mark.parametrize("config_fixture", ["annual", "quarterly"])
def test_last_twelve_months_net_income_null_fiscal_year(
    example_configuration_period_annual,
    example_configuration_period_quarterly,
    config_fixture
):
    config = (example_configuration_period_annual
              if config_fixture == "annual"
              else example_configuration_period_quarterly)

    net_income_valid = DataColumn.load([Decimal('5'), Decimal('7'), Decimal('9')])
    fiscal_year_null = DataColumn.load([None, None, None])
    fiscal_period_valid = DataColumn.load(['FY', 'FY', 'FY'])

    result = calculations.c_last_twelve_months_net_income(
        net_income_valid,
        fiscal_year_null,
        fiscal_period_valid,
        config
    )
    assert DataColumn.fully_equal(result, fiscal_year_null, equal_nulls=True)


@pytest.mark.parametrize("config_fixture", ["annual", "quarterly"])
def test_last_twelve_months_net_income_null_net_income(
    example_configuration_period_annual,
    example_configuration_period_quarterly,
    config_fixture
):
    config = (example_configuration_period_annual
              if config_fixture == "annual"
              else example_configuration_period_quarterly)

    net_income_null = DataColumn.load([None, None, None])
    fiscal_year_valid = DataColumn.load(['2001', '2002', '2003'])
    fiscal_period_valid = DataColumn.load(['FY', 'FY', 'FY'])

    result = calculations.c_last_twelve_months_net_income(
        net_income_null,
        fiscal_year_valid,
        fiscal_period_valid,
        config
    )
    assert DataColumn.fully_equal(result, net_income_null, equal_nulls=True)


def test_last_twelve_months_net_income_quarterly(
        example_configuration_period_quarterly
):
    fi_net_income_quarterly = DataColumn.load(
        [
            None,
            None,
            Decimal('3'),
            Decimal('3'),
            Decimal('3'),
            Decimal('5'),
            Decimal('5'),
            Decimal('5'),
            Decimal('7'),
            Decimal('7'),
            Decimal('7'),
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            Decimal('13'),
            Decimal('13'),
            Decimal('13'),
            Decimal('2'),
            Decimal('2'),
            Decimal('2'),
        ]
    )
    f_fiscal_year_quarterly = DataColumn.load(
        [
            None,
            None,
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
        ]
    )
    f_fiscal_period_quarterly = DataColumn.load(
        [
            None,
            None,
            'Q2',
            'Q2',
            'Q2',
            'Q3',
            'Q3',
            'Q3',
            'Q4',
            'Q4',
            'Q4',
            'Q1',
            'Q1',
            'Q1',
            'Q2',
            'Q2',
            'Q2',
            'Q3',
            'Q3',
            'Q3',
        ]
    )
    expected_result_quarterly = DataColumn.load(
        [
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
            Decimal('26'),
            Decimal('26'),
            Decimal('26'),
            Decimal('36'),
            Decimal('36'),
            Decimal('36'),
            Decimal('33'),
            Decimal('33'),
            Decimal('33'),
        ]
    )
    assert DataColumn.fully_equal(
        calculations.c_last_twelve_months_net_income(
            fi_net_income_quarterly,
            f_fiscal_year_quarterly,
            f_fiscal_period_quarterly,
            example_configuration_period_quarterly
        ),
        expected_result_quarterly,
        equal_nulls=True
    )


def test_last_twelve_months_revenue_annual(
    example_configuration_period_annual
):
    fi_revenue_annual = DataColumn.load(
        [
            None,
            None,
            Decimal('3'),
            Decimal('5'),
            Decimal('7'),
            Decimal('11'),
            Decimal('13'),
            Decimal('2'),
        ]
    )
    f_fiscal_year_annual = DataColumn.load(
        [
            None,
            None,
            '2001',
            '2002',
            '2003',
            '2004',
            '2005',
            '2006',
        ]
    )
    f_fiscal_period_annual = DataColumn.load(
        [
            None,
            None,
            'FY',
            'FY',
            'FY',
            'FY',
            'FY',
            'FY',
        ]
    )
    assert DataColumn.fully_equal(
        calculations.c_last_twelve_months_revenue(
            fi_revenue_annual,
            f_fiscal_year_annual,
            f_fiscal_period_annual,
            example_configuration_period_annual
        ),
        fi_revenue_annual,
        equal_nulls=True
    )


def test_last_twelve_months_revenue_invalid_period():
    fi_net_income_valid = DataColumn.load([Decimal('5'), Decimal('7'), Decimal('9')])
    f_fiscal_year_valid = DataColumn.load(['2001', '2002', '2003'])
    f_fiscal_period_valid = DataColumn.load(['FY', 'FY', 'FY'])

    class DummyConfig:
        period = 'monthly'

    with pytest.raises(CalculationError) as exc_info:
        calculations.c_last_twelve_months_revenue(
            fi_net_income_valid,
            f_fiscal_year_valid,
            f_fiscal_period_valid,
            DummyConfig()
        )
    assert "unexpected period type: monthly" in str(exc_info.value)


@pytest.mark.parametrize("config_fixture", ["annual", "quarterly"])
def test_last_twelve_months_revenue_null_fiscal_period(
    example_configuration_period_annual,
    example_configuration_period_quarterly,
    config_fixture
):
    config = (example_configuration_period_annual
              if config_fixture == "annual"
              else example_configuration_period_quarterly)

    revenue_valid = DataColumn.load([Decimal('5'), Decimal('7'), Decimal('9')])
    fiscal_year_valid = DataColumn.load(['2001', '2002', '2003'])
    fiscal_period_null = DataColumn.load([None, None, None])

    result = calculations.c_last_twelve_months_revenue(
        revenue_valid,
        fiscal_year_valid,
        fiscal_period_null,
        config
    )
    assert DataColumn.fully_equal(result, fiscal_period_null, equal_nulls=True)


@pytest.mark.parametrize("config_fixture", ["annual", "quarterly"])
def test_last_twelve_months_revenue_null_fiscal_year(
    example_configuration_period_annual,
    example_configuration_period_quarterly,
    config_fixture
):
    config = (example_configuration_period_annual
              if config_fixture == "annual"
              else example_configuration_period_quarterly)

    revenue_valid = DataColumn.load([Decimal('5'), Decimal('7'), Decimal('9')])
    fiscal_year_null = DataColumn.load([None, None, None])
    fiscal_period_valid = DataColumn.load(['FY', 'FY', 'FY'])

    result = calculations.c_last_twelve_months_revenue(
        revenue_valid,
        fiscal_year_null,
        fiscal_period_valid,
        config
    )
    assert DataColumn.fully_equal(result, fiscal_year_null, equal_nulls=True)


@pytest.mark.parametrize("config_fixture", ["annual", "quarterly"])
def test_last_twelve_months_revenue_null_revenue(
    example_configuration_period_annual,
    example_configuration_period_quarterly,
    config_fixture
):
    config = (example_configuration_period_annual
              if config_fixture == "annual"
              else example_configuration_period_quarterly)

    revenue_null = DataColumn.load([None, None, None])
    fiscal_year_valid = DataColumn.load(['2001', '2002', '2003'])
    fiscal_period_valid = DataColumn.load(['FY', 'FY', 'FY'])

    result = calculations.c_last_twelve_months_revenue(
        revenue_null,
        fiscal_year_valid,
        fiscal_period_valid,
        config
    )
    assert DataColumn.fully_equal(result, revenue_null, equal_nulls=True)


def test_last_twelve_months_revenue_per_share(
    example_last_twelve_months_revenue_per_share
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


def test_last_twelve_months_revenue_quarterly(
        example_configuration_period_quarterly
):
    fi_revenue_quarterly = DataColumn.load(
        [
            None,
            None,
            Decimal('3'),
            Decimal('3'),
            Decimal('3'),
            Decimal('5'),
            Decimal('5'),
            Decimal('5'),
            Decimal('7'),
            Decimal('7'),
            Decimal('7'),
            Decimal('11'),
            Decimal('11'),
            Decimal('11'),
            Decimal('13'),
            Decimal('13'),
            Decimal('13'),
            Decimal('2'),
            Decimal('2'),
            Decimal('2'),
        ]
    )
    f_fiscal_year_quarterly = DataColumn.load(
        [
            None,
            None,
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2001',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
            '2002',
        ]
    )
    f_fiscal_period_quarterly = DataColumn.load(
        [
            None,
            None,
            'Q2',
            'Q2',
            'Q2',
            'Q3',
            'Q3',
            'Q3',
            'Q4',
            'Q4',
            'Q4',
            'Q1',
            'Q1',
            'Q1',
            'Q2',
            'Q2',
            'Q2',
            'Q3',
            'Q3',
            'Q3',
        ]
    )
    expected_result_quarterly = DataColumn.load(
        [
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
            Decimal('26'),
            Decimal('26'),
            Decimal('26'),
            Decimal('36'),
            Decimal('36'),
            Decimal('36'),
            Decimal('33'),
            Decimal('33'),
            Decimal('33'),
        ]
    )
    assert DataColumn.fully_equal(
        calculations.c_last_twelve_months_revenue(
            fi_revenue_quarterly,
            f_fiscal_year_quarterly,
            f_fiscal_period_quarterly,
            example_configuration_period_quarterly
        ),
        expected_result_quarterly,
        equal_nulls=True
    )


def test_log_returns_dividend_and_split_adjusted(
    example_log_returns_and_annualized_volatility
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


def test_loga_difference_high_and_low(example_logarithmic_difference_high_low):
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


def test_market_cap(example_market_cap):
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


def test_macd_26d_12d_dividend_and_split_adjusted(
    example_moving_average_convergence_divergence
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


def test_macd_26d_12d_split_adjusted(
    example_moving_average_convergence_divergence
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


def test_macd_signal_9d_dividend_and_split_adjusted(
    example_moving_average_convergence_divergence
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


def test_macd_signal_9d_split_adjusted_close(
    example_moving_average_convergence_divergence
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


def test_rsi_14d_dividend_and_split_adjusted(example_relative_strength_index):
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


def test_rsi_14d_split_adjusted(example_relative_strength_index):
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


def test_sales_to_price(example_sales_to_price):
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


def test_simple_moving_average_5d_close_dividend_and_split_adjusted(example_moving_average):
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


def test_simple_moving_average_5d_close_split_adjusted(example_moving_average):
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


def test_simple_moving_average_21d_close_dividend_and_split_adjusted(example_moving_average):
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


def test_simple_moving_average_21d_close_split_adjusted(example_moving_average):
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


def test_simple_moving_average_63d_close_dividend_and_split_adjusted(example_moving_average):
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


def test_simple_moving_average_63d_close_split_adjusted(example_moving_average):
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


def test_simple_moving_average_252d_close_dividend_and_split_adjusted(example_moving_average):
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


def test_simple_moving_average_252d_close_split_adjusted(example_moving_average):
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
