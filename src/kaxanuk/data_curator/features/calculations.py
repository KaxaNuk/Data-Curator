"""
Features Calculation Functions.

Here you can find how we calculate all features from market, fundamental and alternative data.
You can implement your own columns by injecting a custom_calculations module into data_curator.main().
Each output column corresponds to a function with the exact same name but prepended with "c_".
For example column "my_feature" corresponds to function "c_my_feature".

Every function defines as its parameters the names of the columns it uses as input, which are passed
to it as a DataColumn object wrapping a pyarrow array. You can use basic arithmetic on the DataColumn's
and any rows with nulls, divisions by zero, etc. will return null.

Every function must return an iterable of the same length as the input columns that is compatible with
pyarrow arrays, including pandas.Series, numpy 1-dimensional ndarray and our own DataColumns. The result
will be automatically wrapped into a DataColumn.

* If you need to use pandas methods in your functions, you can convert any DataColumn to pandas.Series
with the .to_pandas() method.
* If you need to use pyarrow.compute methods in your functions, you need to use the .to_pyarrow() method
on the columns.

There's examples of both approaches in the following functions.
"""

import pyarrow

from kaxanuk.data_curator.exceptions import CalculationError

# Here you'll find helper functions for calculating more complicated features:
from kaxanuk.data_curator.features import helpers
# Every calculation function parameter's columnar data is wrapped in this object:
from kaxanuk.data_curator.modules.data_column import DataColumn


# noinspection PyShadowingNames
def c_annualized_volatility_5d_log_returns_dividend_and_split_adjusted(
    c_log_returns_dividend_and_split_adjusted
):
    """
    Calculate the annualized volatility of the dividend-and-split-adjusted log returns over a 5-day period.

    Returns are calculated on the respectively adjusted close prices.

    .. category:: Volatility

    Parameters
    ----------
    c_log_returns_dividend_and_split_adjusted : DataColumn
        The log returns of the adjusted close prices.

    Returns
    -------
    DataColumn
        The annualized volatility over a 5-day period.

    Notes
    -----
    The annualized volatility is calculated as:

    .. math::

        \\mathrm{Annualized\\ Volatility} = \\sigma \\times \\sqrt{252}

    In Excel, assuming your log returns are in column A starting at cell A2:

    1. In cell B6, enter::

           =STDEV.S(A2:A6) * SQRT(252)

    2. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.annualized_volatility(
        column=c_log_returns_dividend_and_split_adjusted,
        days=5
    )
    return output


# noinspection PyShadowingNames
def c_annualized_volatility_21d_log_returns_dividend_and_split_adjusted(
    c_log_returns_dividend_and_split_adjusted
):
    """
    Calculate the annualized volatility of the dividend-and-split-adjusted log returns over a 21-day period.

    Returns are calculated on the respectively adjusted close prices.

    .. category:: Volatility

    Parameters
    ----------
    c_log_returns_dividend_and_split_adjusted : DataColumn
        The log returns of the adjusted close prices.

    Returns
    -------
    DataColumn
        The annualized volatility over a 21-day period.

    Notes
    -----
    The annualized volatility is calculated as:

    .. math::

        \\mathrm{Annualized\\ Volatility} = \\sigma \\times \\sqrt{252}

    In Excel, assuming your log returns are in column A starting at cell A2:

    1. In cell B22, enter::

           =STDEV.S(A2:A22) * SQRT(252)

    2. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.annualized_volatility(
        column=c_log_returns_dividend_and_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_annualized_volatility_63d_log_returns_dividend_and_split_adjusted(
    c_log_returns_dividend_and_split_adjusted
):
    """
    Calculate the annualized volatility of the dividend-and-split-adjusted log returns over a 63-day period.

    Returns are calculated on the respectively adjusted close prices.

    .. category:: Volatility

    Parameters
    ----------
    c_log_returns_dividend_and_split_adjusted : DataColumn
        The log returns of the adjusted close prices.

    Returns
    -------
    DataColumn
        The annualized volatility over a 63-day period.

    Notes
    -----
    The annualized volatility is calculated as:

    .. math::

        \\mathrm{Annualized\\ Volatility} = \\sigma \\times \\sqrt{252}

    In Excel, assuming your log returns are in column A starting at cell A2:

    1. In cell B64, enter::

           =STDEV.S(A2:A64) * SQRT(252)

    2. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.annualized_volatility(
        column=c_log_returns_dividend_and_split_adjusted,
        days=63
    )
    return output


# noinspection PyShadowingNames
def c_annualized_volatility_252d_log_returns_dividend_and_split_adjusted(
    c_log_returns_dividend_and_split_adjusted
):
    """
    Calculate the annualized volatility of the dividend-and-split-adjusted log returns over a 252-day period.

    Returns are calculated on the respectively adjusted close prices.

    .. category:: Volatility

    Parameters
    ----------
    c_log_returns_dividend_and_split_adjusted : DataColumn
        The log returns of the adjusted close prices.

    Returns
    -------
    DataColumn
        The annualized volatility over a 252-day period.

    Notes
    -----
    The annualized volatility is calculated as:

    .. math::

        \\mathrm{Annualized\\ Volatility} = \\sigma \\times \\sqrt{252}

    In Excel, assuming your log returns are in column A starting at cell A2:

    1. In cell B253, enter::

           =STDEV.S(A2:A253) * SQRT(252)

    2. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.annualized_volatility(
        column=c_log_returns_dividend_and_split_adjusted,
        days=252
    )
    return output


# noinspection PyShadowingNames
def c_book_to_price(
    fbs_assets,
    fbs_liabilities,
    fbs_preferred_stock_value,
    c_market_cap
):
    """
    Calculate the book-to-price ratio.

    .. category:: Market and Fundamental

    Parameters
    ----------
    fbs_assets : DataColumn
        The total assets.
    fbs_liabilities : DataColumn
        The total liabilities.
    fbs_preferred_stock_value : DataColumn
        The preferred stock.
    c_market_cap : DataColumn
        The market capitalization.

    Returns
    -------
    DataColumn
        The book-to-price ratio.

    Notes
    -----
    Book-to-price is calculated as:

    .. math::

        \\text{Book-to-Price} = \\frac{
            \\mathrm{Total\\ Assets} - (
                \\mathrm{Total\\ Liabilities} + \\mathrm{Preferred\\ Stock}
            )
        }{
            \\mathrm{Market\\ Cap}
        }

    In Excel, assuming total assets in column A, total liabilities in column B,
    preferred stock in column C, and market cap in column D (starting at row 2):

    1. In cell E2, enter::

           =(A2 - (B2 + C2)) / D2

    2. Drag the formula down through column E.
    """
    output = (
        (
            fbs_assets
            - (
                fbs_liabilities
                + fbs_preferred_stock_value
            )
        )
        / c_market_cap
    )
    return output


# noinspection PyShadowingNames
def c_book_value_per_share(
    fbs_assets,
    fbs_liabilities,
    fbs_preferred_stock_value,
    fis_weighted_average_basic_shares_outstanding
):
    """
    Calculate the book value per share.

    .. category:: Fundamental

    Parameters
    ----------
    fbs_assets : DataColumn
        The total assets.
    fbs_liabilities : DataColumn
        The total liabilities.
    fbs_preferred_stock_value : DataColumn
        The preferred stock value.
    fis_weighted_average_basic_shares_outstanding : DataColumn
        The weighted average basic shares outstanding.

    Returns
    -------
    DataColumn
        The book value per share.

    Notes
    -----
    Book value per share is calculated as:

    .. math::

        \\mathrm{Book\\ Value\\ Per\\ Share} = \\frac{
            \\mathrm{Total\\ Assets} - (
                \\mathrm{Total\\ Liabilities} + \\mathrm{Preferred\\ Stock}
            )
        }{
            \\mathrm{Weighted\\ Average\\ Basic\\ Shares\\ Outstanding}
        }

    In Excel, assuming total assets in column A, liabilities in column B,
    preferred stock in column C, and shares outstanding in column D (starting at row 2):

    1. In cell E2, enter::

           =(A2 - (B2 + C2)) / D2

    2. Drag the formula down through column E.
    """
    output = (
        (
            fbs_assets
            - (
                fbs_liabilities
                + fbs_preferred_stock_value
            )
        )
        / fis_weighted_average_basic_shares_outstanding
    )
    return output


# noinspection PyShadowingNames
def c_chaikin_money_flow_21d_dividend_and_split_adjusted(
    m_high_dividend_and_split_adjusted,
    m_low_dividend_and_split_adjusted,
    m_close_dividend_and_split_adjusted,
    m_volume_dividend_and_split_adjusted
):
    """
    Calculate the 21-day Chaikin Money Flow (CMF) using dividend-and-aplit-adjusted prices.

    .. category:: Volume

    Parameters
    ----------
    m_high_dividend_and_split_adjusted : DataColumn
        The adjusted high prices.
    m_low_dividend_and_split_adjusted : DataColumn
        The adjusted low prices.
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.
    m_volume_dividend_and_split_adjusted : DataColumn
        The market volume.

    Returns
    -------
    DataColumn
        The 21-day Chaikin Money Flow.

    Notes
    -----
    Money flow volume (MFV) for each period is:

    .. math::

        \\mathrm{MFV}_t = \\frac{
            (\\mathrm{Close}_t - \\mathrm{Low}_t) - (\\mathrm{High}_t - \\mathrm{Close}_t)
        }{
            \\mathrm{High}_t - \\mathrm{Low}_t
        } \\times \\mathrm{Volume}_t

    Chaikin Money Flow over 21 days is:

    .. math::

        \\mathrm{CMF}_{21} = \\frac{
            \\sum_{i=1}^{21} \\mathrm{MFV}_i
        }{
            \\sum_{i=1}^{21} \\mathrm{Volume}_i
        }

    In Excel, assuming adjusted high in column A, adjusted low in column B,
    adjusted close in column C, and volume in column D (starting at row 2):

    1. In cell E2, enter::

           =(((C2 - B2) - (A2 - C2)) / (A2 - B2)) * D2

    2. Drag the formula down through cell E22.

    3. In cell F22, enter::

           =SUM(E2:E22) / SUM(D2:D22)

    4. Drag the formula down through column F.
    """
    output = helpers.chaikin_money_flow(
        high=m_high_dividend_and_split_adjusted,
        low=m_low_dividend_and_split_adjusted,
        close=m_close_dividend_and_split_adjusted,
        volume=m_volume_dividend_and_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_chaikin_money_flow_21d_split_adjusted(
    m_high_split_adjusted,
    m_low_split_adjusted,
    m_close_split_adjusted,
    m_volume_split_adjusted
):
    """
    Calculate the 21-day Chaikin Money Flow (CMF) using split-adjusted prices.

    .. category:: Volume

    Parameters
    ----------
    m_high_split_adjusted : DataColumn
        The adjusted high prices.
    m_low_split_adjusted : DataColumn
        The adjusted low prices.
    m_close_split_adjusted : DataColumn
        The adjusted close prices.
    m_volume_split_adjusted : DataColumn
        The market volume.

    Returns
    -------
    DataColumn
        The 21-day Chaikin Money Flow.

    Notes
    -----
    Money flow volume (MFV) for each period is:

    .. math::

        \\mathrm{MFV}_t = \\frac{
            (\\mathrm{Close}_t - \\mathrm{Low}_t) - (\\mathrm{High}_t - \\mathrm{Close}_t)
        }{
            \\mathrm{High}_t - \\mathrm{Low}_t
        } \\times \\mathrm{Volume}_t

    Chaikin Money Flow over 21 days is:

    .. math::

        \\mathrm{CMF}_{21} = \\frac{
            \\sum_{i=1}^{21} \\mathrm{MFV}_i
        }{
            \\sum_{i=1}^{21} \\mathrm{Volume}_i
        }

    In Excel, assuming adjusted high in column A, adjusted low in column B,
    adjusted close in column C, and volume in column D (starting at row 2):

    1. In cell E2, enter::

           =(((C2 - B2) - (A2 - C2)) / (A2 - B2)) * D2

    2. Drag the formula down through cell E22.

    3. In cell F22, enter::

           =SUM(E2:E22) / SUM(D2:D22)

    4. Drag the formula down through column F.
    """
    output = helpers.chaikin_money_flow(
        high=m_high_split_adjusted,
        low=m_low_split_adjusted,
        close=m_close_split_adjusted,
        volume=m_volume_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_daily_traded_value(
    m_vwap,
    m_volume,
    m_vwap_split_adjusted,
    m_volume_split_adjusted,
    m_vwap_dividend_and_split_adjusted,
    m_volume_dividend_and_split_adjusted,
):
    """
    Calculate the daily intraday traded value using the given VWAP and volume.

    First try to use the unadjusted VWAP and volume, then fall back to the split-adjusted VWAP and volume, and finally
    to the dividend-and-split-adjusted VWAP and volume if the previous values were not available from the data provider.

    We can use the unadjusted and adjusted data sets interchangeably because the product of the VWAP and volume should
    be invariant if the adjustments were calculated correctly by the data provider.

    .. category:: Volume

    Parameters
    ----------
    m_vwap : DataColumn
        The VWAP of the market.
    m_volume : DataColumn
        The volume of the market.
    m_vwap_split_adjusted : DataColumn
        The split-adjusted VWAP.
    m_volume_split_adjusted : DataColumn
        The split-adjusted volume.
    m_vwap_dividend_and_split_adjusted : DataColumn
        The dividend and split-adjusted VWAP.
    m_volume_dividend_and_split_adjusted : DataColumn
        The dividend and split-adjusted volume.

    Returns
    -------
    DataColumn
        The average daily traded value.

    Notes
    -----
    The average daily traded value is calculated as:

    .. math::

        \\mathrm{Average\\ Daily\\ Traded\\ Value} = \\mathrm{VWAP} \\times \\mathrm{Volume}

    In Excel, assuming VWAP values are in column A (starting at cell A2)
    and volume values are in column B (starting at cell B2):

    1. In cell C2, enter::

           =A2*B2

    2. Drag the formula down to apply it to the remaining rows.
    """
    if (
        not m_vwap.is_null()
        and not m_volume.is_null()
    ):
        output = m_vwap * m_volume
    elif (
        not m_vwap_split_adjusted.is_null()
        and not m_volume_split_adjusted.is_null()
    ):
        output = m_vwap_split_adjusted * m_volume_split_adjusted
    else:
        output = m_vwap_dividend_and_split_adjusted * m_volume_dividend_and_split_adjusted
    return output


# noinspection PyShadowingNames
def c_daily_traded_value_sma_5d(c_daily_traded_value):
    """
    Calculate the 5-day simple moving average of the daily traded value.

    .. category:: Volume

    Parameters
    ----------
    c_daily_traded_value : DataColumn
        The daily traded value.

    Returns
    -------
    DataColumn
        The 5-day moving average of the daily traded value.

    Notes
    -----
    The 5-day moving average is computed as:

    .. math::

        \\mathrm{MA}_{5} = \\frac{1}{5} \\sum_{i=1}^{5} \\mathrm{Daily\\ Traded\\ Value}_{i}

    In Excel, assuming your daily traded values are in column A starting at cell A2:

    1. In cell B6, enter:
        **=AVERAGE(A2:A6)**
    2. Drag the formula from cell B6 down to calculate the 5-day moving average for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=c_daily_traded_value,
        days=5
    )
    return output


# noinspection PyShadowingNames
def c_daily_traded_value_sma_21d(c_daily_traded_value):
    """
    Calculate the 21-day simple moving average of the daily traded value.

    .. category:: Volume

    Parameters
    ----------
    c_daily_traded_value : DataColumn
        The daily traded value.

    Returns
    -------
    DataColumn
        The 21-day moving average of the daily traded value.

    Notes
    -----
    The 21-day moving average is computed as:

    .. math::

        \\mathrm{MA}_{21} = \\frac{1}{21} \\sum_{i=1}^{21} \\mathrm{Daily\\ Traded\\ Value}_{i}

    In Excel, assuming your daily traded values are in column A starting at cell A2:

    1. In cell B22, enter:
        **=AVERAGE(A2:A22)**
    2. Drag the formula from cell B22 down to calculate the 21-day moving average for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=c_daily_traded_value,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_daily_traded_value_sma_63d(c_daily_traded_value):
    """
    Calculate the 63-day simple moving average of the daily traded value.

    .. category:: Volume

    Parameters
    ----------
    c_daily_traded_value : DataColumn
        The daily traded value.

    Returns
    -------
    DataColumn
        The 63-day moving average of the daily traded value.

    Notes
    -----
    The 63-day moving average is computed as:

    .. math::

        \\mathrm{MA}_{63} = \\frac{1}{63} \\sum_{i=1}^{63} \\mathrm{Daily\\ Traded\\ Value}_{i}

    In Excel, assuming your daily traded values are in column A starting at cell A2:

    1. In cell B64, enter:
        **=AVERAGE(A2:A64)**
    2. Drag the formula from cell B64 down to calculate the 63-day moving average for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=c_daily_traded_value,
        days=63
    )
    return output


# noinspection PyShadowingNames
def c_daily_traded_value_sma_252d(c_daily_traded_value):
    """
    Calculate the 252-day simple moving average of the daily traded value.

    .. category:: Volume

    Parameters
    ----------
    c_daily_traded_value : DataColumn
        The daily traded value.

    Returns
    -------
    DataColumn
        The 252-day moving average of the daily traded value.

    Notes
    -----
    The 252-day moving average is computed as:

    .. math::

        \\mathrm{MA}_{252} = \\frac{1}{252} \\sum_{i=1}^{252} \\mathrm{Daily\\ Traded\\ Value}_{i}

    In Excel, assuming your daily traded values are in column A starting at cell A2:

    1. In cell B253, enter:
        **=AVERAGE(A2:A253)**
    2. Drag the formula from cell B253 down to calculate the 252-day moving average for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=c_daily_traded_value,
        days=252
    )
    return output


# noinspection PyShadowingNames
def c_earnings_per_share(
    c_last_twelve_months_net_income,
    fis_weighted_average_basic_shares_outstanding
):
    """
    Calculate the earnings per share (EPS).

    EPS is calculated using last twelve months net income and weighted average basic shares outstanding.

    .. category:: Fundamental

    Parameters
    ----------
    c_last_twelve_months_net_income : DataColumn
        The net income over the last twelve months.
    fis_weighted_average_basic_shares_outstanding : DataColumn
        The weighted average basic shares outstanding.

    Returns
    -------
    DataColumn
        The earnings per share.

    Notes
    -----
    Earnings per share is calculated as:

    .. math::

        \\mathrm{EPS} = \\frac{\\mathrm{Net\\ Income}}{\\mathrm{Weighted\\ Average\\ Basic\\ Shares\\ Outstanding}}

    In Excel, assuming net income in column A and shares outstanding in column B (starting at cell A2):

    1. In cell C2, enter::

           =A2/B2

    2. Drag the formula down to apply it to the remaining rows.
    """
    output = c_last_twelve_months_net_income / fis_weighted_average_basic_shares_outstanding
    return output


# noinspection PyShadowingNames
def c_earnings_to_price(
    c_last_twelve_months_net_income,
    c_market_cap
):
    """
    Calculate the earnings-to-price (E/P) ratio.

    The E/P ratio is calculated using last twelve months net income and market capitalization.

    .. category:: Fundamental

    Parameters
    ----------
    c_last_twelve_months_net_income : DataColumn
        The net income over the last twelve months.
    c_market_cap : DataColumn
        The market capitalization.

    Returns
    -------
    DataColumn
        The earnings-to-price ratio.

    Notes
    -----
    Earnings-to-price ratio is calculated as:

    .. math::

        \\frac{\\mathrm{Net\\ Income}}{\\mathrm{Market\\ Cap}}

    In Excel, assuming net income in column A and market cap in column B (starting at cell A2):

    1. In cell C2, enter::

           =A2/B2

    2. Drag the formula down to apply it to the remaining rows.
    """
    output = c_last_twelve_months_net_income / c_market_cap
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_5d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 5-day (1 week) exponential moving average (EMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 5-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 5. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

           =AVERAGE(A2:A6)

    2. In cell B7, enter::

           =(A7 * (2/(5+1))) + (B6 * (1 - (2/(5+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=5
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_5d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 5-day (1 week) exponential moving average (EMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 5-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 5. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

           =AVERAGE(A2:A6)

    2. In cell B7, enter::

           =(A7 * (2/(5+1))) + (B6 * (1 - (2/(5+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_split_adjusted,
        days=5
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_21d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 21-day (1 month) exponential moving average (EMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 21-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 21. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B22, enter::

           =AVERAGE(A2:A22)

    2. In cell B23, enter::

           =(A23 * (2/(21+1))) + (B22 * (1 - (2/(21+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_21d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 21-day (one month) exponential moving average (EMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 21-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 21. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B22, enter::

           =AVERAGE(A2:A22)

    2. In cell B23, enter::

           =(A23 * (2/(21+1))) + (B22 * (1 - (2/(21+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_63d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 63-day (3 month) exponential moving average (EMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 63-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 63. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B64, enter::

           =AVERAGE(A2:A64)

    2. In cell B65, enter::

           =(A65 * (2/(63+1))) + (B64 * (1 - (2/(63+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=63
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_63d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 63-day (3 month) exponential moving average (EMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 63-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 63. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B64, enter::

           =AVERAGE(A2:A64)

    2. In cell B65, enter::

           =(A65 * (2/(63+1))) + (B64 * (1 - (2/(63+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_split_adjusted,
        days=63
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_252d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 252-day (1 year) exponential moving average (EMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 252-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 252. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B253, enter::

           =AVERAGE(A2:A253)

    2. In cell B254, enter::

           =(A254 * (2/(252+1))) + (B253 * (1 - (2/(252+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=252
    )
    return output


# noinspection PyShadowingNames
def c_exponential_moving_average_252d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 252-day (1 year) exponential moving average (EMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 252-day exponential moving average.

    Notes
    -----
    The smoothing factor K is given by:

    .. math::

        K = \\frac{2}{n + 1}

    with n = 252. The EMA is then:

    .. math::

        \\mathrm{EMA}_{\\mathrm{current}} = (\\mathrm{Adjusted\\ Close} \\times K)
        + (\\mathrm{EMA}_{\\mathrm{previous}} \\times (1 - K))

    In Excel, assuming adjusted close prices are in column A starting at cell A2:

    1. In cell B253, enter::

           =AVERAGE(A2:A253)

    2. In cell B254, enter::

           =(A254 * (2/(252+1))) + (B253 * (1 - (2/(252+1))))

    3. Drag the formula down to apply it to the remaining rows.
    """
    output = helpers.exponential_moving_average(
        column=m_close_split_adjusted,
        days=252
    )
    return output


# noinspection PyShadowingNames
def c_last_twelve_months_net_income(
    fis_net_income,
    f_fiscal_year,
    f_fiscal_period,
    configuration
):
    """
    Calculate the last twelve months net income (LTM net income).

    For quarterly periods this function computes the rolling sum of net income over the last four quarters.

    .. category:: Fundamental

    Parameters
    ----------
    fis_net_income : DataColumn
        The net income for each period.
    f_fiscal_year : DataColumn
        The fiscal year for each period.
    f_fiscal_period : DataColumn
        The fiscal period (e.g., quarter) within each fiscal year.
    configuration : Configuration
        Configuration object with attribute `period` ("quarterly" or "annual").

    Returns
    -------
    DataColumn
        The last twelve months net income.

    Notes
    -----
    For quarterly data, LTM net income is the rolling sum of the current and previous three quarters:

    .. math::

        \\mathrm{LTM\\ Net\\ Income}_t = \\sum_{i=0}^{3} \\mathrm{Net\\ Income}_{t-i}

    For annual data, return the period net income directly.
    """
    if fis_net_income.is_null():
        output = fis_net_income
    elif f_fiscal_year.is_null():
        output = f_fiscal_year
    elif f_fiscal_period.is_null():
        output = f_fiscal_period
    elif configuration.period == 'quarterly':
        period_keys = DataColumn.concatenate(f_fiscal_year, f_fiscal_period)
        output = helpers.indexed_rolling_window_operation(
            key_column=period_keys,
            value_column=fis_net_income,
            operation_function=sum,
            window_length=4
        )
    elif configuration.period == 'annual':
        output = fis_net_income
    else:
        raise CalculationError(
            f"calculations - last_twelve_months_net_income failed, unexpected period type: {configuration.period}"
        )
    return output


# noinspection PyShadowingNames
def c_last_twelve_months_revenue(
    fis_revenues,
    f_fiscal_year,
    f_fiscal_period,
    configuration
):
    """
    Calculate the last twelve months revenue (LTM revenue).

    For quarterly periods this function computes the rolling sum of revenues over the last four quarters.

    .. category:: Fundamental

    Parameters
    ----------
    fis_revenues : DataColumn
        The total revenues for each period.
    f_fiscal_year : DataColumn
        The fiscal year for each period.
    f_fiscal_period : DataColumn
        The fiscal period (e.g., quarter) within each fiscal year.
    configuration : Configuration
        Configuration object with attribute `period` ("quarterly" or "annual").

    Returns
    -------
    DataColumn
        The last twelve months revenue.

    Notes
    -----
    For quarterly data, LTM revenue is the rolling sum of the current and previous three quarters:

    .. math::

        \\mathrm{LTM\\ Revenue}_t = \\sum_{i=0}^{3} \\mathrm{Revenue}_{t-i}

    For annual data, return the period revenue directly.
    """
    if fis_revenues.is_null():
        output = fis_revenues
    elif f_fiscal_year.is_null():
        output = f_fiscal_year
    elif f_fiscal_period.is_null():
        output = f_fiscal_period
    elif configuration.period == 'quarterly':
        period_keys = DataColumn.concatenate(f_fiscal_year, f_fiscal_period)
        output = helpers.indexed_rolling_window_operation(
            key_column=period_keys,
            value_column=fis_revenues,
            operation_function=sum,
            window_length=4
        )
    elif configuration.period == 'annual':
        output = fis_revenues
    else:
        raise CalculationError(
            f"calculations - last_twelve_months_revenue failed, unexpected period type: {configuration.period}"
        )
    return output


# noinspection PyShadowingNames
def c_last_twelve_months_revenue_per_share(
    c_last_twelve_months_revenue,
    fis_weighted_average_basic_shares_outstanding
):
    """
    Calculate the last twelve months revenue per share, using weighted average basic shares outstanding.

    .. category:: Fundamental

    Parameters
    ----------
    c_last_twelve_months_revenue : DataColumn
        The last twelve months revenue.
    fis_weighted_average_basic_shares_outstanding : DataColumn
        The weighted average basic shares outstanding.

    Returns
    -------
    DataColumn
        The last twelve months revenue per share.

    Notes
    -----
    Revenue per share is calculated as:

    .. math::

        \\mathrm{Revenue\\ Per\\ Share}
        =
        \\frac{\\mathrm{LTM\\ Revenue}}
                 {\\mathrm{Weighted\\ Average\\ Basic\\ Shares\\ Outstanding}}


    In Excel, assuming LTM revenue in column A and shares outstanding in column B (starting at cell A2):

    1. In cell C2, enter::

           =A2/B2

    2. Drag the formula down to apply to subsequent rows.
    """
    output = c_last_twelve_months_revenue / fis_weighted_average_basic_shares_outstanding
    return output


# noinspection PyShadowingNames
def c_log_difference_high_to_low(
    m_high,
    m_low
):
    """
    Calculate the logarithmic difference between unadjusted high and low prices.

    .. category:: Volatility

    Parameters
    ----------
    m_high : DataColumn
        The high prices.
    m_low : DataColumn
        The low prices.

    Returns
    -------
    DataColumn
        The logarithmic difference between high and low prices.

    Notes
    -----
    Since `log(high/low) = log(high) - log(low)`, log difference can be calculated as:

    .. math::

        \\mathrm{LogDiff} = \\ln\\left(\\frac{\\mathrm{High}}{\\mathrm{Low}}\\right)

    In Excel, assuming high in column A and low in column B (starting at cell A2):

    1. In cell C2, enter::

           =LN(A2/B2)

    2. Drag the formula down to apply to subsequent rows.
    """
    rebased = m_high / m_low
    result = pyarrow.compute.ln(
        rebased.to_pyarrow()
    )
    output = helpers.replace_infinite_with_none(
        DataColumn.load(result)
    )
    return output


# noinspection PyShadowingNames
def c_log_returns_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the logarithmic returns of dividend-and-split-adjusted close prices.

    Returns are calculated between consecutive adjusted close prices.

    .. category:: Adjustments

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The log returns of the adjusted close prices.

    Notes
    -----
    Log returns are computed as:

    .. math::

        \\mathrm{Log\\ Returns}_t = \\ln\\left(\\frac{P_t}{P_{t-1}}\\right)

    In Excel, assuming adjusted close prices in column A starting at cell A2:

    1. In cell B2, enter::

           =LN(A2/A1)

    2. Drag the formula down to apply to subsequent rows.
    """
    output = helpers.log_returns(m_close_dividend_and_split_adjusted)
    return output


# noinspection PyShadowingNames
def c_macd_26d_12d_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the MACD (Moving Average Convergence Divergence) for 12 and 26 day periods, dividend-and-split-adjusted.

    MACD is calculated as the difference between 12-day and 26-day EMAs of close prices.
    This function uses dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The MACD values.

    Notes
    -----
    MACD is calculated as:

    .. math::

        \\mathrm{MACD}_{26,12} = \\mathrm{EMA}_{12} - \\mathrm{EMA}_{26}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. To calculate the 12-day EMA:
       - In cell B13, enter::

             =AVERAGE(A2:A13)

       - In cell B14, enter::

             =(A14 * (2/(12+1))) + (B13 * (1 - (2/(12+1))))

       - Drag the formula in B14 down through column B.

    2. To calculate the 26-day EMA:
       - In cell C27, enter::

             =AVERAGE(A2:A27)

       - In cell C28, enter::

             =(A28 * (2/(26+1))) + (C27 * (1 - (2/(26+1))))

       - Drag the formula in C28 down through column C.

    3. To compute the MACD line:
       - In cell D27, enter::

             =B27 - C27

       - Drag the formula in D27 down through column D.
    """
    ema12 = helpers.exponential_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=12
    )
    ema26 = helpers.exponential_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=26
    )
    output = ema12 - ema26
    return output


# noinspection PyShadowingNames
def c_macd_26d_12d_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the MACD (Moving Average Convergence Divergence) for 12 and 26 day periods, split-adjusted.

    MACD is calculated as the difference between 12-day and 26-day EMAs of close prices.
    This function uses split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The MACD values.

    Notes
    -----
    MACD is calculated as:

    .. math::

        \\mathrm{MACD}_{26,12} = \\mathrm{EMA}_{12} - \\mathrm{EMA}_{26}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. To calculate the 12-day EMA:
       - In cell B13, enter::

             =AVERAGE(A2:A13)

       - In cell B14, enter::

             =(A14 * (2/(12+1))) + (B13 * (1 - (2/(12+1))))

       - Drag the formula in B14 down through column B.

    2. To calculate the 26-day EMA:
       - In cell C27, enter::

             =AVERAGE(A2:A27)

       - In cell C28, enter::

             =(A28 * (2/(26+1))) + (C27 * (1 - (2/(26+1))))

       - Drag the formula in C28 down through column C.

    3. To compute the MACD line:
       - In cell D27, enter::

             =B27 - C27

       - Drag the formula in D27 down through column D.
    """
    ema12 = helpers.exponential_moving_average(
        column=m_close_split_adjusted,
        days=12
    )
    ema26 = helpers.exponential_moving_average(
        column=m_close_split_adjusted,
        days=26
    )
    output = ema12 - ema26
    return output


# noinspection PyShadowingNames
def c_macd_signal_9d_dividend_and_split_adjusted(
    c_macd_26d_12d_dividend_and_split_adjusted
):
    """
    Calculate the MACD signal line (9-day EMA of MACD) for dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    c_macd_26d_12d_dividend_and_split_adjusted : DataColumn
        The MACD values calculated from the 12-day and 26-day EMAs of the adjusted close prices.

    Returns
    -------
    DataColumn
        The MACD signal line values.

    Notes
    -----
    MACD is computed as the difference between the 12-day and 26-day EMAs:

    .. math::

        \\mathrm{MACD}_{26,12} = \\mathrm{EMA}_{12} - \\mathrm{EMA}_{26}

    The signal line is the 9-day EMA of the MACD series:

    .. math::

        \\mathrm{Signal}_9 = \\mathrm{EMA}_9(\\mathrm{MACD}_{26,12})

    In Excel, assuming your adjusted close prices start in column A at cell A2:

    1. Calculate the 12-day EMA:
       - In cell B13, enter::


           =AVERAGE(A2:A13)

       - In cell B14, enter::


           =(A14*(2/(12+1)))+(B13*(1-(2/(12+1))))

       - Drag the formula down through column B.

    2. Calculate the 26-day EMA:
       - In cell C27, enter::


           =AVERAGE(A2:A27)

       - In cell C28, enter::


           =(A28*(2/(26+1)))+(C27*(1-(2/(26+1))))

       - Drag the formula down through column C.

    3. Calculate the MACD line:
       - In cell D28, enter::


           =B28 - C28

       - Drag the formula down through column D.

    4. Calculate the 9-day EMA of the MACD (signal line):
       - In cell E36, enter::


           =AVERAGE(D28:D36)

       - In cell E37, enter::


           =(D37*(2/(9+1)))+(E36*(1-(2/(9+1))))

       - Drag the formula down through column E.
    """
    output = helpers.exponential_moving_average(
        column=c_macd_26d_12d_dividend_and_split_adjusted,
        days=9
    )
    return output


# noinspection PyShadowingNames
def c_macd_signal_9d_split_adjusted(
    c_macd_26d_12d_split_adjusted
):
    """
    Calculate the MACD signal line (9-day EMA of MACD) for split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    c_macd_26d_12d_split_adjusted : DataColumn
        The MACD values calculated from the 12-day and 26-day EMAs of the adjusted close prices.

    Returns
    -------
    DataColumn
        The MACD signal line values.

    Notes
    -----
    MACD is computed as the difference between the 12-day and 26-day EMAs:

    .. math::

        \\mathrm{MACD}_{26,12} = \\mathrm{EMA}_{12} - \\mathrm{EMA}_{26}

    The signal line is the 9-day EMA of the MACD series:

    .. math::

        \\mathrm{Signal}_9 = \\mathrm{EMA}_9(\\mathrm{MACD}_{26,12})

    In Excel, assuming your adjusted close prices start in column A at cell A2:

    1. Calculate the 12-day EMA:
       - In cell B13, enter::


           =AVERAGE(A2:A13)

       - In cell B14, enter::


           =(A14*(2/(12+1)))+(B13*(1-(2/(12+1))))

       - Drag the formula down through column B.

    2. Calculate the 26-day EMA:
       - In cell C27, enter::


           =AVERAGE(A2:A27)

       - In cell C28, enter::


           =(A28*(2/(26+1)))+(C27*(1-(2/(26+1))))

       - Drag the formula down through column C.

    3. Calculate the MACD line:
       - In cell D28, enter::


           =B28 - C28

       - Drag the formula down through column D.

    4. Calculate the 9-day EMA of the MACD (signal line):
       - In cell E36, enter::


           =AVERAGE(D28:D36)

       - In cell E37, enter::


           =(D37*(2/(9+1)))+(E36*(1-(2/(9+1))))

       - Drag the formula down through column E.
    """
    output = helpers.exponential_moving_average(
        column=c_macd_26d_12d_split_adjusted,
        days=9
    )
    return output


# noinspection PyShadowingNames
def c_market_cap(
    m_close_split_adjusted,
    fis_weighted_average_diluted_shares_outstanding
):
    """
    Calculate the market capitalization using unadjusted close prices and weighted average diluted shares outstanding.

    .. category:: Market and Fundamental

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted closing prices.
    fis_weighted_average_diluted_shares_outstanding : DataColumn
        The weighted average of the diluted shares outstanding.

    Returns
    -------
    DataColumn
        The market capitalization.

    Notes
    -----
    Market cap is calculated as:

    .. math::

        \\mathrm{Market\\ Cap} = \\mathrm{Close} \\times \\mathrm{Weighted\\ Average\\ Diluted\\ Shares\\ Outstanding}

    In Excel, assuming close in column A and shares in column B:

    1. In cell C2, enter::

           =A2 * B2

    2. Drag the formula down to apply to subsequent rows.
    """
    output = m_close_split_adjusted * fis_weighted_average_diluted_shares_outstanding
    return output


# noinspection PyShadowingNames
def c_rsi_14d_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 14-day relative strength index (RSI) for dividend-and-split-adjusted close prices.

    .. category:: Momentum

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 14-day relative strength index.

    Notes
    -----
    The RSI is computed as:

    .. math::

        \\mathrm{RSI}_{14} = 100 - \\frac{100}{1 + \\dfrac{\\mathrm{Average\\ Gain}}{\\mathrm{Average\\ Loss}}}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B3, enter::

        =MAX(A3 - A2, 0)

       and drag down to compute daily gains.

    2. In cell C3, enter::

        =MAX(A2 - A3, 0)

       and drag down to compute daily losses.

    3. In cell D16, enter::

        =AVERAGE(B3:B16)

       to compute the 14-day average gain.

    4. In cell E16, enter::

        =AVERAGE(C3:C16)

       to compute the 14-day average loss.

    5. In cell F16, enter::

        =100 - (100 / (1 + (D16 / E16)))

       to compute the 14-day RSI.
    """
    output = helpers.relative_strength_index(
        column=m_close_dividend_and_split_adjusted,
        days=14
    )
    return output


# noinspection PyShadowingNames
def c_rsi_14d_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 14-day relative strength index (RSI) for split-adjusted close prices.

    .. category:: Momentum

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 14-day relative strength index.

    Notes
    -----
    The RSI is computed as:

    .. math::

        \\mathrm{RSI}_{14} = 100 - \\frac{100}{1 + \\dfrac{\\mathrm{Average\\ Gain}}{\\mathrm{Average\\ Loss}}}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B3, enter::

        =MAX(A3 - A2, 0)

       and drag down to compute daily gains.

    2. In cell C3, enter::

        =MAX(A2 - A3, 0)

       and drag down to compute daily losses.

    3. In cell D16, enter::

        =AVERAGE(B3:B16)

       to compute the 14-day average gain.

    4. In cell E16, enter::

        =AVERAGE(C3:C16)

       to compute the 14-day average loss.

    5. In cell F16, enter::

        =100 - (100 / (1 + (D16 / E16)))

       to compute the 14-day RSI.
    """
    output = helpers.relative_strength_index(
        column=m_close_split_adjusted,
        days=14
    )
    return output


# noinspection PyShadowingNames
def c_sales_to_price(
    c_last_twelve_months_revenue,
    c_market_cap
):
    """
    Calculate the sales-to-price (S/P) ratio.

    .. category:: Market and Fundamental

    Parameters
    ----------
    c_last_twelve_months_revenue : DataColumn
        The last twelve months revenue.
    c_market_cap : DataColumn
        The market capitalization.

    Returns
    -------
    DataColumn
        The sales-to-price ratio.

    Notes
    -----
    The sales-to-price ratio is computed as:

    .. math::

        \\mathrm{Sales\\ to\\ Price} = \\frac{\\mathrm{Revenue}}{\\mathrm{Market\\ Cap}}

    In Excel, assuming LTM revenue in column A and market cap in column B (starting at row 2):

    1. In cell C2, enter::

        =A2 / B2

    2. Drag the formula down to apply it to subsequent rows.
    """
    output = c_last_twelve_months_revenue / c_market_cap
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_5d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 5-day (1 week) simple moving average (SMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 5-day simple moving average.

    Notes
    -----
    The 5-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{5} = \\frac{1}{5} \\sum_{i=0}^{5} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A6)

    2. Drag the formula down to calculate the 5-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=5
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_5d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 5-day (1 week) simple moving average (SMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 5-day simple moving average.

    Notes
    -----
    The 5-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{5} = \\frac{1}{5} \\sum_{i=0}^{5} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A6)

    2. Drag the formula down to calculate the 5-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_split_adjusted,
        days=5
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_21d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 21-day (1 month) simple moving average (SMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 21-day simple moving average.

    Notes
    -----
    The 21-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{21} = \\frac{1}{21} \\sum_{i=0}^{21} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A22)

    2. Drag the formula down to calculate the 21-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_21d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 21-day (1 month) simple moving average (SMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 21-day simple moving average.

    Notes
    -----
    The 21-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{21} = \\frac{1}{21} \\sum_{i=0}^{21} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A22)

    2. Drag the formula down to calculate the 21-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_split_adjusted,
        days=21
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_63d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 63-day (3 month) simple moving average (SMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 63-day simple moving average.

    Notes
    -----
    The 63-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{63} = \\frac{1}{63} \\sum_{i=0}^{63} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A64)

    2. Drag the formula down to calculate the 63-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=63
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_63d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 63-day (3 month) simple moving average (SMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 63-day simple moving average.

    Notes
    -----
    The 63-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{63} = \\frac{1}{63} \\sum_{i=0}^{63} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A64)

    2. Drag the formula down to calculate the 63-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_split_adjusted,
        days=63
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_252d_close_dividend_and_split_adjusted(
    m_close_dividend_and_split_adjusted
):
    """
    Calculate the 252-day (1 year) simple moving average (SMA) of the dividend-and-split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 252-day simple moving average.

    Notes
    -----
    The 252-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{252} = \\frac{1}{252} \\sum_{i=0}^{252} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A253)

    2. Drag the formula down to calculate the 252-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_dividend_and_split_adjusted,
        days=252
    )
    return output


# noinspection PyShadowingNames
def c_simple_moving_average_252d_close_split_adjusted(
    m_close_split_adjusted
):
    """
    Calculate the 252-day (1 year) simple moving average (SMA) of the split-adjusted close prices.

    .. category:: Trend

    Parameters
    ----------
    m_close_split_adjusted : DataColumn
        The adjusted close prices.

    Returns
    -------
    DataColumn
        The 252-day simple moving average.

    Notes
    -----
    The 252-day simple moving average is computed as:

    .. math::

        \\mathrm{SMA}_{252} = \\frac{1}{252} \\sum_{i=0}^{252} \\mathrm{Adjusted\\ Close}_{t-i}

    In Excel, assuming your adjusted close prices are in column A starting at cell A2:

    1. In cell B6, enter::

        =AVERAGE(A2:A253)

    2. Drag the formula down to calculate the 252-day SMA for the rest of the rows.
    """
    output = helpers.simple_moving_average(
        column=m_close_split_adjusted,
        days=252
    )
    return output
