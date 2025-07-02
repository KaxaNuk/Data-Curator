.. _features:

Features
========

.. currentmodule:: kaxanuk.data_curator.features.calculations

Available Calculation Functions
-------------------------------

This section lists all the **predefined calculation functions** provided by Data Curator.
Each function corresponds to a feature that can be used as an output column in your Excel configuration file.

To use one of these features:

- Reference its name (without the ``c_`` prefix) in the ``Output_Columns`` sheet.
- The system will automatically match it to the Python function ``c_<name>``.

All functions operate on `DataColumn` inputs and return iterable values compatible with our internal data.

Adjustments
~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_log_returns_dividend_and_split_adjusted <c_log_returns_dividend_and_split_adjusted_ref>`

Fundamental
~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_book_value_per_share <c_book_value_per_share_ref>`
   * - :ref:`c_earnings_per_share <c_earnings_per_share_ref>`
   * - :ref:`c_earnings_to_price <c_earnings_to_price_ref>`
   * - :ref:`c_last_twelve_months_net_income <c_last_twelve_months_net_income_ref>`
   * - :ref:`c_last_twelve_months_revenue <c_last_twelve_months_revenue_ref>`
   * - :ref:`c_last_twelve_months_revenue_per_share <c_last_twelve_months_revenue_per_share_ref>`

Market and Fundamental
~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_book_to_price <c_book_to_price_ref>`
   * - :ref:`c_market_cap <c_market_cap_ref>`
   * - :ref:`c_sales_to_price <c_sales_to_price_ref>`

Momentum
~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_rsi_14d_dividend_and_split_adjusted <c_rsi_14d_dividend_and_split_adjusted_ref>`
   * - :ref:`c_rsi_14d_split_adjusted <c_rsi_14d_split_adjusted_ref>`

Trend
~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_exponential_moving_average_21d_close_dividend_and_split_adjusted <c_exponential_moving_average_21d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_21d_close_split_adjusted <c_exponential_moving_average_21d_close_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_252d_close_dividend_and_split_adjusted <c_exponential_moving_average_252d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_252d_close_split_adjusted <c_exponential_moving_average_252d_close_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_5d_close_dividend_and_split_adjusted <c_exponential_moving_average_5d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_5d_close_split_adjusted <c_exponential_moving_average_5d_close_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_63d_close_dividend_and_split_adjusted <c_exponential_moving_average_63d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_exponential_moving_average_63d_close_split_adjusted <c_exponential_moving_average_63d_close_split_adjusted_ref>`
   * - :ref:`c_macd_26d_12d_dividend_and_split_adjusted <c_macd_26d_12d_dividend_and_split_adjusted_ref>`
   * - :ref:`c_macd_26d_12d_split_adjusted <c_macd_26d_12d_split_adjusted_ref>`
   * - :ref:`c_macd_signal_9d_dividend_and_split_adjusted <c_macd_signal_9d_dividend_and_split_adjusted_ref>`
   * - :ref:`c_macd_signal_9d_split_adjusted <c_macd_signal_9d_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_21d_close_dividend_and_split_adjusted <c_simple_moving_average_21d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_21d_close_split_adjusted <c_simple_moving_average_21d_close_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_252d_close_dividend_and_split_adjusted <c_simple_moving_average_252d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_252d_close_split_adjusted <c_simple_moving_average_252d_close_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_5d_close_dividend_and_split_adjusted <c_simple_moving_average_5d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_5d_close_split_adjusted <c_simple_moving_average_5d_close_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_63d_close_dividend_and_split_adjusted <c_simple_moving_average_63d_close_dividend_and_split_adjusted_ref>`
   * - :ref:`c_simple_moving_average_63d_close_split_adjusted <c_simple_moving_average_63d_close_split_adjusted_ref>`

Volatility
~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_annualized_volatility_21d_log_returns_dividend_and_split_adjusted <c_annualized_volatility_21d_log_returns_dividend_and_split_adjusted_ref>`
   * - :ref:`c_annualized_volatility_252d_log_returns_dividend_and_split_adjusted <c_annualized_volatility_252d_log_returns_dividend_and_split_adjusted_ref>`
   * - :ref:`c_annualized_volatility_5d_log_returns_dividend_and_split_adjusted <c_annualized_volatility_5d_log_returns_dividend_and_split_adjusted_ref>`
   * - :ref:`c_annualized_volatility_63d_log_returns_dividend_and_split_adjusted <c_annualized_volatility_63d_log_returns_dividend_and_split_adjusted_ref>`
   * - :ref:`c_log_difference_high_to_low <c_log_difference_high_to_low_ref>`

Volume
~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`c_chaikin_money_flow_21d_dividend_and_split_adjusted <c_chaikin_money_flow_21d_dividend_and_split_adjusted_ref>`
   * - :ref:`c_chaikin_money_flow_21d_split_adjusted <c_chaikin_money_flow_21d_split_adjusted_ref>`
   * - :ref:`c_daily_traded_value <c_daily_traded_value_ref>`
   * - :ref:`c_daily_traded_value_sma_21d <c_daily_traded_value_sma_21d_ref>`
   * - :ref:`c_daily_traded_value_sma_252d <c_daily_traded_value_sma_252d_ref>`
   * - :ref:`c_daily_traded_value_sma_5d <c_daily_traded_value_sma_5d_ref>`
   * - :ref:`c_daily_traded_value_sma_63d <c_daily_traded_value_sma_63d_ref>`

.. toctree::
   :hidden:
   :maxdepth: 1

   api/adjustments/index
   api/fundamental/index
   api/market_and_fundamental/index
   api/momentum/index
   api/trend/index
   api/volatility/index
   api/volume/index
