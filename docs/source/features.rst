Features
====================

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

Adjustments
-----------

.. autofunction:: calculations.adjusted_high
.. autofunction:: calculations.adjusted_low
.. autofunction:: calculations.adjusted_open
.. autofunction:: calculations.adjusted_price_ratio
.. autofunction:: calculations.adjusted_vwap
.. autofunction:: calculations.log_returns_adjusted_close

Volatility
----------

.. autofunction:: calculations.annualized_volatility_21d
.. autofunction:: calculations.annualized_volatility_252d
.. autofunction:: calculations.annualized_volatility_5d
.. autofunction:: calculations.annualized_volatility_63d
.. autofunction:: calculations.logarithmic_difference_high_low

Volume
------

.. autofunction:: calculations.average_daily_traded_value
.. autofunction:: calculations.average_daily_traded_value_21d
.. autofunction:: calculations.average_daily_traded_value_252d
.. autofunction:: calculations.average_daily_traded_value_5d
.. autofunction:: calculations.average_daily_traded_value_63d
.. autofunction:: calculations.chaikin_money_flow_21d

Market and Fundamental
----------------------

.. autofunction:: calculations.book_to_price
.. autofunction:: calculations.market_cap
.. autofunction:: calculations.sales_to_price

Fundamental
-----------

.. autofunction:: calculations.book_value_per_share
.. autofunction:: calculations.earnings_per_share
.. autofunction:: calculations.earnings_to_price
.. autofunction:: calculations.last_twelve_months_net_income
.. autofunction:: calculations.last_twelve_months_revenue
.. autofunction:: calculations.last_twelve_months_revenue_per_share

Trend
-----

.. autofunction:: calculations.exponential_moving_average_21d
.. autofunction:: calculations.exponential_moving_average_252d
.. autofunction:: calculations.exponential_moving_average_5d
.. autofunction:: calculations.exponential_moving_average_63d
.. autofunction:: calculations.macd_signal_line_9
.. autofunction:: calculations.moving_average_convergence_divergence_26_12
.. autofunction:: calculations.moving_average_convergence_divergence_signal_9
.. autofunction:: calculations.simple_moving_average_21d
.. autofunction:: calculations.simple_moving_average_252d
.. autofunction:: calculations.simple_moving_average_5d
.. autofunction:: calculations.simple_moving_average_63d

Momentum
--------

.. autofunction:: calculations.relative_strength_index_14d

