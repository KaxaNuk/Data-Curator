.. _feature_naming:

Feature Tag Homogenization
===========================

Calculated features are generated columns or series derived from one or more raw data fields (e.g., moving averages, volatilities, or technical indicators). The names of these features must be consistent, descriptive, and concise. Below are the rules used to construct all calculated feature names.

Prefix Mapping
--------------

- **c_** – Every single‐column calculated feature (and its corresponding function) begins with the ``c_`` prefix.

Naming Rules for Calculated Features
------------------------------------

1. **Single Snake_Case Identifier**

   - Feature names must be a single “word” in ``snake_case`` to serve as valid Python parameter and function names.
   - Example: ``c_simple_moving_average_5d``

2. **Time Parameter Encoding**

   - If the feature uses a fixed time parameter, encode it with ``<number><letter>`` where:

     - ``d`` = days
     - ``w`` = weeks
     - ``m`` = months
     - ``y`` = years

   - Place this ``<number><letter>`` term at the end of the descriptive phrase, separated by an underscore.
   - Example:

     - A 5‐day moving average: ``c_simple_moving_average_5d``
     - A 3‐month volatility: ``c_annualized_volatility_3m``

3. **Preference for Days**

   - Use days (``d``) as the default period type when feasible for maximum clarity (e.g., ``c_returns_20d`` rather than ``c_returns_4w``).

4. **Full Name for Elementary Concepts**

   - For simple features that do not depend on other calculated features, spell out the full concept in words, even if an acronym exists.
   - Example:

     - ``c_simple_moving_average_5d`` (not ``c_sma_5d``)

5. **Industry‐Standard Abbreviations for Complex Indicators**

   - For more sophisticated technical indicators or features built on multiple sub‐features, use standard financial acronyms if they are widely recognized.
   - Examples:

     - ``c_rsi_14d`` (Relative Strength Index over 14 days)
     - ``c_ema_20d_50d_difference`` (difference between 20-day and 50-day Exponential Moving Averages)

6. **Ordering by Relevance**

   - Place the most relevant terms near the beginning of the name so that alphabetically sorted lists surface the key concept first.
   - Use subsequent terms to clarify parameters and other distinguishing details.
   - Example: in ``c_exponential_moving_average_20d_close``, “exponential_moving_average” appears before “20d” and “close.”

7. **Specifying Adjustment Types**

   - If a feature depends on prices adjusted for dividends, splits, or both, append ``_dividend_and_split_adjusted`` at the end.
   - Example:

     - ``c_simple_moving_average_5d_dividend_and_split_adjusted``

8. **Specifying Price Type**

   - If a feature uses a specific price type (e.g., ``open``, ``high``, ``low``, ``close``, or ``adjusted_close``), include that term to distinguish from other price types.
   - Example:

     - A 5-day EMA of the close price: ``c_exponential_moving_average_5d_close``

9. **Omitting “close” for Return‐Based Features**

   - For return calculations, assume the close price by default; do not include “close” in the name.
   - Example: ``c_returns_20d`` implies close‐to‐close returns over 20 days.

10. **Omitting “close” for Common Technical Indicators**

    - For widely used indicators almost always computed on the close price (e.g., RSI, MACD), omit “close” entirely.
    - Examples:

      - ``c_rsi_14d``
      - ``c_macd_26d_12d``

By adhering to these rules, all calculated feature names in Data Curator remain consistent, unambiguous, and easy to discover in an alphabetically ordered list.
