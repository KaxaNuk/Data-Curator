.. _sharadar:

Sharadar
=============================

**Sharadar** publishes end of day prices, fundamentals and corporate actions for US listed stocks and funds,
including delisted ones, which makes it suited to point in time research free of survivorship bias.

- Visit their `official documentation <https://sharadar.com/docs/intro>`_.

- View available plans: `Sharadar Pricing Plans <https://sharadar.com/subscribe>`_.

Sharadar Features
------------------

Market Data
~~~~~~~~~~~~~~~~~

Daily prices of **stocks** and of **funds** (ETFs, closed-end funds and ETNs). Which of the two tables holds
an identifier is read from Sharadar's tickers table.

- **Split adjusted**: open, high, low, close and volume
- **Unadjusted**: close only
- **Dividend and split adjusted**: close only

Fundamentals
~~~~~~~~~~~~~~~~~

Income statement, balance sheet and cash flow of each filing, in the **as-reported** dimensions only (ARQ for
quarterly runs, ARY for annual ones). These are point in time: each filing is dated on the day it was filed and
its figures are not revised by later filings. Any other dimension Sharadar returns is dropped. Filings are read
from their whole history, so the first day of a run already carries the filing in effect on it.

**Share counts and per-share values are adjusted for later splits**, even in the as-reported dimensions: AAPL's
filing of January 2020 carries 17.7 billion shares and an EPS of 1.26, where Apple reported about 4.4 billion and
4.99 before its 2020 split. Combine them with split adjusted prices, never with unadjusted ones.

The reporting currency is taken from the fundamentals row of the tickers table, since the fundamentals table
carries none; its stocks row holds the trading currency instead. Companies filing in a foreign currency report
their fundamentals in it, like Toyota in yen, while their prices are in US dollars.

Dividends and Splits
~~~~~~~~~~~~~~~~~~~~~

Both come from Sharadar's actions table.

- Sharadar publishes each dividend **only adjusted for the splits after it**. That value fills the split
  adjusted dividend, and the dividend as paid is left empty: AAPL's 0.77 of 2019 reads as 0.1925, after its
  2020 4 for 1 split. Deriving the paid amount, if needed, belongs in a custom calculation.

Technical Details
~~~~~~~~~~~~~~~~~

- Every table is read through Sharadar's query API, 30 identifiers per request.
- Rate limited requests wait for the reset Sharadar announces, and network or server errors are retried for
  about four minutes.
- An identifier that needs a table the subscription doesn't cover, like a fund on a stocks-only plan, is
  skipped and logged, and the rest of the run carries on. A rejected API key stops the run.
- Sharadar reuses ticker symbols: when a company is delisted or renamed, its old symbol may get a suffix and
  later belong to another company. Keep the identifiers of a run current with Sharadar's tickers table.
- Sharadar revises past data, as each row's ``lastupdated`` shows, so the same run made on another day may
  return different figures. Keep the date a run was made alongside its output.

Authentication is managed using an API key placed in the `.env` file via the variable:

.. code-block:: ini

   KNDC_API_KEY_SHARADAR=your_key_here

Market Data
-----------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - m_close
     - :abbr:`closeunadj (funds | stocks)`
   * - m_close_dividend_and_split_adjusted
     - :abbr:`closeadj (funds | stocks)`
   * - m_close_split_adjusted
     - :abbr:`close (funds | stocks)`
   * - m_date
     - [ :abbr:`date (funds | stocks)` ] `* <sharadar_preprocessed_legend_>`_
   * - m_high_split_adjusted
     - :abbr:`high (funds | stocks)`
   * - m_low_split_adjusted
     - :abbr:`low (funds | stocks)`
   * - m_open_split_adjusted
     - :abbr:`open (funds | stocks)`
   * - m_volume_split_adjusted
     - :abbr:`volume (funds | stocks)`

Dividends
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - d_dividend_split_adjusted
     - :abbr:`value (actions)`
   * - d_ex_dividend_date
     - [ :abbr:`date (actions)` ] `* <sharadar_preprocessed_legend_>`_

Splits
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - s_denominator
     - [ :abbr:`value (actions)` ] `* <sharadar_preprocessed_legend_>`_
   * - s_numerator
     - [ :abbr:`value (actions)` ] `* <sharadar_preprocessed_legend_>`_
   * - s_split_date
     - [ :abbr:`date (actions)` ] `* <sharadar_preprocessed_legend_>`_

Fundamentals
------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - f_filing_date
     - [ :abbr:`date (fundamentals)` ] `* <sharadar_preprocessed_legend_>`_
   * - f_fiscal_period
     - [ :abbr:`fiscalperiod (fundamentals)` ] `* <sharadar_preprocessed_legend_>`_
   * - f_fiscal_year
     - [ :abbr:`fiscalperiod (fundamentals)` ] `* <sharadar_preprocessed_legend_>`_
   * - f_period_end_date
     - [ :abbr:`reportperiod (fundamentals)` ] `* <sharadar_preprocessed_legend_>`_
   * - f_reported_currency
     - :abbr:`currency, from the tickers table (fundamentals)` `† <sharadar_derived_legend_>`_

Income
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - fis_basic_earnings_per_share
     - :abbr:`eps (fundamentals)`
   * - fis_basic_net_income_available_to_common_stockholders
     - :abbr:`netinccmn (fundamentals)`
   * - fis_cost_of_revenue
     - :abbr:`cor (fundamentals)`
   * - fis_depreciation_and_amortization
     - :abbr:`depamor (fundamentals)`
   * - fis_diluted_earnings_per_share
     - :abbr:`epsdil (fundamentals)`
   * - fis_discontinued_operations_income_after_tax
     - :abbr:`netincdis (fundamentals)`
   * - fis_earnings_before_interest_and_tax
     - :abbr:`ebit (fundamentals)`
   * - fis_earnings_before_interest_tax_depreciation_and_amortization
     - :abbr:`ebitda (fundamentals)`
   * - fis_gross_profit
     - :abbr:`gp (fundamentals)`
   * - fis_income_before_tax
     - :abbr:`ebt (fundamentals)`
   * - fis_income_tax_expense
     - :abbr:`taxexp (fundamentals)`
   * - fis_interest_expense
     - :abbr:`intexp (fundamentals)`
   * - fis_net_income
     - :abbr:`netinc (fundamentals)`
   * - fis_net_income_deductions
     - :abbr:`prefdivis (fundamentals)`
   * - fis_operating_expenses
     - :abbr:`opex (fundamentals)`
   * - fis_operating_income
     - :abbr:`opinc (fundamentals)`
   * - fis_research_and_development_expense
     - :abbr:`rnd (fundamentals)`
   * - fis_revenues
     - :abbr:`revenue (fundamentals)`
   * - fis_selling_general_and_administrative_expense
     - :abbr:`sgna (fundamentals)`
   * - fis_weighted_average_basic_shares_outstanding
     - :abbr:`shareswa (fundamentals)`
   * - fis_weighted_average_diluted_shares_outstanding
     - :abbr:`shareswadil (fundamentals)`

Balance Sheet
-------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - fbs_accumulated_other_comprehensive_income_after_tax
     - :abbr:`accoci (fundamentals)`
   * - fbs_assets
     - :abbr:`assets (fundamentals)`
   * - fbs_cash_and_cash_equivalents
     - :abbr:`cashneq (fundamentals)`
   * - fbs_current_accounts_payable
     - :abbr:`payables (fundamentals)`
   * - fbs_current_assets
     - :abbr:`assetsc (fundamentals)`
   * - fbs_current_liabilities
     - :abbr:`liabilitiesc (fundamentals)`
   * - fbs_current_net_receivables
     - :abbr:`receivables (fundamentals)`
   * - fbs_deferred_revenue
     - :abbr:`deferredrev (fundamentals)`
   * - fbs_investments
     - :abbr:`investments (fundamentals)`
   * - fbs_liabilities
     - :abbr:`liabilities (fundamentals)`
   * - fbs_longterm_debt
     - :abbr:`debtnc (fundamentals)`
   * - fbs_longterm_investments
     - :abbr:`investmentsnc (fundamentals)`
   * - fbs_net_intangible_assets_including_goodwill
     - :abbr:`intangibles (fundamentals)`
   * - fbs_net_inventory
     - :abbr:`inventory (fundamentals)`
   * - fbs_net_property_plant_and_equipment
     - :abbr:`ppnenet (fundamentals)`
   * - fbs_noncurrent_assets
     - :abbr:`assetsnc (fundamentals)`
   * - fbs_noncurrent_deferred_tax_assets
     - :abbr:`taxassets (fundamentals)`
   * - fbs_noncurrent_deferred_tax_liabilities
     - :abbr:`taxliabilities (fundamentals)`
   * - fbs_noncurrent_liabilities
     - :abbr:`liabilitiesnc (fundamentals)`
   * - fbs_retained_earnings
     - :abbr:`retearn (fundamentals)`
   * - fbs_shortterm_debt
     - :abbr:`debtc (fundamentals)`
   * - fbs_shortterm_investments
     - :abbr:`investmentsc (fundamentals)`
   * - fbs_stockholder_equity
     - :abbr:`equity (fundamentals)`
   * - fbs_total_debt_including_capital_lease_obligations
     - :abbr:`debt (fundamentals)`

Cash Flow
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - Sharadar Field
   * - fcf_capital_expenditure
     - :abbr:`capex (fundamentals)`
   * - fcf_cash_and_cash_equivalents_change
     - :abbr:`ncf (fundamentals)`
   * - fcf_cash_exchange_rate_effect
     - :abbr:`ncfx (fundamentals)`
   * - fcf_depreciation_and_amortization
     - :abbr:`depamor (fundamentals)`
   * - fcf_dividend_payments
     - :abbr:`ncfdiv (fundamentals)`
   * - fcf_free_cash_flow
     - :abbr:`fcf (fundamentals)`
   * - fcf_net_business_acquisition_payments
     - :abbr:`ncfbus (fundamentals)`
   * - fcf_net_cash_from_financing_activities
     - :abbr:`ncff (fundamentals)`
   * - fcf_net_cash_from_investing_activities
     - :abbr:`ncfi (fundamentals)`
   * - fcf_net_cash_from_operating_activities
     - :abbr:`ncfo (fundamentals)`
   * - fcf_net_common_stock_issuance_proceeds
     - :abbr:`ncfcommon (fundamentals)`
   * - fcf_net_debt_issuance_proceeds
     - :abbr:`ncfdebt (fundamentals)`
   * - fcf_net_income
     - :abbr:`netinc (fundamentals)`
   * - fcf_other_investing_activities
     - :abbr:`ncfinv (fundamentals)`
   * - fcf_stock_based_compensation
     - :abbr:`sbcomp (fundamentals)`



|

Data Processing
---------------

Hover a field to see the Sharadar table it is read from. Tags Sharadar doesn't publish are not listed, and are left empty.

.. _sharadar_preprocessed_legend:

\* Fields enclosed in brackets [ ... ] with an asterisk indicate preprocessed tags.

.. _sharadar_derived_legend:

† Values computed by the provider rather than read as published.
