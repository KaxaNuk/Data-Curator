.. _fmp:

Financial Modeling Prep
=============================

**FMP** is a trusted provider of stock market and financial data,
offering a wide range of standardized and audited financial information.
This library integrates FMP's API to access historical market prices and
core financial statements for supported instruments.

- Visit their `official documentation <https://site.financialmodelingprep.com/developer/docs/stable>`_.
- View available plans through our referral link: `FMP Pricing Plans`_

.. _FMP Pricing Plans: https://site.financialmodelingprep.com/pricing-plans?couponCode=xss2L2sI


FMP Features
-------------

FMP offers a wide range of financial and market data through a unified REST API.
Below is an overview of the types of data available through this integration.

Market Data
~~~~~~~~~~~~~~~~~

FMP provides historical **time-series data** for multiple asset classes:

- **Stocks**
- **ETFs**
- **Indexes**
- **Cryptocurrencies**
- **Commodities**
- **Forex**

Each asset class supports:

- **Price fields**:
  - Open, High, Low, Close, Volume, VWAP
- **Adjustment types**:
  - Raw (unadjusted)
  - Split-adjusted
  - Dividend & split-adjusted

Fundamentals
~~~~~~~~~~~~~~~~~

FMP offers **audited and standardized financial statements** for public companies, available in:

- **Quarterly**
- **Annual**

The supported statement types include:

- Income Statements
- Balance Sheets
- Cash Flow Statements

Technical Details
~~~~~~~~~~~~~~~~~

This library uses FMP's REST endpoints to fetch:

- Time series for historical price data.
- Standardized fundamentals via financial statements.
- Fully configurable columns and date ranges.

Authentication is managed using an API key placed in the `.env` file via the variable:

.. code-block:: ini

   KNDC_API_KEY_FMP=your_key_here

Market Data
-----------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - m_close
     - :abbr:`adjClose (https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted)`
   * - m_close_dividend_and_split_adjusted
     - :abbr:`adjClose (https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted)`
   * - m_close_split_adjusted
     - :abbr:`close (https://financialmodelingprep.com/stable/historical-price-eod/full)`
   * - m_date
     - [ :abbr:`date (https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted | https://financialmodelingprep.com/stable/historical-price-eod/full | https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted)` ] `* <preprocessed_legend_>`_
   * - m_high
     - :abbr:`adjHigh (https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted)`
   * - m_high_dividend_and_split_adjusted
     - :abbr:`adjHigh (https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted)`
   * - m_high_split_adjusted
     - :abbr:`high (https://financialmodelingprep.com/stable/historical-price-eod/full)`
   * - m_low
     - :abbr:`adjLow (https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted)`
   * - m_low_dividend_and_split_adjusted
     - :abbr:`adjLow (https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted)`
   * - m_low_split_adjusted
     - :abbr:`low (https://financialmodelingprep.com/stable/historical-price-eod/full)`
   * - m_open
     - :abbr:`adjOpen (https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted)`
   * - m_open_dividend_and_split_adjusted
     - :abbr:`adjOpen (https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted)`
   * - m_open_split_adjusted
     - :abbr:`open (https://financialmodelingprep.com/stable/historical-price-eod/full)`
   * - m_volume
     - :abbr:`volume (https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted)`
   * - m_volume_dividend_and_split_adjusted
     - :abbr:`volume (https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted)`
   * - m_volume_split_adjusted
     - :abbr:`volume (https://financialmodelingprep.com/stable/historical-price-eod/full)`
   * - m_vwap_split_adjusted
     - :abbr:`vwap (https://financialmodelingprep.com/stable/historical-price-eod/full)`

Dividends
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - d_declaration_date
     - :abbr:`declarationDate (https://financialmodelingprep.com/stable/dividends)`
   * - d_dividend
     - :abbr:`dividend (https://financialmodelingprep.com/stable/dividends)`
   * - d_dividend_split_adjusted
     - :abbr:`adjDividend (https://financialmodelingprep.com/stable/dividends)`
   * - d_ex_dividend_date
     - [ :abbr:`date (https://financialmodelingprep.com/stable/dividends)` ] `* <preprocessed_legend_>`_
   * - d_payment_date
     - :abbr:`paymentDate (https://financialmodelingprep.com/stable/dividends)`
   * - d_record_date
     - :abbr:`recordDate (https://financialmodelingprep.com/stable/dividends)`

Splits
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - s_denominator
     - :abbr:`denominator (https://financialmodelingprep.com/stable/splits)`
   * - s_numerator
     - :abbr:`numerator (https://financialmodelingprep.com/stable/splits)`
   * - s_split_date
     - [ :abbr:`date (https://financialmodelingprep.com/stable/splits)` ] `* <preprocessed_legend_>`_

Fundamentals
------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - f_accepted_date
     - :abbr:`acceptedDate (https://financialmodelingprep.com/stable/balance-sheet-statement | https://financialmodelingprep.com/stable/cash-flow-statement | https://financialmodelingprep.com/stable/income-statement)`
   * - f_filing_date
     - - [ :abbr:`filingDate (https://financialmodelingprep.com/stable/balance-sheet-statement)`, :abbr:`acceptedDate (https://financialmodelingprep.com/stable/balance-sheet-statement)` ] `* <preprocessed_legend_>`_
       - [ :abbr:`filingDate (https://financialmodelingprep.com/stable/cash-flow-statement | https://financialmodelingprep.com/stable/income-statement)` ] `* <preprocessed_legend_>`_
   * - f_fiscal_period
     - :abbr:`period (https://financialmodelingprep.com/stable/balance-sheet-statement | https://financialmodelingprep.com/stable/cash-flow-statement | https://financialmodelingprep.com/stable/income-statement)`
   * - f_fiscal_year
     - :abbr:`fiscalYear (https://financialmodelingprep.com/stable/balance-sheet-statement | https://financialmodelingprep.com/stable/cash-flow-statement | https://financialmodelingprep.com/stable/income-statement)`
   * - f_period_end_date
     - [ :abbr:`date (https://financialmodelingprep.com/stable/balance-sheet-statement | https://financialmodelingprep.com/stable/cash-flow-statement | https://financialmodelingprep.com/stable/income-statement)` ] `* <preprocessed_legend_>`_
   * - f_reported_currency
     - :abbr:`reportedCurrency (https://financialmodelingprep.com/stable/balance-sheet-statement | https://financialmodelingprep.com/stable/cash-flow-statement | https://financialmodelingprep.com/stable/income-statement)`

Income
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - fis_basic_earnings_per_share
     - :abbr:`eps (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_basic_net_income_available_to_common_stockholders
     - :abbr:`bottomLineNetIncome (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_continuing_operations_income_after_tax
     - :abbr:`netIncomeFromContinuingOperations (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_cost_of_revenue
     - :abbr:`costOfRevenue (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_costs_and_expenses
     - :abbr:`costAndExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_depreciation_and_amortization
     - :abbr:`depreciationAndAmortization (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_diluted_earnings_per_share
     - :abbr:`epsDiluted (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_discontinued_operations_income_after_tax
     - :abbr:`netIncomeFromDiscontinuedOperations (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_earnings_before_interest_and_tax
     - :abbr:`ebit (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_earnings_before_interest_tax_depreciation_and_amortization
     - :abbr:`ebitda (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_general_and_administrative_expense
     - :abbr:`generalAndAdministrativeExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_gross_profit
     - :abbr:`grossProfit (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_income_before_tax
     - :abbr:`incomeBeforeTax (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_income_tax_expense
     - :abbr:`incomeTaxExpense (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_interest_expense
     - :abbr:`interestExpense (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_interest_income
     - :abbr:`interestIncome (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_net_income
     - :abbr:`netIncome (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_net_income_deductions
     - :abbr:`netIncomeDeductions (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_net_interest_income
     - :abbr:`netInterestIncome (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_net_total_other_income
     - :abbr:`totalOtherIncomeExpensesNet (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_nonoperating_income_excluding_interest
     - :abbr:`nonOperatingIncomeExcludingInterest (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_operating_expenses
     - :abbr:`operatingExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_operating_income
     - :abbr:`operatingIncome (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_other_expenses
     - :abbr:`otherExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_other_net_income_adjustments
     - :abbr:`otherAdjustmentsToNetIncome (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_research_and_development_expense
     - :abbr:`researchAndDevelopmentExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_revenues
     - :abbr:`revenue (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_selling_and_marketing_expense
     - :abbr:`sellingAndMarketingExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_selling_general_and_administrative_expense
     - :abbr:`sellingGeneralAndAdministrativeExpenses (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_weighted_average_basic_shares_outstanding
     - :abbr:`weightedAverageShsOut (https://financialmodelingprep.com/stable/income-statement)`
   * - fis_weighted_average_diluted_shares_outstanding
     - :abbr:`weightedAverageShsOutDil (https://financialmodelingprep.com/stable/income-statement)`

Balance Sheet
-------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - fbs_accumulated_other_comprehensive_income_after_tax
     - :abbr:`accumulatedOtherComprehensiveIncomeLoss (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_additional_paid_in_capital
     - :abbr:`additionalPaidInCapital (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_assets
     - :abbr:`totalAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_capital_lease_obligations
     - :abbr:`capitalLeaseObligations (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_cash_and_cash_equivalents
     - :abbr:`cashAndCashEquivalents (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_cash_and_shortterm_investments
     - :abbr:`cashAndShortTermInvestments (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_common_stock_value
     - :abbr:`commonStock (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_accounts_payable
     - :abbr:`accountPayables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_accounts_receivable_after_doubtful_accounts
     - :abbr:`accountsReceivables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_accrued_expenses
     - :abbr:`accruedExpenses (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_assets
     - :abbr:`totalCurrentAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_capital_lease_obligations
     - :abbr:`capitalLeaseObligationsCurrent (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_liabilities
     - :abbr:`totalCurrentLiabilities (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_net_receivables
     - :abbr:`netReceivables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_current_tax_payables
     - :abbr:`taxPayables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_deferred_revenue
     - :abbr:`deferredRevenue (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_goodwill
     - :abbr:`goodwill (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_investments
     - :abbr:`totalInvestments (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_liabilities
     - :abbr:`totalLiabilities (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_longterm_debt
     - :abbr:`longTermDebt (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_longterm_investments
     - :abbr:`longTermInvestments (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_net_debt
     - :abbr:`netDebt (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_net_intangible_assets_excluding_goodwill
     - :abbr:`intangibleAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_net_intangible_assets_including_goodwill
     - :abbr:`goodwillAndIntangibleAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_net_inventory
     - :abbr:`inventory (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_net_property_plant_and_equipment
     - :abbr:`propertyPlantEquipmentNet (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncontrolling_interest
     - :abbr:`minorityInterest (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncurrent_assets
     - :abbr:`totalNonCurrentAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncurrent_capital_lease_obligations
     - :abbr:`capitalLeaseObligationsNonCurrent (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncurrent_deferred_revenue
     - :abbr:`deferredRevenueNonCurrent (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncurrent_deferred_tax_assets
     - :abbr:`taxAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncurrent_deferred_tax_liabilities
     - :abbr:`deferredTaxLiabilitiesNonCurrent (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_noncurrent_liabilities
     - :abbr:`totalNonCurrentLiabilities (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_assets
     - :abbr:`otherAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_current_assets
     - :abbr:`otherCurrentAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_current_liabilities
     - :abbr:`otherCurrentLiabilities (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_liabilities
     - :abbr:`otherLiabilities (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_noncurrent_assets
     - :abbr:`otherNonCurrentAssets (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_noncurrent_liabilities
     - :abbr:`otherNonCurrentLiabilities (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_payables
     - :abbr:`otherPayables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_receivables
     - :abbr:`otherReceivables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_other_stockholder_equity
     - :abbr:`otherTotalStockholdersEquity (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_preferred_stock_value
     - :abbr:`preferredStock (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_prepaid_expenses
     - :abbr:`prepaids (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_retained_earnings
     - :abbr:`retainedEarnings (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_shortterm_debt
     - :abbr:`shortTermDebt (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_shortterm_investments
     - :abbr:`shortTermInvestments (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_stockholder_equity
     - :abbr:`totalStockholdersEquity (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_total_debt_including_capital_lease_obligations
     - :abbr:`totalDebt (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_total_equity_including_noncontrolling_interest
     - :abbr:`totalEquity (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_total_liabilities_and_equity
     - :abbr:`totalLiabilitiesAndTotalEquity (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_total_payables_current_and_noncurrent
     - :abbr:`totalPayables (https://financialmodelingprep.com/stable/balance-sheet-statement)`
   * - fbs_treasury_stock_value
     - :abbr:`treasuryStock (https://financialmodelingprep.com/stable/balance-sheet-statement)`

Cash Flow
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - fcf_accounts_payable_change
     - :abbr:`accountsPayables (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_accounts_receivable_change
     - :abbr:`accountsReceivables (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_capital_expenditure
     - :abbr:`capitalExpenditure (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_cash_and_cash_equivalents_change
     - :abbr:`netChangeInCash (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_cash_exchange_rate_effect
     - :abbr:`effectOfForexChangesOnCash (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_common_stock_dividend_payments
     - :abbr:`commonDividendsPaid (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_common_stock_issuance_proceeds
     - :abbr:`commonStockIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_common_stock_repurchase
     - :abbr:`commonStockRepurchased (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_deferred_income_tax
     - :abbr:`deferredIncomeTax (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_depreciation_and_amortization
     - :abbr:`depreciationAndAmortization (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_dividend_payments
     - :abbr:`netDividendsPaid (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_free_cash_flow
     - :abbr:`freeCashFlow (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_interest_payments
     - :abbr:`interestPaid (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_inventory_change
     - :abbr:`inventory (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_investment_sales_maturities_and_collections_proceeds
     - :abbr:`salesMaturitiesOfInvestments (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_investments_purchase
     - :abbr:`purchasesOfInvestments (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_business_acquisition_payments
     - :abbr:`acquisitionsNet (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_cash_from_financing_activities
     - :abbr:`netCashProvidedByFinancingActivities (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_cash_from_investing_activites
     - :abbr:`netCashProvidedByInvestingActivities (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_cash_from_operating_activities
     - :abbr:`netCashProvidedByOperatingActivities (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_common_stock_issuance_proceeds
     - :abbr:`netCommonStockIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_debt_issuance_proceeds
     - :abbr:`netDebtIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_income
     - :abbr:`netIncome (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_income_tax_payments
     - :abbr:`incomeTaxesPaid (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_longterm_debt_issuance_proceeds
     - :abbr:`longTermNetDebtIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_shortterm_debt_issuance_proceeds
     - :abbr:`shortTermNetDebtIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_net_stock_issuance_proceeds
     - :abbr:`netStockIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_other_financing_activities
     - :abbr:`otherFinancingActivities (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_other_investing_activities
     - :abbr:`otherInvestingActivities (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_other_noncash_items
     - :abbr:`otherNonCashItems (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_other_working_capital
     - :abbr:`otherWorkingCapital (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_period_end_cash
     - :abbr:`cashAtEndOfPeriod (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_period_start_cash
     - :abbr:`cashAtBeginningOfPeriod (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_preferred_stock_dividend_payments
     - :abbr:`preferredDividendsPaid (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_preferred_stock_issuance_proceeds
     - :abbr:`netPreferredStockIssuance (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_property_plant_and_equipment_purchase
     - :abbr:`investmentsInPropertyPlantAndEquipment (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_stock_based_compensation
     - :abbr:`stockBasedCompensation (https://financialmodelingprep.com/stable/cash-flow-statement)`
   * - fcf_working_capital_change
     - :abbr:`changeInWorkingCapital (https://financialmodelingprep.com/stable/cash-flow-statement)`



|

Data Processing
---------------

.. _preprocessed_legend:

\* Fields enclosed in brackets [ ... ] with an asterisk indicate preprocessed tags (hover to see source endpoint).
