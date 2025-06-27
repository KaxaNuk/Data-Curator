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
------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - m_date
     - date
   * - m_open
     - adjOpen
   * - m_high
     - adjHigh
   * - m_low
     - adjLow
   * - m_close
     - adjClose
   * - m_volume
     - volume
   * - m_open_split_adjusted
     - open
   * - m_high_split_adjusted
     - high
   * - m_low_split_adjusted
     - low
   * - m_close_split_adjusted
     - close
   * - m_volume_split_adjusted
     - volume
   * - m_vwap_split_adjusted
     - vwap
   * - m_open_dividend_and_split_adjusted
     - adjOpen
   * - m_high_dividend_and_split_adjusted
     - adjHigh
   * - m_low_dividend_and_split_adjusted
     - adjLow
   * - m_close_dividend_and_split_adjusted
     - adjClose
   * - m_volume_dividend_and_split_adjusted
     - volume

Dividends
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - d_declaration_date
     - declarationDate
   * - d_ex_dividend_date
     - date
   * - d_record_date
     - recordDate
   * - d_payment_date
     - paymentDate
   * - d_dividend
     - dividend
   * - d_dividend_split_adjusted
     - adjDividend

Splits
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - s_split_date
     - date
   * - s_numerator
     - numerator
   * - s_denominator
     - denominator

Fundamentals
------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - f_accepted_date
     - acceptedDate
   * - f_filing_date
     - filingDate
   * - f_fiscal_period
     - period
   * - f_fiscal_year
     - fiscalYear
   * - f_period_end_date
     - date
   * - f_reported_currency
     - reportedCurrency

Income
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - fis_basic_earnings_per_share
     - eps
   * - fis_basic_net_income_available_to_common_stockholders
     - bottomLineNetIncome
   * - fis_continuing_operations_income_after_tax
     - netIncomeFromContinuingOperations
   * - fis_costs_and_expenses
     - costAndExpenses
   * - fis_cost_of_revenue
     - costOfRevenue
   * - fis_depreciation_and_amortization
     - depreciationAndAmortization
   * - fis_diluted_earnings_per_share
     - epsDiluted
   * - fis_discontinued_operations_income_after_tax
     - netIncomeFromDiscontinuedOperations
   * - fis_earnings_before_interest_and_tax
     - ebit
   * - fis_earnings_before_interest_tax_depreciation_and_amortization
     - ebitda
   * - fis_general_and_administrative_expense
     - generalAndAdministrativeExpenses
   * - fis_gross_profit
     - grossProfit
   * - fis_income_before_tax
     - incomeBeforeTax
   * - fis_income_tax_expense
     - incomeTaxExpense
   * - fis_interest_expense
     - interestExpense
   * - fis_interest_income
     - interestIncome
   * - fis_net_income
     - netIncome
   * - fis_net_income_deductions
     - netIncomeDeductions
   * - fis_net_interest_income
     - netInterestIncome
   * - fis_net_total_other_income
     - totalOtherIncomeExpensesNet
   * - fis_nonoperating_income_excluding_interest
     - nonOperatingIncomeExcludingInterest
   * - fis_operating_expenses
     - operatingExpenses
   * - fis_operating_income
     - operatingIncome
   * - fis_other_expenses
     - otherExpenses
   * - fis_other_net_income_adjustments
     - otherAdjustmentsToNetIncome
   * - fis_research_and_development_expense
     - researchAndDevelopmentExpenses
   * - fis_revenues
     - revenue
   * - fis_selling_and_marketing_expense
     - sellingAndMarketingExpenses
   * - fis_selling_general_and_administrative_expense
     - sellingGeneralAndAdministrativeExpenses
   * - fis_weighted_average_basic_shares_outstanding
     - weightedAverageShsOut
   * - fis_weighted_average_diluted_shares_outstanding
     - weightedAverageShsOutDil

Balance Sheet
-------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - fbs_filing_date
     - fillingDate
   * - fbs_accumulated_other_comprehensive_income_after_tax
     - accumulatedOtherComprehensiveIncomeLoss
   * - fbs_additional_paid_in_capital
     - additionalPaidInCapital
   * - fbs_assets
     - totalAssets
   * - fbs_capital_lease_obligations
     - capitalLeaseObligations
   * - fbs_cash_and_cash_equivalents
     - cashAndCashEquivalents
   * - fbs_cash_and_shortterm_investments
     - cashAndShortTermInvestments
   * - fbs_common_stock_value
     - commonStock
   * - fbs_current_accounts_payable
     - accountPayables
   * - fbs_current_accounts_receivable_after_doubtful_accounts
     - accountsReceivables
   * - fbs_current_accrued_expenses
     - accruedExpenses
   * - fbs_current_assets
     - totalCurrentAssets
   * - fbs_current_capital_lease_obligations
     - capitalLeaseObligationsCurrent
   * - fbs_current_liabilities
     - totalCurrentLiabilities
   * - fbs_current_net_receivables
     - netReceivables
   * - fbs_current_tax_payables
     - taxPayables
   * - fbs_deferred_revenue
     - deferredRevenue
   * - fbs_goodwill
     - goodwill
   * - fbs_investments
     - totalInvestments
   * - fbs_liabilities
     - totalLiabilities
   * - fbs_longterm_debt
     - longTermDebt
   * - fbs_longterm_investments
     - longTermInvestments
   * - fbs_net_debt
     - netDebt
   * - fbs_net_intangible_assets_excluding_goodwill
     - intangibleAssets
   * - fbs_net_intangible_assets_including_goodwill
     - goodwillAndIntangibleAssets
   * - fbs_net_inventory
     - inventory
   * - fbs_net_property_plant_and_equipment
     - propertyPlantEquipmentNet
   * - fbs_noncontrolling_interest
     - minorityInterest
   * - fbs_noncurrent_assets
     - totalNonCurrentAssets
   * - fbs_noncurrent_capital_lease_obligations
     - capitalLeaseObligationsNonCurrent
   * - fbs_noncurrent_deferred_revenue
     - deferredRevenueNonCurrent
   * - fbs_noncurrent_deferred_tax_assets
     - taxAssets
   * - fbs_noncurrent_deferred_tax_liabilities
     - deferredTaxLiabilitiesNonCurrent
   * - fbs_noncurrent_liabilities
     - totalNonCurrentLiabilities
   * - fbs_other_assets
     - otherAssets
   * - fbs_other_current_assets
     - otherCurrentAssets
   * - fbs_other_current_liabilities
     - otherCurrentLiabilities
   * - fbs_other_liabilities
     - otherLiabilities
   * - fbs_other_noncurrent_assets
     - otherNonCurrentAssets
   * - fbs_other_noncurrent_liabilities
     - otherNonCurrentLiabilities
   * - fbs_other_payables
     - otherPayables
   * - fbs_other_receivables
     - otherReceivables
   * - fbs_other_stockholder_equity
     - otherTotalStockholdersEquity
   * - fbs_preferred_stock_value
     - preferredStock
   * - fbs_prepaid_expenses
     - prepaids
   * - fbs_retained_earnings
     - retainedEarnings
   * - fbs_shortterm_debt
     - shortTermDebt
   * - fbs_shortterm_investments
     - shortTermInvestments
   * - fbs_stockholder_equity
     - totalStockholdersEquity
   * - fbs_total_debt_including_capital_lease_obligations
     - totalDebt
   * - fbs_total_equity_including_noncontrolling_interest
     - totalEquity
   * - fbs_total_liabilities_and_equity
     - totalLiabilitiesAndTotalEquity
   * - fbs_total_payables_current_and_noncurrent
     - totalPayables
   * - fbs_treasury_stock_value
     - treasuryStock

Cash Flow
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - FMP Tag
   * - fcf_accounts_payable_change
     - accountsPayables
   * - fcf_accounts_receivable_change
     - accountsReceivables
   * - fcf_capital_expenditure
     - capitalExpenditure
   * - fcf_cash_and_cash_equivalents_change
     - netChangeInCash
   * - fcf_cash_exchange_rate_effect
     - effectOfForexChangesOnCash
   * - fcf_common_stock_dividend_payments
     - commonDividendsPaid
   * - fcf_common_stock_issuance_proceeds
     - commonStockIssuance
   * - fcf_common_stock_repurchase
     - commonStockRepurchased
   * - fcf_deferred_income_tax
     - deferredIncomeTax
   * - fcf_depreciation_and_amortization
     - depreciationAndAmortization
   * - fcf_dividend_payments
     - netDividendsPaid
   * - fcf_free_cash_flow
     - freeCashFlow
   * - fcf_interest_payments
     - interestPaid
   * - fcf_inventory_change
     - inventory
   * - fcf_investment_sales_maturities_and_collections_proceeds
     - salesMaturitiesOfInvestments
   * - fcf_investments_purchase
     - purchasesOfInvestments
   * - fcf_net_business_acquisition_payments
     - acquisitionsNet
   * - fcf_net_cash_from_operating_activities
     - netCashProvidedByOperatingActivities
   * - fcf_net_cash_from_investing_activites
     - netCashProvidedByInvestingActivities
   * - fcf_net_cash_from_financing_activities
     - netCashProvidedByFinancingActivities
   * - fcf_net_common_stock_issuance_proceeds
     - netCommonStockIssuance
   * - fcf_net_debt_issuance_proceeds
     - netDebtIssuance
   * - fcf_net_income
     - netIncome
   * - fcf_net_income_tax_payments
     - incomeTaxesPaid
   * - fcf_net_longterm_debt_issuance_proceeds
     - longTermNetDebtIssuance
   * - fcf_net_shortterm_debt_issuance_proceeds
     - shortTermNetDebtIssuance
   * - fcf_net_stock_issuance_proceeds
     - netStockIssuance
   * - fcf_other_financing_activities
     - otherFinancingActivities
   * - fcf_other_investing_activities
     - otherInvestingActivities
   * - fcf_other_noncash_items
     - otherNonCashItems
   * - fcf_other_working_capital
     - otherWorkingCapital
   * - fcf_period_end_cash
     - cashAtEndOfPeriod
   * - fcf_period_start_cash
     - cashAtBeginningOfPeriod
   * - fcf_preferred_stock_dividend_payments
     - preferredDividendsPaid
   * - fcf_preferred_stock_issuance_proceeds
     - netPreferredStockIssuance
   * - fcf_property_plant_and_equipment_purchase
     - investmentsInPropertyPlantAndEquipment
   * - fcf_stock_based_compensation
     - stockBasedCompensation
   * - fcf_working_capital_change
     - changeInWorkingCapital

