.. _lseg:

LSEG Workspace
==============

`LSEG Workspace <https://www.lseg.com/en/data-analytics/products/workspace>`_ (London Stock Exchange Group) is a
professional financial data platform providing access to real-time and historical market data, financial statements,
analytics, and news across global asset classes, offering integration exclusively to active subscribers who possess
a valid subscription and a registered App Key.

This library integrates LSEG through the
`LSEG Data Library for Python <https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python/documentation>`_
(``lseg-data`` package), which connects to the platform either through the LSEG Workspace
desktop application or directly through the LSEG Data Platform.

.. admonition:: Workspace must be running

   The LSEG Workspace desktop application must be open and running on the same machine
   as your Python code when using the Data Curator with this provider.
   If Workspace is closed, all data requests will fail.


LSEG Features
-------------

Market Data
~~~~~~~~~~~

Historical end-of-day time series for a wide range of global instruments, including equities
(stocks, ETFs), indexes, fixed income, foreign exchange, commodities, and funds.

Price fields available per instrument: Open, High, Low, Close, Volume, and VWAP.
Prices are provided in two variants: unadjusted and split-adjusted.
See the :ref:`market-data-table` below for the exact TR tags corresponding to each variant.

Corporate Actions
~~~~~~~~~~~~~~~~~

- **Dividends**: ex-date, pay date, record date, declaration date, gross and adjusted amounts.
- **Splits**: ex-date, numerator and denominator of the split ratio.

Fundamentals
~~~~~~~~~~~~

Standardized quarterly financial statements for public companies:

- Income Statements
- Balance Sheets
- Cash Flow Statements

All fundamental TR fields use ``Period=FQ0``.
See the :ref:`fundamentals-table` below for the exact TR tags.


.. _lseg_setup:

Setup
-----

**Requirements**: An active LSEG Workspace subscription.

**Step 1 - Generate your App Key**

The App Key is generated from inside LSEG Workspace:

1. Open the LSEG Workspace application.
2. In the search bar, type **App Key Generator** and open it.
3. In the **App Key Generator**, enter a display name for your application and click **Register New App**.
4. Select (at minimum) the first three API checkboxes to enable data access.
5. Copy the generated App Key.

.. note::

   The App Key Generator was previously called **App Creator** in earlier versions of
   LSEG Workspace / Eikon. The functionality is equivalent.

**Step 2 - Add the App Key to your** ``.env`` **file**

In your project's ``Config/.env`` file, add:

.. code-block:: ini

   KNDC_APP_KEY_LSEG=your_app_key_here

**Step 3 - Keep Workspace open**

When running Data Curator with the LSEG provider, ensure the LSEG Workspace desktop
application is open and you are signed in before executing any data requests.


.. _market-data-table:


Market Data
-----------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - m_close
     - ``TR.CLOSEPRICE(Adjusted=0)``
   * - m_close_split_adjusted
     - ``TR.CLOSEPRICE(Adjusted=1)``
   * - m_date
     - ``TR.CLOSEPRICE.date``
   * - m_high
     - ``TR.HIGHPRICE(Adjusted=0)``
   * - m_high_split_adjusted
     - ``TR.HIGHPRICE(Adjusted=1)``
   * - m_low
     - ``TR.LOWPRICE(Adjusted=0)``
   * - m_low_split_adjusted
     - ``TR.LOWPRICE(Adjusted=1)``
   * - m_open
     - ``TR.OPENPRICE(Adjusted=0)``
   * - m_open_split_adjusted
     - ``TR.OPENPRICE(Adjusted=1)``
   * - m_volume
     - ``TR.Volume``
   * - m_vwap
     - ``TR.TSVWAP``


Dividends
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - d_declaration_date
     - ``TR.DivAnnouncementDate``
   * - d_dividend
     - ``TR.DivUnadjustedGross``
   * - d_dividend_split_adjusted
     - ``TR.DivAdjustedGross``
   * - d_ex_dividend_date
     - ``TR.DivExDate``
   * - d_payment_date
     - ``TR.DivPayDate``
   * - d_record_date
     - ``TR.DivRecordDate``


Splits
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - s_denominator
     - ``TR.CATermsOldShares(CAEventType=SSP)``
   * - s_numerator
     - ``TR.CATermsNewShares(CAEventType=SSP)``
   * - s_split_date
     - ``TR.CAExDate(CAEventType=SSP)``


.. _fundamentals-table:


Fundamentals
------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - f_filing_date
     - ``TR.F.OriginalAnnouncementDate(Period=FQ0)``
   * - f_period_end_date
     - ``TR.F.PeriodEndDate(Period=FQ0)``


Income
------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - fis_basic_earnings_per_share
     - ``TR.F.EPSBasicInclExordItemsComTot(Period=FQ0)``
   * - fis_basic_net_income_available_to_common_stockholders
     - ``TR.F.IncAvailToComShr(Period=FQ0)``
   * - fis_continuing_operations_income_after_tax
     - ``TR.F.NormNetIncContOps(Period=FQ0)``
   * - fis_cost_of_revenue
     - ``TR.F.CostOfOpRev(Period=FQ0)``
   * - fis_costs_and_expenses
     - ``TR.F.OpExpnTot(Period=FQ0)``
   * - fis_depreciation_and_amortization
     - ``TR.F.DeprAmort(Period=FQ0)``
   * - fis_diluted_earnings_per_share
     - ``TR.F.EPSDilInclExordItemsComTot(Period=FQ0)``
   * - fis_discontinued_operations_income_after_tax
     - ``TR.F.DiscOpsNetOfTaxTot(Period=FQ0)``
   * - fis_earnings_before_interest_and_tax
     - ``TR.F.EBIT(Period=FQ0)``
   * - fis_earnings_before_interest_tax_depreciation_and_amortization
     - ``TR.F.EBITDA(Period=FQ0)``
   * - fis_general_and_administrative_expense
     - ``TR.F.SGA(Period=FQ0)``
   * - fis_gross_profit
     - ``TR.F.GrossTotRevBizActiv(Period=FQ0)``
   * - fis_income_before_tax
     - ``TR.F.IncBefTax(Period=FQ0)``
   * - fis_income_tax_expense
     - ``TR.F.ExciseTaxExpn(Period=FQ0)``
   * - fis_interest_expense
     - ``TR.F.IntrExpn(Period=FQ0)``
   * - fis_interest_income
     - ``TR.F.IntrDivIncFinTot(Period=FQ0)``
   * - fis_net_income
     - ``TR.F.NetIncAfterTax(Period=FQ0)``
   * - fis_net_income_deductions
     - ``TR.F.EarnAdjToNetIncOthExpnInc(Period=FQ0)``
   * - fis_net_interest_income
     - ``TR.F.IntrDivIncExpnNetFin(Period=FQ0)``
   * - fis_net_total_other_income
     - ``TR.F.OthNonOpIncExpnTot(Period=FQ0)``
   * - fis_operating_expenses
     - ``TR.F.OpExpn(Period=FQ0)``
   * - fis_operating_income
     - ``TR.F.OpProfBefNonRecurIncExpn(Period=FQ0)``
   * - fis_other_expenses
     - ``TR.F.OthOpExpn(Period=FQ0)``
   * - fis_other_net_income_adjustments
     - ``TR.F.AdjToNetIncOth(Period=FQ0)``
   * - fis_research_and_development_expense
     - ``TR.F.RnD(Period=FQ0)``
   * - fis_revenues
     - ``TR.F.TotRevenue(Period=FQ0)``
   * - fis_selling_and_marketing_expense
     - ``TR.F.AdExpn(Period=FQ0)``
   * - fis_selling_general_and_administrative_expense
     - ``TR.F.SGATot(Period=FQ0)``
   * - fis_weighted_average_basic_shares_outstanding
     - ``TR.F.ShrUsedToCalcBasicEPSTot(Period=FQ0)``
   * - fis_weighted_average_diluted_shares_outstanding
     - ``TR.F.ShrUsedToCalcDilEPSTot(Period=FQ0)``


Balance Sheet
-------------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - fbs_accumulated_other_comprehensive_income_after_tax
     - ``TR.F.ComprIncAccumTot(Period=FQ0)``
   * - fbs_additional_paid_in_capital
     - ``TR.F.ComStockShrPrem(Period=FQ0)``
   * - fbs_assets
     - ``TR.F.TotAssets(Period=FQ0)``
   * - fbs_capital_lease_obligations
     - ``TR.F.CapLeaseObligLT(Period=FQ0)``
   * - fbs_cash_and_cash_equivalents
     - ``TR.F.CashCashEquivTot(Period=FQ0)``
   * - fbs_cash_and_shortterm_investments
     - ``TR.F.CashSTInvstTot(Period=FQ0)``
   * - fbs_common_stock_value
     - ``TR.F.ComEqTot(Period=FQ0)``
   * - fbs_current_accounts_payable
     - ``TR.F.TradeAcctPbleAccrualsST(Period=FQ0)``
   * - fbs_current_accounts_receivable_after_doubtful_accounts
     - ``TR.F.AcctNotesRcvblTradeNet(Period=FQ0)``
   * - fbs_current_accrued_expenses
     - ``TR.F.AccrExpn(Period=FQ0)``
   * - fbs_current_assets
     - ``TR.F.TotCurrAssets(Period=FQ0)``
   * - fbs_current_capital_lease_obligations
     - ``TR.F.CapLeaseCurrPort(Period=FQ0)``
   * - fbs_current_liabilities
     - ``TR.F.TotCurrLiab(Period=FQ0)``
   * - fbs_current_net_receivables
     - ``TR.F.LoansRcvblNetST(Period=FQ0)``
   * - fbs_current_tax_payables
     - ``TR.F.IncTaxPbleST(Period=FQ0)``
   * - fbs_deferred_revenue
     - ``TR.F.DefRevLT(Period=FQ0)``
   * - fbs_goodwill
     - ``TR.F.GoodwNet(Period=FQ0)``
   * - fbs_investments
     - ``TR.F.InvstTot(Period=FQ0)``
   * - fbs_liabilities
     - ``TR.F.TotLiab(Period=FQ0)``
   * - fbs_longterm_debt
     - ``TR.F.DebtLTTot(Period=FQ0)``
   * - fbs_longterm_investments
     - ``TR.F.InvstLT(Period=FQ0)``
   * - fbs_net_debt
     - ``TR.F.NetDebt(Period=FQ0)``
   * - fbs_net_intangible_assets_excluding_goodwill
     - ``TR.F.IntangExclGoodwNetTot(Period=FQ0)``
   * - fbs_net_intangible_assets_including_goodwill
     - ``TR.F.IntangTotNet(Period=FQ0)``
   * - fbs_net_inventory
     - ``TR.F.InvntTot(Period=FQ0)``
   * - fbs_net_property_plant_and_equipment
     - ``TR.F.PPENetTot(Period=FQ0)``
   * - fbs_noncontrolling_interest
     - ``TR.F.MinIntrNonCtrlIntrTot(Period=FQ0)``
   * - fbs_noncurrent_assets
     - ``TR.F.TotNonCurrAssets(Period=FQ0)``
   * - fbs_noncurrent_capital_lease_obligations
     - ``TR.F.CapLeaseObligLT(Period=FQ0)``
   * - fbs_noncurrent_deferred_revenue
     - ``TR.F.DefRevLT(Period=FQ0)``
   * - fbs_noncurrent_deferred_tax_assets
     - ``TR.F.DefTaxAssetLT(Period=FQ0)``
   * - fbs_noncurrent_deferred_tax_liabilities
     - ``TR.F.DefTaxLiabLT(Period=FQ0)``
   * - fbs_noncurrent_liabilities
     - ``TR.F.TotNonCurrLiab(Period=FQ0)``
   * - fbs_other_assets
     - ``TR.F.OthAssetsTot(Period=FQ0)``
   * - fbs_other_current_assets
     - ``TR.F.OthCurrAssetsTot(Period=FQ0)``
   * - fbs_other_current_liabilities
     - ``TR.F.OthCurrLiabTot(Period=FQ0)``
   * - fbs_other_liabilities
     - ``TR.F.OthLiabTot(Period=FQ0)``
   * - fbs_other_noncurrent_assets
     - ``TR.F.OthNonCurrAssetsTot(Period=FQ0)``
   * - fbs_other_noncurrent_liabilities
     - ``TR.F.OthNonCurrLiabTot(Period=FQ0)``
   * - fbs_other_payables
     - ``TR.F.OthPbleTot(Period=FQ0)``
   * - fbs_other_receivables
     - ``TR.F.RcvblOthTot(Period=FQ0)``
   * - fbs_other_stockholder_equity
     - ``TR.F.EqOth(Period=FQ0)``
   * - fbs_preferred_stock_value
     - ``TR.F.PrefShHoldEq(Period=FQ0)``
   * - fbs_prepaid_expenses
     - ``TR.F.PrepaidExpnTot(Period=FQ0)``
   * - fbs_retained_earnings
     - ``TR.F.RetainedEarnTot(Period=FQ0)``
   * - fbs_shortterm_debt
     - ``TR.F.STDebtFinlSectTot(Period=FQ0)``
   * - fbs_shortterm_investments
     - ``TR.F.STInvstTot(Period=FQ0)``
   * - fbs_stockholder_equity
     - ``TR.F.ComEqTot(Period=FQ0)``
   * - fbs_total_debt_including_capital_lease_obligations
     - ``TR.F.DebtTot(Period=FQ0)``
   * - fbs_total_equity_including_noncontrolling_interest
     - ``TR.F.TotShHoldEq(Period=FQ0)``
   * - fbs_total_liabilities_and_equity
     - ``TR.F.TotLiabEq(Period=FQ0)``
   * - fbs_total_payables_current_and_noncurrent
     - ``TR.F.AcctPble(Period=FQ0)``
   * - fbs_treasury_stock_value
     - ``TR.F.ComShrTrezTot(Period=FQ0)``


Cash Flow
---------

.. list-table::
   :header-rows: 1

   * - Data Curator Tag
     - LSEG Field
   * - fcf_accounts_payable_change
     - ``TR.F.AcctPbleCF(Period=FQ0)``
   * - fcf_accounts_receivable_change
     - ``TR.F.AcctRcvblCF(Period=FQ0)``
   * - fcf_capital_expenditure
     - ``TR.F.CAPEXNetCF(Period=FQ0)``
   * - fcf_cash_and_cash_equivalents_change
     - ``TR.F.NetChgInCashTot(Period=FQ0)``
   * - fcf_cash_exchange_rate_effect
     - ``TR.F.FXEffectsCF(Period=FQ0)``
   * - fcf_common_stock_dividend_payments
     - ``TR.F.DivComCashPaid(Period=FQ0)``
   * - fcf_common_stock_issuance_proceeds
     - ``TR.F.StockComIssuedSoldCF(Period=FQ0)``
   * - fcf_common_stock_repurchase
     - ``TR.F.StockComRepurchRetiredCF(Period=FQ0)``
   * - fcf_deferred_income_tax
     - ``TR.F.DefIncTaxIncTaxCreditsCF(Period=FQ0)``
   * - fcf_depreciation_and_amortization
     - ``TR.F.DeprDeplAmortCF(Period=FQ0)``
   * - fcf_dividend_payments
     - ``TR.F.DivPaidCashTotCF(Period=FQ0)``
   * - fcf_free_cash_flow
     - ``TR.F.NonGAAPFreeCashFlow(Period=FQ0)``
   * - fcf_interest_payments
     - ``TR.F.IntrPaidCash(Period=FQ0)``
   * - fcf_inventory_change
     - ``TR.F.InvntCF(Period=FQ0)``
   * - fcf_investment_sales_maturities_and_collections_proceeds
     - ``TR.F.InvstSecSoldMaturedCF(Period=FQ0)``
   * - fcf_investments_purchase
     - ``TR.F.InvstSecPurchCF(Period=FQ0)``
   * - fcf_net_business_acquisition_payments
     - ``TR.F.AcqOfBizCF(Period=FQ0)``
   * - fcf_net_cash_from_financing_activities
     - ``TR.F.NetCashFlowFin(Period=FQ0)``
   * - fcf_net_cash_from_investing_activities
     - ``TR.F.NetCashFlowInvst(Period=FQ0)``
   * - fcf_net_cash_from_operating_activities
     - ``TR.F.NetCashFlowOp(Period=FQ0)``
   * - fcf_net_common_stock_issuance_proceeds
     - ``TR.F.StockComNetCF(Period=FQ0)``
   * - fcf_net_debt_issuance_proceeds
     - ``TR.F.DebtIssuedLTSTCF(Period=FQ0)``
   * - fcf_net_income
     - ``TR.F.ProfLossStartingLineCF(Period=FQ0)``
   * - fcf_net_income_tax_payments
     - ``TR.F.IncTaxPaidReimbIndirCF(Period=FQ0)``
   * - fcf_net_longterm_debt_issuance_proceeds
     - ``TR.F.DebtIssuedLTCF(Period=FQ0)``
   * - fcf_net_shortterm_debt_issuance_proceeds
     - ``TR.F.DebtIssuedSTCF(Period=FQ0)``
   * - fcf_net_stock_issuance_proceeds
     - ``TR.F.StockComPrefOthIssuedSoldCF(Period=FQ0)``
   * - fcf_other_financing_activities
     - ``TR.F.OthFinCashFlow(Period=FQ0)``
   * - fcf_other_investing_activities
     - ``TR.F.OthInvstCashFlow(Period=FQ0)``
   * - fcf_other_noncash_items
     - ``TR.F.OthNonCashItemsReconcAdjCF(Period=FQ0)``
   * - fcf_other_working_capital
     - ``TR.F.OthAssetsLiabNetCF(Period=FQ0)``
   * - fcf_period_end_cash
     - ``TR.F.NetCashEndBal(Period=FQ0)``
   * - fcf_period_start_cash
     - ``TR.F.NetCashBegBal(Period=FQ0)``
   * - fcf_preferred_stock_dividend_payments
     - ``TR.F.DivPrefCashPaid(Period=FQ0)``
   * - fcf_preferred_stock_issuance_proceeds
     - ``TR.F.StockPrefIssuedSoldCF(Period=FQ0)``
   * - fcf_property_plant_and_equipment_purchase
     - ``TR.F.PPEPurchCF(Period=FQ0)``
   * - fcf_stock_based_compensation
     - ``TR.F.ShrBasedPaymtCF(Period=FQ0)``
   * - fcf_working_capital_change
     - ``TR.F.WkgCapCF(Period=FQ0)``

