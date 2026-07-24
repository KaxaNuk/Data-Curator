"""
Unit tests for the Intrinio provider's point-in-time fundamental derivations.

Intrinio's reported statements omit several line items the curator's contract
defines but that a provider like FMP serves directly. The provider derives the
ones that are exact functions of as-reported values from each period's own
reported tags (never from the restated `calculations` statement), so they stay
point-in-time correct. `_add_derived_fundamental_values` performs that
derivation, injecting `derived_*` synthetic tags into a period's tag values.

These tests pin the arithmetic and the null policy directly, without going
through the whole endpoint-to-entity pipeline.
"""

from kaxanuk.data_curator.data_providers.intrinio import Intrinio


class TestDerivedNetDebtIssuance:
    def test_sums_issuance_and_repayment(self) -> None:
        tag_values = {
            'issuanceofdebt': 1000.0,
            'repaymentofdebt': -400.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_debt_issuance'] == 600.0

    def test_treats_absent_issuance_as_zero_flow(self) -> None:
        tag_values = {
            'repaymentofdebt': -400.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_debt_issuance'] == -400.0

    def test_treats_absent_repayment_as_zero_flow(self) -> None:
        tag_values = {
            'issuanceofdebt': 1000.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_debt_issuance'] == 1000.0

    def test_omitted_when_both_components_absent(self) -> None:
        tag_values = {}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_net_debt_issuance' not in tag_values


class TestDerivedFreeCashFlow:
    def test_adds_negative_capex_to_operating_cash_flow(self) -> None:
        tag_values = {
            'netcashfromoperatingactivities': 5000.0,
            'purchaseofplantpropertyandequipment': -1200.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_free_cash_flow'] == 3800.0

    def test_treats_absent_capex_as_zero_flow(self) -> None:
        tag_values = {
            'netcashfromoperatingactivities': 5000.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_free_cash_flow'] == 5000.0

    def test_omitted_without_operating_cash_flow(self) -> None:
        tag_values = {
            'purchaseofplantpropertyandequipment': -1200.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_free_cash_flow' not in tag_values


class TestDerivedCostsAndExpenses:
    def test_sums_cost_of_revenue_and_operating_expenses(self) -> None:
        tag_values = {
            'totalcostofrevenue': 700.0,
            'totaloperatingexpenses': 250.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_costs_and_expenses'] == 950.0

    def test_omitted_when_a_component_is_absent(self) -> None:
        tag_values = {
            'totalcostofrevenue': 700.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_costs_and_expenses' not in tag_values


class TestDerivedNetIncomeDeductions:
    def test_subtracts_common_income_from_net_income(self) -> None:
        tag_values = {
            'netincome': 1000.0,
            'netincometocommon': 940.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_income_deductions'] == 60.0

    def test_omitted_when_a_component_is_absent(self) -> None:
        tag_values = {
            'netincome': 1000.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_net_income_deductions' not in tag_values


class TestDerivedCashAndShorttermInvestments:
    def test_sums_cash_and_shortterm_investments(self) -> None:
        tag_values = {
            'cashandequivalents': 300.0,
            'shortterminvestments': 120.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_cash_and_shortterm_investments'] == 420.0

    def test_omitted_when_shortterm_investments_absent(self) -> None:
        tag_values = {
            'cashandequivalents': 300.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_cash_and_shortterm_investments' not in tag_values


class TestDerivedEbitAndEbitda:
    def test_ebit_adds_back_net_interest_to_pretax_income(self) -> None:
        # Intrinio's convention, verified live (NVDA FY2026): EBIT = pretax + interest exp - interest inc
        tag_values = {
            'totalpretaxincome': 141_450.0,
            'totalinterestexpense': 259.0,
            'totalinterestincome': 2_300.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_ebit'] == 139_409.0

    def test_ebit_equals_pretax_income_when_interest_is_not_separated(self) -> None:
        # issuers that fold interest into "other income" (e.g. AAPL) expose no interest tags, so
        # absent interest is treated as zero and EBIT equals pre-tax income, exactly as Intrinio does
        tag_values = {'totalpretaxincome': 123_485.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_ebit'] == 123_485.0

    def test_ebitda_adds_depreciation_and_amortization_to_ebit(self) -> None:
        tag_values = {
            'totalpretaxincome': 141_450.0,
            'totalinterestexpense': 259.0,
            'totalinterestincome': 2_300.0,
            'depreciationexpense': 2_843.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_ebitda'] == 142_252.0

    def test_no_ebit_or_ebitda_without_pretax_income(self) -> None:
        tag_values = {'totalinterestexpense': 259.0, 'depreciationexpense': 2_843.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_ebit' not in tag_values
        assert 'derived_ebitda' not in tag_values

    def test_no_ebitda_without_depreciation(self) -> None:
        tag_values = {'totalpretaxincome': 123_485.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_ebit'] == 123_485.0
        assert 'derived_ebitda' not in tag_values


class TestDerivedTotalDebtAndNetDebt:
    def test_total_debt_sums_short_and_long_term_debt(self) -> None:
        # Intrinio folds capital leases into long-term debt, so they are not added again;
        # verified to the dollar vs Intrinio's `debt` on NVDA FY2026 (999 + 7,469 = 8,468)
        tag_values = {'shorttermdebt': 999.0, 'longtermdebt': 7_469.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_total_debt'] == 8_468.0

    def test_total_debt_treats_an_absent_component_as_zero(self) -> None:
        tag_values = {'longtermdebt': 7_469.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_total_debt'] == 7_469.0

    def test_net_debt_subtracts_cash_and_shortterm_investments(self) -> None:
        # verified vs Intrinio's `netdebt` on NVDA FY2026: 8,468 - 10,605 - 51,951 = -54,088
        tag_values = {
            'shorttermdebt': 999.0,
            'longtermdebt': 7_469.0,
            'cashandequivalents': 10_605.0,
            'shortterminvestments': 51_951.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_debt'] == -54_088.0

    def test_no_debt_values_without_any_debt(self) -> None:
        tag_values = {'cashandequivalents': 10_605.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_total_debt' not in tag_values
        assert 'derived_net_debt' not in tag_values

    def test_no_net_debt_without_cash(self) -> None:
        tag_values = {'longtermdebt': 7_469.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_total_debt'] == 7_469.0
        assert 'derived_net_debt' not in tag_values


class TestDerivedNetStockIssuance:
    def test_net_common_stock_issuance_nets_the_repurchase(self) -> None:
        # repurchase is a negative flow, so adding it nets it out (NVDA FY2026)
        tag_values = {'issuanceofcommonequity': 644.0, 'repurchaseofcommonequity': -40_086.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_common_stock_issuance'] == -39_442.0

    def test_net_stock_issuance_includes_preferred(self) -> None:
        tag_values = {
            'issuanceofcommonequity': 644.0,
            'issuanceofpreferredequity': 100.0,
            'repurchaseofcommonequity': -40_086.0,
        }
        Intrinio._add_derived_fundamental_values(tag_values)

        assert tag_values['derived_net_stock_issuance'] == -39_342.0

    def test_omitted_when_no_equity_flows_are_reported(self) -> None:
        tag_values = {'netincome': 100.0}
        Intrinio._add_derived_fundamental_values(tag_values)

        assert 'derived_net_common_stock_issuance' not in tag_values
        assert 'derived_net_stock_issuance' not in tag_values
