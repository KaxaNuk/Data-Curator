"""
Unit tests for the Intrinio provider deriving its own discrete fourth quarter.

Intrinio's `calculated` fourth quarter (`FY - (Q1+Q2+Q3)`) can be built from
restated quarters, which silently breaks the point-in-time contract (proven live
on MSFT 2016/2017). Instead of trusting it, the provider recomputes the discrete
fourth quarter from its own as-reported statements: every additive dollar line is
`FY - (Q1+Q2+Q3)`, and because per-share earnings and share counts don't sum
across quarters, the share counts are kept from Intrinio's Q4 (a count is
unaffected by a revenue restatement) and EPS is recomputed from the derived net
income divided by that count. The annual statements are inputs only and stay in
the list for the reconciliation guard; get_fundamental_data drops them before
assembly.
"""

import datetime
import types

from kaxanuk.data_curator.data_providers.intrinio import (
    Intrinio,
    _IntrinioStatementFinancials,
)


INCOME = 'income_statement'
CASH_FLOW = 'cash_flow_statement'
_PERIOD_END = {
    'Q1': (3, 31),
    'Q2': (6, 30),
    'Q3': (9, 30),
    'Q4': (12, 31),
    'FY': (12, 31),
}


def _stmt(
    fiscal_year: int,
    fiscal_period: str,
    statement_code: str,
    values: dict[str, float],
) -> _IntrinioStatementFinancials:
    (month, day) = _PERIOD_END[fiscal_period]

    return _IntrinioStatementFinancials(
        accepted_date=None,
        filing_date=None,
        fiscal_period=fiscal_period,
        fiscal_year=fiscal_year,
        period_end_date=datetime.date(fiscal_year, month, day),
        reported_currency='USD',
        statement_code=statement_code,
        values=values,
    )


def _q4(
    statements: list[_IntrinioStatementFinancials],
    statement_code: str,
) -> _IntrinioStatementFinancials:
    return next(
        statement
        for statement in statements
        if statement.statement_code == statement_code and statement.fiscal_period == 'Q4'
    )


class TestDeriveDiscreteFourthQuarters:
    def _income_year(self) -> list[_IntrinioStatementFinancials]:
        return [
            _stmt(2024, 'FY', INCOME, {
                'totalrevenue': 400.0,
                'netincome': 100.0,
                'netincometocommon': 100.0,
                'weightedavebasicsharesos': 50.0,
                'basiceps': 2.0,
            }),
            _stmt(2024, 'Q1', INCOME, {'totalrevenue': 100.0, 'netincome': 20.0, 'netincometocommon': 20.0}),
            _stmt(2024, 'Q2', INCOME, {'totalrevenue': 100.0, 'netincome': 30.0, 'netincometocommon': 30.0}),
            _stmt(2024, 'Q3', INCOME, {'totalrevenue': 100.0, 'netincome': 25.0, 'netincometocommon': 25.0}),
            # Intrinio's calculated Q4 with restated (wrong) dollars but a usable share count
            _stmt(2024, 'Q4', INCOME, {
                'totalrevenue': 999.0,
                'netincome': 999.0,
                'netincometocommon': 999.0,
                'weightedavebasicsharesos': 40.0,
                'basiceps': 24.975,
            }),
        ]

    def test_replaces_q4_dollar_flows_with_the_annual_minus_reported_quarters(self) -> None:
        result = Intrinio._derive_discrete_fourth_quarters(self._income_year())
        fourth_quarter = _q4(result, INCOME)

        assert fourth_quarter.values['totalrevenue'] == 100.0
        assert fourth_quarter.values['netincome'] == 25.0
        assert fourth_quarter.values['netincometocommon'] == 25.0

    def test_keeps_the_share_count_and_recomputes_eps_from_derived_income(self) -> None:
        result = Intrinio._derive_discrete_fourth_quarters(self._income_year())
        fourth_quarter = _q4(result, INCOME)

        # the share count is kept from Intrinio's Q4 (a count isn't restated)
        assert fourth_quarter.values['weightedavebasicsharesos'] == 40.0
        # EPS is recomputed from the derived net income (25) over that count (40), not left at 24.975
        assert fourth_quarter.values['basiceps'] == 0.625

    def test_keeps_the_annual_income_in_the_list_for_the_guard(self) -> None:
        result = Intrinio._derive_discrete_fourth_quarters(self._income_year())

        assert any(
            statement.statement_code == INCOME and statement.fiscal_period == 'FY'
            for statement in result
        )

    def test_derives_the_fourth_quarter_cash_flow(self) -> None:
        statements = [
            _stmt(2024, 'FY', CASH_FLOW, {'netcashfromoperatingactivities': 200.0}),
            _stmt(2024, 'Q1', CASH_FLOW, {'netcashfromoperatingactivities': 50.0}),
            _stmt(2024, 'Q2', CASH_FLOW, {'netcashfromoperatingactivities': 60.0}),
            _stmt(2024, 'Q3', CASH_FLOW, {'netcashfromoperatingactivities': 40.0}),
            _stmt(2024, 'Q4', CASH_FLOW, {'netcashfromoperatingactivities': 999.0}),
        ]

        result = Intrinio._derive_discrete_fourth_quarters(statements)

        assert _q4(result, CASH_FLOW).values['netcashfromoperatingactivities'] == 50.0

    def test_leaves_the_fourth_quarter_untouched_when_a_quarter_is_missing(self) -> None:
        statements = [
            statement
            for statement in self._income_year()
            if statement.fiscal_period != 'Q3'
        ]

        result = Intrinio._derive_discrete_fourth_quarters(statements)

        # without Q3 the discrete quarter can't be derived, so Intrinio's value is left as-is
        assert _q4(result, INCOME).values['netincome'] == 999.0


class TestKeepsAnnualCashFlowForDerivation:
    def test_keeps_the_reported_annual_cash_flow_in_quarterly_mode(self) -> None:
        # the annual cash flow is an input for deriving the discrete fourth-quarter cash flow,
        # so quarterly selection must retain it alongside the annual income statement
        summary = types.SimpleNamespace(
            type='reported',
            fiscal_period='FY',
            statement_code='cash_flow_statement',
        )

        assert Intrinio._should_keep_fundamental(
            summary,
            period_mode=Intrinio.FundamentalPeriods.QUARTERLY,
        )
