"""
Unit tests for the Intrinio provider's calculated-Q4 reconciliation guard.

Intrinio serves the year-end quarter as a `calculated` statement equal to
`FY - (Q1 + Q2 + Q3)`, without exposing which vintage (as-reported or restated)
of those quarters it used. The provider selects the as-originally-reported
quarters, so if Intrinio ever recomputes that Q4 from restated quarters, the
discrete fourth quarter would silently carry restated data and break the
point-in-time contract — and nothing in the metadata would reveal it.

The guard recomputes `FY_reported - (Q1 + Q2 + Q3)` from the provider's own
selected reported statements and compares it, line item by line item, to
Intrinio's calculated Q4, warning (never raising) when a core additive line
fails to reconcile. These tests pin that behavior, including that non-additive
per-share and share-count lines are not reconciled and that incomplete years are
skipped rather than flagged.
"""

import datetime
import logging
import types

import pytest

from kaxanuk.data_curator.data_providers.intrinio import (
    Intrinio,
    _IntrinioStatementFinancials,
)
from kaxanuk.data_curator.entities import (
    FundamentalData,
    MarketInstrumentIdentifier,
)


LOGGER_NAME = 'kaxanuk.data_curator.data_providers.intrinio'


def _income(
    fiscal_year: int,
    fiscal_period: str,
    values: dict[str, float],
) -> _IntrinioStatementFinancials:
    """Build a minimal income-statement record for the reconciliation guard to read."""
    return _IntrinioStatementFinancials(
        accepted_date=None,
        filing_date=None,
        fiscal_period=fiscal_period,
        fiscal_year=fiscal_year,
        period_end_date=datetime.date(fiscal_year, 12, 31),
        reported_currency='USD',
        statement_code='income_statement',
        values=values,
    )


def _reconciling_year() -> list[_IntrinioStatementFinancials]:
    # Q4 = FY - (Q1+Q2+Q3): netincome 100-(20+30+25)=25, totalrevenue 400-(100+100+100)=100
    return [
        _income(2024, 'FY', {'netincome': 100.0, 'totalrevenue': 400.0}),
        _income(2024, 'Q1', {'netincome': 20.0, 'totalrevenue': 100.0}),
        _income(2024, 'Q2', {'netincome': 30.0, 'totalrevenue': 100.0}),
        _income(2024, 'Q3', {'netincome': 25.0, 'totalrevenue': 100.0}),
        _income(2024, 'Q4', {'netincome': 25.0, 'totalrevenue': 100.0}),
    ]


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.message
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]


class TestReconcileCalculatedQuarters:
    def test_stays_silent_when_the_quarters_reconcile(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            Intrinio._reconcile_calculated_quarters(
                main_identifier='XXXX',
                income_statements=_reconciling_year(),
            )

        assert _warnings(caplog) == []

    def test_warns_when_the_calculated_q4_does_not_match_the_reported_quarters(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        statements = _reconciling_year()
        # replace Q4 with one that overstates net income by 15 (as if built from restated quarters)
        statements = [
            statement
            for statement in statements
            if statement.fiscal_period != 'Q4'
        ]
        statements.append(_income(2024, 'Q4', {'netincome': 40.0, 'totalrevenue': 100.0}))

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            Intrinio._reconcile_calculated_quarters(
                main_identifier='XXXX',
                income_statements=statements,
            )

        messages = _warnings(caplog)
        assert len(messages) == 1
        assert 'XXXX' in messages[0]
        assert '2024' in messages[0]
        assert 'netincome' in messages[0]
        # the line that does reconcile is not named
        assert 'totalrevenue' not in messages[0]

    def test_does_not_reconcile_non_additive_per_share_and_share_count_lines(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # EPS and weighted-average share counts do not sum across quarters; they must be ignored,
        # not flagged, even though FY - (Q1+Q2+Q3) obviously won't equal the Q4 value for them.
        statements = [
            _income(2024, 'FY', {'netincome': 100.0, 'basiceps': 4.0, 'weightedavebasicsharesos': 25.0}),
            _income(2024, 'Q1', {'netincome': 20.0, 'basiceps': 0.8, 'weightedavebasicsharesos': 25.0}),
            _income(2024, 'Q2', {'netincome': 30.0, 'basiceps': 1.2, 'weightedavebasicsharesos': 25.0}),
            _income(2024, 'Q3', {'netincome': 25.0, 'basiceps': 1.0, 'weightedavebasicsharesos': 25.0}),
            _income(2024, 'Q4', {'netincome': 25.0, 'basiceps': 1.0, 'weightedavebasicsharesos': 25.0}),
        ]

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            Intrinio._reconcile_calculated_quarters(
                main_identifier='XXXX',
                income_statements=statements,
            )

        assert _warnings(caplog) == []

    def test_skips_years_that_are_missing_a_quarter(self, caplog: pytest.LogCaptureFixture) -> None:
        # An incomplete year can't be reconciled; staying silent avoids false alarms.
        statements = [
            _income(2024, 'FY', {'netincome': 100.0}),
            _income(2024, 'Q1', {'netincome': 20.0}),
            _income(2024, 'Q2', {'netincome': 30.0}),
        ]

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            Intrinio._reconcile_calculated_quarters(
                main_identifier='XXXX',
                income_statements=statements,
            )

        assert _warnings(caplog) == []

    def test_tolerates_floating_point_noise(self, caplog: pytest.LogCaptureFixture) -> None:
        # Real values are large floats; a sub-dollar rounding residue must not trip the guard.
        statements = [
            _income(2024, 'FY', {'netincome': 58_471_000_000.0}),
            _income(2024, 'Q1', {'netincome': 13_419_000_000.0}),
            _income(2024, 'Q2', {'netincome': 18_149_000_000.0}),
            _income(2024, 'Q3', {'netincome': 12_898_000_000.0}),
            _income(2024, 'Q4', {'netincome': 14_005_000_000.000004}),
        ]

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            Intrinio._reconcile_calculated_quarters(
                main_identifier='JPM',
                income_statements=statements,
            )

        assert _warnings(caplog) == []


class TestKeepsAnnualIncomeForReconciliation:
    def test_keeps_the_reported_annual_income_statement_in_quarterly_mode(self) -> None:
        # The guard needs the annual income statement, so quarterly selection must retain it
        # (previously only the annual balance sheet was kept).
        summary = types.SimpleNamespace(
            type='reported',
            fiscal_period='FY',
            statement_code='income_statement',
        )

        assert Intrinio._should_keep_fundamental(
            summary,
            period_mode=Intrinio.FundamentalPeriods.QUARTERLY,
        )


def _balance(fiscal_year: int, fiscal_period: str) -> _IntrinioStatementFinancials:
    return _IntrinioStatementFinancials(
        accepted_date=None,
        filing_date=datetime.date(fiscal_year, 12, 31),
        fiscal_period=fiscal_period,
        fiscal_year=fiscal_year,
        period_end_date=datetime.date(fiscal_year, 12, 31),
        reported_currency='USD',
        statement_code='balance_sheet_statement',
        values={'totalassets': 1.0},
    )


class TestGetFundamentalDataWiring:
    def _collected_with_unreconciled_q4(self) -> list[_IntrinioStatementFinancials]:
        return [
            _income(2024, 'FY', {'netincome': 100.0}),
            _income(2024, 'Q1', {'netincome': 20.0}),
            _income(2024, 'Q2', {'netincome': 30.0}),
            _income(2024, 'Q3', {'netincome': 25.0}),
            _income(2024, 'Q4', {'netincome': 40.0}),   # should be 25
            _balance(2024, 'FY'),
        ]

    def test_derives_the_fourth_quarter_and_strips_annual_inputs_before_assembly(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        provider = Intrinio(api_key='12345')
        collected = self._collected_with_unreconciled_q4()
        provider._collect_statement_financials = lambda **_kwargs: collected

        assembled_with: dict[str, list[_IntrinioStatementFinancials]] = {}

        def _capture_assembly(
            *,
            main_identifier: str,
            statement_financials: list[_IntrinioStatementFinancials],
        ) -> FundamentalData:
            assembled_with['statements'] = statement_financials

            return FundamentalData(
                main_identifier=MarketInstrumentIdentifier(main_identifier),
                rows={},
            )

        provider._assemble_fundamental_data = _capture_assembly

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            provider.get_fundamental_data(
                main_identifier='XXXX',
                period='quarterly',
                start_date=datetime.date(2024, 1, 1),
                end_date=datetime.date(2024, 12, 31),
            )

        passed = assembled_with['statements']
        # Intrinio's unreconciled Q4 (netincome 40) was replaced by the derived one: 100-(20+30+25)=25
        fourth_quarter = next(
            statement
            for statement in passed
            if statement.statement_code == 'income_statement' and statement.fiscal_period == 'Q4'
        )
        assert fourth_quarter.values['netincome'] == 25.0
        # the derived quarter reconciles, so the guard stays silent
        assert _warnings(caplog) == []
        # the annual income statement was a derivation input, not passed on to assembly
        assert not any(
            statement.statement_code == 'income_statement' and statement.fiscal_period == 'FY'
            for statement in passed
        )
        # the annual balance sheet (a real quarterly-mode input) survives
        assert any(
            statement.statement_code == 'balance_sheet_statement' and statement.fiscal_period == 'FY'
            for statement in passed
        )
