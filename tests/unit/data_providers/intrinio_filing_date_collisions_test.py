"""
Unit tests for resolving several fiscal periods dated to a single filing.

A filing presents its own period alongside earlier ones as comparatives, and Intrinio dates each
comparative by the filing that carried it. A company that has only ever filed once for a period
therefore has no earlier original statement for it, so several periods share one filing date.
Alcoa's 2016 separation is the usual shape: its 2017-03-15 10-K carries fiscal 2014, 2015 and 2016.

The fundamentals clock runs on the filing date, so those collisions otherwise cost the security
its entire fundamental data.
"""

import datetime

from kaxanuk.data_curator.data_providers.intrinio import (
    Intrinio,
    _IntrinioPeriodRecord,
    _IntrinioStatementFinancials,
)


def _period_record(*, filing_date: str, end_date: str, fiscal_year=None, fiscal_period=None):
    return _IntrinioPeriodRecord(
        reported_currency='usd',
        tag_values={
            'filing_date': datetime.date.fromisoformat(filing_date),
            'end_date': datetime.date.fromisoformat(end_date),
            'fiscal_year': fiscal_year,
            'fiscal_period': fiscal_period,
        },
    )


def _resolve(records):
    return Intrinio._resolve_filing_date_collisions(
        main_identifier='AAAA',
        period_records=records,
    )


class TestResolveFilingDateCollisions:
    def test_keeps_the_latest_period_a_filing_made_known(self) -> None:
        # Alcoa's 2017-03-15 10-K carried fiscal 2014, 2015 and 2016
        records = [
            _period_record(filing_date='2017-03-15', end_date='2014-12-31'),
            _period_record(filing_date='2017-03-15', end_date='2015-12-31'),
            _period_record(filing_date='2017-03-15', end_date='2016-12-31'),
        ]

        resolved = _resolve(records)

        assert len(resolved) == 1
        assert resolved[0].tag_values['end_date'] == datetime.date(2016, 12, 31)

    def test_keeps_the_latest_regardless_of_the_order_they_arrive_in(self) -> None:
        records = [
            _period_record(filing_date='2017-03-15', end_date='2016-12-31'),
            _period_record(filing_date='2017-03-15', end_date='2014-12-31'),
        ]

        resolved = _resolve(records)

        assert resolved[0].tag_values['end_date'] == datetime.date(2016, 12, 31)

    def test_leaves_distinct_filing_dates_alone(self) -> None:
        records = [
            _period_record(filing_date='2024-02-01', end_date='2023-12-31'),
            _period_record(filing_date='2024-05-01', end_date='2024-03-31'),
            _period_record(filing_date='2024-08-01', end_date='2024-06-30'),
        ]

        resolved = _resolve(records)

        assert len(resolved) == 3
        assert sorted(r.tag_values['end_date'] for r in resolved) == [
            datetime.date(2023, 12, 31),
            datetime.date(2024, 3, 31),
            datetime.date(2024, 6, 30),
        ]

    def test_resolves_each_colliding_filing_date_independently(self) -> None:
        records = [
            _period_record(filing_date='2017-05-10', end_date='2016-03-31'),
            _period_record(filing_date='2017-05-10', end_date='2017-03-31'),
            _period_record(filing_date='2017-08-03', end_date='2016-06-30'),
            _period_record(filing_date='2017-08-03', end_date='2017-06-30'),
        ]

        resolved = _resolve(records)

        assert sorted(r.tag_values['end_date'] for r in resolved) == [
            datetime.date(2017, 3, 31),
            datetime.date(2017, 6, 30),
        ]

    def test_leaves_a_single_record_untouched(self) -> None:
        records = [_period_record(filing_date='2024-02-01', end_date='2023-12-31')]

        assert len(_resolve(records)) == 1

    def test_produces_filing_dates_the_data_block_accepts(self) -> None:
        """The clock sync field must end up free of duplicates, which is the whole point."""
        records = [
            _period_record(filing_date='2017-03-15', end_date='2014-12-31'),
            _period_record(filing_date='2017-03-15', end_date='2016-12-31'),
            _period_record(filing_date='2017-05-10', end_date='2017-03-31'),
        ]

        filing_dates = [r.tag_values['filing_date'] for r in _resolve(records)]

        assert len(filing_dates) == len(set(filing_dates))


class TestEmptyIncomeStatements:
    """
    Intrinio serves some income statements as metadata only, carrying no line items.

    Such a period has nothing to report and no reporting currency either, since the currency is
    read off the line items' units, so it cannot form a valid row. Letting one through produced
    either `Incorrect data in FundamentalDataRow.currency` or an empty non-nullable
    `income_statement`, and cost the security its whole fundamental data.
    """

    @staticmethod
    def _statement(*, code: str, values: dict, currency: str | None = 'USD'):
        return _IntrinioStatementFinancials(
            accepted_date=None,
            filing_date=datetime.date(2017, 8, 21),
            fiscal_period='Q3',
            fiscal_year=2017,
            period_end_date=datetime.date(2017, 6, 30),
            reported_currency=currency,
            statement_code=code,
            values=values,
        )

    def test_drops_a_period_whose_income_statement_carries_no_line_items(self) -> None:
        merged = Intrinio._merge_period_statements(period_statements=[
            self._statement(code='income_statement', values={}, currency=None),
        ])

        assert merged is None

    def test_drops_a_period_whose_income_statement_values_are_all_null(self) -> None:
        merged = Intrinio._merge_period_statements(period_statements=[
            self._statement(code='income_statement', values={'totalrevenue': None}, currency=None),
        ])

        assert merged is None

    def test_drops_a_period_whose_income_statement_has_no_reporting_currency(self) -> None:
        merged = Intrinio._merge_period_statements(period_statements=[
            self._statement(code='income_statement', values={'totalrevenue': 10.0}, currency=None),
        ])

        assert merged is None

    def test_keeps_a_period_whose_income_statement_reports_line_items(self) -> None:
        merged = Intrinio._merge_period_statements(period_statements=[
            self._statement(code='income_statement', values={'totalrevenue': 10.0}),
        ])

        assert merged is not None
        assert merged.reported_currency == 'USD'
        assert merged.tag_values['totalrevenue'] == 10.0
