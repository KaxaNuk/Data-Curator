"""
Unit tests for the Intrinio provider's timezone normalization of accepted_date.

Intrinio serves earnings-disclosure timestamps (`earnings_disclosed_at`) as
timezone-aware UTC datetimes, but the curator's `accepted_date` column is
timezone-naive like every other provider's. PyArrow cannot even serialize a
timezone-aware timestamp to CSV on a host without an IANA timezone database
(Windows has none by default), so the statement builder drops the timezone while
preserving the underlying instant. These tests pin that normalization directly.
"""

import datetime
import types

from kaxanuk.data_curator.data_providers.intrinio import Intrinio


def _standardized_response(earnings_disclosed_at: datetime.datetime | None) -> types.SimpleNamespace:
    """Build a minimal standardized-financials response the statement builder can flatten."""
    return types.SimpleNamespace(
        fundamental=types.SimpleNamespace(
            earnings_disclosed_at=earnings_disclosed_at,
            filing_date=datetime.date(2026, 2, 25),
            fiscal_year=2026,
            fiscal_period='Q1',
            end_date=datetime.date(2026, 4, 26),
            statement_code='income_statement',
        ),
        standardized_financials=[],
    )


class TestBuildStatementFinancialsAcceptedDate:
    def test_strips_timezone_from_utc_accepted_date(self) -> None:
        aware = datetime.datetime(2026, 2, 25, 21, 31, 25, tzinfo=datetime.UTC)

        statement = Intrinio._build_statement_financials(_standardized_response(aware))

        assert statement.accepted_date == datetime.datetime(2026, 2, 25, 21, 31, 25)
        assert statement.accepted_date.tzinfo is None

    def test_converts_non_utc_accepted_date_to_utc(self) -> None:
        eastern = datetime.timezone(datetime.timedelta(hours=-5))
        aware = datetime.datetime(2026, 2, 25, 16, 31, 25, tzinfo=eastern)

        statement = Intrinio._build_statement_financials(_standardized_response(aware))

        # 16:31 at UTC-5 is 21:31 UTC, stored as a naive UTC datetime
        assert statement.accepted_date == datetime.datetime(2026, 2, 25, 21, 31, 25)
        assert statement.accepted_date.tzinfo is None

    def test_preserves_none_accepted_date(self) -> None:
        statement = Intrinio._build_statement_financials(_standardized_response(None))

        assert statement.accepted_date is None
