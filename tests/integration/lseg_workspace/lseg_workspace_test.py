"""
Integration tests for LsegWorkspace data provider.

These tests exercise the full LsegWorkspace processing pipeline using real
pickle fixtures instead of the live Refinitiv API.  Only the API transport
layer (session management, HTTP fetch, currency lookup) is mocked; all data
parsing, filtering, deduplication, dividend-adjustment, and entity assembly
run through the production code.

Fixtures contain real LSEG bulk-format data for AAPL.OQ and NVDA.OQ
covering market, fundamental, dividend, and split data.
"""

import datetime
import decimal
from pathlib import Path
from types import SimpleNamespace

import pandas
import pytest

from kaxanuk.data_curator.data_providers.lseg_workspace import LsegWorkspace
from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    DividendDataRow,
    FundamentalData,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    MarketData,
    MarketDataDailyRow,
    MarketInstrumentIdentifier,
    SplitData,
    SplitDataRow,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
PICKLE_MARKET_PATH = FIXTURE_DIR / "lseg_market_test.pkl"
PICKLE_FUNDAMENTAL_PATH = FIXTURE_DIR / "lseg_fundamental_test.pkl"
PICKLE_SPLIT_PATH = FIXTURE_DIR / "lseg_split_test.pkl"
PICKLE_DIVIDEND_PATH = FIXTURE_DIR / "lseg_dividend_test.pkl"

FIXTURE_RICS = ("NVDA.OQ", "AAPL.OQ")

START_DATE = datetime.date(2020, 1, 1)
END_DATE = datetime.date(2026, 12, 31)


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def raw_market_df():
    """Load the market data pickle with dates converted to DD/MM/YYYY format.

    The LSEG API returns market dates in DD/MM/YYYY strings.  The pickle
    stores them as YYYY-MM-DD strings, so we convert here to match the
    format expected by the production processing pipeline.
    """
    df = pandas.read_pickle(PICKLE_MARKET_PATH)  # noqa: S301
    df["Date"] = pandas.to_datetime(df["Date"]).dt.strftime("%d/%m/%Y")
    return df


@pytest.fixture(scope="module")
def raw_fundamental_df():
    """Load the fundamental data pickle as-is (ISO 8601 dates)."""
    return pandas.read_pickle(PICKLE_FUNDAMENTAL_PATH)  # noqa: S301


@pytest.fixture(scope="module")
def raw_split_df():
    """Load the split data pickle as-is (ISO 8601 dates)."""
    return pandas.read_pickle(PICKLE_SPLIT_PATH)  # noqa: S301


@pytest.fixture(scope="module")
def raw_dividend_df():
    """Load the dividend data pickle as-is (ISO 8601 dates)."""
    return pandas.read_pickle(PICKLE_DIVIDEND_PATH)  # noqa: S301


@pytest.fixture(scope="module")
def test_configuration():
    """Build a Configuration covering all fixture RICs and date range."""
    return Configuration(
        start_date=START_DATE,
        end_date=END_DATE,
        period="quarterly",
        identifiers=FIXTURE_RICS,
        columns=(
            "m_open",
            "m_high",
            "m_low",
            "m_close",
            "m_volume",
        ),
    )


@pytest.fixture(scope="module")
def initialized_provider(
    raw_market_df,
    raw_fundamental_df,
    raw_split_df,
    raw_dividend_df,
    test_configuration,
):
    """Create a fully initialized LsegWorkspace with all four data types cached.

    Mocks only the API transport layer (session open, data fetch, currency
    lookup).  The entire processing pipeline — column renaming, date parsing,
    NaN filtering, deduplication, dividend-adjusted price calculation, and
    entity assembly — runs through production code.
    """
    LsegWorkspace._shared_cache = {}
    LsegWorkspace._shared_cache_config_key = None

    def mock_attempt_fetch(tickers, fields):
        """Route to the correct pickle fixture based on the requested fields."""
        fields_str = " ".join(fields)
        if "Period=FQ" in fields_str:
            return raw_fundamental_df.copy()
        if "DivExDate" in fields_str or "DivPayDate" in fields_str:
            return raw_dividend_df.copy()
        if "CAExDate" in fields_str or "CATerms" in fields_str:
            return raw_split_df.copy()
        return raw_market_df.copy()

    mock_session = SimpleNamespace(open_state="OpenState.Opened")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(LsegWorkspace, "_attempt_fetch", staticmethod(mock_attempt_fetch))
        mp.setattr(
            LsegWorkspace,
            "_fetch_currency_data",
            staticmethod(lambda tickers: dict.fromkeys(tickers, "USD")),
        )
        mp.setattr("refinitiv.data.open_session", lambda: None)
        mp.setattr("refinitiv.data.session.get_default", lambda: mock_session)

        provider = LsegWorkspace(api_key=None)
        provider.initialize(configuration=test_configuration)

    return provider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _market_row_count_for_ric(df, ric):
    """Count valid market rows for a RIC (non-null Date and Volume)."""
    ric_data = df[df["Instrument"] == ric].copy()
    ric_data = ric_data.dropna(subset=["Date"])
    ric_data = ric_data.dropna(subset=["Volume"])
    return len(ric_data)


# ===========================================================================
# Market Data Integration Tests
# ===========================================================================


class TestMarketDataIntegration:
    """End-to-end integration tests for market data retrieval."""

    def test_returns_market_data_entity(self, initialized_provider):
        """Verify get_market_data returns a properly typed MarketData entity."""
        result = initialized_provider.get_market_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(result, MarketData)

    def test_main_identifier_matches_request(self, initialized_provider):
        """Verify the main_identifier field matches the requested RIC."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert result.main_identifier == MarketInstrumentIdentifier(ric)

    def test_all_rics_produce_non_empty_results(self, initialized_provider):
        """Verify every fixture RIC yields a non-empty set of daily rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert len(result.daily_rows) > 0, f"No daily rows for {ric}"

    def test_daily_rows_sorted_ascending(self, initialized_provider):
        """Verify daily rows dictionary keys are in ascending date order."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            keys = list(result.daily_rows.keys())
            assert keys == sorted(keys), f"Rows not sorted for {ric}"

    def test_daily_rows_contain_market_data_daily_row(self, initialized_provider):
        """Verify every value in daily_rows is a MarketDataDailyRow instance."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.daily_rows.items():
                assert isinstance(row, MarketDataDailyRow), (
                    f"Expected MarketDataDailyRow for {ric} on {key}"
                )

    def test_row_date_field_matches_key(self, initialized_provider):
        """Verify each row's date attribute matches its dictionary key."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.daily_rows.items():
                expected = datetime.datetime.strptime(key, "%Y-%m-%d").date()  # noqa: DTZ007
                assert row.date == expected, (
                    f"Row date {row.date} != key {key} for {ric}"
                )

    def test_row_count_matches_fixture(self, initialized_provider, raw_market_df):
        """Verify the number of output rows matches the fixture after filtering."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            expected = _market_row_count_for_ric(raw_market_df, ric)
            assert len(result.daily_rows) == expected, (
                f"Expected {expected} rows for {ric}, got {len(result.daily_rows)}"
            )

    def test_unadjusted_ohlcv_populated(self, initialized_provider):
        """Verify unadjusted OHLCV fields are populated on a sample of rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            sample = list(result.daily_rows.values())[:10]
            for row in sample:
                assert row.open is not None
                assert row.high is not None
                assert row.low is not None
                assert row.close is not None
                assert row.volume is not None

    def test_split_adjusted_prices_populated(self, initialized_provider):
        """Verify split-adjusted OHLC fields are populated on a sample of rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            sample = list(result.daily_rows.values())[:10]
            for row in sample:
                assert row.open_split_adjusted is not None
                assert row.high_split_adjusted is not None
                assert row.low_split_adjusted is not None
                assert row.close_split_adjusted is not None

    def test_dividend_and_split_adjusted_prices_populated(self, initialized_provider):
        """Verify dividend+split adjusted OHLC fields are populated on a sample."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            sample = list(result.daily_rows.values())[:10]
            for row in sample:
                assert row.open_dividend_and_split_adjusted is not None
                assert row.high_dividend_and_split_adjusted is not None
                assert row.low_dividend_and_split_adjusted is not None
                assert row.close_dividend_and_split_adjusted is not None

    def test_volume_and_vwap_populated(self, initialized_provider):
        """Verify Volume and VWAP are populated in daily market data rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            sample = list(result.daily_rows.values())[:10]
            for row in sample:
                assert row.volume is not None, f"volume is None for {ric}"
                assert row.vwap is not None, f"vwap is None for {ric}"

    def test_ohlcv_values_non_negative(self, initialized_provider):
        """Verify all OHLCV values are non-negative across every row."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.daily_rows.items():
                if row.open is not None:
                    assert row.open >= 0, f"Negative open for {ric} on {key}"
                if row.high is not None:
                    assert row.high >= 0, f"Negative high for {ric} on {key}"
                if row.low is not None:
                    assert row.low >= 0, f"Negative low for {ric} on {key}"
                if row.close is not None:
                    assert row.close >= 0, f"Negative close for {ric} on {key}"
                if row.volume is not None:
                    assert row.volume >= 0, f"Negative volume for {ric} on {key}"

    def test_low_not_greater_than_high(self, initialized_provider):
        """Verify low price never exceeds high price on any trading day."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.daily_rows.items():
                if row.low is not None and row.high is not None:
                    assert row.low <= row.high, (
                        f"Low ({row.low}) > High ({row.high}) for {ric} on {key}"
                    )

    def test_multi_ticker_results_differ(self, initialized_provider):
        """Verify different tickers produce distinct market data."""
        results = {}
        for ric in FIXTURE_RICS:
            results[ric] = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )

        ric_a, ric_b = FIXTURE_RICS
        rows_a = list(results[ric_a].daily_rows.values())
        rows_b = list(results[ric_b].daily_rows.values())

        some_differ = any(
            a.close != b.close
            for a, b in zip(rows_a[:20], rows_b[:20], strict=True)
            if a.close is not None and b.close is not None
        )
        assert some_differ, "Expected different close prices for different tickers"

    def test_cache_has_all_market_endpoints(self, initialized_provider):
        """Verify the cache contains all three market data endpoint keys."""
        expected = {
            LsegWorkspace.Endpoints.MARKET_DATA_DAILY_UNADJUSTED,
            LsegWorkspace.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED,
            LsegWorkspace.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED,
        }
        for ric in FIXTURE_RICS:
            for ep in expected:
                assert ep in initialized_provider.cache[ric], (
                    f"Missing endpoint {ep} in cache[{ric}]"
                )

    def test_date_keys_are_valid_iso_format(self, initialized_provider):
        """Verify all daily_rows keys parse as valid YYYY-MM-DD dates."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key in result.daily_rows:
                try:
                    datetime.datetime.strptime(key, "%Y-%m-%d")  # noqa: DTZ007
                except ValueError:
                    pytest.fail(
                        f"Invalid date key format for {ric}: {key} "
                        "(expected YYYY-MM-DD)"
                    )


# ===========================================================================
# Fundamental Data Integration Tests
# ===========================================================================


class TestFundamentalDataIntegration:
    """End-to-end integration tests for quarterly fundamental data retrieval."""

    def test_returns_fundamental_data_entity(self, initialized_provider):
        """Verify get_fundamental_data returns a FundamentalData entity."""
        result = initialized_provider.get_fundamental_data(
            main_identifier="AAPL.OQ",
            period="quarterly",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(result, FundamentalData)

    def test_main_identifier_matches_request(self, initialized_provider):
        """Verify the main_identifier field matches the requested RIC."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert result.main_identifier == MarketInstrumentIdentifier(ric)

    def test_all_rics_produce_non_empty_results(self, initialized_provider):
        """Verify every fixture RIC produces non-empty fundamental rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert len(result.rows) > 0, f"No fundamental rows for {ric}"

    def test_rows_contain_fundamental_data_row_entities(self, initialized_provider):
        """Verify each row value is a FundamentalDataRow instance."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                if row is not None:
                    assert isinstance(row, FundamentalDataRow), (
                        f"Expected FundamentalDataRow for {ric} on {key}"
                    )

    def test_quarterly_alignment(self, initialized_provider):
        """Verify fiscal period values are valid quarterly identifiers (Q1-Q4)."""
        valid_periods = {"Q1", "Q2", "Q3", "Q4"}
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                if row is not None:
                    assert row.fiscal_period in valid_periods, (
                        f"Invalid fiscal_period '{row.fiscal_period}' "
                        f"for {ric} on {key}"
                    )

    def test_filing_date_not_before_period_end_date(self, initialized_provider):
        """Verify filing dates are on or after the corresponding period end dates."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for row in result.rows.values():
                if row is not None:
                    assert row.filing_date >= row.period_end_date, (
                        f"Filing {row.filing_date} before period end "
                        f"{row.period_end_date} for {ric}"
                    )

    def test_rows_sorted_by_filing_date(self, initialized_provider):
        """Verify fundamental data rows are sorted by key (filing date) ascending."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            keys = list(result.rows.keys())
            assert keys == sorted(keys), (
                f"Fundamental rows not sorted for {ric}"
            )

    def test_income_statement_populated(self, initialized_provider):
        """Verify income statement sub-entities are present with EBIT data."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            has_ebit = False
            for row in result.rows.values():
                if row is not None and row.income_statement is not None:
                    assert isinstance(
                        row.income_statement,
                        FundamentalDataRowIncomeStatement,
                    )
                    if row.income_statement.earnings_before_interest_and_tax is not None:
                        has_ebit = True
            assert has_ebit, f"No EBIT data found for {ric}"

    def test_balance_sheet_populated(self, initialized_provider):
        """Verify balance sheet sub-entities are present with total assets data."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            has_assets = False
            for row in result.rows.values():
                if row is not None and row.balance_sheet is not None:
                    assert isinstance(
                        row.balance_sheet,
                        FundamentalDataRowBalanceSheet,
                    )
                    if row.balance_sheet.assets is not None:
                        has_assets = True
            assert has_assets, f"No total assets data found for {ric}"

    def test_cash_flow_populated(self, initialized_provider):
        """Verify cash flow sub-entities are present with operating cash flow data."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            has_cf = False
            for row in result.rows.values():
                if row is not None and row.cash_flow is not None:
                    assert isinstance(
                        row.cash_flow,
                        FundamentalDataRowCashFlow,
                    )
                    if row.cash_flow.net_cash_from_operating_activities is not None:
                        has_cf = True
            assert has_cf, f"No operating cash flow data found for {ric}"

    def test_reported_currency_is_usd(self, initialized_provider):
        """Verify reported currency is USD (as returned by the mocked currency lookup)."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for row in result.rows.values():
                if row is not None:
                    assert row.reported_currency == "USD"

    def test_fundamental_cache_endpoint_present(self, initialized_provider):
        """Verify the FUNDAMENTAL_DATA endpoint key exists in the cache."""
        for ric in FIXTURE_RICS:
            assert LsegWorkspace.Endpoints.FUNDAMENTAL_DATA in (
                initialized_provider.cache[ric]
            )

    def test_multi_ticker_fundamental_data_differs(self, initialized_provider):
        """Verify different tickers produce distinct fundamental data."""
        results = {}
        for ric in FIXTURE_RICS:
            results[ric] = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
        ric_a, ric_b = FIXTURE_RICS
        assert results[ric_a].rows.keys() != results[ric_b].rows.keys(), (
            "Expected different filing dates for different tickers"
        )

    def test_multiple_quarters_per_ric(self, initialized_provider):
        """Verify each RIC has data spanning multiple distinct quarters."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            periods = {
                row.fiscal_period
                for row in result.rows.values()
                if row is not None
            }
            assert len(periods) >= 2, (
                f"Expected multiple quarters for {ric}, found {periods}"
            )


# ===========================================================================
# Dividend Data Integration Tests
# ===========================================================================


class TestDividendDataIntegration:
    """End-to-end integration tests for dividend data retrieval."""

    def test_returns_dividend_data_entity(self, initialized_provider):
        """Verify get_dividend_data returns a DividendData entity."""
        result = initialized_provider.get_dividend_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(result, DividendData)

    def test_main_identifier_matches_request(self, initialized_provider):
        """Verify the main_identifier field matches the requested RIC."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert result.main_identifier == MarketInstrumentIdentifier(ric)

    def test_all_rics_produce_non_empty_results(self, initialized_provider):
        """Verify every fixture RIC yields non-empty dividend data rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert len(result.rows) > 0, f"No dividend rows for {ric}"

    def test_rows_contain_dividend_data_row_entities(self, initialized_provider):
        """Verify every dividend row is a DividendDataRow instance."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                assert isinstance(row, DividendDataRow), (
                    f"Expected DividendDataRow for {ric} on {key}"
                )

    def test_ex_dividend_dates_populated(self, initialized_provider):
        """Verify the ex-dividend date is not None on every row."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                assert row.ex_dividend_date is not None, (
                    f"ex_dividend_date is None for {ric} on {key}"
                )

    def test_gross_dividend_amount_positive(self, initialized_provider):
        """Verify gross dividend amounts are positive where present."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                if row.dividend is not None:
                    assert row.dividend > 0, (
                        f"Non-positive dividend {row.dividend} for {ric} on {key}"
                    )

    def test_adjusted_gross_dividend_populated(self, initialized_provider):
        """Verify at least some rows have an adjusted gross dividend amount."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            has_adjusted = any(
                row.dividend_split_adjusted is not None
                for row in result.rows.values()
            )
            assert has_adjusted, (
                f"No adjusted dividend amounts for {ric}"
            )

    def test_dividend_rows_sorted_ascending(self, initialized_provider):
        """Verify dividend rows are sorted by ex-dividend date ascending."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            keys = list(result.rows.keys())
            assert keys == sorted(keys), (
                f"Dividend rows not sorted for {ric}"
            )

    def test_dividend_cache_endpoint_present(self, initialized_provider):
        """Verify the STOCK_DIVIDEND endpoint key exists in the cache."""
        for ric in FIXTURE_RICS:
            assert LsegWorkspace.Endpoints.STOCK_DIVIDEND in (
                initialized_provider.cache[ric]
            )

    def test_multi_ticker_dividend_data_differs(self, initialized_provider):
        """Verify different tickers have different dividend histories."""
        results = {}
        for ric in FIXTURE_RICS:
            results[ric] = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
        ric_a, ric_b = FIXTURE_RICS
        assert results[ric_a].rows.keys() != results[ric_b].rows.keys(), (
            "Expected different ex-dividend dates for different tickers"
        )


# ===========================================================================
# Split Data Integration Tests
# ===========================================================================


class TestSplitDataIntegration:
    """End-to-end integration tests for stock split data retrieval."""

    def test_returns_split_data_entity(self, initialized_provider):
        """Verify get_split_data returns a SplitData entity."""
        result = initialized_provider.get_split_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(result, SplitData)

    def test_main_identifier_matches_request(self, initialized_provider):
        """Verify the main_identifier field matches the requested RIC."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert result.main_identifier == MarketInstrumentIdentifier(ric)

    def test_all_rics_produce_non_empty_results(self, initialized_provider):
        """Verify every fixture RIC yields non-empty split data rows."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert len(result.rows) > 0, f"No split rows for {ric}"

    def test_rows_contain_split_data_row_entities(self, initialized_provider):
        """Verify every split row is a SplitDataRow instance."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                assert isinstance(row, SplitDataRow), (
                    f"Expected SplitDataRow for {ric} on {key}"
                )

    def test_split_numerator_and_denominator_positive(self, initialized_provider):
        """Verify split numerator and denominator are positive."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.rows.items():
                assert row.numerator > 0, (
                    f"Non-positive numerator {row.numerator} for {ric} on {key}"
                )
                assert row.denominator > 0, (
                    f"Non-positive denominator {row.denominator} for {ric} on {key}"
                )

    def test_known_apple_four_for_one_split(self, initialized_provider):
        """Verify the known AAPL 4:1 stock split on 2020-08-31 is present."""
        result = initialized_provider.get_split_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert "2020-08-31" in result.rows, (
            "Expected AAPL 4:1 split on 2020-08-31"
        )
        row = result.rows["2020-08-31"]
        assert row.numerator == 4
        assert row.denominator == 1

    def test_known_nvidia_four_for_one_split(self, initialized_provider):
        """Verify the known NVDA 4:1 stock split on 2021-07-20 is present."""
        result = initialized_provider.get_split_data(
            main_identifier="NVDA.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert "2021-07-20" in result.rows, (
            "Expected NVDA 4:1 split on 2021-07-20"
        )
        row = result.rows["2021-07-20"]
        assert row.numerator == 4
        assert row.denominator == 1

    def test_known_nvidia_ten_for_one_split(self, initialized_provider):
        """Verify the known NVDA 10:1 stock split on 2024-06-07 is present."""
        result = initialized_provider.get_split_data(
            main_identifier="NVDA.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert "2024-06-07" in result.rows, (
            "Expected NVDA 10:1 split on 2024-06-07"
        )
        row = result.rows["2024-06-07"]
        assert row.numerator == 10
        assert row.denominator == 1

    def test_split_rows_sorted_ascending(self, initialized_provider):
        """Verify split rows are sorted by split date ascending."""
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            keys = list(result.rows.keys())
            assert keys == sorted(keys), (
                f"Split rows not sorted for {ric}"
            )

    def test_split_cache_endpoint_present(self, initialized_provider):
        """Verify the STOCK_SPLIT endpoint key exists in the cache."""
        for ric in FIXTURE_RICS:
            assert LsegWorkspace.Endpoints.STOCK_SPLIT in (
                initialized_provider.cache[ric]
            )

    def test_aapl_has_exactly_one_split(self, initialized_provider):
        """Verify AAPL has exactly one split event in the fixture data."""
        result = initialized_provider.get_split_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert len(result.rows) == 1, (
            f"Expected 1 AAPL split, got {len(result.rows)}"
        )

    def test_nvda_has_exactly_two_splits(self, initialized_provider):
        """Verify NVDA has exactly two split events in the fixture data."""
        result = initialized_provider.get_split_data(
            main_identifier="NVDA.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert len(result.rows) == 2, (
            f"Expected 2 NVDA splits, got {len(result.rows)}"
        )


# ===========================================================================
# Cross-Domain Integration Tests
# ===========================================================================


class TestCrossDomainIntegration:
    """Tests that validate interactions and consistency across data types."""

    def test_all_data_types_available_for_all_rics(self, initialized_provider):
        """Verify all four data types can be retrieved for every RIC."""
        for ric in FIXTURE_RICS:
            market = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            fundamental = initialized_provider.get_fundamental_data(
                main_identifier=ric,
                period="quarterly",
                start_date=START_DATE,
                end_date=END_DATE,
            )
            dividend = initialized_provider.get_dividend_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            split = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            assert len(market.daily_rows) > 0
            assert len(fundamental.rows) > 0
            assert len(dividend.rows) > 0
            assert len(split.rows) > 0

    def test_cache_has_all_six_endpoints(self, initialized_provider):
        """Verify each ticker's cache entry contains all six endpoint keys."""
        expected_endpoints = {
            LsegWorkspace.Endpoints.MARKET_DATA_DAILY_UNADJUSTED,
            LsegWorkspace.Endpoints.MARKET_DATA_DAILY_SPLIT_ADJUSTED,
            LsegWorkspace.Endpoints.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED,
            LsegWorkspace.Endpoints.FUNDAMENTAL_DATA,
            LsegWorkspace.Endpoints.STOCK_DIVIDEND,
            LsegWorkspace.Endpoints.STOCK_SPLIT,
        }
        for ric in FIXTURE_RICS:
            cache_entry = initialized_provider.cache[ric]
            for ep in expected_endpoints:
                assert ep in cache_entry, (
                    f"Missing endpoint {ep} in cache[{ric}]"
                )

    def test_split_dates_within_market_data_range(self, initialized_provider):
        """Verify every split date falls within the market data date range."""
        for ric in FIXTURE_RICS:
            market = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            split = initialized_provider.get_split_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )

            market_dates = sorted(market.daily_rows.keys())
            first_market = market_dates[0]
            last_market = market_dates[-1]

            for split_key in split.rows:
                assert first_market <= split_key <= last_market, (
                    f"Split date {split_key} outside market range "
                    f"[{first_market}, {last_market}] for {ric}"
                )

    def test_dividend_adjustment_changes_prices(self, initialized_provider):
        """Verify dividend+split adjusted prices differ from split-only adjusted.

        For AAPL.OQ, which pays quarterly dividends, the dividend adjustment
        should cause the div+split adjusted prices to be lower than (or equal
        to) the split-only adjusted prices for dates before the most recent
        ex-dividend date.
        """
        result = initialized_provider.get_market_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )

        some_differ = False
        for row in result.daily_rows.values():
            if (
                row.close_split_adjusted is not None
                and row.close_dividend_and_split_adjusted is not None
                and row.close_split_adjusted != row.close_dividend_and_split_adjusted
            ):
                some_differ = True
                break

        assert some_differ, (
            "Expected dividend+split adjusted prices to differ from split-only "
            "adjusted for AAPL.OQ (which pays dividends)"
        )

    def test_dividend_adjusted_prices_not_greater_than_split_adjusted(
        self,
        initialized_provider,
    ):
        """Verify dividend+split adjusted close is at most the split-adjusted close.

        Dividend adjustments reduce historical prices, so the dividend+split
        adjusted price should never exceed the split-only adjusted price.
        """
        result = initialized_provider.get_market_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )

        tolerance = decimal.Decimal("0.000001")
        for key, row in result.daily_rows.items():
            split_close = row.close_split_adjusted
            div_split_close = row.close_dividend_and_split_adjusted
            if split_close is not None and div_split_close is not None:
                assert div_split_close <= split_close + tolerance, (
                    f"Div+split close ({div_split_close}) > split close "
                    f"({split_close}) for AAPL.OQ on {key}"
                )

    def test_all_tickers_present_in_cache(self, initialized_provider):
        """Verify every requested ticker has an entry in the provider cache."""
        for ric in FIXTURE_RICS:
            assert ric in initialized_provider.cache, (
                f"Expected {ric} in provider cache"
            )


# ===========================================================================
# Edge Case Tests
# ===========================================================================


class TestEdgeCases:
    """Edge case and boundary condition tests."""

    def test_single_ric_configuration(
        self,
        monkeypatch,
        raw_market_df,
        raw_fundamental_df,
        raw_split_df,
        raw_dividend_df,
    ):
        """Verify initialization and retrieval work with a single-RIC configuration."""
        LsegWorkspace._shared_cache = {}
        LsegWorkspace._shared_cache_config_key = None

        def mock_attempt_fetch(tickers, fields):
            """Route to the correct fixture based on the fields."""
            fields_str = " ".join(fields)
            if "Period=FQ" in fields_str:
                return raw_fundamental_df.copy()
            if "DivExDate" in fields_str:
                return raw_dividend_df.copy()
            if "CAExDate" in fields_str:
                return raw_split_df.copy()
            return raw_market_df.copy()

        config = Configuration(
            start_date=START_DATE,
            end_date=END_DATE,
            period="quarterly",
            identifiers=("AAPL.OQ",),
            columns=("m_open", "m_close"),
        )

        mock_session = SimpleNamespace(open_state="OpenState.Opened")

        monkeypatch.setattr(LsegWorkspace, "_attempt_fetch", staticmethod(mock_attempt_fetch))
        monkeypatch.setattr(
            LsegWorkspace,
            "_fetch_currency_data",
            staticmethod(lambda t: dict.fromkeys(t, "USD")),
        )
        monkeypatch.setattr("refinitiv.data.open_session", lambda: None)
        monkeypatch.setattr("refinitiv.data.session.get_default", lambda: mock_session)

        provider = LsegWorkspace(api_key=None)
        provider.initialize(configuration=config)

        market = provider.get_market_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(market, MarketData)
        assert len(market.daily_rows) > 0

        fundamental = provider.get_fundamental_data(
            main_identifier="AAPL.OQ",
            period="quarterly",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(fundamental, FundamentalData)
        assert len(fundamental.rows) > 0

        dividend = provider.get_dividend_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(dividend, DividendData)
        assert len(dividend.rows) > 0

        split = provider.get_split_data(
            main_identifier="AAPL.OQ",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        assert isinstance(split, SplitData)
        assert len(split.rows) > 0

    def test_split_adjusted_vwap_within_split_adjusted_range(self, initialized_provider):
        """Verify split-adjusted VWAP falls between the split-adjusted low and high.

        VWAP in the LSEG fixture is already split-adjusted, so it must be
        compared against split-adjusted high/low rather than unadjusted prices.
        """
        for ric in FIXTURE_RICS:
            result = initialized_provider.get_market_data(
                main_identifier=ric,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for key, row in result.daily_rows.items():
                if (
                    row.vwap_split_adjusted is not None
                    and row.low_split_adjusted is not None
                    and row.high_split_adjusted is not None
                ):
                    assert row.low_split_adjusted <= row.vwap_split_adjusted <= row.high_split_adjusted, (
                        f"Split-adj VWAP ({row.vwap_split_adjusted}) outside "
                        f"[{row.low_split_adjusted}, {row.high_split_adjusted}] "
                        f"for {ric} on {key}"
                    )
