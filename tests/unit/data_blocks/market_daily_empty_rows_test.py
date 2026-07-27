"""
Unit tests for how the market daily data block handles dates carrying no data.

A date whose every mapped field is null packs as None, which providers do serve: Intrinio returns
empty rows for dates before a security started trading. `MarketData` admits no empty daily row, so
those dates are dropped instead of failing the whole security.

Before this, an empty date cost the security its entire market data, and an empty date that
happened to be the first or last one crashed the run outright.
"""

import datetime
import decimal

import pytest

from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.entities import MarketData, MarketDataDailyRow, MarketInstrumentIdentifier
from kaxanuk.data_curator.exceptions import EntityProcessingError


def _daily_row(day: int) -> MarketDataDailyRow:
    price = decimal.Decimal('10.00')

    return MarketDataDailyRow(
        date=datetime.date(2024, 1, day),
        open=price, high=price, low=price, close=price, volume=100, vwap=price,
        open_split_adjusted=price, high_split_adjusted=price, low_split_adjusted=price,
        close_split_adjusted=price, volume_split_adjusted=100, vwap_split_adjusted=price,
        open_dividend_and_split_adjusted=price, high_dividend_and_split_adjusted=price,
        low_dividend_and_split_adjusted=price, close_dividend_and_split_adjusted=price,
        volume_dividend_and_split_adjusted=100, vwap_dividend_and_split_adjusted=price,
        shares_outstanding=decimal.Decimal('1000'),
    )


def _assemble(rows, monkeypatch):
    monkeypatch.setattr(
        MarketDailyDataBlock,
        'pack_rows_entities_from_consolidated_table',
        classmethod(lambda cls, table: rows),
    )
    monkeypatch.setattr(
        MarketDailyDataBlock,
        'validate_column_sorted_without_duplicates',
        classmethod(lambda cls, column: True),
    )

    return MarketDailyDataBlock.assemble_entities_from_consolidated_table(
        consolidated_table={'MarketDataDailyRow.date': list(rows)},
        common_field_data={
            MarketData: {MarketData.main_identifier: MarketInstrumentIdentifier('AAAA')},
        },
    )


class TestEmptyDailyRows:
    def test_drops_a_leading_empty_date_instead_of_crashing(self, monkeypatch) -> None:
        """Intrinio serves ALO thousands of empty leading dates before it started trading."""
        rows = {
            '2024-01-01': None,
            '2024-01-02': _daily_row(2),
            '2024-01-03': _daily_row(3),
        }

        result = _assemble(rows, monkeypatch)

        assert list(result.daily_rows) == ['2024-01-02', '2024-01-03']
        assert result.start_date == datetime.date(2024, 1, 2)
        assert result.end_date == datetime.date(2024, 1, 3)

    def test_drops_a_trailing_empty_date(self, monkeypatch) -> None:
        rows = {
            '2024-01-01': _daily_row(1),
            '2024-01-02': _daily_row(2),
            '2024-01-03': None,
        }

        result = _assemble(rows, monkeypatch)

        assert list(result.daily_rows) == ['2024-01-01', '2024-01-02']
        assert result.end_date == datetime.date(2024, 1, 2)

    def test_drops_an_interior_empty_date_without_losing_the_security(self, monkeypatch) -> None:
        """One empty date used to cost AIRT its whole market data."""
        rows = {
            '2024-01-01': _daily_row(1),
            '2024-01-02': None,
            '2024-01-03': _daily_row(3),
        }

        result = _assemble(rows, monkeypatch)

        assert list(result.daily_rows) == ['2024-01-01', '2024-01-03']

    def test_keeps_a_series_with_no_empty_dates_untouched(self, monkeypatch) -> None:
        rows = {'2024-01-01': _daily_row(1), '2024-01-02': _daily_row(2)}

        result = _assemble(rows, monkeypatch)

        assert list(result.daily_rows) == ['2024-01-01', '2024-01-02']

    def test_rejects_a_security_whose_every_date_is_empty(self, monkeypatch) -> None:
        rows = {'2024-01-01': None, '2024-01-02': None}

        with pytest.raises(EntityProcessingError):
            _assemble(rows, monkeypatch)
