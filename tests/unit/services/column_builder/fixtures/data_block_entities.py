"""
Main entities of the built-in data blocks and of the intraday market data block, for the ColumnBuilder tests.
"""

import datetime
import decimal

from kaxanuk.data_curator import ReferencePeriods
from kaxanuk.data_curator.data_blocks.dividends import (
    DividendData,
    DividendDataRow,
)
from kaxanuk.data_curator.data_blocks.market_daily import (
    MarketData,
    MarketDataDailyRow,
)
from kaxanuk.data_curator.entities import (
    ClockSpec,
    MarketInstrumentIdentifier,
)
from .data_blocks import (
    IntradayMarketData,
    IntradayMarketDataRow,
)


# The same day of market data from another source, with a different close
ALTERNATIVE_MARKET_DATA = MarketData(
    start_date=datetime.date(2024, 1, 30),
    end_date=datetime.date(2024, 1, 30),
    main_identifier=MarketInstrumentIdentifier('TEST'),
    clock_spec=ClockSpec(
        reference_period=ReferencePeriods.DAY,
        periods_per_row=1,
    ),
    rows={
        '2024-01-30': MarketDataDailyRow(
            date=datetime.date(2024, 1, 30),
            open=None,
            high=None,
            low=None,
            close=decimal.Decimal('20.5'),
            volume=None,
            vwap=None,
            open_split_adjusted=None,
            high_split_adjusted=None,
            low_split_adjusted=None,
            close_split_adjusted=None,
            volume_split_adjusted=None,
            vwap_split_adjusted=None,
            open_dividend_and_split_adjusted=None,
            high_dividend_and_split_adjusted=None,
            low_dividend_and_split_adjusted=None,
            close_dividend_and_split_adjusted=None,
            volume_dividend_and_split_adjusted=None,
            vwap_dividend_and_split_adjusted=None,
        ),
    },
)
# A single dividend, going ex-dividend on the market data's day and getting paid afterward
DIVIDEND_DATA = DividendData(
    main_identifier=MarketInstrumentIdentifier('TEST'),
    clock_spec=ClockSpec(
        reference_period=ReferencePeriods.DAY,
        periods_per_row=0,
    ),
    rows={
        '2024-01-30': DividendDataRow(
            declaration_date=datetime.date(2024, 1, 2),
            ex_dividend_date=datetime.date(2024, 1, 30),
            record_date=datetime.date(2024, 1, 31),
            payment_date=datetime.date(2024, 2, 15),
            dividend=decimal.Decimal('0.25'),
            dividend_split_adjusted=decimal.Decimal('0.25'),
        ),
    },
)
# Hourly bars in the exchange's local time, around MARKET_DATA's single day, which is DIVIDEND_DATA's ex-dividend date
INTRADAY_MARKET_DATA = IntradayMarketData(
    main_identifier=MarketInstrumentIdentifier('TEST'),
    clock_spec=ClockSpec(
        reference_period=ReferencePeriods.HOUR,
        periods_per_row=1,
    ),
    rows={
        '2024-01-29T15:00:00': IntradayMarketDataRow(
            timestamp=datetime.datetime(2024, 1, 29, 15),
            close=decimal.Decimal('10.1'),
        ),
        '2024-01-30T09:00:00': IntradayMarketDataRow(
            timestamp=datetime.datetime(2024, 1, 30, 9),
            close=decimal.Decimal('10.2'),
        ),
        '2024-01-30T10:00:00': IntradayMarketDataRow(
            timestamp=datetime.datetime(2024, 1, 30, 10),
            close=decimal.Decimal('10.4'),
        ),
        '2024-01-31T09:00:00': IntradayMarketDataRow(
            timestamp=datetime.datetime(2024, 1, 31, 9),
            close=decimal.Decimal('10.6'),
        ),
    },
)
# A single day of market data, with only its close
MARKET_DATA = MarketData(
    start_date=datetime.date(2024, 1, 30),
    end_date=datetime.date(2024, 1, 30),
    main_identifier=MarketInstrumentIdentifier('TEST'),
    clock_spec=ClockSpec(
        reference_period=ReferencePeriods.DAY,
        periods_per_row=1,
    ),
    rows={
        '2024-01-30': MarketDataDailyRow(
            date=datetime.date(2024, 1, 30),
            open=None,
            high=None,
            low=None,
            close=decimal.Decimal('10.5'),
            volume=None,
            vwap=None,
            open_split_adjusted=None,
            high_split_adjusted=None,
            low_split_adjusted=None,
            close_split_adjusted=None,
            volume_split_adjusted=None,
            vwap_split_adjusted=None,
            open_dividend_and_split_adjusted=None,
            high_dividend_and_split_adjusted=None,
            low_dividend_and_split_adjusted=None,
            close_dividend_and_split_adjusted=None,
            volume_dividend_and_split_adjusted=None,
            vwap_dividend_and_split_adjusted=None,
        ),
    },
)
