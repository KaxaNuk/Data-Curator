"""
Unit tests for the Intrinio provider's split-only market-price adjustment.

Intrinio's stock price endpoint serves raw and fully (split-and-dividend)
adjusted prices, but not the split-only adjusted prices the curator's contract
also defines. The provider derives them by scaling each raw price by the row's
cumulative split factor, accumulated from the exact per-row split ratios. These
tests pin that accumulation and scaling on small synthetic series, including a
compounded multi-split case.
"""

import datetime
import types

from kaxanuk.data_curator.data_providers.intrinio import Intrinio


def _price_record(
    date: datetime.date,
    *,
    close: float,
    split_ratio: float = 1.0,
    volume: int = 0,
    adj_volume: int = 0,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
) -> types.SimpleNamespace:
    """Build a minimal stock-price record exposing the attributes the builder reads."""
    return types.SimpleNamespace(
        date=date,
        open=close if open_ is None else open_,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
        volume=volume,
        adj_open=None,
        adj_high=None,
        adj_low=None,
        adj_close=None,
        adj_volume=adj_volume,
        split_ratio=split_ratio,
    )


class TestResolveCumulativeSplitFactors:
    def test_accumulates_split_ratio_over_earlier_dates(self) -> None:
        # A 10-for-1 split takes effect on the later date, so every earlier date is scaled by 0.1
        # and the split date itself (and anything after it) stays at 1.0.
        records = [
            _price_record(datetime.date(2020, 1, 1), close=100.0),
            _price_record(datetime.date(2020, 6, 1), close=10.0, split_ratio=0.1),
        ]

        factors = Intrinio._resolve_cumulative_split_factors(records)

        assert factors == [0.1, 1.0]

    def test_no_splits_yields_unit_factors(self) -> None:
        records = [
            _price_record(datetime.date(2020, 1, 1), close=100.0),
            _price_record(datetime.date(2020, 6, 1), close=110.0),
        ]

        factors = Intrinio._resolve_cumulative_split_factors(records)

        assert factors == [1.0, 1.0]

    def test_compounds_multiple_splits(self) -> None:
        # Two later splits (0.5 then 0.1) compound for the earliest date: 0.5 * 0.1 = 0.05.
        records = [
            _price_record(datetime.date(2019, 1, 1), close=200.0),
            _price_record(datetime.date(2020, 1, 1), close=100.0, split_ratio=0.5),
            _price_record(datetime.date(2021, 1, 1), close=10.0, split_ratio=0.1),
        ]

        factors = Intrinio._resolve_cumulative_split_factors(records)

        assert factors == [0.05, 0.1, 1.0]


class TestBuildStockPriceEndpointTable:
    def test_scales_prices_by_the_cumulative_split_factor(self) -> None:
        records = [
            _price_record(
                datetime.date(2020, 1, 1),
                close=100.0, open_=98.0, high=104.0, low=96.0,
            ),
            _price_record(
                datetime.date(2020, 6, 1),
                close=12.0, open_=11.0, high=13.0, low=10.0, split_ratio=0.1,
            ),
        ]

        table = Intrinio._build_stock_price_endpoint_table(records=records)
        columns = table.to_pydict()

        # rows are emitted newest-first: post-split row unchanged (factor 1.0), pre-split scaled by 0.1
        assert columns['split_adjusted_close'] == [12.0, 10.0]
        assert columns['split_adjusted_open'] == [11.0, 9.8]
        assert columns['split_adjusted_high'] == [13.0, 10.4]
        assert columns['split_adjusted_low'] == [10.0, 9.6]

    def test_orders_rows_descending_by_date(self) -> None:
        records = [
            _price_record(datetime.date(2020, 6, 1), close=12.0, split_ratio=0.1),
            _price_record(datetime.date(2020, 1, 1), close=100.0),
        ]

        table = Intrinio._build_stock_price_endpoint_table(records=records)
        columns = table.to_pydict()

        assert columns['date'] == [datetime.date(2020, 6, 1), datetime.date(2020, 1, 1)]
        assert columns['split_adjusted_close'] == [12.0, 10.0]

    def test_preserves_nulls_in_split_adjusted_prices(self) -> None:
        record = _price_record(datetime.date(2020, 6, 1), close=10.0)
        record.high = None

        table = Intrinio._build_stock_price_endpoint_table(records=[record])
        columns = table.to_pydict()

        assert columns['split_adjusted_high'] == [None]


def _dividend_record(date: datetime.date, dividend: float | None) -> types.SimpleNamespace:
    return types.SimpleNamespace(date=date, dividend=dividend)


def _split_record(date: datetime.date, split_ratio: float) -> types.SimpleNamespace:
    return types.SimpleNamespace(date=date, split_ratio=split_ratio)


class TestBuildDividendEndpointTable:
    def test_split_adjusts_dividends_by_later_splits_only(self) -> None:
        dividends = [
            _dividend_record(datetime.date(2020, 1, 1), 0.21),
            _dividend_record(datetime.date(2023, 1, 1), 0.24),
        ]
        splits = [
            _split_record(datetime.date(2022, 4, 14), 0.5),   # later than the 2020 dividend only
            _split_record(datetime.date(2019, 1, 31), 0.1),   # earlier than both dividends: ignored
        ]

        table = Intrinio._build_dividend_endpoint_table(dividend_records=dividends, split_records=splits)
        columns = table.to_pydict()

        # 2020 dividend scaled by the later 0.5 split; 2023 dividend has no later split
        assert columns['derived_dividend_split_adjusted'] == [0.105, 0.24]
        assert columns['dividend'] == [0.21, 0.24]

    def test_preserves_record_order(self) -> None:
        dividends = [
            _dividend_record(datetime.date(2023, 1, 1), 0.24),
            _dividend_record(datetime.date(2020, 1, 1), 0.21),
        ]

        table = Intrinio._build_dividend_endpoint_table(dividend_records=dividends, split_records=[])
        columns = table.to_pydict()

        assert columns['date'] == [datetime.date(2023, 1, 1), datetime.date(2020, 1, 1)]

    def test_sums_dividends_sharing_an_ex_date(self) -> None:
        # an ADR paying a regular plus a special dividend on the same ex-date
        dividends = [
            _dividend_record(datetime.date(2024, 6, 13), 1.0),
            _dividend_record(datetime.date(2024, 6, 13), 0.66),
            _dividend_record(datetime.date(2023, 12, 20), 1.0),
        ]

        table = Intrinio._build_dividend_endpoint_table(dividend_records=dividends, split_records=[])
        columns = table.to_pydict()

        # the two 2024-06-13 dividends are summed into one total, keeping ex-dates unique
        assert columns['date'] == [datetime.date(2024, 6, 13), datetime.date(2023, 12, 20)]
        assert columns['dividend'] == [1.66, 1.0]

    def test_drops_dividend_without_amount(self) -> None:
        dividends = [_dividend_record(datetime.date(2020, 1, 1), None)]

        table = Intrinio._build_dividend_endpoint_table(dividend_records=dividends, split_records=[])

        assert table.num_rows == 0
