"""
Unit tests for the typing of the Intrinio stock price columns the provider serves empty.

Intrinio leaves whole numeric series empty for some securities: no traded volume
for non-exchange-traded funds, no market capitalization (so no derived share
count) for others. PyArrow infers those columns as null-typed, and the toolkit
rejects a null-typed endpoint column as an all-null one, which would cost the
security its prices along with the missing series. These tests pin that every
numeric price column carries its real type even when it holds nothing but nulls.
"""

import datetime
import types

import pyarrow

from kaxanuk.data_curator.data_providers.intrinio import Intrinio


def _price_record(
    *,
    date: datetime.date,
    close: float,
    volume: float | None = 1000,
    adj_volume: float | None = 1000,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        date=date,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        adj_open=close,
        adj_high=close,
        adj_low=close,
        adj_close=close,
        adj_volume=adj_volume,
        split_ratio=1.0,
    )


class TestEmptyStockPriceColumnTypes:
    def test_types_the_volume_columns_of_a_security_that_reports_no_volume(self) -> None:
        table = Intrinio._build_stock_price_endpoint_table(
            records=[
                _price_record(
                    date=datetime.date(2024, 1, day),
                    close=10.0,
                    volume=None,
                    adj_volume=None,
                )
                for day in (2, 3)
            ],
            market_caps={
                datetime.date(2024, 1, 2): 1_000.0,
                datetime.date(2024, 1, 3): 1_000.0,
            },
        )

        assert table.schema.field('volume').type == pyarrow.float64()
        assert table.schema.field('adj_volume').type == pyarrow.float64()

    def test_leaves_no_null_typed_column_when_the_security_reports_neither_volume_nor_market_cap(
        self
    ) -> None:
        table = Intrinio._build_stock_price_endpoint_table(
            records=[
                _price_record(
                    date=datetime.date(2024, 1, day),
                    close=10.0,
                    volume=None,
                    adj_volume=None,
                )
                for day in (2, 3)
            ],
            market_caps={},
        )

        null_typed_columns = [
            field.name
            for field in table.schema
            if field.type == pyarrow.null()
        ]

        assert null_typed_columns == []

    def test_keeps_the_date_column_a_date(self) -> None:
        table = Intrinio._build_stock_price_endpoint_table(
            records=[_price_record(date=datetime.date(2024, 1, 2), close=10.0)],
            market_caps={},
        )

        assert table.schema.field('date').type == pyarrow.date32()

    def test_keeps_the_populated_columns_untouched(self) -> None:
        records = [
            _price_record(date=datetime.date(2024, 1, 2), close=10.0, volume=None, adj_volume=None),
            _price_record(date=datetime.date(2024, 1, 3), close=11.0, volume=None, adj_volume=None),
        ]

        table = Intrinio._build_stock_price_endpoint_table(records=records, market_caps={})
        rows = {row['date']: row for row in table.to_pylist()}

        assert rows[datetime.date(2024, 1, 2)]['close'] == 10.0
        assert rows[datetime.date(2024, 1, 3)]['close'] == 11.0
        assert rows[datetime.date(2024, 1, 2)]['volume'] is None
