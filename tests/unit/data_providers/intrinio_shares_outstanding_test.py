"""
Unit tests for the Intrinio provider's derived point-in-time shares outstanding.

Intrinio serves no daily share count, and its reported ones are unusable for a
market capitalization: they are weighted averages over a fiscal period, and
Intrinio restates them for later splits on annual periods while leaving
quarterly ones as-reported, so consecutive periods differ by whole split
factors.

Its daily `marketcap` series is built on the dividend-and-split-adjusted close
instead, so dividing the two cancels the adjustment and leaves the share count
on the same split basis as the curator's `*_split_adjusted` prices. These tests
pin that relationship, and pin that the field degrades to null rather than
failing where Intrinio's market cap coverage is shallower than its prices,
absent entirely, or refused.
"""

import datetime
import types

import intrinio_sdk.rest
import pyarrow
import pytest

from kaxanuk.data_curator.data_providers.intrinio import Intrinio


def _price_record(
    *,
    date: datetime.date,
    close: float,
    adj_close: float,
    split_ratio: float = 1.0,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        date=date,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1000,
        adj_open=adj_close,
        adj_high=adj_close,
        adj_low=adj_close,
        adj_close=adj_close,
        adj_volume=1000,
        split_ratio=split_ratio,
    )


def _shares_by_date(table) -> dict:
    rows = table.to_pylist()

    return {row['date']: row['derived_shares_outstanding'] for row in rows}


class TestDeriveSharesOutstanding:
    def test_divides_the_market_cap_by_the_fully_adjusted_close(self) -> None:
        shares = Intrinio._derive_shares_outstanding(
            market_cap=2_000_000.0,
            close_dividend_and_split_adjusted=50.0,
        )

        assert shares == 40_000.0

    @pytest.mark.parametrize(
        ('market_cap', 'adjusted_close'),
        [
            (None, 50.0),
            (2_000_000.0, None),
            # a zero adjusted close would otherwise raise instead of degrading to null
            (2_000_000.0, 0.0),
        ],
    )
    def test_returns_null_on_missing_or_zero_inputs(
        self,
        market_cap: float | None,
        adjusted_close: float | None,
    ) -> None:
        assert Intrinio._derive_shares_outstanding(
            market_cap=market_cap,
            close_dividend_and_split_adjusted=adjusted_close,
        ) is None


class TestSharesOutstandingAcrossSplits:
    def test_shares_and_market_cap_pass_through_a_split_without_a_jump(self) -> None:
        # a 4:1 split on the third day: the raw close quarters, and Intrinio's adjusted close
        # restates the earlier days onto the post-split basis
        records = [
            _price_record(date=datetime.date(2024, 1, 1), close=400.0, adj_close=100.0),
            _price_record(date=datetime.date(2024, 1, 2), close=404.0, adj_close=101.0),
            _price_record(date=datetime.date(2024, 1, 3), close=102.0, adj_close=102.0,
                          split_ratio=0.25),
        ]
        # the market cap is continuous across the split, as the real series is
        market_caps = {
            datetime.date(2024, 1, 1): 100.0 * 8_000_000.0,
            datetime.date(2024, 1, 2): 101.0 * 8_000_000.0,
            datetime.date(2024, 1, 3): 102.0 * 8_000_000.0,
        }

        table = Intrinio._build_stock_price_endpoint_table(
            records=records,
            market_caps=market_caps,
        )
        rows = {row['date']: row for row in table.to_pylist()}

        # the derived share count does not move on the split date
        assert [rows[date]['derived_shares_outstanding'] for date in sorted(rows)] == [
            8_000_000.0,
            8_000_000.0,
            8_000_000.0,
        ]
        # and neither does the market cap it implies, beyond the day's actual price move
        caps = [
            rows[date]['derived_shares_outstanding'] * rows[date]['split_adjusted_close']
            for date in sorted(rows)
        ]
        assert caps == [800_000_000.0, 808_000_000.0, 816_000_000.0]


class TestSharesOutstandingCoverage:
    def test_leaves_dates_the_market_cap_series_misses_null(self) -> None:
        records = [
            _price_record(date=datetime.date(2024, 1, 1), close=10.0, adj_close=10.0),
            _price_record(date=datetime.date(2024, 1, 2), close=20.0, adj_close=20.0),
        ]
        # Intrinio's market cap history is shallower than its price history, so the earlier
        # date is uncovered
        market_caps = {datetime.date(2024, 1, 2): 400.0}

        shares = _shares_by_date(Intrinio._build_stock_price_endpoint_table(
            records=records,
            market_caps=market_caps,
        ))

        assert shares == {
            datetime.date(2024, 1, 1): None,
            datetime.date(2024, 1, 2): 20.0,
        }

    @pytest.mark.parametrize('market_caps', [None, {}])
    def test_leaves_every_date_null_without_any_market_caps(self, market_caps: dict | None) -> None:
        records = [
            _price_record(date=datetime.date(2024, 1, 1), close=10.0, adj_close=10.0),
            _price_record(date=datetime.date(2024, 1, 2), close=20.0, adj_close=20.0),
        ]

        shares = _shares_by_date(Intrinio._build_stock_price_endpoint_table(
            records=records,
            market_caps=market_caps,
        ))

        assert set(shares.values()) == {None}

    def test_types_an_entirely_empty_shares_column_rather_than_leaving_it_null_typed(self) -> None:
        """
        An all-null column must still carry its real type.

        Intrinio covers no market capitalization at all for some securities. PyArrow would infer
        the resulting column as null-typed, which the toolkit rejects as an all-null endpoint
        column, and the security would lose its prices along with its share count.
        """
        table = Intrinio._build_stock_price_endpoint_table(
            records=[_price_record(date=datetime.date(2024, 1, 1), close=10.0, adj_close=10.0)],
            market_caps={},
        )
        shares_type = table.schema.field(
            table.column_names.index('derived_shares_outstanding')
        ).type

        assert shares_type == pyarrow.float64()


class TestRejectSharesOutstandingScaleBreaks:
    def test_drops_the_dates_whose_scale_breaks_with_the_series(self) -> None:
        # Intrinio reports some stretches in millions: a share count a millionth of its
        # neighbours' is the vendor's scale breaking, not the company's issuance
        series = [1_000_000_000.0] * 30
        series[10] = 1_000.0
        series[11] = 1_050.0

        rejected = Intrinio._reject_shares_outstanding_scale_breaks(series)

        assert rejected[10] is None
        assert rejected[11] is None
        assert rejected[:10] == [1_000_000_000.0] * 10
        assert rejected[12:] == [1_000_000_000.0] * 18

    def test_keeps_ordinary_issuance_and_buybacks(self) -> None:
        # a real share count drifts; nothing here is orders of magnitude out
        series = [1_000_000_000.0 * (1 + index / 50) for index in range(30)]

        assert Intrinio._reject_shares_outstanding_scale_breaks(series) == series

    def test_leaves_a_series_too_short_to_judge_untouched(self) -> None:
        series = [1_000_000_000.0, 1_000.0, None]

        assert Intrinio._reject_shares_outstanding_scale_breaks(series) == series


class TestRequestMarketCaps:
    def _provider_with_company_api(self, company_api) -> Intrinio:
        provider = Intrinio(api_key='12345')
        provider._intrinio_sdk = types.SimpleNamespace(CompanyApi=lambda: company_api)

        return provider

    def test_accumulates_every_page_into_a_lookup_by_date(self) -> None:
        pages = [
            types.SimpleNamespace(
                historical_data=[
                    types.SimpleNamespace(date=datetime.date(2024, 1, 2), value=200.0),
                ],
                next_page='page2',
            ),
            types.SimpleNamespace(
                historical_data=[
                    types.SimpleNamespace(date=datetime.date(2024, 1, 1), value=100.0),
                ],
                next_page=None,
            ),
        ]
        requested_tags = []

        class _CompanyApi:
            def get_company_historical_data(self, identifier, tag, **kwargs):
                requested_tags.append(tag)

                return pages[len(requested_tags) - 1]

        provider = self._provider_with_company_api(_CompanyApi())
        market_caps = provider._request_market_caps(
            main_identifier='AAAA',
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 2),
        )

        assert market_caps == {
            datetime.date(2024, 1, 1): 100.0,
            datetime.date(2024, 1, 2): 200.0,
        }
        assert requested_tags == ['marketcap', 'marketcap']

    def test_skips_records_missing_a_date_or_a_value(self) -> None:
        class _CompanyApi:
            def get_company_historical_data(self, identifier, tag, **kwargs):
                return types.SimpleNamespace(
                    historical_data=[
                        types.SimpleNamespace(date=datetime.date(2024, 1, 1), value=None),
                        types.SimpleNamespace(date=None, value=100.0),
                        types.SimpleNamespace(date=datetime.date(2024, 1, 2), value=200.0),
                    ],
                    next_page=None,
                )

        provider = self._provider_with_company_api(_CompanyApi())

        assert provider._request_market_caps(
            main_identifier='AAAA',
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 2),
        ) == {datetime.date(2024, 1, 2): 200.0}

    @pytest.mark.parametrize('status', [403, 404, 500])
    def test_degrades_to_an_empty_lookup_on_any_api_error(self, status: int) -> None:
        """A market cap failure must never cost the identifier its prices."""
        class _CompanyApi:
            def get_company_historical_data(self, identifier, tag, **kwargs):
                raise intrinio_sdk.rest.ApiException(status=status, reason='Nope')

        provider = self._provider_with_company_api(_CompanyApi())

        assert provider._request_market_caps(
            main_identifier='AAAA',
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 2),
        ) == {}


class TestRejectSharesOutstandingSpikes:
    def test_drops_a_date_that_breaks_with_its_neighbours_and_reverts(self) -> None:
        # Alphabet's 2022-07-26 sits at a twentieth of the days around it, a week after its
        # 20-for-1 split, with the series back to normal the next day
        series = [1.317e10] * 12 + [6.585e8] + [1.3044e10] * 12

        rejected = Intrinio._reject_shares_outstanding_spikes(series)

        assert rejected[12] is None
        assert rejected[:12] == series[:12]
        assert rejected[13:] == series[13:]

    def test_keeps_a_real_step_however_large(self) -> None:
        """The two sides disagree across a real step, which is what tells it from a spike."""
        series = [1.0e9] * 12 + [2.5e9] * 12

        assert Intrinio._reject_shares_outstanding_spikes(series) == series

    def test_keeps_an_ordinary_drift(self) -> None:
        series = [1.0e9 - index * 1.0e7 for index in range(24)]

        assert Intrinio._reject_shares_outstanding_spikes(series) == series

    def test_leaves_a_series_too_short_to_judge_untouched(self) -> None:
        series = [1.0e9, 5.0e7, 1.0e9]

        assert Intrinio._reject_shares_outstanding_spikes(series) == series


class TestEmptyPriceHistory:
    def test_builds_an_empty_table_for_a_security_with_no_prices(self) -> None:
        """
        A security Intrinio serves no prices for must not crash the run.

        An empty record list yields a table with no columns at all, so there is no shares column
        to type. The caller rejects the security for having no data, as it did before the shares
        column existed.
        """
        table = Intrinio._build_stock_price_endpoint_table(
            records=[],
            market_caps={},
        )

        assert table.num_rows == 0

    def test_builds_an_empty_table_when_market_caps_are_omitted_too(self) -> None:
        assert Intrinio._build_stock_price_endpoint_table(records=[]).num_rows == 0
