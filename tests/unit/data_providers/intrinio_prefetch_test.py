"""
Unit tests for the Intrinio provider's batched read-ahead of per-ticker data.

Intrinio serves prices, dividends and splits one ticker at a time, and the
curator walks the universe sequentially, so a large request spends almost all of
its wall clock waiting on serial HTTP round trips. `initialize` records the
universe and its date range, which lets the provider fetch a batch of upcoming
tickers concurrently on the first request that misses the cache; the curator's
sequential loop is then served from memory.

Batching, rather than prefetching the whole universe at once, keeps memory
bounded: a deep date range across thousands of tickers would not fit otherwise.

A prefetch is an optimization and must never lose or corrupt data, so these tests
also pin the fallbacks: without a known universe the provider still resolves a
single ticker, and one ticker failing inside a batch must not swallow that
failure nor damage its neighbours.
"""

import datetime
import types

import pytest

from kaxanuk.data_curator.data_providers.intrinio import Intrinio
from kaxanuk.data_curator.entities import Configuration


START_DATE = datetime.date(2020, 1, 1)
END_DATE = datetime.date(2024, 12, 31)


def _configuration(identifiers: tuple[str, ...]) -> Configuration:
    return Configuration(
        start_date=START_DATE,
        end_date=END_DATE,
        period='quarterly',
        identifiers=identifiers,
        columns=('m_close',),
    )


def _price_record() -> types.SimpleNamespace:
    """Build one minimal stock price record, enough for the market table to assemble."""
    return types.SimpleNamespace(
        date=datetime.date(2024, 1, 2),
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        volume=1000,
        adj_open=10.0,
        adj_high=11.0,
        adj_low=9.0,
        adj_close=10.5,
        adj_volume=1000,
        split_ratio=1.0,
    )


def _provider_recording_price_requests(
    *,
    failing_identifier: str | None = None,
) -> tuple[Intrinio, list[str]]:
    """Build a provider whose stock price requests are stubbed, recording each requested ticker."""
    provider = Intrinio(api_key='12345')
    requested: list[str] = []

    def _record_price_request(
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list:
        requested.append(main_identifier)
        if main_identifier == failing_identifier:
            msg = f"{main_identifier} price request failed"

            raise ValueError(msg)

        return [_price_record()]

    provider._request_stock_prices = _record_price_request

    return (provider, requested)


class TestBatchedPrefetch:
    def test_prefetches_the_upcoming_batch_on_a_cache_miss(self) -> None:
        (provider, requested) = _provider_recording_price_requests()
        provider.initialize(configuration=_configuration(('AAAA', 'BBBB', 'CCCC')))

        provider.get_market_data(
            main_identifier='AAAA',
            start_date=START_DATE,
            end_date=END_DATE,
        )
        after_first_ticker = sorted(requested)

        provider.get_market_data(
            main_identifier='BBBB',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        # the first ticker pulls its whole batch concurrently, so the second is already cached
        assert after_first_ticker == ['AAAA', 'BBBB', 'CCCC']
        assert sorted(requested) == ['AAAA', 'BBBB', 'CCCC']

    def test_resolves_a_single_ticker_without_a_known_universe(self) -> None:
        (provider, requested) = _provider_recording_price_requests()

        provider.get_market_data(
            main_identifier='AAAA',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        # no initialize() means no universe to read ahead over, so only the asked ticker is fetched
        assert requested == ['AAAA']

    def test_surfaces_a_failing_ticker_without_losing_its_neighbours(self) -> None:
        (provider, requested) = _provider_recording_price_requests(failing_identifier='BBBB')
        provider.initialize(configuration=_configuration(('AAAA', 'BBBB', 'CCCC')))

        provider.get_market_data(
            main_identifier='AAAA',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        # the healthy neighbours are still served from the batch, without re-requesting them
        provider.get_market_data(
            main_identifier='CCCC',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert requested.count('CCCC') == 1

        # the failing ticker is never cached as empty: it is retried and its error surfaces
        with pytest.raises(ValueError, match='BBBB'):
            provider.get_market_data(
                main_identifier='BBBB',
                start_date=START_DATE,
                end_date=END_DATE,
            )
