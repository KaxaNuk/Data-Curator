"""
Unit tests for the Intrinio provider's per-ticker split request cache.

Both `get_dividend_data` (which split-adjusts each dividend by the splits that
followed its ex-date) and `get_split_data` need the same security split
adjustments. Requesting them twice doubles the split traffic for every ticker in
the universe, which is pure waste on a large request, so the provider resolves
them once per ticker and date range and reuses the result.

The cache must stay keyed by both the identifier and the date range: reusing one
ticker's splits for another, or a narrow range's splits for a wider one, would
silently corrupt both the split rows and the split-adjusted dividends.
"""

import datetime

from kaxanuk.data_curator.data_providers.intrinio import Intrinio


START_DATE = datetime.date(2020, 1, 1)
END_DATE = datetime.date(2024, 12, 31)


def _provider_recording_split_requests() -> tuple[Intrinio, list[tuple[str, datetime.date]]]:
    """Build a provider whose split and dividend requests are stubbed, recording each split call."""
    provider = Intrinio(api_key='12345')
    requested: list[tuple[str, datetime.date]] = []

    def _record_split_request(
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list:
        requested.append((main_identifier, start_date))

        return []

    provider._request_splits = _record_split_request
    provider._request_dividends = lambda **_kwargs: []

    return (provider, requested)


class TestSplitRequestCache:
    def test_requests_splits_once_for_dividends_and_splits(self) -> None:
        (provider, requested) = _provider_recording_split_requests()

        provider.get_dividend_data(
            main_identifier='XXXX',
            start_date=START_DATE,
            end_date=END_DATE,
        )
        provider.get_split_data(
            main_identifier='XXXX',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert requested == [('XXXX', START_DATE)]

    def test_requests_splits_separately_for_each_identifier(self) -> None:
        (provider, requested) = _provider_recording_split_requests()

        provider.get_split_data(
            main_identifier='AAAA',
            start_date=START_DATE,
            end_date=END_DATE,
        )
        provider.get_split_data(
            main_identifier='BBBB',
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert requested == [('AAAA', START_DATE), ('BBBB', START_DATE)]

    def test_refetches_when_the_date_range_changes(self) -> None:
        (provider, requested) = _provider_recording_split_requests()
        earlier_start = datetime.date(2010, 1, 1)

        provider.get_split_data(
            main_identifier='XXXX',
            start_date=START_DATE,
            end_date=END_DATE,
        )
        provider.get_split_data(
            main_identifier='XXXX',
            start_date=earlier_start,
            end_date=END_DATE,
        )

        assert requested == [('XXXX', START_DATE), ('XXXX', earlier_start)]
