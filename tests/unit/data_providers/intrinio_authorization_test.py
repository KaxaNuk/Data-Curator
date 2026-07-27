"""
Unit tests for the Intrinio provider's handling of HTTP 403 responses.

Intrinio answers with 403 "You do not have sufficient access to view this data"
for securities the account's subscription does not cover, such as delisted or
OTC tickers. That is a property of the individual security, not of the run, so
each request method raises `DataProviderAuthorizationError`, which the curator
catches per ticker and skips. Left unclassified it would surface as a generic
`ApiEndpointError`, which aborts the whole universe on the first uncovered
ticker.
"""

import datetime
import types

import intrinio_sdk.rest
import pytest

from kaxanuk.data_curator.data_providers.intrinio import Intrinio
from kaxanuk.data_curator.exceptions import DataProviderAuthorizationError


START_DATE = datetime.date(2020, 1, 1)
END_DATE = datetime.date(2024, 12, 31)


class _ForbiddenApi:
    """Stand-in for any Intrinio SDK API class, answering every call with a 403."""

    def __getattr__(self, name: str):
        def _forbidden(*args, **kwargs):
            raise intrinio_sdk.rest.ApiException(
                status=403,
                reason='Forbidden',
            )

        return _forbidden


def _provider_denied_by_subscription() -> Intrinio:
    """Build a provider whose every SDK endpoint answers with a 403."""
    provider = Intrinio(api_key='12345')
    provider._intrinio_sdk = types.SimpleNamespace(
        CompanyApi=_ForbiddenApi,
        FundamentalsApi=_ForbiddenApi,
        SecurityApi=_ForbiddenApi,
    )

    return provider


class TestForbiddenIdentifier:
    @pytest.mark.parametrize(
        'method_name',
        [
            '_request_dividends',
            '_request_splits',
            '_request_stock_prices',
        ],
    )
    def test_dated_endpoints_raise_authorization_error(self, method_name: str) -> None:
        provider = _provider_denied_by_subscription()

        with pytest.raises(DataProviderAuthorizationError) as raised:
            getattr(provider, method_name)(
                main_identifier='AFH',
                start_date=START_DATE,
                end_date=END_DATE,
            )

        assert raised.value.http_code == 403
        assert raised.value.retryable is False
        assert 'AFH' in str(raised.value)

    def test_fundamentals_endpoint_raises_authorization_error(self) -> None:
        provider = _provider_denied_by_subscription()

        with pytest.raises(DataProviderAuthorizationError) as raised:
            provider._request_fundamentals(main_identifier='AFH')

        assert raised.value.http_code == 403

    def test_standardized_financials_endpoint_raises_authorization_error(self) -> None:
        provider = _provider_denied_by_subscription()

        with pytest.raises(DataProviderAuthorizationError) as raised:
            provider._request_standardized_financials(fundamental_id='fun_abc123')

        assert raised.value.http_code == 403
