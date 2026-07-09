"""
Integration tests for the Intrinio data provider.

These tests replace the provider's `_intrinio_sdk` class variable with the
`intrinio_sdk_mock` fixture module, so that the API-calling classes
(`AccountApi`, `CompanyApi`, `FundamentalsApi`, `SecurityApi`) serve the local
pickle fixtures instead of hitting the live Intrinio service.  Everything else
runs through the production provider code.

The tests exercise the documented public surface of the provider:
    1. Instantiation with an API key.
    2. `validate_api_key`.
    3. `initialize`.
    4. Each `get_*` data method, checking it returns a non-empty instance of
       its expected return entity.
"""

import datetime

import pytest

from kaxanuk.data_curator.data_providers.intrinio import Intrinio
from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    FundamentalData,
    MarketData,
    SplitData,
)
from .fixtures import intrinio_sdk_mock


TEST_API_KEY = '12345'
MAIN_IDENTIFIER = 'AAPL'
PERIOD = 'quarterly'
START_DATE = datetime.date(2019, 1, 1)
END_DATE = datetime.date(2024, 12, 31)


@pytest.fixture(scope='module')
def configuration() -> Configuration:
    """
    Build a Configuration covering the test identifier and date range.
    """
    return Configuration(
        start_date=START_DATE,
        end_date=END_DATE,
        period=PERIOD,
        identifiers=(
            MAIN_IDENTIFIER,
        ),
        columns=(
            'm_close',
        ),
    )


@pytest.fixture(scope='module')
def provider() -> Intrinio:
    """
    Create an Intrinio provider whose SDK is replaced by the mock module.

    The `_intrinio_sdk` class variable is overwritten directly with the mock
    module so that the provider's API-calling classes serve the local pickle
    fixtures instead of hitting the live Intrinio service.
    """
    Intrinio._intrinio_sdk = intrinio_sdk_mock

    return Intrinio(api_key=TEST_API_KEY)


@pytest.fixture(scope='module')
def initialized_provider(
    provider: Intrinio,
    configuration: Configuration,
) -> Intrinio:
    """
    Return the mocked provider after running its `initialize` setup.
    """
    provider.initialize(configuration=configuration)

    return provider


class TestValidateApiKey:
    def test_validate_api_key_completes(
        self,
        provider: Intrinio,
    ) -> None:
        result = provider.validate_api_key()

        assert result is True


class TestInitialize:
    def test_initialize_does_not_raise(
        self,
        provider: Intrinio,
        configuration: Configuration,
    ) -> None:
        result = provider.initialize(configuration=configuration)

        assert result is None


class TestGetMarketData:
    def test_get_market_data_returns_market_data_instance(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_market_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert isinstance(result, MarketData)

    def test_get_market_data_returns_non_empty_rows(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_market_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(result.daily_rows) > 0


class TestGetFundamentalData:
    def test_get_fundamental_data_returns_fundamental_data_instance(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_fundamental_data(
            main_identifier=MAIN_IDENTIFIER,
            period=PERIOD,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert isinstance(result, FundamentalData)

    def test_get_fundamental_data_returns_non_empty_rows(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_fundamental_data(
            main_identifier=MAIN_IDENTIFIER,
            period=PERIOD,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(result.rows) > 0


class TestGetDividendData:
    def test_get_dividend_data_returns_dividend_data_instance(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_dividend_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert isinstance(result, DividendData)

    def test_get_dividend_data_returns_non_empty_rows(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_dividend_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(result.rows) > 0


class TestGetSplitData:
    def test_get_split_data_returns_split_data_instance(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_split_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert isinstance(result, SplitData)

    def test_get_split_data_returns_non_empty_rows(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_split_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        assert len(result.rows) > 0
