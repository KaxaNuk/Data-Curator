"""
Integration tests for the Sharadar data provider.

These tests replace the provider's `_sharadar_client` class variable with the `sharadar_client_mock` fixture
module's client, so that every table is served from the local pickle fixtures instead of the live Sharadar API.
Everything else runs through the production provider code.

The tests exercise the public surface of the provider:
    1. `validate_api_key`.
    2. `initialize`.
    3. `get_data_block_data` for each data block, checking it returns a non-empty instance of its entity with
       every mapped field populated.
"""

import datetime

import pytest

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.data_providers.sharadar import Sharadar
from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    FundamentalData,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    MarketData,
    SplitData,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataProviderToolkit
from .fixtures import sharadar_client_mock


TEST_API_KEY = 'test-api-key'
EQUITY_IDENTIFIER = 'KXNB'
FUND_IDENTIFIER = 'KXNF'
PERIOD = 'quarterly'
START_DATE = datetime.date(2022, 1, 1)
END_DATE = datetime.date(2025, 12, 31)


@pytest.fixture(scope='module', autouse=True)
def isolated_toolkit_memoization() -> None:
    """
    Clear the toolkit's memoized column remaps around this module.

    The toolkit memoizes each data block's remaps for the whole process, which holds for a run, where a single
    provider feeds each data block, but not for a test session, where other providers' tests run first.
    """
    memoized_remaps = (
        DataProviderToolkit._data_block_endpoint_column_remaps,
        DataProviderToolkit._data_block_endpoint_field_preprocessors,
    )
    for cache in memoized_remaps:
        cache.clear()

    yield

    for cache in memoized_remaps:
        cache.clear()


@pytest.fixture(scope='module')
def configuration() -> Configuration:
    """
    Build a Configuration covering the test identifiers and date range.
    """
    return Configuration(
        start_date=START_DATE,
        end_date=END_DATE,
        period=PERIOD,
        identifiers=(
            EQUITY_IDENTIFIER,
            FUND_IDENTIFIER,
        ),
        columns=(
            'm_close',
        ),
    )


@pytest.fixture(scope='module')
def provider() -> Sharadar:
    """
    Create a Sharadar provider whose client is replaced by the mock client, restoring it afterwards.
    """
    original_client = Sharadar._sharadar_client
    Sharadar._sharadar_client = sharadar_client_mock.SharadarClient

    yield Sharadar(api_key=TEST_API_KEY)

    Sharadar._sharadar_client = original_client


@pytest.fixture(scope='module')
def initialized_provider(
    provider: Sharadar,
    configuration: Configuration,
) -> Sharadar:
    """
    Return the mocked provider after running its `initialize` setup.
    """
    provider.initialize(configuration=configuration)

    return provider


class TestValidateApiKey:
    def test_validate_api_key_completes(
        self,
        provider: Sharadar,
    ) -> None:
        result = provider.validate_api_key()

        assert result is True


class TestInitialize:
    def test_initialize_does_not_raise(
        self,
        provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = provider.initialize(configuration=configuration)

        assert result is None


class TestGetMarketData:
    @pytest.mark.parametrize('identifier', [EQUITY_IDENTIFIER, FUND_IDENTIFIER])
    def test_get_market_data_returns_market_data_instance(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
        identifier: str,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=identifier,
            data_block=MarketDailyDataBlock,
            configuration=configuration,
        )

        assert isinstance(result, MarketData)

    @pytest.mark.parametrize('identifier', [EQUITY_IDENTIFIER, FUND_IDENTIFIER])
    def test_get_market_data_returns_non_empty_rows(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
        identifier: str,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=identifier,
            data_block=MarketDailyDataBlock,
            configuration=configuration,
        )

        assert len(result.rows) > 0

    @pytest.mark.parametrize('identifier', [EQUITY_IDENTIFIER, FUND_IDENTIFIER])
    def test_get_market_data_every_row_incorporates_every_mapped_tag(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
        identifier: str,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=identifier,
            data_block=MarketDailyDataBlock,
            configuration=configuration,
        )
        endpoint_field_maps = Sharadar.get_data_block_endpoint_tag_map()[MarketDailyDataBlock]
        mapped_fields = [
            field
            for endpoint_fields in endpoint_field_maps.values()
            for field in endpoint_fields
        ]
        unpopulated_row_fields = [
            (row_date, field.__name__)
            for (row_date, row) in result.rows.items()
            for field in mapped_fields
            if getattr(row, field.__name__) is None
        ]

        assert unpopulated_row_fields == []


class TestGetFundamentalData:
    def test_get_fundamental_data_returns_fundamental_data_instance(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=FundamentalsDataBlock,
            configuration=configuration,
        )

        assert isinstance(result, FundamentalData)

    def test_get_fundamental_data_returns_non_empty_rows(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=FundamentalsDataBlock,
            configuration=configuration,
        )

        assert len(result.rows) > 0

    def test_get_fundamental_data_every_mapped_tag_populated_across_rows(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=FundamentalsDataBlock,
            configuration=configuration,
        )
        # Fundamental rows nest their fields under sub-entities, so each field's __objclass__ tells which part
        # of a row holds it, and a single filing rarely reports every line item, so a tag counts as
        # incorporated when any row carries a value for it.
        sub_entity_attribute_by_entity = {
            FundamentalDataRow: None,
            FundamentalDataRowBalanceSheet: 'balance_sheet',
            FundamentalDataRowCashFlow: 'cash_flow',
            FundamentalDataRowIncomeStatement: 'income_statement',
        }
        endpoint_field_maps = Sharadar.get_data_block_endpoint_tag_map()[FundamentalsDataBlock]
        fields = [
            field
            for endpoint_fields in endpoint_field_maps.values()
            for field in endpoint_fields
        ]
        populated_tags = set()
        for row in result.rows.values():
            for field in fields:
                sub_entity_attribute = sub_entity_attribute_by_entity[field.__objclass__]
                row_part = row if sub_entity_attribute is None else getattr(row, sub_entity_attribute)
                if (
                    row_part is not None
                    and getattr(row_part, field.__name__) is not None
                ):
                    populated_tags.add(f"{field.__objclass__.__name__}.{field.__name__}")

        expected_populated_tags = {
            f"{field.__objclass__.__name__}.{field.__name__}"
            for field in fields
        }

        assert populated_tags == expected_populated_tags

    def test_get_fundamental_data_of_a_fund_is_empty(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=FUND_IDENTIFIER,
            data_block=FundamentalsDataBlock,
            configuration=configuration,
        )

        assert len(result.rows) == 0


class TestGetDividendData:
    def test_get_dividend_data_returns_dividend_data_instance(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=DividendsDataBlock,
            configuration=configuration,
        )

        assert isinstance(result, DividendData)

    def test_get_dividend_data_returns_non_empty_rows(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=DividendsDataBlock,
            configuration=configuration,
        )

        assert len(result.rows) > 0

    def test_get_dividend_data_every_row_incorporates_every_mapped_tag(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=DividendsDataBlock,
            configuration=configuration,
        )
        endpoint_field_maps = Sharadar.get_data_block_endpoint_tag_map()[DividendsDataBlock]
        mapped_fields = [
            field
            for endpoint_fields in endpoint_field_maps.values()
            for field in endpoint_fields
        ]
        unpopulated_row_fields = [
            (row_date, field.__name__)
            for (row_date, row) in result.rows.items()
            for field in mapped_fields
            if getattr(row, field.__name__) is None
        ]

        assert unpopulated_row_fields == []


class TestGetSplitData:
    def test_get_split_data_returns_split_data_instance(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=SplitsDataBlock,
            configuration=configuration,
        )

        assert isinstance(result, SplitData)

    def test_get_split_data_returns_non_empty_rows(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=SplitsDataBlock,
            configuration=configuration,
        )

        assert len(result.rows) > 0

    def test_get_split_data_every_row_incorporates_every_mapped_tag(
        self,
        initialized_provider: Sharadar,
        configuration: Configuration,
    ) -> None:
        result = initialized_provider.get_data_block_data(
            main_identifier=EQUITY_IDENTIFIER,
            data_block=SplitsDataBlock,
            configuration=configuration,
        )
        endpoint_field_maps = Sharadar.get_data_block_endpoint_tag_map()[SplitsDataBlock]
        mapped_fields = [
            field
            for endpoint_fields in endpoint_field_maps.values()
            for field in endpoint_fields
        ]
        unpopulated_row_fields = [
            (row_date, field.__name__)
            for (row_date, row) in result.rows.items()
            for field in mapped_fields
            if getattr(row, field.__name__) is None
        ]

        assert unpopulated_row_fields == []
