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

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.data_providers.intrinio import Intrinio
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
from .fixtures import intrinio_sdk_mock


TEST_API_KEY = '12345'
MAIN_IDENTIFIER = 'XXXX'
PERIOD = 'quarterly'
START_DATE = datetime.date(2019, 1, 1)
END_DATE = datetime.date(2024, 12, 31)

# Mapped fundamental tags that no row populates for the test identifier over the test range: the
# captured fixtures simply don't report these line items (the source issuer has no capital leases,
# non-controlling interest, discontinued operations, etc.), so they can't be exercised without
# fabricating data. Listed as "Entity.field" qualified names to stay unambiguous across sub-entities.
# The coverage test asserts every *other* mapped tag is populated, and that each name here really is
# absent, so this stays honest: broaden the fixtures and a stale entry will fail the test.
FUNDAMENTAL_TAGS_ABSENT_FROM_FIXTURES = frozenset({
    'FundamentalDataRowBalanceSheet.capital_lease_obligations',
    'FundamentalDataRowBalanceSheet.longterm_investments',
    'FundamentalDataRowBalanceSheet.noncontrolling_interest',
    'FundamentalDataRowBalanceSheet.noncurrent_deferred_revenue',
    'FundamentalDataRowBalanceSheet.noncurrent_deferred_tax_liabilities',
    'FundamentalDataRowBalanceSheet.other_assets',
    'FundamentalDataRowBalanceSheet.other_current_assets',
    'FundamentalDataRowBalanceSheet.other_current_liabilities',
    'FundamentalDataRowBalanceSheet.treasury_stock_value',
    'FundamentalDataRowCashFlow.cash_exchange_rate_effect',
    'FundamentalDataRowCashFlow.interest_payments',
    'FundamentalDataRowCashFlow.preferred_stock_issuance_proceeds',
    'FundamentalDataRowIncomeStatement.discontinued_operations_income_after_tax',
    'FundamentalDataRowIncomeStatement.net_interest_income',
})


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

    def test_get_market_data_every_row_incorporates_every_mapped_tag(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_market_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        # The tag map's keys are the entity-field descriptors the provider populates, and each
        # descriptor's __name__ is the attribute that holds that field's value on a result row.
        endpoint_field_maps = Intrinio.get_data_block_endpoint_tag_map()[MarketDailyDataBlock]
        mapped_fields = [
            field
            for endpoint_fields in endpoint_field_maps.values()
            for field in endpoint_fields
        ]
        unpopulated_row_fields = [
            (row_date, field.__name__)
            for (row_date, row) in result.daily_rows.items()
            for field in mapped_fields
            if getattr(row, field.__name__) is None
        ]

        assert unpopulated_row_fields == []


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

    def test_get_fundamental_data_every_expected_tag_populated_across_rows(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_fundamental_data(
            main_identifier=MAIN_IDENTIFIER,
            period=PERIOD,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        # The tag map's keys are the entity-field descriptors the provider populates. Fundamental
        # rows nest their columns under sub-entities, so each descriptor's __objclass__ tells us
        # which part of a row holds it (top-level fields live on the row itself). Unlike the dense
        # data blocks, a single fundamental period rarely reports every line item, so a tag counts
        # as incorporated when any row carries a non-null value for it.
        endpoint_field_maps = Intrinio.get_data_block_endpoint_tag_map()[FundamentalsDataBlock]
        mapped_fields = [
            field
            for endpoint_fields in endpoint_field_maps.values()
            for field in endpoint_fields
        ]
        sub_entity_attribute_by_entity = {
            FundamentalDataRow: None,
            FundamentalDataRowBalanceSheet: 'balance_sheet',
            FundamentalDataRowCashFlow: 'cash_flow',
            FundamentalDataRowIncomeStatement: 'income_statement',
        }
        populated_tags = set()
        for row in result.rows.values():
            if row is None:
                continue

            for field in mapped_fields:
                sub_entity_attribute = sub_entity_attribute_by_entity[field.__objclass__]
                row_part = (
                    row if sub_entity_attribute is None
                    else getattr(row, sub_entity_attribute)
                )
                if (
                    row_part is not None
                    and getattr(row_part, field.__name__) is not None
                ):
                    populated_tags.add(f"{field.__objclass__.__name__}.{field.__name__}")

        expected_populated_tags = {
            f"{field.__objclass__.__name__}.{field.__name__}"
            for field in mapped_fields
        } - FUNDAMENTAL_TAGS_ABSENT_FROM_FIXTURES

        assert populated_tags == expected_populated_tags


    def test_interim_quarter_rows_carry_cash_flow(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        # Intrinio serves the discrete second- and third-quarter cash flows only as calculated
        # statements; the provider must keep them so those quarters aren't left without cash flow.
        result = initialized_provider.get_fundamental_data(
            main_identifier=MAIN_IDENTIFIER,
            period=PERIOD,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        interim_rows = [
            row
            for row in result.rows.values()
            if (
                row is not None
                and row.fiscal_period in ('Q2', 'Q3')
            )
        ]

        assert len(interim_rows) > 0
        assert all(row.cash_flow is not None for row in interim_rows)


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

    def test_get_dividend_data_every_row_incorporates_every_mapped_tag(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_dividend_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        # The tag map's keys are the entity-field descriptors the provider populates, and each
        # descriptor's __name__ is the attribute that holds that field's value on a result row.
        endpoint_field_maps = Intrinio.get_data_block_endpoint_tag_map()[DividendsDataBlock]
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

    def test_get_split_data_every_row_incorporates_every_mapped_tag(
        self,
        initialized_provider: Intrinio,
    ) -> None:
        result = initialized_provider.get_split_data(
            main_identifier=MAIN_IDENTIFIER,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        # The tag map's keys are the entity-field descriptors the provider populates, and each
        # descriptor's __name__ is the attribute that holds that field's value on a result row.
        endpoint_field_maps = Intrinio.get_data_block_endpoint_tag_map()[SplitsDataBlock]
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
