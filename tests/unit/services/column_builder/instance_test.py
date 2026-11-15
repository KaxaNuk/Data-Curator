import datetime
import decimal

import pytest

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import InjectedDependencyError
from kaxanuk.data_curator.features import calculations
from kaxanuk.data_curator.services.column_builder import ColumnBuilder
from .fixtures import (
    data_block_entities,
    data_blocks,
)


@pytest.fixture
def market_close_configuration():
    return Configuration(
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2024, 2, 1),
        period='annual',
        identifiers=('TEST',),
        columns=('m_close',),
    )


class TestDunderInit:
    def test_data_blocks_sharing_main_entity(self):
        # Each data block's columns come from its own entity, even though both entities are MarketData instances
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=(
                'm_close',
                'mx_close',
            ),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[data_blocks.AlternativeMarketDailyDataBlock],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
                data_blocks.AlternativeMarketDailyDataBlock: data_block_entities.ALTERNATIVE_MARKET_DATA,
            },
        )
        result = column_builder.process_columns(configuration.columns).to_pydict()
        expected = {
            'm_close': [
                decimal.Decimal('10.5'),
            ],
            'mx_close': [
                decimal.Decimal('20.5'),
            ],
        }

        assert result == expected

    def test_data_block_outside_run(
        self,
        market_close_configuration,
    ):
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=market_close_configuration,
            data_blocks=[],
        )

        with pytest.raises(InjectedDependencyError):
            ColumnBuilder(
                data_block_entities={
                    MarketDailyDataBlock: data_block_entities.MARKET_DATA,
                    data_blocks.AlternativeMarketDailyDataBlock: data_block_entities.ALTERNATIVE_MARKET_DATA,
                },
            )

    def test_entity_of_another_main_entity(
        self,
        market_close_configuration,
    ):
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=market_close_configuration,
            data_blocks=[],
        )

        with pytest.raises(InjectedDependencyError):
            ColumnBuilder(
                data_block_entities={
                    MarketDailyDataBlock: data_block_entities.MARKET_DATA,
                    DividendsDataBlock: data_block_entities.MARKET_DATA,
                },
            )

    def test_without_master_clock_entity(
        self,
        market_close_configuration,
    ):
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=market_close_configuration,
            data_blocks=[],
        )

        with pytest.raises(InjectedDependencyError):
            ColumnBuilder(
                data_block_entities={},
            )
