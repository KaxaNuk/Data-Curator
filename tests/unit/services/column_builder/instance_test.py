import datetime
import decimal

import pytest

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCalculationError,
    InjectedDependencyError,
)
from kaxanuk.data_curator.services.column_builder import ColumnBuilder
from .fixtures import (
    calculations,
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

class TestProcessColumns:
    def test_dividend_on_its_ex_dividend_date(self):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=('d_ex_dividend_date_dividend',),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                DividendsDataBlock: data_block_entities.DIVIDEND_DATA,
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
            },
        )
        result = column_builder.process_columns(configuration.columns).to_pydict()
        expected = {
            'd_ex_dividend_date_dividend': [
                decimal.Decimal('0.25'),
            ],
        }

        assert result == expected

    def test_dividend_paid_after_master_clock_dates(self):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=('d_payment_date_dividend',),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                DividendsDataBlock: data_block_entities.DIVIDEND_DATA,
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
            },
        )
        result = column_builder.process_columns(configuration.columns).to_pydict()
        expected = {
            'd_payment_date_dividend': [
                None,
            ],
        }

        assert result == expected

    def test_data_block_without_entity(self):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=('d_ex_dividend_date_dividend',),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
            },
        )
        result = column_builder.process_columns(configuration.columns).to_pydict()
        expected = {
            'd_ex_dividend_date_dividend': [
                None,
            ],
        }

        assert result == expected

    def test_failing_calculation(self):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=('c_failing_calculation',),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
            },
        )

        with pytest.raises(ColumnBuilderCalculationError):
            column_builder.process_columns(configuration.columns)

    def test_calculation_output_with_wrong_length(self):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=('c_wrong_length_calculation',),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
            },
        )

        with pytest.raises(ColumnBuilderCalculationError):
            column_builder.process_columns(configuration.columns)
