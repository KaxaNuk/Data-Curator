import datetime
import decimal

import pytest

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCalculationError,
    ColumnBuilderUninitializedError,
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
    def test_uninitialized_configuration(
        self,
        market_close_configuration,
        monkeypatch,
    ):
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=market_close_configuration,
            data_blocks=[],
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
            },
        )
        monkeypatch.setattr(ColumnBuilder, '_configuration', None)

        with pytest.raises(ColumnBuilderUninitializedError):
            column_builder.process_columns(market_close_configuration.columns)

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

    def test_dividend_on_intraday_master_clock(self):
        # The ex-dividend date starts at midnight, so the dividend takes effect from the first bar of its day
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=(
                'mi_timestamp',
                'd_ex_dividend_date_dividend',
            ),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[data_blocks.IntradayMarketDataBlock],
            master_clock_data_block=data_blocks.IntradayMarketDataBlock,
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                DividendsDataBlock: data_block_entities.DIVIDEND_DATA,
                data_blocks.IntradayMarketDataBlock: data_block_entities.INTRADAY_MARKET_DATA,
            },
        )
        result = column_builder.process_columns(configuration.columns).to_pydict()
        expected = {
            'mi_timestamp': [
                datetime.datetime(2024, 1, 29, 15),
                datetime.datetime(2024, 1, 30, 9),
                datetime.datetime(2024, 1, 30, 10),
                datetime.datetime(2024, 1, 31, 9),
            ],
            'd_ex_dividend_date_dividend': [
                None,
                decimal.Decimal('0.25'),
                None,
                None,
            ],
        }

        assert result == expected

    def test_daily_rows_on_intraday_master_clock(self):
        # Each daily bar is only known once its day ends, so its close appears from the first bar of the next day
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 2, 1),
            period='annual',
            identifiers=('TEST',),
            columns=('m_close',),
        )
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=[calculations],
            configuration=configuration,
            data_blocks=[data_blocks.IntradayMarketDataBlock],
            master_clock_data_block=data_blocks.IntradayMarketDataBlock,
        )
        column_builder = ColumnBuilder(
            data_block_entities={
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
                data_blocks.IntradayMarketDataBlock: data_block_entities.INTRADAY_MARKET_DATA,
            },
        )
        result = column_builder.process_columns(configuration.columns).to_pydict()
        expected = {
            'm_close': [
                None,
                None,
                None,
                decimal.Decimal('10.5'),
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
