import datetime

import pyarrow
import pytest

from kaxanuk.data_curator import ReferencePeriods
from kaxanuk.data_curator.data_blocks.market_daily import (
    MarketDailyDataBlock,
    MarketData,
)
from kaxanuk.data_curator.entities import (
    ClockSpec,
    MarketInstrumentIdentifier,
)
from kaxanuk.data_curator.exceptions import EntityProcessingError


@pytest.fixture
def common_field_data():
    return {
        MarketData: {
            MarketData.main_identifier: MarketInstrumentIdentifier('TEST'),
            MarketData.clock_spec: ClockSpec(
                reference_period=ReferencePeriods.DAY,
                periods_per_row=1,
            ),
        },
    }


class TestAssembleEntitiesFromConsolidatedTable:
    def test_start_and_end_dates_of_first_and_last_rows(
        self,
        common_field_data,
    ):
        result = MarketDailyDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=pyarrow.table({
                'MarketDataDailyRow.date': [
                    datetime.date(2024, 1, 30),
                    datetime.date(2024, 1, 31),
                    datetime.date(2024, 2, 1),
                ],
                'MarketDataDailyRow.close': [
                    10.5,
                    11.5,
                    12.5,
                ],
            }),
            common_field_data=common_field_data,
        )
        expected = (
            datetime.date(2024, 1, 30),
            datetime.date(2024, 2, 1),
        )

        assert (result.start_date, result.end_date) == expected

    def test_first_row_without_data(
        self,
        common_field_data,
    ):
        # The first row only has its date, so it gets packed as None
        with pytest.raises(EntityProcessingError):
            MarketDailyDataBlock.assemble_entities_from_consolidated_table(
                consolidated_table=pyarrow.table({
                    'MarketDataDailyRow.date': [
                        datetime.date(2024, 1, 30),
                        datetime.date(2024, 1, 31),
                    ],
                    'MarketDataDailyRow.close': pyarrow.array(
                        [None, 11.5],
                        type=pyarrow.float64(),
                    ),
                }),
                common_field_data=common_field_data,
            )

    def test_last_row_without_data(
        self,
        common_field_data,
    ):
        # The last row only has its date, so it gets packed as None
        with pytest.raises(EntityProcessingError):
            MarketDailyDataBlock.assemble_entities_from_consolidated_table(
                consolidated_table=pyarrow.table({
                    'MarketDataDailyRow.date': [
                        datetime.date(2024, 1, 30),
                        datetime.date(2024, 1, 31),
                    ],
                    'MarketDataDailyRow.close': pyarrow.array(
                        [10.5, None],
                        type=pyarrow.float64(),
                    ),
                }),
                common_field_data=common_field_data,
            )

    def test_empty_table(
        self,
        common_field_data,
    ):
        with pytest.raises(EntityProcessingError):
            MarketDailyDataBlock.assemble_entities_from_consolidated_table(
                consolidated_table=pyarrow.table({
                    'MarketDataDailyRow.date': pyarrow.array(
                        [],
                        type=pyarrow.date32(),
                    ),
                    'MarketDataDailyRow.close': pyarrow.array(
                        [],
                        type=pyarrow.float64(),
                    ),
                }),
                common_field_data=common_field_data,
            )
