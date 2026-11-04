import datetime

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.modules.data_column import ReferencePeriods


@pytest.fixture
def daily_master_clock():
    return DataColumn.load(
        [
            datetime.date(2024, 1, 30),
            datetime.date(2024, 1, 31),
        ],
        clock_master=True,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.DAY,
    )


class TestPrivateCastToMasterClockType:
    def test_same_type(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods._cast_to_master_clock_type(
            master_clock_column=daily_master_clock,
            reference_clock_column=reference_clock,
        )

        assert result is reference_clock.reference_array

    def test_timestamps_to_dates(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 31, 16),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods._cast_to_master_clock_type(
            master_clock_column=daily_master_clock,
            reference_clock_column=reference_clock,
        )
        expected = pyarrow.array(
            [
                datetime.date(2024, 1, 31),
            ]
        )

        assert result.equals(expected)

    def test_dates_to_timestamps(self):
        master_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10),
                datetime.datetime(2024, 1, 30, 11),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
        )
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
            ],
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods._cast_to_master_clock_type(
            master_clock_column=master_clock,
            reference_clock_column=reference_clock,
        )
        expected = pyarrow.array(
            [
                datetime.datetime(2024, 1, 30),
            ]
        )

        assert result.equals(expected)

    def test_reference_clock_without_timestamps(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn(
            pyarrow.nulls(2),
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods._cast_to_master_clock_type(
            master_clock_column=daily_master_clock,
            reference_clock_column=reference_clock,
        )
        expected = pyarrow.array(
            [None, None],
            type=pyarrow.date32(),
        )

        assert result.equals(expected)
