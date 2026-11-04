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
            datetime.date(2024, 2, 1),
        ],
        clock_master=True,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.DAY,
    )


class TestExactMatch:
    def test_unsorted_reference_rows(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
                datetime.date(2024, 1, 30),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.exact_match,
        )
        result = clock_sync_methods.UpscaleMethods.exact_match(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [1, 0, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_last_of_duplicate_reference_rows(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 30),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.exact_match,
        )
        result = clock_sync_methods.UpscaleMethods.exact_match(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [1, None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_reference_timestamps_on_daily_master_clock(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 16),
                datetime.datetime(2024, 1, 31, 16),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.exact_match,
        )
        result = clock_sync_methods.UpscaleMethods.exact_match(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 1, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_weekly_clocks_labeled_by_different_days(self):
        # The master clock weeks are labeled by Friday, and the reference clock weeks by Monday
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 5),
                datetime.date(2024, 1, 12),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.WEEK,
        )
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 8),
            ],
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.WEEK,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.exact_match,
        )
        result = clock_sync_methods.UpscaleMethods.exact_match(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_null_reference_timestamp(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                None,
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.exact_match,
        )
        result = clock_sync_methods.UpscaleMethods.exact_match(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, 1, None],
            type=pyarrow.int64(),
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
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.exact_match,
        )
        result = clock_sync_methods.UpscaleMethods.exact_match(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)


class TestInfill:
    def test_latest_reference_row_at_or_before_each_master_row(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2023, 12, 31),
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_master_clock_rows_before_all_reference_rows(
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
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_unsorted_reference_rows(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
                datetime.date(2023, 12, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [1, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_last_of_duplicate_reference_rows(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_null_reference_timestamp(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                None,
                datetime.date(2023, 12, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [1, 1, 1],
            type=pyarrow.int64(),
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
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_reference_timestamps_on_daily_master_clock(
        self,
        daily_master_clock,
    ):
        # The reference timestamp gets truncated to its date, so it also matches the master clock row of that date
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 16),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        result = clock_sync_methods.UpscaleMethods.infill(
            daily_master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)
