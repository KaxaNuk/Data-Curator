import datetime

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.exceptions import DataColumnSyncError
from kaxanuk.data_curator.modules.data_column import ReferencePeriods


class TestCalendarPeriods:
    def test_intraday_rows_on_daily_master_clock(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
                datetime.date(2024, 2, 1),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10),
                datetime.datetime(2024, 1, 30, 11),
                datetime.datetime(2024, 1, 31, 10),
                datetime.datetime(2024, 2, 1, 10),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 0, 1, 2],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_rows_outside_all_master_clock_periods(self):
        # The master clock has the trading days Friday 2024-01-05 and Monday 2024-01-08
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 5),
                datetime.date(2024, 1, 8),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 5, 10),
                datetime.datetime(2024, 1, 6, 10),
                datetime.datetime(2024, 1, 8, 10),
                datetime.datetime(2024, 1, 9, 10),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, None, 1, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_weekly_master_clock_labeled_by_friday(self):
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
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 12),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 0, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_monthly_master_clock_labeled_by_month_end(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
                datetime.date(2024, 2, 29),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
        )
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 15),
                datetime.date(2024, 2, 10),
                datetime.date(2024, 3, 1),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 1, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_quarterly_master_clock(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 3, 31),
                datetime.date(2024, 6, 30),
            ],
            clock_master=True,
            clock_periods_per_row=3,
            clock_reference_period=ReferencePeriods.MONTH,
        )
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
                datetime.date(2024, 2, 29),
                datetime.date(2024, 3, 31),
                datetime.date(2024, 4, 30),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, 0, 0, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_null_reference_timestamp(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10),
                None,
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [0, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_reference_clock_without_timestamps(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        reference_clock = DataColumn(
            pyarrow.nulls(2),
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        result = clock_sync_methods.DownscaleMethods.calendar_periods(
            master_clock,
            reference_clock,
        )
        expected = pyarrow.array(
            [None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_several_master_clock_rows_in_same_period(self):
        # Monday 2024-01-01 and Wednesday 2024-01-03 fall in the same week
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 3),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.WEEK,
        )
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 1),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )

        with pytest.raises(DataColumnSyncError):
            clock_sync_methods.DownscaleMethods.calendar_periods(
                master_clock,
                reference_clock,
            )
