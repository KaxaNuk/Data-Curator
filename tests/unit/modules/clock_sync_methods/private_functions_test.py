import datetime

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.exceptions import DataColumnSyncError
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


@pytest.fixture
def half_hourly_labeled_master_clock():
    # Hourly bars starting at the half hours, like those of a session opening at 09:30
    return DataColumn.load(
        [
            datetime.datetime(2024, 1, 30, 9, 30),
            datetime.datetime(2024, 1, 30, 10, 30),
        ],
        clock_master=True,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.HOUR,
    )


@pytest.fixture
def master_timestamps():
    return pyarrow.array([
        datetime.date(2024, 1, 30),
        datetime.date(2024, 1, 31),
        datetime.date(2024, 2, 1),
    ])


class TestPrivateCalculateMasterPeriods:
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
        result = clock_sync_methods._calculate_master_periods(master_clock)
        expected = pyarrow.array([
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 8),
        ])

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

        with pytest.raises(DataColumnSyncError):
            clock_sync_methods._calculate_master_periods(master_clock)

    def test_intraday_master_clock_labeled_by_half_hours(
        self,
        half_hourly_labeled_master_clock,
    ):
        result = clock_sync_methods._calculate_master_periods(half_hourly_labeled_master_clock)

        assert result is half_hourly_labeled_master_clock.reference_array

    def test_overlapping_intraday_master_clock_rows(self):
        master_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 9, 30),
                datetime.datetime(2024, 1, 30, 10),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
        )

        with pytest.raises(DataColumnSyncError):
            clock_sync_methods._calculate_master_periods(master_clock)


class TestPrivateCalculatePeriodEnds:
    def test_daily_clock(
        self,
        daily_master_clock,
    ):
        result = clock_sync_methods._calculate_period_ends(
            clock_column=daily_master_clock,
            timestamps=daily_master_clock.reference_array,
        )
        expected = pyarrow.array([
            datetime.date(2024, 1, 31),
            datetime.date(2024, 2, 1),
        ])

        assert result.equals(expected)

    def test_intraday_clock_labeled_by_half_hours(
        self,
        half_hourly_labeled_master_clock,
    ):
        result = clock_sync_methods._calculate_period_ends(
            clock_column=half_hourly_labeled_master_clock,
            timestamps=half_hourly_labeled_master_clock.reference_array,
        )
        expected = pyarrow.array([
            datetime.datetime(2024, 1, 30, 10, 30),
            datetime.datetime(2024, 1, 30, 11, 30),
        ])

        assert result.equals(expected)


class TestPrivateCalculateReferencePeriods:
    def test_floored_to_master_clock_periods(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10),
                datetime.datetime(2024, 1, 31, 16),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=daily_master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        result = clock_sync_methods._calculate_reference_periods(
            master_clock_column=daily_master_clock,
            reference_clock_column=reference_clock,
        )
        expected = pyarrow.array([
            datetime.date(2024, 1, 30),
            datetime.date(2024, 1, 31),
        ])

        assert result.equals(expected)

    def test_null_reference_timestamp(
        self,
        daily_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
                None,
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=daily_master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.DAY,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        result = clock_sync_methods._calculate_reference_periods(
            master_clock_column=daily_master_clock,
            reference_clock_column=reference_clock,
        )
        expected = pyarrow.array([
            datetime.date(2024, 1, 30),
            None,
        ])

        assert result.equals(expected)

    def test_intraday_master_clock_labeled_by_half_hours(
        self,
        half_hourly_labeled_master_clock,
    ):
        reference_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10, 15),
                datetime.datetime(2024, 1, 30, 11, 45),
                datetime.datetime(2024, 1, 30, 9),
                None,
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_master=half_hourly_labeled_master_clock,
            clock_periods_per_row=0,
            clock_reference_period=ReferencePeriods.MINUTE,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )
        result = clock_sync_methods._calculate_reference_periods(
            master_clock_column=half_hourly_labeled_master_clock,
            reference_clock_column=reference_clock,
        )
        # 10:15 falls in the bar from 09:30 to 10:30, while 11:45 falls after the bar from 10:30 to 11:30, and 09:00
        # before all the bars, so they keep their own timestamps
        expected = pyarrow.array([
            datetime.datetime(2024, 1, 30, 9, 30),
            datetime.datetime(2024, 1, 30, 11, 45),
            datetime.datetime(2024, 1, 30, 9),
            None,
        ])

        assert result.equals(expected)


class TestPrivateCalculateSpanDuration:
    def test_hourly_clock(self):
        master_clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 9, 30),
            ],
            clock_master=True,
            clock_periods_per_row=2,
            clock_reference_period=ReferencePeriods.HOUR,
        )
        result = clock_sync_methods._calculate_span_duration(master_clock)

        assert result.as_py() == datetime.timedelta(hours=2)

    def test_calendar_period_clock(self):
        master_clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
        )

        with pytest.raises(DataColumnSyncError):
            clock_sync_methods._calculate_span_duration(master_clock)

    def test_column_without_clock(self):
        with pytest.raises(DataColumnSyncError):
            clock_sync_methods._calculate_span_duration(
                DataColumn.load([1, 2])
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
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
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
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
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
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
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
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
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


class TestPrivateMarkRowsBeforeMasterClock:
    def test_rows_before_first_master_clock_period(self):
        result = clock_sync_methods._mark_rows_before_master_clock(
            master_periods=pyarrow.array([
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ]),
            master_row_indices=pyarrow.array(
                [None, 0, 1],
                type=pyarrow.int64(),
            ),
            reference_periods=pyarrow.array([
                datetime.date(2024, 1, 29),
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ]),
        )
        expected = pyarrow.array(
            [-1, 0, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_null_reference_period(self):
        result = clock_sync_methods._mark_rows_before_master_clock(
            master_periods=pyarrow.array([
                datetime.date(2024, 1, 30),
            ]),
            master_row_indices=pyarrow.array(
                [None],
                type=pyarrow.int64(),
            ),
            reference_periods=pyarrow.array(
                [None],
                type=pyarrow.date32(),
            ),
        )
        expected = pyarrow.array(
            [None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_master_clock_without_rows(self):
        master_row_indices = pyarrow.array(
            [None],
            type=pyarrow.int64(),
        )
        result = clock_sync_methods._mark_rows_before_master_clock(
            master_periods=pyarrow.array(
                [],
                type=pyarrow.date32(),
            ),
            master_row_indices=master_row_indices,
            reference_periods=pyarrow.array([
                datetime.date(2024, 1, 29),
            ]),
        )

        assert result is master_row_indices


class TestPrivateMatchLatestBoundaryRows:
    def test_latest_boundary_at_or_before_each_timestamp(self):
        result = clock_sync_methods._match_latest_boundary_rows(
            boundaries=pyarrow.array([
                datetime.datetime(2024, 1, 30, 9, 30),
                datetime.datetime(2024, 1, 30, 10, 30),
            ]),
            timestamps=pyarrow.array([
                datetime.datetime(2024, 1, 30, 11),
                datetime.datetime(2024, 1, 30, 9, 30),
                datetime.datetime(2024, 1, 30, 10, 29),
            ]),
        )
        expected = pyarrow.array(
            [1, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_timestamps_before_all_boundaries(self):
        result = clock_sync_methods._match_latest_boundary_rows(
            boundaries=pyarrow.array([
                datetime.datetime(2024, 1, 30, 9, 30),
            ]),
            timestamps=pyarrow.array([
                datetime.datetime(2024, 1, 30, 9),
            ]),
        )
        expected = pyarrow.array(
            [None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_null_timestamp(self):
        result = clock_sync_methods._match_latest_boundary_rows(
            boundaries=pyarrow.array([
                datetime.datetime(2024, 1, 30, 9, 30),
            ]),
            timestamps=pyarrow.array(
                [None],
                type=pyarrow.timestamp('us'),
            ),
        )
        expected = pyarrow.array(
            [None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)


class TestPrivateMatchLatestReferenceRows:
    def test_latest_reference_timestamp_at_or_before_each_master_timestamp(
        self,
        master_timestamps,
    ):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=master_timestamps,
            reference_timestamps=pyarrow.array([
                datetime.date(2024, 1, 29),
                datetime.date(2024, 1, 31),
            ]),
        )
        expected = pyarrow.array(
            [0, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_master_timestamps_before_all_reference_timestamps(
        self,
        master_timestamps,
    ):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=master_timestamps,
            reference_timestamps=pyarrow.array([
                datetime.date(2024, 1, 31),
            ]),
        )
        expected = pyarrow.array(
            [None, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_unsorted_reference_timestamps(
        self,
        master_timestamps,
    ):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=master_timestamps,
            reference_timestamps=pyarrow.array([
                datetime.date(2024, 1, 31),
                datetime.date(2024, 1, 29),
            ]),
        )
        expected = pyarrow.array(
            [1, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_last_of_equal_reference_timestamps(
        self,
        master_timestamps,
    ):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=master_timestamps,
            reference_timestamps=pyarrow.array([
                datetime.date(2024, 1, 31),
                datetime.date(2024, 1, 31),
            ]),
        )
        expected = pyarrow.array(
            [None, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_null_reference_timestamp(
        self,
        master_timestamps,
    ):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=master_timestamps,
            reference_timestamps=pyarrow.array([
                None,
                datetime.date(2024, 1, 29),
            ]),
        )
        expected = pyarrow.array(
            [1, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_empty_reference_timestamps(
        self,
        master_timestamps,
    ):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=master_timestamps,
            reference_timestamps=pyarrow.array(
                [],
                type=pyarrow.date32(),
            ),
        )
        expected = pyarrow.array(
            [None, None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_equal_master_timestamps(self):
        result = clock_sync_methods._match_latest_reference_rows(
            master_timestamps=pyarrow.array([
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ]),
            reference_timestamps=pyarrow.array([
                datetime.date(2024, 1, 30),
            ]),
        )
        expected = pyarrow.array(
            [0, 0, 0],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)
