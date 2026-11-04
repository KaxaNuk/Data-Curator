import datetime

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.exceptions import DataColumnSyncError
from kaxanuk.data_curator.modules.data_column import ReferencePeriods

from .fixtures import (
    clocks,
    sync_methods,
)


@pytest.fixture
def daily_master_clock():
    return clocks.create_daily_master_clock()


@pytest.fixture
def hourly_reference_clock(daily_master_clock):
    return clocks.create_hourly_reference_clock(daily_master_clock)


@pytest.fixture
def monthly_reference_clock(daily_master_clock):
    return clocks.create_monthly_reference_clock(daily_master_clock)


@pytest.fixture
def quarterly_reference_clock(daily_master_clock):
    return DataColumn.load(
        [
            datetime.date(2023, 12, 31),
        ],
        clock_master=daily_master_clock,
        clock_periods_per_row=3,
        clock_reference_period=ReferencePeriods.MONTH,
        clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
    )


class CountingUpscaleMethod:
    """
    Upscale method counting how many times it gets called, while matching the rows like the infill method.
    """

    def __init__(self):
        self.call_count = 0

    def __call__(
        self,
        master_clock_column,
        reference_clock_column,
    ):
        self.call_count += 1

        return clock_sync_methods.UpscaleMethods.infill(
            master_clock_column,
            reference_clock_column,
        )


class TestArray:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])
        expected = pyarrow.array([1, 2, 3])

        assert column.array.equals(expected)

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        expected = pyarrow.array(
            [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
                datetime.date(2024, 2, 1),
            ]
        )

        assert daily_master_clock.array.equals(expected)

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        expected = pyarrow.array([1, 2, 3])

        assert column.array.equals(expected)

    def test_upscaling_reference_clock(
        self,
        monthly_reference_clock,
    ):
        expected = pyarrow.array(
            [
                datetime.date(2023, 12, 31),
                datetime.date(2024, 1, 31),
                datetime.date(2024, 1, 31),
            ]
        )

        assert monthly_reference_clock.array.equals(expected)

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        expected = pyarrow.array([10, 20, 20])

        assert column.array.equals(expected)

    def test_column_on_upscaling_clock_with_unmatched_master_row(
        self,
        daily_master_clock,
    ):
        clock = DataColumn.load(
            [
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )
        column = DataColumn.load(
            [10],
            reference_clock_column=clock,
        )
        expected = pyarrow.array([None, 10, 10])

        assert column.array.equals(expected)

    def test_downscaling_reference_clock(
        self,
        hourly_reference_clock,
    ):
        expected = pyarrow.array(
            [
                datetime.datetime(2024, 1, 30, 11),
                datetime.datetime(2024, 1, 31, 10),
                datetime.datetime(2024, 2, 1, 10),
            ]
        )

        assert hourly_reference_clock.array.equals(expected)

    def test_column_on_downscaling_clock_with_clock_consolidation_method(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        expected = pyarrow.array([2, 3, 4])

        assert column.array.equals(expected)

    def test_column_on_downscaling_clock_with_own_consolidation_method(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
            reference_clock_column=hourly_reference_clock,
        )
        expected = pyarrow.array([5, 3, 4])

        assert column.array.equals(expected)

    def test_column_on_downscaling_clock_with_empty_master_rows(
        self,
        daily_master_clock,
    ):
        clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10),
                datetime.datetime(2024, 1, 30, 11),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        column = DataColumn.load(
            [1, 2],
            reference_clock_column=clock,
        )
        expected = pyarrow.array([2, None, None])

        assert column.array.equals(expected)

    def test_cached(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.array is column.array

    def test_consolidation_method_returning_wrong_length(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            consolidation_method=sync_methods.consolidate_into_one_row,
            reference_clock_column=hourly_reference_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column.array  # noqa: B018  accessing the property runs the sync

    def test_consolidation_method_not_returning_array(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            consolidation_method=sync_methods.consolidate_into_list,
            reference_clock_column=hourly_reference_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column.array  # noqa: B018  accessing the property runs the sync


class TestClockChainParameters:
    def test_parameter_names(self):
        column = DataColumn.load([1, 2, 3])

        assert column.clock_chain_parameters.keys() == DataColumn.ClockChainParameters.__optional_keys__

    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert all(
            parameter is None
            for parameter in column.clock_chain_parameters.values()
        )

    @pytest.mark.parametrize(
        'parameter_name',
        [
            'clock_downscale_method',
            'clock_master',
            'clock_periods_per_row',
            'clock_reference_period',
            'clock_upscale_method',
            'consolidation_method',
        ]
    )
    def test_reference_clock(
        self,
        daily_master_clock,
        parameter_name,
    ):
        parameters = {
            'clock_downscale_method': clock_sync_methods.DownscaleMethods.calendar_periods,
            'clock_master': daily_master_clock,
            'clock_periods_per_row': 1,
            'clock_reference_period': ReferencePeriods.MONTH,
            'clock_upscale_method': clock_sync_methods.UpscaleMethods.infill,
            'consolidation_method': clock_sync_methods.ConsolidationMethods.last,
        }
        clock = DataColumn.load(
            [
                datetime.date(2023, 12, 31),
            ],
            **parameters,
        )

        assert clock.clock_chain_parameters[parameter_name] is parameters[parameter_name]

    @pytest.mark.parametrize(
        'parameter_name',
        [
            'consolidation_method',
            'reference_clock_column',
        ]
    )
    def test_column_on_clock(
        self,
        hourly_reference_clock,
        parameter_name,
    ):
        parameters = {
            'consolidation_method': clock_sync_methods.ConsolidationMethods.max,
            'reference_clock_column': hourly_reference_clock,
        }
        column = DataColumn.load(
            [5, 2, 3, 4],
            **parameters,
        )

        assert column.clock_chain_parameters[parameter_name] is parameters[parameter_name]

    def test_column_on_clock_without_own_consolidation_method(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )

        assert column.clock_chain_parameters['consolidation_method'] is None

    def test_returns_new_dict(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        parameters = column.clock_chain_parameters
        parameters['reference_clock_column'] = None

        assert column.clock_chain_parameters['reference_clock_column'] is daily_master_clock


class TestConsolidationMethod:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.consolidation_method is None

    def test_column_without_clock_with_own_method(self):
        column = DataColumn.load(
            [1, 2, 3],
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert column.consolidation_method is clock_sync_methods.ConsolidationMethods.max

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.consolidation_method is None

    def test_downscaling_reference_clock(
        self,
        hourly_reference_clock,
    ):
        assert hourly_reference_clock.consolidation_method is clock_sync_methods.ConsolidationMethods.last

    def test_column_on_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )

        assert column.consolidation_method is clock_sync_methods.ConsolidationMethods.last

    def test_column_on_clock_with_own_method(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
            reference_clock_column=hourly_reference_clock,
        )

        assert column.consolidation_method is clock_sync_methods.ConsolidationMethods.max

    def test_column_on_clock_without_method(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.consolidation_method is None


class TestDownscaleMethod:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.downscale_method is None

    def test_master_clock_with_method(self):
        clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
            ],
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )

        assert clock.downscale_method is clock_sync_methods.DownscaleMethods.calendar_periods

    def test_downscaling_reference_clock(
        self,
        hourly_reference_clock,
    ):
        assert hourly_reference_clock.downscale_method is clock_sync_methods.DownscaleMethods.calendar_periods

    def test_column_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )

        assert column.downscale_method is clock_sync_methods.DownscaleMethods.calendar_periods

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.downscale_method is None


class TestMasterClockColumn:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.master_clock_column is None

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.master_clock_column is daily_master_clock

    def test_reference_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        assert monthly_reference_clock.master_clock_column is daily_master_clock

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )

        assert column.master_clock_column is daily_master_clock

    def test_column_on_reference_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.master_clock_column is daily_master_clock


class TestPeriodsPerRow:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.periods_per_row is None

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.periods_per_row == 1

    def test_reference_clock(
        self,
        quarterly_reference_clock,
    ):
        assert quarterly_reference_clock.periods_per_row == 3

    def test_column_on_reference_clock(
        self,
        quarterly_reference_clock,
    ):
        column = DataColumn.load(
            [10],
            reference_clock_column=quarterly_reference_clock,
        )

        assert column.periods_per_row == 3


class TestReferenceArray:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])
        expected = pyarrow.array([1, 2, 3])

        assert column.reference_array.equals(expected)

    def test_column_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        expected = pyarrow.array([5, 2, 3, 4])

        assert column.reference_array.equals(expected)

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        expected = pyarrow.array([10, 20])

        assert column.reference_array.equals(expected)


class TestReferenceClockColumn:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.reference_clock_column is None

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.reference_clock_column is daily_master_clock

    def test_reference_clock(
        self,
        monthly_reference_clock,
    ):
        assert monthly_reference_clock.reference_clock_column is monthly_reference_clock

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )

        assert column.reference_clock_column is daily_master_clock

    def test_column_on_reference_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.reference_clock_column is monthly_reference_clock


class TestReferencePeriod:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.reference_period is None

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.reference_period is ReferencePeriods.DAY

    def test_reference_clock(
        self,
        monthly_reference_clock,
    ):
        assert monthly_reference_clock.reference_period is ReferencePeriods.MONTH

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )

        assert column.reference_period is ReferencePeriods.DAY

    def test_column_on_reference_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.reference_period is ReferencePeriods.MONTH


class TestSyncDirection:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.sync_direction is None

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.sync_direction is None

    def test_upscaling_reference_clock(
        self,
        monthly_reference_clock,
    ):
        assert monthly_reference_clock.sync_direction is DataColumn.SyncDirections.UPSCALE

    def test_downscaling_reference_clock(
        self,
        hourly_reference_clock,
    ):
        assert hourly_reference_clock.sync_direction is DataColumn.SyncDirections.DOWNSCALE

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )

        assert column.sync_direction is None

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.sync_direction is DataColumn.SyncDirections.UPSCALE

    def test_column_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )

        assert column.sync_direction is DataColumn.SyncDirections.DOWNSCALE


class TestSyncIndices:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.sync_indices is None

    def test_master_clock(
        self,
        daily_master_clock,
    ):
        assert daily_master_clock.sync_indices is None

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )

        assert column.sync_indices is None

    def test_upscaling_reference_clock(
        self,
        monthly_reference_clock,
    ):
        expected = pyarrow.array(
            [0, 1, 1],
            type=pyarrow.int64(),
        )

        assert monthly_reference_clock.sync_indices.equals(expected)

    def test_downscaling_reference_clock(
        self,
        hourly_reference_clock,
    ):
        expected = pyarrow.array(
            [0, 0, 1, 2],
            type=pyarrow.int64(),
        )

        assert hourly_reference_clock.sync_indices.equals(expected)

    def test_column_on_reference_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.sync_indices is monthly_reference_clock.sync_indices

    def test_cached(
        self,
        monthly_reference_clock,
    ):
        assert monthly_reference_clock.sync_indices is monthly_reference_clock.sync_indices

    def test_calculated_once_for_all_columns_on_clock(
        self,
        daily_master_clock,
    ):
        upscale_method = CountingUpscaleMethod()
        clock = DataColumn.load(
            [
                datetime.date(2023, 12, 31),
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=upscale_method,
        )
        first_column = DataColumn.load(
            [10, 20],
            reference_clock_column=clock,
        )
        second_column = DataColumn.load(
            [30, 40],
            reference_clock_column=clock,
        )
        first_column.array  # noqa: B018  accessing the property runs the sync
        second_column.array  # noqa: B018  accessing the property runs the sync

        assert upscale_method.call_count == 1

    @pytest.mark.parametrize(
        'clock_upscale_method',
        [
            sync_methods.match_into_float_indices,
            sync_methods.match_into_list,
            sync_methods.match_into_one_row,
        ]
    )
    def test_invalid_upscale_method_result(
        self,
        daily_master_clock,
        clock_upscale_method,
    ):
        clock = DataColumn.load(
            [
                datetime.date(2023, 12, 31),
                datetime.date(2024, 1, 31),
            ],
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_upscale_method,
        )

        with pytest.raises(DataColumnSyncError):
            clock.sync_indices  # noqa: B018  accessing the property runs the sync

    def test_invalid_downscale_method_result(
        self,
        daily_master_clock,
    ):
        clock = DataColumn.load(
            [
                datetime.datetime(2024, 1, 30, 10),
                datetime.datetime(2024, 1, 30, 11),
            ],
            clock_downscale_method=sync_methods.match_into_one_row,
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.HOUR,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )

        with pytest.raises(DataColumnSyncError):
            clock.sync_indices  # noqa: B018  accessing the property runs the sync


class TestType:
    def test_int32_column(self):
        data_type = pyarrow.int32()
        column = DataColumn(
            pyarrow.array(
                [1, 2, 3],
                type=data_type
            )
        )

        assert column.type == data_type

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.type == pyarrow.int64()

    def test_column_on_downscaling_clock_changing_type(
        self,
        hourly_reference_clock,
    ):
        # The mean of the integer reference array rows is a float at the master clock's scale
        column = DataColumn.load(
            [5, 2, 3, 4],
            consolidation_method=clock_sync_methods.ConsolidationMethods.mean,
            reference_clock_column=hourly_reference_clock,
        )

        assert column.type == pyarrow.float64()


class TestUpscaleMethod:
    def test_column_without_clock(self):
        column = DataColumn.load([1, 2, 3])

        assert column.upscale_method is None

    def test_master_clock_with_method(self):
        clock = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )

        assert clock.upscale_method is clock_sync_methods.UpscaleMethods.infill

    def test_upscaling_reference_clock(
        self,
        monthly_reference_clock,
    ):
        assert monthly_reference_clock.upscale_method is clock_sync_methods.UpscaleMethods.infill

    def test_column_on_upscaling_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )

        assert column.upscale_method is clock_sync_methods.UpscaleMethods.infill

    def test_column_on_downscaling_clock(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [5, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )

        assert column.upscale_method is None
