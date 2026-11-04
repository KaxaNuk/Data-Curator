import datetime

import pandas
import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.exceptions import DataColumnParameterError
from kaxanuk.data_curator.modules.data_column import ReferencePeriods

from .fixtures import clocks


@pytest.fixture
def daily_dates_array():
    return pyarrow.array(
        [
            datetime.date(2024, 1, 30),
            datetime.date(2024, 1, 31),
            datetime.date(2024, 2, 1),
        ]
    )


@pytest.fixture
def daily_master_clock():
    return clocks.create_daily_master_clock()


@pytest.fixture
def hourly_reference_clock(daily_master_clock):
    return clocks.create_hourly_reference_clock(daily_master_clock)


@pytest.fixture
def hourly_timestamps_array():
    return pyarrow.array(
        [
            datetime.datetime(2024, 1, 30, 10),
            datetime.datetime(2024, 1, 30, 11),
            datetime.datetime(2024, 1, 31, 10),
        ]
    )


@pytest.fixture
def monthly_dates_array():
    return pyarrow.array(
        [
            datetime.date(2023, 12, 31),
            datetime.date(2024, 1, 31),
        ]
    )


@pytest.fixture
def monthly_reference_clock(daily_master_clock):
    return clocks.create_monthly_reference_clock(daily_master_clock)


class TestDunderInit:
    def test_without_clock_parameters(self):
        column = DataColumn(
            pyarrow.array([1, 2, 3])
        )

        assert column.master_clock_column is None

    @pytest.mark.parametrize(
        'array',
        [
            # Dates
            pyarrow.array(
                [
                    datetime.date(2024, 1, 30),
                    datetime.date(2024, 1, 31),
                ]
            ),
            # Timestamps
            pyarrow.array(
                [
                    datetime.datetime(2024, 1, 30, 10),
                    datetime.datetime(2024, 1, 30, 11),
                ]
            ),
            # A single date
            pyarrow.array(
                [
                    datetime.date(2024, 1, 30),
                ]
            ),
            # No dates
            pyarrow.array(
                [],
                type=pyarrow.date32(),
            ),
        ]
    )
    def test_master_clock_with_valid_array(
        self,
        array,
    ):
        column = DataColumn(
            array,
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )

        assert column.master_clock_column is column

    def test_master_clock_with_unused_sync_methods(
        self,
        daily_dates_array,
    ):
        column = DataColumn(
            daily_dates_array,
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )

        assert column.master_clock_column is column

    def test_master_clock_without_sync_direction(
        self,
        daily_dates_array,
    ):
        column = DataColumn(
            daily_dates_array,
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )

        assert column.sync_direction is None

    @pytest.mark.parametrize(
        'array',
        [
            # Dates
            pyarrow.array(
                [
                    datetime.date(2023, 12, 31),
                    datetime.date(2024, 1, 31),
                ]
            ),
            # Timestamps
            pyarrow.array(
                [
                    datetime.datetime(2023, 12, 31, 16),
                    datetime.datetime(2024, 1, 31, 16),
                ]
            ),
            # Only nulls, for a clock without any timestamps
            pyarrow.nulls(2),
            # Unsorted dates with a null and a duplicate
            pyarrow.array(
                [
                    datetime.date(2024, 1, 31),
                    None,
                    datetime.date(2023, 12, 31),
                    datetime.date(2024, 1, 31),
                ]
            ),
        ]
    )
    def test_reference_clock_with_valid_array(
        self,
        daily_master_clock,
        array,
    ):
        column = DataColumn(
            array,
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
        )

        assert column.master_clock_column is daily_master_clock

    @pytest.mark.parametrize(
        (
            'master_period',
            'master_periods_per_row',
            'reference_period',
            'reference_periods_per_row',
            'expected',
        ),
        [
            # Calendar periods, compared by their number of months
            (ReferencePeriods.MONTH, 1, ReferencePeriods.MONTH, 3, DataColumn.SyncDirections.UPSCALE),
            (ReferencePeriods.MONTH, 3, ReferencePeriods.MONTH, 1, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.YEAR, 1, ReferencePeriods.MONTH, 12, DataColumn.SyncDirections.UPSCALE),
            (ReferencePeriods.MONTH, 12, ReferencePeriods.YEAR, 1, DataColumn.SyncDirections.UPSCALE),
            # Fixed periods, compared by their number of seconds
            (ReferencePeriods.DAY, 1, ReferencePeriods.HOUR, 1, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.DAY, 1, ReferencePeriods.HOUR, 24, DataColumn.SyncDirections.UPSCALE),
            (ReferencePeriods.DAY, 1, ReferencePeriods.WEEK, 1, DataColumn.SyncDirections.UPSCALE),
            (ReferencePeriods.MINUTE, 1, ReferencePeriods.SECOND, 59, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.HOUR, 1, ReferencePeriods.MINUTE, 90, DataColumn.SyncDirections.UPSCALE),
            # Fixed and calendar periods, with months spanning from 28 to 31 days
            (ReferencePeriods.MONTH, 1, ReferencePeriods.DAY, 27, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.MONTH, 1, ReferencePeriods.DAY, 31, DataColumn.SyncDirections.UPSCALE),
            (ReferencePeriods.MONTH, 1, ReferencePeriods.WEEK, 1, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.DAY, 1, ReferencePeriods.MONTH, 1, DataColumn.SyncDirections.UPSCALE),
            (ReferencePeriods.WEEK, 5, ReferencePeriods.MONTH, 1, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.YEAR, 1, ReferencePeriods.WEEK, 1, DataColumn.SyncDirections.DOWNSCALE),
            (ReferencePeriods.WEEK, 1, ReferencePeriods.YEAR, 1, DataColumn.SyncDirections.UPSCALE),
        ]
    )
    def test_reference_clock_with_orderable_span(
        self,
        master_period,
        master_periods_per_row,
        reference_period,
        reference_periods_per_row,
        expected,
    ):
        master_clock = DataColumn(
            pyarrow.array(
                [
                    datetime.date(2024, 1, 31),
                ]
            ),
            clock_master=True,
            clock_periods_per_row=master_periods_per_row,
            clock_reference_period=master_period,
        )
        reference_clock = DataColumn(
            pyarrow.array(
                [
                    datetime.date(2024, 1, 31),
                ]
            ),
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=master_clock,
            clock_periods_per_row=reference_periods_per_row,
            clock_reference_period=reference_period,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )

        assert reference_clock.sync_direction is expected

    @pytest.mark.parametrize(
        (
            'master_period',
            'master_periods_per_row',
            'reference_period',
            'reference_periods_per_row',
        ),
        [
            # Spans within the 28 to 31 days of a month, or the 336 to 372 days of 12 of them
            (ReferencePeriods.MONTH, 1, ReferencePeriods.DAY, 28),
            (ReferencePeriods.MONTH, 1, ReferencePeriods.DAY, 30),
            (ReferencePeriods.MONTH, 1, ReferencePeriods.WEEK, 4),
            (ReferencePeriods.DAY, 30, ReferencePeriods.MONTH, 1),
            (ReferencePeriods.YEAR, 1, ReferencePeriods.DAY, 365),
        ]
    )
    def test_reference_clock_with_unorderable_span(
        self,
        master_period,
        master_periods_per_row,
        reference_period,
        reference_periods_per_row,
    ):
        master_clock = DataColumn(
            pyarrow.array(
                [
                    datetime.date(2024, 1, 31),
                ]
            ),
            clock_master=True,
            clock_periods_per_row=master_periods_per_row,
            clock_reference_period=master_period,
        )

        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array(
                    [
                        datetime.date(2024, 1, 31),
                    ]
                ),
                clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
                clock_master=master_clock,
                clock_periods_per_row=reference_periods_per_row,
                clock_reference_period=reference_period,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=clock_sync_methods.ConsolidationMethods.last,
            )

    def test_column_on_master_clock(
        self,
        daily_master_clock,
    ):
        column = DataColumn(
            pyarrow.array([1, 2, 3]),
            reference_clock_column=daily_master_clock,
        )

        assert column.reference_clock_column is daily_master_clock

    def test_column_on_reference_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn(
            pyarrow.array([1, 2]),
            reference_clock_column=monthly_reference_clock,
        )

        assert column.reference_clock_column is monthly_reference_clock

    def test_column_on_clock_with_consolidation_method(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn(
            pyarrow.array([1, 2, 3, 4]),
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
            reference_clock_column=hourly_reference_clock,
        )

        assert column.consolidation_method is clock_sync_methods.ConsolidationMethods.max

    def test_column_without_clock_with_consolidation_method(self):
        column = DataColumn(
            pyarrow.array([1, 2, 3]),
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert column.consolidation_method is clock_sync_methods.ConsolidationMethods.max

    @pytest.mark.parametrize(
        'clock_master',
        [
            False,
            'master',
            1,
        ]
    )
    def test_invalid_clock_master(
        self,
        daily_dates_array,
        clock_master,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=clock_master,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
            )

    @pytest.mark.parametrize(
        'reference_clock_column',
        [
            'clock',
            pyarrow.array([1, 2, 3]),
        ]
    )
    def test_invalid_reference_clock_column(
        self,
        reference_clock_column,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array([1, 2, 3]),
                reference_clock_column=reference_clock_column,
            )

    @pytest.mark.parametrize(
        'non_callable_method',
        [
            {'clock_downscale_method': 'max'},
            {'clock_upscale_method': 'max'},
            {'consolidation_method': 'max'},
        ]
    )
    def test_non_callable_method(
        self,
        daily_dates_array,
        non_callable_method,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
                **non_callable_method,
            )

    def test_master_clock_with_reference_clock_column(
        self,
        daily_dates_array,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
                reference_clock_column=daily_master_clock,
            )

    def test_reference_clock_with_reference_clock_column(
        self,
        daily_dates_array,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=daily_master_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.MONTH,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                reference_clock_column=daily_master_clock,
            )

    def test_clock_master_pointing_to_reference_clock(
        self,
        monthly_reference_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array(
                    [
                        datetime.date(2023, 12, 31),
                    ]
                ),
                clock_master=monthly_reference_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.YEAR,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            )

    def test_clock_master_pointing_to_non_clock_column(
        self,
        daily_master_clock,
        monthly_dates_array,
    ):
        non_clock_column = DataColumn(
            pyarrow.array([1, 2, 3]),
            reference_clock_column=daily_master_clock,
        )

        with pytest.raises(DataColumnParameterError):
            DataColumn(
                monthly_dates_array,
                clock_master=non_clock_column,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.MONTH,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            )

    def test_reference_clock_column_pointing_to_non_clock_column(
        self,
        daily_master_clock,
    ):
        non_clock_column = DataColumn(
            pyarrow.array([1, 2, 3]),
            reference_clock_column=daily_master_clock,
        )

        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array([4, 5, 6]),
                reference_clock_column=non_clock_column,
            )

    def test_array_length_not_matching_reference_clock(
        self,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array([1, 2]),
                reference_clock_column=daily_master_clock,
            )

    @pytest.mark.parametrize(
        'clock_periods',
        [
            # Only the periods per row
            {'clock_periods_per_row': 1},
            # Only the reference period
            {'clock_reference_period': ReferencePeriods.DAY},
            # Neither of them
            {},
        ]
    )
    def test_master_clock_without_both_periods(
        self,
        daily_dates_array,
        clock_periods,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=True,
                **clock_periods,
            )

    @pytest.mark.parametrize(
        'clock_periods',
        [
            # Only the periods per row
            {'clock_periods_per_row': 1},
            # Only the reference period
            {'clock_reference_period': ReferencePeriods.MONTH},
            # Neither of them
            {},
        ]
    )
    def test_reference_clock_without_both_periods(
        self,
        daily_master_clock,
        monthly_dates_array,
        clock_periods,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                monthly_dates_array,
                clock_master=daily_master_clock,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                **clock_periods,
            )

    @pytest.mark.parametrize(
        'clock_periods',
        [
            # Only the periods per row
            {'clock_periods_per_row': 1},
            # Only the reference period
            {'clock_reference_period': ReferencePeriods.DAY},
            # Both of them
            {
                'clock_periods_per_row': 1,
                'clock_reference_period': ReferencePeriods.DAY,
            },
        ]
    )
    def test_non_clock_with_clock_periods(
        self,
        clock_periods,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array([1, 2, 3]),
                **clock_periods,
            )

    @pytest.mark.parametrize(
        'clock_sync_method',
        [
            {'clock_downscale_method': clock_sync_methods.DownscaleMethods.calendar_periods},
            {'clock_upscale_method': clock_sync_methods.UpscaleMethods.infill},
        ]
    )
    def test_non_clock_with_clock_sync_method(
        self,
        daily_master_clock,
        clock_sync_method,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array([1, 2, 3]),
                reference_clock_column=daily_master_clock,
                **clock_sync_method,
            )

    @pytest.mark.parametrize(
        'clock_reference_period',
        [
            # The value of a ReferencePeriods member, instead of the member itself
            'day',
            86_400,
        ]
    )
    def test_invalid_reference_period(
        self,
        daily_dates_array,
        clock_reference_period,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=clock_reference_period,
            )

    @pytest.mark.parametrize(
        'clock_periods_per_row',
        [
            0,
            -1,
            1.5,
            # A bool, as bool is a subclass of int
            True,
            '1',
        ]
    )
    def test_invalid_periods_per_row(
        self,
        daily_dates_array,
        clock_periods_per_row,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                daily_dates_array,
                clock_master=True,
                clock_periods_per_row=clock_periods_per_row,
                clock_reference_period=ReferencePeriods.DAY,
            )

    @pytest.mark.parametrize(
        'array',
        [
            pyarrow.array([1, 2, 3]),
            pyarrow.array(['2024-01-30', '2024-01-31']),
            pyarrow.nulls(2),
        ]
    )
    def test_master_clock_with_non_temporal_array(
        self,
        array,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                array,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
            )

    @pytest.mark.parametrize(
        'array',
        [
            pyarrow.array([1, 2]),
            pyarrow.array(['2023-12-31', '2024-01-31']),
        ]
    )
    def test_reference_clock_with_non_temporal_array(
        self,
        daily_master_clock,
        array,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                array,
                clock_master=daily_master_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.MONTH,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            )

    def test_master_clock_with_null_timestamp(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                pyarrow.array(
                    [
                        datetime.date(2024, 1, 30),
                        None,
                        datetime.date(2024, 2, 1),
                    ]
                ),
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
            )

    @pytest.mark.parametrize(
        'array',
        [
            # A repeated date
            pyarrow.array(
                [
                    datetime.date(2024, 1, 30),
                    datetime.date(2024, 1, 30),
                    datetime.date(2024, 1, 31),
                ]
            ),
            # Decreasing dates
            pyarrow.array(
                [
                    datetime.date(2024, 1, 31),
                    datetime.date(2024, 1, 30),
                ]
            ),
        ]
    )
    def test_master_clock_not_strictly_increasing(
        self,
        array,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                array,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
            )

    def test_upscaling_reference_clock_without_upscale_method(
        self,
        daily_master_clock,
        monthly_dates_array,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                monthly_dates_array,
                clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
                clock_master=daily_master_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.MONTH,
                consolidation_method=clock_sync_methods.ConsolidationMethods.last,
            )

    def test_downscaling_reference_clock_without_downscale_method(
        self,
        daily_master_clock,
        hourly_timestamps_array,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                hourly_timestamps_array,
                clock_master=daily_master_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.HOUR,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=clock_sync_methods.ConsolidationMethods.last,
            )

    def test_downscaling_reference_clock_without_consolidation_method(
        self,
        daily_master_clock,
        hourly_timestamps_array,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn(
                hourly_timestamps_array,
                clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
                clock_master=daily_master_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.HOUR,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            )


class TestLoad:
    def test_changing_dtype(self):
        data = [1, 2, 3, None]
        int_column = DataColumn.load(
            pyarrow.array(data, type=pyarrow.int32())
        )
        float_column = DataColumn.load(
            pyarrow.array(data, type=pyarrow.float64())
        )
        recast_column = DataColumn.load(int_column, dtype=pyarrow.float64())

        assert DataColumn.fully_equal(
            float_column,
            recast_column,
            equal_nulls=True
        )

    def test_from_pandas_with_nans(self):
        serie = pandas.Series([2.1, 5.2, float('nan'), 14])
        column = DataColumn.load([2.1, 5.2, float('nan'), 14])

        assert DataColumn.fully_equal(
            DataColumn.load(serie),
            column,
            equal_nulls=True
        )

    def test_data_column_without_parameters_returns_same_column(self):
        column = DataColumn.load([1, 2, 3, None])
        result = DataColumn.load(column)

        assert result is column

    def test_iterable_with_clock_chain_parameters(self):
        column = DataColumn.load(
            [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ],
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )

        assert column.master_clock_column is column

    def test_iterable_with_invalid_clock_chain_parameter(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn.load(
                [1, 2, 3],
                clock_periods_per_row=1,
            )

    def test_data_column_with_clock_chain_parameter_returns_new_column(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        result = DataColumn.load(
            column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert result is not column

    def test_data_column_with_clock_chain_parameter_overrides_it(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3, 4],
            consolidation_method=clock_sync_methods.ConsolidationMethods.min,
            reference_clock_column=hourly_reference_clock,
        )
        result = DataColumn.load(
            column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert result.consolidation_method is clock_sync_methods.ConsolidationMethods.max

    def test_data_column_with_clock_chain_parameter_keeps_the_others(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        result = DataColumn.load(
            column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert result.reference_clock_column is hourly_reference_clock

    def test_data_column_with_clock_chain_parameter_keeps_reference_array(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        result = DataColumn.load(
            column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert result.reference_array.equals(column.reference_array)

    def test_data_column_with_none_clock_chain_parameter_clears_it(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        result = DataColumn.load(
            column,
            reference_clock_column=None,
        )

        assert result.reference_clock_column is None

    def test_data_column_with_dtype_keeps_clock_chain_parameters(
        self,
        daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        result = DataColumn.load(
            column,
            dtype=pyarrow.float64(),
        )

        assert result.reference_clock_column is daily_master_clock

    def test_data_column_with_dtype_and_clock_chain_parameter(
        self,
        hourly_reference_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3, 4],
            reference_clock_column=hourly_reference_clock,
        )
        result = DataColumn.load(
            column,
            dtype=pyarrow.float64(),
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
        )

        assert result.reference_array.type == pyarrow.float64()

    def test_data_column_turned_into_clock_keeping_its_reference_clock(
        self,
        daily_dates_array,
        daily_master_clock,
    ):
        column = DataColumn.load(
            daily_dates_array,
            reference_clock_column=daily_master_clock,
        )

        with pytest.raises(DataColumnParameterError):
            DataColumn.load(
                column,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
            )

    def test_data_column_turned_into_clock_clearing_its_reference_clock(
        self,
        daily_dates_array,
        daily_master_clock,
    ):
        column = DataColumn.load(
            daily_dates_array,
            reference_clock_column=daily_master_clock,
        )
        result = DataColumn.load(
            column,
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            reference_clock_column=None,
        )

        assert result.master_clock_column is result

    def test_data_column_with_invalid_clock_chain_parameter(self):
        column = DataColumn.load([1, 2, 3])

        with pytest.raises(DataColumnParameterError):
            DataColumn.load(
                column,
                clock_periods_per_row=1,
            )
