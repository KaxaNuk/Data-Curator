import datetime
from decimal import Decimal

import pyarrow
import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.exceptions import (
    DataColumnParameterError,
    DataColumnSyncError,
)
from kaxanuk.data_curator.modules.data_column import ReferencePeriods

from .fixtures import (
    clocks,
    sync_methods,
)


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


@pytest.fixture
def other_daily_master_clock():
    return clocks.create_daily_master_clock()


class TestPrivateCalculateMasterSyncedArray:
    def test_without_sync_direction(
        self,
        daily_master_clock,
    ):
        reference_array = pyarrow.array([1, 2, 3])
        result = DataColumn._calculate_master_synced_array(
            consolidation_method=None,
            master_clock_column=daily_master_clock,
            reference_array=reference_array,
            sync_direction=None,
            sync_indices=None,
        )

        assert result is reference_array

    def test_without_sync_indices(
        self,
        daily_master_clock,
    ):
        reference_array = pyarrow.array([10, 20])
        result = DataColumn._calculate_master_synced_array(
            consolidation_method=None,
            master_clock_column=daily_master_clock,
            reference_array=reference_array,
            sync_direction=DataColumn.SyncDirections.UPSCALE,
            sync_indices=None,
        )

        assert result is reference_array

    def test_without_master_clock(self):
        reference_array = pyarrow.array([10, 20])
        result = DataColumn._calculate_master_synced_array(
            consolidation_method=None,
            master_clock_column=None,
            reference_array=reference_array,
            sync_direction=DataColumn.SyncDirections.UPSCALE,
            sync_indices=pyarrow.array(
                [0, 1, 1],
                type=pyarrow.int64(),
            ),
        )

        assert result is reference_array

    def test_upscale(
        self,
        daily_master_clock,
    ):
        result = DataColumn._calculate_master_synced_array(
            consolidation_method=None,
            master_clock_column=daily_master_clock,
            reference_array=pyarrow.array([10, 20]),
            sync_direction=DataColumn.SyncDirections.UPSCALE,
            sync_indices=pyarrow.array(
                [0, None, 1],
                type=pyarrow.int64(),
            ),
        )
        expected = pyarrow.array([10, None, 20])

        assert result.equals(expected)

    def test_downscale(
        self,
        daily_master_clock,
    ):
        result = DataColumn._calculate_master_synced_array(
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
            master_clock_column=daily_master_clock,
            reference_array=pyarrow.array([1, 2, 3, 4]),
            sync_direction=DataColumn.SyncDirections.DOWNSCALE,
            sync_indices=pyarrow.array(
                [0, 0, 2, None],
                type=pyarrow.int64(),
            ),
        )
        expected = pyarrow.array([3, None, 3])

        assert result.equals(expected)

    def test_downscale_without_consolidation_method(
        self,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_master_synced_array(
                consolidation_method=None,
                master_clock_column=daily_master_clock,
                reference_array=pyarrow.array([1, 2, 3, 4]),
                sync_direction=DataColumn.SyncDirections.DOWNSCALE,
                sync_indices=pyarrow.array(
                    [0, 0, 1, 2],
                    type=pyarrow.int64(),
                ),
            )

    @pytest.mark.parametrize(
        'consolidation_method',
        [
            sync_methods.consolidate_into_list,
            sync_methods.consolidate_into_one_row,
        ]
    )
    def test_downscale_with_invalid_consolidation_method_result(
        self,
        daily_master_clock,
        consolidation_method,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_master_synced_array(
                consolidation_method=consolidation_method,
                master_clock_column=daily_master_clock,
                reference_array=pyarrow.array([1, 2, 3, 4]),
                sync_direction=DataColumn.SyncDirections.DOWNSCALE,
                sync_indices=pyarrow.array(
                    [0, 0, 1, 2],
                    type=pyarrow.int64(),
                ),
            )


class TestPrivateCalculateOperationReferenceClock:
    def test_without_operands(self):
        result = DataColumn._calculate_operation_reference_clock()

        assert result is None

    def test_only_scalars(self):
        result = DataColumn._calculate_operation_reference_clock(
            1,
            'text',
        )

        assert result is None

    def test_only_columns_without_clock(self):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load([1, 2, 3]),
            DataColumn.load([4, 5]),
        )

        assert result is None

    def test_columns_on_master_clock(
        self,
        daily_master_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load(
                [1, 2, 3],
                reference_clock_column=daily_master_clock,
            ),
            DataColumn.load(
                [4, 5, 6],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result is daily_master_clock

    def test_columns_on_same_reference_clock(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load(
                [10, 20],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [30, 40],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result is monthly_reference_clock

    def test_reference_clock_and_column_on_it(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            monthly_reference_clock,
            DataColumn.load(
                [10, 20],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result is monthly_reference_clock

    def test_column_on_reference_clock_and_scalars(
        self,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load(
                [10, 20],
                reference_clock_column=monthly_reference_clock,
            ),
            1,
            'text',
        )

        assert result is monthly_reference_clock

    def test_columns_on_different_reference_clocks(
        self,
        daily_master_clock,
        hourly_reference_clock,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load(
                [1, 2, 3, 4],
                reference_clock_column=hourly_reference_clock,
            ),
            DataColumn.load(
                [10, 20],
                reference_clock_column=monthly_reference_clock,
            ),
        )

        assert result is daily_master_clock

    def test_columns_on_reference_clock_and_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load(
                [10, 20],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load(
                [1, 2, 3],
                reference_clock_column=daily_master_clock,
            ),
        )

        assert result is daily_master_clock

    def test_column_on_reference_clock_and_column_without_clock_matching_master_length(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_operation_reference_clock(
            DataColumn.load(
                [10, 20],
                reference_clock_column=monthly_reference_clock,
            ),
            DataColumn.load([1, 2, 3]),
        )

        assert result is daily_master_clock

    def test_column_on_reference_clock_and_column_without_clock_matching_reference_length(
        self,
        monthly_reference_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_operation_reference_clock(
                DataColumn.load(
                    [10, 20],
                    reference_clock_column=monthly_reference_clock,
                ),
                DataColumn.load([1, 2]),
            )

    def test_columns_on_different_master_clocks(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_operation_reference_clock(
                DataColumn.load(
                    [1, 2, 3],
                    reference_clock_column=daily_master_clock,
                ),
                DataColumn.load(
                    [4, 5, 6],
                    reference_clock_column=other_daily_master_clock,
                ),
            )


class TestPrivateCalculateSpanSeconds:
    @pytest.mark.parametrize(
        (
            'period',
            'periods_per_row',
            'expected',
        ),
        [
            (ReferencePeriods.SECOND, 1, 1),
            (ReferencePeriods.MINUTE, 2, 120),
            (ReferencePeriods.HOUR, 1, 3_600),
            (ReferencePeriods.DAY, 2, 172_800),
            (ReferencePeriods.WEEK, 1, 604_800),
        ]
    )
    def test_fixed_period(
        self,
        period,
        periods_per_row,
        expected,
    ):
        result = DataColumn._calculate_span_seconds(
            month_days=31,
            period=period,
            periods_per_row=periods_per_row,
        )

        assert result == expected

    @pytest.mark.parametrize(
        (
            'month_days',
            'period',
            'periods_per_row',
            'expected',
        ),
        [
            # 28 days
            (28, ReferencePeriods.MONTH, 1, 2_419_200),
            # 93 days
            (31, ReferencePeriods.MONTH, 3, 8_035_200),
            # 360 days
            (30, ReferencePeriods.YEAR, 1, 31_104_000),
        ]
    )
    def test_calendar_period(
        self,
        month_days,
        period,
        periods_per_row,
        expected,
    ):
        result = DataColumn._calculate_span_seconds(
            month_days=month_days,
            period=period,
            periods_per_row=periods_per_row,
        )

        assert result == expected


class TestPrivateCalculateSyncDirection:
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
    def test_orderable_spans(
        self,
        master_period,
        master_periods_per_row,
        reference_period,
        reference_periods_per_row,
        expected,
    ):
        result = DataColumn._calculate_sync_direction(
            master_period=master_period,
            master_periods_per_row=master_periods_per_row,
            reference_period=reference_period,
            reference_periods_per_row=reference_periods_per_row,
        )

        assert result is expected

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
    def test_unorderable_spans(
        self,
        master_period,
        master_periods_per_row,
        reference_period,
        reference_periods_per_row,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._calculate_sync_direction(
                master_period=master_period,
                master_periods_per_row=master_periods_per_row,
                reference_period=reference_period,
                reference_periods_per_row=reference_periods_per_row,
            )


class TestPrivateCalculateSyncIndices:
    def test_upscale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        result = DataColumn._calculate_sync_indices(
            clock_downscale_method=None,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            master_clock_column=daily_master_clock,
            reference_clock_column=monthly_reference_clock,
            sync_direction=DataColumn.SyncDirections.UPSCALE,
        )
        expected = pyarrow.array(
            [0, 1, 1],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_downscale(
        self,
        daily_master_clock,
        hourly_reference_clock,
    ):
        result = DataColumn._calculate_sync_indices(
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_upscale_method=None,
            master_clock_column=daily_master_clock,
            reference_clock_column=hourly_reference_clock,
            sync_direction=DataColumn.SyncDirections.DOWNSCALE,
        )
        expected = pyarrow.array(
            [0, 0, 1, 2],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_upscale_without_upscale_method(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_sync_indices(
                clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
                clock_upscale_method=None,
                master_clock_column=daily_master_clock,
                reference_clock_column=monthly_reference_clock,
                sync_direction=DataColumn.SyncDirections.UPSCALE,
            )

    def test_downscale_without_downscale_method(
        self,
        daily_master_clock,
        hourly_reference_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_sync_indices(
                clock_downscale_method=None,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                master_clock_column=daily_master_clock,
                reference_clock_column=hourly_reference_clock,
                sync_direction=DataColumn.SyncDirections.DOWNSCALE,
            )

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
        monthly_reference_clock,
        clock_upscale_method,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_sync_indices(
                clock_downscale_method=None,
                clock_upscale_method=clock_upscale_method,
                master_clock_column=daily_master_clock,
                reference_clock_column=monthly_reference_clock,
                sync_direction=DataColumn.SyncDirections.UPSCALE,
            )

    def test_invalid_downscale_method_result(
        self,
        daily_master_clock,
        hourly_reference_clock,
    ):
        with pytest.raises(DataColumnSyncError):
            DataColumn._calculate_sync_indices(
                clock_downscale_method=sync_methods.match_into_one_row,
                clock_upscale_method=None,
                master_clock_column=daily_master_clock,
                reference_clock_column=hourly_reference_clock,
                sync_direction=DataColumn.SyncDirections.DOWNSCALE,
            )


class TestPrivateGetOperationArray:
    def test_scalar(
        self,
        monthly_reference_clock,
    ):
        scalar = pyarrow.scalar(5)
        result = DataColumn._get_operation_array(
            scalar,
            monthly_reference_clock,
        )

        assert result is scalar

    def test_column_without_clock_in_operation_without_clock(self):
        column = DataColumn.load([1, 2, 3])
        result = DataColumn._get_operation_array(
            column,
            None,
        )

        assert result is column.reference_array

    def test_column_at_operation_scale(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        result = DataColumn._get_operation_array(
            column,
            monthly_reference_clock,
        )

        assert result is column.reference_array

    def test_column_synced_to_operation_scale(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        result = DataColumn._get_operation_array(
            column,
            daily_master_clock,
        )
        expected = pyarrow.array([10, 20, 20])

        assert result.equals(expected)


class TestPrivateMaskDualArrayNulls:
    def test_both_with_nulls(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'),    None,   Decimal('3.3'), Decimal('4.1'), None,           None,   Decimal('5.5')]
        )
        float_array = pyarrow.array(
            [None,              2.2,    None,           4.4,            float('nan'),   6.6,    float('nan')]
        )
        result = DataColumn._mask_dual_array_nulls(decimal_array, float_array)
        expected = pyarrow.array(
            [True,              True,   True,           False,          True,           True,   True]
        )

        assert result.equals(expected)

    def test_second_with_nulls(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'),    Decimal('2'),   Decimal('3.3'), Decimal('4.1')]
        )
        float_array = pyarrow.array(
            [None,              2.2,            None,           4.4]
        )
        result = DataColumn._mask_dual_array_nulls(decimal_array, float_array)
        expected = pyarrow.array(
            [True,              False,          True,           False]
        )

        assert result.equals(expected)


class TestPrivateMaskZeroes:
    def test_decimal(self):
        array = pyarrow.array(
            [Decimal('0'),  Decimal('1'),   Decimal('2'),   Decimal('0'),   Decimal('3'),   Decimal('0')]
        )
        mask = DataColumn._mask_zeroes(
            array.cast(
                pyarrow.float64()
            )
        )
        expected = [True,   False,          False,          True,           False,          True]

        assert mask.tolist() == expected

    def test_float(self):
        array = pyarrow.array(
            [0.0,   1.0,    2.0,    0.0,    3.0,    0.0,    0.0000000000001],
            type=pyarrow.float64()
        )
        mask = DataColumn._mask_zeroes(array)
        expected = [True, False, False, True,   False,  True,   False]

        assert mask.tolist() == expected

    def test_int(self):
        array = pyarrow.array(
            [0,     1,      2,      0,   3,     0],
            type=pyarrow.int64()
        )
        mask = DataColumn._mask_zeroes(array)
        expected = [True, False, False, True, False, True]

        assert mask.tolist() == expected

    def test_with_excessive_decimals(self):
        column1 = pyarrow.array(
            [Decimal('0')],
            type=pyarrow.decimal128(33, 23)
        )
        mask = DataColumn._mask_zeroes(column1)
        result = [True]

        assert DataColumn.fully_equal(
            DataColumn.load(mask),
            DataColumn.load(result),
            equal_nulls=True
        )


# noinspection DuplicatedCode
class TestPrivateReturnNullColumnOnNullOperand:
    def test_null_self_decimal_operand(self):
        null_array = pyarrow.array(
            [None,      None,           None, None],
            type=pyarrow.null()
        )
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        null_column = DataColumn(null_array)
        decimal_column = DataColumn(decimal_array)
        output = null_column._return_null_column_on_null_operand(decimal_column)
        expected = pyarrow.nulls(
            len(null_column)
        )

        assert output.array.equals(expected)

    def test_decimal_self_null_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        null_array = pyarrow.array(
            [None,      None,           None, None],
            type=pyarrow.null()
        )
        decimal_column = DataColumn(decimal_array)
        null_column = DataColumn(null_array)
        output = decimal_column._return_null_column_on_null_operand(null_column)
        expected = pyarrow.nulls(
            len(decimal_column)
        )

        assert output.array.equals(expected)

    def test_null_self_null_operand(self):
        null_array = pyarrow.array(
            [None, None, None, None],
            type=pyarrow.null()
        )
        null_column = DataColumn(null_array)
        null_array = DataColumn(null_array)
        output = null_column._return_null_column_on_null_operand(null_array)
        expected = pyarrow.nulls(
            len(null_column)
        )

        assert output.array.equals(expected)

    def test_decimal_self_none_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        decimal_column = DataColumn(decimal_array)
        output = decimal_column._return_null_column_on_null_operand(None)
        expected = pyarrow.nulls(
            len(decimal_column)
        )

        assert output.array.equals(expected)

    def test_decimal_self_null_scalar_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), Decimal('2.2'), None, Decimal('3.3')]
        )
        null_scalar = pyarrow.scalar(
            None,
            type=pyarrow.null()
        )
        decimal_column = DataColumn(decimal_array)
        output = decimal_column._return_null_column_on_null_operand(null_scalar)
        expected = pyarrow.nulls(
            len(decimal_column)
        )

        assert output.array.equals(expected)

    def test_decimal_self_float_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'),    None,   Decimal('3.3'), Decimal('4.1'), None,           None,   Decimal('5.5')]
        )
        float_array = pyarrow.array(
            [None,              2.2,    None,           4.4,            float('nan'),   6.6,    float('nan')]
        )
        decimal_column = DataColumn(decimal_array)
        float_column = DataColumn(float_array)
        result = decimal_column._return_null_column_on_null_operand(float_column)

        assert result is None

    def test_decimal_self_scalar_int_operand(self):
        decimal_array = pyarrow.array(
            [Decimal('1.1'), None, Decimal('3.3'), Decimal('4.1'), None, None, Decimal('5.5')]
        )
        decimal_column = DataColumn(decimal_array)
        result = decimal_column._return_null_column_on_null_operand(2)

        assert result is None

    def test_null_self_on_reference_clock_and_operand_on_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        null_column = DataColumn(
            pyarrow.nulls(2),
            reference_clock_column=monthly_reference_clock,
        )
        operand = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        output = null_column._return_null_column_on_null_operand(operand)

        assert output.reference_clock_column is daily_master_clock

    def test_null_operand_on_reference_clock_and_self_on_master_clock(
        self,
        daily_master_clock,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        null_operand = DataColumn(
            pyarrow.nulls(2),
            reference_clock_column=monthly_reference_clock,
        )
        output = column._return_null_column_on_null_operand(null_operand)

        assert output.reference_clock_column is daily_master_clock

    def test_null_operand_on_other_master_clock(
        self,
        daily_master_clock,
        other_daily_master_clock,
    ):
        column = DataColumn.load(
            [1, 2, 3],
            reference_clock_column=daily_master_clock,
        )
        null_operand = DataColumn(
            pyarrow.nulls(3),
            reference_clock_column=other_daily_master_clock,
        )

        with pytest.raises(DataColumnSyncError):
            column._return_null_column_on_null_operand(null_operand)

    def test_null_scalar_operand_with_self_on_reference_clock(
        self,
        monthly_reference_clock,
    ):
        column = DataColumn.load(
            [10, 20],
            reference_clock_column=monthly_reference_clock,
        )
        output = column._return_null_column_on_null_operand(None)

        assert output.reference_clock_column is monthly_reference_clock


class TestPrivateReplaceArrayMaskWithNones:
    def test_mixed(self):
        array = pyarrow.array(
            [1,     2,      3,      4,      5]
        )
        mask = pyarrow.array(
            [True, False,   True,   False,  True],
        )
        # noinspection PyTypeChecker
        result = DataColumn._replace_array_mask_with_nones(array, mask)
        expected = pyarrow.array(
            [None, 2,       None,   4,      None]
        )

        assert result.equals(expected)

    def test_with_existing_nulls(self):
        array = pyarrow.array(
            [1,     None, 3,    None,   5]
        )
        mask = pyarrow.array(
            [False, True, True, False,  True]
        )
        # noinspection PyTypeChecker
        result = DataColumn._replace_array_mask_with_nones(array, mask)
        expected = pyarrow.array(
            [1,     None, None, None,   None]
        )

        assert result.equals(expected)


class TestPrivateValidateClockArray:
    def test_non_clock_with_non_temporal_array(self):
        result = DataColumn._validate_clock_array(
            array=pyarrow.array([1, 2, 3]),
            clock_master=None,
        )

        assert result is None

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
        result = DataColumn._validate_clock_array(
            array=array,
            clock_master=True,
        )

        assert result is None

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
        result = DataColumn._validate_clock_array(
            array=array,
            clock_master=daily_master_clock,
        )

        assert result is None

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
            DataColumn._validate_clock_array(
                array=array,
                clock_master=True,
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
            DataColumn._validate_clock_array(
                array=array,
                clock_master=daily_master_clock,
            )

    def test_master_clock_with_null_timestamp(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_array(
                array=pyarrow.array(
                    [
                        datetime.date(2024, 1, 30),
                        None,
                        datetime.date(2024, 2, 1),
                    ]
                ),
                clock_master=True,
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
            DataColumn._validate_clock_array(
                array=array,
                clock_master=True,
            )


class TestPrivateValidateClockParameters:
    def test_non_clock_without_clock(self):
        result = DataColumn._validate_clock_parameters(
            array=pyarrow.array([1, 2, 3]),
            clock_downscale_method=None,
            clock_master=None,
            clock_periods_per_row=None,
            clock_reference_period=None,
            clock_upscale_method=None,
            consolidation_method=None,
            reference_clock_column=None,
        )

        assert result is None

    def test_non_clock_without_clock_with_consolidation_method(self):
        result = DataColumn._validate_clock_parameters(
            array=pyarrow.array([1, 2, 3]),
            clock_downscale_method=None,
            clock_master=None,
            clock_periods_per_row=None,
            clock_reference_period=None,
            clock_upscale_method=None,
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
            reference_clock_column=None,
        )

        assert result is None

    def test_non_clock_on_clock(
        self,
        daily_master_clock,
    ):
        result = DataColumn._validate_clock_parameters(
            array=pyarrow.array([1, 2, 3]),
            clock_downscale_method=None,
            clock_master=None,
            clock_periods_per_row=None,
            clock_reference_period=None,
            clock_upscale_method=None,
            consolidation_method=clock_sync_methods.ConsolidationMethods.max,
            reference_clock_column=daily_master_clock,
        )

        assert result is None

    def test_master_clock(
        self,
        daily_dates_array,
    ):
        result = DataColumn._validate_clock_parameters(
            array=daily_dates_array,
            clock_downscale_method=None,
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
            clock_upscale_method=None,
            consolidation_method=None,
            reference_clock_column=None,
        )

        assert result is None

    def test_reference_clock_with_all_sync_methods(
        self,
        daily_master_clock,
        monthly_dates_array,
    ):
        result = DataColumn._validate_clock_parameters(
            array=monthly_dates_array,
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_master=daily_master_clock,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
            reference_clock_column=None,
        )

        assert result is None

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
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=None,
                clock_master=clock_master,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=None,
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
            DataColumn._validate_clock_parameters(
                array=pyarrow.array([1, 2, 3]),
                clock_downscale_method=None,
                clock_master=None,
                clock_periods_per_row=None,
                clock_reference_period=None,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=reference_clock_column,
            )

    @pytest.mark.parametrize(
        (
            'clock_downscale_method',
            'clock_upscale_method',
            'consolidation_method',
        ),
        [
            ('max', None, None),
            (None, 'max', None),
            (None, None, 'max'),
        ]
    )
    def test_non_callable_method(
        self,
        daily_dates_array,
        clock_downscale_method,
        clock_upscale_method,
        consolidation_method,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=clock_downscale_method,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
                clock_upscale_method=clock_upscale_method,
                consolidation_method=consolidation_method,
                reference_clock_column=None,
            )

    def test_master_clock_with_reference_clock_column(
        self,
        daily_dates_array,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=None,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.DAY,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=daily_master_clock,
            )

    def test_reference_clock_with_reference_clock_column(
        self,
        daily_dates_array,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=None,
                clock_master=daily_master_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.MONTH,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=None,
                reference_clock_column=daily_master_clock,
            )

    def test_clock_master_pointing_to_reference_clock(
        self,
        monthly_reference_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=pyarrow.array(
                    [
                        datetime.date(2023, 12, 31),
                    ]
                ),
                clock_downscale_method=None,
                clock_master=monthly_reference_clock,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.YEAR,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=None,
                reference_clock_column=None,
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
            DataColumn._validate_clock_parameters(
                array=monthly_dates_array,
                clock_downscale_method=None,
                clock_master=non_clock_column,
                clock_periods_per_row=1,
                clock_reference_period=ReferencePeriods.MONTH,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=None,
                reference_clock_column=None,
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
            DataColumn._validate_clock_parameters(
                array=pyarrow.array([4, 5, 6]),
                clock_downscale_method=None,
                clock_master=None,
                clock_periods_per_row=None,
                clock_reference_period=None,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=non_clock_column,
            )

    def test_array_length_not_matching_reference_clock(
        self,
        daily_master_clock,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=pyarrow.array([1, 2]),
                clock_downscale_method=None,
                clock_master=None,
                clock_periods_per_row=None,
                clock_reference_period=None,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=daily_master_clock,
            )

    @pytest.mark.parametrize(
        (
            'clock_periods_per_row',
            'clock_reference_period',
        ),
        [
            (1, None),
            (None, ReferencePeriods.DAY),
            (None, None),
        ]
    )
    def test_master_clock_without_both_periods(
        self,
        daily_dates_array,
        clock_periods_per_row,
        clock_reference_period,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=None,
                clock_master=True,
                clock_periods_per_row=clock_periods_per_row,
                clock_reference_period=clock_reference_period,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=None,
            )

    @pytest.mark.parametrize(
        (
            'clock_periods_per_row',
            'clock_reference_period',
        ),
        [
            (1, None),
            (None, ReferencePeriods.MONTH),
            (None, None),
        ]
    )
    def test_reference_clock_without_both_periods(
        self,
        daily_master_clock,
        monthly_dates_array,
        clock_periods_per_row,
        clock_reference_period,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=monthly_dates_array,
                clock_downscale_method=None,
                clock_master=daily_master_clock,
                clock_periods_per_row=clock_periods_per_row,
                clock_reference_period=clock_reference_period,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=None,
                reference_clock_column=None,
            )

    @pytest.mark.parametrize(
        (
            'clock_periods_per_row',
            'clock_reference_period',
        ),
        [
            (1, None),
            (None, ReferencePeriods.DAY),
            (1, ReferencePeriods.DAY),
        ]
    )
    def test_non_clock_with_clock_periods(
        self,
        clock_periods_per_row,
        clock_reference_period,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=pyarrow.array([1, 2, 3]),
                clock_downscale_method=None,
                clock_master=None,
                clock_periods_per_row=clock_periods_per_row,
                clock_reference_period=clock_reference_period,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=None,
            )

    @pytest.mark.parametrize(
        (
            'clock_downscale_method',
            'clock_upscale_method',
        ),
        [
            (clock_sync_methods.DownscaleMethods.calendar_periods, None),
            (None, clock_sync_methods.UpscaleMethods.infill),
        ]
    )
    def test_non_clock_with_clock_sync_method(
        self,
        daily_master_clock,
        clock_downscale_method,
        clock_upscale_method,
    ):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_clock_parameters(
                array=pyarrow.array([1, 2, 3]),
                clock_downscale_method=clock_downscale_method,
                clock_master=None,
                clock_periods_per_row=None,
                clock_reference_period=None,
                clock_upscale_method=clock_upscale_method,
                consolidation_method=None,
                reference_clock_column=daily_master_clock,
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
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=None,
                clock_master=True,
                clock_periods_per_row=1,
                clock_reference_period=clock_reference_period,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=None,
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
            DataColumn._validate_clock_parameters(
                array=daily_dates_array,
                clock_downscale_method=None,
                clock_master=True,
                clock_periods_per_row=clock_periods_per_row,
                clock_reference_period=ReferencePeriods.DAY,
                clock_upscale_method=None,
                consolidation_method=None,
                reference_clock_column=None,
            )


class TestPrivateValidateSyncMethods:
    def test_without_sync_direction(self):
        result = DataColumn._validate_sync_methods(
            clock_downscale_method=None,
            clock_upscale_method=None,
            consolidation_method=None,
            sync_direction=None,
        )

        assert result is None

    def test_upscale_with_only_upscale_method(self):
        result = DataColumn._validate_sync_methods(
            clock_downscale_method=None,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
            consolidation_method=None,
            sync_direction=DataColumn.SyncDirections.UPSCALE,
        )

        assert result is None

    def test_downscale_with_only_downscale_and_consolidation_methods(self):
        result = DataColumn._validate_sync_methods(
            clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
            clock_upscale_method=None,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
            sync_direction=DataColumn.SyncDirections.DOWNSCALE,
        )

        assert result is None

    def test_upscale_without_upscale_method(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_sync_methods(
                clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
                clock_upscale_method=None,
                consolidation_method=clock_sync_methods.ConsolidationMethods.last,
                sync_direction=DataColumn.SyncDirections.UPSCALE,
            )

    def test_downscale_without_downscale_method(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_sync_methods(
                clock_downscale_method=None,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=clock_sync_methods.ConsolidationMethods.last,
                sync_direction=DataColumn.SyncDirections.DOWNSCALE,
            )

    def test_downscale_without_consolidation_method(self):
        with pytest.raises(DataColumnParameterError):
            DataColumn._validate_sync_methods(
                clock_downscale_method=clock_sync_methods.DownscaleMethods.calendar_periods,
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
                consolidation_method=None,
                sync_direction=DataColumn.SyncDirections.DOWNSCALE,
            )
