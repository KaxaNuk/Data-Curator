"""
Clock columns for the DataColumn tests.

Each function creates new columns on every call, as DataColumns cache their synced data on first access.
"""

import datetime

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.modules.data_column import ReferencePeriods


def create_daily_master_clock() -> DataColumn:
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


def create_hourly_reference_clock(master_clock: DataColumn) -> DataColumn:
    return DataColumn.load(
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


def create_monthly_reference_clock(master_clock: DataColumn) -> DataColumn:
    return DataColumn.load(
        [
            datetime.date(2023, 12, 31),
            datetime.date(2024, 1, 31),
        ],
        clock_master=master_clock,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.MONTH,
        clock_upscale_method=clock_sync_methods.UpscaleMethods.infill,
    )
