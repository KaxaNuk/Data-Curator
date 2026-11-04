"""
Sync methods with invalid results, for the DataColumn tests checking that they get rejected.
"""

import pyarrow

from kaxanuk.data_curator import DataColumn


def consolidate_into_list(
    reference_array: pyarrow.Array,
    master_row_indices: pyarrow.Int64Array,
    master_row_count: int,
) -> list:
    return [None] * master_row_count


def consolidate_into_one_row(
    reference_array: pyarrow.Array,
    master_row_indices: pyarrow.Int64Array,
    master_row_count: int,
) -> pyarrow.Array:
    return reference_array.slice(0, 1)


def match_into_float_indices(
    master_clock_column: DataColumn,
    reference_clock_column: DataColumn,
) -> pyarrow.Array:
    return pyarrow.array(
        [0.0] * len(master_clock_column.reference_array)
    )


def match_into_list(
    master_clock_column: DataColumn,
    reference_clock_column: DataColumn,
) -> list:
    return [0] * len(master_clock_column.reference_array)


def match_into_one_row(
    master_clock_column: DataColumn,
    reference_clock_column: DataColumn,
) -> pyarrow.Array:
    return pyarrow.array(
        [0],
        type=pyarrow.int64(),
    )
