import datetime
import enum
import pathlib

import pyarrow
import pytest

from kaxanuk.data_curator.exceptions import (
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderMultiEndpointCommonDataOrderError,
    DataProviderToolkitRuntimeError,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataProviderToolkit

from .fixtures import (
    endpoint_maps,
    entity_tables,
)


@pytest.fixture
def endpoints_fundamental_example():
    class Endpoints(enum.StrEnum):
        BALANCE_SHEET_STATEMENT = 'balance-sheet-statement'
        CASH_FLOW_STATEMENT = 'cash-flow-statement'
        INCOME_STATEMENT = 'income-statement'

    return Endpoints


def check_if_table_preserves_row_order(
    merged_table: pyarrow.Table,
    original_table: pyarrow.Table,
) -> bool:
    """
        Check if merged_table preserves the row order of original_table,
        ignoring rows that are not in the original table.

        Args:
            original_table: The original PyArrow table
            merged_table: The merged PyArrow table

        Returns:
            True if row order is preserved, False otherwise
        """
    # Get all column names for comparison
    columns = original_table.column_names

    # Track the last found index in merged table
    last_found_index = -1

    # Iterate through each row in the original table
    for i in range(original_table.num_rows):
        # Create a filter to find this row in the merged table
        # We'll compare all columns to ensure we find the exact row
        filter_mask = None

        for col_name in columns:
            original_value = original_table[col_name][i]

            # Handle null values specially
            if original_value.is_valid:
                col_comparison = pyarrow.compute.equal(merged_table[col_name], original_value)
            else:
                col_comparison = pyarrow.compute.is_null(merged_table[col_name])

            filter_mask = (
                col_comparison if filter_mask is None
                else pyarrow.compute.and_(filter_mask, col_comparison)
            )

        # Find indices where the row matches
        matching_indices = pyarrow.compute.filter(
            pyarrow.array(range(merged_table.num_rows)),
            filter_mask
        )

        if len(matching_indices) == 0:
            # Row from original table not found in merged table
            return False

        # Get the first (should be only) matching index
        current_index = matching_indices[0].as_py()

        # Check if this index comes after the previous one
        if current_index <= last_found_index:
            return False

        last_found_index = current_index

    return True


def reverse_pyarrow_array(array: pyarrow.Array):
    indices = pyarrow.array(reversed(range(len(array))))
    return array.take(indices)


class TestTestCheckIfTablePreservesRowOrder:
    def test_check_if_table_preserves_row_order_1(self):
        test1 = check_if_table_preserves_row_order(
            entity_tables.COMPOUND_KEY_MERGED_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
        )

        assert test1

    def test_check_if_table_preserves_row_order_2(self):
        test2 = check_if_table_preserves_row_order(
            entity_tables.COMPOUND_KEY_MERGED_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
        )

        assert test2


class TestTestReversePyarrowArray:
    def test_reverse_pyarrow_array(self):
        array = pyarrow.array([3, 1, 5, 7])
        expected = pyarrow.array([7, 5, 1, 3])
        result = reverse_pyarrow_array(
            array
        )

        assert result.equals(expected)


class TestConsolidateProcessedEndpointTables:
    def test_consolidate_processed_endpoint_tables(self):
        result = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_CONSISTENT,
            table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
        )
        expected = entity_tables.CONSOLIDATED_TABLE

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_reversed(self):
        result = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_CONSISTENT_REVERSED,
            table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date],
            predominant_order_descending=True
        )
        expected = entity_tables.CONSOLIDATED_TABLE_REVERSED

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_single(self):
        result = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_SINGLE,
            table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
        )
        expected = entity_tables.ENDPOINT_TABLES_SINGLE[
            entity_tables.Endpoints.BALANCE_SHEET_STATEMENT
        ]

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_inconsistent_tables(self):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_INCONSISTENT,
                table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
            )

    def test_consolidate_processed_endpoint_tables_inconsistent_tables_with_none(self):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=entity_tables.ENDPOINT_TABLES_INCONSISTENT_WITH_NONE,
                table_merge_fields=[entity_tables.MiniFundamentalRow.filing_date]
            )


class TestFindCommonTableMissingRowsMask:
    def test_find_common_table_missing_rows_mask(self):
        common_rows_table = pyarrow.table({
            'date': pyarrow.array([
                datetime.date.fromisoformat('2021-01-01'),
                datetime.date.fromisoformat('2021-01-02'),
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-05'),
                datetime.date.fromisoformat('2021-01-06'),
            ]),
            'value': pyarrow.array([10, 20, 30, None, 50, 60]),
        })
        subset_rows_table = pyarrow.table({
            'date': pyarrow.array([
                datetime.date.fromisoformat('2021-01-02'),  # from common (index 1)
                datetime.date.fromisoformat('2021-01-04'),  # from common (index 3)
                datetime.date.fromisoformat('2021-01-06'),  # from common (index 5)
                datetime.date.fromisoformat('2021-01-07'),  # not in common
            ]),
            'value': pyarrow.array([20, None, 60, 70]),
        })
        result = DataProviderToolkit.find_common_table_missing_rows_mask(
            common_rows_table=common_rows_table,
            subset_rows_table=subset_rows_table,
        )
        expected = pyarrow.array([True, False, True, False, True, False])

        assert result.equals(expected)

    def test_find_common_table_missing_rows_mask_empty_subset(self):
        commmon_rows_table = pyarrow.table({
            'date': pyarrow.array([
                datetime.date.fromisoformat('2021-01-02'),  # from common (index 1)
                datetime.date.fromisoformat('2021-01-04'),  # from common (index 3)
                datetime.date.fromisoformat('2021-01-06'),  # from common (index 5)
                datetime.date.fromisoformat('2021-01-07'),  # not in common
            ]),
            'value': pyarrow.array([20, None, 60, 70]),
        })
        subset_rows_table = pyarrow.table({
            'date': pyarrow.array([]),
            'value': pyarrow.array([]),
        })
        result = DataProviderToolkit.find_common_table_missing_rows_mask(
            common_rows_table=commmon_rows_table,
            subset_rows_table=subset_rows_table,
        )
        expected = pyarrow.array([True, True, True, True])

        assert result.equals(expected)

    def test_find_common_table_missing_rows_mask_empty_common(self):
        commmon_rows_table = pyarrow.table({
            'date': pyarrow.array([]),
            'value': pyarrow.array([]),
        })
        subset_rows_table = pyarrow.table({
            'date': pyarrow.array([
                datetime.date.fromisoformat('2021-01-02'),  # from common (index 1)
                datetime.date.fromisoformat('2021-01-04'),  # from common (index 3)
                datetime.date.fromisoformat('2021-01-06'),  # from common (index 5)
                datetime.date.fromisoformat('2021-01-07'),  # not in common
            ]),
            'value': pyarrow.array([20, None, 60, 70]),
        })
        result = DataProviderToolkit.find_common_table_missing_rows_mask(
            common_rows_table=commmon_rows_table,
            subset_rows_table=subset_rows_table,
        )

        assert result is None


class TestPrivateCalculateEndpointColumnRemaps:
    def test_calculate_endpoint_column_remaps(self):
        result = DataProviderToolkit._calculate_endpoint_column_remaps(
            endpoint_maps.ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS

        assert result == expected


class TestPrivateCalculateEndpointFieldPreprocessors:
    def test_calculate_endpoint_field_preprocessors(self):
        result = DataProviderToolkit._calculate_endpoint_field_preprocessors(
            endpoint_maps.ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS

        assert result == expected


class TestPrivateClearTableRowsByPrimaryKey:
    def test_clear_table_rows_by_single_primary_key(self):
        table = pyarrow.table({
            'primary_key': [1, 2, 3, 4],
            'other_column': [1, 2, 3, 4]
        })
        primary_keys_table = pyarrow.table({
            'primary_key': [1, 3],
        })
        result = DataProviderToolkit._clear_table_rows_by_primary_key(
            table,
            primary_keys_table,
            ['primary_key']
        )
        expected = pyarrow.table({
            'primary_key': [1, 2, 3, 4],
            'other_column': [None, 2, None, 4]
        })

        assert result.equals(expected)

    def test_clear_table_rows_by_multiple_primary_keys_with_none(self):
        table = pyarrow.table({
            'primary_key1': [
                datetime.date.fromisoformat('2021-01-03'),  # to clear
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-04'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
            ],
            'primary_key2': [1, None, 1, 3, 1, 4],
            'other_column1': [1, 2, 3, 4, 5, 6],
            'other_column2': [7, 7, None, None, 9, 6],
        })
        primary_keys_table = pyarrow.table({
            'primary_key1': [
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-05'),
                datetime.date.fromisoformat('2021-01-05'),
            ],
            'primary_key2': [1, 3, 1, 4],
        })
        result = DataProviderToolkit._clear_table_rows_by_primary_key(
            table,
            primary_keys_table,
            ['primary_key1', 'primary_key2']
        )
        expected = pyarrow.table({
            'primary_key1': [
                datetime.date.fromisoformat('2021-01-03'),  # to clear
                datetime.date.fromisoformat('2021-01-03'),
                datetime.date.fromisoformat('2021-01-04'),
                datetime.date.fromisoformat('2021-01-04'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
                datetime.date.fromisoformat('2021-01-05'),  # to clear
            ],
            'primary_key2': [1, None, 1, 3, 1, 4],
            'other_column1': [None, 2, 3, None, None, None],
            'other_column2': [None, 7, None, None, None, None],
        })

        assert result.equals(expected)

    def test_clear_table_rows_by_primary_keys_fails_on_discrepant_columns(self):
        table = pyarrow.table({
            'primary_key': [1, 2, 3, 4],
            'other_column': [1, 2, 3, 4]
        })
        primary_keys_table = pyarrow.table({
            'missing_primary_key': [1, 3],
        })

        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._clear_table_rows_by_primary_key(
                table,
                primary_keys_table,
                ['primary_key']
            )


class TestPrivateCreateTableFromJsonString:
    def test_create_table_from_json_string(self):
        base_dir = pathlib.Path(__file__).parent
        relative_path = f'{base_dir}/fixtures/market_daily.json'
        json = pathlib.Path(relative_path).read_text()
        result = DataProviderToolkit._create_table_from_json_string(json)
        expected = pyarrow.table({
            'symbol': pyarrow.array(['NVDA', 'NVDA']),
            'date': pyarrow.array(
                [
                    datetime.datetime.fromisoformat('2025-11-14'),
                    datetime.datetime.fromisoformat('2025-11-13')
                ],
                type=pyarrow.timestamp('s')
            ),
            'adjOpen': pyarrow.array([182.86, 191.05]),
            'adjHigh': pyarrow.array([190.68, 191.44]),
            'adjLow': pyarrow.array([180.58, 183.85]),
            'adjClose': pyarrow.array([189.42, 186.86]),
            'volume': pyarrow.array([130626834, 206750700])
            })

        assert result.equals(expected)


class TestPrivateMergeArraySubsetsPreservingOrder:
    def test_merge_single_column_keys(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            pyarrow.table({
                'date':
                    entity_tables.filing_dates,
            }),
            pyarrow.table({
                'date':
                    entity_tables.filing_dates_subset,
            }),
            pyarrow.table({
                'date':
                    entity_tables.filing_dates_shifted
            }),
        ])
        expected = pyarrow.table({
            'date': entity_tables.filing_dates_all
        })

        assert result.equals(expected)

    def test_merge_single_column_keys_descending(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order(
            [
                pyarrow.table({
                    'date':
                        reverse_pyarrow_array(entity_tables.filing_dates),
                }),
                pyarrow.table({
                    'date':
                        reverse_pyarrow_array(entity_tables.filing_dates_subset),
                }),
                pyarrow.table({
                    'date':
                        reverse_pyarrow_array(entity_tables.filing_dates_shifted)
                }),
            ],
            predominant_order_descending=True
        )
        expected = pyarrow.table({
            'date': reverse_pyarrow_array(entity_tables.filing_dates_all)
        })

        assert result.equals(expected)

    def test_merge_compound_column_keys(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_SUBSET1_TABLE,
            entity_tables.COMPOUND_KEY_SUBSET2_TABLE,
            entity_tables.COMPOUND_KEY_SUBSET3_TABLE,
        ])
        expected = entity_tables.COMPOUND_KEY_MERGED_TABLE

        assert result.equals(expected)

    def test_merge_single_subset(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_SUBSET1_TABLE
        ])
        expected = entity_tables.COMPOUND_KEY_SUBSET1_TABLE

        assert result.equals(expected)

    def test_merge_single_column_inconsistent_order_fails(self):
        with pytest.raises(DataProviderMultiEndpointCommonDataOrderError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates_inconsistent,
                }),
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates_subset,
                }),
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates_shifted
                }),
            ])

    def test_merge_key_tables_with_different_column_names_fails(self):
        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates,
                }),
                pyarrow.table({
                    'other_date':
                        entity_tables.filing_dates_subset,
                })
            ])

    def test_merge_key_tables_with_different_column_numbers_fails(self):
        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates,
                }),
                entity_tables.COMPOUND_KEY_SUBSET1_TABLE,
            ])

    def test_merge_key_tables_with_duplicate_rows_fails(self):
        with pytest.raises(DataProviderToolkitRuntimeError):
            DataProviderToolkit._merge_primary_key_subsets_preserving_order([
                entity_tables.COMPOUND_KEY_MERGED_TABLE,
                entity_tables.COMPOUND_KEY_MERGED_TABLE_DUPLICATE_ROWS
            ])

    def test_merge_nondeterministic_order_subsets_preserves_subset_order(self):
        result = DataProviderToolkit._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
        ])

        assert (    # noqa: PT018
            check_if_table_preserves_row_order(
                result,
                entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
            )
            and check_if_table_preserves_row_order(
                result,
                entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
            )
        )


class TestPrivateProcessRemappedEndpointTables:
    def test_process_remapped_endpoint_tables(self):
        result = DataProviderToolkit._process_remapped_endpoint_tables(
            endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS,
            endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_FIELD
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PROCESSED

        assert (    # noqa: PT018
            result[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT]
            )
            and result[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT]
            )
        )


class TestPrivateRemapEndpointTableColumns:
    def test_remap_endpoint_table_columns(self):
        result = DataProviderToolkit._remap_endpoint_table_columns(
            endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS,
            endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_TAG
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_FIELD

        assert (    # noqa: PT018
            result[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT]
            )
            and result[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT]
            )
        )
