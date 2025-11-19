import datetime
import pathlib

import pyarrow
import pytest

from kaxanuk.data_curator.exceptions import (
    DataProviderIncorrectMappingTypeError,
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderToolkitRuntimeError,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataProviderToolkit
from .fixtures import (
    endpoint_maps,
    entity_columns,
    entity_tables,
)


@pytest.fixture(scope="function")
def mixin_instance():
    return DataProviderToolkit()


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

            if filter_mask is None:
                filter_mask = col_comparison
            else:
                filter_mask = pyarrow.compute.and_(filter_mask, col_comparison)

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
    def test_check_if_table_preserves_row_order(self):
        test1 = check_if_table_preserves_row_order(
            entity_tables.COMPOUND_KEY_MERGED_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
        )
        test2 = check_if_table_preserves_row_order(
            entity_tables.COMPOUND_KEY_MERGED_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
        )

        assert (
            test1
            and test2
        )


class TestTestReversePyarrowArray:
    def test_reverse_pyarrow_array(self):
        array = pyarrow.array([3, 1, 5, 7])
        expected = pyarrow.array([7, 5, 1, 3])
        result = reverse_pyarrow_array(
            array
        )

        assert result.equals(expected)


class TestPrivateCalculateEndpointColumnRemaps:
    def test_calculate_endpoint_column_remaps(self, mixin_instance):
        result = mixin_instance._calculate_endpoint_column_remaps(
            endpoint_maps.ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS

        assert result == expected


class TestPrivateCalculateEndpointFieldPreprocessors:
    def test_calculate_endpoint_field_preprocessors(self, mixin_instance):
        result = mixin_instance._calculate_endpoint_field_preprocessors(
            endpoint_maps.ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS

        assert result == expected


class TestPrivateConsolidateProcessedEndpointTables:
    def test_consolidate_processed_endpoint_tables(self, mixin_instance):
        result = mixin_instance._consolidate_processed_endpoint_tables(
            entity_tables.ENDPOINT_TABLES_CONSISTENT,
            [entity_tables.MiniFundamentalRow.filing_date]
        )
        expected = entity_tables.CONSOLIDATED_TABLE

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_single(self, mixin_instance):
        result = mixin_instance._consolidate_processed_endpoint_tables(
            entity_tables.ENDPOINT_TABLES_SINGLE,
            [entity_tables.MiniFundamentalRow.filing_date]
        )
        expected = entity_tables.ENDPOINT_TABLES_SINGLE[
            entity_tables.Endpoints.BALANCE_SHEET_STATEMENT
        ]

        assert result.equals(expected)

    def test_consolidate_processed_endpoint_tables_inconsistent_tables(self, mixin_instance):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            mixin_instance._consolidate_processed_endpoint_tables(
                entity_tables.ENDPOINT_TABLES_INCONSISTENT,
                [entity_tables.MiniFundamentalRow.filing_date]
            )

    def test_consolidate_processed_endpoint_tables_inconsistent_tables_with_none(self, mixin_instance):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            mixin_instance._consolidate_processed_endpoint_tables(
                entity_tables.ENDPOINT_TABLES_INCONSISTENT_WITH_NONE,
                [entity_tables.MiniFundamentalRow.filing_date]
            )


class TestPrivateCreateTableFromJsonString:
    def test_create_table_from_json_string(self, mixin_instance):
        base_dir = pathlib.Path(__file__).parent
        relative_path = f'{base_dir}/fixtures/market_daily.json'
        json = pathlib.Path(relative_path).read_text()
        result = mixin_instance._create_table_from_json_string(json)
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
    def test_merge_single_column_keys(self, mixin_instance):
        result = mixin_instance._merge_primary_key_subsets_preserving_order([
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

    def test_merge_single_column_keys_descending(self, mixin_instance):
        result = mixin_instance._merge_primary_key_subsets_preserving_order(
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

    def test_merge_compound_column_keys(self, mixin_instance):
        result = mixin_instance._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_SUBSET1_TABLE,
            entity_tables.COMPOUND_KEY_SUBSET2_TABLE,
            entity_tables.COMPOUND_KEY_SUBSET3_TABLE,
        ])
        expected = entity_tables.COMPOUND_KEY_MERGED_TABLE

        assert result.equals(expected)

    def test_merge_single_subset(self, mixin_instance):
        result = mixin_instance._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_SUBSET1_TABLE
        ])
        expected = entity_tables.COMPOUND_KEY_SUBSET1_TABLE

        assert result.equals(expected)

    def test_merge_single_column_inconsistent_order_fails(self, mixin_instance):
        with pytest.raises(DataProviderMultiEndpointCommonDataDiscrepancyError):
            mixin_instance._merge_primary_key_subsets_preserving_order([
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

    def test_merge_key_tables_with_different_column_names_fails(self, mixin_instance):
        with pytest.raises(DataProviderToolkitRuntimeError):
            mixin_instance._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates,
                }),
                pyarrow.table({
                    'other_date':
                        entity_tables.filing_dates_subset,
                })
            ])

    def test_merge_key_tables_with_different_column_numbers_fails(self, mixin_instance):
        with pytest.raises(DataProviderToolkitRuntimeError):
            mixin_instance._merge_primary_key_subsets_preserving_order([
                pyarrow.table({
                    'date':
                        entity_tables.filing_dates,
                }),
                entity_tables.COMPOUND_KEY_SUBSET1_TABLE,
            ])

    def test_merge_key_tables_with_duplicate_rows_fails(self, mixin_instance):
        with pytest.raises(DataProviderToolkitRuntimeError):
            mixin_instance._merge_primary_key_subsets_preserving_order([
                entity_tables.COMPOUND_KEY_MERGED_TABLE,
                entity_tables.COMPOUND_KEY_MERGED_TABLE_DUPLICATE_ROWS
            ])

    def test_merge_nondeterministic_order_subsets_preserves_subset_order(self, mixin_instance):
        result = mixin_instance._merge_primary_key_subsets_preserving_order([
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET1_TABLE,
            entity_tables.COMPOUND_KEY_NONDETERMINISTIC_SUBSET2_TABLE,
        ])

        assert (
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
    def test_process_remapped_endpoint_tables(self, mixin_instance):
        result = mixin_instance._process_remapped_endpoint_tables(
            endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_PREPROCESSORS,
            endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_FIELD
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PROCESSED

        assert (
            result[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT]
            )
            and result[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT]
            )
        )


class TestPrivateRemapEndpointTableColumns:
    def test_remap_endpoint_table_columns(self, mixin_instance):
        result = mixin_instance._remap_endpoint_table_columns(
            endpoint_maps.EXAMPLE_ENDPOINT_FIELD_MAP_MIXED_PREPROCESSOR_TAGS_COLUMN_REMAPS,
            endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_TAG
        )
        expected = endpoint_maps.EXAMPLE_ENDPOINT_TABLES_PER_FIELD

        assert (
            result[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.BALANCE_SHEET_STATEMENT]
            )
            and result[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT].equals(
                expected[endpoint_maps.Endpoints.CASH_FLOW_STATEMENT]
            )
        )
