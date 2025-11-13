import dataclasses
import enum
import io
import types
import typing

import networkx
import pyarrow
import pyarrow.compute
import pyarrow.json

from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    EntityBuildingTables,
)
from kaxanuk.data_curator.entities import (
    BaseDataEntity,
)
from kaxanuk.data_curator.exceptions import (
    DataProviderIncorrectMappingTypeError,
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderParsingError,
    DataProviderToolkitArgumentError,
    DataProviderToolkitRuntimeError,
)
from kaxanuk.data_curator.modules.data_column import DataColumn


type ColumnRemap = str   # new entity.field or entity.field$tag column name
type ConsolidatedFieldsTable = pyarrow.Table    # table with consolidated data from all endpoints of a data block
type Endpoint = enum.StrEnum    # identifier of a particular endpoint
type EntityField = types.MemberDescriptorType   # entity field
type PrimaryKeyTable = pyarrow.Table     # table with primary key columns for table merges
type TagName = str  # name of the data provider tag

@dataclasses.dataclass(slots=True, frozen=True)
class PreprocessedFieldMapping:
    tags: list[TagName]
    preprocessors: list[typing.Callable]

type EndpointFieldMap = dict[
    Endpoint,
    dict[
        EntityField,
        TagName | PreprocessedFieldMapping
    ]
]
type EndpointColumnRemaps = dict[
    Endpoint,
    dict[
        TagName,
        list[ColumnRemap]
    ]
]
type EndpointFieldPreprocessors = dict[
    Endpoint,
    dict[
        EntityField,
        PreprocessedFieldMapping
    ]
]
type EndpointTables = dict[
    Endpoint,
    pyarrow.Table
]
type EntityClassNameMap = dict[
    str,
    type[BaseDataEntity]
]
type EntityEndpoints = dict[
    type[BaseDataEntity],
    set[Endpoint]
]
type EntityFieldColumns = dict[
    type[BaseDataEntity],
    dict[
        EntityField,
        list[pyarrow.Array]
    ]
]
type EntityRelationMap = dict[
    type[BaseDataEntity],
    dict[
        EntityField,
        type[BaseDataEntity]
    ]
]


class DataProviderFieldPreprocessors:
    @staticmethod
    def convert_millions_to_units(column: DataColumn) -> DataColumn:
        return column * 1_000_000


class DataProviderToolkitPlugin:
    # endpoint column remaps cache
    _data_block_endpoint_column_remaps: dict[
        type[BaseDataBlock],
        EndpointColumnRemaps
    ]
    # endpoint field preprocessors cache
    _data_block_endpoint_field_preprocessors: dict[
        type[BaseDataBlock],
        EndpointFieldPreprocessors
    ]
    _data_block_entity_class_name_map: dict[
        type[BaseDataBlock],
        EntityClassNameMap
    ]

    @classmethod
    def consolidate_endpoint_tables(
        cls,
        *,
        data_block: type[BaseDataBlock],
        endpoint_field_map: EndpointFieldMap,
        endpoint_tables: EndpointTables,
        endpoint_table_merge_fields: list[EntityField],
        predominant_order_descending: bool = False,
    ) -> ConsolidatedFieldsTable:
        if not issubclass(data_block, BaseDataBlock):
            msg = f"data_block parameter needs to be a subclass of BaseDataBlock"

            raise DataProviderToolkitArgumentError(msg)

        # get map from tags to remapped columns
        if data_block not in cls._data_block_endpoint_column_remaps:
            cls._data_block_endpoint_column_remaps[data_block] = cls._calculate_endpoint_column_remaps(
            endpoint_field_map
        )
        endpoint_column_remaps = cls._data_block_endpoint_column_remaps[data_block]

        # get preprocessors
        if data_block not in cls._data_block_endpoint_field_preprocessors:
            cls._data_block_endpoint_field_preprocessors[data_block] = cls._calculate_endpoint_field_preprocessors(
            endpoint_field_map
        )
        endpoint_field_preprocessors = cls._data_block_endpoint_field_preprocessors[data_block]

        # transform table columns per tag to columns per entity.field$tag
        remapped_endpoint_tables = cls._remap_endpoint_table_columns(
            endpoint_column_remaps,
            endpoint_tables,
        )

        # run processors
        processed_endpoint_tables = cls._process_remapped_endpoint_tables(
            endpoint_field_preprocessors,
            remapped_endpoint_tables,
        )

        # fuse all tables into one
        consolidated_table = cls._consolidate_processed_endpoint_tables(
            processed_endpoint_tables,
            endpoint_table_merge_fields,
            predominant_order_descending=predominant_order_descending
        )

        return consolidated_table


    @classmethod
    def create_endpoint_tables_from_json_mapping(
        cls,
        /,
        endpoint_json_strings: dict[Endpoint, str],
    ) -> EndpointTables:
        return {
            endpoint: cls._create_table_from_json_string(json_string)
            for (endpoint, json_string) in endpoint_json_strings.items()
        }

    @classmethod
    def create_entity_tables_from_consolidated_table(
        cls,
        *,
        table: ConsolidatedFieldsTable,
        data_block: type[BaseDataBlock],
    ) -> EntityBuildingTables:
        if not issubclass(data_block, BaseDataBlock):
            msg = f"data_block parameter needs to be a subclass of BaseDataBlock"

            raise DataProviderToolkitArgumentError(msg)

        if data_block not in cls._data_block_endpoint_field_preprocessors:
            cls._data_block_entity_class_name_map[data_block] = {
                entity.__name__: entity
                for (_, entity) in data_block.prefix_to_entity_map
            }
        entity_class_name_map = cls._data_block_entity_class_name_map[data_block]

        return cls._split_consolidated_table_into_entity_tables(
            table,
            entity_class_name_map,
        )

    @staticmethod
    def _calculate_endpoint_column_remaps(
        endpoint_field_map: EndpointFieldMap
    ) -> EndpointColumnRemaps:
        endpoint_column_remaps: EndpointColumnRemaps = {}

        for (endpoint, field_mappings) in endpoint_field_map.items():
            # Initialize the tag-to-column-remaps dict for this endpoint
            if endpoint not in endpoint_column_remaps:
                endpoint_column_remaps[endpoint] = {}

            for (entity_field, mapping_value) in field_mappings.items():
                # Get the entity class and field name from the entity_field descriptor
                entity_class = entity_field.__objclass__
                entity_name = entity_class.__name__
                field_name = entity_field.__name__

                if isinstance(mapping_value, str):
                    # It's a TagName - create one "entity.field" remap
                    tag_name = mapping_value
                    column_remap = f"{entity_name}.{field_name}"
                    tag_remap = {tag_name: column_remap}

                elif isinstance(mapping_value, PreprocessedFieldMapping):
                    # It's a PreprocessedFieldMapping - create one "entity.field$tag" per tag
                    tag_remap = {
                        tag_name: f"{entity_name}.{field_name}${tag_name}"
                        for tag_name in mapping_value.tags
                    }

                else:
                    msg = f"Invalid mapping value for {endpoint}.{entity_name}.{field_name}:"

                    raise DataProviderIncorrectMappingTypeError(msg)

                for (tag_name, column_remap) in tag_remap.items():
                    if tag_name not in endpoint_column_remaps[endpoint]:
                        endpoint_column_remaps[endpoint][tag_name] = []

                    endpoint_column_remaps[endpoint][tag_name].append(column_remap)

        return endpoint_column_remaps

    @staticmethod
    def _calculate_endpoint_field_preprocessors(
        endpoint_field_map: EndpointFieldMap
    ) -> EndpointFieldPreprocessors:
        preprocessors = {
            endpoint: {
                entity_field: mapping_value
                for (entity_field, mapping_value) in field_mappings.items()
                if isinstance(mapping_value, PreprocessedFieldMapping)
            }
            for (endpoint, field_mappings) in endpoint_field_map.items()
        }

        return preprocessors

    @staticmethod
    def _consolidate_processed_endpoint_tables(
        processed_endpoint_tables: EndpointTables,
        table_merge_fields: list[EntityField],
        *,
        predominant_order_descending: bool = False,
    ) -> pyarrow.Table:
        # if single table, return it
        if not processed_endpoint_tables:
            return pyarrow.table({})

        if len(processed_endpoint_tables) == 1:
            return next(iter(processed_endpoint_tables.values()))

        # get primary key ordering using _merge_primary_key_subsets_preserving_order
        pk_col_names = [
            f"{field.__objclass__.__name__}.{field.__name__}"
            for field in table_merge_fields
        ]
        primary_key_subsets = [
            table.select(pk_col_names)
            for table in processed_endpoint_tables.values()
            if all(
                pk in table.column_names
                for pk in pk_col_names
            )
        ]
        if not primary_key_subsets:
            msg = "None of the provided tables contain the required primary key columns for merging."

            raise DataProviderToolkitRuntimeError(msg)

        merged_pk_table = DataProviderToolkitPlugin._merge_primary_key_subsets_preserving_order(
            primary_key_subsets,
            predominant_order_descending=predominant_order_descending,
        )

        order_col_name = "__order_col"
        pk_table_with_order = merged_pk_table.add_column(
            0,
            order_col_name,
            pyarrow.array(range(merged_pk_table.num_rows))
        )

        # Align all tables to the master primary key list and create validity masks in one pass
        aligned_tables = []
        validity_masks = {}
        indicator_col = '__indicator_for_validity'

        for (endpoint, original_table) in processed_endpoint_tables.items():
            has_pk_cols = all(pk in original_table.column_names for pk in pk_col_names)

            if not has_pk_cols:
                validity_masks[endpoint] = pyarrow.array(
                    [False] * len(merged_pk_table),
                    type=pyarrow.bool_()
                )
                aligned_tables.append(pyarrow.table({}))  # Empty placeholder

                continue

            # Join and sort once
            table_with_indicator = original_table.append_column(
                indicator_col,
                pyarrow.array([True] * len(original_table))
            )
            aligned_table_with_helpers = pk_table_with_order.join(
                table_with_indicator,
                keys=pk_col_names,
                join_type="left outer"
            ).sort_by(order_col_name)

            # Extract validity mask from the aligned table
            validity_masks[endpoint] = aligned_table_with_helpers[indicator_col].is_valid()

            final_cols = [
                col
                for col in aligned_table_with_helpers.column_names
                if col not in [order_col_name, indicator_col]
            ]
            aligned_tables.append(
                aligned_table_with_helpers.select(final_cols)
            )

        endpoints = list(processed_endpoint_tables.keys())

        # Build column index for efficient lookup: col_name -> list of (table_idx, has_col)
        col_to_tables = {}
        for i, table in enumerate(aligned_tables):
            for col_name in table.column_names:
                if col_name not in pk_col_names:
                    if col_name not in col_to_tables:
                        col_to_tables[col_name] = []

                    col_to_tables[col_name].append(i)

        # Check for discrepancies only on common columns between table pairs
        for (col_name, table_indices) in col_to_tables.items():
            if len(table_indices) < 2:
                # No overlap, no discrepancy possible

                continue

            # Check all pairs that share this column
            for idx in range(len(table_indices)):
                for jdx in range(idx + 1, len(table_indices)):
                    i = table_indices[idx]
                    j = table_indices[jdx]

                    endpoint1 = endpoints[i]
                    endpoint2 = endpoints[j]

                    common_rows_mask = pyarrow.compute.and_(
                        validity_masks[endpoint1],
                        validity_masks[endpoint2]
                    )

                    if not pyarrow.compute.any(common_rows_mask).as_py():
                        continue

                    col1_common = aligned_tables[i].column(col_name).filter(common_rows_mask)
                    col2_common = aligned_tables[j].column(col_name).filter(common_rows_mask)

                    are_equal = pyarrow.compute.equal(
                        col1_common,
                        col2_common
                    ).fill_null(False)
                    both_null = pyarrow.compute.and_(
                        pyarrow.compute.is_null(col1_common),
                        pyarrow.compute.is_null(col2_common)
                    )
                    no_discrepancy = pyarrow.compute.or_(are_equal, both_null)

                    if not pyarrow.compute.all(no_discrepancy).as_py():
                        msg = (
                            f"Discrepancy found in common column '{col_name}' "
                            f"between different endpoints."
                        )

                        raise DataProviderMultiEndpointCommonDataDiscrepancyError(msg)

        # Build consolidated table efficiently
        # Start with primary keys
        consolidated_columns = {
            name: merged_pk_table[name]
            for name in pk_col_names
        }

        # Group columns by field name (without entity prefix)
        field_to_full_names = {}
        all_col_names = sorted(col_to_tables.keys())

        for full_name in all_col_names:
            field = full_name.split('.', 1)[1]
            if field not in field_to_full_names:
                field_to_full_names[field] = []

            field_to_full_names[field].append(full_name)

        # Process each field group
        for (field, full_names) in sorted(field_to_full_names.items()):
            # Collect unique arrays for this field (deduplicate using id() for efficiency)
            seen_array_ids = set()
            arrays_to_coalesce = []

            for full_name in full_names:
                for table_idx in col_to_tables[full_name]:
                    arr = aligned_tables[table_idx][full_name]
                    arr_id = id(arr)

                    # Only add if not seen (by object identity first, then equality)
                    if arr_id not in seen_array_ids:
                        # Check equality against already collected arrays
                        is_duplicate = any(
                            arr.equals(unique_arr)
                            for unique_arr in arrays_to_coalesce
                        )
                        if not is_duplicate:
                            arrays_to_coalesce.append(arr)
                            seen_array_ids.add(arr_id)

            if arrays_to_coalesce:
                merged_column = pyarrow.compute.coalesce(*arrays_to_coalesce)
                for full_name in full_names:
                    consolidated_columns[full_name] = merged_column

        # Build final table with correct column order
        final_column_order = [*pk_col_names, *all_col_names]

        return pyarrow.table({
            name: consolidated_columns[name]
            for name in final_column_order
        })

    @staticmethod
    def _create_table_from_json_string(json_string: str):
        # Convert string to bytes and create a buffer
        json_bytes = json_string.encode('utf-8')
        buffer = io.BytesIO(json_bytes)

        # Read into PyArrow table
        try:
            table = pyarrow.json.read_json(
                buffer,
                pyarrow.json.ParseOptions(
                    newlines_in_values=True
                )
            )
        except pyarrow.ArrowInvalid as error:
            msg = f"Error parsing JSON string: {error}"

            raise DataProviderParsingError(msg) from error

        return table

    @staticmethod
    def _merge_primary_key_subsets_preserving_order(
        primary_key_subsets_tables: list[PrimaryKeyTable],
        predominant_order_descending: bool = False,
    ) -> PrimaryKeyTable:
        if not primary_key_subsets_tables:
            return pyarrow.table({})

        first_table = primary_key_subsets_tables[0]
        schema = first_table.schema
        column_names = schema.names

        if not column_names:
            msg = "Input tables have no columns."

            raise DataProviderToolkitRuntimeError(msg)

        graph = networkx.DiGraph()

        for table in primary_key_subsets_tables:
            if table.schema != schema:
                if len(table.column_names) != len(column_names):
                    msg = "Input tables have different number of columns."
                elif table.column_names != column_names:
                    msg = "Input tables have different column names."
                else:
                    msg = "Input tables have different column types."

                raise DataProviderToolkitRuntimeError(msg)

            if table.num_rows == 0:
                continue

            # Filter out rows where all columns are null, as they are not valid keys
            all_null_mask = pyarrow.compute.is_null(
                table[column_names[0]]
            )
            for col_name in column_names[1:]:
                all_null_mask = pyarrow.compute.and_(
                    all_null_mask,
                    pyarrow.compute.is_null(
                        table[col_name]
                    )
                )
            keep_mask = pyarrow.compute.invert(all_null_mask)
            filtered_table = table.filter(keep_mask)

            if filtered_table.num_rows == 0:
                continue

            # Check for duplicate rows in the valid key data
            if (
                filtered_table.group_by(column_names).aggregate([]).num_rows
                != filtered_table.num_rows
            ):
                msg = "Input table contains duplicate rows."

                raise DataProviderToolkitRuntimeError(msg)

            rows_as_dicts = filtered_table.to_pylist()
            rows_as_tuples = [
                tuple(
                    row[name]
                    for name in column_names
                )
                for row in rows_as_dicts
            ]
            networkx.add_path(graph, rows_as_tuples)

        if not graph.nodes:
            return pyarrow.Table.from_pylist([], schema=schema)

        try:
            if predominant_order_descending:
                # For descending order, we topologically sort the reversed graph
                # and then reverse the result. This correctly handles tie-breaking.
                sorted_rows = list(
                    reversed(
                        list(
                            networkx.lexicographical_topological_sort(
                                graph.reverse(copy=True)
                            )
                        )
                    )
                )
            else:
                sorted_rows = list(
                    networkx.lexicographical_topological_sort(graph)
                )
        except networkx.NetworkXUnfeasible:
            msg = "Inconsistent key order between tables results in a circular dependency."

            raise DataProviderMultiEndpointCommonDataDiscrepancyError(msg)

        if not sorted_rows:
            return pyarrow.Table.from_pylist([], schema=schema)

        columns_as_tuples = list(zip(*sorted_rows))
        arrays = [
            pyarrow.array(col_data, type=field.type)
            for (col_data, field) in zip(columns_as_tuples, schema)
        ]

        return pyarrow.Table.from_arrays(arrays, names=column_names)

    @staticmethod
    def _process_remapped_endpoint_tables(
        endpoint_field_preprocessors: EndpointFieldPreprocessors,
        remapped_endpoint_tables: EndpointTables,
    ) -> EndpointTables:
        processed_tables: EndpointTables = {}

        for (endpoint, table) in remapped_endpoint_tables.items():
            # Get preprocessors for this endpoint (if any)
            field_preprocessors = endpoint_field_preprocessors.get(endpoint, {})

            if not field_preprocessors:
                # No preprocessors for this endpoint, keep table as-is
                processed_tables[endpoint] = table

                continue

            # Track which columns are inputs to preprocessors (will be removed)
            columns_to_remove = set()
            # Track new processed columns to add
            new_columns = {}

            # Process each field that has preprocessors
            for (entity_field, preprocessed_mapping) in field_preprocessors.items():
                entity_class = entity_field.__objclass__
                entity_name = entity_class.__name__
                field_name = entity_field.__name__

                # Build input column names from tags: "entity.field$tag"
                input_column_names = [
                    f"{entity_name}.{field_name}${tag}"
                    for tag in preprocessed_mapping.tags
                ]

                # Mark input columns for removal
                columns_to_remove.update(input_column_names)

                # Load input columns and wrap in DataColumn.load()
                input_columns = [
                    DataColumn.load(table[col_name])
                    for col_name in input_column_names
                ]

                # Chain preprocessors
                result = input_columns
                for preprocessor in preprocessed_mapping.preprocessors:
                    # Apply preprocessor with current result(s) as positional arguments
                    if isinstance(result, list):
                        # First preprocessor gets multiple columns
                        result = preprocessor(*result)
                    else:
                        # Subsequent preprocessors get single output from previous
                        result = preprocessor(result)

                    # Wrap output in DataColumn.load() for next preprocessor
                    result = DataColumn.load(result)

                # Get final pyarrow.Array
                final_column = result.to_pyarrow()

                # Store with name "entity.field" (without $tag suffix)
                output_column_name = f"{entity_name}.{field_name}"
                new_columns[output_column_name] = final_column

            # Build the new table: keep non-processed columns + add processed columns
            result_columns_dict = {}

            # Add columns that weren't processed
            for col_name in table.column_names:
                if col_name not in columns_to_remove:
                    result_columns_dict[col_name] = table[col_name]

            # Add newly processed columns
            result_columns_dict.update(new_columns)

            processed_tables[endpoint] = pyarrow.table(result_columns_dict)

        return processed_tables

    @staticmethod
    def _remap_endpoint_table_columns(
        endpoint_column_remaps: EndpointColumnRemaps,
        endpoint_tables: EndpointTables,
    ) -> EndpointTables:
        remapped_tables: EndpointTables = {}

        for endpoint, table in endpoint_tables.items():
            if endpoint not in endpoint_column_remaps:
                # No remapping for this endpoint, keep the table as-is
                remapped_tables[endpoint] = table

                continue

            column_remaps = endpoint_column_remaps[endpoint]
            new_columns_dict = {}

            # Process each column in the original table
            for tag_name in table.column_names:
                if tag_name not in column_remaps:
                    # Column not in remaps, skip it

                    continue

                # Get the original column data
                original_column = table[tag_name]

                # Create one column for each remap target
                for new_column_name in column_remaps[tag_name]:
                    new_columns_dict[new_column_name] = original_column

            # Create the new table with remapped columns
            remapped_tables[endpoint] = pyarrow.table(new_columns_dict)

        return remapped_tables

    @staticmethod
    def _split_consolidated_table_into_entity_tables(
        table: ConsolidatedFieldsTable,
        entity_class_name_map: EntityClassNameMap,
    ) -> EntityBuildingTables:
        entity_columns: dict[type[BaseDataEntity], dict[str, pyarrow.Array]] = {}
        for column_name in table.column_names:
            try:
                entity_name, field_name = column_name.split('.', 1)
            except ValueError:
                msg = f"Invalid column name format: '{column_name}'. Expected 'Entity.field'."

                raise DataProviderIncorrectMappingTypeError(msg) from None

            entity_class = entity_class_name_map.get(entity_name)
            if not entity_class:
                msg = f"Entity '{entity_name}' from column '{column_name}' not found."

                raise DataProviderIncorrectMappingTypeError(msg)

            if not dataclasses.is_dataclass(entity_class):
                msg = f"Entity class '{entity_name}' is not a dataclass."

                raise DataProviderIncorrectMappingTypeError(msg)

            entity_fields = {f.name for f in dataclasses.fields(entity_class)}
            if field_name not in entity_fields:
                msg = (
                    f"Field '{field_name}' not found in entity '{entity_name}' "
                    f"for column '{column_name}'."
                )

                raise DataProviderIncorrectMappingTypeError(msg)

            if entity_class not in entity_columns:
                entity_columns[entity_class] = {}

            entity_columns[entity_class][field_name] = table.column(column_name)

        entity_tables: EntityBuildingTables = {
            entity_class: pyarrow.table(columns)
            for entity_class, columns in entity_columns.items()
        }

        return entity_tables
