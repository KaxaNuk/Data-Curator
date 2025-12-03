import dataclasses
import enum
import io
import re
import typing

import networkx
import pyarrow
import pyarrow.compute
import pyarrow.json

from kaxanuk.data_curator.data_blocks.base_data_block import (
    ConsolidatedFieldsTable,
    BaseDataBlock,
    EntityField,
)
from kaxanuk.data_curator.entities import (
    BaseDataEntity,
)
from kaxanuk.data_curator.exceptions import (
    DataProviderIncorrectMappingTypeError,
    DataProviderMultiEndpointCommonDataDiscrepancyError,
    DataProviderMultiEndpointCommonDataOrderError,
    DataProviderParsingError,
    DataProviderToolkitArgumentError,
    DataProviderToolkitNoDataError,
    DataProviderToolkitRuntimeError,
)
from kaxanuk.data_curator.modules.data_column import DataColumn


type ColumnRemap = str   # new entity.field or entity.field$tag column name

type Endpoint = enum.StrEnum    # identifier of a particular endpoint
# column names are either "entity.field" for primary keys, or "endpoint_name$entity.field" for specific endpoints
type EndpointDiscrepanciesTable = pyarrow.Table    # mostly for error handler use
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
type DataBlockEndpointTagMap = dict[
    type[BaseDataBlock],
    EndpointFieldMap
]
type EndpointColumnRemaps = dict[
    Endpoint,
    dict[
        TagName,
        list[ColumnRemap]
    ]
]
type DataBlockEndpointColumnRemaps = dict[
        type[BaseDataBlock],
        EndpointColumnRemaps
    ]
type EndpointFieldPreprocessors = dict[
    Endpoint,
    dict[
        EntityField,
        PreprocessedFieldMapping
    ]
]
type DataBlockEndpointFieldPreprocessors = dict[
    type[BaseDataBlock],
    EndpointFieldPreprocessors
]
type EndpointTables = dict[
    Endpoint,
    pyarrow.Table
]
type EntityClassNameMap = dict[
    str,
    type[BaseDataEntity]
]
type DataBlockEntityClassNameMap = dict[
        type[BaseDataBlock],
        EntityClassNameMap
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
type ProcessedEndpointTables = dict[    # endpoint tables that have been remapped and had the preprocessors applied
    Endpoint,
    pyarrow.Table
]


class DataProviderFieldPreprocessors:
    @staticmethod
    def convert_millions_to_units(column: DataColumn) -> DataColumn:
        """
        Convert financial values from millions to individual units.

        Takes a column containing values expressed in millions and multiplies
        each value by 1,000,000 to convert to standard units.

        Parameters
        ----------
        column
            Column containing values in millions

        Returns
        -------
        DataColumn
            Column with values converted to standard units
        """
        return column * 1_000_000

    @staticmethod
    def cast_datetime_to_date(column: DataColumn) -> DataColumn:
        """
        Cast datetime values to date type.

        Converts a column containing datetime values to date32 type,
        discarding time information.

        Parameters
        ----------
        column
            Column containing datetime values

        Returns
        -------
        DataColumn
            Column with values cast to date type
        """
        return DataColumn.load(
            column.to_pyarrow().cast(pyarrow.date32())
        )


class DataProviderToolkit:
    # endpoint column remaps cache
    _data_block_endpoint_column_remaps: typing.ClassVar[
        DataBlockEndpointColumnRemaps
    ] = {}
    # endpoint field preprocessors cache
    _data_block_endpoint_field_preprocessors: typing.ClassVar[
        DataBlockEndpointFieldPreprocessors
    ] = {}
    _data_block_entity_class_name_map: typing.ClassVar[
        DataBlockEntityClassNameMap
    ] = {}

    @classmethod
    def clear_discrepant_processed_endpoint_tables_rows(
        cls,
        *,
        discrepancy_table: EndpointDiscrepanciesTable,
        processed_endpoint_tables: ProcessedEndpointTables,
        key_column_names: list[str],
        preserved_column_names: list[str],
    ) -> EndpointTables:
        """
        Clear discrepant rows from processed endpoint tables.

        Identifies rows in processed endpoint tables that match primary keys
        in the discrepancy table and sets non-preserved column values to null
        for those rows across all endpoints.

        Parameters
        ----------
        discrepancy_table
            Table containing primary keys of discrepant rows
        processed_endpoint_tables
            Dictionary mapping endpoints to their processed tables
        key_column_names
            List of primary key column names
        preserved_column_names
            List of names of columns to preserve (not set to null)

        Returns
        -------
        EndpointTables
            Dictionary mapping endpoints to tables with discrepant rows cleared
        """
        # get table with just the primary keys
        primary_keys_table = discrepancy_table.select(key_column_names)
        # for each endpoint table
        output_tables = {}
        for (endpoint, table) in processed_endpoint_tables.items():
            output_tables[endpoint] = cls._clear_table_rows_by_primary_key(
                table=table,
                clear_rows_primary_keys=primary_keys_table,
                preserved_column_names=preserved_column_names,
            )

        return output_tables

    @staticmethod
    def consolidate_processed_endpoint_tables(
        *,
        processed_endpoint_tables: ProcessedEndpointTables,
        table_merge_fields: list[EntityField],
        predominant_order_descending: bool = False,
    ) -> ConsolidatedFieldsTable:
        """
        Consolidate multiple endpoint tables into a single unified table.

        Merges processed tables from different endpoints by their primary keys,
        preserving row order and coalescing values from different endpoints.
        Validates that common columns across endpoints have consistent values
        for shared rows.

        Parameters
        ----------
        processed_endpoint_tables
            Dictionary mapping endpoints to their processed tables
        table_merge_fields
            List of entity fields to use as primary keys for merging
        predominant_order_descending
            Whether the predominant ordering is descending

        Returns
        -------
        ConsolidatedFieldsTable
            Consolidated table containing all data from all endpoints

        Raises
        ------
        DataProviderMultiEndpointCommonDataDiscrepancyError
            When common columns have inconsistent values across endpoints
        DataProviderToolkitRuntimeError
            When no tables contain required primary key columns
        """
        # if single table, return it
        if not processed_endpoint_tables:
            return pyarrow.table({})

        if len(processed_endpoint_tables) == 1:
            return next(
                iter(processed_endpoint_tables.values())
            )

        # get primary key ordering
        key_column_names = [
            f"{field.__objclass__.__name__}.{field.__name__}"
            for field in table_merge_fields
        ]
        primary_key_subsets = [
            table.select(key_column_names)
            for table in processed_endpoint_tables.values()
            if all(
                pk in table.column_names
                for pk in key_column_names
            )
        ]
        if not primary_key_subsets:
            msg = "None of the provided tables contain the required primary key columns for merging."

            raise DataProviderToolkitRuntimeError(msg)

        merged_key_table = DataProviderToolkit._merge_primary_key_subsets_preserving_order(
            primary_key_subsets,
            predominant_order_descending=predominant_order_descending,
        )

        order_col_name = "__order_col"
        key_table_with_order = merged_key_table.add_column(
            0,
            order_col_name,
            pyarrow.array(range(merged_key_table.num_rows))
        )

        # Align all tables to the master primary key list and create validity masks in one pass
        aligned_tables = []
        validity_masks = {}
        indicator_col = '__indicator_for_validity'

        for (endpoint, original_table) in processed_endpoint_tables.items():
            has_pk_cols = all(
                key in original_table.column_names
                for key in key_column_names
            )

            if not has_pk_cols:
                validity_masks[endpoint] = pyarrow.array(
                    [False] * len(merged_key_table),
                    type=pyarrow.bool_()
                )
                aligned_tables.append(
                    pyarrow.table({})   # Empty placeholder
                )

                continue

            # Join and sort once
            table_with_indicator = original_table.append_column(
                indicator_col,
                pyarrow.array([True] * len(original_table))
            )
            aligned_table_with_helpers = key_table_with_order.join(
                table_with_indicator,
                keys=key_column_names,
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
        for (index, col_name) in (
            (i, col)
            for (i, table) in enumerate(aligned_tables)
            for col in table.column_names
            if col not in key_column_names
        ):
            if col_name not in col_to_tables:
                col_to_tables[col_name] = []

            col_to_tables[col_name].append(index)

        discrepant_columns = set()
        discrepant_rows_mask = None

        # Check for discrepancies only on common columns between table pairs
        for (col_name, table_indices) in col_to_tables.items():
            if len(table_indices) <= 1:
                # No overlap, no discrepancy possible

                continue

            # Check all pairs that share this column
            for (idx, jdx) in (
                (i, j)
                for i in range(len(table_indices))
                for j in range(i + 1, len(table_indices))
            ):
                index = table_indices[idx]
                j = table_indices[jdx]

                endpoint1 = endpoints[index]
                endpoint2 = endpoints[j]

                common_rows_mask = pyarrow.compute.and_(
                    validity_masks[endpoint1],
                    validity_masks[endpoint2]
                )

                if not pyarrow.compute.any(common_rows_mask).as_py():
                    # no common rows, no discrepancy possible

                    continue

                col1_common = aligned_tables[index].column(col_name).filter(common_rows_mask)
                col2_common = aligned_tables[j].column(col_name).filter(common_rows_mask)

                are_equal = pyarrow.compute.equal(
                    col1_common,
                    col2_common
                ).fill_null(fill_value=False)
                both_null = pyarrow.compute.and_(
                    pyarrow.compute.is_null(col1_common),
                    pyarrow.compute.is_null(col2_common)
                )
                no_discrepancy_mask = pyarrow.compute.or_(are_equal, both_null)

                if not pyarrow.compute.all(no_discrepancy_mask).as_py():
                    # handle discrepancy
                    discrepancy_mask = pyarrow.compute.invert(no_discrepancy_mask)

                    # Track this column as discrepant
                    discrepant_columns.add(col_name)

                    # Expand discrepancy_mask from common rows back to full table size
                    # Start with all False
                    full_size_discrepancy = pyarrow.array(
                        [False] * len(merged_key_table)
                    )

                    # Set to True where common_rows_mask is True AND discrepancy_mask is True
                    # replace_with_mask replaces values where mask is True with values from the replacement array
                    # Convert both masks to Array if they are ChunkedArrays
                    if isinstance(common_rows_mask, pyarrow.ChunkedArray):
                        common_rows_mask = common_rows_mask.combine_chunks()
                    if isinstance(discrepancy_mask, pyarrow.ChunkedArray):
                        discrepancy_mask = discrepancy_mask.combine_chunks()

                    full_size_discrepancy = pyarrow.compute.replace_with_mask(
                        full_size_discrepancy,
                        common_rows_mask,
                        discrepancy_mask
                    )

                    # Combine with master mask
                    if discrepant_rows_mask is None:
                        discrepant_rows_mask = full_size_discrepancy
                    else:
                        discrepant_rows_mask = pyarrow.compute.or_(
                            discrepant_rows_mask,
                            full_size_discrepancy
                        )

        # Create debug table with all discrepant rows and columns
        if discrepant_columns:
            discrepancy_table = DataProviderToolkit._calculate_common_column_discrepancies(
                discrepant_columns=discrepant_columns,
                discrepant_rows_mask=discrepant_rows_mask,
                primary_keys_table=merged_key_table,
                key_column_names=key_column_names,
                aligned_tables=aligned_tables,
                endpoints=endpoints,
            )

            raise DataProviderMultiEndpointCommonDataDiscrepancyError(
                discrepant_columns=discrepant_columns,
                discrepancies_table=discrepancy_table,
                key_column_names=key_column_names,
            )

        # Build consolidated table efficiently
        # Start with primary keys
        consolidated_columns = {
            name: merged_key_table[name]
            for name in key_column_names
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
        for (_field, full_names) in sorted(field_to_full_names.items()):
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
        final_column_order = [*key_column_names, *all_col_names]

        return pyarrow.table({
            name: consolidated_columns[name]
            for name in final_column_order
        })

    @classmethod
    def create_endpoint_tables_from_json_mapping(
        cls,
        /,
        endpoint_json_strings: dict[Endpoint, str],
    ) -> EndpointTables:
        """
        Create endpoint tables from JSON string representations.

        Parses JSON strings for each endpoint and converts them into PyArrow
        tables, handling both JSON arrays and newline-delimited JSON formats.

        Parameters
        ----------
        endpoint_json_strings
            Dictionary mapping endpoints to their JSON string data

        Returns
        -------
        EndpointTables
            Dictionary mapping endpoints to parsed PyArrow tables

        Raises
        ------
        DataProviderToolkitRuntimeError
            When JSON parsing fails for any endpoint
        """
        try:
            endpoint_tables = {
                endpoint: cls._create_table_from_json_string(json_string)
                for (endpoint, json_string) in endpoint_json_strings.items()
            }
        except DataProviderParsingError as error:
            msg = f"Failed to parse endpoint tables: {error}"

            raise DataProviderToolkitRuntimeError(msg) from error

        return endpoint_tables

    @staticmethod
    def find_common_table_missing_rows_mask(
        common_rows_table: pyarrow.Table,
        subset_rows_table: pyarrow.Table,
    ) -> pyarrow.BooleanArray | None:
        """
        Identify rows in common table that are missing from subset table.

        Performs a null-safe comparison between two tables by column position
        to determine which rows in the common table are not present in the
        subset table.

        Parameters
        ----------
        common_rows_table
            Table containing all potential rows
        subset_rows_table
            Table containing a subset of rows to check against

        Returns
        -------
        pyarrow.BooleanArray or None
            Boolean mask where True indicates missing rows, or None if
            common table is empty

        Raises
        ------
        DataProviderToolkitArgumentError
            When tables have different number of columns
        """
        if common_rows_table.num_columns != subset_rows_table.num_columns:
            msg = "Tables have different number of columns"

            raise DataProviderToolkitArgumentError(msg)

        if common_rows_table.num_rows == 0:
            return None

        column_names = common_rows_table.column_names

        if subset_rows_table.num_rows == 0:
            return pyarrow.array(
                [True] * common_rows_table.num_rows,
                type=pyarrow.bool_()
            )

        # Ensure both tables have matching schemas for the key columns
        subset_renamed = subset_rows_table.rename_columns(column_names)

        # Cast subset columns to match common_rows_table schema
        cast_columns = {}
        for col_name in column_names:
            common_col_type = common_rows_table.schema.field(col_name).type
            subset_col = subset_renamed[col_name]
            if subset_col.type != common_col_type:
                cast_columns[col_name] = subset_col.cast(common_col_type)
            else:
                cast_columns[col_name] = subset_col

        subset_with_matching_types = pyarrow.table(cast_columns)

        # Add an order column to preserve original row order
        order_col_name = "__order_col__"
        common_with_order = common_rows_table.add_column(
            0,
            order_col_name,
            pyarrow.array(range(common_rows_table.num_rows))
        )

        # Strategy: Replace NULLs with unique placeholder values that won't collide
        # Then join, then verify NULL matches separately

        # Create hash columns for NULL-safe comparison
        hash_col_name = "__null_hash__"

        # Build hash arrays that encode NULL positions
        common_hash_parts = []
        subset_hash_parts = []

        for col_name in column_names:
            common_col = common_rows_table[col_name]
            subset_col = subset_with_matching_types[col_name]

            # Create a hash part: "1" if null, "0" if not null
            common_null_indicator = pyarrow.compute.if_else(
                pyarrow.compute.is_null(common_col),
                pyarrow.array(['1'] * len(common_col)),
                pyarrow.array(['0'] * len(common_col))
            )
            subset_null_indicator = pyarrow.compute.if_else(
                pyarrow.compute.is_null(subset_col),
                pyarrow.array(['1'] * len(subset_col)),
                pyarrow.array(['0'] * len(subset_col))
            )

            common_hash_parts.append(common_null_indicator)
            subset_hash_parts.append(subset_null_indicator)

        # Concatenate to create null pattern hash
        common_null_hash = common_hash_parts[0]
        for part in common_hash_parts[1:]:
            common_null_hash = pyarrow.compute.binary_join_element_wise(
                common_null_hash, part, ''
            )

        subset_null_hash = subset_hash_parts[0]
        for part in subset_hash_parts[1:]:
            subset_null_hash = pyarrow.compute.binary_join_element_wise(
                subset_null_hash, part, ''
            )

        # Add hash column to tables
        common_with_hash = common_with_order.append_column(
            hash_col_name,
            common_null_hash
        )
        subset_with_hash = subset_with_matching_types.append_column(
            hash_col_name,
            subset_null_hash
        )

        # Replace NULLs with fill values for join (they must match by NULL pattern first via hash)
        common_filled_cols = {
            order_col_name: common_with_hash[order_col_name],
            hash_col_name: common_with_hash[hash_col_name]
        }
        subset_filled_cols = {hash_col_name: subset_with_hash[hash_col_name]}

        for col_name in column_names:
            common_col = common_with_hash[col_name]
            subset_col = subset_with_hash[col_name]

            # Fill NULLs with a type-appropriate value (0 for numeric, empty string for string, etc.)
            # The actual value doesn't matter as we filter by hash first
            if (
                pyarrow.types.is_integer(common_col.type)
                or pyarrow.types.is_floating(common_col.type)
            ):
                fill_value = 0
            elif (
                pyarrow.types.is_string(common_col.type)
                or pyarrow.types.is_large_string(common_col.type)
            ):
                fill_value = ''
            elif pyarrow.types.is_date(common_col.type):
                fill_value = 0  # Will be cast to epoch date
            else:
                # For other types, try using the type's default
                fill_value = (
                    pyarrow.scalar(
                        None,
                        type=common_col.type
                    ).as_py()
                    or 0
                )

            common_filled_cols[col_name] = pyarrow.compute.fill_null(common_col, fill_value)
            subset_filled_cols[col_name] = pyarrow.compute.fill_null(subset_col, fill_value)

        common_filled = pyarrow.table(common_filled_cols)
        subset_filled = pyarrow.table(subset_filled_cols)

        # Perform join using hash + all columns as keys
        join_keys = [hash_col_name, *column_names]

        indicator_col = "__indicator_for_mask__"
        subset_with_indicator = subset_filled.append_column(
            indicator_col,
            pyarrow.array(
                [False] * subset_filled.num_rows,
                type=pyarrow.bool_()
            )
        )

        joined_table = common_filled.join(
            subset_with_indicator,
            keys=join_keys,
            join_type="left outer"
        ).sort_by(order_col_name)

        indicator_column = joined_table.column(indicator_col)
        mask = pyarrow.compute.is_null(indicator_column).combine_chunks()

        if pyarrow.compute.any(mask).as_py():
            return mask
        else:
            return None

    @staticmethod
    def format_consolidated_discrepancy_table_for_output(
        *,
        discrepancy_table: pyarrow.Table,
        output_column_renames: list[str] | dict[str, str],
        csv_separator: str = "|",
    ) -> str:
        """
        Format a discrepancy table as CSV string for output.

        Converts a PyArrow table to CSV format with renamed columns and
        specified separator, preserving datetime object formatting.

        Parameters
        ----------
        discrepancy_table
            Table containing discrepancy data to format
        output_column_renames
            New column names as positional list or mapping dictionary
        csv_separator
            Character to use as CSV field separator

        Returns
        -------
        str
            CSV-formatted string representation of the table
        """
        renamed_table = discrepancy_table.rename_columns(output_column_renames)

        # convert to pandas, preserving all datetime settings
        return (
            renamed_table
            .to_pandas(timestamp_as_object=True)
            .to_csv(sep=csv_separator, index=False)
        )

    @classmethod
    def format_endpoint_discrepancy_table_for_output(
        cls,
        *,
        data_block: type[BaseDataBlock],
        discrepancy_table: EndpointDiscrepanciesTable,
        endpoints_enum: enum.StrEnum,
        endpoint_field_map: EndpointFieldMap,
        csv_separator: str = "|",
    ) -> str:
        """
        Format an endpoint discrepancy table with provider-specific naming.

        Converts internal column naming (entity.field format) to provider
        endpoint tag format (endpoint.tag) and outputs as CSV string.

        Parameters
        ----------
        data_block
            Data block class defining the entity structure
        discrepancy_table
            Table containing endpoint discrepancy data
        endpoints_enum
            Enum defining available endpoints
        endpoint_field_map
            Mapping from entity fields to provider tags per endpoint
        csv_separator
            Character to use as CSV field separator

        Returns
        -------
        str
            CSV-formatted string with provider-specific column names

        Raises
        ------
        DataProviderToolkitRuntimeError
            When column name parsing fails
        """
        column_names = discrepancy_table.column_names
        column_new_names = []

        # find mapping from column names to "endpoint.tag" format
        for column_name in column_names:
            try:
                if '$' in column_name:
                    (endpoint_name, rest) = column_name.split('$', 1)
                    (entity_name, field_name) = rest.split('.', 1)
                else:
                    (entity_name, field_name) = column_name.split('.', 1)
                    endpoint_name = None
            except ValueError as error:
                # split failed for some reason
                msg = f"Failed to format discrepancy table column name '{column_name}': {error}"

                raise DataProviderToolkitRuntimeError(msg) from error

            entity = data_block.get_entity_class_name_map()[entity_name]
            field = getattr(entity, field_name)

            if endpoint_name is not None:
                endpoint = endpoints_enum[endpoint_name]
                tag_name = endpoint_field_map[endpoint][field]
                if isinstance(tag_name, PreprocessedFieldMapping): # PreprocessedFieldMapping
                    tag_name = "+".join(tag_name.tags)
                column_new_names.append(f"{endpoint.value}.{tag_name}")
            else:
                column_new_names.append(field_name)

        return cls.format_consolidated_discrepancy_table_for_output(
            discrepancy_table=discrepancy_table,
            output_column_renames=column_new_names,
            csv_separator=csv_separator,
        )

    @classmethod
    def process_endpoint_tables(
        cls,
        *,
        data_block: type[BaseDataBlock],
        endpoint_field_map: EndpointFieldMap,
        endpoint_tables: EndpointTables,
    ) -> ProcessedEndpointTables:
        """
        Process raw endpoint tables through remapping and preprocessing.

        Transforms provider-specific tag names to entity.field format and
        applies configured preprocessor functions to compute derived fields
        from raw data.

        Parameters
        ----------
        data_block
            Data block class defining the entity structure
        endpoint_field_map
            Mapping from entity fields to provider tags per endpoint
        endpoint_tables
            Dictionary mapping endpoints to raw data tables

        Returns
        -------
        ProcessedEndpointTables
            Dictionary mapping endpoints to processed tables with standardized
            column names and computed fields

        Raises
        ------
        DataProviderToolkitArgumentError
            When data_block is not a BaseDataBlock subclass
        DataProviderToolkitNoDataError
            When all provided tables are empty
        DataProviderToolkitRuntimeError
            When preprocessor execution fails
        """
        if not issubclass(data_block, BaseDataBlock):
            msg = "data_block parameter needs to be a subclass of BaseDataBlock"

            raise DataProviderToolkitArgumentError(msg)

        max_table_length = max(
            len(table)
            for table in endpoint_tables.values()
        )
        if max_table_length == 0:
            msg = "All provided endpoint tables are empty."

            raise DataProviderToolkitNoDataError(msg)

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
        try:
            processed_endpoint_tables = cls._process_remapped_endpoint_tables(
                endpoint_field_preprocessors,
                remapped_endpoint_tables,
            )
        except pyarrow.lib.ArrowInvalid as error:
            msg = f"Error running data provider preprocessors: {error}"

            raise DataProviderToolkitRuntimeError(msg) from error

        return processed_endpoint_tables

    @staticmethod
    def _calculate_common_column_discrepancies(
        discrepant_columns: set[str],
        discrepant_rows_mask: pyarrow.BooleanArray,
        primary_keys_table: PrimaryKeyTable,
        key_column_names: list[str],
        aligned_tables: list[pyarrow.Table],
        endpoints: list[Endpoint],
    ) -> pyarrow.Table:
        """
        Create a debug table showing all discrepancy details.

        Builds a table containing primary keys and values from all endpoints
        for columns and rows where discrepancies were detected, enabling
        detailed analysis of data inconsistencies.

        Parameters
        ----------
        discrepant_columns
            Set of column names with detected discrepancies
        discrepant_rows_mask
            Boolean mask indicating rows containing any discrepancy
        primary_keys_table
            Table containing primary key columns
        key_column_names
            List of primary key column names
        aligned_tables
            List of tables aligned to common primary keys
        endpoints
            List of endpoint identifiers corresponding to aligned_tables

        Returns
        -------
        pyarrow.Table
            Table with primary keys and endpoint-specific columns for all
            discrepant data points
        """
        # Start building output table with primary keys
        output_columns = {}

        # Filter primary keys to discrepant rows only
        discrepant_row_keys = primary_keys_table.filter(discrepant_rows_mask)
        for col_name in key_column_names:
            output_columns[col_name] = discrepant_row_keys[col_name]

        # For each discrepant column, add values from all endpoints that have it
        for col_name in sorted(discrepant_columns):
            # Find all endpoints that have this column
            for (i, table) in enumerate(aligned_tables):
                if col_name in table.column_names:
                    endpoint = endpoints[i]
                    # Get the column from the aligned table and filter to discrepant rows
                    col_array = table[col_name].filter(discrepant_rows_mask)
                    output_columns[f"{endpoint.name}${col_name}"] = col_array

        return pyarrow.table(output_columns)

    @staticmethod
    def _calculate_endpoint_column_remaps(
        endpoint_field_map: EndpointFieldMap
    ) -> EndpointColumnRemaps:
        """
        Calculate column name remapping from provider tags to entity fields.

        Analyzes the endpoint field map to determine how provider-specific
        tag names should be renamed to standardized entity.field format,
        handling both simple mappings and preprocessed field mappings.

        Parameters
        ----------
        endpoint_field_map
            Mapping from entity fields to provider tags per endpoint

        Returns
        -------
        EndpointColumnRemaps
            Dictionary mapping endpoints to tag-based column rename operations

        Raises
        ------
        DataProviderIncorrectMappingTypeError
            When a mapping value has an invalid type
        """
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
        """
        Extract preprocessor configurations from endpoint field map.

        Filters the endpoint field map to retain only fields that require
        preprocessing through PreprocessedFieldMapping objects.

        Parameters
        ----------
        endpoint_field_map
            Mapping from entity fields to provider tags per endpoint

        Returns
        -------
        EndpointFieldPreprocessors
            Dictionary mapping endpoints to fields requiring preprocessing
        """
        return {
            endpoint: {
                entity_field: mapping_value
                for (entity_field, mapping_value) in field_mappings.items()
                if isinstance(mapping_value, PreprocessedFieldMapping)
            }
            for (endpoint, field_mappings) in endpoint_field_map.items()
        }

    @staticmethod
    def _clear_table_rows_by_primary_key(
        table: pyarrow.Table,
        clear_rows_primary_keys: PrimaryKeyTable,
        preserved_column_names: list[str],
    ) -> pyarrow.Table:
        """
        Set non-preserved column values to null for specified primary keys.

        Identifies rows in the table matching the provided primary keys and
        nullifies all column values except those in the preserved list.

        Parameters
        ----------
        table
            Table to clear rows from
        clear_rows_primary_keys
            Table containing primary keys of rows to clear
        preserved_column_names
            List of names of columns to keep unchanged

        Returns
        -------
        pyarrow.Table
            Table with specified rows cleared in non-preserved columns

        Raises
        ------
        DataProviderToolkitRuntimeError
            When required columns are missing or type incompatibilities exist
        """
        key_columns = clear_rows_primary_keys.column_names

        # Verify all key columns exist in the target table
        for col in (key_columns + preserved_column_names):
            if col not in table.column_names:
                msg = f"DataProviderToolkit._clear_table_rows_by_primary_key error: Column '{col}' not found in table."

                raise DataProviderToolkitRuntimeError(msg)

        if (
            table.num_rows == 0
            or clear_rows_primary_keys.num_rows == 0
        ):
            return table

        # Combine chunks to ensure we work with flat Arrays, avoiding 'Mask must be array' errors
        table_combined = table.combine_chunks()

        # Add a temporary row index column to track rows
        row_index_col_name = "__temp_row_index__"
        indices_array = pyarrow.array(
            range(table_combined.num_rows)
        )
        table_with_index = table_combined.append_column(
            row_index_col_name,
            indices_array
        )

        # Perform an inner join to identify rows in 'table' that match 'clear_rows_primary_keys'
        # PyArrow join handles nulls as equal by default
        try:
            matches = table_with_index.select([*key_columns, row_index_col_name]).join(
                clear_rows_primary_keys,
                keys=key_columns,
                join_type="inner"
            )
        except pyarrow.lib.ArrowInvalid as error:
            # Propagate error if types are incompatible
            msg = f"DataProviderToolkit._clear_table_rows_by_primary_key error: {error}"

            raise DataProviderToolkitRuntimeError(msg) from error

        if matches.num_rows == 0:
            return table_combined

        # Extract indices of rows to be cleared
        rows_to_clear_indices = matches[row_index_col_name]

        if len(rows_to_clear_indices) == 0:
            return table_combined

        # Create boolean mask for the whole table
        # table_with_index columns are likely ChunkedArrays (even if 1 chunk)
        all_indices = table_with_index[row_index_col_name]

        # is_in returns a ChunkedArray. We must flatten it because replace_with_mask requires an Array mask.
        rows_to_clear_mask = pyarrow.compute.is_in(
            all_indices,
            value_set=rows_to_clear_indices
        ).combine_chunks()

        new_columns = {}
        for col_name in table_combined.column_names:
            # Get column as a single flat Array
            original_col = table_combined[col_name].combine_chunks()

            if col_name in preserved_column_names:
                # Keep primary keys as-is
                new_columns[col_name] = original_col
            else:
                # Replace values with None where mask is True
                null_scalar = pyarrow.scalar(None, type=original_col.type)
                new_col = pyarrow.compute.replace_with_mask(
                    original_col,
                    rows_to_clear_mask,
                    null_scalar
                )
                new_columns[col_name] = new_col

        return pyarrow.Table.from_pydict(new_columns)

    @staticmethod
    def _create_table_from_json_string(json_string: str) -> pyarrow.Table:
        """
        Parse JSON string into a PyArrow table.

        Converts JSON data from array or newline-delimited format into a
        PyArrow table, handling format normalization automatically.

        Parameters
        ----------
        json_string
            JSON string in array or newline-delimited format

        Returns
        -------
        pyarrow.Table
            Parsed table, or empty table if input is empty

        Raises
        ------
        DataProviderParsingError
            When JSON parsing fails due to invalid format
        """
        # PyArrow expects newline-delimited JSON, not JSON arrays
        # Convert JSON array to NDJSON with simple text transformation
        json_string_stripped = json_string.strip()

        if (
            json_string_stripped.startswith('[')
            and json_string_stripped.endswith(']')
        ):
            # Remove outer array brackets
            json_string_stripped = json_string_stripped[1:-1].strip()
            # Replace pattern of }\n  { or },\n  { with }\n{
            json_string_stripped = re.sub(
                r'\}\s*,\s*\{',
                '}\n{',
                json_string_stripped
            )

        if len(json_string_stripped) == 0:
            return pyarrow.table({})

        # Convert string to bytes and create a buffer
        json_bytes = json_string_stripped.encode('utf-8')
        buffer = io.BytesIO(json_bytes)

        # Read into PyArrow table
        try:
            # @todo: read as string and manually infer each column type, to prevent dates read as datetime
            table = pyarrow.json.read_json(
                buffer,
                parse_options=pyarrow.json.ParseOptions(
                    newlines_in_values=True,
                ),
            )
        except pyarrow.lib.ArrowInvalid as error:
            msg = f"Error parsing JSON string: {error}"

            raise DataProviderParsingError(msg) from error

        return table

    @staticmethod
    def _merge_primary_key_subsets_preserving_order(
        primary_key_subsets_tables: list[PrimaryKeyTable],
        *,
        predominant_order_descending: bool = False,
    ) -> PrimaryKeyTable:
        """
        Merge primary key subsets while preserving consistent ordering.

        Combines multiple tables containing subsets of primary keys into a
        single unified ordering using topological sorting to maintain order
        consistency across all input subsets.

        Parameters
        ----------
        primary_key_subsets_tables
            List of tables each containing a subset of primary keys
        predominant_order_descending
            Whether the predominant sort order is descending (True) or ascending (False)

        Returns
        -------
        PrimaryKeyTable
            Table containing merged primary keys in consistent order

        Raises
        ------
        DataProviderToolkitRuntimeError
            When tables have incompatible schemas, contain duplicates, or
            have no columns
        DataProviderMultiEndpointCommonDataOrderError
            When input orderings create circular dependencies
        """
        if not primary_key_subsets_tables:
            return pyarrow.table({})

        first_table = primary_key_subsets_tables[0]
        schema = first_table.schema
        column_names = schema.names

        if not column_names:
            msg = "Primary key merge tables have no columns."

            raise DataProviderToolkitRuntimeError(msg)

        graph = networkx.DiGraph()

        for table in primary_key_subsets_tables:
            if table.schema != schema:
                if len(table.column_names) != len(column_names):
                    msg = "Primary key merge tables have different number of columns."
                elif table.column_names != column_names:
                    msg = "Primary key merge tables have different column names."
                else:
                    msg = "Primary key merge tables have different column types."

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
                msg = "Primary key merge table contains duplicate rows."

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

            raise DataProviderMultiEndpointCommonDataOrderError(msg) from None

        if not sorted_rows:
            return pyarrow.Table.from_pylist([], schema=schema)

        columns_as_tuples = list(
            zip(*sorted_rows, strict=True)
        )
        arrays = [
            pyarrow.array(col_data, type=field.type)
            for (col_data, field) in zip(
                columns_as_tuples,
                schema,
                strict=True
            )
        ]

        return pyarrow.Table.from_arrays(arrays, names=column_names)

    @staticmethod
    def _process_remapped_endpoint_tables(
        endpoint_field_preprocessors: EndpointFieldPreprocessors,
        remapped_endpoint_tables: EndpointTables,
    ) -> EndpointTables:
        """
        Apply preprocessor functions to remapped endpoint tables.

        Executes configured preprocessor chains on input columns to compute
        derived field values, replacing raw input columns with processed
        outputs.

        Parameters
        ----------
        endpoint_field_preprocessors
            Dictionary mapping endpoints to field preprocessing configurations
        remapped_endpoint_tables
            Dictionary mapping endpoints to tables with remapped columns

        Returns
        -------
        EndpointTables
            Dictionary mapping endpoints to tables with preprocessed fields
        """
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
                    result = (
                        preprocessor(*result) if isinstance(result, list)
                        else preprocessor(result)
                    )

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
        """
        Rename table columns from provider tags to entity field format.

        Transforms column names in endpoint tables according to the provided
        remapping configuration, duplicating columns when needed for
        preprocessor inputs.

        Parameters
        ----------
        endpoint_column_remaps
            Dictionary mapping endpoints to tags to column renames
        endpoint_tables
            Dictionary mapping endpoints to raw tables

        Returns
        -------
        EndpointTables
            Dictionary mapping endpoints to tables with remapped column names
        """
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
