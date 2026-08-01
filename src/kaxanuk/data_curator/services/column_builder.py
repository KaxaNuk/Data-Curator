"""
Handles the logic of building the columnar output of the system.
"""

import dataclasses
import inspect
import logging
import types
import typing

import networkx
import pyarrow

import kaxanuk.data_curator.data_blocks
from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    EntityField,
)
from kaxanuk.data_curator.data_blocks.market_daily import (
    MarketDailyDataBlock,
)
from kaxanuk.data_curator.entities import (
    BaseDataEntity,
    Configuration,
)
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCircularDependenciesError,
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderNoDatesToInfillError,
    ColumnBuilderUnavailableEntityFieldError,
    ColumnBuilderUninitializedError,
    InjectedDependencyError,
)
# can't import directly from kaxanuk.data_curator.DataColumn because of circular import error:
from kaxanuk.data_curator.modules.data_column import DataColumn
from kaxanuk.data_curator.services.entity_helper import DataclassProtocol


# Type for a list of modules with calculation functions
type CalculationModules = list[types.ModuleType]
# Type for the identifiers of the columns, e.g. c_eps
type ColumnIdentifier = str
# Type for the container of the columns that have been completely calculated
type CompletedColumns = dict[ColumnIdentifier, DataColumn | Configuration]
# Type for the built rows of each data block, keyed by each of the block's prefixes
type DataBlockRowsByPrefix = dict[str, DataRows | ExpandedDatedFactors]
# Type for the indexed data rows we'll use to build some columns
type DataRows = dict[str, DataclassProtocol | None]
# Type for factors expanded by date (example: dividends, splits)
type ExpandedDatedFactors = dict[str, dict[str, typing.Any] | None]


@dataclasses.dataclass(frozen=True, slots=True)
class CalculatedColumn:
    """
    Precomputed calculation function and parameter names of a calculated column.
    """
    # the calculation function that produces the column:
    function: typing.Callable
    # the names of the columns the function takes as parameters, in order:
    parameter_names: tuple[ColumnIdentifier, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnExtractor:
    """
    Precomputed instructions for extracting a data block's columns under a single prefix.
    """
    # the field of the block's main row entity that holds the subentity, or None for direct/dated-factor columns:
    subentity_field_name: str | None
    # the column names available under this prefix, used to validate requested columns:
    valid_column_names: frozenset[str]


@dataclasses.dataclass(frozen=True, slots=True)
class ResolvedDataBlocks:
    """
    The data blocks resolved for a run, and the built-in blocks that the passed ones replaced.
    """
    # all the data blocks the run will use, the passed ones first, then the built-ins none of them shadowed:
    data_blocks: list[type[BaseDataBlock]]
    # the passed data blocks that claimed at least one prefix of each shadowed built-in data block:
    shadowers_by_built_in_block: dict[
        type[BaseDataBlock],
        list[type[BaseDataBlock]]
    ]


# Type for the calculated columns, keyed by column identifier
type CalculatedColumnsByName = dict[ColumnIdentifier, CalculatedColumn]
# Type for the column extractors of every data block prefix, keyed by prefix
type ColumnExtractorsByPrefix = dict[str, ColumnExtractor]


class ColumnBuilder:
    """
    Class for building the columns we need, based on the data entities provided.

    Initiate it with the data entities, and then run process_columns().
    """

    # the prefix identifying calculated columns, whose values come from calculation functions:
    CALCULATED_COLUMN_PREFIX: typing.ClassVar[str] = 'c'
    # the data block whose clock sync field acts as the master clock when the user doesn't choose one;
    # a passed data block that shadows it takes over the role, as it replaces it in the run:
    DEFAULT_MASTER_CLOCK_DATA_BLOCK: typing.ClassVar[type[BaseDataBlock]] = MarketDailyDataBlock
    # the names of the columns that are provided directly, without a prefix or dependencies:
    FIXED_COLUMN_NAMES: typing.ClassVar[tuple[ColumnIdentifier, ...]] = ('configuration',)
    # @todo replace once entities expose their rows through a uniform accessor:
    # candidate attribute names holding a data entity's row dict, tried in order
    ROW_FIELD_NAMES: typing.ClassVar[tuple[str, ...]] = (
        'rows',
    )

    _calculated_columns: typing.ClassVar[CalculatedColumnsByName] = {}
    _calculation_modules: typing.ClassVar[CalculationModules] = []
    _column_extractors_by_prefix: typing.ClassVar[ColumnExtractorsByPrefix] = {}
    _configuration: typing.ClassVar[Configuration | None] = None
    _data_blocks: typing.ClassVar[list[type[BaseDataBlock]]] = []
    # the data block resolved as the master clock, which all the other data blocks' rows get synced against:
    _master_clock_data_block: typing.ClassVar[type[BaseDataBlock] | None] = None
    _sorted_required_columns: typing.ClassVar[list[ColumnIdentifier]] = []

    def __init__(
        self,
        *,
        data_entities: list[BaseDataEntity],
    ):
        if not self._data_blocks:
            msg = "ColumnBuilder used before its data blocks were initialized"

            raise ColumnBuilderUninitializedError(msg)

        # Resolve each entity to its data block and rows, and locate the master clock rows that all the
        # other blocks' rows will be synced against.
        entity_rows_by_block = {}
        master_clock_rows = None
        for entity in data_entities:
            data_block = self._get_data_block_for_entity(entity, self._data_blocks)
            entity_rows = self._get_entity_rows(entity, self.ROW_FIELD_NAMES)
            entity_rows_by_block[data_block] = entity_rows
            if data_block is self._master_clock_data_block:
                master_clock_rows = entity_rows

        if master_clock_rows is None:
            msg = "No master clock data entity passed to ColumnBuilder"

            raise InjectedDependencyError(msg)

        # Build each block's rows according to its column strategy, keyed by every one of the block's prefixes.
        self._rows_by_prefix: DataBlockRowsByPrefix = {}
        for (data_block, entity_rows) in entity_rows_by_block.items():
            if data_block is self._master_clock_data_block:
                built_rows = entity_rows
            elif len(data_block.dated_factor_date_fields) > 0:
                built_rows = self._expand_dated_factors(
                    iter(master_clock_rows.keys()),
                    tuple(
                        field.__name__
                        for field in data_block.dated_factor_date_fields
                    ),
                    tuple(
                        field.__name__
                        for field in data_block.dated_factor_value_fields
                    ),
                    entity_rows,
                )
            else:
                built_rows = self._infill_data(
                    iter(master_clock_rows.keys()),
                    entity_rows,
                )

            for prefix in data_block.prefix_entity_map:
                self._rows_by_prefix[prefix] = built_rows

        # Data blocks with no passed entity have no data at all, so all their columns resolve to None, just like
        # the columns of a block whose passed entity contains no rows.
        empty_block_rows = dict.fromkeys(master_clock_rows.keys())
        for data_block in self._data_blocks:
            if data_block in entity_rows_by_block:
                continue

            for prefix in data_block.prefix_entity_map:
                self._rows_by_prefix[prefix] = empty_block_rows

    @classmethod
    def get_sorted_required_columns(cls) -> list[ColumnIdentifier]:
        """
        Return the required columns in topologically sorted dependency order.

        Returns
        -------
        The required columns sorted so that each column comes after the columns it depends on

        Raises
        ------
        ColumnBuilderUninitializedError
        """
        if not cls._data_blocks:
            msg = "ColumnBuilder requires data blocks to be initialized before calculating the required columns"

            raise ColumnBuilderUninitializedError(msg)

        return cls._sorted_required_columns

    @classmethod
    def initialize_data_blocks(
        cls,
        *,
        calculation_modules: CalculationModules,
        configuration: Configuration,
        data_blocks: list[type[BaseDataBlock]],
        master_clock_data_block: type[BaseDataBlock] | None = None,
    ) -> None:
        """
        Initialize the data blocks and calculate the topologically sorted required columns.

        The passed data blocks are resolved against the built-in ones, which fill in every prefix the passed
        blocks leave unclaimed. The columns requested in the configuration are then resolved, together with all
        their dependencies, into a dependency-ordered list saved for later retrieval through
        get_sorted_required_columns.

        Parameters
        ----------
        calculation_modules
            The modules containing the calculated column functions
        configuration
            The configuration containing the requested output columns
        data_blocks
            The data block classes to use, which shadow any built-in block sharing their prefixes. Pass an empty
            list to only use the built-in data blocks.
        master_clock_data_block
            The data block whose clock sync field will act as the master clock, or None to use the default one,
            or whichever passed data block shadowed it

        Raises
        ------
        ColumnBuilderCircularDependenciesError
        ColumnBuilderUnavailableEntityFieldError
        InjectedDependencyError
        """
        if not isinstance(configuration, Configuration):
            msg = "Incorrect configuration passed to ColumnBuilder"

            raise InjectedDependencyError(msg)

        cls._calculation_modules = calculation_modules
        cls._configuration = configuration

        resolved_data_blocks = cls._resolve_data_blocks(
            data_blocks,
            cls._get_built_in_data_blocks(),
        )
        cls._data_blocks = resolved_data_blocks.data_blocks
        cls._master_clock_data_block = cls._resolve_master_clock_data_block(
            resolved_data_blocks,
            master_clock_data_block,
        )

        cls._column_extractors_by_prefix = cls._calculate_column_extractors(cls._data_blocks)
        base_column_prefixes = frozenset(cls._column_extractors_by_prefix.keys())
        master_clock_column = cls._get_master_clock_column(cls._master_clock_data_block)
        cls._sorted_required_columns = cls._calculate_sorted_required_columns(
            set(configuration.columns),
            base_column_prefixes=base_column_prefixes,
            calculation_modules=calculation_modules,
            fixed_column_names=cls.FIXED_COLUMN_NAMES,
            master_clock_column=master_clock_column,
        )
        cls._calculated_columns = cls._calculate_calculated_columns(
            cls._sorted_required_columns,
            calculation_modules,
        )

    def process_columns(
        self,
        columns: tuple[ColumnIdentifier, ...]
    ) -> pyarrow.Table:
        """
        Create the required columns by using the data entities and running the calculation functions.

        The columns are processed in the topologically sorted dependency order calculated during data block
        initialization, so each column can be built in a single pass with all its dependencies already resolved.

        Parameters
        ----------
        columns
            Tuple containing the columns to process and output

        Returns
        -------
        A pyarrow.Table containing all the output columns

        Raises
        ------
        ColumnBuilderCustomFunctionNotFoundError
        ColumnBuilderUnavailableEntityFieldError
        """
        # here we will save the columns that we've finished obtaining:
        completed_columns: CompletedColumns = {}
        completed_columns['configuration'] = self._configuration

        for column in self._sorted_required_columns:
            if column in completed_columns:
                continue

            completed_columns[column] = self._process_column(
                column,
                completed_columns,
                calculated_columns=self._calculated_columns,
                column_extractors=self._column_extractors_by_prefix,
                data_block_rows_by_prefix=self._rows_by_prefix,
            )

        return pyarrow.Table.from_pydict(
            {
                column: completed_columns[column]
                for column in columns
            }
        )

    @classmethod
    def _build_column_subgraph(
        cls,
        column: ColumnIdentifier,
        *,
        base_column_prefixes: frozenset[str],
        calculation_modules: CalculationModules,
        fixed_column_names: tuple[ColumnIdentifier, ...],
        resolution_path: tuple[ColumnIdentifier, ...] = (),
    ) -> networkx.DiGraph:
        """
        Build the dependency subgraph for a column, recursively including all the columns it depends on.

        Fixed and base columns are leaf nodes, while calculated columns (prefix 'c') recurse into the columns
        required as parameters by their calculation function. The chain of calculated columns currently being
        resolved is tracked in `resolution_path` so that a circular dependency is detected the moment a column
        reappears on it, before the recursion can overflow the stack.

        Parameters
        ----------
        column
            The column to build the dependency subgraph for
        base_column_prefixes
            The prefixes of the base columns provided by the data blocks
        calculation_modules
            The modules containing the calculated column functions
        fixed_column_names
            The names of the columns that are provided directly, without a prefix or dependencies
        resolution_path
            The calculated columns currently being resolved above this one, in dependency order

        Returns
        -------
        The column's dependency subgraph, with directed edges pointing from each dependency to its dependent column

        Raises
        ------
        ColumnBuilderCircularDependenciesError
        ColumnBuilderUnavailableEntityFieldError
        """
        if column in resolution_path:
            cycle_start = resolution_path.index(column)
            cycle_columns = [
                *resolution_path[cycle_start:],
                column,
            ]
            msg = " ".join([
                "Circular dependency detected between the following column calculation functions:",
                " -> ".join(cycle_columns),
            ])

            raise ColumnBuilderCircularDependenciesError(msg)

        subgraph = networkx.DiGraph()
        subgraph.add_node(column)
        column_prefix = column.split('_', 1)[0]

        if (
            column in fixed_column_names
            or column_prefix in base_column_prefixes
        ):
            # Fixed and base columns are leaf nodes with no dependencies.

            return subgraph

        if column_prefix != cls.CALCULATED_COLUMN_PREFIX:
            msg = f"Column {column} with unknown prefix: {column_prefix}"

            raise ColumnBuilderUnavailableEntityFieldError(msg)

        calculation_function = cls._get_calculation_function(
            column,
            calculation_modules
        )
        calculation_function_params = cls._get_function_params(calculation_function)
        for parameter in calculation_function_params:
            parameter_subgraph = cls._build_column_subgraph(
                parameter,
                base_column_prefixes=base_column_prefixes,
                calculation_modules=calculation_modules,
                fixed_column_names=fixed_column_names,
                resolution_path=(*resolution_path, column),
            )
            subgraph.update(parameter_subgraph)
            subgraph.add_edge(parameter, column)

        return subgraph

    @classmethod
    def _build_dependency_graph(
        cls,
        required_columns: set[ColumnIdentifier],
        *,
        base_column_prefixes: frozenset[str],
        calculation_modules: CalculationModules,
        fixed_column_names: tuple[ColumnIdentifier, ...],
    ) -> networkx.DiGraph:
        """
        Build the full column dependency graph with `required_columns` as roots.

        Parameters
        ----------
        required_columns
            The columns the system must output, used as the roots of the dependency graph
        base_column_prefixes
            The prefixes of the base columns provided by the data blocks
        calculation_modules
            The modules containing the calculated column functions
        fixed_column_names
            The names of the columns that are provided directly, without a prefix or dependencies

        Returns
        -------
        The full column dependency graph, with directed edges pointing from each dependency to its dependent column
        """
        dependency_graph = networkx.DiGraph()
        for column in required_columns:
            column_subgraph = cls._build_column_subgraph(
                column,
                base_column_prefixes=base_column_prefixes,
                calculation_modules=calculation_modules,
                fixed_column_names=fixed_column_names,
            )
            dependency_graph.update(column_subgraph)

        return dependency_graph

    @classmethod
    def _calculate_calculated_columns(
        cls,
        sorted_columns: list[ColumnIdentifier],
        calculation_modules: CalculationModules,
    ) -> CalculatedColumnsByName:
        """
        Precompute the calculation function and parameter names of every calculated column.

        Resolving each calculated column's function and parameters once here keeps the per-column processing
        free of the repeated function lookup and signature inspection.

        Parameters
        ----------
        sorted_columns
            The required columns, among which the calculated ones will be resolved
        calculation_modules
            The modules containing the calculated column functions

        Returns
        -------
        A calculated column for each calculated column in `sorted_columns`
        """
        calculated_columns = {}
        for column in sorted_columns:
            if column.split('_', 1)[0] != cls.CALCULATED_COLUMN_PREFIX:
                continue

            calculation_function = cls._get_calculation_function(
                column,
                calculation_modules
            )
            calculated_columns[column] = CalculatedColumn(
                function=calculation_function,
                parameter_names=tuple(
                    cls._get_function_params(calculation_function)
                ),
            )

        return calculated_columns

    @classmethod
    def _calculate_column_extractors(
        cls,
        data_blocks: list[type[BaseDataBlock]],
    ) -> ColumnExtractorsByPrefix:
        """
        Precompute, for every data block prefix, how to extract and validate its columns.

        Resolving each prefix's extraction strategy once here keeps the per-column processing free of any data
        block traversal or entity introspection.

        Parameters
        ----------
        data_blocks
            The data blocks whose prefixes will be mapped to their column extractors

        Returns
        -------
        A column extractor for each prefix declared across the data blocks
        """
        column_extractors = {}
        for data_block in data_blocks:
            dated_factor_entity = (
                data_block.dated_factor_date_fields[0].__objclass__
                if len(data_block.dated_factor_date_fields) > 0
                else None
            )
            main_row_entity = data_block.clock_sync_field.__objclass__
            for (prefix, entity) in data_block.prefix_entity_map.items():
                if entity is dated_factor_entity:
                    column_extractors[prefix] = ColumnExtractor(
                        subentity_field_name=None,
                        valid_column_names=cls._get_combined_column_names(
                            data_block.dated_factor_date_fields,
                            data_block.dated_factor_value_fields,
                        ),
                    )
                elif entity is main_row_entity:
                    column_extractors[prefix] = ColumnExtractor(
                        subentity_field_name=None,
                        valid_column_names=cls._get_entity_property_names(entity),
                    )
                else:
                    column_extractors[prefix] = ColumnExtractor(
                        subentity_field_name=cls._get_subentity_field_name(main_row_entity, entity),
                        valid_column_names=cls._get_entity_property_names(entity),
                    )

        return column_extractors

    @classmethod
    def _calculate_sorted_required_columns(
        cls,
        required_columns: set[ColumnIdentifier],
        *,
        base_column_prefixes: frozenset[str],
        calculation_modules: CalculationModules,
        fixed_column_names: tuple[ColumnIdentifier, ...],
        master_clock_column: ColumnIdentifier,
    ) -> list[ColumnIdentifier]:
        """
        Return all the required columns and their dependencies, topologically sorted in dependency order.

        The master clock column is always included and placed at the start of the list, so it's available as the
        clock sync base for all the other columns.

        Parameters
        ----------
        required_columns
            The columns the system must output
        base_column_prefixes
            The prefixes of the base columns provided by the data blocks
        calculation_modules
            The modules containing the calculated column functions
        fixed_column_names
            The names of the columns that are provided directly, without a prefix or dependencies
        master_clock_column
            The column that serves as the clock sync base, always placed at the start of the result

        Returns
        -------
        All the required columns and their dependencies, sorted so that each column comes after the columns it
        depends on, with the master clock column at the start

        Raises
        ------
        ColumnBuilderCircularDependenciesError
        ColumnBuilderUnavailableEntityFieldError
        """
        rooted_columns = required_columns | {master_clock_column}
        dependency_graph = cls._build_dependency_graph(
            rooted_columns,
            base_column_prefixes=base_column_prefixes,
            calculation_modules=calculation_modules,
            fixed_column_names=fixed_column_names,
        )
        sorted_columns = cls._topological_sort(dependency_graph)
        columns_after_master_clock = [
            column
            for column in sorted_columns
            if column != master_clock_column
        ]

        return [
            master_clock_column,
            *columns_after_master_clock,
        ]

    @staticmethod
    def _expand_dated_factors(
        dates: typing.Iterator,
        date_fields: tuple[str, ...],
        factor_fields: tuple[str, ...],
        data_rows: DataRows
    ) -> ExpandedDatedFactors:
        """
        Create an outer product between date_fields and factor_fields, with value factor_field for each date_field.

        An example use is dividends, where there are 4 dates and 2 amounts associated with each dividend, and we want
        1 column for each date and amount combination, with the amount as value for that specific date, and None
        otherwise.

        Parameters
        ----------
        dates
            An iterator with dates that we will use as indices for all row data associations

        date_fields
            The date field names

        factor_fields
            The factor field names

        data_rows
            The original data rows containing entities with a field per each `date_fields` and each `factor_fields`

        Returns
        -------
        A dict with date strings as keys, and dicts with date_fields * factor_fields columns as values
        """
        if len(data_rows) < 1:
            return dict.fromkeys(dates)

        # expand the factor_fields data for each date_fields
        expanded_dates = {
            key: {}
            for key in date_fields
        }
        for row in data_rows.values():
            for date_field in date_fields:
                if getattr(row, date_field) is not None:
                    cur_date = str(getattr(row, date_field))
                    expanded_dates[date_field][cur_date] = {}
                    for factor_field in factor_fields:
                        expanded_dates[date_field][cur_date][factor_field] = getattr(row, factor_field)

        output_data = {}
        for date in dates:
            output_data[date] = {}
            for expanded_date_field, expanded_data in expanded_dates.items():
                if date in expanded_data:
                    for factor_field, factor_data in expanded_data[date].items():
                        factor_field_identifier = f'{expanded_date_field}_{factor_field}'
                        output_data[date][factor_field_identifier] = factor_data

        return output_data

    @classmethod
    def _generate_column(
        cls,
        data_entity_rows: dict[str, object],
        field: str,
        subfield: str | None = None
    ) -> DataColumn:
        """
        Return a DataColumn containing the column data for the chosen field.subfield of the data entity rows.

        Parameters
        ----------
        data_entity_rows
            The rows on the data entity from which to extract the fields and subfields
        field
            The name of the field
        subfield
            The name of the subfield

        Returns
        -------
        The generated DataColumn
        """
        return DataColumn.load(
            [
                cls._get_field_from_row(row, field, subfield)
                for row in data_entity_rows.values()
            ],
        )

    @staticmethod
    def _get_built_in_data_blocks() -> list[type[BaseDataBlock]]:
        """
        Return all the built-in data block classes.

        Each data block package published in the public API of the built-in data_blocks package holds one data
        block class, so a new built-in data block only needs its package added to that __all__ to be picked here.

        Returns
        -------
        The list of all the built-in data block classes
        """
        built_in_data_blocks = []
        for package_name in kaxanuk.data_curator.data_blocks.__all__:
            data_block_package = getattr(kaxanuk.data_curator.data_blocks, package_name)
            if not inspect.ismodule(data_block_package):
                continue

            for member_name in data_block_package.__all__:
                member = getattr(data_block_package, member_name)
                if (
                    inspect.isclass(member)
                    and issubclass(member, BaseDataBlock)
                ):
                    built_in_data_blocks.append(member)

        return built_in_data_blocks

    @staticmethod
    def _get_calculation_function(
        function_name: str,
        calculation_modules: CalculationModules
    ) -> typing.Callable:
        """
        Look for a calculation function among the custom_module (if exists) or the calculations_module, and return it.

        Parameters
        ----------
        function_name
            The name of the function we're looking for
        calculation_modules
            The list of modules where we're searching for the function.

        Returns
        -------
        The function itself, from the first module in the list that defines it

        Raises
        ------
        ColumnBuilderCustomFunctionNotFoundError
        """
        for calculation_module in calculation_modules:
            if hasattr(calculation_module, function_name):
                return getattr(calculation_module, function_name)
        else:   # noqa: PLW0120
            msg = f"Custom column calculation function not found: {function_name}"
            raise ColumnBuilderCustomFunctionNotFoundError(msg)

    @staticmethod
    def _get_combined_column_names(
        date_fields: tuple[EntityField, ...],
        value_fields: tuple[EntityField, ...],
    ) -> frozenset[str]:
        """
        Return the combined column names produced by all the date and value field combinations.

        Parameters
        ----------
        date_fields
            The entity date fields denoting the dates of the events
        value_fields
            The entity value fields denoting the values ascribed to each date

        Returns
        -------
        The names of the columns generated from all the date/value field combinations
        """
        return frozenset(
            f'{date_field.__name__}_{value_field.__name__}'
            for date_field in date_fields
            for value_field in value_fields
        )

    @staticmethod
    def _get_data_block_for_entity(
        entity: BaseDataEntity,
        data_blocks: list[type[BaseDataBlock]],
    ) -> type[BaseDataBlock]:
        """
        Return the data block whose main entity matches the given entity instance.

        Parameters
        ----------
        entity
            The data entity instance whose owning data block we want to find
        data_blocks
            The data blocks to search, each identified by its main entity

        Returns
        -------
        The data block whose main_entity matches the type of `entity`

        Raises
        ------
        InjectedDependencyError
        """
        for data_block in data_blocks:
            if isinstance(entity, data_block.main_entity):
                return data_block

        msg = f"No data block found for entity of type {type(entity).__name__}"

        raise InjectedDependencyError(msg)

    @staticmethod
    def _get_entity_property_names(entity_class: type) -> frozenset[str]:
        """
        Return the names of all the properties of an entity class.

        Useful for validating whether a column name corresponds to an entity property.

        Parameters
        ----------
        entity_class
            The class whose property names we want to collect

        Returns
        -------
        The names of all the properties of the class
        """
        return frozenset(
            member[0]
            for member in inspect.getmembers(entity_class)
        )

    @staticmethod
    def _get_entity_rows(
        entity: BaseDataEntity,
        row_field_names: tuple[str, ...],
    ) -> DataRows:
        """
        Return the row dict of a data entity, looked up through the candidate row field names.

        Parameters
        ----------
        entity
            The data entity whose rows we want to extract
        row_field_names
            The candidate attribute names holding the entity's row dict, tried in order

        Returns
        -------
        The entity's row dict

        Raises
        ------
        InjectedDependencyError
        """
        for row_field_name in row_field_names:
            if hasattr(entity, row_field_name):
                return getattr(entity, row_field_name)

        msg = f"No known row field found on entity of type {type(entity).__name__}"

        raise InjectedDependencyError(msg)

    @staticmethod
    def _get_field_from_row(
        row: dict | object,
        field: str,
        subfield: str | None = None
    ) -> typing.Any:
        """
        Extract a data entity field or subfield from a row, returning None if not exists.

        Parameters
        ----------
        row
            The rows on the data entity from which to extract the fields and subfields
        field
            The name of the field
        subfield
            The name of the subfield

        Returns
        -------
        The requested field or subfield, or None if not exists
        """
        if isinstance(row, dict):
            field_value = row.get(field, None)
            if subfield is None:
                return field_value

            if isinstance(field_value, dict):
                return field_value.get(subfield, None)

            return None

        # A single getattr with a default avoids the extra hasattr lookup on this per-cell hot path;
        # a missing attribute and an attribute that is None both resolve to None, as before.
        field_value = getattr(row, field, None)
        if subfield is None:
            return field_value

        return getattr(field_value, subfield, None)

    @staticmethod
    def _get_function_params(callable_function: typing.Callable) -> list[str]:
        """
        Return the names of the callable function's parameters.

        Parameters
        ----------
        callable_function
            The callable whose parameter names we want to extract

        Returns
        -------
        list of the callable's parameter names

        Raises
        ------
        TypeError
        """
        if not callable(callable_function):
            msg = "ColumnBuilder._get_function_params callable_function parameter is not callable"
            raise TypeError(msg)

        return list(
            inspect.signature(callable_function).parameters.keys()
        )

    @staticmethod
    def _get_master_clock_column(
        master_clock_data_block: type[BaseDataBlock]
    ) -> ColumnIdentifier:
        """
        Reconstruct the master clock column identifier from the master clock data block.

        The identifier is built from the prefix mapped to the data block's clock sync field entity and the name
        of that field, matching the base column identifier format.

        Parameters
        ----------
        master_clock_data_block
            The data block whose clock sync field acts as the master clock

        Returns
        -------
        The master clock column identifier

        Raises
        ------
        InjectedDependencyError
        """
        clock_sync_field = master_clock_data_block.clock_sync_field
        clock_sync_entity = clock_sync_field.__objclass__
        for (prefix, entity) in master_clock_data_block.prefix_entity_map.items():
            if issubclass(entity, clock_sync_entity):
                return f'{prefix}_{clock_sync_field.__name__}'

        msg = " ".join([
            "Master clock data block has no prefix mapped to its clock sync field entity:",
            clock_sync_entity.__name__,
        ])

        raise InjectedDependencyError(msg)

    @staticmethod
    def _get_subentity_field_name(
        parent_entity: type[BaseDataEntity],
        subentity: type[BaseDataEntity],
    ) -> str:
        """
        Return the name of the field on `parent_entity` that holds the `subentity`.

        Parameters
        ----------
        parent_entity
            The entity class whose fields will be searched
        subentity
            The subentity class we're looking for among the parent entity's fields

        Returns
        -------
        The name of the parent entity field holding the subentity

        Raises
        ------
        ColumnBuilderUnavailableEntityFieldError
        """
        for (field_name, field_type) in typing.get_type_hints(parent_entity).items():
            field_types = (
                typing.get_args(field_type)
                or (field_type,)
            )
            if subentity in field_types:
                return field_name

        msg = " ".join([
            f"Entity {parent_entity.__name__} has no field holding subentity:",
            subentity.__name__,
        ])

        raise ColumnBuilderUnavailableEntityFieldError(msg)

    @staticmethod
    def _infill_data(
        dates: typing.Iterator,
        data_rows: DataRows
    ) -> DataRows:
        """
        Infill the data rows, duplicating the previous row for each date in `dates` not present in `data_rows`.

        Parameters
        ----------
        dates
            The dates that will serve as the indices of the data that we will be infilling
        data_rows
            The data that will be used to infill the rows for each date in `dates`

        Returns
        -------
        The infilled  data rows for all `dates`

        Raises
        ------
        ColumnBuilderNoDatesToInfillError
        """
        if len(data_rows) < 1:
            return dict.fromkeys(dates)

        # @todo save each repeating date's data as a separate Array, all consolidated inside a ChunkedArray
        infilled_data = {}
        data_row_dates = iter(data_rows.keys())
        previous_data_row_date = None
        current_data_row_date = next(data_row_dates)

        try:
            first_date = next(dates)
        except StopIteration as err:
            raise ColumnBuilderNoDatesToInfillError from err

        # Find the data_row right before the first date in dates
        if first_date > current_data_row_date:
            while first_date >= current_data_row_date:
                try:
                    previous_data_row_date = current_data_row_date
                    current_data_row_date = next(data_row_dates)
                    continue
                except StopIteration:
                    break

        # need to fill in the first element as we already advanced the dates internal cursor
        infilled_data[first_date] = data_rows.get(previous_data_row_date)

        for date in dates:
            if date < current_data_row_date:
                infilled_data[date] = data_rows.get(previous_data_row_date)
            else:
                try:
                    previous_data_row_date = current_data_row_date
                    current_data_row_date = next(data_row_dates)
                except StopIteration:
                    pass

                infilled_data[date] = data_rows.get(previous_data_row_date)

        return infilled_data

    @staticmethod
    def _map_prefixes_to_data_blocks(
        data_blocks: list[type[BaseDataBlock]],
    ) -> dict[str, type[BaseDataBlock]]:
        """
        Map every prefix declared by the data blocks to the data block declaring it.

        Parameters
        ----------
        data_blocks
            The data blocks whose declared prefixes will be mapped

        Returns
        -------
        Each prefix declared across the data blocks, mapped to its declaring data block

        Raises
        ------
        InjectedDependencyError
        """
        blocks_by_prefix = {}
        colliding_blocks_by_prefix = {}
        for data_block in data_blocks:
            for prefix in data_block.prefix_entity_map:
                if prefix not in blocks_by_prefix:
                    blocks_by_prefix[prefix] = data_block

                    continue

                if prefix not in colliding_blocks_by_prefix:
                    colliding_blocks_by_prefix[prefix] = [
                        blocks_by_prefix[prefix],
                    ]

                colliding_blocks_by_prefix[prefix].append(data_block)

        if len(colliding_blocks_by_prefix) > 0:
            collision_descriptions = []
            for prefix in sorted(colliding_blocks_by_prefix):
                colliding_block_names = ", ".join(
                    colliding_block.__name__
                    for colliding_block in colliding_blocks_by_prefix[prefix]
                )
                collision_descriptions.append(
                    f"prefix '{prefix}' declared by {colliding_block_names}"
                )

            msg = " ".join([
                "The following data blocks declare colliding prefixes:",
                "; ".join(collision_descriptions),
            ])

            raise InjectedDependencyError(msg)

        return blocks_by_prefix

    @classmethod
    def _process_column(
        cls,
        column: ColumnIdentifier,
        completed_columns: CompletedColumns,
        *,
        calculated_columns: CalculatedColumnsByName,
        column_extractors: ColumnExtractorsByPrefix,
        data_block_rows_by_prefix: DataBlockRowsByPrefix,
    ) -> DataColumn:
        """
        Calculate a single column, assuming all the columns it depends on are already in completed_columns.

        Calculated columns run their precomputed calculation function. Every other column is built from its
        prefix's precomputed column extractor: the requested column is validated against the extractor's valid
        column names, then generated directly or through the extractor's subentity field.

        Parameters
        ----------
        column
            The column to calculate
        completed_columns
            The CompletedColumns containing the already calculated columns, including this column's dependencies
        calculated_columns
            The precomputed calculation functions and parameter names, keyed by calculated column identifier
        column_extractors
            The precomputed column extractors, keyed by each data block prefix
        data_block_rows_by_prefix
            The built data block rows, keyed by each of the blocks' prefixes

        Returns
        -------
        The calculated column

        Raises
        ------
        ColumnBuilderUnavailableEntityFieldError
        """
        [column_type, column_name] = column.split('_', 1)

        if column_type == cls.CALCULATED_COLUMN_PREFIX:
            calculated_column = calculated_columns[column]
            param_columns = (
                completed_columns[parameter_name]
                for parameter_name in calculated_column.parameter_names
            )

            # Execute the calculation function and wrap it in DataColumn (in case of Pandas.Series, etc.)
            return DataColumn.load(
                calculated_column.function(*param_columns)
            )

        if column_type not in column_extractors:
            msg = f"Column {column_name} with unknown prefix: {column_type}"

            raise ColumnBuilderUnavailableEntityFieldError(msg)

        column_extractor = column_extractors[column_type]

        if column_name not in column_extractor.valid_column_names:
            msg = f"Column not available under prefix '{column_type}': {column}"

            raise ColumnBuilderUnavailableEntityFieldError(msg)

        data_block_rows = data_block_rows_by_prefix[column_type]

        if column_extractor.subentity_field_name is not None:
            return cls._generate_column(
                data_block_rows,
                column_extractor.subentity_field_name,
                column_name
            )

        return cls._generate_column(
            data_block_rows,
            column_name
        )

    @classmethod
    def _resolve_data_blocks(
        cls,
        passed_data_blocks: list[type[BaseDataBlock]],
        built_in_data_blocks: list[type[BaseDataBlock]],
    ) -> ResolvedDataBlocks:
        """
        Resolve the data blocks of a run, with the passed ones shadowing the built-in ones sharing their prefixes.

        A passed data block shadows every built-in data block declaring at least one of the passed block's
        prefixes, which lets a user replace a built-in block just by declaring its prefixes, without having to
        pass the replaced block in. Only the built-in blocks whose prefixes are all left unclaimed are added.

        Parameters
        ----------
        passed_data_blocks
            The data blocks passed by the user, which take precedence over the built-in ones
        built_in_data_blocks
            The discovered built-in data blocks, which fill in the prefixes the passed blocks leave unclaimed

        Returns
        -------
        The data blocks the run will use, together with the built-in blocks the passed ones shadowed

        Raises
        ------
        InjectedDependencyError
        """
        blocks_by_passed_prefix = cls._map_prefixes_to_data_blocks(passed_data_blocks)
        # the passed blocks go first, so they win the entity lookups against any built-in block they subclass:
        resolved_data_blocks = list(passed_data_blocks)
        shadowers_by_built_in_block = {}

        for built_in_data_block in built_in_data_blocks:
            if built_in_data_block in passed_data_blocks:
                # the user passed this built-in data block in explicitly, so it isn't shadowed by anything

                continue

            shadowing_data_blocks = []
            unclaimed_prefixes = []
            for prefix in built_in_data_block.prefix_entity_map:
                if prefix not in blocks_by_passed_prefix:
                    unclaimed_prefixes.append(prefix)

                    continue

                shadowing_data_block = blocks_by_passed_prefix[prefix]
                if shadowing_data_block not in shadowing_data_blocks:
                    shadowing_data_blocks.append(shadowing_data_block)

            if len(shadowing_data_blocks) < 1:
                resolved_data_blocks.append(built_in_data_block)

                continue

            shadowers_by_built_in_block[built_in_data_block] = shadowing_data_blocks

            if len(unclaimed_prefixes) > 0:
                shadowing_data_block_names = ", ".join(
                    shadowing_block.__name__
                    for shadowing_block in shadowing_data_blocks
                )
                logging.getLogger(__name__).info(
                    "Data block %s was replaced by %s, so its columns under these prefixes are unavailable: %s",
                    built_in_data_block.__name__,
                    shadowing_data_block_names,
                    ", ".join(unclaimed_prefixes),
                )

        return ResolvedDataBlocks(
            data_blocks=resolved_data_blocks,
            shadowers_by_built_in_block=shadowers_by_built_in_block,
        )

    @classmethod
    def _resolve_master_clock_data_block(
        cls,
        resolved_data_blocks: ResolvedDataBlocks,
        master_clock_data_block: type[BaseDataBlock] | None,
    ) -> type[BaseDataBlock]:
        """
        Resolve the data block whose clock sync field will act as the master clock.

        An explicitly chosen data block is always used. Otherwise the default master clock data block is used, or
        the single passed data block that shadowed it, as that block replaced it in the run.

        Parameters
        ----------
        resolved_data_blocks
            The run's resolved data blocks and the built-in blocks the passed ones shadowed
        master_clock_data_block
            The data block explicitly chosen as the master clock, or None to resolve the default one

        Returns
        -------
        The data block whose clock sync field will act as the master clock

        Raises
        ------
        InjectedDependencyError
        """
        if master_clock_data_block is not None:
            return master_clock_data_block

        if cls.DEFAULT_MASTER_CLOCK_DATA_BLOCK in resolved_data_blocks.data_blocks:
            return cls.DEFAULT_MASTER_CLOCK_DATA_BLOCK

        default_shadowing_blocks = resolved_data_blocks.shadowers_by_built_in_block.get(
            cls.DEFAULT_MASTER_CLOCK_DATA_BLOCK,
            [],
        )
        if len(default_shadowing_blocks) == 1:
            return default_shadowing_blocks[0]

        default_shadowing_block_names = ", ".join(
            default_shadowing_block.__name__
            for default_shadowing_block in default_shadowing_blocks
        )
        msg = " ".join([
            f"The default master clock data block {cls.DEFAULT_MASTER_CLOCK_DATA_BLOCK.__name__}",
            f"was replaced by these data blocks: {default_shadowing_block_names}.",
            "Choose which data block will act as the master clock by passing it as master_clock_data_block.",
        ])

        raise InjectedDependencyError(msg)

    @staticmethod
    def _topological_sort(dependency_graph: networkx.DiGraph) -> list[ColumnIdentifier]:
        """
        Return the columns in the dependency graph in topologically sorted order.

        This ensures the columns are processed in the correct order, with all their dependencies calculated first.

        Parameters
        ----------
        dependency_graph
            The column dependency graph to sort

        Returns
        -------
        The column identifiers in topological order
        """
        return list(
            networkx.topological_sort(dependency_graph)
        )
