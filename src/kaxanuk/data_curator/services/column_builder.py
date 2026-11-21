"""
Handles the logic of building the columnar output of the system.
"""

import collections.abc
import dataclasses
import inspect
import logging
import types
import typing

import networkx
import pyarrow
import pyarrow.compute

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
    ClockSpec,
    Configuration,
)
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCalculationError,
    ColumnBuilderCircularDependenciesError,
    ColumnBuilderClockError,
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderUnavailableEntityFieldError,
    ColumnBuilderUninitializedError,
    DataColumnParameterError,
    DataColumnSyncError,
    InjectedDependencyError,
)
# can't import directly from kaxanuk.data_curator.DataColumn because of circular import error:
from kaxanuk.data_curator.modules.data_column import (
    ConsolidationMethod,
    DataColumn,
)


# Type for a list of modules with calculation functions
type CalculationModules = list[types.ModuleType]
# Type for the identifiers of the columns, e.g. c_eps
type ColumnIdentifier = str
# Type for the container of the columns that have been completely calculated
type CompletedColumns = dict[ColumnIdentifier, DataColumn | Configuration]
# Type for the clocks syncing a data block's rows to the master clock, keyed by the name of the field each one tracks
type DataBlockClocks = dict[str, DataColumn]
# Type for the data entities, keyed by the data block each one belongs to
type DataBlockEntities = dict[type[BaseDataBlock], BaseDataEntity]
# Type for the indexed data rows we'll use to build some columns
type DataRows = dict[str, BaseDataEntity | None]


@dataclasses.dataclass(frozen=True, slots=True)
class CalculatedColumn:
    """
    Precomputed calculation function and parameter names of a calculated column.
    """
    # the calculation function that produces the column:
    function: typing.Callable
    # the module the calculation function was found in, which tells custom calculations from built-in ones:
    module: types.ModuleType
    # the names of the columns the function takes as parameters, in order:
    parameter_names: tuple[ColumnIdentifier, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class ClockedDataBlockRows:
    """
    A data block's own rows, with the clocks that sync them to the master clock.
    """
    # the clocks syncing the rows, keyed by the name of the field each one tracks:
    clocks: DataBlockClocks
    # the rows of the data block's main entity, in the order of its row keys:
    rows: tuple[BaseDataEntity | None, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnExtractor:
    """
    Precomputed instructions for extracting a data block's columns under a single prefix.
    """
    # the method for consolidating each column's values when its rows get downscaled, keyed by column name:
    consolidation_methods: dict[str, ConsolidationMethod]
    # the data block whose rows hold the columns:
    data_block: type[BaseDataBlock]
    # the fields of each dated factor column, keyed by column name, or empty for the other prefixes:
    dated_factor_columns: dict[str, 'DatedFactorColumn']
    # the field of the block's main row entity that holds the subentity, or None for direct/dated-factor columns:
    subentity_field_name: str | None
    # the column names available under this prefix, used to validate requested columns:
    valid_column_names: frozenset[str]


@dataclasses.dataclass(frozen=True, slots=True)
class DatedFactorColumn:
    """
    The fields of a dated factor column, which holds each row's value on the dates of one of the row's date fields.
    """
    # the date field whose clock syncs the column's values:
    date_field: EntityField
    # the field holding the column's values:
    value_field: EntityField


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

    Initiate it with the data entities, keyed by the data block each one belongs to, and then run process_columns().
    """

    # the prefix identifying calculated columns, whose values come from calculation functions:
    CALCULATED_COLUMN_PREFIX: typing.ClassVar[str] = 'c'
    # the data block whose clock sync field acts as the master clock when the user doesn't choose one;
    # a passed data block that shadows it takes over the role, as it replaces it in the run:
    DEFAULT_MASTER_CLOCK_DATA_BLOCK: typing.ClassVar[type[BaseDataBlock]] = MarketDailyDataBlock
    # the names of the columns that are provided directly, without a prefix or dependencies:
    FIXED_COLUMN_NAMES: typing.ClassVar[tuple[ColumnIdentifier, ...]] = ('configuration',)

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
        data_block_entities: DataBlockEntities,
    ):
        if not self._data_blocks:
            msg = "ColumnBuilder used before its data blocks were initialized"

            raise ColumnBuilderUninitializedError(msg)

        self._validate_data_block_entities(
            data_block_entities,
            self._data_blocks,
        )

        # The master clock comes from the master clock data block's rows, which all the other blocks' rows get synced
        # to. The entities come keyed by their data blocks, as several blocks can share a main entity.
        master_clock_data_block = self._master_clock_data_block
        if (
            master_clock_data_block is None
            or master_clock_data_block not in data_block_entities
        ):
            msg = "No master clock data entity passed to ColumnBuilder"

            raise InjectedDependencyError(msg)

        master_clock_entity = data_block_entities[master_clock_data_block]
        self._master_clock_column = self._build_master_clock_column(
            clock_spec=self._get_entity_clock_spec(
                master_clock_entity,
                master_clock_data_block.clock_spec_field,
            ),
            data_block=master_clock_data_block,
            rows=self._get_entity_rows(
                master_clock_entity,
                master_clock_data_block.rows_field,
            ),
        )

        # Each data block's rows keep their own scale, with the clocks that sync them to the master clock. The data
        # blocks with no passed entity have no data at all, so all their columns resolve to None.
        self._clocked_rows_by_block: dict[type[BaseDataBlock], ClockedDataBlockRows] = {}
        for (data_block, entity) in data_block_entities.items():
            entity_rows = self._get_entity_rows(entity, data_block.rows_field)
            self._clocked_rows_by_block[data_block] = ClockedDataBlockRows(
                clocks=self._build_data_block_clocks(
                    clock_spec=self._get_entity_clock_spec(
                        entity,
                        data_block.clock_spec_field,
                    ),
                    data_block=data_block,
                    master_clock_column=self._master_clock_column,
                    master_clock_data_block=master_clock_data_block,
                    rows=entity_rows,
                ),
                rows=tuple(entity_rows.values()),
            )

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
        # A data block with the calculated columns' prefix would take over all of them
        cls._validate_reserved_prefixes(
            resolved_data_blocks.data_blocks,
            (cls.CALCULATED_COLUMN_PREFIX,),
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
        ColumnBuilderCalculationError
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
                clocked_rows_by_block=self._clocked_rows_by_block,
                column_extractors=self._column_extractors_by_prefix,
                master_clock_column=self._master_clock_column,
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
    def _build_data_block_clocks(
        cls,
        *,
        clock_spec: ClockSpec,
        data_block: type[BaseDataBlock],
        master_clock_column: DataColumn,
        master_clock_data_block: type[BaseDataBlock],
        rows: DataRows,
    ) -> DataBlockClocks:
        """
        Build the clocks syncing a data block's rows to the master clock, keyed by the name of the field each tracks.

        Every data block gets a clock for its clock sync field, built from its row keys, as a row without data still
        has its key, while the master clock data block's is the master clock itself. Each dated factor date field also
        gets its own clock, built from the field's values, as each row's dates differ between those fields.

        Parameters
        ----------
        clock_spec
            The time scale of the data block's rows, as assigned by its data provider
        data_block
            The data block whose rows get synced
        master_clock_column
            The master clock column of the run
        master_clock_data_block
            The data block whose rows make up the master clock
        rows
            The rows of the data block's main entity, keyed by their clock sync values

        Returns
        -------
        The data block's clocks, keyed by the name of the field each one tracks

        Raises
        ------
        ColumnBuilderClockError
            If the data block's rows can't be synced to the master clock
        """
        clock_sync_field_name = data_block.clock_sync_field.__name__
        if data_block is master_clock_data_block:
            clocks = {
                clock_sync_field_name: master_clock_column,
            }
        else:
            clocks = {
                clock_sync_field_name: cls._build_reference_clock_column(
                    clock_field=data_block.clock_sync_field,
                    clock_spec=clock_spec,
                    data_block=data_block,
                    master_clock_column=master_clock_column,
                    timestamps=cls._calculate_key_timestamps(data_block, rows),
                ),
            }

        for date_field in data_block.dated_factor_date_fields:
            if date_field.__name__ in clocks:
                continue

            clocks[date_field.__name__] = cls._build_reference_clock_column(
                clock_field=date_field,
                clock_spec=clock_spec,
                data_block=data_block,
                master_clock_column=master_clock_column,
                timestamps=cls._calculate_field_timestamps(date_field.__name__, rows),
            )

        return clocks

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
    def _build_master_clock_column(
        cls,
        *,
        clock_spec: ClockSpec,
        data_block: type[BaseDataBlock],
        rows: DataRows,
    ) -> DataColumn:
        """
        Build the master clock from the master clock data block's row keys, as a row without data still has its key.

        Parameters
        ----------
        clock_spec
            The time scale of the master clock data block's rows, as assigned by its data provider
        data_block
            The master clock data block
        rows
            The rows of the master clock data block's main entity, keyed by their clock sync values

        Returns
        -------
        The master clock column

        Raises
        ------
        ColumnBuilderClockError
            If the data block's row keys can't make up a master clock, like when they aren't strictly increasing
        """
        timestamps = cls._calculate_key_timestamps(data_block, rows)
        try:

            return DataColumn(
                timestamps,
                clock_master=True,
                clock_periods_per_row=clock_spec.periods_per_row,
                clock_reference_period=clock_spec.reference_period,
            )
        except DataColumnParameterError as error:
            msg = f"{data_block.__name__} rows can't make up the master clock: {error}"

            raise ColumnBuilderClockError(
                msg,
                data_block=data_block,
            ) from error

    @staticmethod
    def _build_reference_clock_column(
        *,
        clock_field: EntityField,
        clock_spec: ClockSpec,
        data_block: type[BaseDataBlock],
        master_clock_column: DataColumn,
        timestamps: pyarrow.Array,
    ) -> DataColumn:
        """
        Build a reference clock syncing a data block's rows to the master clock, through one of the rows' fields.

        The clock gets synced right away, so a data block whose rows can't be synced gets reported before any of its
        columns get built.

        Parameters
        ----------
        clock_field
            The field of the data block's rows whose values the clock tracks
        clock_spec
            The time scale of the data block's rows, as assigned by its data provider
        data_block
            The data block whose rows get synced
        master_clock_column
            The master clock column of the run
        timestamps
            The timestamps of the data block's rows for the clock field, one per row

        Returns
        -------
        The reference clock column

        Raises
        ------
        ColumnBuilderClockError
            If the data block's rows can't be synced to the master clock, like when its clock spec can't be ordered
            against the master clock's
        """
        try:
            reference_clock_column = DataColumn(
                timestamps,
                clock_downscale_method=data_block.clock_downscale_method,
                clock_master=master_clock_column,
                clock_periods_per_row=clock_spec.periods_per_row,
                clock_reference_period=clock_spec.reference_period,
                clock_upscale_method=data_block.clock_upscale_method,
                consolidation_method=data_block.get_downscale_consolidation_method(clock_field),
            )
            # The sync indices get calculated on first access and then cached
            reference_clock_column.sync_indices    # noqa: B018
        except (
            DataColumnParameterError,
            DataColumnSyncError,
        ) as error:
            msg = " ".join([
                f"{data_block.__name__} rows can't be synced to the master clock",
                f"through {clock_field.__name__}: {error}",
            ])

            raise ColumnBuilderClockError(
                msg,
                data_block=data_block,
            ) from error

        return reference_clock_column

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

            calculation_module = cls._get_calculation_module(
                column,
                calculation_modules
            )
            calculation_function = getattr(calculation_module, column)
            calculated_columns[column] = CalculatedColumn(
                function=calculation_function,
                module=calculation_module,
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
            # The data block's entities holding the fields can be extensions of the entities declaring them
            dated_factor_entity = (
                data_block.get_field_entity(
                    data_block.dated_factor_date_fields[0]
                )
                if len(data_block.dated_factor_date_fields) > 0
                else None
            )
            main_row_entity = data_block.get_field_entity(
                data_block.clock_sync_field
            )
            for (prefix, entity) in data_block.prefix_entity_map.items():
                if entity is dated_factor_entity:
                    dated_factor_columns = cls._get_dated_factor_columns(
                        data_block.dated_factor_date_fields,
                        data_block.dated_factor_value_fields,
                    )
                    column_extractors[prefix] = ColumnExtractor(
                        consolidation_methods={
                            column_name: data_block.get_downscale_consolidation_method(
                                dated_factor_column.value_field
                            )
                            for (column_name, dated_factor_column) in dated_factor_columns.items()
                        },
                        data_block=data_block,
                        dated_factor_columns=dated_factor_columns,
                        subentity_field_name=None,
                        valid_column_names=frozenset(dated_factor_columns),
                    )

                    continue

                field_names = cls._get_entity_field_names(entity)
                column_extractors[prefix] = ColumnExtractor(
                    consolidation_methods={
                        field_name: data_block.get_downscale_consolidation_method(
                            getattr(entity, field_name)
                        )
                        for field_name in field_names
                    },
                    data_block=data_block,
                    dated_factor_columns={},
                    subentity_field_name=(
                        None
                        if entity is main_row_entity
                        else cls._get_subentity_field_name(main_row_entity, entity)
                    ),
                    valid_column_names=field_names,
                )

        return column_extractors

    @staticmethod
    def _calculate_field_timestamps(
        field_name: str,
        rows: DataRows,
    ) -> pyarrow.Array:
        """
        Return the values of a date field of each row, as the timestamps of the clock tracking that field.

        Parameters
        ----------
        field_name
            The name of the date field
        rows
            The rows holding the field

        Returns
        -------
        The field's value of each row, null for the rows without data or without a value
        """
        return pyarrow.array([
            getattr(row, field_name, None)
            for row in rows.values()
        ])

    @staticmethod
    def _calculate_key_timestamps(
        data_block: type[BaseDataBlock],
        rows: DataRows,
    ) -> pyarrow.Array:
        """
        Return the dates of a data block's row keys, as the timestamps of the clock tracking its clock sync field.

        Parameters
        ----------
        data_block
            The data block whose rows get keyed by their clock sync values
        rows
            The rows of the data block's main entity, keyed by their clock sync values in 'YYYY-MM-DD' format

        Returns
        -------
        The date of each row key

        Raises
        ------
        ColumnBuilderClockError
            If any row key isn't a date in 'YYYY-MM-DD' format
        """
        try:

            return pyarrow.compute.cast(
                pyarrow.array(
                    list(rows.keys()),
                    type=pyarrow.string(),
                ),
                pyarrow.date32(),
            )
        except pyarrow.ArrowInvalid as error:
            msg = f"{data_block.__name__} rows have keys that aren't dates in 'YYYY-MM-DD' format: {error}"

            raise ColumnBuilderClockError(
                msg,
                data_block=data_block,
            ) from error

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

    @classmethod
    def _generate_column(
        cls,
        rows: tuple[BaseDataEntity | None, ...],
        field: str,
        subfield: str | None = None,
        *,
        clock_column: DataColumn,
        consolidation_method: ConsolidationMethod,
    ) -> DataColumn:
        """
        Return a DataColumn holding the chosen field.subfield of each row, synced to the master clock by the clock.

        Parameters
        ----------
        rows
            The rows of a data block's main entity, from which to extract the fields and subfields
        field
            The name of the field
        subfield
            The name of the subfield
        clock_column
            The clock tracking the rows, either the master clock or a reference clock synced to it
        consolidation_method
            The method for consolidating the column's values when the rows get downscaled

        Returns
        -------
        The generated DataColumn, with one value per row
        """
        return DataColumn.load(
            [
                cls._get_field_from_row(row, field, subfield)
                for row in rows
            ],
            consolidation_method=consolidation_method,
            reference_clock_column=clock_column,
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

    @classmethod
    def _get_calculation_function(
        cls,
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
        calculation_module = cls._get_calculation_module(
            function_name,
            calculation_modules,
        )

        return getattr(calculation_module, function_name)

    @staticmethod
    def _get_calculation_module(
        function_name: str,
        calculation_modules: CalculationModules
    ) -> types.ModuleType:
        """
        Return the first module defining a calculation function, from the custom modules and then the built-in one.

        Parameters
        ----------
        function_name
            The name of the function we're looking for
        calculation_modules
            The list of modules where we're searching for the function, in order of precedence

        Returns
        -------
        The first module in the list that defines the function

        Raises
        ------
        ColumnBuilderCustomFunctionNotFoundError
        """
        for calculation_module in calculation_modules:
            if hasattr(calculation_module, function_name):
                return calculation_module
        else:   # noqa: PLW0120
            msg = f"Custom column calculation function not found: {function_name}"
            raise ColumnBuilderCustomFunctionNotFoundError(msg)

    @staticmethod
    def _get_dated_factor_columns(
        date_fields: tuple[EntityField, ...],
        value_fields: tuple[EntityField, ...],
    ) -> dict[str, DatedFactorColumn]:
        """
        Return the dated factor columns produced by all the date and value field combinations, keyed by column name.

        Parameters
        ----------
        date_fields
            The entity date fields denoting the dates of the events
        value_fields
            The entity value fields denoting the values ascribed to each date

        Returns
        -------
        The fields of each column generated from the date/value field combinations, keyed by the column's name
        """
        return {
            f'{date_field.__name__}_{value_field.__name__}': DatedFactorColumn(
                date_field=date_field,
                value_field=value_field,
            )
            for date_field in date_fields
            for value_field in value_fields
        }

    @staticmethod
    def _get_entity_clock_spec(
        entity: BaseDataEntity,
        clock_spec_field: EntityField,
    ) -> ClockSpec:
        """
        Return the clock spec of a data block's main entity, held by the data block's clock spec field.

        Parameters
        ----------
        entity
            The data block's main entity whose clock spec we want to extract
        clock_spec_field
            The data block's main entity field holding the clock spec

        Returns
        -------
        The time scale of the entity's rows, as assigned by its data provider
        """
        return getattr(entity, clock_spec_field.__name__)

    @staticmethod
    def _get_entity_field_names(entity_class: type) -> frozenset[str]:
        """
        Return the names of all the dataclass fields of an entity class.

        Useful for validating whether a column name corresponds to an entity field.

        Parameters
        ----------
        entity_class
            The dataclass whose field names we want to collect

        Returns
        -------
        The names of all the dataclass fields of the class
        """
        return frozenset(
            field.name
            for field in dataclasses.fields(entity_class)
        )

    @staticmethod
    def _get_entity_rows(
        entity: BaseDataEntity,
        rows_field: EntityField,
    ) -> DataRows:
        """
        Return the row dict of a data block's main entity, held by the data block's rows field.

        Parameters
        ----------
        entity
            The data block's main entity whose rows we want to extract
        rows_field
            The data block's main entity field holding the rows

        Returns
        -------
        The entity's row dict
        """
        return getattr(entity, rows_field.__name__)

    @staticmethod
    def _get_field_from_row(
        row: BaseDataEntity | None,
        field: str,
        subfield: str | None = None
    ) -> typing.Any:
        """
        Extract a data entity field or subfield from a row, returning None if not exists.

        Parameters
        ----------
        row
            The row of a data block's main entity from which to extract the field or subfield, or None for a row
            without data
        field
            The name of the field
        subfield
            The name of the subfield

        Returns
        -------
        The requested field or subfield, or None if not exists
        """
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
        clock_sync_entity = master_clock_data_block.get_field_entity(clock_sync_field)
        for (prefix, entity) in master_clock_data_block.prefix_entity_map.items():
            if entity is clock_sync_entity:
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

        The field can be typed either with the subentity, or with an entity the subentity extends.

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
            if any(
                (
                    isinstance(candidate_type, type)
                    and issubclass(subentity, candidate_type)
                )
                for candidate_type in field_types
            ):
                return field_name

        msg = " ".join([
            f"Entity {parent_entity.__name__} has no field holding subentity:",
            subentity.__name__,
        ])

        raise ColumnBuilderUnavailableEntityFieldError(msg)

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
        clocked_rows_by_block: dict[type[BaseDataBlock], ClockedDataBlockRows],
        column_extractors: ColumnExtractorsByPrefix,
        master_clock_column: DataColumn,
    ) -> DataColumn:
        """
        Calculate a single column, assuming all the columns it depends on are already in completed_columns.

        Calculated columns run their precomputed calculation function, whose output gets wrapped onto the run's
        clocks. Every other column is built from its prefix's precomputed column extractor: the requested column is
        validated against the extractor's valid column names, then generated from its data block's own rows, directly
        or through the extractor's subentity field, and synced to the master clock by the clock tracking its rows.

        Parameters
        ----------
        column
            The column to calculate
        completed_columns
            The CompletedColumns containing the already calculated columns, including this column's dependencies
        calculated_columns
            The precomputed calculation functions and parameter names, keyed by calculated column identifier
        clocked_rows_by_block
            The rows of each data block that got an entity, with the clocks syncing them to the master clock
        column_extractors
            The precomputed column extractors, keyed by each data block prefix
        master_clock_column
            The master clock column of the run

        Returns
        -------
        The calculated column

        Raises
        ------
        ColumnBuilderCalculationError
        ColumnBuilderUnavailableEntityFieldError
        """
        [column_type, column_name] = column.split('_', 1)

        if column_type == cls.CALCULATED_COLUMN_PREFIX:
            calculated_column = calculated_columns[column]
            param_columns = [
                completed_columns[parameter_name]
                for parameter_name in calculated_column.parameter_names
            ]
            # Anything the calculation raises is the calculation's own error, so it gets reported as such
            try:
                calculation_output = calculated_column.function(*param_columns)
            except Exception as error:
                msg = f"Calculation {column} raised {type(error).__name__}: {error}"

                raise ColumnBuilderCalculationError(
                    msg,
                    calculation_module=calculated_column.module,
                    column=column,
                ) from error

            return cls._wrap_calculation_output(
                calculation_output,
                calculated_column=calculated_column,
                column=column,
                master_clock_column=master_clock_column,
            )

        if column_type not in column_extractors:
            msg = f"Column {column_name} with unknown prefix: {column_type}"

            raise ColumnBuilderUnavailableEntityFieldError(msg)

        column_extractor = column_extractors[column_type]

        if column_name not in column_extractor.valid_column_names:
            msg = f"Column not available under prefix '{column_type}': {column}"

            raise ColumnBuilderUnavailableEntityFieldError(msg)

        if column_extractor.data_block not in clocked_rows_by_block:
            # The data block got no entity, so its columns have no data at all
            return DataColumn(
                pyarrow.nulls(len(master_clock_column)),
                reference_clock_column=master_clock_column,
            )

        clocked_rows = clocked_rows_by_block[column_extractor.data_block]
        consolidation_method = column_extractor.consolidation_methods[column_name]

        if column_name in column_extractor.dated_factor_columns:
            dated_factor_column = column_extractor.dated_factor_columns[column_name]

            return cls._generate_column(
                clocked_rows.rows,
                dated_factor_column.value_field.__name__,
                clock_column=clocked_rows.clocks[dated_factor_column.date_field.__name__],
                consolidation_method=consolidation_method,
            )

        clock_column = clocked_rows.clocks[column_extractor.data_block.clock_sync_field.__name__]

        if column_extractor.subentity_field_name is not None:
            return cls._generate_column(
                clocked_rows.rows,
                column_extractor.subentity_field_name,
                column_name,
                clock_column=clock_column,
                consolidation_method=consolidation_method,
            )

        return cls._generate_column(
            clocked_rows.rows,
            column_name,
            clock_column=clock_column,
            consolidation_method=consolidation_method,
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
        # the passed blocks go first, followed by the built-in blocks none of them shadows:
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

    @staticmethod
    def _validate_data_block_entities(
        data_block_entities: DataBlockEntities,
        data_blocks: list[type[BaseDataBlock]],
    ) -> None:
        """
        Validate that each data entity is keyed by one of the run's data blocks, and is an instance of its main entity.

        Parameters
        ----------
        data_block_entities
            The data entities, keyed by the data block each one belongs to
        data_blocks
            The data blocks the run uses

        Raises
        ------
        InjectedDependencyError
        """
        unknown_data_block_names = [
            data_block.__name__
            for data_block in data_block_entities
            if data_block not in data_blocks
        ]
        if len(unknown_data_block_names) > 0:
            msg = " ".join([
                "The following data blocks passed to ColumnBuilder aren't among its initialized data blocks:",
                ", ".join(unknown_data_block_names),
            ])

            raise InjectedDependencyError(msg)

        mismatched_entity_descriptions = [
            f"{data_block.__name__}: {type(entity).__name__}"
            for (data_block, entity) in data_block_entities.items()
            if not isinstance(entity, data_block.main_entity)
        ]
        if len(mismatched_entity_descriptions) > 0:
            msg = " ".join([
                "The following data entities passed to ColumnBuilder aren't instances of",
                "their data block's main entity:",
                ", ".join(mismatched_entity_descriptions),
            ])

            raise InjectedDependencyError(msg)

    @staticmethod
    def _validate_reserved_prefixes(
        data_blocks: list[type[BaseDataBlock]],
        reserved_prefixes: tuple[str, ...],
    ) -> None:
        """
        Validate that none of the data blocks declares a prefix that ColumnBuilder reserves for other columns.

        Parameters
        ----------
        data_blocks
            The data blocks the run uses
        reserved_prefixes
            The prefixes that no data block can declare, like the calculated columns' one

        Raises
        ------
        InjectedDependencyError
        """
        reserved_prefix_descriptions = [
            f"{data_block.__name__}: '{prefix}'"
            for data_block in data_blocks
            for prefix in data_block.prefix_entity_map
            if prefix in reserved_prefixes
        ]
        if len(reserved_prefix_descriptions) > 0:
            msg = " ".join([
                "The following data blocks declare prefixes reserved by ColumnBuilder:",
                ", ".join(reserved_prefix_descriptions),
            ])

            raise InjectedDependencyError(msg)

    @staticmethod
    def _wrap_calculation_output(
        calculation_output: object,
        *,
        calculated_column: CalculatedColumn,
        column: ColumnIdentifier,
        master_clock_column: DataColumn,
    ) -> DataColumn:
        """
        Wrap a calculation function's output in a DataColumn synced to the run's master clock.

        A DataColumn already synced to the run's master clock stays as it is. Any other output, like a pandas Series,
        a pyarrow Array, or a DataColumn with no clock, gets loaded at the master clock's scale, so it needs exactly
        one row per master clock row.

        Parameters
        ----------
        calculation_output
            Whatever the calculation function returned
        calculated_column
            The calculated column whose function returned the output
        column
            The identifier of the calculated column
        master_clock_column
            The master clock column of the run

        Returns
        -------
        The output as a DataColumn synced to the run's master clock

        Raises
        ------
        ColumnBuilderCalculationError
            If the output is synced to another master clock, can't be loaded into a DataColumn, or doesn't have one row
            per master clock row
        """
        if isinstance(calculation_output, DataColumn):
            if calculation_output.master_clock_column is master_clock_column:
                return calculation_output

            if calculation_output.master_clock_column is not None:
                msg = f"Calculation {column} returned a column synced to a master clock from outside the run"

                raise ColumnBuilderCalculationError(
                    msg,
                    calculation_module=calculated_column.module,
                    column=column,
                )
        elif not isinstance(calculation_output, collections.abc.Iterable):
            msg = " ".join([
                f"Calculation {column} returned a {type(calculation_output).__name__},",
                "instead of an iterable with one value per master clock row",
            ])

            raise ColumnBuilderCalculationError(
                msg,
                calculation_module=calculated_column.module,
                column=column,
            )

        try:
            loaded_output = DataColumn.load(calculation_output)
        except (
            pyarrow.ArrowException,
            TypeError,
        ) as error:
            msg = " ".join([
                f"Calculation {column} returned a {type(calculation_output).__name__},",
                f"which can't be loaded into a DataColumn: {error}",
            ])

            # Not chained, as only the errors the calculation raises itself get chained
            raise ColumnBuilderCalculationError(
                msg,
                calculation_module=calculated_column.module,
                column=column,
            ) from None

        if len(loaded_output) != len(master_clock_column):
            msg = " ".join([
                f"Calculation {column} returned {len(loaded_output)} rows,",
                f"instead of one for each of the {len(master_clock_column)} master clock rows",
            ])

            raise ColumnBuilderCalculationError(
                msg,
                calculation_module=calculated_column.module,
                column=column,
            )

        return DataColumn.load(
            loaded_output,
            reference_clock_column=master_clock_column,
        )
