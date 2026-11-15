import dataclasses
import datetime
import decimal
import types
import typing

import networkx
import pyarrow
import pyarrow.types

from kaxanuk.data_curator.entities.base_data_entity import BaseDataEntity
from kaxanuk.data_curator.entities.clock_spec import ClockSpec
from kaxanuk.data_curator.exceptions import (
    DataBlockError,
    DataBlockEntityMissingValueError,
    DataBlockEntityPackingError,
    DataBlockIncorrectMappingTypeError,
    DataBlockIncorrectPackingStructureError,
    DataBlockRowEntityErrorGroup,
    DataBlockTypeConversionError,
    DataBlockTypeConversionNotImplementedError,
    DataBlockTypeConversionRuntimeError,
    EntityTypeError,
    EntityValueError,
)
from kaxanuk.data_curator.modules.data_column import (
    ConsolidationMethod,
    DownscaleMethod,
    UpscaleMethod,
)


type ConsolidatedFieldsTable = pyarrow.Table    # table with consolidated data from all endpoints of a data block
type ConsolidationMethodMap = dict[     # entities and entity fields mapped to their consolidation methods
    type[BaseDataEntity] | EntityField,
    ConsolidationMethod
]
type EntityBuildingTables = dict[
    type[BaseDataEntity],
    pyarrow.Table
]
type EntityToClassNameMap = dict[
    str,
    type[BaseDataEntity]
]
type EntityField = types.MemberDescriptorType   # entity field
type FieldValueToEntityMap = dict[
    type[BaseDataEntity],
    dict[EntityField, typing.Any]
]
type OrderedEntityRelationMap = dict[
    type[BaseDataEntity],
    dict[
        EntityField,
        type[BaseDataEntity]
    ]
]
type PrefixEntityMap = dict[    # column prefixes mapped to the entities whose fields they hold
    str,
    type[BaseDataEntity]
]


class BaseDataBlock:
    """
    Base class for all data blocks, each of which packages the entities and column logic of a data type.

    Within a data block's own package, modules may import each other with single-dot relative imports; all
    references to the framework outside the block (entities, exceptions, services, this base class) use
    absolute imports, so the block stays self-contained and relocatable.
    """

    # method for matching each of the block's clock rows to the master clock row it belongs to,
    # used when the block's rows span less time than the master clock's:
    clock_downscale_method: DownscaleMethod
    # main entity field holding the ClockSpec of the block's rows, which the data provider assigns:
    clock_spec_field: EntityField
    # entity field that will be synced to the master clock:
    clock_sync_field: EntityField
    # method for matching each master clock row to at most one of the block's clock rows,
    # used when the block's rows span at least as long as the master clock's:
    clock_upscale_method: UpscaleMethod
    # entity date and value fields whose combinations each produce one dated-factor column;
    # empty means this block uses plain clock-synced column extraction instead:
    dated_factor_date_fields: tuple[EntityField, ...] = ()
    dated_factor_value_fields: tuple[EntityField, ...] = ()
    # methods for consolidating the block's rows belonging to each master clock row, used when the block's rows span
    # less time than the master clock's; each key is either a prefix_entity_map entity or one of its parent classes,
    # setting the default method for its fields, or a field of one of those entities, overriding the default method:
    # (an entity only needs a key of its own or of a parent class when some of its fields don't have one)
    downscale_consolidation_methods: typing.ClassVar[ConsolidationMethodMap]
    # identifier based block entities will be grouped by this field's type:
    # (the system only supports one single identifier type for grouping across all used data blocks)
    # (None means no grouping, so this data block's columns will be accessible for all identifiers)
    grouping_identifier_field: EntityField | None
    # main entity class that contains linked references to all other entities of the block:
    main_entity: type[BaseDataEntity]
    # map of entity prefixes to entity classes:
    # (without underscores, as column names get split at their first one, and with the shortest prefix starting
    # all the other ones)
    prefix_entity_map: typing.ClassVar[PrefixEntityMap]
    # main entity field holding the rows of the clock sync field's entity, keyed by their clock sync values:
    rows_field: EntityField

    _entity_class_name_map: EntityToClassNameMap
    _ordered_entity_relations: OrderedEntityRelationMap

    def __init_subclass__(
        cls,
        /,
        **kwargs    # noqa: ANN003
    ):
        super().__init_subclass__(**kwargs)

        # Validate any subclass has required class variables defined
        # for var in cls.required_class_vars:
        for var in BaseDataBlock.__annotations__:
            if (
                not var.startswith("_")
                and not hasattr(cls, var)
            ):
                msg = f"{cls.__name__} must define '{var}' class variable"

                raise TypeError(msg)

        cls._validate_clock_methods(
            clock_downscale_method=cls.clock_downscale_method,
            clock_upscale_method=cls.clock_upscale_method,
            data_block_name=cls.__name__,
        )
        # The prefix entity map goes first, as the other validations rely on its entities
        cls._validate_prefix_entity_map(
            data_block_name=cls.__name__,
            prefix_entity_map=cls.prefix_entity_map,
        )
        cls._validate_downscale_consolidation_methods(
            downscale_consolidation_methods=cls.downscale_consolidation_methods,
            data_block_name=cls.__name__,
            prefix_entity_map=cls.prefix_entity_map,
        )

        cls._entity_class_name_map = {
            entity.__name__: entity
            for (_, entity) in cls.prefix_entity_map.items()
        }
        # Calculated for each data block, so a subclass never packs its rows with its parent's entity relations
        cls._ordered_entity_relations = cls._calculate_ordered_entity_relation_map(
            cls.main_entity,
            cls.prefix_entity_map,
        )
        cls._validate_prefix_entity_reachability(
            data_block_name=cls.__name__,
            ordered_entity_relations=cls._ordered_entity_relations,
            prefix_entity_map=cls.prefix_entity_map,
        )
        # The main entity fields go last, as checking the rows field relies on the entity relations
        cls._validate_rows_field(
            clock_sync_entity=cls.get_field_entity(cls.clock_sync_field),
            data_block_name=cls.__name__,
            main_entity=cls.main_entity,
            ordered_entity_relations=cls._ordered_entity_relations,
            rows_field=cls.rows_field,
        )
        cls._validate_clock_spec_field(
            clock_spec_field=cls.clock_spec_field,
            data_block_name=cls.__name__,
            main_entity=cls.main_entity,
        )

    @staticmethod
    def convert_value_to_type(
        original_value: typing.Any,
        to_type: typing.Any
    ) -> typing.Any:
        """
        Convert a value to the specified type.

        Handles nullable types (Union with None), date conversions, Decimal conversions,
        and basic primitive type conversions. Returns None for nullable types when the
        original value is None or empty string.

        Parameters
        ----------
        original_value
            The value to be converted.
        to_type
            The target type to convert to. May be a Union type with None.

        Returns
        -------
            The converted value in the target type, or None if applicable.

        Raises
        ------
        DataBlockTypeConversionNotImplementedError
            If conversion to the target type is not implemented.
        DataBlockTypeConversionRuntimeError
            If conversion fails during runtime.
        """
        try:
            type_is_nullable = (
                isinstance(to_type, types.UnionType)
                and len(to_type.__args__) == 2  # noqa: PLR2004
                and to_type.__args__[1] == types.NoneType
            )
            if type_is_nullable:
                if original_value in [None, '']:
                    # if type is nullable with empty value, return None

                    return None
                else:
                    to_type = to_type.__args__[0]

            if type(original_value) is to_type:
                return original_value

            match to_type.__name__:
                case 'date':
                    if isinstance(original_value, datetime.datetime):
                        return original_value.date()
                    else:
                        return datetime.date.fromisoformat(original_value)
                case 'Decimal':
                    return decimal.Decimal(
                        str(original_value)
                    )
                case 'int':
                    return int(original_value)
                case 'float':
                    return float(original_value)
                case 'str':
                    return str(original_value)
                case default_type:
                    raise DataBlockTypeConversionNotImplementedError(
                        default_type,
                        original_value
                    )
        except (
            decimal.InvalidOperation,
            TypeError,
            ValueError
        ) as error:
            raise DataBlockTypeConversionRuntimeError(
                to_type.__name__,
                original_value
            ) from error

    @classmethod
    def create_entity_tables_from_consolidated_table(
        cls,
        /,
        table: ConsolidatedFieldsTable,
    ) -> EntityBuildingTables:
        """
        Create separate entity tables from a consolidated fields table.

        Splits a table with columns in 'Entity.field' format into individual tables
        per entity class, where each table contains only the fields for that entity.

        Parameters
        ----------
        table
            Table with columns in the format 'EntityName.field_name'.

        Returns
        -------
            Dictionary mapping entity classes to their corresponding tables.

        Raises
        ------
        DataBlockIncorrectMappingTypeError
            If column names are invalid or reference non-existent entities/fields.
        """
        return cls._split_consolidated_table_into_entity_tables(
            table,
            cls._entity_class_name_map,
        )

    @classmethod
    def get_downscale_consolidation_method(
        cls,
        field: EntityField,
    ) -> ConsolidationMethod:
        """
        Get the method for consolidating an entity field's values when the data block's rows get downscaled.

        The field's own key in downscale_consolidation_methods takes precedence, followed by the key of the data
        block's entity holding the field, and then the keys of that entity's parent classes, from the closest one.

        Parameters
        ----------
        field
            Entity field descriptor.

        Returns
        -------
            The field's downscale consolidation method.

        Raises
        ------
        DataBlockError
            If the field descriptor is missing required attributes, or the field has no downscale consolidation
            method, as it doesn't belong to any prefix entity.
        """
        consolidation_method = cls._find_downscale_consolidation_method(
            downscale_consolidation_methods=cls.downscale_consolidation_methods,
            entity=cls.get_field_entity(field),
            field=field,
        )
        if consolidation_method is None:
            msg = " ".join([
                f"{cls.__name__} has no downscale consolidation method for field",
                cls.get_field_qualified_name(field),
            ])

            raise DataBlockError(msg)

        return consolidation_method

    @classmethod
    def get_entity_class_name_map(cls) -> EntityToClassNameMap:
        """
        Get the mapping of entity class names to entity classes.

        Returns
        -------
            Dictionary mapping entity class names (as strings) to entity class types.
        """
        return cls._entity_class_name_map

    @classmethod
    def get_field_entity(
        cls,
        field: EntityField,
    ) -> type[BaseDataEntity]:
        """
        Get the data block's entity holding an entity field.

        That's the prefix entity that is or extends the entity declaring the field, so the fields an extension inherits
        belong to it, or otherwise the entity declaring the field itself.

        Parameters
        ----------
        field
            Entity field descriptor.

        Returns
        -------
            The entity class holding the field.

        Raises
        ------
        DataBlockError
            If the field descriptor is missing its '__objclass__' attribute.
        TypeError
            If several prefix entities extend the entity declaring the field.
        """
        if not hasattr(field, '__objclass__'):
            msg = "Field descriptor is missing required attribute '__objclass__'"

            raise DataBlockError(msg)

        return cls._resolve_block_entity(
            field.__objclass__,
            cls.prefix_entity_map,
        )

    @classmethod
    def get_field_qualified_name(
        cls,
        field: EntityField,
    ) -> str:
        """
        Get the fully qualified name of an entity field.

        Constructs the qualified name in the format 'EntityName.field_name' from a field descriptor, with the name of
        the data block's entity holding the field, so the fields an extension inherits get named after it.

        Parameters
        ----------
        field
            Entity field descriptor.

        Returns
        -------
            Qualified field name in the format 'EntityName.field_name'.

        Raises
        ------
        DataBlockError
            If the field descriptor is missing required attributes.
        """
        if (
            not hasattr(field, '__objclass__')
            or not hasattr(field, '__name__')
        ):
            msg = "Field descriptor is missing required attributes '__objclass__' or '__name__'"

            raise DataBlockError(msg)

        field_entity = cls.get_field_entity(field)

        return f"{field_entity.__name__}.{field.__name__}"

    @classmethod
    def pack_rows_entities_from_consolidated_table(
        cls,
        /,
        table: ConsolidatedFieldsTable,
    ) -> dict[
        str,
        BaseDataEntity | None
    ]:
        """
        Pack entity instances from a consolidated table into hierarchical entities.

        Converts a consolidated table into a dictionary of fully populated entity instances,
        organized by the clock sync field value. Handles entity dependencies, type conversions,
        and nullable entity fields.

        Parameters
        ----------
        table
            Table with columns in the format 'EntityName.field_name'.

        Returns
        -------
            Dictionary mapping clock sync field values (as ISO format strings) to
            instantiated main entity objects or None for rows with all null data,
            which is empty for tables without rows.

        Raises
        ------
        DataBlockEntityMissingValueError
            If a row with data has no value for one of its entity's non-nullable fields.
        DataBlockEntityPackingError
            If type conversion fails during entity creation.
        DataBlockIncorrectPackingStructureError
            If the entity structure or clock sync field is invalid.
        DataBlockRowEntityErrorGroup
            If entity instantiation fails for one or more rows.
        """
        entity_table_map = cls.create_entity_tables_from_consolidated_table(
            table=table,
        )

        try:
            row_entities = cls._pack_entity_hierarchy_rows(
                clock_sync_field=cls.clock_sync_field,
                ordered_entities=cls._ordered_entity_relations,
                entity_table_map=entity_table_map,
            )
        except (
            DataBlockEntityPackingError,
            DataBlockIncorrectPackingStructureError,
        ):
            raise
        except DataBlockRowEntityErrorGroup:
            raise

        return row_entities

    @staticmethod
    def validate_column_sorted_without_duplicates(
        column: pyarrow.Array,
        *,
        descending: bool = False,
    ) -> bool:
        """
        Validate that a column is sorted without duplicate values.

        Checks whether the column values are in strictly ascending (default) or
        strictly descending order with no duplicate values.

        Parameters
        ----------
        column
            PyArrow array to validate.
        descending
            If True, validate descending order; otherwise ascending order.

        Returns
        -------
            True if the column is sorted without duplicates, False otherwise.
        """
        if len(column) <= 1:
            return True

        # Get two shifted views of the column
        if descending:
            # For descending: each element should be > the next (strict inequality for no duplicates)
            return pyarrow.compute.all(
                pyarrow.compute.greater(column[:-1], column[1:])
            ).as_py()
        else:
            # For ascending: each element should be < the next (strict inequality for no duplicates)
            return pyarrow.compute.all(
                pyarrow.compute.less(column[:-1], column[1:])
            ).as_py()

    @staticmethod
    def _calculate_ordered_entity_relation_map(
        main_entity: type[BaseDataEntity],
        prefix_entity_map: PrefixEntityMap,
    ) -> OrderedEntityRelationMap:
        """
        Calculate a topologically sorted map of entity relations.

        Entities without BaseDataEntity subclass fields come first, followed by
        entities whose subclass fields have already been added as keys. Each entity
        a field refers to gets replaced by the prefix entity extending it, if any.

        Parameters
        ----------
        main_entity
            The main entity class from which to derive all related entities.
        prefix_entity_map
            The data block's prefixes, mapped to their entities.

        Returns
        -------
            Dictionary where keys are entity classes in dependency order,
            and values are either an empty dict (no subclass fields) or a dict mapping field
            descriptors to their corresponding entity types.

        Raises
        ------
        TypeError
            If several prefix entities extend the same entity a field refers to.
        """
        # Build a directed graph where edges point from entities to their dependencies
        # (typed with plain classes, as mypy rejects type[BaseDataEntity] for the stubs' Hashable bound on nodes)
        dependency_graph: networkx.DiGraph[type] = networkx.DiGraph()

        # Track entity dependencies (field -> entity mappings)
        entity_dependencies: dict[
            type[BaseDataEntity],
            dict[EntityField, type[BaseDataEntity]]
        ] = {}

        # Discover all entities and their dependencies using BFS
        entities_to_process: list[type[BaseDataEntity]] = [main_entity]
        processed_entities: set[type[BaseDataEntity]] = set()

        while entities_to_process:
            entity_class = entities_to_process.pop(0)

            if entity_class in processed_entities:
                continue

            processed_entities.add(entity_class)

            if not dataclasses.is_dataclass(entity_class):
                continue

            # Add entity as a node in the graph
            dependency_graph.add_node(entity_class)

            # Collect dependencies for this entity
            entity_deps: dict[EntityField, type[BaseDataEntity]] = {}
            fields = dataclasses.fields(entity_class)

            for field in fields:
                non_optional_field_type = BaseDataBlock._unwrap_optional_type(field.type)
                nondict_type = BaseDataBlock._unwrap_dict_value_type(non_optional_field_type)
                inner_type = BaseDataBlock._unwrap_optional_type(nondict_type)

                if (
                    not isinstance(inner_type, type)
                    or not issubclass(inner_type, BaseDataEntity)
                ):
                    continue

                field_descriptor = getattr(entity_class, field.name)
                # A prefix entity extending the field's entity takes its place, so its rows get packed instead
                dependency_entity = BaseDataBlock._resolve_block_entity(
                    inner_type,
                    prefix_entity_map,
                )
                entity_deps[field_descriptor] = dependency_entity

                # Add edge from entity to its dependency (entity depends on dependency_entity)
                dependency_graph.add_edge(entity_class, dependency_entity)

                # Queue dependency for processing
                if dependency_entity not in processed_entities:
                    entities_to_process.append(dependency_entity)

            entity_dependencies[entity_class] = entity_deps

        # Perform topological sort (reversed because edges point to dependencies)
        # Entities with no dependencies come first, and every node is an entity class
        sorted_entities = typing.cast(
            list[type[BaseDataEntity]],
            list(
                networkx.topological_sort(dependency_graph)
            ),
        )
        sorted_entities.reverse()

        # Build result in topological order
        result: OrderedEntityRelationMap = {
            entity: entity_dependencies[entity]
            for entity in sorted_entities
        }

        return result

    @staticmethod
    def _find_downscale_consolidation_method(
        *,
        downscale_consolidation_methods: ConsolidationMethodMap,
        entity: type[BaseDataEntity],
        field: EntityField,
    ) -> ConsolidationMethod | None:
        """
        Find the downscale consolidation method of an entity field held by an entity.

        The field's own key takes precedence, followed by the keys of the entity and its parent classes, from the
        closest one.

        Parameters
        ----------
        downscale_consolidation_methods
            The data block's entities and entity fields, mapped to their downscale consolidation methods.
        entity
            The data block's entity holding the field.
        field
            Entity field descriptor.

        Returns
        -------
            The field's downscale consolidation method, or None if it has none.
        """
        if field in downscale_consolidation_methods:
            return downscale_consolidation_methods[field]

        for candidate_entity in entity.__mro__:
            if candidate_entity in downscale_consolidation_methods:
                return downscale_consolidation_methods[candidate_entity]

        return None

    @staticmethod
    def _is_nullable_type(type_hint: object) -> bool:
        """
        Check whether a type hint accepts None, by being a union that includes it.

        Parameters
        ----------
        type_hint
            Type hint to check, which can be any annotation object, like a class, a union or a generic alias.

        Returns
        -------
            Whether the type hint accepts None.
        """
        origin_type = typing.get_origin(type_hint)
        is_union = (
            origin_type is typing.Union
            or origin_type is types.UnionType
        )

        return (
            is_union
            and types.NoneType in typing.get_args(type_hint)
        )

    @classmethod
    def _pack_entity_hierarchy_rows(
        cls,
        clock_sync_field: EntityField,
        ordered_entities: OrderedEntityRelationMap,
        entity_table_map: EntityBuildingTables,
    ) -> dict[
        str,
        BaseDataEntity | None
    ]:
        """
        Pack entity instances in hierarchical order from entity tables.

        Processes entities in dependency order, instantiating child entities before
        parents and linking them through field references. Handles nullable entities
        by creating None values when all non-key fields are None.

        Parameters
        ----------
        clock_sync_field
            Entity field used as the master clock for synchronization.
        ordered_entities
            Topologically sorted map of entities and their dependencies.
        entity_table_map
            Dictionary mapping entity classes to their data tables.

        Returns
        -------
            Dictionary mapping clock sync field values (as ISO format strings) to
            instantiated entity objects or None, which is empty for tables without rows.

        Raises
        ------
        DataBlockIncorrectPackingStructureError
            If the ordered entities structure is invalid or missing the clock sync field.
        DataBlockEntityMissingValueError
            If a row with data has no value for one of its entity's non-nullable fields.
        DataBlockEntityPackingError
            If type conversion fails during entity creation.
        DataBlockRowEntityErrorGroup
            If entity instantiation fails for one or more rows.
        """
        # The clock sync field can be inherited by an extension of the entity declaring it
        clock_sync_entity = cls.get_field_entity(clock_sync_field)
        if clock_sync_entity not in entity_table_map:
            msg = " ".join([
                f"Clock sync field '{clock_sync_field.__name__}'",
                f"not found in entity '{clock_sync_entity.__name__}'",
                f"for data block '{cls.__name__}'"
            ])

            raise DataBlockIncorrectPackingStructureError(msg)

        clock_column = entity_table_map[clock_sync_entity].column(
            clock_sync_field.__name__
        )

        # A table without rows has nothing to pack, which the data blocks then report as empty
        if len(clock_column) < 1:
            return {}

        dependency_rows: dict[
            type[BaseDataEntity],
            list[BaseDataEntity | None]
        ] = {
            entity: []
            for entity in ordered_entities
        }
        master_rows: dict[str, BaseDataEntity | None] = {}
        entity_creation_exceptions = []

        for (entity, entity_dependencies) in ordered_entities.items():
            if entity not in entity_table_map:
                # Entity has no columns in the consolidated table (e.g. a
                # fundamental endpoint returned zero records). Pack as None for
                # every clock row; parent entities must declare this child as
                # Optional to accept the null.
                dependency_rows[entity] = [None] * len(clock_column)

                continue

            table = entity_table_map[entity]
            column_names = set(table.column_names)
            row_field_names = {
                field.name
                for field in dataclasses.fields(entity)
            }

            dependency_field_names = {
                field.__name__
                for (field, _) in entity_dependencies.items()
            }
            missing_field_names = (
                row_field_names
                - column_names
                - dependency_field_names
            )
            non_key_common_field_names = (
                row_field_names
                - dependency_field_names
                - missing_field_names
                - {clock_sync_field.__name__}
            )
            # Unlike the entity's own annotations, its type hints also include the fields it inherits
            field_types = typing.get_type_hints(entity)

            for (index, row) in enumerate(
                table.to_pylist()
            ):
                # if whole field data in row is None, pack empty entity
                if all(
                    row[field_name] is None
                    for field_name in non_key_common_field_names
                ):
                    if clock_sync_entity is entity:
                        date = row[clock_sync_field.__name__].isoformat()
                        master_rows[date] = None
                    else:
                        dependency_rows[entity].append(None)
                    continue

                typed_row = dict.fromkeys(missing_field_names)
                try:
                    for (name, value) in row.items():
                        # Checked before converting, as some conversions would turn None into a value, like "None"
                        if (
                            value is None
                            and not cls._is_nullable_type(field_types[name])
                        ):
                            raise DataBlockEntityMissingValueError(
                                entity.__name__,
                                clock_column[index].as_py(),
                                name,
                            )

                        typed_row[name] = cls.convert_value_to_type(
                            value,
                            field_types[name]
                        )
                except DataBlockTypeConversionError as error:
                    raise DataBlockEntityPackingError(
                        entity.__name__,
                        clock_column[index].as_py()
                    ) from error
                except DataBlockTypeConversionRuntimeError as error:
                    raise DataBlockEntityPackingError(
                        entity.__name__,
                        clock_column[index].as_py()
                    ) from error

                # add dependency rows to typed row
                for (field, dependency_entity) in entity_dependencies.items():
                    typed_row[field.__name__] = dependency_rows[dependency_entity][index]

                try:
                    row_entity = entity(**typed_row)
                except (
                    EntityTypeError,
                    EntityValueError,
                ) as error:
                    entity_creation_exceptions.append(error)

                    continue

                if clock_sync_entity is entity:
                    date = row[clock_sync_field.__name__].isoformat()
                    master_rows[date] = row_entity
                else:
                    dependency_rows[entity].append(row_entity)

            if entity_creation_exceptions:
                msg = f"Entity creation errors occured during data block '{cls.__name__}' packing"

                raise DataBlockRowEntityErrorGroup(msg, entity_creation_exceptions)

            # entity with clock sync field is the last entity to be processed
            if clock_sync_entity is entity:
                break

        if len(master_rows) < 1:
            msg = f"Ordered entities structure missing clock sync column for data block '{cls.__name__}'"

            raise DataBlockIncorrectPackingStructureError(msg)

        return master_rows

    @staticmethod
    def _resolve_block_entity(
        entity: type[BaseDataEntity],
        prefix_entity_map: PrefixEntityMap,
    ) -> type[BaseDataEntity]:
        """
        Resolve the entity a data block uses in place of an entity.

        A prefix entity is used as itself, while any other entity gets replaced by the prefix entity extending it, so
        a data block subclass can extend its parent's entities just by mapping their prefixes to the extensions. The
        entities without any prefix entity extending them are used as themselves.

        Parameters
        ----------
        entity
            The entity class to resolve.
        prefix_entity_map
            The data block's prefixes, mapped to their entities.

        Returns
        -------
            The prefix entity that is or extends the entity, or otherwise the entity itself.

        Raises
        ------
        TypeError
            If several prefix entities extend the entity.
        """
        # The same entity can be mapped to several prefixes
        prefix_entities = list(
            dict.fromkeys(
                prefix_entity_map.values()
            )
        )

        if entity in prefix_entities:
            return entity

        extending_prefix_entities = [
            prefix_entity
            for prefix_entity in prefix_entities
            if issubclass(prefix_entity, entity)
        ]
        if len(extending_prefix_entities) > 1:
            msg = " ".join([
                f"Several prefix entities extend {entity.__name__}, so it can't be replaced by a single one:",
                ", ".join(
                    extending_prefix_entity.__name__
                    for extending_prefix_entity in extending_prefix_entities
                ),
            ])

            raise TypeError(msg)

        if len(extending_prefix_entities) == 1:
            return extending_prefix_entities[0]

        return entity

    @staticmethod
    def _split_consolidated_table_into_entity_tables(
        table: ConsolidatedFieldsTable,
        entity_class_name_map: EntityToClassNameMap,
    ) -> EntityBuildingTables:
        """
        Split a consolidated table into separate entity tables.

        Parses column names in 'EntityName.field_name' format and groups columns
        by entity class, validating that all referenced entities and fields exist.

        Parameters
        ----------
        table
            Table with columns in the format 'EntityName.field_name'.
        entity_class_name_map
            Dictionary mapping entity class names to entity class types.

        Returns
        -------
            Dictionary mapping entity classes to their corresponding tables.

        Raises
        ------
        DataBlockIncorrectMappingTypeError
            If column names are malformed, reference non-existent entities,
            entity classes are not dataclasses, or fields don't exist on entities.
        """
        entity_columns: dict[type[BaseDataEntity], dict[str, pyarrow.Array]] = {}
        for column_name in table.column_names:
            try:
                entity_name, field_name = column_name.split('.', 1)
            except ValueError:
                msg = f"Invalid column name format: '{column_name}'. Expected 'Entity.field'."

                raise DataBlockIncorrectMappingTypeError(msg) from None

            entity_class = entity_class_name_map.get(entity_name)
            if not entity_class:
                msg = f"Entity '{entity_name}' from column '{column_name}' not found."

                raise DataBlockIncorrectMappingTypeError(msg)

            if not dataclasses.is_dataclass(entity_class):
                msg = f"Entity class '{entity_name}' is not a dataclass."

                raise DataBlockIncorrectMappingTypeError(msg)

            entity_fields = {f.name for f in dataclasses.fields(entity_class)}
            if field_name not in entity_fields:
                msg = (
                    f"Field '{field_name}' not found in entity '{entity_name}' for column '{column_name}'"
                )

                raise DataBlockIncorrectMappingTypeError(msg)

            if entity_class not in entity_columns:
                entity_columns[entity_class] = {}

            entity_columns[entity_class][field_name] = table.column(column_name)

        entity_tables: EntityBuildingTables = {
            entity_class: pyarrow.table(columns)
            for (entity_class, columns) in entity_columns.items()
        }

        return entity_tables

    @staticmethod
    def _unwrap_dict_value_type(
        type_hint: object
    ) -> object:
        """
        Extract the value type from a dict type annotation.

        Extracts the second type argument from a dict generic type annotation.
        Returns the type hint unchanged if it's not a dict type.

        Parameters
        ----------
        type_hint
            Type hint to unwrap, which can be any annotation object, like a class, a union or a generic alias.

        Returns
        -------
            The value type from dict[key, value], or the original type hint if not a dict.
        """
        origin_type = typing.get_origin(type_hint)
        if origin_type is dict:
            args = typing.get_args(type_hint)
            if args and len(args) > 1:
                # Return the second argument (value type)
                return args[1]

        # If it's not a generic dict, return the type_hint as-is
        return type_hint

    @staticmethod
    def _unwrap_optional_type(type_hint: object) -> object:
        """
        Extract the actual type from Optional/Union with None.

        Removes NoneType from Union type annotations to get the underlying type(s).
        Returns the type hint unchanged if it's not a Union type.

        Parameters
        ----------
        type_hint
            Type hint to unwrap, which can be any annotation object, like a class, a union or a generic alias.

        Returns
        -------
            The non-None type from Optional[T] or Union[T, None], tuple of types
            if multiple non-None types exist, or the original type hint if not a Union.
        """
        origin_type = typing.get_origin(type_hint)
        if (
            origin_type is typing.Union
            or origin_type is types.UnionType
        ):
            args = typing.get_args(type_hint)
            # Filter out NoneType
            non_none_args = [
                arg
                for arg in args
                if arg is not type(None)
            ]
            if len(non_none_args) == 1:
                return non_none_args[0]
            else:
                return tuple(non_none_args)

        return type_hint

    @staticmethod
    def _validate_clock_methods(
        *,
        clock_downscale_method: DownscaleMethod,
        clock_upscale_method: UpscaleMethod,
        data_block_name: str,
    ) -> None:
        """
        Validate that the data block's methods for syncing its clock to the master clock are callable.

        Parameters
        ----------
        clock_downscale_method
            The data block's method for matching each of its clock rows to the master clock row it belongs to
        clock_upscale_method
            The data block's method for matching each master clock row to at most one of its clock rows
        data_block_name
            The name of the data block, for the error messages

        Raises
        ------
        TypeError
        """
        if not callable(clock_downscale_method):
            msg = f"{data_block_name} clock_downscale_method must be callable"

            raise TypeError(msg)

        if not callable(clock_upscale_method):
            msg = f"{data_block_name} clock_upscale_method must be callable"

            raise TypeError(msg)

    @staticmethod
    def _validate_clock_spec_field(
        *,
        clock_spec_field: EntityField,
        data_block_name: str,
        main_entity: type[BaseDataEntity],
    ) -> None:
        """
        Validate that the clock spec field is a field of the main entity, typed as a ClockSpec that can't be None.

        Parameters
        ----------
        clock_spec_field
            The main entity field holding the ClockSpec of the data block's rows
        data_block_name
            The name of the data block, for the error messages
        main_entity
            The data block's main entity

        Raises
        ------
        TypeError
        """
        main_entity_fields = [
            getattr(main_entity, field.name)
            for field in dataclasses.fields(main_entity)
        ]
        if clock_spec_field not in main_entity_fields:
            msg = f"{data_block_name} clock_spec_field must be a field of its main_entity, {main_entity.__name__}"

            raise TypeError(msg)

        field_type = typing.get_type_hints(main_entity)[clock_spec_field.__name__]
        if field_type is not ClockSpec:
            msg = " ".join([
                f"{data_block_name} clock_spec_field must be typed as ClockSpec, without allowing None,",
                f"instead of {field_type}",
            ])

            raise TypeError(msg)

    @staticmethod
    def _validate_downscale_consolidation_methods(
        *,
        downscale_consolidation_methods: ConsolidationMethodMap,
        data_block_name: str,
        prefix_entity_map: PrefixEntityMap,
    ) -> None:
        """
        Validate that the data block's consolidation methods cover every field of its prefix entities.

        Each key needs to be either an entity of the prefix entity map or one of its parent classes other than
        BaseDataEntity, setting the default method for its fields, or a field of one of those entities, overriding the
        default method, while each value needs to be callable. Every field of each prefix entity needs a method, either
        through its own key, or through the key of its entity or of one of its entity's parent classes.

        Parameters
        ----------
        downscale_consolidation_methods
            The data block's entities and entity fields, mapped to their consolidation methods
        data_block_name
            The name of the data block, for the error messages
        prefix_entity_map
            The data block's prefixes, mapped to their entities

        Raises
        ------
        TypeError
        """
        if not isinstance(downscale_consolidation_methods, dict):
            msg = f"{data_block_name} downscale_consolidation_methods must be a dict"

            raise TypeError(msg)

        noncallable_method_keys = [
            repr(key)
            for (key, method) in downscale_consolidation_methods.items()
            if not callable(method)
        ]
        if len(noncallable_method_keys) > 0:
            msg = " ".join([
                f"{data_block_name} downscale_consolidation_methods has non-callable methods for these keys:",
                ", ".join(noncallable_method_keys),
            ])

            raise TypeError(msg)

        # As every entity extends BaseDataEntity, its key would silently set the method of every field
        if BaseDataEntity in downscale_consolidation_methods:
            msg = " ".join([
                f"{data_block_name} downscale_consolidation_methods can't have BaseDataEntity as a key,",
                "as it would set the method of every field",
            ])

            raise TypeError(msg)

        # The same entity can be mapped to several prefixes
        prefix_entities = list(
            dict.fromkeys(
                prefix_entity_map.values()
            )
        )
        prefix_entity_classes = {
            entity_class
            for entity in prefix_entities
            for entity_class in entity.__mro__
            if (
                issubclass(entity_class, BaseDataEntity)
                and entity_class is not BaseDataEntity
            )
        }
        prefix_entity_fields = {
            getattr(entity, field.name)
            for entity in prefix_entities
            for field in dataclasses.fields(entity)
        }
        unknown_keys = [
            repr(key)
            for key in downscale_consolidation_methods
            if (
                key not in prefix_entity_classes
                and key not in prefix_entity_fields
            )
        ]
        if len(unknown_keys) > 0:
            msg = " ".join([
                f"{data_block_name} downscale_consolidation_methods has keys that are neither",
                "prefix_entity_map entities, their parent classes, nor their fields:",
                ", ".join(unknown_keys),
            ])

            raise TypeError(msg)

        fields_without_methods = [
            f"{entity.__name__}.{field.name}"
            for entity in prefix_entities
            for field in dataclasses.fields(entity)
            if (
                BaseDataBlock._find_downscale_consolidation_method(
                    downscale_consolidation_methods=downscale_consolidation_methods,
                    entity=entity,
                    field=getattr(
                        entity,
                        field.name,
                    ),
                )
                is None
            )
        ]
        if len(fields_without_methods) > 0:
            msg = " ".join([
                f"{data_block_name} downscale_consolidation_methods has no method for these fields,",
                "nor for their entities:",
                ", ".join(fields_without_methods),
            ])

            raise TypeError(msg)

    @staticmethod
    def _validate_prefix_entity_map(
        *,
        data_block_name: str,
        prefix_entity_map: PrefixEntityMap,
    ) -> None:
        """
        Validate that the data block's prefixes can start column names, and that each one maps to an entity.

        The prefix entity map needs to be a non-empty dict, whose prefixes are non-empty strings without underscores,
        as column names get split at their first one, and whose values are BaseDataEntity subclasses. The shortest
        prefix also needs to start all the other ones, like f does for fbs, fcf and fis.

        Parameters
        ----------
        data_block_name
            The name of the data block, for the error messages
        prefix_entity_map
            The data block's prefixes, mapped to their entities

        Raises
        ------
        TypeError
        """
        if (
            not isinstance(prefix_entity_map, dict)
            or len(prefix_entity_map) < 1
        ):
            msg = f"{data_block_name} prefix_entity_map must be a non-empty dict"

            raise TypeError(msg)

        invalid_prefixes = [
            repr(prefix)
            for prefix in prefix_entity_map
            if (
                not isinstance(prefix, str)
                or len(prefix) < 1
                or '_' in prefix
            )
        ]
        if len(invalid_prefixes) > 0:
            msg = " ".join([
                f"{data_block_name} prefix_entity_map prefixes must be non-empty strings without underscores,",
                "as column names get split at their first one:",
                ", ".join(invalid_prefixes),
            ])

            raise TypeError(msg)

        non_entity_prefixes = [
            repr(prefix)
            for (prefix, entity) in prefix_entity_map.items()
            if (
                not isinstance(entity, type)
                or not issubclass(entity, BaseDataEntity)
            )
        ]
        if len(non_entity_prefixes) > 0:
            msg = " ".join([
                f"{data_block_name} prefix_entity_map maps these prefixes to something other than",
                "a BaseDataEntity subclass:",
                ", ".join(non_entity_prefixes),
            ])

            raise TypeError(msg)

        shortest_prefix = min(
            prefix_entity_map,
            key=len,
        )
        prefixes_without_shortest_prefix = [
            repr(prefix)
            for prefix in prefix_entity_map
            if not prefix.startswith(shortest_prefix)
        ]
        if len(prefixes_without_shortest_prefix) > 0:
            msg = " ".join([
                f"{data_block_name} prefix_entity_map prefixes must all start with the shortest one,",
                f"{shortest_prefix!r}, unlike these:",
                ", ".join(prefixes_without_shortest_prefix),
            ])

            raise TypeError(msg)

    @staticmethod
    def _validate_prefix_entity_reachability(
        *,
        data_block_name: str,
        ordered_entity_relations: OrderedEntityRelationMap,
        prefix_entity_map: PrefixEntityMap,
    ) -> None:
        """
        Validate that the main entity reaches every prefix entity, as the unreachable ones can't get packed.

        A prefix entity is unreachable when no field of the main entity's hierarchy refers to it, nor to an entity it
        extends, or when a prefix entity it extends is also mapped, taking its place.

        Parameters
        ----------
        data_block_name
            The name of the data block, for the error messages
        ordered_entity_relations
            The entities reached from the main entity, mapped to their relations
        prefix_entity_map
            The data block's prefixes, mapped to their entities

        Raises
        ------
        TypeError
        """
        unreachable_prefix_entities = [
            f"'{prefix}': {entity.__name__}"
            for (prefix, entity) in prefix_entity_map.items()
            if entity not in ordered_entity_relations
        ]
        if len(unreachable_prefix_entities) > 0:
            msg = " ".join([
                f"{data_block_name} main_entity doesn't reach these prefix_entity_map entities,",
                "so their rows can't get packed:",
                ", ".join(unreachable_prefix_entities),
            ])

            raise TypeError(msg)

    @staticmethod
    def _validate_rows_field(
        *,
        clock_sync_entity: type[BaseDataEntity],
        data_block_name: str,
        main_entity: type[BaseDataEntity],
        ordered_entity_relations: OrderedEntityRelationMap,
        rows_field: EntityField,
    ) -> None:
        """
        Validate that the rows field is the main entity field holding the rows of the clock sync field's entity.

        Parameters
        ----------
        clock_sync_entity
            The data block's entity holding the clock sync field
        data_block_name
            The name of the data block, for the error messages
        main_entity
            The data block's main entity
        ordered_entity_relations
            The entities reached from the main entity, mapped to their relations
        rows_field
            The main entity field holding the rows

        Raises
        ------
        TypeError
        """
        main_entity_relations = ordered_entity_relations.get(main_entity, {})
        held_entity = next(
            (
                entity
                for (field, entity) in main_entity_relations.items()
                if field is rows_field
            ),
            None,
        )
        if held_entity is not clock_sync_entity:
            msg = " ".join([
                f"{data_block_name} rows_field must be the field of its main_entity, {main_entity.__name__},",
                f"holding the rows of its clock sync field's entity, {clock_sync_entity.__name__}",
            ])

            raise TypeError(msg)
