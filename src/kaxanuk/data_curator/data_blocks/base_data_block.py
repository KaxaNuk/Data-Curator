import dataclasses
import datetime
import decimal
import types
import typing

import pyarrow
import pyarrow.types

from kaxanuk.data_curator.entities import BaseDataEntity
from kaxanuk.data_curator.exceptions import (
    DataBlockIncorrectMappingTypeError,
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    DataBlockIncorrectPackingStructureError,
    DataBlockRowEntityErrorGroup,
    DataBlockTypeConversionError,
    DataBlockTypeConversionNotImplementedError,
    DataBlockTypeConversionRuntimeError,
    EntityProcessingError,
    EntityTypeError,
    EntityValueError,
)


type ConsolidatedFieldsTable = pyarrow.Table    # table with consolidated data from all endpoints of a data block
type EntityBuildingTables = dict[type[BaseDataEntity], pyarrow.Table]
type EntityToClassNameMap = dict[
    str,
    type[BaseDataEntity]
]
type EntityField = types.MemberDescriptorType   # entity field
type EntityRelationsHierarchy = dict[
    tuple[
        types.MemberDescriptorType | None,
        type[BaseDataEntity]
    ],
    EntityRelationsHierarchy | None
]
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


class BaseDataBlock:
    # entity field that will be synced to the master clock:
    clock_sync_field: EntityField
    # identifier based block entities will be grouped by this field's type:
    # (the system only supports one single identifier type for grouping across all used data blocks)
    # (None means no grouping, so this data block's columns will be accessible for all identifiers)
    grouping_identifier_field: EntityField | None
    # main entity class that contains linked references to all other entities of the block:
    main_entity: type[BaseDataEntity]
    # map of entity prefixes to entity classes:
    # (must all be unique, with the shortest one being the common prefix to all the other ones)
    prefix_to_entity_map: dict[str, type[BaseDataEntity]]

    _entity_class_name_map: EntityToClassNameMap
    _ordered_entity_relations: OrderedEntityRelationMap


    def __init_subclass__(
        cls,
        /,
        **kwargs
    ):
        super().__init_subclass__(**kwargs)

        # Validate any subclass has required class variables defined
        # for var in cls.required_class_vars:
        for var in BaseDataBlock.__annotations__:
            if (
                not var.startswith("_")
                and not hasattr(cls, var)
            ):
                raise TypeError(f"{cls.__name__} must define '{var}' class variable")

        # @todo validate prefix_to_entity_map

        cls._entity_class_name_map = {
            entity.__name__: entity
            for (_, entity) in cls.prefix_to_entity_map.items()
        }

        # @todo set main_prefix ???

    @staticmethod
    def convert_value_to_type(
        original_value: typing.Any,
        to_type: typing.Any
    ) -> typing.Any:
        try:
            # if type is nullable with empty value, return None
            if (
                isinstance(to_type, types.UnionType)
                and len(to_type.__args__) == 2
                and to_type.__args__[1] == types.NoneType
            ):
                if original_value in [None, '']:
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
        return cls._split_consolidated_table_into_entity_tables(
            table,
            cls._entity_class_name_map,
        )

    @classmethod
    def get_entity_class_name_map(cls) -> EntityToClassNameMap:
        return cls._entity_class_name_map

    # @todo unit tests
    @staticmethod
    def get_field_qualified_name(field: EntityField) -> str:
        if (
            not hasattr(field, '__objclass__')
            or not hasattr(field, '__name__')
        ):
            # @todo raise specific error
            raise ValueError(
                f"Field descriptor is missing required attributes '__objclass__' or '__name__'"
            )

        return f"{field.__objclass__.__name__}.{field.__name__}"

    @classmethod
    def pack_rows_entities_from_consolidated_table(
        cls,
        /,
        table: ConsolidatedFieldsTable,
    ):
        if not getattr(cls, '_ordered_entity_relations', None):
            cls._ordered_entity_relations = cls._calculate_ordered_entity_relation_map(
                cls.main_entity
            )
        ordered_entities = cls._ordered_entity_relations

        entity_table_map = cls.create_entity_tables_from_consolidated_table(
            table=table,
        )

        row_entities = cls._pack_entity_hierarchy_rows(
            clock_sync_field=cls.clock_sync_field,
            ordered_entities=ordered_entities,
            entity_table_map=entity_table_map,
        )

        return row_entities

    @staticmethod
    def _calculate_ordered_entity_relation_map(
        main_entity: type[BaseDataEntity]
    ) -> OrderedEntityRelationMap:
        """
        Calculate a topologically sorted map of entity relations.

        Entities without BaseDataEntity subclass fields come first, followed by
        entities whose subclass fields have already been added as keys.

        Returns:
            OrderedEntityRelationMap: Dict where keys are entity classes in dependency order,
            and values are either an empty dict (no subclass fields) or a dict mapping field
            descriptors to their corresponding entity types.
        """
        # Collect all entities and their direct BaseDataEntity field dependencies
        all_entities_with_deps: dict[
            type[BaseDataEntity],
            dict[EntityField, type[BaseDataEntity]]
        ] = {}

        entities_to_process: list[type[BaseDataEntity]] = [main_entity]
        processed_entities: set[type[BaseDataEntity]] = set()

        # First pass: collect all entities and their direct dependencies
        while entities_to_process:
            entity_class = entities_to_process.pop(0)

            if entity_class in processed_entities:
                continue

            processed_entities.add(entity_class)

            if not dataclasses.is_dataclass(entity_class):
                continue

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
                entity_deps[field_descriptor] = inner_type

                # Add this entity to be processed
                if inner_type not in processed_entities:
                    entities_to_process.append(inner_type)

            # Store the dependencies (empty dict if no dependencies)
            all_entities_with_deps[entity_class] = entity_deps

        # Second pass: topologically sort entities by dependencies
        result: OrderedEntityRelationMap = {}
        remaining_entities = set(all_entities_with_deps.keys())

        while remaining_entities:
            # Find entities whose dependencies are all already in result
            ready_entities: list[type[BaseDataEntity]] = []

            for entity in remaining_entities:
                deps = all_entities_with_deps[entity]

                if not deps:
                    # No dependencies, this entity is ready
                    ready_entities.append(entity)
                else:
                    # Check if all dependencies are already in result
                    all_deps_ready = all(
                        dep_entity in result
                        for dep_entity in deps.values()
                    )
                    if all_deps_ready:
                        ready_entities.append(entity)

            if not ready_entities:
                # This shouldn't happen with valid data (would indicate circular dependency)
                break

            # Add ready entities to result in the order they were first encountered
            # (which preserves definition order due to our BFS collection above)
            for entity in ready_entities:
                result[entity] = all_entities_with_deps[entity]
                remaining_entities.remove(entity)

        return result

    # @todo unit tests
    @classmethod
    def _pack_entity_hierarchy_rows(
        cls,
        clock_sync_field: EntityField,
        ordered_entities: OrderedEntityRelationMap,
        entity_table_map: EntityBuildingTables,
    ):
        clock_type = BaseDataBlock._unwrap_optional_type(
            clock_sync_field.__objclass__.__annotations__[clock_sync_field.__name__]
        )
        clock_column = entity_table_map[clock_sync_field.__objclass__].column(
            clock_sync_field.__name__
        )
        dependency_rows = {
            entity: []
            for entity in ordered_entities.keys()
        }
        master_rows = {}
        entity_creation_exceptions = []

        for (entity, entity_dependencies) in ordered_entities.items():
            if entity not in entity_table_map:
                msg = " ".join([
                    f"Ordered entities structure contains unused entity '{entity.__name__}'",
                    f"for data block '{cls.__name__}'"
                ])

                raise DataBlockIncorrectPackingStructureError(msg)

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

            for (index, row) in enumerate(
                table.to_pylist()
            ):
                # if whole field data in row is None, pack empty entity
                if all(
                    row[field_name] is None
                    for field_name in non_key_common_field_names
                ):
                    if clock_sync_field.__objclass__ is entity:
                        date = row[clock_sync_field.__name__].isoformat()
                        master_rows[date] = None
                    else:
                        dependency_rows[entity].append(None)
                    continue

                typed_row = {
                    name: None
                    for name in missing_field_names
                }
                try:
                    for (name, value) in row.items():
                        typed_row[name] = cls.convert_value_to_type(
                            value,
                            entity.__annotations__[name]
                        )
                except DataBlockTypeConversionError as error:
                    raise DataBlockEntityPackingError(
                        entity.__name__,
                        clock_column[index].as_py()
                    ) from error
                except DataBlockTypeConversionRuntimeError as error:
                    # @todo add more specific error info if problem is None value in non-nullable field
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

                if clock_sync_field.__objclass__ is entity:
                    date = row[clock_sync_field.__name__].isoformat()
                    master_rows[date] = row_entity
                else:
                    dependency_rows[entity].append(row_entity)

            if entity_creation_exceptions:
                msg = f"Entity creation errors occured during data block '{cls.__name__}' packing"

                raise DataBlockRowEntityErrorGroup(msg, entity_creation_exceptions)

            # entity with clock sync field is the last entity to be processed
            if clock_sync_field.__objclass__ is entity:
                break

        if len(master_rows) < 1:
            msg = f"Ordered entities structure missing clock sync column for data block '{cls.__name__}'"

            raise DataBlockIncorrectPackingStructureError(msg)

        return master_rows

    @staticmethod
    def _split_consolidated_table_into_entity_tables(
        table: ConsolidatedFieldsTable,
        entity_class_name_map: EntityToClassNameMap,
    ) -> EntityBuildingTables:
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

    # @todo unit tests
    @staticmethod
    def _unwrap_dict_value_type(
        type_hint: type
    ) -> type | tuple[type, ...]:
        """Extract the value type from dict[K, V]."""
        origin_type = typing.get_origin(type_hint)
        if origin_type is dict:
            args = typing.get_args(type_hint)
            if args and len(args) >= 2:
                # Return the second argument (value type)
                return args[1]

        # If it's not a generic dict, return the type_hint as-is
        return type_hint

    @staticmethod
    def _unwrap_list_type(
        type_hint: type
    ) -> type | tuple[type, ...]:
        """Extract the element type from list[T]."""
        origin_type = typing.get_origin(type_hint)
        if origin_type is list:
            args = typing.get_args(type_hint)
            if args:
                if len(args) == 1:
                    return args[0]
                else:
                    return tuple(args)

        # If it's not a generic list, return the type_hint as-is
        return type_hint

    @staticmethod
    def _unwrap_optional_type(type_hint: type) -> type | tuple[type, ...]:
        """Extract the actual type from Optional/Union with None."""
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
