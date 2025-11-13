import dataclasses

from kaxanuk.data_curator.data_blocks.base_data_block import EntityRelations
from kaxanuk.data_curator.entities import BaseDataEntity


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubentity1(BaseDataEntity):
    id: int
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubentity2(BaseDataEntity):
    id: int
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubentity3(BaseDataEntity):
    id: int
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubentity4(BaseDataEntity):
    id: int
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleEntity(BaseDataEntity):
    id: int
    name: str
    arrayed_subentity: list[ExampleSubentity1]
    arrayed_nullable_subentity: list[ExampleSubentity2] | None
    simple_subentity: ExampleSubentity3
    simple_nullable_subentity: ExampleSubentity4 | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleNestedEntity(BaseDataEntity):
    id: int
    name: str
    entity: ExampleEntity


EXAMPLE_ENTITY_RELATIONS: EntityRelations = {
    (None, ExampleEntity): {
        (ExampleEntity.arrayed_subentity, list[ExampleSubentity1]): None,
        (ExampleEntity.arrayed_nullable_subentity, list[ExampleSubentity2]): None,
        (ExampleEntity.simple_subentity, ExampleSubentity3): None,
        (ExampleEntity.simple_nullable_subentity, ExampleSubentity4): None,
    }
}

EXAMPLE_NESTED_ENTITY_RELATIONS: EntityRelations = {
    (None, ExampleNestedEntity): {
        (ExampleNestedEntity.entity, ExampleEntity): {
            (ExampleEntity.arrayed_subentity, list[ExampleSubentity1]): None,
            (ExampleEntity.arrayed_nullable_subentity, list[ExampleSubentity2]): None,
            (ExampleEntity.simple_subentity, ExampleSubentity3): None,
            (ExampleEntity.simple_nullable_subentity, ExampleSubentity4): None,
        }
    }
}
