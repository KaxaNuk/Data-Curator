import dataclasses

from kaxanuk.data_curator.entities import BaseDataEntity


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubSubentity1(BaseDataEntity):
    id: int
    name: str

@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubentity1(BaseDataEntity):
    id: int
    name: str
    subsubentity: ExampleSubSubentity1


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
    # arrayed_subentity: list[ExampleSubentity1]
    # arrayed_nullable_subentity: list[ExampleSubentity2] | None
    simple_subentity: ExampleSubentity3
    simple_nullable_subentity: ExampleSubentity4 | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleNestedEntity(BaseDataEntity):
    id: int
    name: str
    entity: ExampleEntity


EXAMPLE_ENTITY_RELATIONS_LINEAR_SORT = {
    ExampleSubentity3: {},
    ExampleSubentity4: {},
    ExampleEntity: {
        ExampleEntity.simple_subentity: ExampleSubentity3,
        ExampleEntity.simple_nullable_subentity: ExampleSubentity4,
    },
}
EXAMPLE_NESTED_ENTITY_RELATIONS_LINEAR_SORT = {
    ExampleSubentity3: {},
    ExampleSubentity4: {},
    ExampleEntity: {
        ExampleEntity.simple_subentity: ExampleSubentity3,
        ExampleEntity.simple_nullable_subentity: ExampleSubentity4,
    },
    ExampleNestedEntity: {
        ExampleNestedEntity.entity: ExampleEntity
    }
}
