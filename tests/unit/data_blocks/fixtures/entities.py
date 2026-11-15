import dataclasses
import datetime
import decimal

from kaxanuk.data_curator.entities import (
    BaseDataEntity,
    ClockSpec,
)
from ...fixtures import entity_tables


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
class ExampleExtendedSubentity2(ExampleSubentity2):
    description: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubentity3(BaseDataEntity):
    id: int
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleBlockEntity(BaseDataEntity):
    clock_spec: ClockSpec
    rows: dict[str, ExampleSubentity2]
    subentity: ExampleSubentity3


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


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleDatedRow(BaseDataEntity):
    date: datetime.date
    value: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleExtendedDatedRow(ExampleDatedRow):
    extra: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleOtherExtendedDatedRow(ExampleDatedRow):
    other: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleDatedRows(BaseDataEntity):
    clock_spec: ClockSpec
    rows: dict[str, ExampleDatedRow]


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleNullableClockSpecDatedRows(BaseDataEntity):
    clock_spec: ClockSpec | None
    rows: dict[str, ExampleDatedRow]


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleLabeledRow(BaseDataEntity):
    date: datetime.date
    label: str
    value: decimal.Decimal | None


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleLabeledRows(BaseDataEntity):
    clock_spec: ClockSpec
    rows: dict[str, ExampleLabeledRow]


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleExtendedDatedRows(BaseDataEntity):
    clock_spec: ClockSpec
    rows: dict[str, ExampleExtendedDatedRow]


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleMiniFundamentalRows(BaseDataEntity):
    clock_spec: ClockSpec
    rows: dict[str, entity_tables.MiniFundamentalRow | None]


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
