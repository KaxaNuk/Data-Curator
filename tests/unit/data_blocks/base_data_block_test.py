

import pytest

from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from .fixtures.entities import (
    ExampleEntity,
    ExampleNestedEntity,
    ExampleSubentity1,
    ExampleSubentity2,
    EXAMPLE_ENTITY_RELATIONS,
    EXAMPLE_NESTED_ENTITY_RELATIONS,
)


class TestCalculateEntityRelations:
    def test_calculate_entity_relations_nested(self):
        result = BaseDataBlock._calculate_entity_relations(
            ExampleNestedEntity
        )

        assert result == EXAMPLE_NESTED_ENTITY_RELATIONS

    def test_calculate_entity_relations_nonnested(self):
        result = BaseDataBlock._calculate_entity_relations(
            ExampleEntity
        )

        assert result == EXAMPLE_ENTITY_RELATIONS

