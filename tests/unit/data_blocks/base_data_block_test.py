

import pytest

from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from kaxanuk.data_curator.exceptions import (
    DataBlockIncorrectMappingTypeError,
)
from .fixtures import entities
from ..fixtures import entity_tables

class TestPrivateCalculateOrderedEntityRelationMap:
    def test_example_entity(self):
        result = BaseDataBlock._calculate_ordered_entity_relation_map(
            entities.ExampleEntity
        )

        assert result == entities.EXAMPLE_ENTITY_RELATIONS_LINEAR_SORT

    def test_example_nested_entity(self):
        result = BaseDataBlock._calculate_ordered_entity_relation_map(
            entities.ExampleNestedEntity
        )

        assert result == entities.EXAMPLE_NESTED_ENTITY_RELATIONS_LINEAR_SORT


class TestPrivateSplitConsolidatedTableIntoEntityTables:
    def test_existing_entities(self):
        result = BaseDataBlock._split_consolidated_table_into_entity_tables(
            entity_tables.CONSOLIDATED_TABLE,
            {
                'MiniFundamentalRow': entity_tables.MiniFundamentalRow,
                'MiniFundamentalRowBalanceSheet': entity_tables.MiniFundamentalRowBalanceSheet,
                'MiniFundamentalRowCashFlow': entity_tables.MiniFundamentalRowCashFlow,
                'MiniFundamentalRowIncomeStatement': entity_tables.MiniFundamentalRowIncomeStatement,
            }
        )
        expected = entity_tables.ENTITY_TABLES

        assert (    # noqa: PT018
            result[entity_tables.MiniFundamentalRow].equals(
                expected[entity_tables.MiniFundamentalRow]
            )
            and result[entity_tables.MiniFundamentalRowBalanceSheet].equals(
                expected[entity_tables.MiniFundamentalRowBalanceSheet]
            )
            and result[entity_tables.MiniFundamentalRowCashFlow].equals(
                expected[entity_tables.MiniFundamentalRowCashFlow]
            )
            and result[entity_tables.MiniFundamentalRowIncomeStatement].equals(
                expected[entity_tables.MiniFundamentalRowIncomeStatement]
            )
        )

    def test_inexistent_entity_fails(self):
        with pytest.raises(DataBlockIncorrectMappingTypeError):
            BaseDataBlock._split_consolidated_table_into_entity_tables(
                entity_tables.INEXISTENT_ENTITY_KEYS_TABLE,
                {
                    'MiniFundamentalRow': entity_tables.MiniFundamentalRow,
                    'MiniFundamentalRowBalanceSheet': entity_tables.MiniFundamentalRowBalanceSheet,
                    'MiniFundamentalRowCashFlow': entity_tables.MiniFundamentalRowCashFlow,
                    'MiniFundamentalRowIncomeStatement': entity_tables.MiniFundamentalRowIncomeStatement,
                }
            )

    def test_inexistent_entity_field_fails(self):
        with pytest.raises(DataBlockIncorrectMappingTypeError):
            BaseDataBlock._split_consolidated_table_into_entity_tables(
                entity_tables.INEXISTENT_ENTITY_FIELD_KEYS_TABLE,
                {
                    'MiniFundamentalRow': entity_tables.MiniFundamentalRow,
                    'MiniFundamentalRowBalanceSheet': entity_tables.MiniFundamentalRowBalanceSheet,
                    'MiniFundamentalRowCashFlow': entity_tables.MiniFundamentalRowCashFlow,
                    'MiniFundamentalRowIncomeStatement': entity_tables.MiniFundamentalRowIncomeStatement,
                }
            )

    def test_non_entity_field_key_fails(self):
        with pytest.raises(DataBlockIncorrectMappingTypeError):
            BaseDataBlock._split_consolidated_table_into_entity_tables(
                entity_tables.NON_ENTITY_FIELD_KEYS_TABLE,
                {
                    'MiniFundamentalRow': entity_tables.MiniFundamentalRow,
                    'MiniFundamentalRowBalanceSheet': entity_tables.MiniFundamentalRowBalanceSheet,
                    'MiniFundamentalRowCashFlow': entity_tables.MiniFundamentalRowCashFlow,
                    'MiniFundamentalRowIncomeStatement': entity_tables.MiniFundamentalRowIncomeStatement,
                }
            )
