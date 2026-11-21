import datetime
import decimal
import typing

import pyarrow
import pytest

from kaxanuk.data_curator import clock_sync_methods
from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import (
    MarketDailyDataBlock,
    MarketDataDailyRow,
)
from kaxanuk.data_curator.entities import BaseDataEntity
from kaxanuk.data_curator.exceptions import (
    DataBlockEntityMissingValueError,
    DataBlockEntityPackingError,
    DataBlockError,
    DataBlockIncorrectMappingTypeError,
    DataBlockIncorrectPackingStructureError,
    DataBlockRowEntityErrorGroup,
    DataBlockTypeConversionRuntimeError,
)
from .fixtures import entities
from ..fixtures import entity_tables


class ExampleDataBlock(BaseDataBlock):
    """
    Complete data block declaration, which the tests can subclass to override a single attribute.
    """

    clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
    clock_spec_field = entities.ExampleBlockEntity.clock_spec
    clock_sync_field = entities.ExampleSubentity2.id
    clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
    # one entity sets the default method of its fields, while the other one has all its fields listed:
    downscale_consolidation_methods: typing.ClassVar = {
        entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
        entities.ExampleSubentity3.id: clock_sync_methods.ConsolidationMethods.last,
        entities.ExampleSubentity3.name: clock_sync_methods.ConsolidationMethods.first,
    }
    grouping_identifier_field = None
    main_entity = entities.ExampleBlockEntity
    prefix_entity_map: typing.ClassVar = {
        'example': entities.ExampleSubentity2,
        'examplesub': entities.ExampleSubentity3,
    }
    rows_field = entities.ExampleBlockEntity.rows


class ExampleDatedDataBlock(BaseDataBlock):
    """
    Data block packing dated rows, which the tests can subclass to pack an extension of the rows' entity.
    """

    clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
    clock_spec_field = entities.ExampleDatedRows.clock_spec
    clock_sync_field = entities.ExampleDatedRow.date
    clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
    downscale_consolidation_methods: typing.ClassVar = {
        entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
    }
    grouping_identifier_field = None
    main_entity = entities.ExampleDatedRows
    prefix_entity_map: typing.ClassVar = {
        'dated': entities.ExampleDatedRow,
    }
    rows_field = entities.ExampleDatedRows.rows


class ExampleDatedExtensionDataBlock(ExampleDatedDataBlock):
    """
    Data block replacing its parent data block's row entity with an extension, which keeps everything else.
    """

    prefix_entity_map: typing.ClassVar = {
        'dated': entities.ExampleExtendedDatedRow,
    }


class ExampleExtendedDatedDataBlock(BaseDataBlock):
    """
    Data block packing the rows of an entity extending another one, without subclassing another data block.
    """

    clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
    clock_spec_field = entities.ExampleExtendedDatedRows.clock_spec
    clock_sync_field = entities.ExampleExtendedDatedRow.date
    clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
    downscale_consolidation_methods: typing.ClassVar = {
        entities.ExampleExtendedDatedRow: clock_sync_methods.ConsolidationMethods.last,
    }
    grouping_identifier_field = None
    main_entity = entities.ExampleExtendedDatedRows
    prefix_entity_map: typing.ClassVar = {
        'dated': entities.ExampleExtendedDatedRow,
    }
    rows_field = entities.ExampleExtendedDatedRows.rows


class ExampleLabeledDataBlock(BaseDataBlock):
    """
    Data block packing rows with a non-nullable field besides their clock sync field.
    """

    clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
    clock_spec_field = entities.ExampleLabeledRows.clock_spec
    clock_sync_field = entities.ExampleLabeledRow.date
    clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
    downscale_consolidation_methods: typing.ClassVar = {
        entities.ExampleLabeledRow: clock_sync_methods.ConsolidationMethods.last,
    }
    grouping_identifier_field = None
    main_entity = entities.ExampleLabeledRows
    prefix_entity_map: typing.ClassVar = {
        'labeled': entities.ExampleLabeledRow,
    }
    rows_field = entities.ExampleLabeledRows.rows


class ExampleMiniFundamentalsDataBlock(BaseDataBlock):
    """
    Data block packing rows with subentities, made of the shared mini fundamental entities.
    """

    clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
    clock_spec_field = entities.ExampleMiniFundamentalRows.clock_spec
    clock_sync_field = entity_tables.MiniFundamentalRow.filing_date
    clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
    downscale_consolidation_methods: typing.ClassVar = {
        entity_tables.MiniFundamentalRow: clock_sync_methods.ConsolidationMethods.last,
        entity_tables.MiniFundamentalRowBalanceSheet: clock_sync_methods.ConsolidationMethods.last,
        entity_tables.MiniFundamentalRowCashFlow: clock_sync_methods.ConsolidationMethods.sum,
        entity_tables.MiniFundamentalRowIncomeStatement: clock_sync_methods.ConsolidationMethods.sum,
    }
    grouping_identifier_field = None
    main_entity = entities.ExampleMiniFundamentalRows
    prefix_entity_map: typing.ClassVar = {
        'f': entity_tables.MiniFundamentalRow,
        'fbs': entity_tables.MiniFundamentalRowBalanceSheet,
        'fcf': entity_tables.MiniFundamentalRowCashFlow,
        'fis': entity_tables.MiniFundamentalRowIncomeStatement,
    }
    rows_field = entities.ExampleMiniFundamentalRows.rows


@pytest.fixture
def example_prefix_entity_map():
    return {
        'example': entities.ExampleSubentity2,
        'examplesub': entities.ExampleSubentity3,
    }


@pytest.fixture
def extended_dated_table():
    return pyarrow.table({
        'ExampleExtendedDatedRow.date': [
            datetime.date(2024, 1, 30),
        ],
        'ExampleExtendedDatedRow.value': [
            1.5,
        ],
        'ExampleExtendedDatedRow.extra': [
            3,
        ],
    })


@pytest.fixture
def mini_fundamental_entity_relations():
    return {
        entity_tables.MiniFundamentalRowBalanceSheet: {},
        entity_tables.MiniFundamentalRowCashFlow: {},
        entity_tables.MiniFundamentalRowIncomeStatement: {},
        entity_tables.MiniFundamentalRow: {
            entity_tables.MiniFundamentalRow.balance_sheet: entity_tables.MiniFundamentalRowBalanceSheet,
            entity_tables.MiniFundamentalRow.cash_flow: entity_tables.MiniFundamentalRowCashFlow,
            entity_tables.MiniFundamentalRow.income_statement: entity_tables.MiniFundamentalRowIncomeStatement,
        },
    }


@pytest.fixture
def mini_fundamental_entity_tables():
    # The second date's balance sheet has no data at all
    return {
        entity_tables.MiniFundamentalRow: pyarrow.table({
            'accepted_date': [
                datetime.datetime(2024, 1, 30, 16),
                datetime.datetime(2024, 1, 31, 16),
            ],
            'filing_date': [
                datetime.date(2024, 1, 30),
                datetime.date(2024, 1, 31),
            ],
        }),
        entity_tables.MiniFundamentalRowBalanceSheet: pyarrow.table({
            'assets': pyarrow.array(
                [1.5, None],
                type=pyarrow.float64(),
            ),
            'cash_and_cash_equivalents': pyarrow.array(
                [2.5, None],
                type=pyarrow.float64(),
            ),
        }),
        entity_tables.MiniFundamentalRowCashFlow: pyarrow.table({
            'net_income': [
                3.5,
                4.5,
            ],
            'net_income_tax_payments': pyarrow.array(
                [None, 0.5],
                type=pyarrow.float64(),
            ),
        }),
        entity_tables.MiniFundamentalRowIncomeStatement: pyarrow.table({
            'cost_of_revenue': [
                5.5,
                6.5,
            ],
            'net_income': [
                3.5,
                4.5,
            ],
        }),
    }


class TestDunderInitSubclass:
    def test_complete_declaration(self):
        result = ExampleDataBlock.get_entity_class_name_map()
        expected = {
            'ExampleSubentity2': entities.ExampleSubentity2,
            'ExampleSubentity3': entities.ExampleSubentity3,
        }

        assert result == expected

    def test_missing_clock_downscale_method(self):
        with pytest.raises(TypeError):
            class IncompleteDataBlock(BaseDataBlock):
                clock_spec_field = entities.ExampleBlockEntity.clock_spec
                clock_sync_field = entities.ExampleSubentity2.id
                clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                }
                grouping_identifier_field = None
                main_entity = entities.ExampleBlockEntity
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                }
                rows_field = entities.ExampleBlockEntity.rows

    def test_missing_clock_spec_field(self):
        with pytest.raises(TypeError):
            class IncompleteDataBlock(BaseDataBlock):
                clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
                clock_sync_field = entities.ExampleSubentity2.id
                clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                }
                grouping_identifier_field = None
                main_entity = entities.ExampleBlockEntity
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                }
                rows_field = entities.ExampleBlockEntity.rows

    def test_missing_clock_upscale_method(self):
        with pytest.raises(TypeError):
            class IncompleteDataBlock(BaseDataBlock):
                clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
                clock_spec_field = entities.ExampleBlockEntity.clock_spec
                clock_sync_field = entities.ExampleSubentity2.id
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                }
                grouping_identifier_field = None
                main_entity = entities.ExampleBlockEntity
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                }
                rows_field = entities.ExampleBlockEntity.rows

    def test_missing_downscale_consolidation_methods(self):
        with pytest.raises(TypeError):
            class IncompleteDataBlock(BaseDataBlock):
                clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
                clock_spec_field = entities.ExampleBlockEntity.clock_spec
                clock_sync_field = entities.ExampleSubentity2.id
                clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
                grouping_identifier_field = None
                main_entity = entities.ExampleBlockEntity
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                }
                rows_field = entities.ExampleBlockEntity.rows

    def test_missing_rows_field(self):
        with pytest.raises(TypeError):
            class IncompleteDataBlock(BaseDataBlock):
                clock_downscale_method = clock_sync_methods.DownscaleMethods.group_by_calendar_period
                clock_spec_field = entities.ExampleBlockEntity.clock_spec
                clock_sync_field = entities.ExampleSubentity2.id
                clock_upscale_method = clock_sync_methods.UpscaleMethods.infill_from_timestamp
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                }
                grouping_identifier_field = None
                main_entity = entities.ExampleBlockEntity
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                }

    def test_noncallable_clock_downscale_method(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                clock_downscale_method = 'group_by_calendar_period'

    def test_noncallable_clock_upscale_method(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                clock_upscale_method = 'infill_from_timestamp'

    def test_downscale_consolidation_methods_not_dict(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                downscale_consolidation_methods = (
                    (
                        entities.ExampleSubentity2,
                        clock_sync_methods.ConsolidationMethods.last,
                    ),
                    (
                        entities.ExampleSubentity3,
                        clock_sync_methods.ConsolidationMethods.last,
                    ),
                )

    def test_noncallable_consolidation_method(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity2: 'last',
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                }

    def test_entity_key_outside_prefix_entities(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity1: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                }

    def test_field_key_outside_prefix_entities(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity1.id: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                }

    def test_field_without_consolidation_method(self):
        with pytest.raises(TypeError):
            class IncompleteDataBlock(ExampleDataBlock):
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3.id: clock_sync_methods.ConsolidationMethods.last,
                }

    def test_base_data_entity_key(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                downscale_consolidation_methods: typing.ClassVar = {
                    BaseDataEntity: clock_sync_methods.ConsolidationMethods.last,
                }

    def test_prefix_entity_replacing_parent_entity(self):
        # The parent data block's consolidation methods and main entity also apply to the extension
        result = ExampleDatedExtensionDataBlock.get_entity_class_name_map()
        expected = {
            'ExampleExtendedDatedRow': entities.ExampleExtendedDatedRow,
        }

        assert result == expected

    def test_prefix_entities_replacing_same_parent_entity(self):
        with pytest.raises(TypeError):
            class AmbiguousDataBlock(ExampleDatedDataBlock):
                prefix_entity_map: typing.ClassVar = {
                    'dated': entities.ExampleExtendedDatedRow,
                    'datedother': entities.ExampleOtherExtendedDatedRow,
                }

    def test_prefix_entity_unreachable_from_main_entity(self):
        # No field of the main entity's hierarchy refers to the second prefix's entity
        with pytest.raises(TypeError):
            class UnreachableDataBlock(ExampleDatedDataBlock):
                downscale_consolidation_methods: typing.ClassVar = {
                    entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                }
                prefix_entity_map: typing.ClassVar = {
                    'dated': entities.ExampleDatedRow,
                    'datedsub': entities.ExampleSubentity3,
                }

    def test_prefix_entity_map_not_dict(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                prefix_entity_map = (
                    (
                        'example',
                        entities.ExampleSubentity2,
                    ),
                    (
                        'examplesub',
                        entities.ExampleSubentity3,
                    ),
                )

    def test_empty_prefix_entity_map(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                prefix_entity_map: typing.ClassVar = {}

    def test_prefix_with_underscore(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                    'example_sub': entities.ExampleSubentity3,
                }

    def test_prefix_mapped_to_non_entity(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                    'examplesub': str,
                }

    def test_prefixes_not_starting_with_shortest_prefix(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                prefix_entity_map: typing.ClassVar = {
                    'example': entities.ExampleSubentity2,
                    'other': entities.ExampleSubentity3,
                }

    def test_clock_spec_field_of_another_entity(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                clock_spec_field = entities.ExampleDatedRows.clock_spec

    def test_clock_spec_field_not_typed_as_clock_spec(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                clock_spec_field = entities.ExampleBlockEntity.subentity

    def test_nullable_clock_spec_field(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDatedDataBlock):
                clock_spec_field = entities.ExampleNullableClockSpecDatedRows.clock_spec
                main_entity = entities.ExampleNullableClockSpecDatedRows
                rows_field = entities.ExampleNullableClockSpecDatedRows.rows

    def test_rows_field_of_another_entity(self):
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                rows_field = entities.ExampleDatedRows.rows

    def test_rows_field_not_holding_clock_sync_entity_rows(self):
        # The main entity's subentity field holds the second prefix's entity, not the clock sync field's one
        with pytest.raises(TypeError):
            class InvalidDataBlock(ExampleDataBlock):
                rows_field = entities.ExampleBlockEntity.subentity


class TestConvertValueToType:
    def test_datetime_from_iso_string(self):
        result = BaseDataBlock.convert_value_to_type(
            '2026-11-20 15:30:00',
            datetime.datetime,
        )

        assert result == datetime.datetime(2026, 11, 20, 15, 30)

    def test_local_datetime(self):
        result = BaseDataBlock.convert_value_to_type(
            datetime.datetime(2026, 11, 20, 15, 30),
            datetime.datetime | None,
        )

        assert result == datetime.datetime(2026, 11, 20, 15, 30)

    def test_empty_nullable_datetime(self):
        result = BaseDataBlock.convert_value_to_type(
            '',
            datetime.datetime | None,
        )

        assert result is None

    def test_iso_string_with_utc_offset(self):
        with pytest.raises(DataBlockTypeConversionRuntimeError):
            BaseDataBlock.convert_value_to_type(
                '2026-11-20T15:30:00-05:00',
                datetime.datetime,
            )

    def test_datetime_with_time_zone(self):
        with pytest.raises(DataBlockTypeConversionRuntimeError):
            BaseDataBlock.convert_value_to_type(
                datetime.datetime(2026, 11, 20, 20, 30, tzinfo=datetime.UTC),
                datetime.datetime,
            )

    def test_invalid_datetime_string(self):
        with pytest.raises(DataBlockTypeConversionRuntimeError):
            BaseDataBlock.convert_value_to_type(
                '2026-11-20 25:30:00',
                datetime.datetime,
            )

    def test_datetime_to_date(self):
        result = BaseDataBlock.convert_value_to_type(
            datetime.datetime(2026, 11, 20, 15, 30),
            datetime.date,
        )

        assert result == datetime.date(2026, 11, 20)


class TestGetDownscaleConsolidationMethod:
    def test_field_key(self):
        result = ExampleDataBlock.get_downscale_consolidation_method(
            entities.ExampleSubentity3.name
        )

        assert result is clock_sync_methods.ConsolidationMethods.first

    def test_entity_key(self):
        result = ExampleDataBlock.get_downscale_consolidation_method(
            entities.ExampleSubentity2.name
        )

        assert result is clock_sync_methods.ConsolidationMethods.last

    def test_field_key_overriding_entity_key(self):
        class OverridingDataBlock(ExampleDataBlock):
            downscale_consolidation_methods: typing.ClassVar = {
                entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleSubentity2.name: clock_sync_methods.ConsolidationMethods.first,
                entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
            }

        result = OverridingDataBlock.get_downscale_consolidation_method(
            entities.ExampleSubentity2.name
        )

        assert result is clock_sync_methods.ConsolidationMethods.first

    def test_new_field_of_extension_with_parent_entity_key(self):
        result = ExampleDatedExtensionDataBlock.get_downscale_consolidation_method(
            entities.ExampleExtendedDatedRow.extra
        )

        assert result is clock_sync_methods.ConsolidationMethods.last

    def test_field_outside_prefix_entities(self):
        with pytest.raises(DataBlockError):
            ExampleDataBlock.get_downscale_consolidation_method(
                entities.ExampleSubentity1.id
            )


class TestGetFieldEntity:
    def test_prefix_entity_field(self):
        result = ExampleDataBlock.get_field_entity(
            entities.ExampleSubentity2.id
        )

        assert result is entities.ExampleSubentity2

    def test_inherited_field_of_prefix_entity(self):
        result = ExampleDatedExtensionDataBlock.get_field_entity(
            entities.ExampleDatedRow.date
        )

        assert result is entities.ExampleExtendedDatedRow

    def test_field_outside_prefix_entities(self):
        result = ExampleDatedExtensionDataBlock.get_field_entity(
            entities.ExampleDatedRows.rows
        )

        assert result is entities.ExampleDatedRows

    def test_non_field(self):
        with pytest.raises(DataBlockError):
            ExampleDataBlock.get_field_entity('id')


class TestGetFieldQualifiedName:
    def test_prefix_entity_field(self):
        result = ExampleDataBlock.get_field_qualified_name(
            entities.ExampleSubentity2.id
        )

        assert result == 'ExampleSubentity2.id'

    def test_inherited_field_of_prefix_entity(self):
        result = ExampleDatedExtensionDataBlock.get_field_qualified_name(
            entities.ExampleDatedRow.date
        )

        assert result == 'ExampleExtendedDatedRow.date'

    def test_non_field(self):
        with pytest.raises(DataBlockError):
            ExampleDataBlock.get_field_qualified_name('id')


class TestPackRowsEntitiesFromConsolidatedTable:
    def test_inherited_fields_of_subclassed_entity(
        self,
        extended_dated_table,
    ):
        result = ExampleExtendedDatedDataBlock.pack_rows_entities_from_consolidated_table(
            extended_dated_table
        )
        expected = {
            '2024-01-30': entities.ExampleExtendedDatedRow(
                date=datetime.date(2024, 1, 30),
                value=decimal.Decimal('1.5'),
                extra=3,
            ),
        }

        assert result == expected

    def test_subclass_block_after_parent_block_packed(
        self,
        extended_dated_table,
    ):
        # The parent data block packs first, like when a provider reuses the parent data block's assembly
        ExampleDatedDataBlock.pack_rows_entities_from_consolidated_table(
            pyarrow.table({
                'ExampleDatedRow.date': [
                    datetime.date(2024, 1, 30),
                ],
                'ExampleDatedRow.value': [
                    1.5,
                ],
            })
        )

        # Declared inside the test, so it can't have packed any rows before its parent data block
        class ExtendedDatedDataBlock(ExampleDatedDataBlock):
            clock_spec_field = entities.ExampleExtendedDatedRows.clock_spec
            downscale_consolidation_methods: typing.ClassVar = {
                entities.ExampleExtendedDatedRow: clock_sync_methods.ConsolidationMethods.last,
            }
            main_entity = entities.ExampleExtendedDatedRows
            prefix_entity_map: typing.ClassVar = {
                'dated': entities.ExampleExtendedDatedRow,
            }
            rows_field = entities.ExampleExtendedDatedRows.rows

        result = ExtendedDatedDataBlock.pack_rows_entities_from_consolidated_table(
            extended_dated_table
        )
        expected = {
            '2024-01-30': entities.ExampleExtendedDatedRow(
                date=datetime.date(2024, 1, 30),
                value=decimal.Decimal('1.5'),
                extra=3,
            ),
        }

        assert result == expected

    def test_parent_entity_replaced_by_prefix_entity(
        self,
        extended_dated_table,
    ):
        # The main entity's rows are typed with the parent entity, but get packed as the prefix entity extending it
        result = ExampleDatedExtensionDataBlock.pack_rows_entities_from_consolidated_table(
            extended_dated_table
        )
        expected = {
            '2024-01-30': entities.ExampleExtendedDatedRow(
                date=datetime.date(2024, 1, 30),
                value=decimal.Decimal('1.5'),
                extra=3,
            ),
        }

        assert result == expected

    def test_empty_table(self):
        result = ExampleDatedDataBlock.pack_rows_entities_from_consolidated_table(
            pyarrow.table({
                'ExampleDatedRow.date': pyarrow.array(
                    [],
                    type=pyarrow.date32(),
                ),
                'ExampleDatedRow.value': pyarrow.array(
                    [],
                    type=pyarrow.float64(),
                ),
            })
        )

        assert result == {}


class TestPrivateCalculateOrderedEntityRelationMap:
    def test_example_entity(self):
        result = BaseDataBlock._calculate_ordered_entity_relation_map(
            entities.ExampleEntity,
            {},
        )

        assert result == entities.EXAMPLE_ENTITY_RELATIONS_LINEAR_SORT

    def test_example_nested_entity(self):
        result = BaseDataBlock._calculate_ordered_entity_relation_map(
            entities.ExampleNestedEntity,
            {},
        )

        assert result == entities.EXAMPLE_NESTED_ENTITY_RELATIONS_LINEAR_SORT

    def test_prefix_entity_replacing_parent_entity(self):
        result = BaseDataBlock._calculate_ordered_entity_relation_map(
            entities.ExampleDatedRows,
            {
                'dated': entities.ExampleExtendedDatedRow,
            },
        )
        expected = {
            entities.ExampleExtendedDatedRow: {},
            entities.ExampleDatedRows: {
                entities.ExampleDatedRows.rows: entities.ExampleExtendedDatedRow,
            },
        }

        assert result == expected

    def test_prefix_entities_replacing_same_parent_entity(self):
        with pytest.raises(TypeError):
            BaseDataBlock._calculate_ordered_entity_relation_map(
                entities.ExampleDatedRows,
                {
                    'dated': entities.ExampleExtendedDatedRow,
                    'datedother': entities.ExampleOtherExtendedDatedRow,
                },
            )


class TestPrivateFindDownscaleConsolidationMethod:
    def test_field_key(self):
        result = BaseDataBlock._find_downscale_consolidation_method(
            downscale_consolidation_methods={
                entities.ExampleDatedRow.value: clock_sync_methods.ConsolidationMethods.sum,
            },
            entity=entities.ExampleDatedRow,
            field=entities.ExampleDatedRow.value,
        )

        assert result is clock_sync_methods.ConsolidationMethods.sum

    def test_entity_key(self):
        result = BaseDataBlock._find_downscale_consolidation_method(
            downscale_consolidation_methods={
                entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
            },
            entity=entities.ExampleDatedRow,
            field=entities.ExampleDatedRow.value,
        )

        assert result is clock_sync_methods.ConsolidationMethods.last

    def test_field_key_overriding_entity_key(self):
        result = BaseDataBlock._find_downscale_consolidation_method(
            downscale_consolidation_methods={
                entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleDatedRow.value: clock_sync_methods.ConsolidationMethods.sum,
            },
            entity=entities.ExampleDatedRow,
            field=entities.ExampleDatedRow.value,
        )

        assert result is clock_sync_methods.ConsolidationMethods.sum

    def test_parent_entity_key(self):
        result = BaseDataBlock._find_downscale_consolidation_method(
            downscale_consolidation_methods={
                entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
            },
            entity=entities.ExampleExtendedDatedRow,
            field=entities.ExampleExtendedDatedRow.extra,
        )

        assert result is clock_sync_methods.ConsolidationMethods.last

    def test_closest_entity_key(self):
        result = BaseDataBlock._find_downscale_consolidation_method(
            downscale_consolidation_methods={
                entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleExtendedDatedRow: clock_sync_methods.ConsolidationMethods.sum,
            },
            entity=entities.ExampleExtendedDatedRow,
            field=entities.ExampleExtendedDatedRow.value,
        )

        assert result is clock_sync_methods.ConsolidationMethods.sum

    def test_without_key(self):
        result = BaseDataBlock._find_downscale_consolidation_method(
            downscale_consolidation_methods={
                entities.ExampleDatedRow.date: clock_sync_methods.ConsolidationMethods.last,
            },
            entity=entities.ExampleDatedRow,
            field=entities.ExampleDatedRow.value,
        )

        assert result is None


class TestPrivateIsNullableType:
    def test_type_or_none(self):
        result = BaseDataBlock._is_nullable_type(decimal.Decimal | None)

        assert result is True

    def test_several_types_or_none(self):
        result = BaseDataBlock._is_nullable_type(int | str | None)

        assert result is True

    def test_several_types(self):
        result = BaseDataBlock._is_nullable_type(int | str)

        assert result is False

    def test_single_type(self):
        result = BaseDataBlock._is_nullable_type(str)

        assert result is False


class TestPrivatePackEntityHierarchyRows:
    def test_rows_keyed_by_clock_date_with_their_subentities(
        self,
        mini_fundamental_entity_relations,
        mini_fundamental_entity_tables,
    ):
        # The second date's balance sheet has no data, so it gets packed as None
        result = ExampleMiniFundamentalsDataBlock._pack_entity_hierarchy_rows(
            clock_sync_field=entity_tables.MiniFundamentalRow.filing_date,
            ordered_entities=mini_fundamental_entity_relations,
            entity_table_map=mini_fundamental_entity_tables,
        )
        expected = {
            '2024-01-30': entity_tables.MiniFundamentalRow(
                accepted_date=datetime.datetime(2024, 1, 30, 16),
                balance_sheet=entity_tables.MiniFundamentalRowBalanceSheet(
                    assets=decimal.Decimal('1.5'),
                    cash_and_cash_equivalents=decimal.Decimal('2.5'),
                ),
                cash_flow=entity_tables.MiniFundamentalRowCashFlow(
                    net_income=decimal.Decimal('3.5'),
                    net_income_tax_payments=None,
                ),
                income_statement=entity_tables.MiniFundamentalRowIncomeStatement(
                    cost_of_revenue=decimal.Decimal('5.5'),
                    net_income=decimal.Decimal('3.5'),
                ),
                filing_date=datetime.date(2024, 1, 30),
            ),
            '2024-01-31': entity_tables.MiniFundamentalRow(
                accepted_date=datetime.datetime(2024, 1, 31, 16),
                balance_sheet=None,
                cash_flow=entity_tables.MiniFundamentalRowCashFlow(
                    net_income=decimal.Decimal('4.5'),
                    net_income_tax_payments=decimal.Decimal('0.5'),
                ),
                income_statement=entity_tables.MiniFundamentalRowIncomeStatement(
                    cost_of_revenue=decimal.Decimal('6.5'),
                    net_income=decimal.Decimal('4.5'),
                ),
                filing_date=datetime.date(2024, 1, 31),
            ),
        }

        assert result == expected

    def test_clock_entity_row_without_data(self):
        result = MarketDailyDataBlock._pack_entity_hierarchy_rows(
            clock_sync_field=MarketDataDailyRow.date,
            ordered_entities={
                MarketDataDailyRow: {},
            },
            entity_table_map={
                MarketDataDailyRow: pyarrow.table({
                    'date': [
                        datetime.date(2024, 1, 30),
                        datetime.date(2024, 1, 31),
                    ],
                    'close': pyarrow.array(
                        [1.5, None],
                        type=pyarrow.float64(),
                    ),
                }),
            },
        )

        assert result['2024-01-31'] is None

    def test_entity_without_columns(
        self,
        mini_fundamental_entity_relations,
        mini_fundamental_entity_tables,
    ):
        # As when an endpoint returns no data for the entity, it gets packed as None in every row
        result = ExampleMiniFundamentalsDataBlock._pack_entity_hierarchy_rows(
            clock_sync_field=entity_tables.MiniFundamentalRow.filing_date,
            ordered_entities=mini_fundamental_entity_relations,
            entity_table_map={
                entity: table
                for (entity, table) in mini_fundamental_entity_tables.items()
                if entity is not entity_tables.MiniFundamentalRowCashFlow
            },
        )
        expected = [
            None,
            None,
        ]

        assert [row.cash_flow for row in result.values()] == expected

    def test_empty_tables(
        self,
        mini_fundamental_entity_relations,
    ):
        result = ExampleMiniFundamentalsDataBlock._pack_entity_hierarchy_rows(
            clock_sync_field=entity_tables.MiniFundamentalRow.filing_date,
            ordered_entities=mini_fundamental_entity_relations,
            entity_table_map={
                entity_tables.MiniFundamentalRow: pyarrow.table({
                    'filing_date': pyarrow.array(
                        [],
                        type=pyarrow.date32(),
                    ),
                    'accepted_date': pyarrow.array(
                        [],
                        type=pyarrow.timestamp('us'),
                    ),
                }),
            },
        )

        assert result == {}

    def test_clock_entity_without_columns(
        self,
        mini_fundamental_entity_relations,
        mini_fundamental_entity_tables,
    ):
        with pytest.raises(DataBlockIncorrectPackingStructureError):
            ExampleMiniFundamentalsDataBlock._pack_entity_hierarchy_rows(
                clock_sync_field=entity_tables.MiniFundamentalRow.filing_date,
                ordered_entities=mini_fundamental_entity_relations,
                entity_table_map={
                    entity: table
                    for (entity, table) in mini_fundamental_entity_tables.items()
                    if entity is not entity_tables.MiniFundamentalRow
                },
            )

    def test_ordered_entities_without_clock_entity(
        self,
        mini_fundamental_entity_tables,
    ):
        with pytest.raises(DataBlockIncorrectPackingStructureError):
            ExampleMiniFundamentalsDataBlock._pack_entity_hierarchy_rows(
                clock_sync_field=entity_tables.MiniFundamentalRow.filing_date,
                ordered_entities={
                    entity_tables.MiniFundamentalRowBalanceSheet: {},
                },
                entity_table_map=mini_fundamental_entity_tables,
            )

    def test_value_failing_type_conversion(
        self,
        mini_fundamental_entity_relations,
        mini_fundamental_entity_tables,
    ):
        with pytest.raises(DataBlockEntityPackingError):
            ExampleMiniFundamentalsDataBlock._pack_entity_hierarchy_rows(
                clock_sync_field=entity_tables.MiniFundamentalRow.filing_date,
                ordered_entities=mini_fundamental_entity_relations,
                entity_table_map={
                    **mini_fundamental_entity_tables,
                    entity_tables.MiniFundamentalRowBalanceSheet: pyarrow.table({
                        'assets': [
                            'not a number',
                            'not a number',
                        ],
                        'cash_and_cash_equivalents': pyarrow.array(
                            [None, None],
                            type=pyarrow.float64(),
                        ),
                    }),
                },
            )

    def test_missing_value_of_non_nullable_field(self):
        # The row has a value, so it doesn't get packed as None, while missing its non-nullable label
        with pytest.raises(DataBlockEntityMissingValueError):
            ExampleLabeledDataBlock._pack_entity_hierarchy_rows(
                clock_sync_field=entities.ExampleLabeledRow.date,
                ordered_entities={
                    entities.ExampleLabeledRow: {},
                },
                entity_table_map={
                    entities.ExampleLabeledRow: pyarrow.table({
                        'date': [
                            datetime.date(2024, 1, 30),
                        ],
                        'label': pyarrow.array(
                            [None],
                            type=pyarrow.string(),
                        ),
                        'value': [
                            1.5,
                        ],
                    }),
                },
            )

    def test_rows_failing_entity_validation(self):
        # MarketDataDailyRow rejects negative prices
        with pytest.raises(DataBlockRowEntityErrorGroup):
            MarketDailyDataBlock._pack_entity_hierarchy_rows(
                clock_sync_field=MarketDataDailyRow.date,
                ordered_entities={
                    MarketDataDailyRow: {},
                },
                entity_table_map={
                    MarketDataDailyRow: pyarrow.table({
                        'date': [
                            datetime.date(2024, 1, 30),
                        ],
                        'close': [
                            -1.5,
                        ],
                    }),
                },
            )


class TestPrivateResolveBlockEntity:
    def test_prefix_entity(self):
        result = BaseDataBlock._resolve_block_entity(
            entities.ExampleDatedRow,
            {
                'dated': entities.ExampleDatedRow,
            },
        )

        assert result is entities.ExampleDatedRow

    def test_parent_of_prefix_entity(self):
        result = BaseDataBlock._resolve_block_entity(
            entities.ExampleDatedRow,
            {
                'dated': entities.ExampleExtendedDatedRow,
            },
        )

        assert result is entities.ExampleExtendedDatedRow

    def test_prefix_entity_with_prefix_entity_extending_it(self):
        result = BaseDataBlock._resolve_block_entity(
            entities.ExampleDatedRow,
            {
                'dated': entities.ExampleDatedRow,
                'datedextended': entities.ExampleExtendedDatedRow,
            },
        )

        assert result is entities.ExampleDatedRow

    def test_parent_of_entity_on_several_prefixes(self):
        result = BaseDataBlock._resolve_block_entity(
            entities.ExampleDatedRow,
            {
                'dated': entities.ExampleExtendedDatedRow,
                'datedextended': entities.ExampleExtendedDatedRow,
            },
        )

        assert result is entities.ExampleExtendedDatedRow

    def test_entity_outside_prefix_entities(self):
        result = BaseDataBlock._resolve_block_entity(
            entities.ExampleDatedRows,
            {
                'dated': entities.ExampleExtendedDatedRow,
            },
        )

        assert result is entities.ExampleDatedRows

    def test_parent_of_several_prefix_entities(self):
        with pytest.raises(TypeError):
            BaseDataBlock._resolve_block_entity(
                entities.ExampleDatedRow,
                {
                    'dated': entities.ExampleExtendedDatedRow,
                    'datedother': entities.ExampleOtherExtendedDatedRow,
                },
            )


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


class TestPrivateUnwrapDictValueType:
    def test_dict(self):
        result = BaseDataBlock._unwrap_dict_value_type(dict[str, entities.ExampleDatedRow])

        assert result is entities.ExampleDatedRow

    def test_dict_without_type_arguments(self):
        result = BaseDataBlock._unwrap_dict_value_type(dict)

        assert result is dict

    def test_non_dict(self):
        result = BaseDataBlock._unwrap_dict_value_type(entities.ExampleDatedRow)

        assert result is entities.ExampleDatedRow

    def test_dict_or_none(self):
        # Only a dict gets unwrapped, so its optionality needs to be unwrapped first
        type_hint = dict[str, entities.ExampleDatedRow] | None
        result = BaseDataBlock._unwrap_dict_value_type(type_hint)

        assert result is type_hint


class TestPrivateUnwrapOptionalType:
    def test_type_or_none(self):
        result = BaseDataBlock._unwrap_optional_type(entities.ExampleDatedRow | None)

        assert result is entities.ExampleDatedRow

    def test_several_types_or_none(self):
        result = BaseDataBlock._unwrap_optional_type(int | str | None)
        expected = (
            int,
            str,
        )

        assert result == expected

    def test_several_types(self):
        result = BaseDataBlock._unwrap_optional_type(int | str)
        expected = (
            int,
            str,
        )

        assert result == expected

    def test_single_type(self):
        result = BaseDataBlock._unwrap_optional_type(entities.ExampleDatedRow)

        assert result is entities.ExampleDatedRow


class TestPrivateValidateClockMethods:
    def test_callable_methods(self):
        result = BaseDataBlock._validate_clock_methods(
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_calendar_period,
            clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
            data_block_name='ExampleDataBlock',
        )

        assert result is None

    def test_noncallable_clock_downscale_method(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_clock_methods(
                clock_downscale_method='group_by_calendar_period',
                clock_upscale_method=clock_sync_methods.UpscaleMethods.infill_from_timestamp,
                data_block_name='ExampleDataBlock',
            )

    def test_noncallable_clock_upscale_method(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_clock_methods(
                clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_calendar_period,
                clock_upscale_method='infill_from_timestamp',
                data_block_name='ExampleDataBlock',
            )

    def test_no_clock_upscale_method_for_instant_rows(self):
        result = BaseDataBlock._validate_clock_methods(
            clock_downscale_method=clock_sync_methods.DownscaleMethods.group_by_next_period_end,
            clock_upscale_method=None,
            data_block_name='ExampleDataBlock',
        )

        assert result is None


class TestPrivateValidateClockSpecField:
    def test_main_entity_field_typed_as_clock_spec(self):
        result = BaseDataBlock._validate_clock_spec_field(
            clock_spec_field=entities.ExampleDatedRows.clock_spec,
            data_block_name='ExampleDatedDataBlock',
            main_entity=entities.ExampleDatedRows,
        )

        assert result is None

    def test_field_of_another_entity(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_clock_spec_field(
                clock_spec_field=entities.ExampleBlockEntity.clock_spec,
                data_block_name='ExampleDatedDataBlock',
                main_entity=entities.ExampleDatedRows,
            )

    def test_field_not_typed_as_clock_spec(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_clock_spec_field(
                clock_spec_field=entities.ExampleDatedRows.rows,
                data_block_name='ExampleDatedDataBlock',
                main_entity=entities.ExampleDatedRows,
            )

    def test_nullable_field(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_clock_spec_field(
                clock_spec_field=entities.ExampleNullableClockSpecDatedRows.clock_spec,
                data_block_name='ExampleDatedDataBlock',
                main_entity=entities.ExampleNullableClockSpecDatedRows,
            )

    def test_value_that_is_not_field(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_clock_spec_field(
                clock_spec_field='clock_spec',
                data_block_name='ExampleDatedDataBlock',
                main_entity=entities.ExampleDatedRows,
            )


class TestPrivateValidateDownscaleConsolidationMethods:
    def test_entity_keys(
        self,
        example_prefix_entity_map,
    ):
        result = BaseDataBlock._validate_downscale_consolidation_methods(
            downscale_consolidation_methods={
                entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.sum,
            },
            data_block_name='ExampleDataBlock',
            prefix_entity_map=example_prefix_entity_map,
        )

        assert result is None

    def test_field_keys_for_every_field(
        self,
        example_prefix_entity_map,
    ):
        result = BaseDataBlock._validate_downscale_consolidation_methods(
            downscale_consolidation_methods={
                entities.ExampleSubentity2.id: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleSubentity2.name: clock_sync_methods.ConsolidationMethods.first,
                entities.ExampleSubentity3.id: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleSubentity3.name: clock_sync_methods.ConsolidationMethods.first,
            },
            data_block_name='ExampleDataBlock',
            prefix_entity_map=example_prefix_entity_map,
        )

        assert result is None

    def test_entity_key_with_field_override(
        self,
        example_prefix_entity_map,
    ):
        result = BaseDataBlock._validate_downscale_consolidation_methods(
            downscale_consolidation_methods={
                entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleSubentity2.name: clock_sync_methods.ConsolidationMethods.first,
                entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.sum,
            },
            data_block_name='ExampleDataBlock',
            prefix_entity_map=example_prefix_entity_map,
        )

        assert result is None

    def test_inherited_field_keys(self):
        # The fields inherited by an entity keep their parent entity's descriptors
        result = BaseDataBlock._validate_downscale_consolidation_methods(
            downscale_consolidation_methods={
                entities.ExampleSubentity2.id: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleSubentity2.name: clock_sync_methods.ConsolidationMethods.last,
                entities.ExampleExtendedSubentity2.description: clock_sync_methods.ConsolidationMethods.last,
            },
            data_block_name='ExampleDataBlock',
            prefix_entity_map={
                'example': entities.ExampleExtendedSubentity2,
            },
        )

        assert result is None

    def test_parent_entity_key_of_prefix_entity(self):
        result = BaseDataBlock._validate_downscale_consolidation_methods(
            downscale_consolidation_methods={
                entities.ExampleDatedRow: clock_sync_methods.ConsolidationMethods.last,
            },
            data_block_name='ExampleDataBlock',
            prefix_entity_map={
                'dated': entities.ExampleExtendedDatedRow,
            },
        )

        assert result is None

    def test_entity_on_several_prefixes(self):
        result = BaseDataBlock._validate_downscale_consolidation_methods(
            downscale_consolidation_methods={
                entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
            },
            data_block_name='ExampleDataBlock',
            prefix_entity_map={
                'example': entities.ExampleSubentity2,
                'examplesub': entities.ExampleSubentity2,
            },
        )

        assert result is None

    def test_not_dict(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods=(
                    (
                        entities.ExampleSubentity2,
                        clock_sync_methods.ConsolidationMethods.last,
                    ),
                    (
                        entities.ExampleSubentity3,
                        clock_sync_methods.ConsolidationMethods.last,
                    ),
                ),
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )

    def test_noncallable_method(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods={
                    entities.ExampleSubentity2: 'last',
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                },
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )

    def test_entity_key_outside_prefix_entities(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods={
                    entities.ExampleSubentity1: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                },
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )

    def test_field_key_outside_prefix_entities(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods={
                    entities.ExampleSubentity1.id: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3: clock_sync_methods.ConsolidationMethods.last,
                },
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )

    def test_field_without_method(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods={
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                    entities.ExampleSubentity3.id: clock_sync_methods.ConsolidationMethods.last,
                },
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )

    def test_prefix_entity_without_any_key(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods={
                    entities.ExampleSubentity2: clock_sync_methods.ConsolidationMethods.last,
                },
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )

    def test_base_data_entity_key(
        self,
        example_prefix_entity_map,
    ):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_downscale_consolidation_methods(
                downscale_consolidation_methods={
                    BaseDataEntity: clock_sync_methods.ConsolidationMethods.last,
                },
                data_block_name='ExampleDataBlock',
                prefix_entity_map=example_prefix_entity_map,
            )


class TestPrivateValidatePrefixEntityMap:
    def test_prefixes_starting_with_shortest_prefix(
        self,
        example_prefix_entity_map,
    ):
        result = BaseDataBlock._validate_prefix_entity_map(
            data_block_name='ExampleDataBlock',
            prefix_entity_map=example_prefix_entity_map,
        )

        assert result is None

    def test_single_prefix(self):
        result = BaseDataBlock._validate_prefix_entity_map(
            data_block_name='ExampleDataBlock',
            prefix_entity_map={
                'example': entities.ExampleSubentity2,
            },
        )

        assert result is None

    def test_not_dict(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map=[
                    (
                        'example',
                        entities.ExampleSubentity2,
                    ),
                ],
            )

    def test_empty(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={},
            )

    def test_non_string_prefix(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={
                    1: entities.ExampleSubentity2,
                },
            )

    def test_empty_prefix(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={
                    '': entities.ExampleSubentity2,
                },
            )

    def test_prefix_with_underscore(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={
                    'example_sub': entities.ExampleSubentity2,
                },
            )

    def test_class_that_is_not_entity(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={
                    'example': str,
                },
            )

    def test_value_that_is_not_class(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={
                    'example': 'ExampleSubentity2',
                },
            )

    def test_prefixes_not_starting_with_shortest_prefix(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_map(
                data_block_name='ExampleDataBlock',
                prefix_entity_map={
                    'example': entities.ExampleSubentity2,
                    'other': entities.ExampleSubentity3,
                },
            )


class TestPrivateValidatePrefixEntityReachability:
    def test_reachable_prefix_entities(self):
        result = BaseDataBlock._validate_prefix_entity_reachability(
            data_block_name='ExampleDataBlock',
            ordered_entity_relations={
                entities.ExampleSubentity2: {},
                entities.ExampleSubentity3: {},
                entities.ExampleBlockEntity: {
                    entities.ExampleBlockEntity.rows: entities.ExampleSubentity2,
                    entities.ExampleBlockEntity.subentity: entities.ExampleSubentity3,
                },
            },
            prefix_entity_map={
                'example': entities.ExampleSubentity2,
                'examplesub': entities.ExampleSubentity3,
            },
        )

        assert result is None

    def test_unreachable_prefix_entity(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_prefix_entity_reachability(
                data_block_name='ExampleDataBlock',
                ordered_entity_relations={
                    entities.ExampleSubentity2: {},
                },
                prefix_entity_map={
                    'example': entities.ExampleSubentity2,
                    'examplesub': entities.ExampleSubentity3,
                },
            )


class TestPrivateValidateRowsField:
    def test_field_holding_clock_sync_entity_rows(self):
        result = BaseDataBlock._validate_rows_field(
            clock_sync_entity=entities.ExampleDatedRow,
            data_block_name='ExampleDatedDataBlock',
            main_entity=entities.ExampleDatedRows,
            ordered_entity_relations={
                entities.ExampleDatedRow: {},
                entities.ExampleDatedRows: {
                    entities.ExampleDatedRows.rows: entities.ExampleDatedRow,
                },
            },
            rows_field=entities.ExampleDatedRows.rows,
        )

        assert result is None

    def test_field_holding_another_entity(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_rows_field(
                clock_sync_entity=entities.ExampleSubentity2,
                data_block_name='ExampleDataBlock',
                main_entity=entities.ExampleBlockEntity,
                ordered_entity_relations={
                    entities.ExampleSubentity2: {},
                    entities.ExampleSubentity3: {},
                    entities.ExampleBlockEntity: {
                        entities.ExampleBlockEntity.rows: entities.ExampleSubentity2,
                        entities.ExampleBlockEntity.subentity: entities.ExampleSubentity3,
                    },
                },
                rows_field=entities.ExampleBlockEntity.subentity,
            )

    def test_field_of_another_entity(self):
        with pytest.raises(TypeError):
            BaseDataBlock._validate_rows_field(
                clock_sync_entity=entities.ExampleSubentity2,
                data_block_name='ExampleDataBlock',
                main_entity=entities.ExampleBlockEntity,
                ordered_entity_relations={
                    entities.ExampleSubentity2: {},
                    entities.ExampleSubentity3: {},
                    entities.ExampleBlockEntity: {
                        entities.ExampleBlockEntity.rows: entities.ExampleSubentity2,
                        entities.ExampleBlockEntity.subentity: entities.ExampleSubentity3,
                    },
                },
                rows_field=entities.ExampleDatedRows.rows,
            )
