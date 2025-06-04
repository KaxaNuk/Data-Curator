import dataclasses
import datetime
from decimal import Decimal

import pytest

from kaxanuk.data_curator.entities import MainIdentifier
from kaxanuk.data_curator.exceptions import EntityFieldTypeError
import kaxanuk.data_curator.services.entity_helper as module


@dataclasses.dataclass(frozen=True, slots=True)
class ComplexFields:
    nullable_str_field: str | None
    dict_field: dict[int: str]
    ticker_entity_field: MainIdentifier


@dataclasses.dataclass(frozen=True, slots=True)
class SimpleFields:
    int_field: int
    str_field: str
    float_field: float
    date_field: datetime.date
    decimal_field: Decimal


class TestDetectFieldTypeErrors:
    @pytest.mark.parametrize(
        'nullable_str',
        [
            'lulz',
            None
        ]
    )
    def test_detect_complex_types_correct(self, nullable_str):
        instance = ComplexFields(
            nullable_str_field=nullable_str,
            dict_field={12: 'ok'},
            ticker_entity_field=MainIdentifier('F'),
        )

        assert len(module.detect_field_type_errors(instance)) == 0

    # noinspection PyTypeChecker
    def test_detect_complex_types_incorrect(self):
        instance = ComplexFields(
            nullable_str_field=15,
            dict_field=None,    # currently doesn't check complex non-nullable types
            ticker_entity_field='F',
        )

        assert len(module.detect_field_type_errors(instance)) == 2

    def test_detect_simple_types_correct(self):
        instance = SimpleFields(
            int_field=12,
            str_field='some string',
            float_field=34.5,
            date_field=datetime.date.fromisoformat('2014-07-21'),
            decimal_field=Decimal('10.1'),
        )

        assert len(module.detect_field_type_errors(instance)) == 0

    # noinspection PyTypeChecker
    def test_detect_simple_types_incorrect(self):
        instance = SimpleFields(
            int_field="some string",
            str_field=34.5,
            float_field=datetime.date.fromisoformat('2014-07-21'),
            date_field=Decimal('10.1'),
            decimal_field=12,
        )

        assert len(module.detect_field_type_errors(instance)) == 5


class TestFillFields:
    def test_fill_simple_fields(self):
        field_correspondences = {
            'int_field': 'int_field',
            'str_field': 'str_field',
            'float_field': 'float_field',
            'date_field': 'date_field',
            'decimal_field': 'decimal_field',
        }

        data_row = {
            'int_field': "12",
            'str_field': "some string",
            'float_field': "34.5",
            'date_field': '2014-07-21',
            'decimal_field': '10.1',
        }

        filled_fields = module.convert_data_row_into_entity_fields(data_row, field_correspondences, SimpleFields)
        instance = SimpleFields(**filled_fields)

        assert len(module.detect_field_type_errors(instance)) == 0


class TestPrivateConvertToType:
    def test_convert_to_date(self):
        assert (
            module._convert_to_type(
                'test_field',
                "2014-01-04",
                datetime.date
            )
            == datetime.date(2014, 1, 4)
        )

    def test_convert_to_date_exception(self):
        with pytest.raises(EntityFieldTypeError):
            module._convert_to_type(
                'test_field',
                "121212",
                datetime.date
            )

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (0.3333333, Decimal('0.3333333')),
            ('15', Decimal('15')),
        ]
    )
    def test_convert_to_decimal(self, value, expected):
        assert (
            module._convert_to_type(
                'test_field',
                value,
                Decimal
            )
            == expected
        )

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (False, Decimal('0')),
            (True, Decimal('1')),
            (None, None),
        ]
    )
    def test_convert_to_decimal_exception(self, value, expected):
        with pytest.raises(EntityFieldTypeError):
            module._convert_to_type(
                'test_field',
                value,
                Decimal
            )

    def test_convert_to_int(self):
        assert (
            module._convert_to_type(
                'test_field',
                "1",
                int
            )
            == 1
        )

    def test_convert_to_int_exception(self):
        with pytest.raises(EntityFieldTypeError):
            module._convert_to_type(
                'test_field',
                "not an int",
                int
            )

    def test_convert_to_float(self):
        assert (
            module._convert_to_type(
                'test_field',
                "3.14",
                float
            )
            == 3.14
        )

    def test_convert_to_float_exception(self):
        with pytest.raises(EntityFieldTypeError):
            module._convert_to_type(
                'test_field',
                "not a float",
                float
            )

    def test_convert_to_str(self):
        assert (
            module._convert_to_type(
                'test_field',
                10,
                str
            )
            == "10"
        )

    def test_convert_to_unknown_type(self):
        with pytest.raises(NotImplementedError):
            module._convert_to_type(
                'test_field',
                10,
                MainIdentifier
            )

    def test_convert_to_union_type_non_null(self):
        assert (
            module._convert_to_type(
                'test_field',
                "some value",
                str | None
            )
            == "some value"
        )

    def test_convert_to_union_type_non_null_exception(self):
        with pytest.raises(EntityFieldTypeError):
            module._convert_to_type(
                'test_field',
                "not an int",
                int | None
            )

    def test_convert_to_union_type_null(self):
        assert (
            module._convert_to_type(
                'test_field',
                None,
                str | None
            )
            is None
        )

    def test_convert_date_to_itself(self):
        assert (
            module._convert_to_type(
                'test_field',
                datetime.date.fromisoformat("2014-01-04"),
                datetime.date
            )
            == datetime.date(2014, 1, 4)
        )
