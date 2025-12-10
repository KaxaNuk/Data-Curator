"""
Functions for handling entity fields in a quick and easy manner.
"""

import dataclasses
import datetime
import decimal
import types
import typing
import warnings

from kaxanuk.data_curator.exceptions import (
    EntityFieldTypeError
)


# Protocol for type-checking dataclasses
@typing.runtime_checkable
class DataclassProtocol(typing.Protocol):
    __dataclass_fields__: dict


def detect_field_type_errors(
    dataclass_entity: DataclassProtocol
) -> list[str]:
    """
    Return all the field type errors for the given dataclass entity, or an empty list if no errors found.

    Parameters
    ----------
    dataclass_entity : DataclassProtocol
        A dataclass entity whose field type annotations we'll use to convert the data to the desired types

    Returns
    -------
    list[str]
        List of field type errors found
    """
    errors = []
    for field in dataclasses.fields(dataclass_entity):
        field_name = field.name
        field_type = field.type
        is_nullable = (
            isinstance(field_type, types.UnionType)
            and len(field_type.__args__) == 2
            and field_type.__args__[1] == types.NoneType
        )
        if is_nullable:
            # for a nullable type, get the non-null type
            base_field_type = field_type.__args__[0]
        elif isinstance(field_type, types.GenericAlias):
            # skip check of non-simple types

            continue
        else:
            base_field_type = field_type

        field_value = getattr(dataclass_entity, field_name)

        if isinstance(field_value, base_field_type):
            continue

        if field_value is not None:
            errors.append(
                f"Incorrect {field_name} type, expecting {base_field_type}, got {type(field_value)}"
            )
        elif not is_nullable:
            errors.append(
                f"Empty value received for non-nullable {field_name}"
            )

    return errors


def convert_data_row_into_entity_fields(
    data_row: dict[str, typing.Any],
    field_correspondences: dict[str, str],
    entity: DataclassProtocol
) -> dict[str, typing.Any]:
    """
    Convert a data row into the fields of dataclass `entity`, applying the required conversions.

    Takes a data_row, renames its keys based on field_correspondences (removing the missing ones), and
    converts its value types according to the typehints of the `entity` fields with the same names.

    Just pass its result to the entity constructor in destructured form using the ** operator

    Parameters
    ----------
    data_row
        the row of data we'll extract and convert the fields from
    field_correspondences
        the correspondences between the names of the keys in data_row and the names we'll return
    entity
        A dataclass entity whose field type annotations we'll use to convert the data to the desired types

    Returns
    -------
    dict[str, typing.Any]
        a dictionary with the field_correspondences as keys, and the converted fields as values
    """
    warnings.warn(
        "entity_helper.convert_data_row_into_entity_fields is deprecated and will be removed in a future version",
        DeprecationWarning,
        stacklevel=2
    )
    return {
        item[0]: _convert_to_type(
            item[0],
            (
                data_row[field_correspondences[item[0]]] if field_correspondences[item[0]] is not None
                else None
            ),
            item[1]
        )
        for item in entity.__annotations__.items()
    }


def _convert_to_type(
    field_name: str,
    field_value: typing.Any,
    to_type: typing.Any
) -> typing.Any:
    """
    Convert the field to the required `to_type`.

    Parameters
    ----------
    field_name : str
        the name of the field
    field_value : typing.Any
        the field value to convert
    to_type: typing.Any
        the type of the field to convert to

    Returns
    -------
    typing.Any
        the field converted to to_type
    """
    try:
        # if type is nullable with empty value, return None
        if (
            isinstance(to_type, types.UnionType)
            and len(to_type.__args__) == 2
            and to_type.__args__[1] == types.NoneType
        ):
            if field_value in [None, '']:
                return None
            else:
                to_type = to_type.__args__[0]

        if isinstance(field_value, to_type):
            return field_value

        match to_type.__name__:
            case 'date':
                return datetime.date.fromisoformat(field_value)
            case 'Decimal':
                return decimal.Decimal(str(field_value))
            case 'int':
                return int(field_value)
            case 'float':
                return float(field_value)
            case 'str':
                return str(field_value)
            case default:
                msg = f"Entity_helper unknown type in _convert_to_type: {default}"

                raise NotImplementedError(msg)
    except (
        decimal.InvalidOperation,
        TypeError,
        ValueError
    ) as error:
        msg = f"Field {field_name} with value {field_value} cannot be converted to {to_type}"
        raise EntityFieldTypeError(msg) from error
