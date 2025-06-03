"""
These fixtures are for tests in this directory
"""


import dataclasses
import datetime
import decimal


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleEntity:
    field_int: int
    field_str: str
    field_date: datetime.date
    field_decimal: decimal.Decimal
    field_subentity: 'ExampleSubEntity'


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubEntity:
    subfield_int: int
    subfield_str: str
    subfield_date: datetime.date
    subfield_decimal: decimal.Decimal


def example_sum_function_1(a:float, b:float) -> float:
    return a + b
