"""
Functions for validating values that don't make sense as Value Objects.
"""

import re
import typing


_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def is_date_pattern(value: typing.Any) -> bool:
    return (
        isinstance(value, str)
        and _DATE_PATTERN.fullmatch(value) is not None
    )
