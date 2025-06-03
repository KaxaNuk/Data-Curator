"""
Main identifier value object for use in all entities and aggregates.
"""

import dataclasses
import re

from kaxanuk.data_curator.exceptions import EntityValueError


@dataclasses.dataclass(frozen=True, slots=True)
class MainIdentifier:
    identifier: str

    def __post_init__(self):
        if not isinstance(self.identifier, str):
            raise EntityValueError("Main identifier must be a string")

        identifier_pattern = re.compile(r"^\S+$")
        if identifier_pattern.fullmatch(self.identifier) is None:
            msg = f"Main identifier should not contain whitespace: {self.identifier}"
            raise EntityValueError(msg)
