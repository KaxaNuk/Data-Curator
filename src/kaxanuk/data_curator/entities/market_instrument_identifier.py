"""
Main identifier value object for use in all entities and aggregates.
"""

import dataclasses

from kaxanuk.data_curator.entities.main_identifier import MainIdentifier


@dataclasses.dataclass(frozen=True, slots=True)
class MarketInstrumentIdentifier(MainIdentifier):
    identifier: str
