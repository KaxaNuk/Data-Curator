"""
Clock specification value object, describing the time scale of a data block's rows.
"""

import dataclasses

from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError,
)
from kaxanuk.data_curator.modules.data_column import ReferencePeriods


@dataclasses.dataclass(frozen=True, slots=True)
class ClockSpec:
    """
    The time scale of a data block's rows, which the data provider supplying them assigns.

    Its fields match the time scale parameters of the DataColumn clocks the rows get synced through.
    """

    # the base reference period of the rows:
    reference_period: ReferencePeriods
    # the number of reference periods spanned by each row:
    periods_per_row: int

    def __post_init__(self):
        if not isinstance(self.reference_period, ReferencePeriods):
            msg = f"{self.__class__.__name__}.reference_period must be a ReferencePeriods member"

            raise EntityTypeError(msg)

        # bool is an int subclass, but not a valid number of periods
        if (
            not isinstance(self.periods_per_row, int)
            or isinstance(self.periods_per_row, bool)
        ):
            msg = f"{self.__class__.__name__}.periods_per_row must be an int"

            raise EntityTypeError(msg)

        if self.periods_per_row < 1:
            msg = f"{self.__class__.__name__}.periods_per_row must be at least 1"

            raise EntityValueError(msg)
