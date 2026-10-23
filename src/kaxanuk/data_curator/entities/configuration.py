"""
Dataclass encapsulating the user's selected configuration.
"""

import datetime
import dataclasses
import re

from kaxanuk.data_curator.exceptions import ConfigurationError


# @todo this goes into the fundamentals data block
CONFIGURATION_PERIODS = (
    'annual',
    'quarterly',
)


@dataclasses.dataclass(frozen=True, slots=True)
class Configuration:
    start_date: datetime.date
    end_date: datetime.date
    period: str
    identifiers: tuple[str, ...]
    columns: tuple[str, ...]

    def __post_init__(self):
        if (
            not isinstance(self.start_date, datetime.date)
            or isinstance(self.start_date, datetime.datetime)
        ):
            msg = f"Incorrect {self.__class__.__name__}.start_date type, expecting datetime.date"

            raise ConfigurationError(msg)

        if (
            not isinstance(self.end_date, datetime.date)
            or isinstance(self.end_date, datetime.datetime)
        ):
            msg = f"Incorrect {self.__class__.__name__}.end_date type, expecting datetime.date"

            raise ConfigurationError(msg)

        if self.period not in CONFIGURATION_PERIODS:
            possible_periods = ', '.join(CONFIGURATION_PERIODS)
            msg = f"Incorrect {self.__class__.__name__}.interval, expecting one of: {possible_periods}"

            raise ConfigurationError(msg)

        # only the prefixed format is checked here, as the valid prefixes depend on the data blocks of each run,
        # which ColumnBuilder validates the columns against
        column_pattern = re.compile(
            r"^[a-z][a-z0-9]*_[A-Za-z0-9_]+$"
        )
        incorrect_columns = [
            column for column in self.columns
                if not column_pattern.match(column)
        ]
        if len(incorrect_columns) > 0:
            msg = " ".join([
                f"Incorrect column name in {self.__class__.__name__}.columns:",
                ", ".join(incorrect_columns)
            ])

            raise ConfigurationError(msg)
