"""
Dataclass encapsulating the user's selected configuration.
"""

import datetime
import dataclasses
import re

from kaxanuk.data_curator.exceptions import ConfigurationError


CONFIGURATION_COLUMN_PREFIXES = {
    'c': 'calculations',
    'd': 'dividends',
    'f': 'fundamental_data',
    'fbs': 'fundamental_data_balance_sheet',
    'fcf': 'fundamental_data_cash_flow',
    'fis': 'fundamental_data_income',
    'm': 'market_data',
    's': 'split_data',
}
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
            raise ConfigurationError("Incorrect Configuration.start_date type, expecting datetime.date")

        if (
            not isinstance(self.end_date, datetime.date)
            or isinstance(self.end_date, datetime.datetime)
        ):
            raise ConfigurationError("Incorrect Configuration.end_date type, expecting datetime.date")

        if self.period not in CONFIGURATION_PERIODS:
            possible_periods = ', '.join(CONFIGURATION_PERIODS)
            msg = f"Incorrect Configuration.interval, expecting one of: {possible_periods}"
            raise ConfigurationError(msg)

        valid_prefixes = "|".join(CONFIGURATION_COLUMN_PREFIXES.keys())
        column_pattern = re.compile(
            r"^(" + valid_prefixes + r")_[A-Za-z0-9_]+$"
        )
        incorrect_columns = [
            column for column in self.columns
                if not column_pattern.match(column)
        ]
        if len(incorrect_columns) > 0:
            msg = " ".join([
                "Incorrect column name in Configuration.columns:",
                ", ".join(incorrect_columns)
            ])
            raise ConfigurationError(msg)
