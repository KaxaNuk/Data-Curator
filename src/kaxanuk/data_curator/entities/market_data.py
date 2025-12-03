import dataclasses
import datetime

from kaxanuk.data_curator.entities import BaseDataEntity
from kaxanuk.data_curator.entities.market_data_daily_row import MarketDataDailyRow
from kaxanuk.data_curator.entities.main_identifier import MainIdentifier
from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError
)
from kaxanuk.data_curator.services import (
    entity_helper,
    validator
)


@dataclasses.dataclass(frozen=True, slots=True)
class MarketData(BaseDataEntity):
    start_date: datetime.date
    end_date: datetime.date
    main_identifier: MainIdentifier
    daily_rows: dict[str, MarketDataDailyRow]

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for symbol {self.main_identifier}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)

        if not all(
            isinstance(row, MarketDataDailyRow)
            for row in self.daily_rows.values()
        ):
            msg = "Incorrect data in MarketData.daily_rows"

            raise EntityValueError(msg)

        if any(
            not validator.is_date_pattern(key)
            for key in self.daily_rows
        ):
            msg = "MarketData.daily_rows keys need to be date strings in 'YYYY-MM-DD' format"

            raise EntityValueError(msg)

        if list(self.daily_rows.keys()) != sorted(self.daily_rows.keys()):
            msg = "MarketData.daily_rows are not correctly sorted by date"

            raise EntityValueError(msg)
