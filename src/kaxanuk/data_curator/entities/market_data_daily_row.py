import dataclasses
import datetime
import decimal

from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError
)
from kaxanuk.data_curator.services import entity_helper


@dataclasses.dataclass(frozen=True, slots=True)
class MarketDataDailyRow:
    date: datetime.date
    open: decimal.Decimal | None
    high: decimal.Decimal | None
    low: decimal.Decimal | None
    close: decimal.Decimal | None
    volume: int | None
    vwap: decimal.Decimal | None
    open_split_adjusted: decimal.Decimal | None
    high_split_adjusted: decimal.Decimal | None
    low_split_adjusted: decimal.Decimal | None
    close_split_adjusted: decimal.Decimal | None
    volume_split_adjusted: int | None
    vwap_split_adjusted: decimal.Decimal | None
    open_dividend_and_split_adjusted: decimal.Decimal | None
    high_dividend_and_split_adjusted: decimal.Decimal | None
    low_dividend_and_split_adjusted: decimal.Decimal | None
    close_dividend_and_split_adjusted: decimal.Decimal | None
    volume_dividend_and_split_adjusted: int | None
    vwap_dividend_and_split_adjusted: decimal.Decimal | None

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for date {self.date!s}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)

        for field in dataclasses.fields(self):
            if field.name == 'date':
                continue

            # Check that no numeric fields are negative
            field_value = getattr(self, field.name)
            if (
                field_value is not None
                and field_value < type(field_value)(0)
            ):
                msg = f"Negative MarketDataDailyRow.{field.name} for date {self.date!s}"

                raise EntityValueError(msg)

        low_high_field_pairs = [
            ('low', 'high'),
            ('low_split_adjusted', 'high_split_adjusted'),
            ('low_dividend_and_split_adjusted', 'high_dividend_and_split_adjusted')
        ]
        for (low_field, high_field) in low_high_field_pairs:
            low_value = getattr(self, low_field)
            high_value = getattr(self, high_field)

            if (
                low_value is not None
                and high_value is not None
                and low_value > high_value
            ):
                msg = f"MarketDataDailyRow {low_field} > {high_field} for date {self.date!s}"

                raise EntityValueError(msg)
