import dataclasses
import datetime
import decimal

from kaxanuk.data_curator.entities import BaseDataEntity
from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
)
from kaxanuk.data_curator.services import entity_helper


DIVIDEND_DATE_FIELDS = (
    'declaration_date',
    'ex_dividend_date',
    'record_date',
    'payment_date',
)
DIVIDEND_FACTOR_FIELDS = (
    'dividend',
    'dividend_split_adjusted',
)


@dataclasses.dataclass(frozen=True, slots=True)
class DividendDataRow(BaseDataEntity):
    declaration_date: datetime.date | None
    ex_dividend_date: datetime.date
    record_date: datetime.date | None
    payment_date: datetime.date | None
    dividend: decimal.Decimal
    dividend_split_adjusted: decimal.Decimal | None

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for ex_dividend_date {self.ex_dividend_date!s}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)
