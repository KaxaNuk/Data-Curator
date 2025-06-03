import dataclasses
import datetime

from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
)
from kaxanuk.data_curator.services import entity_helper


@dataclasses.dataclass(frozen=True, slots=True)
class SplitDataRow:
    split_date: datetime.date
    numerator: float
    denominator: float

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for split_date {self.split_date!s}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)
