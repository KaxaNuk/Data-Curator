import dataclasses

from kaxanuk.data_curator.entities.fundamental_data_row import FundamentalDataRow
from kaxanuk.data_curator.entities.main_identifier import MainIdentifier
from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError,
    FundamentalDataUnsortedRowDatesError,
)
from kaxanuk.data_curator.services import (
    entity_helper,
    validator
)


@dataclasses.dataclass(frozen=True, slots=True)
class FundamentalData:
    main_identifier: MainIdentifier
    rows: dict[str, FundamentalDataRow | None]

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for symbol {self.main_identifier}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)

        if not all(
            (
                row is None
                or isinstance(row, FundamentalDataRow)
            )
            for row in self.rows.values()
        ):
            raise EntityValueError("Incorrect data in FundamentalData.rows")

        if any(
            not validator.is_date_pattern(key)
            for key in self.rows
        ):
            raise EntityValueError("FundamentalData.rows keys need to be date strings in 'YYYY-MM-DD' format")

        if not list(self.rows.keys()) == sorted(self.rows.keys()):
            raise FundamentalDataUnsortedRowDatesError
