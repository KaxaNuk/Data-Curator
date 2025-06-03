import dataclasses

from kaxanuk.data_curator.entities.dividend_data_row import DividendDataRow
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
class DividendData:
    main_identifier: MainIdentifier
    rows: dict[str, DividendDataRow]

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for symbol {self.main_identifier}",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)

        if any(
            not validator.is_date_pattern(key)
            for key in self.rows
        ):
            raise EntityValueError("DividendData.rows keys need to be date strings in 'YYYY-MM-DD' format")

        if not all(
            isinstance(row, DividendDataRow)
            for row in self.rows.values()
        ):
            raise EntityValueError("Incorrect data in DividendData.rows")

