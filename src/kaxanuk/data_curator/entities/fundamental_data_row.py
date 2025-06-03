import dataclasses
import datetime
import re

from kaxanuk.data_curator.entities.fundamental_data_row_balance_sheet import FundamentalDataRowBalanceSheet
from kaxanuk.data_curator.entities.fundamental_data_row_cash_flow import FundamentalDataRowCashFlow
from kaxanuk.data_curator.entities.fundamental_data_row_income_statement import FundamentalDataRowIncomeStatement
from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError,
)
from kaxanuk.data_curator.services import entity_helper

FUNDAMENTAL_DATA_ROW_PERIODS = [
    'FY',
    'Q1',
    'Q2',
    'Q3',
    'Q4',
]


@dataclasses.dataclass(frozen=True, slots=True)
class FundamentalDataRow:
    accepted_date: datetime.datetime | None
    balance_sheet: FundamentalDataRowBalanceSheet | None
    cash_flow: FundamentalDataRowCashFlow | None
    income_statement: FundamentalDataRowIncomeStatement
    filing_date: datetime.date
    fiscal_period: str
    fiscal_year: int
    period_end_date: datetime.date
    reported_currency: str

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__} for filing_date {self.filing_date!s}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)

        currency_pattern = re.compile(r"^[A-Z]{3}$")
        if not currency_pattern.fullmatch(self.reported_currency):
            raise EntityValueError("Incorrect data in FundamentalDataRow.currency")

        fiscal_year_pattern = re.compile(r"^[0-9]{4}$")
        if not fiscal_year_pattern.fullmatch(str(self.fiscal_year)):
            raise EntityValueError("Incorrect data in FundamentalDataRow.fiscal_year")

        if self.fiscal_period not in FUNDAMENTAL_DATA_ROW_PERIODS:
            possible_periods = ', '.join(FUNDAMENTAL_DATA_ROW_PERIODS)
            raise EntityValueError(
                f"Incorrect FundamentalDataRow.fiscal_period, expecting one of: {possible_periods}"
            )

        if (
            self.balance_sheet is not None
            and not isinstance(self.balance_sheet, FundamentalDataRowBalanceSheet)
        ):
            raise EntityValueError("Incorrect FundamentalDataRow.balance_sheet format")

        if (
            self.cash_flow is not None
            and not isinstance(self.cash_flow, FundamentalDataRowCashFlow)
        ):
            raise EntityValueError("Incorrect FundamentalDataRow.cash_flow format")

        if not isinstance(self.income_statement, FundamentalDataRowIncomeStatement):
            raise EntityValueError("Incorrect FundamentalDataRow.income_statement format")
