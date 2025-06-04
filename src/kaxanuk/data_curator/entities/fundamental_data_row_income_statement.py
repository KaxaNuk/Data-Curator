import dataclasses
import decimal

from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError
)
from kaxanuk.data_curator.services import entity_helper


@dataclasses.dataclass(frozen=True, slots=True)
class FundamentalDataRowIncomeStatement:
    basic_earnings_per_share: float | None
    basic_net_income_available_to_common_stockholders: decimal.Decimal | None
    continuing_operations_income_after_tax: decimal.Decimal | None
    costs_and_expenses: decimal.Decimal | None
    cost_of_revenue: decimal.Decimal | None
    depreciation_and_amortization: decimal.Decimal | None
    diluted_earnings_per_share: float | None
    discontinued_operations_income_after_tax: decimal.Decimal | None
    earnings_before_interest_and_tax: decimal.Decimal | None
    earnings_before_interest_tax_depreciation_and_amortization: decimal.Decimal | None
    general_and_administrative_expense: decimal.Decimal | None
    gross_profit: decimal.Decimal | None
    income_before_tax: decimal.Decimal | None
    income_tax_expense: decimal.Decimal | None
    interest_expense: decimal.Decimal | None
    interest_income: decimal.Decimal | None
    net_income: decimal.Decimal | None
    net_income_deductions: decimal.Decimal | None
    net_interest_income: decimal.Decimal | None
    net_total_other_income: decimal.Decimal | None
    nonoperating_income_excluding_interest: decimal.Decimal | None
    operating_expenses: decimal.Decimal | None
    operating_income: decimal.Decimal | None
    other_expenses: decimal.Decimal | None
    other_net_income_adjustments: decimal.Decimal | None
    research_and_development_expense: decimal.Decimal | None
    revenues: decimal.Decimal | None
    selling_and_marketing_expense: decimal.Decimal | None
    selling_general_and_administrative_expense: decimal.Decimal | None
    weighted_average_basic_shares_outstanding: int | None
    weighted_average_diluted_shares_outstanding: int | None

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)

        for field_name in (
            'weighted_average_basic_shares_outstanding',
            'weighted_average_diluted_shares_outstanding',
        ):
            field_value = getattr(self, field_name)
            if (
                field_value is not None
                and field_value < decimal.Decimal(0)
            ):
                msg = f"Negative FundamentalDataRowIncome.{field_name} detected"

                raise EntityValueError(msg)
