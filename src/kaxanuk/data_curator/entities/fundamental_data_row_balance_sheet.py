import dataclasses
import decimal

from kaxanuk.data_curator.exceptions import EntityTypeError
from kaxanuk.data_curator.services import entity_helper


@dataclasses.dataclass(frozen=True, slots=True)
class FundamentalDataRowBalanceSheet:
    accumulated_other_comprehensive_income_after_tax: decimal.Decimal | None
    additional_paid_in_capital: decimal.Decimal | None
    assets: decimal.Decimal | None
    capital_lease_obligations: decimal.Decimal | None
    cash_and_cash_equivalents: decimal.Decimal | None
    cash_and_shortterm_investments: decimal.Decimal | None
    common_stock_value: decimal.Decimal | None
    current_accounts_payable: decimal.Decimal | None
    current_accounts_receivable_after_doubtful_accounts: decimal.Decimal | None
    current_accrued_expenses: decimal.Decimal | None
    current_assets: decimal.Decimal | None
    current_capital_lease_obligations: decimal.Decimal | None
    current_liabilities: decimal.Decimal | None
    current_net_receivables: decimal.Decimal | None
    current_tax_payables: decimal.Decimal | None
    deferred_revenue: decimal.Decimal | None
    goodwill: decimal.Decimal | None
    investments: decimal.Decimal | None
    liabilities: decimal.Decimal | None
    longterm_debt: decimal.Decimal | None
    longterm_investments: decimal.Decimal | None
    net_debt: decimal.Decimal | None
    net_intangible_assets_excluding_goodwill: decimal.Decimal | None
    net_intangible_assets_including_goodwill: decimal.Decimal | None
    net_inventory: decimal.Decimal | None
    net_property_plant_and_equipment: decimal.Decimal | None
    noncontrolling_interest: decimal.Decimal | None
    noncurrent_assets: decimal.Decimal | None
    noncurrent_capital_lease_obligations: decimal.Decimal | None
    noncurrent_deferred_revenue: decimal.Decimal | None
    noncurrent_deferred_tax_assets: decimal.Decimal | None
    noncurrent_deferred_tax_liabilities: decimal.Decimal | None
    noncurrent_liabilities: decimal.Decimal | None
    other_assets: decimal.Decimal | None
    other_current_assets: decimal.Decimal | None
    other_current_liabilities: decimal.Decimal | None
    other_liabilities: decimal.Decimal | None
    other_noncurrent_assets: decimal.Decimal | None
    other_noncurrent_liabilities: decimal.Decimal | None
    other_payables: decimal.Decimal | None
    other_receivables: decimal.Decimal | None
    other_stockholder_equity: decimal.Decimal | None
    preferred_stock_value: decimal.Decimal | None
    prepaid_expenses: decimal.Decimal | None
    retained_earnings: decimal.Decimal | None
    shortterm_debt: decimal.Decimal | None
    shortterm_investments: decimal.Decimal | None
    stockholder_equity: decimal.Decimal | None
    total_debt_including_capital_lease_obligations: decimal.Decimal | None
    total_equity_including_noncontrolling_interest: decimal.Decimal | None
    total_liabilities_and_equity: decimal.Decimal | None
    total_payables_current_and_noncurrent: decimal.Decimal | None
    treasury_stock_value: decimal.Decimal | None

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)
