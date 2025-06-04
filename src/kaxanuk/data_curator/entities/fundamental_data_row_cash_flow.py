import dataclasses
import decimal

from kaxanuk.data_curator.exceptions import EntityTypeError
from kaxanuk.data_curator.services import entity_helper


@dataclasses.dataclass(frozen=True, slots=True)
class FundamentalDataRowCashFlow:
    accounts_payable_change: decimal.Decimal | None
    accounts_receivable_change: decimal.Decimal | None
    capital_expenditure: decimal.Decimal | None
    cash_and_cash_equivalents_change: decimal.Decimal | None
    cash_exchange_rate_effect: decimal.Decimal | None
    common_stock_dividend_payments: decimal.Decimal | None
    common_stock_issuance_proceeds: decimal.Decimal | None
    common_stock_repurchase: decimal.Decimal | None
    deferred_income_tax: decimal.Decimal | None
    depreciation_and_amortization: decimal.Decimal | None
    dividend_payments: decimal.Decimal | None
    free_cash_flow: decimal.Decimal | None
    interest_payments: decimal.Decimal | None
    inventory_change: decimal.Decimal | None
    investment_sales_maturities_and_collections_proceeds: decimal.Decimal | None
    investments_purchase: decimal.Decimal | None
    net_business_acquisition_payments: decimal.Decimal | None
    net_cash_from_operating_activities: decimal.Decimal | None
    net_cash_from_investing_activites: decimal.Decimal | None
    net_cash_from_financing_activities: decimal.Decimal | None
    net_common_stock_issuance_proceeds: decimal.Decimal | None
    net_debt_issuance_proceeds: decimal.Decimal | None
    net_income: decimal.Decimal | None
    net_income_tax_payments: decimal.Decimal | None
    net_longterm_debt_issuance_proceeds: decimal.Decimal | None
    net_shortterm_debt_issuance_proceeds: decimal.Decimal | None
    net_stock_issuance_proceeds: decimal.Decimal | None
    other_financing_activities: decimal.Decimal | None
    other_investing_activities: decimal.Decimal | None
    other_noncash_items: decimal.Decimal | None
    other_working_capital: decimal.Decimal | None
    period_end_cash: decimal.Decimal | None
    period_start_cash: decimal.Decimal | None
    preferred_stock_dividend_payments: decimal.Decimal | None
    preferred_stock_issuance_proceeds: decimal.Decimal | None
    property_plant_and_equipment_purchase: decimal.Decimal | None
    stock_based_compensation: decimal.Decimal | None
    working_capital_change: decimal.Decimal | None

    def __post_init__(self):
        field_type_errors = entity_helper.detect_field_type_errors(self)
        if len(field_type_errors):
            msg = " ".join([
                f"Field type errors found in {self.__class__.__name__}:",
                "\n\t".join(field_type_errors)
            ])

            raise EntityTypeError(msg)
