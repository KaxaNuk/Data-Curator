import copy
import dataclasses
import datetime
import decimal

import pytest

from kaxanuk.data_curator import DataColumn
from kaxanuk.data_curator.entities import (
    DividendDataRow,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    MarketDataDailyRow,
    #SplitDataRow
)
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderNoDatesToInfillError,
    ColumnBuilderUnavailableEntityFieldError,
)
from kaxanuk.data_curator.services.column_builder import (
    ColumnBuilder,
    ColumnIdentifier,
    CompletedColumns,
    DataRows,
    PostponedColumns
)
from . import fixture_entities      # @todo: do we really need them in a different file?
from .fixtures import calculations
# note: data_column_debugger is defined in tests/conftest.py


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleSubEntity1:
    subfield1: int
    subfield2: str


@dataclasses.dataclass(frozen=True, slots=True)
class ExampleEntity1:
    field1: int
    field2: str
    subfield_field1: ExampleSubEntity1


@pytest.fixture(scope="module")
def example_entity_rows_fundamental_data():
    row = FundamentalDataRow(
        accepted_date=None,
        filing_date=datetime.date(2020, 1, 3),
        period_end_date=datetime.date(2020, 1, 3),
        reported_currency='USD',
        fiscal_year=2019,
        fiscal_period='Q3',

        balance_sheet=FundamentalDataRowBalanceSheet(
            accumulated_other_comprehensive_income_after_tax=decimal.Decimal('800'),
            additional_paid_in_capital=decimal.Decimal('1000'),
            assets=decimal.Decimal('100000'),
            capital_lease_obligations=decimal.Decimal('90'),
            cash_and_cash_equivalents=decimal.Decimal('20000'),
            cash_and_shortterm_investments=decimal.Decimal('450'),
            common_stock_value=decimal.Decimal('150000'),
            current_accounts_payable=decimal.Decimal('250'),
            current_accounts_receivable_after_doubtful_accounts=decimal.Decimal('300000'),
            current_accrued_expenses=decimal.Decimal('500'),
            current_assets=decimal.Decimal('30'),
            current_capital_lease_obligations=decimal.Decimal('50'),
            current_liabilities=decimal.Decimal('50'),
            current_net_receivables=decimal.Decimal('300000'),
            current_tax_payables=decimal.Decimal('80'),
            deferred_revenue=decimal.Decimal('70'),
            goodwill=decimal.Decimal('50'),
            investments=decimal.Decimal('980'),
            liabilities=decimal.Decimal('40'),
            longterm_debt=decimal.Decimal('40'),
            longterm_investments=decimal.Decimal('8000'),
            net_debt=decimal.Decimal('68000'),
            net_intangible_assets_excluding_goodwill=decimal.Decimal('60'),
            net_intangible_assets_including_goodwill=decimal.Decimal('70'),
            net_inventory=decimal.Decimal('10'),
            net_property_plant_and_equipment=decimal.Decimal('40'),
            noncontrolling_interest=decimal.Decimal('220'),
            noncurrent_assets=decimal.Decimal('30'),
            noncurrent_capital_lease_obligations=decimal.Decimal('40'),
            noncurrent_deferred_revenue=decimal.Decimal('30'),
            noncurrent_deferred_tax_assets=decimal.Decimal('600'),
            noncurrent_deferred_tax_liabilities=decimal.Decimal('20'),
            noncurrent_liabilities=decimal.Decimal('90'),
            other_assets=decimal.Decimal('10'),
            other_current_assets=decimal.Decimal('20'),
            other_current_liabilities=decimal.Decimal('60'),
            other_liabilities=decimal.Decimal('80'),
            other_noncurrent_assets=decimal.Decimal('40'),
            other_noncurrent_liabilities=decimal.Decimal('10'),
            other_payables=decimal.Decimal('330'),
            other_receivables=decimal.Decimal('440'),
            other_stockholder_equity=decimal.Decimal('7080'),
            preferred_stock_value=decimal.Decimal('5000'),
            prepaid_expenses=decimal.Decimal('5'),
            retained_earnings=decimal.Decimal('8900'),
            shortterm_debt=decimal.Decimal('5600'),
            shortterm_investments=decimal.Decimal('12000'),
            stockholder_equity=decimal.Decimal('700'),
            total_debt_including_capital_lease_obligations=decimal.Decimal('66600'),
            total_equity_including_noncontrolling_interest=decimal.Decimal('450'),
            total_liabilities_and_equity=decimal.Decimal('650'),
            total_payables_current_and_noncurrent=decimal.Decimal('580'),
            treasury_stock_value=decimal.Decimal('100'),
        ),
        cash_flow=FundamentalDataRowCashFlow(
            accounts_payable_change=decimal.Decimal('5460'),
            accounts_receivable_change=decimal.Decimal('5680'),
            capital_expenditure=decimal.Decimal('780'),
            cash_and_cash_equivalents_change=decimal.Decimal('10'),
            cash_exchange_rate_effect=decimal.Decimal('550'),
            common_stock_dividend_payments=decimal.Decimal('70'),
            common_stock_issuance_proceeds=decimal.Decimal('60'),
            common_stock_repurchase=decimal.Decimal('50'),
            deferred_income_tax=decimal.Decimal('15600'),
            depreciation_and_amortization=decimal.Decimal('4500'),
            dividend_payments=decimal.Decimal('70'),
            free_cash_flow=decimal.Decimal('50'),
            interest_payments=decimal.Decimal('30'),
            inventory_change=decimal.Decimal('6780'),
            investment_sales_maturities_and_collections_proceeds=decimal.Decimal('40'),
            investments_purchase=decimal.Decimal('550'),
            net_business_acquisition_payments=decimal.Decimal('3400'),
            net_cash_from_operating_activities=decimal.Decimal('980'),
            net_cash_from_investing_activites=decimal.Decimal('20'),
            net_cash_from_financing_activities=decimal.Decimal('70'),
            net_common_stock_issuance_proceeds=decimal.Decimal('10'),
            net_debt_issuance_proceeds=decimal.Decimal('500'),
            net_income=decimal.Decimal('450000'),
            net_income_tax_payments=decimal.Decimal('1000'),
            net_longterm_debt_issuance_proceeds=decimal.Decimal('300'),
            net_shortterm_debt_issuance_proceeds=decimal.Decimal('200'),
            net_stock_issuance_proceeds=decimal.Decimal('10'),
            other_financing_activities=decimal.Decimal('80'),
            other_investing_activities=decimal.Decimal('20'),
            other_noncash_items=decimal.Decimal('6700'),
            other_working_capital=decimal.Decimal('550'),
            period_end_cash=decimal.Decimal('200'),
            period_start_cash=decimal.Decimal('3000'),
            preferred_stock_dividend_payments=decimal.Decimal('0'),
            preferred_stock_issuance_proceeds=decimal.Decimal('0'),
            property_plant_and_equipment_purchase=decimal.Decimal('930'),
            stock_based_compensation=decimal.Decimal('7760'),
            working_capital_change=decimal.Decimal('7860'),
        ),
        income_statement=FundamentalDataRowIncomeStatement(
            basic_earnings_per_share=10.0,
            basic_net_income_available_to_common_stockholders=decimal.Decimal('30000'),
            continuing_operations_income_after_tax=decimal.Decimal('29500'),
            costs_and_expenses=decimal.Decimal('890'),
            cost_of_revenue=decimal.Decimal('54000'),
            depreciation_and_amortization=decimal.Decimal('70'),
            diluted_earnings_per_share=10.0,
            discontinued_operations_income_after_tax=decimal.Decimal('500'),
            earnings_before_interest_and_tax=decimal.Decimal('890'),
            earnings_before_interest_tax_depreciation_and_amortization=decimal.Decimal('770'),
            general_and_administrative_expense=decimal.Decimal('570'),
            gross_profit=decimal.Decimal('200'),
            income_before_tax=decimal.Decimal('800'),
            income_tax_expense=decimal.Decimal('40'),
            interest_expense=decimal.Decimal('90'),
            interest_income=decimal.Decimal('80'),
            net_income=decimal.Decimal('30000'),
            net_income_deductions=decimal.Decimal('210'),
            net_interest_income=decimal.Decimal('-10'),
            net_total_other_income=decimal.Decimal('650'),
            nonoperating_income_excluding_interest=decimal.Decimal('100'),
            operating_expenses=decimal.Decimal('3540'),
            operating_income=decimal.Decimal('1200'),
            other_expenses=decimal.Decimal('1240'),
            other_net_income_adjustments=decimal.Decimal('120'),
            research_and_development_expense=decimal.Decimal('150'),
            revenues=decimal.Decimal('740000'),
            selling_and_marketing_expense=decimal.Decimal('320'),
            selling_general_and_administrative_expense=decimal.Decimal('1680'),
            weighted_average_basic_shares_outstanding=250,
            weighted_average_diluted_shares_outstanding=60,
        ),
    )

    return {
        '2020-01-01': None,
        '2020-01-03': row,
        '2020-01-04': row,
    }


@pytest.fixture(scope="module")
def example_entity_rows_market_data():
    return {
        '2020-01-01': MarketDataDailyRow(
            date=datetime.date(2020, 1, 1),
            open=decimal.Decimal('11.67'),
            high=decimal.Decimal('17.24'),
            low=decimal.Decimal('10.44'),
            close=decimal.Decimal('16.28'),
            volume=12365478,
            vwap=decimal.Decimal('15.245'),
            open_split_adjusted=decimal.Decimal('11.37'),
            high_split_adjusted=decimal.Decimal('17.14'),
            low_split_adjusted=decimal.Decimal('10.24'),
            close_split_adjusted=decimal.Decimal('16.18'),
            volume_split_adjusted=23654789,
            vwap_split_adjusted=decimal.Decimal('15.245'),
            open_dividend_and_split_adjusted=decimal.Decimal('11.27'),
            high_dividend_and_split_adjusted=decimal.Decimal('17.04'),
            low_dividend_and_split_adjusted=decimal.Decimal('10.14'),
            close_dividend_and_split_adjusted=decimal.Decimal('16.08'),
            volume_dividend_and_split_adjusted=23654789,
            vwap_dividend_and_split_adjusted=decimal.Decimal('15.045'),
        ),
        '2020-01-03': MarketDataDailyRow(
            date=datetime.date(2020, 1, 3),
            open=decimal.Decimal('20.02'),
            high=decimal.Decimal('24.48'),
            low=decimal.Decimal('12.67'),
            close=decimal.Decimal('20.01'),
            volume=500,
            vwap=decimal.Decimal('22.64'),
            open_split_adjusted=decimal.Decimal('19.02'),
            high_split_adjusted=decimal.Decimal('24.28'),
            low_split_adjusted=decimal.Decimal('12.47'),
            close_split_adjusted=decimal.Decimal('19.01'),
            volume_split_adjusted=1500,
            vwap_split_adjusted=decimal.Decimal('21.64'),
            open_dividend_and_split_adjusted=decimal.Decimal('18.02'),
            high_dividend_and_split_adjusted=decimal.Decimal('24.18'),
            low_dividend_and_split_adjusted=decimal.Decimal('12.27'),
            close_dividend_and_split_adjusted=decimal.Decimal('18.01'),
            volume_dividend_and_split_adjusted=1500,
            vwap_dividend_and_split_adjusted=decimal.Decimal('20.64'),
        ),
        '2020-01-04': MarketDataDailyRow(
            date=datetime.date(2020, 1, 4),
            open=decimal.Decimal('27.29'),
            high=decimal.Decimal('27.69'),
            low=decimal.Decimal('22.40'),
            close=decimal.Decimal('23.75'),
            volume=600,
            vwap=decimal.Decimal('26.94'),
            open_split_adjusted=decimal.Decimal('27.19'),
            high_split_adjusted=decimal.Decimal('27.59'),
            low_split_adjusted=decimal.Decimal('22.30'),
            close_split_adjusted=decimal.Decimal('23.65'),
            volume_split_adjusted=1600,
            vwap_split_adjusted=decimal.Decimal('26.74'),
            open_dividend_and_split_adjusted=decimal.Decimal('27.09'),
            high_dividend_and_split_adjusted=decimal.Decimal('27.49'),
            low_dividend_and_split_adjusted=decimal.Decimal('22.20'),
            close_dividend_and_split_adjusted=decimal.Decimal('23.55'),
            volume_dividend_and_split_adjusted=1600,
            vwap_dividend_and_split_adjusted=decimal.Decimal('26.54'),
        ),
    }


@pytest.fixture(scope="module")
def example_expanded_dividend_data():
    return {
        '2020-01-01': None,
        '2020-01-03': {
            'declaration_date_dividend': decimal.Decimal('0.64'),
            'declaration_date_split_adjusted_dividend': decimal.Decimal('0.71'),
            'ex_dividend_date_dividend': decimal.Decimal('0.64'),
            'ex_dividend_date_split_adjusted_dividend': decimal.Decimal('0.71'),
            'record_date_dividend': decimal.Decimal('0.64'),
            'record_date_split_adjusted_dividend': decimal.Decimal('0.71'),
            'payment_date_dividend': decimal.Decimal('0.64'),
            'payment_date_split_adjusted_dividend': decimal.Decimal('0.71'),
        },
        '2020-01-04': {
            'declaration_date_dividend': decimal.Decimal('0.69'),
            'declaration_date_split_adjusted_dividend': decimal.Decimal('0.88'),
            'ex_dividend_date_dividend': decimal.Decimal('0.69'),
            'ex_dividend_date_split_adjusted_dividend': decimal.Decimal('0.88'),
            'record_date_dividend': decimal.Decimal('0.69'),
            'record_date_split_adjusted_dividend': decimal.Decimal('0.88'),
            'payment_date_dividend': decimal.Decimal('0.69'),
            'payment_date_split_adjusted_dividend': decimal.Decimal('0.88'),
        }
    }


@pytest.fixture(scope="module")
def example_expanded_split_data():
    return {
        '2020-01-01': None,
        '2020-01-03': {
            'split_date_numerator': 4,
            'split_date_denominator': 1,
        },
        '2020-01-04': {
            'split_date_numerator': 2,
            'split_date_denominator': 5,
        }
    }


@pytest.fixture(scope="module")
def example_entities_with_subentities():
    return {
        '1': fixture_entities.ExampleEntity(
            field_int=10,
            field_str='lol',
            field_date=datetime.date(2020, 1, 1),
            field_decimal=decimal.Decimal('1.23'),
            field_subentity=fixture_entities.ExampleSubEntity(
                subfield_int=11,
                subfield_str='string 1',
                subfield_date=datetime.date(2020, 1, 2),
                subfield_decimal=decimal.Decimal('1.231'),
            ),
        ),
        '2': fixture_entities.ExampleEntity(
            field_int=20,
            field_str='hi',
            field_date=datetime.date(2020, 2, 1),
            field_decimal=decimal.Decimal('2.23'),
            field_subentity=fixture_entities.ExampleSubEntity(
                subfield_int=21,
                subfield_str='string 2',
                subfield_date=datetime.date(2020, 2, 2),
                subfield_decimal=decimal.Decimal('1.232'),
            ),
        ),
    }


@pytest.fixture(scope="module")
def example_expand_dated_factors_dividend_data():
    data_rows: DataRows = {
        '2021-01-01': DividendDataRow(
            declaration_date=datetime.date.fromisoformat('2020-11-05'),
            ex_dividend_date=datetime.date.fromisoformat('2021-01-01'),
            record_date=datetime.date.fromisoformat('2021-01-02'),
            payment_date=datetime.date.fromisoformat('2021-02-04'),
            dividend=decimal.Decimal('2.1'),
            dividend_split_adjusted=decimal.Decimal('2.225'),
        ),
        '2021-03-15': DividendDataRow(
            declaration_date=datetime.date.fromisoformat('2021-02-10'),
            ex_dividend_date=datetime.date.fromisoformat('2021-03-15'),
            record_date=datetime.date.fromisoformat('2021-03-18'),
            payment_date=datetime.date.fromisoformat('2021-03-30'),
            dividend=decimal.Decimal('2.3'),
            dividend_split_adjusted=decimal.Decimal('2.431'),
        ),
        '2022-07-07': DividendDataRow(
            declaration_date=datetime.date.fromisoformat('2022-04-30'),
            ex_dividend_date=datetime.date.fromisoformat('2022-07-07'),
            record_date=datetime.date.fromisoformat('2022-07-08'),
            payment_date=datetime.date.fromisoformat('2022-09-01'),
            dividend=decimal.Decimal('1.7'),
            dividend_split_adjusted=decimal.Decimal('1.865'),
        )
    }
    data_rows_with_nones: DataRows = {
        '2021-01-01': DividendDataRow(
            declaration_date=None,
            ex_dividend_date=datetime.date.fromisoformat('2021-01-01'),
            record_date=None,
            payment_date=None,
            dividend=decimal.Decimal('2.1'),
            dividend_split_adjusted=decimal.Decimal('2.225'),
        ),
        '2021-03-15': DividendDataRow(
            declaration_date=datetime.date.fromisoformat('2021-02-10'),
            ex_dividend_date=datetime.date.fromisoformat('2021-03-15'),
            record_date=datetime.date.fromisoformat('2021-03-18'),
            payment_date=datetime.date.fromisoformat('2021-03-30'),
            dividend=decimal.Decimal('2.3'),
            dividend_split_adjusted=decimal.Decimal('2.431'),
        ),
        '2022-07-07': DividendDataRow(
            declaration_date=None,
            ex_dividend_date=datetime.date.fromisoformat('2022-07-07'),
            record_date=datetime.date.fromisoformat('2022-07-08'),
            payment_date=None,
            dividend=decimal.Decimal('1.7'),
            dividend_split_adjusted=decimal.Decimal('1.865'),
        )
    }
    expected_result_full = {
        '2020-11-05': {
            'declaration_date_dividend': decimal.Decimal('2.1'),
            'declaration_date_dividend_split_adjusted': decimal.Decimal('2.225'),
        },
        '2021-01-01': {
            'ex_dividend_date_dividend': decimal.Decimal('2.1'),
            'ex_dividend_date_dividend_split_adjusted': decimal.Decimal('2.225'),
        },
        '2021-01-02': {
            'record_date_dividend': decimal.Decimal('2.1'),
            'record_date_dividend_split_adjusted': decimal.Decimal('2.225'),
        },
        '2021-02-04': {
            'payment_date_dividend': decimal.Decimal('2.1'),
            'payment_date_dividend_split_adjusted': decimal.Decimal('2.225'),
        },
        '2021-02-10': {
            'declaration_date_dividend': decimal.Decimal('2.3'),
            'declaration_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2021-03-15': {
            'ex_dividend_date_dividend': decimal.Decimal('2.3'),
            'ex_dividend_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2021-03-18': {
            'record_date_dividend': decimal.Decimal('2.3'),
            'record_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2021-03-30': {
            'payment_date_dividend': decimal.Decimal('2.3'),
            'payment_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2022-04-30': {
            'declaration_date_dividend': decimal.Decimal('1.7'),
            'declaration_date_dividend_split_adjusted': decimal.Decimal('1.865'),
        },
        '2022-07-07': {
            'ex_dividend_date_dividend': decimal.Decimal('1.7'),
            'ex_dividend_date_dividend_split_adjusted': decimal.Decimal('1.865'),
        },
        '2022-07-08': {
            'record_date_dividend': decimal.Decimal('1.7'),
            'record_date_dividend_split_adjusted': decimal.Decimal('1.865'),
        },
        '2022-09-01': {
            'payment_date_dividend': decimal.Decimal('1.7'),
            'payment_date_dividend_split_adjusted': decimal.Decimal('1.865'),
        },
        '2023-01-01': {}
    }
    expected_result_with_nones = {
        '2020-11-05': {},
        '2021-01-01': {
            'ex_dividend_date_dividend': decimal.Decimal('2.1'),
            'ex_dividend_date_dividend_split_adjusted': decimal.Decimal('2.225'),
        },
        '2021-01-02': {},
        '2021-02-04': {},
        '2021-02-10': {
            'declaration_date_dividend': decimal.Decimal('2.3'),
            'declaration_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2021-03-15': {
            'ex_dividend_date_dividend': decimal.Decimal('2.3'),
            'ex_dividend_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2021-03-18': {
            'record_date_dividend': decimal.Decimal('2.3'),
            'record_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2021-03-30': {
            'payment_date_dividend': decimal.Decimal('2.3'),
            'payment_date_dividend_split_adjusted': decimal.Decimal('2.431'),
        },
        '2022-04-30': {},
        '2022-07-07': {
            'ex_dividend_date_dividend': decimal.Decimal('1.7'),
            'ex_dividend_date_dividend_split_adjusted': decimal.Decimal('1.865'),
        },
        '2022-07-08': {
            'record_date_dividend': decimal.Decimal('1.7'),
            'record_date_dividend_split_adjusted': decimal.Decimal('1.865'),
        },
        '2022-09-01': {},
        '2023-01-01': {}
    }

    return {
        'data_rows': data_rows,
        'data_rows_with_nones': data_rows_with_nones,
        'expected_result_full': expected_result_full,
        'expected_result_with_nones': expected_result_with_nones,
    }


class TestPrivateAddColumnDependency:
    def test_add_column_to_empty_postponed_columns(self):
        postponed:PostponedColumns = {}
        column_name:ColumnIdentifier = 'column1'
        dependency_name:ColumnIdentifier = 'subcolumn1'
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name
        )

        assert column_name in postponed
        assert dependency_name in postponed[column_name]

    def test_add_column_not_in_postponed_columns(self):
        existing_column_came:ColumnIdentifier = 'column2'
        existing_dependency_name:ColumnIdentifier = 'subcolumn2'
        postponed: PostponedColumns = {
            existing_column_came: [existing_dependency_name]
        }
        column_name: ColumnIdentifier = 'column1'
        dependency_name: ColumnIdentifier = 'subcolumn1'
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name
        )

        assert column_name in postponed
        assert dependency_name in postponed[column_name]
        assert existing_column_came in postponed
        assert existing_dependency_name in postponed[existing_column_came]

    def test_add_dependency_not_in_postponed_columns(self):
        existing_dependency_name:ColumnIdentifier = 'subcolumn2'
        column_name: ColumnIdentifier = 'column1'
        postponed: PostponedColumns = {
            column_name: [existing_dependency_name]
        }
        dependency_name: ColumnIdentifier = 'subcolumn1'
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name
        )

        assert dependency_name in postponed[column_name]
        assert existing_dependency_name in postponed[column_name]

    def test_add_existing_dependency_to_postponed_columns(self):
        column_name: ColumnIdentifier = 'column1'
        dependency_name: ColumnIdentifier = 'subcolumn1'
        postponed: PostponedColumns = {
            column_name: [dependency_name]
        }
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name
        )

        assert dependency_name in postponed[column_name]
        assert len(postponed[column_name]) == 1


class TestPrivateExpandDatedFactors:
    def test_expand_dated_factors_full(self, example_expand_dated_factors_dividend_data):
        result = ColumnBuilder._expand_dated_factors(
            iter(example_expand_dated_factors_dividend_data['expected_result_full'].keys()),
            ColumnBuilder._DIVIDEND_DATE_FIELDS,
            ColumnBuilder._DIVIDEND_FACTOR_FIELDS,
            example_expand_dated_factors_dividend_data['data_rows']
        )

        assert result == example_expand_dated_factors_dividend_data['expected_result_full']

    def test_expand_dated_factors_no_data_rows(self):
        data_rows:DataRows = {}
        expected_result = {
            '2022-07-07': None,
            '2022-10-07': None,
            '2023-01-14': None,
        }
        result = ColumnBuilder._expand_dated_factors(
            iter(expected_result.keys()),
            ColumnBuilder._DIVIDEND_DATE_FIELDS,
            ColumnBuilder._DIVIDEND_FACTOR_FIELDS,
            data_rows
        )

        assert result == expected_result

    def test_expand_dated_factors_more_dates_than_data_rows(self, example_expand_dated_factors_dividend_data):
        results_with_extra_dates = {
            **example_expand_dated_factors_dividend_data['expected_result_full'],
            '2020-11-01': {},   # before all dates
            '2021-01-03': {},
            '2021-01-05': {},
            '2023-01-06': {},    #after all dates
        }
        expected_result = dict(
            sorted(
                results_with_extra_dates.items()
            )
        )

        result = ColumnBuilder._expand_dated_factors(
            iter(expected_result.keys()),
            ColumnBuilder._DIVIDEND_DATE_FIELDS,
            ColumnBuilder._DIVIDEND_FACTOR_FIELDS,
            example_expand_dated_factors_dividend_data['data_rows']
        )

        assert result == expected_result

    def test_expand_dated_factors_with_none_date_fields(self, example_expand_dated_factors_dividend_data):
        result = ColumnBuilder._expand_dated_factors(
            iter(example_expand_dated_factors_dividend_data['expected_result_with_nones'].keys()),
            ColumnBuilder._DIVIDEND_DATE_FIELDS,
            ColumnBuilder._DIVIDEND_FACTOR_FIELDS,
            example_expand_dated_factors_dividend_data['data_rows_with_nones']
        )

        assert result == example_expand_dated_factors_dividend_data['expected_result_with_nones']


class TestPrivateGenerateColumn:
    def test_generate_column_with_empty_rows(self):
        data_entity_rows = {'1': None}
        field = 'test_field'
        subfield = 'test_subfield'
        result = ColumnBuilder._generate_column(
            data_entity_rows,
            field,
            subfield
        )
        expected_result = DataColumn.load([None])

        assert DataColumn.fully_equal(
            expected_result,
            result,
            equal_nulls=True
        )

    def test_generate_column_with_full_rows(
        self,
        example_entities_with_subentities
    ):
        field = 'field_subentity'
        subfield = 'subfield_int'
        expected_result = DataColumn.load([
            11,
            21,
        ])
        result = ColumnBuilder._generate_column(
            example_entities_with_subentities,
            field,
            subfield
        )

        assert DataColumn.fully_equal(
            DataColumn.load(expected_result),
            result,
            equal_nulls=True
        )


class TestPrivateGetCalculationFunction:
    def test_get_calculation_function_existing(self):
        result = ColumnBuilder._get_calculation_function(
            'example_sum_function_1',
            [fixture_entities]
        )
        expected_result = fixture_entities.example_sum_function_1

        assert result is expected_result

    def test_get_calculation_function_missing(self):
        with pytest.raises(ColumnBuilderCustomFunctionNotFoundError):
            ColumnBuilder._get_calculation_function(
                'example_nonexistent_function',
                [fixture_entities]
            )


class TestPrivateGetClassOfFirstNonEmptyRow:
    def test_empty_data_rows(self):
        data_rows = {}

        assert ColumnBuilder._get_class_of_first_non_empty_row(data_rows) is None

    def test_nested_data_rows_with_empty_first_row(self):
        data_rows = {
            '2021-01-01': None,
            '2021-01-02': ExampleEntity1(
                field1=10,
                field2='hi',
                subfield_field1=ExampleSubEntity1(
                    subfield1=15,
                    subfield2="hello",
                )
            ),
            '2021-01-03': None,
        }
        expected = ExampleSubEntity1
        result = ColumnBuilder._get_class_of_first_non_empty_row(
            data_rows,
            'subfield_field1'
        )

        assert result == expected

    def test_nested_data_rows_with_non_empty_first_row(self):
        data_rows = {
            '2021-01-01': ExampleEntity1(
                field1=10,
                field2='hi',
                subfield_field1=ExampleSubEntity1(
                    subfield1=15,
                    subfield2="hello",
                )
            ),
            '2021-01-02': None,
        }
        expected = ExampleSubEntity1
        result = ColumnBuilder._get_class_of_first_non_empty_row(
            data_rows,
            'subfield_field1'
        )

        assert result == expected

    def test_non_empty_rows_with_empty_data(self):
        data_rows = {
            '2021-01-01': None,
            '2021-01-02': None,
            '2021-01-03': None,
        }
        result = ColumnBuilder._get_class_of_first_non_empty_row(
            data_rows,
        )

        assert result is None

    def test_simple_data_rows_with_empty_first_row(self):
        data_rows = {
            '2021-01-01': None,
            '2021-01-02': ExampleSubEntity1(
                subfield1=15,
                subfield2="hello",
            ),
            '2021-01-03': None,
        }
        expected = ExampleSubEntity1
        result = ColumnBuilder._get_class_of_first_non_empty_row(
            data_rows,
        )

        assert result == expected

    def test_simple_data_rows_with_non_empty_first_row(self):
        data_rows = {
            '2021-01-01': ExampleSubEntity1(
                subfield1=15,
                subfield2="hello",
            ),
            '2021-01-02': None,
        }
        expected = ExampleSubEntity1
        result = ColumnBuilder._get_class_of_first_non_empty_row(
            data_rows,
        )

        assert result == expected


class TestPrivateGetCombinedFieldColumnNames:
    def test_get_combined_field_column_names(self):
        field_source = 'test'
        date_fields = (
            'date_type_1',
            'date_type_2',
        )
        factor_fields = (
            'factor_type_1',
            'factor_type_2',
        )
        result = ColumnBuilder._get_combined_field_column_names(
            field_source,
            date_fields,
            factor_fields
        )
        memoized_result = ColumnBuilder._get_combined_field_column_names(
            field_source,
            date_fields,
            factor_fields
        )
        expected_result = [
            'date_type_1_factor_type_1',
            'date_type_1_factor_type_2',
            'date_type_2_factor_type_1',
            'date_type_2_factor_type_2',
        ]

        assert result == expected_result
        assert memoized_result == expected_result


class TestPrivateGetFieldFromRow:
    def test_empty_row(self):
        result = ColumnBuilder._get_field_from_row(
            None,
            'whatever'
        )

        assert result is None

    def test_dict_existing_field(self):
        test_value = 189
        test_dict = {
            'a': 115,
            'b': test_value
        }

        assert (
            ColumnBuilder._get_field_from_row(
                test_dict,
                'b'
            )
            == test_value
        )

    def test_dict_existing_subfield(self):
        test_value = 57
        test_dict = {
            'c': 115,
            'd': {
                'e': 54,
                'f': test_value,
            }
        }

        assert (
            ColumnBuilder._get_field_from_row(
                test_dict,
                'd',
                'f'
            )
            == test_value
        )

    def test_dict_missing_field(self):
        test_dict = {
            'a': 115,
            'b': 189
        }

        assert (
            ColumnBuilder._get_field_from_row(
                test_dict,
                'c'
            )
            is None
        )

    def test_dict_missing_subfield(self):
        test_dict = {
            'c': 115,
            'd': {
                'e': 127,
                'f': 54
            }
        }

        assert (
            ColumnBuilder._get_field_from_row(
                test_dict,
                'd',
                'g'
            )
            is None
        )

    def test_dict_with_non_dict_subfield(self):
        test_dict = {
            'c': 115,
            'd': {
                'e': 127,
                'f': 54
            }
        }

        assert (
            ColumnBuilder._get_field_from_row(
                test_dict,
                'c',
                'g'
            )
            is None
        )

    def test_entity_existing_field(
        self,
        example_entities_with_subentities
    ):
        assert (
            ColumnBuilder._get_field_from_row(
                example_entities_with_subentities['1'],
                'field_str',
            )
            == 'lol'
        )

    def test_entity_existing_subfield(
        self,
        example_entities_with_subentities
    ):
        assert (
            ColumnBuilder._get_field_from_row(
                example_entities_with_subentities['1'],
                'field_subentity',
                'subfield_str'
            )
            == 'string 1'
        )

    def test_entity_missing_field(
        self,
        example_entities_with_subentities
    ):
        assert (
            ColumnBuilder._get_field_from_row(
                example_entities_with_subentities['1'],
                'nonexistent_field',
            )
            is None
        )

    def test_entity_missing_subfield(
        self,
        example_entities_with_subentities
    ):
        assert (
            ColumnBuilder._get_field_from_row(
                example_entities_with_subentities['1'],
                'field_subentity',
                'nonexistent_field'
            )
            is None
        )


class TestPrivateGetFunctionParams:
    def test_get_function_params_correct(self):
        result = ColumnBuilder._get_function_params(fixture_entities.example_sum_function_1)
        expected_result = [
            'a',
            'b',
        ]

        assert result == expected_result

    def test_get_function_params_not_callable(self):
        with pytest.raises(TypeError):
            ColumnBuilder._get_function_params(42)  # type: ignore


class TestPrivateInfillData:
    def test_infill_data_rows_with_dates_starting_after(self):
        data_rows = {
            '2021-04-03': ExampleSubEntity1(
                1,
                'subtest1'
            ),
            '2021-04-06': ExampleSubEntity1(
                2,
                'subtest2'
            ),
            '2021-04-12': ExampleSubEntity1(
                3,
                'subtest3'
            ),
        }
        expected_result = {
            '2021-04-04': data_rows['2021-04-03'],
            '2021-04-06': data_rows['2021-04-06'],
            '2021-04-09': data_rows['2021-04-06'],
            '2022-12-21': data_rows['2021-04-12'],
        }
        dates = iter([
            '2021-04-04',
            '2021-04-06',
            '2021-04-09',
            '2022-12-21',
        ])

        assert (
            ColumnBuilder._infill_data(
                dates,
                data_rows
            )
            == expected_result
        )

    def test_infill_data_rows_with_dates_starting_before(self):
        data_rows = {
            '2021-04-03': ExampleSubEntity1(
                1,
                'subtest1'
            ),
            '2021-04-06': ExampleSubEntity1(
                2,
                'subtest2'
            ),
            '2021-04-12': ExampleSubEntity1(
                3,
                'subtest3'
            ),
        }
        expected_result = {
            '2021-04-02': None,
            '2021-04-04': data_rows['2021-04-03'],
            '2021-04-06': data_rows['2021-04-06'],
            '2021-04-09': data_rows['2021-04-06'],
            '2022-12-21': data_rows['2021-04-12'],
        }
        dates = iter(
            expected_result.keys()
        )

        assert (
            ColumnBuilder._infill_data(
                dates,
                data_rows
            )
            == expected_result
        )

    def test_infill_data_rows_with_first_date_after_all_rows(self):
        data_rows = {
            '2021-04-03': ExampleSubEntity1(
                1,
                'subtest1'
            ),
            '2021-04-06': ExampleSubEntity1(
                2,
                'subtest2'
            ),
            '2021-04-12': ExampleSubEntity1(
                3,
                'subtest3'
            ),
        }
        expected_result = {
            '2021-04-13': data_rows['2021-04-12'],
            '2022-12-21': data_rows['2021-04-12'],
        }
        dates = iter([
            '2021-04-13',
            '2022-12-21',
        ])

        assert (
            ColumnBuilder._infill_data(
                dates,
                data_rows
            )
            == expected_result
        )

    def test_infill_data_rows_without_dates(self):
        dates = iter({})
        data_rows = {
            '2021-04-03': ExampleSubEntity1(
                1,
                'subtest1'
            ),
            '2021-04-06': ExampleSubEntity1(
                2,
                'subtest2'
            ),
            '2021-04-12': ExampleSubEntity1(
                3,
                'subtest3'
            ),
        }
        with pytest.raises(ColumnBuilderNoDatesToInfillError):
            ColumnBuilder._infill_data(
                dates,
                data_rows
            )

    def test_infill_dates_without_data_rows(self):
        expected_result = {
            '2021-04-04': None,
            '2021-04-09': None,
            '2022-12-21': None,
        }
        dates = iter(
            expected_result.keys()
        )

        assert (
            ColumnBuilder._infill_data(
                dates,
                {}
            )
            == expected_result
        )


class TestPrivateProcessColumnsWithAvailableDependencies:
    def test_column_already_in_completed_columns(
        self,
        example_entity_rows_market_data,
    ):
        expected_completed_columns = {
            'm_vwap': DataColumn.load([
                decimal.Decimal('1.251'),
                decimal.Decimal('1.252'),
                None,
                decimal.Decimal('1.264'),
                None,
                decimal.Decimal('1.247'),
                decimal.Decimal('1.252'),
            ])
        }
        completed_columns = copy.deepcopy(expected_completed_columns)
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'m_vwap'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )

        assert (
            set(expected_completed_columns.keys())
            == set(completed_columns.keys())
        )

        assert DataColumn.fully_equal(
            expected_completed_columns['m_vwap'],
            completed_columns['m_vwap'],
            equal_nulls=True
        )

    def test_c_column_with_c_params_all_available(
        self,
        example_entity_rows_market_data,
    ):
        completed_columns: CompletedColumns = {
            'm_close': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].close,
                example_entity_rows_market_data['2020-01-03'].close,
                example_entity_rows_market_data['2020-01-04'].close,
            ]),
            'c_sum_mhigh_and_mlow': DataColumn.load([
                (
                    example_entity_rows_market_data['2020-01-01'].high
                    + example_entity_rows_market_data['2020-01-01'].low
                ),
                (
                    example_entity_rows_market_data['2020-01-03'].high
                    + example_entity_rows_market_data['2020-01-03'].low
                ),
                (
                    example_entity_rows_market_data['2020-01-04'].high
                    + example_entity_rows_market_data['2020-01-04'].low
                ),
            ]),
        }
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'c_multiply_sum_by_mclose'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            (
                (
                    example_entity_rows_market_data['2020-01-01'].high
                    + example_entity_rows_market_data['2020-01-01'].low
                )
                * example_entity_rows_market_data['2020-01-01'].close
            ),
             (
                 (
                    example_entity_rows_market_data['2020-01-03'].high
                    + example_entity_rows_market_data['2020-01-03'].low
                 )
                 * example_entity_rows_market_data['2020-01-03'].close
            ),
              (
                  (
                    example_entity_rows_market_data['2020-01-04'].high
                    + example_entity_rows_market_data['2020-01-04'].low
                  )
                  * example_entity_rows_market_data['2020-01-04'].close
            ),
        ])

        assert DataColumn.fully_equal(
            completed_columns['c_multiply_sum_by_mclose'],
            expected_column,
            equal_nulls=True
        )

    def test_c_column_postponed_without_c_params_all_available(
        self,
        example_entity_rows_market_data,
    ):
        completed_columns: CompletedColumns = {
            'm_high': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].high,
                example_entity_rows_market_data['2020-01-03'].high,
                example_entity_rows_market_data['2020-01-04'].high,
            ]),
            'm_low': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].low,
                example_entity_rows_market_data['2020-01-03'].low,
                example_entity_rows_market_data['2020-01-04'].low,
            ]),
        }
        postponed_columns: PostponedColumns = {
            'c_sum_mhigh_and_mlow': []
        }
        ColumnBuilder._process_columns_with_available_dependencies(
            {'c_sum_mhigh_and_mlow'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_postponed_columns = {}

        assert postponed_columns == expected_postponed_columns

    def test_c_column_with_c_params_unavailable_not_postponed(
        self,
        example_entity_rows_market_data,
    ):
        expected_completed_columns: CompletedColumns = {
            'm_close': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].close,
                example_entity_rows_market_data['2020-01-03'].close,
                example_entity_rows_market_data['2020-01-04'].close,
            ]),
        }
        completed_columns = copy.deepcopy(expected_completed_columns)
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'c_multiply_sum_by_mclose'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_postponed_columns = {
            'c_multiply_sum_by_mclose': ['c_sum_mhigh_and_mlow']
        }

        assert (
            set(expected_completed_columns.keys())
            == set(completed_columns.keys())
        )

        assert (
            expected_postponed_columns
            == postponed_columns
        )

    def test_c_column_without_c_params_all_available(
        self,
        example_entity_rows_market_data
    ):
        completed_columns: CompletedColumns = {
            'm_high': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].high,
                example_entity_rows_market_data['2020-01-03'].high,
                example_entity_rows_market_data['2020-01-04'].high,
            ]),
            'm_low': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].low,
                example_entity_rows_market_data['2020-01-03'].low,
                example_entity_rows_market_data['2020-01-04'].low,
            ]),
        }
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'c_sum_mhigh_and_mlow'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            (
                example_entity_rows_market_data['2020-01-01'].high
                + example_entity_rows_market_data['2020-01-01'].low
            ),
            (
                example_entity_rows_market_data['2020-01-03'].high
                + example_entity_rows_market_data['2020-01-03'].low
            ),
            (
                example_entity_rows_market_data['2020-01-04'].high
                + example_entity_rows_market_data['2020-01-04'].low
            ),
        ])

        assert DataColumn.fully_equal(
            completed_columns['c_sum_mhigh_and_mlow'],
            expected_column,
            equal_nulls=True
        )

    def test_c_column_without_c_params_with_param_not_yet_completed(
        self,
        example_entity_rows_market_data
    ):
        completed_columns: CompletedColumns = {
            'm_high': DataColumn.load([
                example_entity_rows_market_data['2020-01-01'].high,
                example_entity_rows_market_data['2020-01-03'].high,
                example_entity_rows_market_data['2020-01-04'].high,
            ]),
        }
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'c_sum_mhigh_and_mlow'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            (
                example_entity_rows_market_data['2020-01-01'].high
                + example_entity_rows_market_data['2020-01-01'].low
            ),
            (
                example_entity_rows_market_data['2020-01-03'].high
                + example_entity_rows_market_data['2020-01-03'].low
            ),
            (
                example_entity_rows_market_data['2020-01-04'].high
                + example_entity_rows_market_data['2020-01-04'].low
            ),
        ])

        assert DataColumn.fully_equal(
            completed_columns['c_sum_mhigh_and_mlow'],
            expected_column,
            equal_nulls=True
        )

    def test_d_column_existent_property(
        self,
        example_entity_rows_market_data,
        example_expanded_dividend_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'d_ex_dividend_date_dividend'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows=example_expanded_dividend_data,
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            None,
            example_expanded_dividend_data['2020-01-03']['ex_dividend_date_dividend'],
            example_expanded_dividend_data['2020-01-04']['ex_dividend_date_dividend'],
        ])

        assert DataColumn.fully_equal(
            completed_columns['d_ex_dividend_date_dividend'],
            expected_column,
            equal_nulls=True
        )

    def test_d_column_nonexistent_property(
        self,
        example_entity_rows_market_data,
        example_expand_dated_factors_dividend_data
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'d_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows=example_expand_dated_factors_dividend_data['expected_result_full'],
                expanded_split_data_rows={},
                infilled_fundamental_data_rows={},
                market_data_rows=example_entity_rows_market_data,
            )

    def test_f_column_existent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'f_fiscal_year'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].fiscal_year,
            example_entity_rows_fundamental_data['2020-01-04'].fiscal_year,
        ])

        assert DataColumn.fully_equal(
            completed_columns['f_fiscal_year'],
            expected_column,
            equal_nulls=True
        )

    def test_f_column_nonexistent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'f_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows={},
                infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
                market_data_rows=example_entity_rows_market_data,
            )

    def test_fbs_column_existent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'fbs_total_liabilities_and_equity'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].balance_sheet.total_liabilities_and_equity,
            example_entity_rows_fundamental_data['2020-01-04'].balance_sheet.total_liabilities_and_equity,
        ])

        assert DataColumn.fully_equal(
            completed_columns['fbs_total_liabilities_and_equity'],
            expected_column,
            equal_nulls=True
        )

    def test_fbs_column_nonexistent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'fbs_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows={},
                infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
                market_data_rows=example_entity_rows_market_data,
            )

    def test_fcf_column_existent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'fcf_net_income'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].cash_flow.net_income,
            example_entity_rows_fundamental_data['2020-01-04'].cash_flow.net_income,
        ])

        assert DataColumn.fully_equal(
            completed_columns['fcf_net_income'],
            expected_column,
            equal_nulls=True
        )

    def test_fcf_column_nonexistent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'fcf_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows={},
                infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
                market_data_rows=example_entity_rows_market_data,
            )

    def test_fis_column_existent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'fis_gross_profit'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].income_statement.gross_profit,
            example_entity_rows_fundamental_data['2020-01-04'].income_statement.gross_profit,
        ])

        assert DataColumn.fully_equal(
            completed_columns['fis_gross_profit'],
            expected_column,
            equal_nulls=True
        )

    def test_fis_column_nonexistent_property(
        self,
        example_entity_rows_market_data,
        example_entity_rows_fundamental_data
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'fis_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows={},
                infilled_fundamental_data_rows=example_entity_rows_fundamental_data,
                market_data_rows=example_entity_rows_market_data,
            )

    def test_m_column_existent_property(
        self,
        example_entity_rows_market_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'m_close'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows={},
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            example_entity_rows_market_data['2020-01-01'].close,
            example_entity_rows_market_data['2020-01-03'].close,
            example_entity_rows_market_data['2020-01-04'].close,
        ])

        assert DataColumn.fully_equal(
            completed_columns['m_close'],
            expected_column,
            equal_nulls=True
        )

    def test_m_column_nonexistent_property(self, example_entity_rows_market_data):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'m_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows={},
                infilled_fundamental_data_rows={},
                market_data_rows=example_entity_rows_market_data,
            )

    def test_s_column_existent_property(
        self,
        example_entity_rows_market_data,
        example_expanded_split_data,
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        ColumnBuilder._process_columns_with_available_dependencies(
            {'s_split_date_numerator'},
            completed_columns,
            postponed_columns,
            calculation_modules=[calculations],
            expanded_dividend_data_rows={},
            expanded_split_data_rows=example_expanded_split_data,
            infilled_fundamental_data_rows={},
            market_data_rows=example_entity_rows_market_data,
        )
        expected_column = DataColumn.load([
            None,
            example_expanded_split_data['2020-01-03']['split_date_numerator'],
            example_expanded_split_data['2020-01-04']['split_date_numerator'],
        ])

        assert DataColumn.fully_equal(
            completed_columns['s_split_date_numerator'],
            expected_column,
            equal_nulls=True
        )

    def test_s_column_nonexistent_property(
        self,
        example_entity_rows_market_data,
        example_expanded_split_data
    ):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'s_non_existent_column'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows=example_expanded_split_data,
                infilled_fundamental_data_rows={},
                market_data_rows=example_entity_rows_market_data,
            )

    def test_unknown_prefix(self, example_entity_rows_market_data):
        completed_columns: CompletedColumns = {}
        postponed_columns: PostponedColumns = {}
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_columns_with_available_dependencies(
                {'7_non_existent_prefix'},
                completed_columns,
                postponed_columns,
                calculation_modules=[calculations],
                expanded_dividend_data_rows={},
                expanded_split_data_rows={},
                infilled_fundamental_data_rows={},
                market_data_rows=example_entity_rows_market_data,
            )


class TestPrivatePropertyExistsInClass:
    def test_existing_property(self):
        assert (
            ColumnBuilder._property_exists_in_class(
                fixture_entities.ExampleEntity,
                'field_str'
            )
            is True
        )

    def test_missing_property(self):
        assert (
            ColumnBuilder._property_exists_in_class(
                fixture_entities.ExampleEntity,
                'nonexistent_field'
            )
            is False
        )


class TestPrivateRemoveColumnDependency:
    def test_remove_existing_column_dependency(self):
        postponed: PostponedColumns = {}
        column_name: ColumnIdentifier = 'column1'
        dependency_name1: ColumnIdentifier = 'subcolumn1'
        dependency_name2: ColumnIdentifier = 'subcolumn2'
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name1
        )
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name2
        )
        ColumnBuilder._remove_column_dependency(
            postponed,
            dependency_name1
        )
        expected_result = {
            column_name: [dependency_name2]
        }

        assert postponed == expected_result

    def test_remove_nonexisting_column_dependency(self):
        postponed: PostponedColumns = {}
        column_name: ColumnIdentifier = 'column1'
        dependency_name1: ColumnIdentifier = 'subcolumn1'
        dependency_name2: ColumnIdentifier = 'subcolumn2'
        ColumnBuilder._add_column_dependency(
            postponed,
            column_name,
            dependency_name2
        )
        ColumnBuilder._remove_column_dependency(
            postponed,
            dependency_name1
        )
        expected_result = {
            column_name: [dependency_name2]
        }

        assert postponed == expected_result
