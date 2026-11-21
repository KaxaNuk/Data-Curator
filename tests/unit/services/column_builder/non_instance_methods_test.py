import datetime
import decimal
import typing

import pyarrow
import pytest

from kaxanuk.data_curator import (
    DataColumn,
    ReferencePeriods,
    clock_sync_methods,
)
from kaxanuk.data_curator.data_blocks.dividends import (
    DividendsDataBlock,
)
from kaxanuk.data_curator.data_blocks.fundamentals import (
    FundamentalsDataBlock,
)
from kaxanuk.data_curator.data_blocks.market_daily import (
    MarketDailyDataBlock,
)
from kaxanuk.data_curator.data_blocks.splits import (
    SplitsDataBlock,
)
from kaxanuk.data_curator.entities import (
    ClockSpec,
    Configuration,
    DividendDataRow,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    MarketDataDailyRow,
    SplitDataRow,
)
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCalculationError,
    ColumnBuilderClockError,
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderUnavailableEntityFieldError,
    InjectedDependencyError,
)
from kaxanuk.data_curator.services.column_builder import (
    CalculatedColumn,
    ClockedDataBlockRows,
    ColumnBuilder,
    CompletedColumns,
    DatedFactorColumn,
)
from .fixtures import (
    calculations,
    data_block_entities,
    data_blocks,
    entities,
)
# note: data_column_debugger is defined in tests/conftest.py


DAILY_CLOCK_SPEC = ClockSpec(
    reference_period=ReferencePeriods.DAY,
    periods_per_row=1,
)
# the dividend and split rows are instants dated without a time
EVENT_CLOCK_SPEC = ClockSpec(
    reference_period=ReferencePeriods.DAY,
    periods_per_row=0,
)
QUARTERLY_CLOCK_SPEC = ClockSpec(
    reference_period=ReferencePeriods.MONTH,
    periods_per_row=3,
)


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
            net_cash_from_investing_activities=decimal.Decimal('20'),
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
def example_entities_with_subentities():
    return {
        '1': entities.ExampleEntity(
            field_int=10,
            field_str='lol',
            field_date=datetime.date(2020, 1, 1),
            field_decimal=decimal.Decimal('1.23'),
            field_subentity=entities.ExampleSubEntity(
                subfield_int=11,
                subfield_str='string 1',
                subfield_date=datetime.date(2020, 1, 2),
                subfield_decimal=decimal.Decimal('1.231'),
            ),
        ),
        '2': entities.ExampleEntity(
            field_int=20,
            field_str='hi',
            field_date=datetime.date(2020, 2, 1),
            field_decimal=decimal.Decimal('2.23'),
            field_subentity=entities.ExampleSubEntity(
                subfield_int=21,
                subfield_str='string 2',
                subfield_date=datetime.date(2020, 2, 2),
                subfield_decimal=decimal.Decimal('1.232'),
            ),
        ),
    }


@pytest.fixture(scope="module")
def example_calculated_column():
    return CalculatedColumn(
        function=calculations.c_sum_mhigh_and_mlow,
        module=calculations,
        parameter_names=(
            'm_high',
            'm_low',
        ),
    )


@pytest.fixture(scope="module")
def example_clocked_rows_by_block(
    example_entity_rows_dividend_data,
    example_entity_rows_fundamental_data,
    example_entity_rows_market_data,
    example_entity_rows_split_data,
    example_master_clock_column,
):
    # Each built-in data block's rows, with the clocks the ColumnBuilder builds for them
    return {
        DividendsDataBlock: ClockedDataBlockRows(
            clocks=ColumnBuilder._build_data_block_clocks(
                clock_spec=EVENT_CLOCK_SPEC,
                data_block=DividendsDataBlock,
                master_clock_column=example_master_clock_column,
                master_clock_data_block=MarketDailyDataBlock,
                rows=example_entity_rows_dividend_data,
            ),
            rows=tuple(example_entity_rows_dividend_data.values()),
        ),
        FundamentalsDataBlock: ClockedDataBlockRows(
            clocks=ColumnBuilder._build_data_block_clocks(
                clock_spec=QUARTERLY_CLOCK_SPEC,
                data_block=FundamentalsDataBlock,
                master_clock_column=example_master_clock_column,
                master_clock_data_block=MarketDailyDataBlock,
                rows=example_entity_rows_fundamental_data,
            ),
            rows=tuple(example_entity_rows_fundamental_data.values()),
        ),
        MarketDailyDataBlock: ClockedDataBlockRows(
            clocks=ColumnBuilder._build_data_block_clocks(
                clock_spec=DAILY_CLOCK_SPEC,
                data_block=MarketDailyDataBlock,
                master_clock_column=example_master_clock_column,
                master_clock_data_block=MarketDailyDataBlock,
                rows=example_entity_rows_market_data,
            ),
            rows=tuple(example_entity_rows_market_data.values()),
        ),
        SplitsDataBlock: ClockedDataBlockRows(
            clocks=ColumnBuilder._build_data_block_clocks(
                clock_spec=EVENT_CLOCK_SPEC,
                data_block=SplitsDataBlock,
                master_clock_column=example_master_clock_column,
                master_clock_data_block=MarketDailyDataBlock,
                rows=example_entity_rows_split_data,
            ),
            rows=tuple(example_entity_rows_split_data.values()),
        ),
    }


@pytest.fixture(scope="module")
def example_clocked_rows_of_dividends_sharing_dates(example_master_clock_column):
    # Two dividends declared on 2020-01-02, between the master clock dates, and paid on the same date
    dividend_rows = {
        '2020-01-03': DividendDataRow(
            declaration_date=datetime.date(2020, 1, 2),
            ex_dividend_date=datetime.date(2020, 1, 3),
            record_date=None,
            payment_date=datetime.date(2020, 1, 4),
            dividend=decimal.Decimal('0.64'),
            dividend_split_adjusted=decimal.Decimal('0.64'),
        ),
        '2020-01-04': DividendDataRow(
            declaration_date=datetime.date(2020, 1, 2),
            ex_dividend_date=datetime.date(2020, 1, 4),
            record_date=None,
            payment_date=datetime.date(2020, 1, 4),
            dividend=decimal.Decimal('0.69'),
            dividend_split_adjusted=decimal.Decimal('0.69'),
        ),
    }

    return {
        DividendsDataBlock: ClockedDataBlockRows(
            clocks=ColumnBuilder._build_data_block_clocks(
                clock_spec=EVENT_CLOCK_SPEC,
                data_block=DividendsDataBlock,
                master_clock_column=example_master_clock_column,
                master_clock_data_block=MarketDailyDataBlock,
                rows=dividend_rows,
            ),
            rows=tuple(dividend_rows.values()),
        ),
    }


@pytest.fixture(scope="module")
def example_entity_rows_dividend_data():
    return {
        '2020-01-03': DividendDataRow(
            declaration_date=datetime.date(2019, 12, 1),
            ex_dividend_date=datetime.date(2020, 1, 3),
            record_date=datetime.date(2020, 1, 4),
            payment_date=datetime.date(2020, 1, 4),
            dividend=decimal.Decimal('0.64'),
            dividend_split_adjusted=decimal.Decimal('0.71'),
        ),
        '2020-01-04': DividendDataRow(
            declaration_date=datetime.date(2020, 1, 1),
            ex_dividend_date=datetime.date(2020, 1, 4),
            record_date=None,
            payment_date=datetime.date(2020, 2, 1),
            dividend=decimal.Decimal('0.69'),
            dividend_split_adjusted=decimal.Decimal('0.88'),
        ),
    }


@pytest.fixture(scope="module")
def example_entity_rows_split_data():
    return {
        '2020-01-03': SplitDataRow(
            split_date=datetime.date(2020, 1, 3),
            numerator=4.0,
            denominator=1.0,
        ),
        '2020-01-04': SplitDataRow(
            split_date=datetime.date(2020, 1, 4),
            numerator=2.0,
            denominator=5.0,
        ),
    }


@pytest.fixture(scope="module")
def example_master_clock_column():
    # The dates of the market data rows
    return DataColumn(
        pyarrow.array([
            datetime.date(2020, 1, 1),
            datetime.date(2020, 1, 3),
            datetime.date(2020, 1, 4),
        ]),
        clock_master=True,
        clock_periods_per_row=1,
        clock_reference_period=ReferencePeriods.DAY,
    )


class TestInitializeDataBlocks:
    def test_data_block_claiming_calculated_column_prefix(self):
        # A data block with the calculated columns' prefix would take over all of them
        class CalculatedPrefixDataBlock(MarketDailyDataBlock):
            prefix_entity_map: typing.ClassVar = {
                'c': MarketDataDailyRow,
            }

        with pytest.raises(InjectedDependencyError):
            ColumnBuilder.initialize_data_blocks(
                calculation_modules=[],
                configuration=Configuration(
                    start_date=datetime.date(2024, 1, 1),
                    end_date=datetime.date(2024, 2, 1),
                    period='annual',
                    identifiers=('TEST',),
                    columns=('m_close',),
                ),
                data_blocks=[CalculatedPrefixDataBlock],
            )


class TestPrivateBuildDataBlockClocks:
    def test_master_clock_data_block(
        self,
        example_entity_rows_market_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._build_data_block_clocks(
            clock_spec=DAILY_CLOCK_SPEC,
            data_block=MarketDailyDataBlock,
            master_clock_column=example_master_clock_column,
            master_clock_data_block=MarketDailyDataBlock,
            rows=example_entity_rows_market_data,
        )

        assert result['date'] is example_master_clock_column

    def test_clock_sync_field_from_row_keys(
        self,
        example_entity_rows_fundamental_data,
        example_master_clock_column,
    ):
        # The first row has no data, but still has its key
        result = ColumnBuilder._build_data_block_clocks(
            clock_spec=QUARTERLY_CLOCK_SPEC,
            data_block=FundamentalsDataBlock,
            master_clock_column=example_master_clock_column,
            master_clock_data_block=MarketDailyDataBlock,
            rows=example_entity_rows_fundamental_data,
        )
        expected = pyarrow.array(
            [
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 3),
                datetime.date(2020, 1, 4),
            ],
            type=pyarrow.date32(),
        )

        assert result['filing_date'].reference_array.equals(expected)

    def test_dated_factor_date_fields(
        self,
        example_entity_rows_dividend_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._build_data_block_clocks(
            clock_spec=EVENT_CLOCK_SPEC,
            data_block=DividendsDataBlock,
            master_clock_column=example_master_clock_column,
            master_clock_data_block=MarketDailyDataBlock,
            rows=example_entity_rows_dividend_data,
        )
        expected = {
            'declaration_date',
            'ex_dividend_date',
            'payment_date',
            'record_date',
        }

        assert set(result) == expected

    def test_dated_factor_date_field_values(
        self,
        example_entity_rows_dividend_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._build_data_block_clocks(
            clock_spec=EVENT_CLOCK_SPEC,
            data_block=DividendsDataBlock,
            master_clock_column=example_master_clock_column,
            master_clock_data_block=MarketDailyDataBlock,
            rows=example_entity_rows_dividend_data,
        )
        expected = pyarrow.array([
            datetime.date(2020, 1, 4),
            datetime.date(2020, 2, 1),
        ])

        assert result['payment_date'].reference_array.equals(expected)


class TestPrivateBuildMasterClockColumn:
    def test_row_keys(self, example_entity_rows_market_data):
        result = ColumnBuilder._build_master_clock_column(
            clock_spec=DAILY_CLOCK_SPEC,
            data_block=MarketDailyDataBlock,
            rows=example_entity_rows_market_data,
        )
        expected = pyarrow.array(
            [
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 3),
                datetime.date(2020, 1, 4),
            ],
            type=pyarrow.date32(),
        )

        assert result.reference_array.equals(expected)

    def test_clock_spec(self, example_entity_rows_market_data):
        result = ColumnBuilder._build_master_clock_column(
            clock_spec=QUARTERLY_CLOCK_SPEC,
            data_block=MarketDailyDataBlock,
            rows=example_entity_rows_market_data,
        )
        expected = (
            ReferencePeriods.MONTH,
            3,
        )

        assert (result.reference_period, result.periods_per_row) == expected

    def test_unsorted_row_keys(self):
        with pytest.raises(ColumnBuilderClockError):
            ColumnBuilder._build_master_clock_column(
                clock_spec=DAILY_CLOCK_SPEC,
                data_block=MarketDailyDataBlock,
                rows={
                    '2020-01-03': None,
                    '2020-01-01': None,
                },
            )


class TestPrivateBuildReferenceClockColumn:
    def test_reference_clock(self, example_master_clock_column):
        # The filing is known from its filing date onward
        result = ColumnBuilder._build_reference_clock_column(
            clock_field=FundamentalDataRow.filing_date,
            clock_spec=QUARTERLY_CLOCK_SPEC,
            data_block=FundamentalsDataBlock,
            master_clock_column=example_master_clock_column,
            timestamps=pyarrow.array([
                datetime.date(2020, 1, 2),
            ]),
        )
        expected = pyarrow.array(
            [
                None,
                0,
                0,
            ],
            type=pyarrow.int64(),
        )

        assert result.sync_indices.equals(expected)

    def test_clock_spec_unorderable_against_master_clock(self):
        # 4 weeks can be shorter or longer than a month, depending on the month
        master_clock_column = DataColumn(
            pyarrow.array([
                datetime.date(2020, 1, 31),
            ]),
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.MONTH,
        )

        with pytest.raises(ColumnBuilderClockError):
            ColumnBuilder._build_reference_clock_column(
                clock_field=MarketDataDailyRow.date,
                clock_spec=ClockSpec(
                    reference_period=ReferencePeriods.WEEK,
                    periods_per_row=4,
                ),
                data_block=MarketDailyDataBlock,
                master_clock_column=master_clock_column,
                timestamps=pyarrow.array([
                    datetime.date(2020, 1, 31),
                ]),
            )

    def test_rows_unsyncable_to_master_clock(self):
        # The weekly master clock has two rows in the same week, so the daily rows can't belong to a single one
        master_clock_column = DataColumn(
            pyarrow.array([
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 2),
            ]),
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.WEEK,
        )

        with pytest.raises(ColumnBuilderClockError):
            ColumnBuilder._build_reference_clock_column(
                clock_field=MarketDataDailyRow.date,
                clock_spec=DAILY_CLOCK_SPEC,
                data_block=MarketDailyDataBlock,
                master_clock_column=master_clock_column,
                timestamps=pyarrow.array([
                    datetime.date(2024, 1, 1),
                ]),
            )


class TestPrivateCalculateColumnExtractors:
    def test_main_row_entity(self):
        result = ColumnBuilder._calculate_column_extractors([MarketDailyDataBlock])

        assert result['m'].subentity_field_name is None

    def test_subentity(self):
        result = ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock])

        assert result['fbs'].subentity_field_name == 'balance_sheet'

    def test_dated_factor_entity(self):
        result = ColumnBuilder._calculate_column_extractors([DividendsDataBlock])
        expected = frozenset({
            'declaration_date_dividend',
            'declaration_date_dividend_split_adjusted',
            'ex_dividend_date_dividend',
            'ex_dividend_date_dividend_split_adjusted',
            'record_date_dividend',
            'record_date_dividend_split_adjusted',
            'payment_date_dividend',
            'payment_date_dividend_split_adjusted',
        })

        assert result['d'].valid_column_names == expected

    def test_data_block(self):
        result = ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock])

        assert result['fbs'].data_block is FundamentalsDataBlock

    def test_entity_method_unavailable_as_column(self):
        result = ColumnBuilder._calculate_column_extractors([MarketDailyDataBlock])

        assert '__init__' not in result['m'].valid_column_names

    def test_field_consolidation_method(self):
        result = ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock])

        assert result['fis'].consolidation_methods['revenues'] is clock_sync_methods.ConsolidationMethods.sum

    def test_dated_factor_consolidation_method(self):
        # The column's values are its value field's, so they get consolidated like it
        result = ColumnBuilder._calculate_column_extractors([SplitsDataBlock])

        assert (
            result['s'].consolidation_methods['split_date_numerator']
            is clock_sync_methods.ConsolidationMethods.product
        )

    def test_dated_factor_columns(self):
        result = ColumnBuilder._calculate_column_extractors([SplitsDataBlock])
        expected = {
            'split_date_numerator': DatedFactorColumn(
                date_field=SplitDataRow.split_date,
                value_field=SplitDataRow.numerator,
            ),
            'split_date_denominator': DatedFactorColumn(
                date_field=SplitDataRow.split_date,
                value_field=SplitDataRow.denominator,
            ),
        }

        assert result['s'].dated_factor_columns == expected

    def test_extended_main_row_entity(self):
        result = ColumnBuilder._calculate_column_extractors([data_blocks.ExtendedMarketDailyDataBlock])

        assert result['m'].subentity_field_name is None

    def test_extended_subentity(self):
        result = ColumnBuilder._calculate_column_extractors([data_blocks.ExtendedFundamentalsDataBlock])

        assert result['fbs'].subentity_field_name == 'balance_sheet'

    def test_extended_dated_factor_entity(self):
        result = ColumnBuilder._calculate_column_extractors([data_blocks.ExtendedDividendsDataBlock])
        expected = frozenset({
            'declaration_date_dividend',
            'declaration_date_dividend_split_adjusted',
            'ex_dividend_date_dividend',
            'ex_dividend_date_dividend_split_adjusted',
            'record_date_dividend',
            'record_date_dividend_split_adjusted',
            'payment_date_dividend',
            'payment_date_dividend_split_adjusted',
        })

        assert result['d'].valid_column_names == expected


class TestPrivateCalculateFieldTimestamps:
    def test_field_values(self, example_entity_rows_dividend_data):
        result = ColumnBuilder._calculate_field_timestamps(
            'payment_date',
            example_entity_rows_dividend_data,
        )
        expected = pyarrow.array([
            datetime.date(2020, 1, 4),
            datetime.date(2020, 2, 1),
        ])

        assert result.equals(expected)

    def test_rows_without_field_value(self, example_entity_rows_dividend_data):
        result = ColumnBuilder._calculate_field_timestamps(
            'record_date',
            example_entity_rows_dividend_data,
        )
        expected = pyarrow.array([
            datetime.date(2020, 1, 4),
            None,
        ])

        assert result.equals(expected)

    def test_rows_without_data(self):
        result = ColumnBuilder._calculate_field_timestamps(
            'payment_date',
            {
                '2020-01-03': None,
            },
        )
        expected = pyarrow.array([
            None,
        ])

        assert result.equals(expected)


class TestPrivateCalculateKeyTimestamps:
    def test_date_keys(self, example_entity_rows_market_data):
        result = ColumnBuilder._calculate_key_timestamps(
            MarketDailyDataBlock,
            example_entity_rows_market_data,
        )
        expected = pyarrow.array(
            [
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 3),
                datetime.date(2020, 1, 4),
            ],
            type=pyarrow.date32(),
        )

        assert result.equals(expected)

    def test_no_rows(self):
        result = ColumnBuilder._calculate_key_timestamps(
            MarketDailyDataBlock,
            {},
        )

        assert result.type == pyarrow.date32()

    def test_keys_that_are_not_dates(self):
        with pytest.raises(ColumnBuilderClockError):
            ColumnBuilder._calculate_key_timestamps(
                MarketDailyDataBlock,
                {
                    '2020-13-01': None,
                },
            )


class TestPrivateGenerateColumn:
    def test_rows_without_data(self, example_master_clock_column):
        result = ColumnBuilder._generate_column(
            (
                None,
                None,
                None,
            ),
            'test_field',
            'test_subfield',
            clock_column=example_master_clock_column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        expected_result = DataColumn.load([
            None,
            None,
            None,
        ])

        assert DataColumn.fully_equal(
            expected_result,
            result,
            equal_nulls=True
        )

    def test_full_rows(self, example_entities_with_subentities):
        clock_column = DataColumn(
            pyarrow.array([
                datetime.date(2020, 1, 1),
                datetime.date(2020, 2, 1),
            ]),
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )
        result = ColumnBuilder._generate_column(
            tuple(example_entities_with_subentities.values()),
            'field_subentity',
            'subfield_int',
            clock_column=clock_column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )
        expected_result = DataColumn.load([
            11,
            21,
        ])

        assert DataColumn.fully_equal(
            expected_result,
            result,
            equal_nulls=True
        )

    def test_clock_tracking_rows(self, example_master_clock_column):
        result = ColumnBuilder._generate_column(
            (
                None,
                None,
                None,
            ),
            'test_field',
            clock_column=example_master_clock_column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.last,
        )

        assert result.reference_clock_column is example_master_clock_column

    def test_consolidation_method(self, example_master_clock_column):
        result = ColumnBuilder._generate_column(
            (
                None,
                None,
                None,
            ),
            'test_field',
            clock_column=example_master_clock_column,
            consolidation_method=clock_sync_methods.ConsolidationMethods.sum,
        )

        assert result.consolidation_method is clock_sync_methods.ConsolidationMethods.sum


class TestPrivateGetCalculationFunction:
    def test_existing(self):
        result = ColumnBuilder._get_calculation_function(
            'example_sum_function_1',
            [entities]
        )
        expected_result = entities.example_sum_function_1

        assert result is expected_result

    def test_missing(self):
        with pytest.raises(ColumnBuilderCustomFunctionNotFoundError):
            ColumnBuilder._get_calculation_function(
                'example_nonexistent_function',
                [entities]
            )


class TestPrivateGetCalculationModule:
    def test_module_defining_function(self):
        result = ColumnBuilder._get_calculation_module(
            'example_sum_function_1',
            [
                calculations,
                entities,
            ]
        )

        assert result is entities

    def test_missing(self):
        with pytest.raises(ColumnBuilderCustomFunctionNotFoundError):
            ColumnBuilder._get_calculation_module(
                'example_nonexistent_function',
                [entities]
            )


class TestPrivateGetDatedFactorColumns:
    def test_date_and_value_field_combinations(self):
        result = ColumnBuilder._get_dated_factor_columns(
            (
                SplitDataRow.split_date,
            ),
            (
                SplitDataRow.numerator,
                SplitDataRow.denominator,
            ),
        )
        expected = {
            'split_date_numerator': DatedFactorColumn(
                date_field=SplitDataRow.split_date,
                value_field=SplitDataRow.numerator,
            ),
            'split_date_denominator': DatedFactorColumn(
                date_field=SplitDataRow.split_date,
                value_field=SplitDataRow.denominator,
            ),
        }

        assert result == expected


class TestPrivateGetEntityClockSpec:
    def test_clock_spec_held_by_clock_spec_field(self):
        result = ColumnBuilder._get_entity_clock_spec(
            data_block_entities.MARKET_DATA,
            MarketDailyDataBlock.clock_spec_field,
        )

        assert result is data_block_entities.MARKET_DATA.clock_spec


class TestPrivateGetEntityFieldNames:
    def test_existing_field(self):
        assert (
            'field_str'
            in ColumnBuilder._get_entity_field_names(
                entities.ExampleEntity
            )
        )

    def test_missing_field(self):
        assert (
            'nonexistent_field'
            not in ColumnBuilder._get_entity_field_names(
                entities.ExampleEntity
            )
        )

    def test_method(self):
        assert (
            '__init__'
            not in ColumnBuilder._get_entity_field_names(
                entities.ExampleEntity
            )
        )


class TestPrivateGetFieldFromRow:
    def test_empty_row(self):
        result = ColumnBuilder._get_field_from_row(
            None,
            'whatever'
        )

        assert result is None

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
    def test_callable(self):
        result = ColumnBuilder._get_function_params(entities.example_sum_function_1)
        expected_result = [
            'a',
            'b',
        ]

        assert result == expected_result

    def test_not_callable(self):
        with pytest.raises(TypeError):
            ColumnBuilder._get_function_params(42)  # type: ignore


class TestPrivateGetMasterClockColumn:
    def test_built_in_data_block(self):
        result = ColumnBuilder._get_master_clock_column(MarketDailyDataBlock)

        assert result == 'm_date'

    def test_extended_main_row_entity(self):
        result = ColumnBuilder._get_master_clock_column(data_blocks.ExtendedMarketDailyDataBlock)

        assert result == 'm_date'

    def test_clock_sync_entity_after_other_prefixes(self):
        class ReorderedFundamentalsDataBlock(FundamentalsDataBlock):
            prefix_entity_map: typing.ClassVar = {
                'fbs': FundamentalDataRowBalanceSheet,
                'fcf': FundamentalDataRowCashFlow,
                'fis': FundamentalDataRowIncomeStatement,
                'f': FundamentalDataRow,
            }

        result = ColumnBuilder._get_master_clock_column(ReorderedFundamentalsDataBlock)

        assert result == 'f_filing_date'

    def test_clock_sync_entity_without_prefix(self):
        # The rows still hold the clock sync field's entity, but only their balance sheets get a prefix
        class BalanceSheetFundamentalsDataBlock(FundamentalsDataBlock):
            downscale_consolidation_methods: typing.ClassVar = {
                FundamentalDataRowBalanceSheet: clock_sync_methods.ConsolidationMethods.last,
            }
            prefix_entity_map: typing.ClassVar = {
                'fbs': FundamentalDataRowBalanceSheet,
            }

        with pytest.raises(InjectedDependencyError):
            ColumnBuilder._get_master_clock_column(BalanceSheetFundamentalsDataBlock)


class TestPrivateGetSubentityFieldName:
    def test_subentity(self):
        result = ColumnBuilder._get_subentity_field_name(
            FundamentalDataRow,
            FundamentalDataRowBalanceSheet,
        )

        assert result == 'balance_sheet'

    def test_extended_subentity(self):
        # The field is typed with the entity the subentity extends
        result = ColumnBuilder._get_subentity_field_name(
            FundamentalDataRow,
            data_blocks.ExtendedBalanceSheet,
        )

        assert result == 'balance_sheet'

    def test_entity_without_field_holding_subentity(self):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._get_subentity_field_name(
                FundamentalDataRow,
                MarketDataDailyRow,
            )


class TestPrivateGetEntityRows:
    def test_rows_held_by_rows_field(self):
        result = ColumnBuilder._get_entity_rows(
            data_block_entities.MARKET_DATA,
            MarketDailyDataBlock.rows_field,
        )

        assert result is data_block_entities.MARKET_DATA.rows


class TestPrivateProcessColumn:
    def test_c_column_with_base_params(
        self,
        example_entity_rows_market_data,
        example_master_clock_column,
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
        result = ColumnBuilder._process_column(
            'c_sum_mhigh_and_mlow',
            completed_columns,
            calculated_columns=ColumnBuilder._calculate_calculated_columns(
                ['c_sum_mhigh_and_mlow'],
                [calculations],
            ),
            clocked_rows_by_block={},
            column_extractors={},
            master_clock_column=example_master_clock_column,
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
            result,
            expected_column,
            equal_nulls=True
        )

    def test_c_column_with_calculated_param(
        self,
        example_entity_rows_market_data,
        example_master_clock_column,
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
        result = ColumnBuilder._process_column(
            'c_multiply_sum_by_mclose',
            completed_columns,
            calculated_columns=ColumnBuilder._calculate_calculated_columns(
                ['c_multiply_sum_by_mclose'],
                [calculations],
            ),
            clocked_rows_by_block={},
            column_extractors={},
            master_clock_column=example_master_clock_column,
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
            result,
            expected_column,
            equal_nulls=True
        )

    def test_c_column_raising_error(self, example_master_clock_column):
        with pytest.raises(ColumnBuilderCalculationError):
            ColumnBuilder._process_column(
                'c_failing_calculation',
                {
                    'm_close': DataColumn.load([
                        1.0,
                        2.0,
                        3.0,
                    ]),
                },
                calculated_columns=ColumnBuilder._calculate_calculated_columns(
                    ['c_failing_calculation'],
                    [calculations],
                ),
                clocked_rows_by_block={},
                column_extractors={},
                master_clock_column=example_master_clock_column,
            )

    def test_d_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_dividend_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'd_ex_dividend_date_dividend',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_dividend_data['2020-01-03'].dividend,
            example_entity_rows_dividend_data['2020-01-04'].dividend,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_d_column_of_another_date_field(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_dividend_data,
        example_master_clock_column,
    ):
        # Only the first dividend gets paid on a master clock date
        result = ColumnBuilder._process_column(
            'd_payment_date_dividend',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            None,
            example_entity_rows_dividend_data['2020-01-03'].dividend,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_d_column_dated_before_master_clock(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_dividend_data,
        example_master_clock_column,
    ):
        # The first dividend gets declared before the master clock starts, so it doesn't belong to any of its rows
        result = ColumnBuilder._process_column(
            'd_declaration_date_dividend',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            example_entity_rows_dividend_data['2020-01-04'].dividend,
            None,
            None,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_d_column_dated_between_master_clock_dates(
        self,
        example_clocked_rows_of_dividends_sharing_dates,
        example_master_clock_column,
    ):
        # Both dividends get declared on 2020-01-02, so they take effect from the next master clock date
        result = ColumnBuilder._process_column(
            'd_declaration_date_dividend',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_of_dividends_sharing_dates,
            column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            decimal.Decimal('1.33'),
            None,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_d_columns_sharing_master_clock_date(
        self,
        example_clocked_rows_of_dividends_sharing_dates,
        example_master_clock_column,
    ):
        # Both dividends get paid on 2020-01-04, so they get summed
        result = ColumnBuilder._process_column(
            'd_payment_date_dividend',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_of_dividends_sharing_dates,
            column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            None,
            decimal.Decimal('1.33'),
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_d_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                'd_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_column_of_data_block_without_entity(
        self,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'd_ex_dividend_date_dividend',
            {},
            calculated_columns={},
            clocked_rows_by_block={},
            column_extractors=ColumnBuilder._calculate_column_extractors([DividendsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            None,
            None,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_f_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_fundamental_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'f_fiscal_year',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].fiscal_year,
            example_entity_rows_fundamental_data['2020-01-04'].fiscal_year,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_f_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                'f_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_fbs_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_fundamental_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'fbs_total_liabilities_and_equity',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].balance_sheet.total_liabilities_and_equity,
            example_entity_rows_fundamental_data['2020-01-04'].balance_sheet.total_liabilities_and_equity,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_fbs_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                'fbs_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_fcf_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_fundamental_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'fcf_net_income',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].cash_flow.net_income,
            example_entity_rows_fundamental_data['2020-01-04'].cash_flow.net_income,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_fcf_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                'fcf_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_fis_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_fundamental_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'fis_gross_profit',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_fundamental_data['2020-01-03'].income_statement.gross_profit,
            example_entity_rows_fundamental_data['2020-01-04'].income_statement.gross_profit,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_fis_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                'fis_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([FundamentalsDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_m_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_market_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            'm_close',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([MarketDailyDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            example_entity_rows_market_data['2020-01-01'].close,
            example_entity_rows_market_data['2020-01-03'].close,
            example_entity_rows_market_data['2020-01-04'].close,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_m_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                'm_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([MarketDailyDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_s_column_existent_property(
        self,
        example_clocked_rows_by_block,
        example_entity_rows_split_data,
        example_master_clock_column,
    ):
        result = ColumnBuilder._process_column(
            's_split_date_numerator',
            {},
            calculated_columns={},
            clocked_rows_by_block=example_clocked_rows_by_block,
            column_extractors=ColumnBuilder._calculate_column_extractors([SplitsDataBlock]),
            master_clock_column=example_master_clock_column,
        )
        expected_column = DataColumn.load([
            None,
            example_entity_rows_split_data['2020-01-03'].numerator,
            example_entity_rows_split_data['2020-01-04'].numerator,
        ])

        assert DataColumn.fully_equal(
            result,
            expected_column,
            equal_nulls=True
        )

    def test_s_column_nonexistent_property(
        self,
        example_clocked_rows_by_block,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                's_non_existent_column',
                {},
                calculated_columns={},
                clocked_rows_by_block=example_clocked_rows_by_block,
                column_extractors=ColumnBuilder._calculate_column_extractors([SplitsDataBlock]),
                master_clock_column=example_master_clock_column,
            )

    def test_unknown_prefix(self, example_master_clock_column):
        with pytest.raises(ColumnBuilderUnavailableEntityFieldError):
            ColumnBuilder._process_column(
                '7_non_existent_prefix',
                {},
                calculated_columns={},
                clocked_rows_by_block={},
                column_extractors={},
                master_clock_column=example_master_clock_column,
            )


class TestPrivateValidateDataBlockEntities:
    def test_entities_of_their_data_blocks(self):
        result = ColumnBuilder._validate_data_block_entities(
            {
                MarketDailyDataBlock: data_block_entities.MARKET_DATA,
                data_blocks.AlternativeMarketDailyDataBlock: data_block_entities.ALTERNATIVE_MARKET_DATA,
            },
            [
                MarketDailyDataBlock,
                data_blocks.AlternativeMarketDailyDataBlock,
                DividendsDataBlock,
            ],
        )

        assert result is None

    def test_data_block_outside_data_blocks(self):
        with pytest.raises(InjectedDependencyError):
            ColumnBuilder._validate_data_block_entities(
                {
                    MarketDailyDataBlock: data_block_entities.MARKET_DATA,
                    data_blocks.AlternativeMarketDailyDataBlock: data_block_entities.ALTERNATIVE_MARKET_DATA,
                },
                [
                    MarketDailyDataBlock,
                ],
            )

    def test_entity_of_another_main_entity(self):
        with pytest.raises(InjectedDependencyError):
            ColumnBuilder._validate_data_block_entities(
                {
                    DividendsDataBlock: data_block_entities.MARKET_DATA,
                },
                [
                    DividendsDataBlock,
                ],
            )


class TestPrivateValidateReservedPrefixes:
    def test_data_blocks_without_reserved_prefixes(self):
        result = ColumnBuilder._validate_reserved_prefixes(
            [
                MarketDailyDataBlock,
                FundamentalsDataBlock,
            ],
            ('c',),
        )

        assert result is None

    def test_data_block_claiming_reserved_prefix(self):
        class CalculatedPrefixDataBlock(MarketDailyDataBlock):
            prefix_entity_map: typing.ClassVar = {
                'c': MarketDataDailyRow,
            }

        with pytest.raises(InjectedDependencyError):
            ColumnBuilder._validate_reserved_prefixes(
                [
                    MarketDailyDataBlock,
                    CalculatedPrefixDataBlock,
                ],
                ('c',),
            )


class TestPrivateWrapCalculationOutput:
    def test_column_synced_to_master_clock(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        calculation_output = DataColumn.load(
            [
                1,
                2,
                3,
            ],
            reference_clock_column=example_master_clock_column,
        )
        result = ColumnBuilder._wrap_calculation_output(
            calculation_output,
            calculated_column=example_calculated_column,
            column='c_sum_mhigh_and_mlow',
            master_clock_column=example_master_clock_column,
        )

        assert result is calculation_output

    def test_column_without_clock(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        result = ColumnBuilder._wrap_calculation_output(
            DataColumn.load([
                1,
                2,
                3,
            ]),
            calculated_column=example_calculated_column,
            column='c_sum_mhigh_and_mlow',
            master_clock_column=example_master_clock_column,
        )

        assert result.reference_clock_column is example_master_clock_column

    def test_list(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        result = ColumnBuilder._wrap_calculation_output(
            [
                1,
                2,
                3,
            ],
            calculated_column=example_calculated_column,
            column='c_sum_mhigh_and_mlow',
            master_clock_column=example_master_clock_column,
        )
        expected = [
            1,
            2,
            3,
        ]

        assert result.array.to_pylist() == expected

    def test_output_with_other_length_than_master_clock(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderCalculationError):
            ColumnBuilder._wrap_calculation_output(
                [
                    1,
                    2,
                ],
                calculated_column=example_calculated_column,
                column='c_sum_mhigh_and_mlow',
                master_clock_column=example_master_clock_column,
            )

    def test_column_synced_to_another_master_clock(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        other_master_clock_column = DataColumn(
            pyarrow.array([
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 3),
                datetime.date(2020, 1, 4),
            ]),
            clock_master=True,
            clock_periods_per_row=1,
            clock_reference_period=ReferencePeriods.DAY,
        )

        with pytest.raises(ColumnBuilderCalculationError):
            ColumnBuilder._wrap_calculation_output(
                DataColumn.load(
                    [
                        1,
                        2,
                        3,
                    ],
                    reference_clock_column=other_master_clock_column,
                ),
                calculated_column=example_calculated_column,
                column='c_sum_mhigh_and_mlow',
                master_clock_column=example_master_clock_column,
            )

    def test_output_not_iterable(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderCalculationError):
            ColumnBuilder._wrap_calculation_output(
                42,
                calculated_column=example_calculated_column,
                column='c_sum_mhigh_and_mlow',
                master_clock_column=example_master_clock_column,
            )

    def test_output_unloadable_into_column(
        self,
        example_calculated_column,
        example_master_clock_column,
    ):
        with pytest.raises(ColumnBuilderCalculationError):
            ColumnBuilder._wrap_calculation_output(
                [
                    object(),
                    object(),
                    object(),
                ],
                calculated_column=example_calculated_column,
                column='c_sum_mhigh_and_mlow',
                master_clock_column=example_master_clock_column,
            )
