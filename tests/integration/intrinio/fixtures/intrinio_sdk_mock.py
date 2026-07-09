import datetime
import pathlib
import pickle
import types

import intrinio_sdk
import intrinio_sdk.rest


_STOCK_PRICES_FIXTURE_PATH = pathlib.Path(__file__).parent / 'stock_prices.pkl'
with _STOCK_PRICES_FIXTURE_PATH.open('rb') as _stock_prices_fixture_file:
    _STOCK_PRICE_ROWS = pickle.load(_stock_prices_fixture_file)


_STANDARDIZED_FINANCIALS_FIXTURE_PATHS = (
    pathlib.Path(__file__).parent / 'standardized_FY.pkl',
    pathlib.Path(__file__).parent / 'standardized_QTR.pkl',
)
_STANDARDIZED_FINANCIALS_RECORDS = {}
for _standardized_financials_fixture_path in _STANDARDIZED_FINANCIALS_FIXTURE_PATHS:
    with _standardized_financials_fixture_path.open('rb') as _standardized_financials_fixture_file:
        _STANDARDIZED_FINANCIALS_RECORDS.update(
            pickle.load(_standardized_financials_fixture_file)
        )


_COMPANY_FUNDAMENTALS_FIXTURE_PATHS = {
    'FY': pathlib.Path(__file__).parent / 'fundamentals_FY.pkl',
    'QTR': pathlib.Path(__file__).parent / 'fundamentals_QTR.pkl',
}
_COMPANY_FUNDAMENTALS_RECORDS = {}
for _company_fundamentals_type, _company_fundamentals_fixture_path in _COMPANY_FUNDAMENTALS_FIXTURE_PATHS.items():
    with _company_fundamentals_fixture_path.open('rb') as _company_fundamentals_fixture_file:
        _COMPANY_FUNDAMENTALS_RECORDS[_company_fundamentals_type] = pickle.load(
            _company_fundamentals_fixture_file
        )


class AccountApi:
    def get_account_current_usage(
        self
    ) -> intrinio_sdk.models.api_response_account_usages.ApiResponseAccountUsages:
        account = intrinio_sdk.models.api_response_account_usages_account.ApiResponseAccountUsagesAccount(
            email='kaxanuk@example.com'
        )
        usage = intrinio_sdk.models.account_current_usage.AccountCurrentUsage(
            access_code='api',
            restriction='one_min_call_limit',
            count='1',
            limit='2000',
            seconds_until_reset='10',
            percentage_used='0.05',
        )

        return intrinio_sdk.models.api_response_account_usages.ApiResponseAccountUsages(
            usage=[
                usage,
            ],
            account=account,
        )


class ApiClient:
    def __init__(
        self,
    ):
        self.configuration = types.SimpleNamespace(
            api_key={
                'api_key': None,
            },
        )

    def allow_retries(
        self,
        setting,
    ):
        pass


class CompanyApi:
    def get_company_fundamentals(
        self,
        identifier,
        **kwargs
    ) -> intrinio_sdk.models.api_response_company_fundamentals.ApiResponseCompanyFundamentals:
        fundamentals_type = kwargs.get('type')
        if fundamentals_type not in _COMPANY_FUNDAMENTALS_RECORDS:
            msg = "".join(
                (
                    "Mock only supports the 'FY' and 'QTR' fundamental types, got: ",
                    repr(fundamentals_type),
                )
            )

            raise ValueError(msg)

        record = _COMPANY_FUNDAMENTALS_RECORDS[fundamentals_type]
        fundamentals = [
            intrinio_sdk.models.fundamental_summary.FundamentalSummary(**fundamental)
            for fundamental in record['fundamentals']
        ]
        company = intrinio_sdk.models.company_summary.CompanySummary(
            **record['company']
        )

        return intrinio_sdk.models.api_response_company_fundamentals.ApiResponseCompanyFundamentals(
            fundamentals=fundamentals,
            company=company,
            next_page=None,
        )


class FundamentalsApi:
    def get_fundamental_standardized_financials(
        self,
        financial_id: str,
    ) -> intrinio_sdk.models.api_response_standardized_financials.ApiResponseStandardizedFinancials:
        if financial_id not in _STANDARDIZED_FINANCIALS_RECORDS:

            raise intrinio_sdk.rest.ApiException(
                status=404,
                reason='Not Found',
            )

        record = _STANDARDIZED_FINANCIALS_RECORDS[financial_id]
        standardized_financials = [
            intrinio_sdk.models.standardized_financial.StandardizedFinancial(
                data_tag=intrinio_sdk.models.data_tag_summary.DataTagSummary(
                    **standardized_financial['data_tag']
                ),
                value=standardized_financial['value'],
            )
            for standardized_financial in record['standardized_financials']
        ]
        company = intrinio_sdk.models.company_summary.CompanySummary(
            id='com_agj00z',
            ticker='XXXX',
            name='XXXX CORP',
            lei='549300S4KLFTLO7GSQ80',
            cik='0001045810',
        )
        fundamental = intrinio_sdk.models.fundamental.Fundamental(
            id=record['id'],
            statement_code=record['statement_code'],
            fiscal_year=record['fiscal_year'],
            fiscal_period=record['fiscal_period'],
            type=record['type'],
            start_date=record['start_date'],
            end_date=record['end_date'],
            filing_date=record['filing_date'],
            is_latest=record['is_latest'],
            updated_date=record['updated_date'],
            first_calculable=record['first_calculable'],
            earnings_disclosed_at=record['earnings_disclosed_at'],
            standardized_signature=record['standardized_signature'],
            reported_signature=record['reported_signature'],
            company=company,
        )

        return intrinio_sdk.models.api_response_standardized_financials.ApiResponseStandardizedFinancials(
            standardized_financials=standardized_financials,
            fundamental=fundamental,
            next_page=None,
        )

class SecurityApi:
    def get_security_stock_price_adjustments_dividends(
        self,
        identifier,
        **kwargs
    ) -> intrinio_sdk.models.api_response_security_stock_price_adjustments.ApiResponseSecurityStockPriceAdjustments:
        first_adjustment = intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment(
            date=datetime.date(2024, 8, 10),
            dividend=0.24,
            dividend_currency='USD',
            factor=0.999031549148881,
            split_ratio=1.0,
        )
        second_adjustment = intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment(
            date=datetime.date(2023, 8, 11),
            dividend=0.24,
            dividend_currency='USD',
            factor=0.99886636145629,
            split_ratio=1.0,
        )
        third_adjustment = intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment(
            date=datetime.date(2020, 2, 12),
            dividend=0.21,
            dividend_currency='USD',
            factor=0.998690374250743,
            split_ratio=1.0,
        )
        fourth_adjustment = intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment(
            date=datetime.date(2019, 2, 10),
            dividend=0.21,
            dividend_currency='USD',
            factor=0.998901726485964,
            split_ratio=1.0,
        )

        return intrinio_sdk.models.api_response_security_stock_price_adjustments.ApiResponseSecurityStockPriceAdjustments(
            stock_price_adjustments=[
                first_adjustment,
                second_adjustment,
                third_adjustment,
                fourth_adjustment,
            ],
            security=None,
            next_page=None,
        )

    def get_security_stock_price_adjustments_splits(
        self,
        identifier,
        **kwargs
    ) -> intrinio_sdk.models.api_response_security_stock_price_adjustments.ApiResponseSecurityStockPriceAdjustments:
        first_adjustment = intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment(
            date=datetime.date(2022, 4, 14),
            dividend=0.0,
            dividend_currency=None,
            factor=0.333333333333333,
            split_ratio=0.333333333333333,
        )
        second_adjustment = intrinio_sdk.models.stock_price_adjustment.StockPriceAdjustment(
            date=datetime.date(2019, 1, 31),
            dividend=0.0,
            dividend_currency=None,
            factor=0.1,
            split_ratio=0.1,
        )

        return intrinio_sdk.models.api_response_security_stock_price_adjustments.ApiResponseSecurityStockPriceAdjustments(
            stock_price_adjustments=[
                first_adjustment,
                second_adjustment,
            ],
            security=None,
            next_page=None,
        )

    def get_security_stock_prices(
        self,
        identifier,
        **kwargs
    ) -> intrinio_sdk.models.api_response_security_stock_prices.ApiResponseSecurityStockPrices:
        frequency = kwargs.get('frequency', 'daily')
        if frequency != 'daily':
            msg = "".join(
                (
                    "Mock only supports the 'daily' frequency, got: ",
                    repr(frequency),
                )
            )

            raise ValueError(msg)

        stock_prices = [
            intrinio_sdk.models.stock_price_summary.StockPriceSummary(**price_row)
            for price_row in _STOCK_PRICE_ROWS
        ]

        return intrinio_sdk.models.api_response_security_stock_prices.ApiResponseSecurityStockPrices(
            stock_prices=stock_prices,
            security=None,
            next_page=None,
        )
