"""
Integration tests for the Intrinio provider's bulk fundamentals path.

These exercise the whole bulk route without any API key: fixture CSV rows are
parsed into the shared cache and run through `get_fundamental_data`, and the
download orchestration is tested by mocking only the two network seams. The bulk
path reuses the same selection, merge and remapping pipeline as the live-API
path, so the assertions confirm the same field mappings and point-in-time
derivations come out of it.
"""

import csv
import datetime
import io
import types
import zipfile

import pytest

from kaxanuk.data_curator.data_providers.intrinio import Intrinio
from kaxanuk.data_curator.entities import (
    Configuration,
    FundamentalData,
)


_METADATA_COLUMNS = (
    'fundamental_id',
    'company_id',
    'name',
    'cik',
    'ticker',
    'start_date',
    'end_date',
    'months',
    'fiscal_year',
    'fiscal_period',
    'filing_date',
    'updated_date',
    'fundamental_type',
)


def _bulk_row(
    *,
    fundamental_id: str,
    fundamental_type: str,
    filing_date: str,
    tag_values: dict[str, float],
) -> dict[str, str]:
    """Build one bulk fundamentals CSV row (as a DictReader would) for ticker XXXX, 2023 Q1."""
    metadata = {
        'fundamental_id': fundamental_id,
        'company_id': 'com_1',
        'name': 'XXXX CORP',
        'cik': '',
        'ticker': 'XXXX',
        'start_date': '2023-01-01',
        'end_date': '2023-03-31',
        'months': '3',
        'fiscal_year': '2023',
        'fiscal_period': 'Q1',
        'filing_date': filing_date,
        'updated_date': filing_date[:10],
        'fundamental_type': fundamental_type,
    }

    return {
        **metadata,
        **{tag: str(value) for (tag, value) in tag_values.items()},
    }


# one quarterly period's four statements for ticker XXXX, keyed by statement code
_STATEMENT_ROWS = {
    'income_statement': [
        _bulk_row(
            fundamental_id='fun_inc',
            fundamental_type='reported',
            filing_date='2023-05-01 00:00:00 +0000',
            tag_values={
                'totalrevenue': 1000,
                'totalcostofrevenue': 600,
                'totaloperatingexpenses': 150,
                'netincome': 200,
                'netincometocommon': 190,
                'incometaxexpense': 50,
            },
        ),
    ],
    'balance_sheet_statement': [
        _bulk_row(
            fundamental_id='fun_bal',
            fundamental_type='reported',
            filing_date='2023-05-01 00:00:00 +0000',
            tag_values={
                'cashandequivalents': 300,
                'shortterminvestments': 120,
                'totalassets': 5000,
                'totalcurrentassets': 1500,
                'totalliabilities': 2000,
            },
        ),
    ],
    'cash_flow_statement': [
        _bulk_row(
            fundamental_id='fun_cf',
            fundamental_type='reported',
            filing_date='2023-05-01 00:00:00 +0000',
            tag_values={
                'netcashfromoperatingactivities': 500,
                'purchaseofplantpropertyandequipment': -100,
                'issuanceofdebt': 0,
                'repaymentofdebt': -30,
                'depreciationexpense': 40,
                'increasedecreaseinoperatingcapital': -20,
            },
        ),
    ],
    'calculations': [
        _bulk_row(
            fundamental_id='fun_calc',
            fundamental_type='calculated',
            filing_date='',
            tag_values={
                'adjbasiceps': 1.5,
            },
        ),
    ],
}


def _csv_text(rows: list[dict[str, str]]) -> str:
    """Render bulk statement rows into the wide CSV text a real statement file holds."""
    fieldnames = list(rows[0].keys())
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    return buffer.getvalue()


def _zip_bytes(csv_text: str, member_name: str) -> bytes:
    """Pack CSV text into a bulk-style .zip alongside a `*_KEY.csv` the parser must skip."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr(member_name, csv_text)
        archive.writestr(member_name.replace('.csv', '_KEY.csv'), 'tag,description\n')

    return buffer.getvalue()


def _build_bulk_cache() -> dict:
    """Parse the fixture statement rows into a per-ticker bulk cache."""
    cache: dict = {}
    for (statement_code, rows) in _STATEMENT_ROWS.items():
        parsed = Intrinio._parse_bulk_fundamental_statements(
            source=io.BytesIO(_csv_text(rows).encode()),
            statement_code=statement_code,
        )
        for (ticker, records) in parsed.items():
            cache.setdefault(ticker, []).extend(records)

    return cache


@pytest.fixture(autouse=True)
def _reset_bulk_cache():
    """Keep the class-level bulk cache from leaking between tests."""
    Intrinio._bulk_fundamentals_cache = None
    Intrinio._bulk_cache_universe_key = None
    yield
    Intrinio._bulk_fundamentals_cache = None
    Intrinio._bulk_cache_universe_key = None


class TestGetFundamentalDataFromBulkCache:
    def test_bulk_path_maps_and_derives_like_the_api_path(self) -> None:
        Intrinio._bulk_fundamentals_cache = _build_bulk_cache()
        Intrinio._bulk_cache_universe_key = ('XXXX',)
        provider = Intrinio(api_key='12345')

        result = provider.get_fundamental_data(
            main_identifier='XXXX',
            period='quarterly',
            start_date=datetime.date(2023, 1, 1),
            end_date=datetime.date(2023, 12, 31),
        )

        assert isinstance(result, FundamentalData)
        assert len(result.rows) == 1
        row = next(iter(result.rows.values()))

        assert row.fiscal_year == 2023
        assert row.fiscal_period == 'Q1'
        assert row.reported_currency == 'USD'
        # direct tag mappings
        assert float(row.income_statement.revenues) == 1000
        assert float(row.income_statement.cost_of_revenue) == 600
        assert float(row.income_statement.net_income) == 200
        assert float(row.balance_sheet.assets) == 5000
        assert float(row.cash_flow.net_cash_from_operating_activities) == 500
        assert float(row.cash_flow.depreciation_and_amortization) == 40
        assert float(row.cash_flow.working_capital_change) == -20
        # point-in-time derivations, identical to the API path
        assert float(row.income_statement.costs_and_expenses) == 750          # 600 + 150
        assert float(row.income_statement.net_income_deductions) == 10         # 200 - 190
        assert float(row.balance_sheet.cash_and_shortterm_investments) == 420  # 300 + 120
        assert float(row.cash_flow.free_cash_flow) == 400                      # 500 + (-100)
        assert float(row.cash_flow.net_debt_issuance_proceeds) == -30          # 0 + (-30)


class TestInitializePopulatesBulkCache:
    def test_initialize_downloads_and_caches_the_universe(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # mock the two network seams: link discovery and the per-file download/unzip/read
        file_urls = {
            'income_statement': 'https://bulk.example.com/income',
            'balance_sheet_statement': 'https://bulk.example.com/balance',
            'cash_flow_statement': 'https://bulk.example.com/cash_flow',
            'calculations': 'https://bulk.example.com/calculations',
        }
        file_names = {
            'income_statement': 'US_FIN_INCOME_STATEMENT.zip',
            'balance_sheet_statement': 'US_FIN_BALANCE_SHEET_STATEMENT.zip',
            'cash_flow_statement': 'US_FIN_CASH_FLOW_STATEMENT.zip',
            'calculations': 'US_FIN_CALCULATIONS.zip',
        }
        links_response = types.SimpleNamespace(
            bulk_downloads=[
                types.SimpleNamespace(
                    name='US Fundamentals, 10+ years, reported and restated',
                    links=[
                        types.SimpleNamespace(name=file_names[statement_code], url=url)
                        for (statement_code, url) in file_urls.items()
                    ],
                ),
            ],
        )
        # mock only the raw network read; the real unzip + PyArrow parse + concurrent merge run
        archive_by_url = {
            url: _zip_bytes(
                _csv_text(_STATEMENT_ROWS[statement_code]),
                f'{file_names[statement_code][:-4]}.csv',
            )
            for (statement_code, url) in file_urls.items()
        }
        monkeypatch.setattr(Intrinio, 'BULK_FUNDAMENTALS_MIN_UNIVERSE_SIZE', 1)
        monkeypatch.setattr(Intrinio, '_request_bulk_download_links', lambda self: links_response)
        monkeypatch.setattr(
            Intrinio,
            '_download_bulk_archive_bytes',
            classmethod(lambda cls, url: archive_by_url[url]),
        )

        provider = Intrinio(api_key='12345')
        configuration = Configuration(
            start_date=datetime.date(2023, 1, 1),
            end_date=datetime.date(2023, 12, 31),
            period='quarterly',
            identifiers=('XXXX',),
            columns=('m_close',),
        )
        provider.initialize(configuration=configuration)

        assert Intrinio._bulk_fundamentals_cache is not None
        assert 'XXXX' in Intrinio._bulk_fundamentals_cache
        # all four statements landed for the ticker
        statement_codes = {
            record.statement_code
            for record in Intrinio._bulk_fundamentals_cache['XXXX']
        }
        assert statement_codes == {
            'income_statement',
            'balance_sheet_statement',
            'cash_flow_statement',
            'calculations',
        }
