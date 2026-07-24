"""
Unit tests for the Intrinio provider's bulk fundamentals CSV parsing and download.

The Intrinio bulk fundamentals downloads are wide CSVs: metadata columns describe
each period and every other column is a standardized line-item tag. The parser
reads one statement file with PyArrow, so the whole-market file is tokenized and
type-converted in native code and only the requested tickers are materialized as
Python objects, then turns those rows into per-ticker `_BulkFundamentalRecord`s
that carry both the summary attributes the selection helpers read and the
statement financials. This keeps the bulk path on the same downstream pipeline as
the live-API path. The statement files are independent, so they are downloaded and
parsed concurrently, with the concurrency kept deliberately low so a modest
machine is never pushed into swapping.
"""

import datetime
import io
import os
import types
import zipfile

import pyarrow.csv

from kaxanuk.data_curator.data_providers.intrinio import (
    Intrinio,
    _BulkFundamentalRecord,
)


# a minimal slice of the real US_FIN/INDU income statement schema (metadata columns + two tags)
_INCOME_CSV = (
    "fundamental_id,company_id,name,cik,ticker,start_date,end_date,months,"
    "fiscal_year,fiscal_period,filing_date,updated_date,totalrevenue,totalcostofrevenue,"
    "fundamental_type,pdf_mapping_confidence\n"
    "fun_1,com_1,XXXX CORP,,XXXX,2023-01-01,2023-03-31,3,2023,Q1,"
    "2023-05-01 00:00:00 +0000,2023-05-01 12:00:00.000000+00:00,1000,600,reported,\n"
    "fun_2,com_1,XXXX CORP,,XXXX,2023-01-01,2023-12-31,12,2023,FY,"
    "2024-02-01 00:00:00 +0000,2024-02-01 12:00:00.000000+00:00,4200,2500,reported,\n"
    "fun_3,com_2,YYYY CORP,,YYYY,2023-01-01,2023-03-31,3,2023,Q1,"
    "2023-05-02 00:00:00 +0000,2023-05-02 12:00:00.000000+00:00,500,300,reported,\n"
)


def _source(text: str) -> io.BytesIO:
    return io.BytesIO(text.encode())


class TestParseBulkFundamentalStatements:
    def test_groups_records_by_ticker(self) -> None:
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(_INCOME_CSV),
            statement_code='income_statement',
        )

        assert set(result) == {'XXXX', 'YYYY'}
        assert len(result['XXXX']) == 2
        assert len(result['YYYY']) == 1

    def test_parses_metadata_into_record_fields(self) -> None:
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(_INCOME_CSV),
            statement_code='income_statement',
        )
        quarter = next(record for record in result['XXXX'] if record.fiscal_period == 'Q1')

        assert quarter.statement_code == 'income_statement'
        assert quarter.type == 'reported'
        assert quarter.fiscal_year == 2023
        assert quarter.filing_date == datetime.date(2023, 5, 1)
        assert quarter.updated_date == datetime.datetime(2023, 5, 1)
        assert quarter.period_end_date == datetime.date(2023, 3, 31)
        assert quarter.reported_currency == 'USD'

    def test_collects_only_tag_columns_into_values(self) -> None:
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(_INCOME_CSV),
            statement_code='income_statement',
        )
        quarter = next(record for record in result['XXXX'] if record.fiscal_period == 'Q1')

        assert quarter.values == {'totalrevenue': 1000.0, 'totalcostofrevenue': 600.0}
        # metadata columns must never leak into the tag values
        assert 'ticker' not in quarter.values
        assert 'fiscal_year' not in quarter.values
        assert 'fundamental_type' not in quarter.values

    def test_coerces_integer_tag_cells_to_float(self) -> None:
        # PyArrow infers whole-number tag columns as integers; the record's values must still be floats
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(_INCOME_CSV),
            statement_code='income_statement',
        )
        quarter = next(record for record in result['XXXX'] if record.fiscal_period == 'Q1')

        assert isinstance(quarter.values['totalrevenue'], float)

    def test_skips_rows_without_a_ticker(self) -> None:
        tickerless_row = (
            "fun_4,com_3,ZZZ CORP,,,2023-01-01,2023-03-31,3,2023,Q1,"
            "2023-05-01 00:00:00 +0000,2023-05-01 12:00:00.000000+00:00,10,5,reported,\n"
        )
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(_INCOME_CSV + tickerless_row),
            statement_code='income_statement',
        )

        assert '' not in result
        assert set(result) == {'XXXX', 'YYYY'}

    def test_materializes_only_the_requested_tickers(self) -> None:
        # the whole-market file holds every US company; a universe filter keeps only the wanted rows
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(_INCOME_CSV),
            statement_code='income_statement',
            tickers=frozenset({'XXXX'}),
        )

        assert set(result) == {'XXXX'}

    def test_omits_empty_and_non_numeric_tag_cells_from_values(self) -> None:
        # a blank or non-numeric tag cell must not land in the record's values (matching the API path)
        csv_text = (
            "fundamental_id,ticker,fiscal_year,fiscal_period,filing_date,updated_date,end_date,"
            "totalrevenue,totalcostofrevenue,totaloperatingexpenses,fundamental_type\n"
            "fun_1,XXXX,2023,Q1,2023-05-01 00:00:00 +0000,2023-05-01,2023-03-31,"
            "1000,,not_a_number,reported\n"
        )
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(csv_text),
            statement_code='income_statement',
        )
        quarter = result['XXXX'][0]

        assert quarter.values == {'totalrevenue': 1000.0}

    def test_tolerates_a_newline_inside_a_quoted_field(self) -> None:
        # a quoted embedded newline (odd but legal CSV the old reader tolerated) must not split the
        # row and abort the whole file's parse, which would drop the entire bulk to the API fallback
        csv_text = (
            'fundamental_id,name,ticker,fiscal_year,fiscal_period,filing_date,updated_date,end_date,'
            'totalrevenue,fundamental_type\n'
            'fun_a,"FOO\nBAR CORP",XXXX,2023,Q1,2023-05-01 00:00:00 +0000,2023-05-01,2023-03-31,'
            '1000,reported\n'
        )
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(csv_text),
            statement_code='income_statement',
        )

        assert set(result) == {'XXXX'}
        assert result['XXXX'][0].values == {'totalrevenue': 1000.0}

    def test_keeps_tickers_that_look_like_null_tokens(self) -> None:
        # real symbols such as NA or NULL must not be dropped as if they were missing values,
        # which is what the old string parse did and what a naive PyArrow null policy would break
        csv_text = (
            "fundamental_id,ticker,fiscal_year,fiscal_period,filing_date,updated_date,end_date,"
            "totalrevenue,fundamental_type\n"
            "fun_a,NA,2023,Q1,2023-05-01 00:00:00 +0000,2023-05-01,2023-03-31,10,reported\n"
            "fun_b,NULL,2023,Q1,2023-05-01 00:00:00 +0000,2023-05-01,2023-03-31,20,reported\n"
        )
        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(csv_text),
            statement_code='income_statement',
            tickers=frozenset({'NA', 'NULL'}),
        )

        assert set(result) == {'NA', 'NULL'}

    def test_reads_a_tag_column_first_blank_then_valued_across_a_block_boundary(self) -> None:
        # PyArrow's streaming reader types each column from its first block only; a tag column left
        # blank throughout that block must not be typed `null`, which then fails on a real value
        # further down the whole-market file (the live failure was
        # "CSV conversion error to null: invalid value '780000000.0'").
        header = (
            "fundamental_id,ticker,fiscal_year,fiscal_period,filing_date,updated_date,end_date,"
            "totalrevenue,sometag,fundamental_type\n"
        )
        blank_row = (
            "fun_a,XXXX,2023,Q1,2023-05-01 00:00:00 +0000,2023-05-01,2023-03-31,1000,,reported\n"
        )
        valued_row = (
            "fun_b,XXXX,2023,Q1,2023-05-01 00:00:00 +0000,2023-05-01,2023-03-31,1000,780000000.0,reported\n"
        )
        block_size = pyarrow.csv.ReadOptions().block_size
        # push the valued row past the first block so the blank column has already been type-inferred
        blank_rows = (block_size // len(blank_row.encode())) + 50
        csv_text = header + blank_row * blank_rows + valued_row

        result = Intrinio._parse_bulk_fundamental_statements(
            source=_source(csv_text),
            statement_code='income_statement',
            tickers=frozenset({'XXXX'}),
        )

        valued_records = [
            record
            for record in result['XXXX']
            if record.values.get('sometag') == 780_000_000.0
        ]
        assert len(valued_records) == 1


class TestDownloadBulkFundamentalsConcurrency:
    def test_unions_each_statement_file_into_the_per_ticker_cache(
        self,
        monkeypatch,
    ) -> None:
        links_response = types.SimpleNamespace(
            bulk_downloads=[
                types.SimpleNamespace(
                    name='US Fundamentals, 10+ years, reported and restated',
                    links=[
                        types.SimpleNamespace(name='US_FIN_INCOME_STATEMENT.zip', url='https://bulk/inc'),
                        types.SimpleNamespace(name='US_FIN_BALANCE_SHEET_STATEMENT.zip', url='https://bulk/bal'),
                    ],
                ),
            ],
        )
        per_file = {
            'https://bulk/inc': {'XXXX': ['inc-x'], 'YYYY': ['inc-y']},
            'https://bulk/bal': {'XXXX': ['bal-x']},
        }
        monkeypatch.setattr(Intrinio, '_request_bulk_download_links', lambda self: links_response)
        monkeypatch.setattr(
            Intrinio,
            '_download_and_parse_bulk_file',
            classmethod(lambda cls, *, url, statement_code, tickers: per_file[url]),
        )

        provider = Intrinio(api_key='12345')
        result = provider._download_bulk_fundamentals(tickers=frozenset({'XXXX', 'YYYY'}))

        # a ticker's records are the union of every statement file that carried it
        assert set(result['XXXX']) == {'inc-x', 'bal-x'}
        assert result['YYYY'] == ['inc-y']

    def test_download_concurrency_stays_low_and_capped_by_cpu(self) -> None:
        # never user-configurable: the safe low default must hold for every machine, gaming PC or not
        concurrency = Intrinio.BULK_DOWNLOAD_CONCURRENCY
        available_cpus = os.cpu_count() or 1
        assert concurrency >= 1
        assert concurrency <= min(2, available_cpus)


class TestDownloadAndParseBulkFile:
    def test_parses_the_data_csv_and_skips_the_key_csv(
        self,
        monkeypatch,
    ) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('US_FIN_INCOME_STATEMENT.csv', _INCOME_CSV)
            archive.writestr('US_FIN_INCOME_STATEMENT_KEY.csv', 'tag,description\nfoo,bar\n')

        monkeypatch.setattr(
            Intrinio,
            '_download_bulk_archive_bytes',
            classmethod(lambda cls, url: buffer.getvalue()),
        )

        result = Intrinio._download_and_parse_bulk_file(
            url='https://bulk/inc',
            statement_code='income_statement',
            tickers=frozenset({'XXXX', 'YYYY'}),
        )

        assert set(result) == {'XXXX', 'YYYY'}
        assert all(isinstance(record, _BulkFundamentalRecord) for record in result['XXXX'])
