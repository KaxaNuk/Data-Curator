import datetime

import pyarrow

from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from .fixtures import (
    irregular_filings
)


class TestFindConsolidatedTableIrregularFilingRows:
    def test_find_consolidated_table_irregular_filing_rows(self):
        table = irregular_filings.IRREGULAR_FILINGS_TABLE
        result = FundamentalsDataBlock._find_irregular_filing_rows(
            consolidated_table=table,
            filing_date_column_name='filing_date',
            period_end_date_column_name='period_end_date',
        )
        expected = irregular_filings.IRREGULAR_FILINGS_MASK

        assert result.equals(expected)

    def test_find_consolidated_table_irregular_filing_rows_with_none(self):
        table = irregular_filings.IRREGULAR_FILINGS_WITH_NONE_TABLE
        result = FundamentalsDataBlock._find_irregular_filing_rows(
            consolidated_table=table,
            filing_date_column_name='filing_date',
            period_end_date_column_name='period_end_date',
        )
        expected = irregular_filings.IRREGULAR_FILINGS_WITH_NONE_MASK

        assert result.equals(expected)


class TestPrivateCalculateArrayPosteriorDuplicatesMask:
    def test_calculate_array_posterior_duplicates_mask(self):
        dates_with_dupes = pyarrow.array([
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 2),
            datetime.date(2023, 1, 1),  # duplicate
            datetime.date(2023, 1, 3),
            datetime.date(2023, 1, 2),  # duplicate
        ])
        result = FundamentalsDataBlock._calculate_array_posterior_duplicates_mask(dates_with_dupes)
        expected = pyarrow.array([False, False, True, False, True])

        assert result.equals(expected)

    def test_calculate_array_posterior_duplicates_mask_no_duplicates(self):
        dates_no_dupes = pyarrow.array([
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 2),
            datetime.date(2023, 1, 3),
        ])
        result = FundamentalsDataBlock._calculate_array_posterior_duplicates_mask(dates_no_dupes)
        expected = pyarrow.array([False, False, False])

        assert result.equals(expected)
