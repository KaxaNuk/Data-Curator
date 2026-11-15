import datetime

import pyarrow

from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from .fixtures import (
    irregular_filings
)


class TestFindConsolidatedTableRowSpanMonths:
    def test_quarterly_rows(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2024, 3, 31),
                datetime.date(2024, 6, 30),
                datetime.date(2024, 9, 30),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_row_span_months(table)
        expected = 3

        assert result == expected

    def test_half_yearly_rows(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2023, 12, 31),
                datetime.date(2024, 6, 30),
                datetime.date(2024, 12, 31),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_row_span_months(table)
        expected = 6

        assert result == expected

    def test_half_yearly_rows_in_descending_order(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2024, 12, 31),
                datetime.date(2024, 6, 30),
                datetime.date(2023, 12, 31),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_row_span_months(table)
        expected = 6

        assert result == expected

    def test_single_row(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2024, 6, 30),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_row_span_months(table)

        assert result is None


class TestFindConsolidatedTableSupersededFrequencyRows:
    def test_single_reporting_frequency(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2024, 3, 31),
                datetime.date(2024, 6, 30),
                datetime.date(2024, 9, 30),
                datetime.date(2024, 12, 31),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_superseded_frequency_rows(table)

        assert result is None

    def test_half_yearly_reporting_changed_to_quarterly(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2022, 6, 30),
                datetime.date(2022, 12, 31),
                datetime.date(2023, 6, 30),
                datetime.date(2023, 9, 30),
                datetime.date(2023, 12, 31),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_superseded_frequency_rows(table)
        expected = pyarrow.array([
            True,
            True,
            True,
            False,
            False,
        ])

        assert result.equals(expected)

    def test_rows_in_descending_order(self):
        table = pyarrow.table({
            'FundamentalDataRow.period_end_date': [
                datetime.date(2023, 12, 31),
                datetime.date(2023, 9, 30),
                datetime.date(2023, 6, 30),
                datetime.date(2022, 12, 31),
                datetime.date(2022, 6, 30),
            ],
        })
        result = FundamentalsDataBlock.find_consolidated_table_superseded_frequency_rows(table)
        expected = pyarrow.array([
            False,
            False,
            True,
            True,
            True,
        ])

        assert result.equals(expected)


class TestPrivateFindIrregularFilingRows:
    def test_irregular_filings(self):
        table = irregular_filings.IRREGULAR_FILINGS_TABLE
        result = FundamentalsDataBlock._find_irregular_filing_rows(
            consolidated_table=table,
            filing_date_column_name='filing_date',
            period_end_date_column_name='period_end_date',
        )
        expected = irregular_filings.IRREGULAR_FILINGS_MASK

        assert result.equals(expected)

    def test_irregular_filings_with_none(self):
        table = irregular_filings.IRREGULAR_FILINGS_WITH_NONE_TABLE
        result = FundamentalsDataBlock._find_irregular_filing_rows(
            consolidated_table=table,
            filing_date_column_name='filing_date',
            period_end_date_column_name='period_end_date',
        )
        expected = irregular_filings.IRREGULAR_FILINGS_WITH_NONE_MASK

        assert result.equals(expected)


class TestPrivateCalculateArrayPosteriorDuplicatesMask:
    def test_with_duplicates(self):
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

    def test_no_duplicates(self):
        dates_no_dupes = pyarrow.array([
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 2),
            datetime.date(2023, 1, 3),
        ])
        result = FundamentalsDataBlock._calculate_array_posterior_duplicates_mask(dates_no_dupes)
        expected = pyarrow.array([False, False, False])

        assert result.equals(expected)


class TestPrivateCalculateRowSpansMonths:
    def test_quarterly_dates(self):
        result = FundamentalsDataBlock._calculate_row_spans_months([
            datetime.date(2024, 3, 31),
            datetime.date(2024, 6, 30),
            datetime.date(2024, 9, 30),
        ])
        expected = [None, 3, 3]

        assert result == expected

    def test_half_yearly_dates(self):
        result = FundamentalsDataBlock._calculate_row_spans_months([
            datetime.date(2023, 12, 31),
            datetime.date(2024, 6, 30),
            datetime.date(2024, 12, 31),
        ])
        expected = [None, 6, 6]

        assert result == expected

    def test_dates_deviating_within_tolerance(self):
        # half-yearly gaps of 5 and 7 months, from period end dates on 31 January
        result = FundamentalsDataBlock._calculate_row_spans_months([
            datetime.date(2007, 1, 31),
            datetime.date(2007, 6, 30),
            datetime.date(2008, 1, 31),
        ])
        expected = [None, 6, 6]

        assert result == expected

    def test_gap_matching_neither_span(self):
        result = FundamentalsDataBlock._calculate_row_spans_months([
            datetime.date(2023, 12, 31),
            datetime.date(2024, 9, 30),
        ])
        expected = [None, None]

        assert result == expected

    def test_none_date(self):
        result = FundamentalsDataBlock._calculate_row_spans_months([
            datetime.date(2024, 3, 31),
            datetime.date(2024, 6, 30),
            None,
        ])
        expected = [None, 3, None]

        assert result == expected

    def test_no_dates(self):
        result = FundamentalsDataBlock._calculate_row_spans_months([])
        expected = []

        assert result == expected


class TestPrivateFindLatestFrequencyStart:
    def test_single_reporting_frequency(self):
        result = FundamentalsDataBlock._find_latest_frequency_start([None, 3, 3, 3])
        expected = 0

        assert result == expected

    def test_missing_quarter(self):
        result = FundamentalsDataBlock._find_latest_frequency_start([None, 3, 6, 3])
        expected = 0

        assert result == expected

    def test_quarterly_after_half_yearly(self):
        result = FundamentalsDataBlock._find_latest_frequency_start([None, 6, 6, 3, 3])
        expected = 3

        assert result == expected

    def test_half_yearly_after_quarterly(self):
        result = FundamentalsDataBlock._find_latest_frequency_start([None, 3, 3, 6, 6])
        expected = 3

        assert result == expected

    def test_no_measured_spans(self):
        result = FundamentalsDataBlock._find_latest_frequency_start([None])
        expected = 0

        assert result == expected


class TestPrivateFindLatestRowSpanMonths:
    def test_quarterly_spans(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, 3, 3])
        expected = 3

        assert result == expected

    def test_half_yearly_spans(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, 6, 6])
        expected = 6

        assert result == expected

    def test_single_half_yearly_span(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, 6])
        expected = 6

        assert result == expected

    def test_missing_latest_quarter(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, 3, 3, 6])
        expected = 3

        assert result == expected

    def test_half_yearly_after_quarterly(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, 3, 3, 6, 6])
        expected = 6

        assert result == expected

    def test_unmeasured_span_between_half_yearly_ones(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, 6, None, 6])
        expected = 6

        assert result == expected

    def test_no_measured_spans(self):
        result = FundamentalsDataBlock._find_latest_row_span_months([None, None])

        assert result is None
