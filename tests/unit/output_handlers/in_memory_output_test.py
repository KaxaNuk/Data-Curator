import datetime

import pyarrow
import pytest

from kaxanuk.data_curator.exceptions import (
    OutputHandlerError,
    PassedArgumentError,
)
from kaxanuk.data_curator.output_handlers.in_memory_output import InMemoryOutput


@pytest.fixture
def intraday_in_memory_output():
    # The output of two identifiers, indexed by an intraday master clock column instead of m_date
    in_memory_output = InMemoryOutput()
    in_memory_output.output_data(
        main_identifier='AAPL',
        columns=pyarrow.table({
            'mi_timestamp': [
                datetime.datetime(2024, 1, 30, 9, 30),
                datetime.datetime(2024, 1, 30, 10, 30),
            ],
            'mi_close': [
                10.5,
                11.0,
            ],
        }),
    )
    in_memory_output.output_data(
        main_identifier='MSFT',
        columns=pyarrow.table({
            'mi_timestamp': [
                datetime.datetime(2024, 1, 30, 9, 30),
            ],
            'mi_close': [
                20.5,
            ],
        }),
    )

    return in_memory_output


class TestExportDataframe:
    def test_index_names(
        self,
        intraday_in_memory_output,
    ):
        result = intraday_in_memory_output.export_dataframe('mi_timestamp')

        assert result.index.names == [
            'main_identifier',
            'mi_timestamp',
        ]

    def test_index_values(
        self,
        intraday_in_memory_output,
    ):
        result = intraday_in_memory_output.export_dataframe('mi_timestamp')
        expected = [
            ('AAPL', datetime.datetime(2024, 1, 30, 9, 30)),
            ('AAPL', datetime.datetime(2024, 1, 30, 10, 30)),
            ('MSFT', datetime.datetime(2024, 1, 30, 9, 30)),
        ]

        assert result.index.tolist() == expected

    def test_column_values(
        self,
        intraday_in_memory_output,
    ):
        result = intraday_in_memory_output.export_dataframe('mi_timestamp')

        assert result['mi_close'].tolist() == [
            10.5,
            11.0,
            20.5,
        ]

    def test_missing_date_index_column(
        self,
        intraday_in_memory_output,
    ):
        with pytest.raises(PassedArgumentError):
            intraday_in_memory_output.export_dataframe('m_date')

    def test_without_data(self):
        with pytest.raises(OutputHandlerError):
            InMemoryOutput().export_dataframe('m_date')
