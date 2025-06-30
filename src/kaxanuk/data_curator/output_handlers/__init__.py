"""
Package containing the interface and implementations of the data output handlers.
"""

# make these modules part of the public API of the base namespace
from kaxanuk.data_curator.output_handlers.output_handler_interface import OutputHandlerInterface
from kaxanuk.data_curator.output_handlers.csv_output import CsvOutput
from kaxanuk.data_curator.output_handlers.in_memory_output import InMemoryOutput
from kaxanuk.data_curator.output_handlers.parquet_output import ParquetOutput


__all__ = [
    'CsvOutput',
    'InMemoryOutput',
    'OutputHandlerInterface',
    'ParquetOutput',
]
