import logging
import pathlib
import time

import pyarrow.csv
import pytest

from kaxanuk.data_curator import DataColumn


@pytest.fixture
def data_column_debugger():
    return DataColumnDebugger


class DataColumnDebugger:
    @staticmethod
    def dump_data_columns_to_csv(
        **columns: DataColumn
    ) -> None:
        r"""
        Dump the values of the given columns to a CSV file.

        The name of the passed kwarg becomes the header of the respective column.

        Parameters
        ----------
        columns

        Returns
        -------
        None

        Examples
        --------
        >>> DataColumnDebugger.dump_data_columns_to_csv(
        ...     my_header_1=DataColumn.load([1,2,3]),
        ...     my_header_2=DataColumn.load([4,5,6])
        ... )
        Data columns dumped to C:\projects\DataCurator\.debug\dump_data_columns_1245012544.csv
        """
        logging.basicConfig(level=logging.INFO)

        file_prefix = 'dump_data_columns_'
        dump_dir_name = '.debug'
        rootdir = pathlib.Path(__file__).resolve().parent.parent
        current_timestamp = int(time.time())
        full_dump_path = f"{rootdir}/{dump_dir_name}/{file_prefix}{current_timestamp}.csv"
        (
            pathlib
            .Path(f"{rootdir}/{dump_dir_name}")
            .mkdir(parents=True, exist_ok=True)
        )

        data_table = pyarrow.Table.from_pydict(columns)
        pyarrow.csv.write_csv(
            data_table,
            full_dump_path
        )

        msg = f"Data columns dumped to {full_dump_path}"
        logging.info(msg)
