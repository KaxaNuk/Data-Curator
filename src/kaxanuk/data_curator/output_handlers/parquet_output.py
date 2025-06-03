import pathlib

import pyarrow
import pyarrow.parquet

from kaxanuk.data_curator.output_handlers.output_handler_interface import OutputHandlerInterface


class ParquetOutput(OutputHandlerInterface):
    """
    Generates Parquet dataset output of the processed columns data.

    Parameters
    ----------
    output_base_dir
        The path that will contain all the output subdirectories
    """

    def __init__(
        self,
        *,
        output_base_dir: str,
    ):
        self.output_base_dir = output_base_dir

    def output_data(
        self,
        *,
        main_identifier: str,
        columns: pyarrow.Table
    ) -> bool:
        """
        Output the identifier's processed data to a csv file.

        Parameters
        ----------
        main_identifier
            The identifier (ticker, etc.) of the data, Will be used to name the output parquet file.
        columns
            PyArrow Table containing all output columns.
            Structure example:
            {
                'm_open': pyarrow.Array,
                'm_close': pyarrow.array,
                ....
            }

        Returns
        -------
        bool
        """
        (
            pathlib
                .Path(self.output_base_dir)
                .mkdir(parents=True, exist_ok=True)
        )

        data_features_output_file_path = f'{self.output_base_dir}/{main_identifier}.parquet'

        pyarrow.parquet.write_table(
            columns,
            data_features_output_file_path
        )

        return True
