import pandas
import pyarrow

from kaxanuk.data_curator.exceptions import (
    OutputHandlerError,
    PassedArgumentError,
)
from kaxanuk.data_curator.output_handlers.output_handler_interface import OutputHandlerInterface


class InMemoryOutput(OutputHandlerInterface):
    """
    Retains in-memory the output of the processed columns data, for exporting as variables.
    """

    def __init__(
        self
    ):
        self.data = {}

    def output_data(
        self,
        *,
        main_identifier: str,
        columns: pyarrow.Table
    ) -> bool:
        """
        Store the output data in memory.

        Parameters
        ----------
        main_identifier
            The identifier (ticker, etc.) of the data, Will be used to name the output csv file.
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
            True if the data was successfully stored.
        """
        self.data[main_identifier] = columns

        return True

    def export_dataframe(
        self,
        date_index_column: str,
    ) -> pandas.DataFrame:
        """
        Export the stored data as a pandas DataFrame, indexed by each identifier and the date index column.

        Parameters
        ----------
        date_index_column
            The output column holding the dates or timestamps of the rows, like the master clock column, e.g. 'm_date'
            for daily market data

        Returns
        -------
        The DataFrame containing the stored data.

        Raises
        ------
        OutputHandlerError
            If there's no data to export
        PassedArgumentError
            If the output of any identifier doesn't have the date index column
        """
        if len(self.data) < 1:
            msg = "No data to export."

            raise OutputHandlerError(msg)

        dataframes = []
        for (identifier, table) in self.data.items():
            dataframe = table.to_pandas()

            if date_index_column not in dataframe.columns:
                msg = " ".join([
                    f"Unable to index the in-memory output by {date_index_column},",
                    f"as the output of {identifier} doesn't have that column.",
                ])

                raise PassedArgumentError(msg)

            dataframe['main_identifier'] = identifier
            dataframes.append(dataframe)

        # Concatenate and set a MultiIndex, preserving all dates
        combined_dataframe = pandas.concat(
            dataframes,
            axis=0,
            ignore_index=True
        )
        combined_dataframe.set_index(
            [
                'main_identifier',
                date_index_column,
            ],
            inplace=True
        )

        return combined_dataframe
