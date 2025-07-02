import pandas
import pyarrow

from kaxanuk.data_curator.exceptions import OutputHandlerError
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

    def export_dataframe(self) -> pandas.DataFrame:
        """
        Export the stored data as a pandas DataFrame.

        Returns
        -------
        The DataFrame containing the stored data.
        """
        if len(self.data) < 1:
            raise OutputHandlerError("No data to export.")

        dataframes = []
        for (identifier, table) in self.data.items():
            dataframe = table.to_pandas()

            if 'm_date' not in dataframe.columns:
                raise OutputHandlerError("Unable to index in-memory output by date as 'm_date' column is missing.")

            dataframe['main_identifier'] = identifier
            dataframes.append(dataframe)

        # Concatenate and set a MultiIndex, preserving all dates
        combined_dataframe = pandas.concat(
            dataframes,
            axis=0,
            ignore_index=True
        )
        combined_dataframe.set_index(
            ['main_identifier', 'm_date'],
            inplace=True
        )

        return combined_dataframe
