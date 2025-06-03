"""
Interface for generating output from each identifier's obtained and calculated columns.
"""

import abc

import pyarrow


class OutputHandlerInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
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
            The identifier (ticker, etc.) of the data
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
