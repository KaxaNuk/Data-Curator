"""
Create your own custom Data Curator feature calculation functions.

To add a custom calculation, you should have this file in your own project's Config folder.

Each function needs to start with c_ as a prefix, and the rest of the name can be anything as
long as it's a valid Python function name.

Each function declares as arguments the names of each column it needs as input, which
are provided to it in our custom DataColumn objects. DataColumn acts as apyarrow.Array wrapper
but with neat features like:
- operator overloading (so you can directly perform arithmetic operations between columns,
like in pandas)
- automatically treating the result of any operation involving NaN or null elements as null, since
we consider any null a missing value

Each function needs to return an iterable supported by pyarrow.array(), of the same length
(preferably another DataColumn, a pyarrow.Array, a pandas.Series, or a 1D numpy.ndarray).
The result will automatically be wrapped in a DataColumn for any successive functions that use
that as input. Yes, you can absolutely chain functions together and are encouraged to do so!

Once you've added your function to the file, you need to add its name to the Output_Columns
sheet of the data_curator_parameters.xlsx file. Don't forget that your function name needs to
start with c_ as a prefix!

See more examples of how to program custom functions by checking our built-in calculations at
https://github.com/KaxaNuk/Data-Curator/blob/main/src/kaxanuk/data_curator/features/calculations.py
"""

# Here you'll find helper functions for calculating more complicated features:
from kaxanuk.data_curator.features import helpers


def c_test(m_open_split_adjusted, m_close_split_adjusted):
    """
    Example feature calculation function.

    Receives the market open and market close columns, and returns a column with their difference.

    For this function to generate an output column, you need to:
    1. Make sure it's in your project's Config/custom_calculations.py file.
    2. Add c_test to the Output_Columns sheet in your Config/data_curator_parameters.xlsx file.

    Parameters
    ----------
    m_open_split_adjusted : kaxanuk.data_curator.DataColumn
    m_close_split_adjusted : kaxanuk.data_curator.DataColumn

    Returns
    -------
    kaxanuk.data_curator.DataColumn
    """
    # we're just doing a subtraction here, but you can implement any logic
    # just remember to return the same number of rows in a single column!
    return m_close_split_adjusted - m_open_split_adjusted
