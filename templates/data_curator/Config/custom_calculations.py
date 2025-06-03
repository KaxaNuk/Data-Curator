"""
Easily add your own custom feature calculation function.

To add a custom calculation function, you need to have this file the Config folder under the
project's root directory (and not the templates directory!), and add your functions there.

Each function needs to start with c_ as a prefix, and the rest of the name can be anything as
long as it's a valid Python function name.

Each function declares as arguments the names of each column it needs as input, which
are provided to it as our custom DataColumn objects that act as pyarrow.Array wrappers
but with neat features like:
- operator overloading (so you can directly perform arithmetic operations between columns,
like in pandas)
- automatically casting any operations involving NaN or null elements as null, as we
consider any null a missing value

Each function needs to return an iterable supported by pyarrow.array(), of the same length
(preferably another DataColumn, a pyarrow.Array, a pandas.Series or a 1D numpy.ndarray).
The result will automatically be wrapped in a DataColumn for any successive functions that
use that as input. Yes, you can absolutely chain together functions, and are encouraged to
do so!

Once you've added your function to the file, you need to add its name to the Output_Columns
sheet of the parameters_datacurator.xlsx file. Don't forget that your function name needs to
start with c_ as a prefix!

See more examples of how easy it is to program custom functions by checking out the file
src/kaxanuk/data_curator/features/calculations.py
"""

# Here you'll find helper functions for calculating more complicated features:
from kaxanuk.data_curator.features import helpers


def c_test(m_open, m_close):
    """
    Example features calculation function.

    Receives the market open and market close columns, and returns a column with their difference.

    For this function to generate an output column, you need to:
    1. Place it in the Config/custom_calculations.py file (if it doesn't exist you can copy
    this file there).
    2. Add c_test to the Output_Columns sheet in the Config/parameters_datacurator.xlsx file.

    Parameters
    ----------
    m_open : kaxanuk.data_curator.DataColumn
    m_close : kaxanuk.data_curator.DataColumn

    Returns
    -------
    kaxanuk.data_curator.DataColumn
    """
    # we're just doing a subtraction here, but you can implement any logic
    # just remember to return the same number of rows in a single column!
    return m_close - m_open
