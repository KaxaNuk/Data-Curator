.. _custom_calculations:

Custom Calculations
====================

You can easily define your own **custom feature functions** to extend the capabilities of Data Curator. These are written in pure Python and live inside the file:

``Config/custom_calculations.py``

This file must exist under your **project's root `Config/` directory**, not in the templates.

How It Works
------------

Custom calculations allow you to define reusable features or metrics that are computed using one or more input columns.

Each custom function:

- Must start with the prefix ``c_``.
- Can have any valid Python name after the prefix.
- Receives **`DataColumn`** objects as arguments.
- Can also take a ``configuration`` parameter, which receives the run's ``Configuration`` object.
- Must return either a `DataColumn` computed from its arguments, or another iterable (like a `pyarrow.Array` or a pandas `Series`) with **one value per output row**.
- Functions can be composed, they may be used directly or as intermediate steps within other calculations.

Example
-------

Here’s a simple example of a custom function that calculates net margin:

.. code-block:: python

   def c_net_margin(fis_net_income, fis_revenues):
       return fis_net_income / fis_revenues

When this function is called, `fis_net_income` and `fis_revenues` are passed as `DataColumn` objects, so this function supports operations like `+`, `-`, `/`, comparisons, and null-safe logic automatically.

Why DataColumn?
---------------

All column arguments to your functions are instances of :ref:`data_column`. This provides:

- Arithmetic like `col1 + col2`
- Logical operators like `col1 > 5`
- Null-safe operations (any operation involving a null yields null)
- Seamless conversion to pandas or pyarrow if needed

Computing on the Columns' Own Rows
----------------------------------

Each column is synced to the output rows through its own clock, so the financial statements' columns hold one row per statement, and the market data's columns one row per day, even when the output has intraday rows. The DataColumn operators and the :ref:`helpers <helpers>` already compute on those rows, so a 21-day moving average always spans 21 days of data. To compute on them with your own pandas or pyarrow code:

- For a single column, compute on ``column.reference_array``, and wrap the result with ``DataColumn.load(result, reference_clock_column=column.reference_clock_column)``.
- For several columns, get the clock to compute on with ``helpers.find_operation_clock([column_1, column_2])``, each column's values on its rows with ``helpers.get_float_values(column, operation_clock_column=operation_clock_column)``, and wrap the result with ``DataColumn.load(result, reference_clock_column=operation_clock_column)``.

The clock chain then syncs the result to the output rows. Computing on ``column.to_pandas()`` or ``column.to_pyarrow()`` instead uses the output rows directly, so a rolling window over them counts output rows, like intraday bars.

Usage in Excel
--------------

Once you've written your function in ``custom_calculations.py``, reference it by name directly in the **columns** list of your Excel configuration file.

For example, to use a function called ``c_net_margin``, simply add it like this:

::

   c_net_margin

Each entry should match exactly the name of the function defined in your code, including the ``c_`` prefix.


When a Calculation Fails
------------------------

If a custom calculation raises an error, or returns something that can't be an output column, the run stops with a
message naming the calculation, its file, and the identifier being processed. When the calculation raised the error
itself, the message includes its traceback, starting from the calculation function.

Best Practices
--------------

- Chain your functions to make them modular and testable.
- Prefer returning a `DataColumn` directly for consistency.

More Examples
-------------

To explore how advanced functions are built using `DataColumn`, see:

``src/kaxanuk/data_curator/features/calculations.py``

