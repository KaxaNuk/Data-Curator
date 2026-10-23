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
- Must return an iterable (preferably a `DataColumn`, `pyarrow.Array`) of the **same length**.
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

Usage in Excel
--------------

Once you've written your function in ``custom_calculations.py``, reference it by name directly in the **columns** list of your Excel configuration file.

For example, to use a function called ``c_net_margin``, simply add it like this:

::

   c_net_margin

Each entry should match exactly the name of the function defined in your code, including the ``c_`` prefix.


Best Practices
--------------

- Chain your functions to make them modular and testable.
- Prefer returning a `DataColumn` directly for consistency.

More Examples
-------------

To explore how advanced functions are built using `DataColumn`, see:

``src/kaxanuk/data_curator/features/calculations.py``

