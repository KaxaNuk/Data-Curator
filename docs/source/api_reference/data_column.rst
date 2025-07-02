.. _data_column:

DataColumn
==========

The `DataColumn` class is the foundational abstraction for all tabular operations in **Data Curator**. It wraps a `pyarrow.Array` and adds element-wise operations, comparison logic, and composability for calculated columns.

It powers the **calculated column system**, Boolean logic in filters, type-safe arithmetic across datasets, and more.

Overview
--------

At its core, `DataColumn`:

- Encapsulates a `pyarrow.Array`.
- Enables arithmetic and comparison operations (`+`, `-`, `==`, `//`, etc.).
- Ensures null propagation and broadcasting consistency.
- Supports composable transformations for use in **custom calculations**.

Basic Usage
-----------

.. code-block:: python

   from kaxanuk.data_curator.modules.data_column import DataColumn

   col_a = DataColumn.load([1, 2, 3])
   col_b = DataColumn.load([10, 20, 30])

   result = col_a + col_b   # Element-wise addition
   filtered = col_a > 1     # Element-wise comparison returns boolean DataColumn

   result.to_pandas()       # Export to pandas
   result.to_pyarrow()      # Export to pyarrow

Arithmetic Operators
--------------------

You can apply arithmetic operations directly using standard Python syntax:

- `+` (via `__add__`)
- `-` (via `__sub__`)
- `*` (via `__mul__`)
- `/` (via `__truediv__`)
- `//` (via `__floordiv__`)
- `%` (via `__mod__`)

Reflected versions like `3 + col` also work thanks to:

- `__radd__`, `__rsub__`, `__rmul__`, `__rtruediv__`, `__rfloordiv__`, `__rmod__`

All operations return a new `DataColumn`, with null-aware and type-safe behavior:

.. code-block:: python

   col = DataColumn.load([2, 4, 6])

   col + 1         # [3, 5, 7]
   col * 2         # [4, 8, 12]
   10 - col        # [8, 6, 4]
   col / 2         # [1.0, 2.0, 3.0]

Comparison Operators
--------------------

`DataColumn` supports element-wise comparison using standard Python syntax:

- `==` (via `__eq__`)
- `!=` (via `__ne__`)
- `<` (via `__lt__`)
- `<=` (via `__le__`)
- `>` (via `__gt__`)
- `>=` (via `__ge__`)

Each of these returns a new `DataColumn` of boolean values:

.. code-block:: python

   col = DataColumn.load([5, 10, 15])

   col > 7       # [False, True, True]
   col == 10     # [False, True, False]
   col <= col    # [True, True, True]

Logical Functions
-----------------

Combine multiple boolean columns using:

- `DataColumn.boolean_and(...)`
- `DataColumn.boolean_or(...)`

These work across multiple `DataColumn` objects, pyarrow scalars or booleans. Optionally enable `allow_null_comparisons=True` for Kleene logic.

.. code-block:: python

   DataColumn.boolean_and(col1 > 0, col2 < 100)
   DataColumn.boolean_or(col1.is_null(), col2 == 5)

Equality Utilities
------------------

To compare full columns for equality:

- `equal(...)` — element-wise
- `fully_equal(...)` — total match (returns `True`, `False` or `None`)

.. code-block:: python

   DataColumn.equal(col1, col2, equal_nulls=True)
   DataColumn.fully_equal(col1, col2, skip_nulls=True)

String Concatenation
--------------------

Use `concatenate` to merge string-type columns or scalars:

.. code-block:: python

   DataColumn.concatenate(col1, col2, separator="-", null_replacement="N/A")

This returns a new `DataColumn` with joined string values.

Loading and Conversion
----------------------

You can create and convert `DataColumn` objects easily:

- `DataColumn.load(...)` — from list, pandas.Series, or pyarrow.Array
- `.to_pandas()` — convert to `pandas.Series`
- `.to_pyarrow()` — convert to `pyarrow.Array`
- `.type` — get native `pyarrow` type
- `.is_null()` — detect if the array is fully null

Advanced Behavior
-----------------

Null-safe math and broadcasting are internally managed through helper methods:

- `_mask_dual_array_nulls(...)`
- `_replace_array_mask_with_nones(...)`
- `_return_null_column_on_null_operand(...)`

These ensure safe and predictable behavior in pipelines, especially in user-defined calculations.

API Reference
-------------

.. automodule:: kaxanuk.data_curator.modules.data_column
   :members:
   :undoc-members:
   :show-inheritance:
