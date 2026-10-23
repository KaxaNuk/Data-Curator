.. _testing_guide:

Testing Guide
=============

All tests run with ``pytest``, and live in the ``tests`` folder, split into the ``unit`` and ``integration`` suites.

Running the Tests
-----------------

- Run the whole suite with coverage:

  ::

      pdm run test

- Run a single file, or only the tests whose class or method names match an expression:

  ::

      pytest tests/unit/entities/configuration_test.py
      pytest tests -k TestPrivateInfillData

- If you use the devcontainer, run these commands in its terminal, where the repository is mounted at ``/app``.
- Tests must not need a network connection or API keys, as CI runs them on Python 3.12, 3.13 and 3.14.

Unit Tests
----------

Folder Structure
~~~~~~~~~~~~~~~~

- Unit tests live in ``tests/unit``, whose subfolders mirror the ones in ``src/kaxanuk/data_curator``.
- Each test file is named after the module it tests, with the ``_test.py`` suffix, e.g. ``tests/unit/entities/configuration_test.py`` tests ``src/kaxanuk/data_curator/entities/configuration.py``.
- A module's tests can be split into several files, each covering a specific type of test, inside a folder named after the module, e.g. ``arithmetic_test.py`` and ``comparison_test.py`` in ``tests/unit/modules/data_column/``.
- Every folder inside ``tests`` holding Python files is a regular Python package, containing an ``__init__.py`` file even if it's empty.
- Fixtures used by several tests can go in a ``fixtures`` subfolder of the folder holding those tests.

::

    ├── src
    │   └── kaxanuk
    │       └── data_curator
    │           ├── entities
    │           │   ├── __init__.py
    │           │   └── configuration.py
    │           └── modules
    │               ├── __init__.py
    │               └── data_column.py
    └── tests
        └── unit
            ├── __init__.py
            ├── entities
            │   ├── __init__.py
            │   └── configuration_test.py
            └── modules
                ├── __init__.py
                └── data_column
                    ├── __init__.py
                    ├── arithmetic_test.py
                    └── comparison_test.py

File Structure
~~~~~~~~~~~~~~

- Group the test methods of a file into classes, each one testing a single function or method.
- Name each class ``Test`` followed by the CamelCase name of the function or method it tests, using ``TestPrivate`` for the ones starting with an underscore, and ``TestDunder`` for dunder methods, e.g. ``TestPrivateInfillData`` for ``_infill_data``, and ``TestDunderTruediv`` for ``__truediv__``.
- Name each test method ``test_`` followed by a short description of what makes the test unique.
- Use a single ``assert`` per test method, unless the test's setup is complex enough to require intermediate assertions between its steps.
- Prioritize readability and auditability over conciseness.
- When using fixtures, avoid chains of intermediate functions for transforming them into the test inputs and expected outputs, as they make it hard to understand what the test is doing.

::

    # tests/unit/modules/data_column/arithmetic_test.py
    class TestDunderTruediv:
        def test_int_column_divided_by_int_scalar(self):
            column = DataColumn.load(
                [2, 4, 6]
            )
            expected = DataColumn.load(
                [1.0, 2.0, 3.0]
            )
            result = column / 2

            assert DataColumn.fully_equal(
                result,
                expected,
                approximate_floats=True,
            )

Test Helpers
------------

- Use ``pytest.mark.parametrize`` to run the same test over several inputs, instead of repeating it.
- Compare ``DataColumn`` objects with ``DataColumn.fully_equal``, passing ``approximate_floats=True`` for float results, and ``equal_nulls=True`` when nulls in the same positions should count as equal.
- To inspect columns while debugging a test, request the ``data_column_debugger`` fixture from ``tests/conftest.py``, and pass the columns as keyword arguments to its ``dump_data_columns_to_csv`` method, which writes them to a CSV file in the repository's ``.debug`` folder.
- Use pytest's ``monkeypatch`` fixture for replacing objects, as the linter forbids ``unittest.mock``.
- pytest runs with ``--strict-markers``, so any custom marker needs to be registered in ``pyproject.toml``.

Integration Tests
-----------------

- Integration tests live in ``tests/integration``, in a folder per data provider, e.g. ``tests/integration/sharadar/sharadar_test.py``.
- They must never call the live provider APIs: save pickle fixtures with the exact same structure as what the provider's library or web service returns in the folder's ``fixtures`` subfolder, and replace only the transport layer that fetches them, so that all the parsing, validation and entity assembly runs through the production code.
- The fixtures must never redistribute real data: anonymize the tickers (e.g. the made-up ``KXNB`` in the Sharadar fixtures) and modify the values, while keeping their structure exactly the same.
- For example, the Sharadar tests replace the provider's ``_sharadar_client`` class variable with the mock client in ``fixtures/sharadar_client_mock.py``, which serves each table from the local pickle fixtures.

Bug Fixes
---------

- Every bug fix needs a regression test reproducing the bug: write it first, check that it fails, and then check that it passes after the fix.
