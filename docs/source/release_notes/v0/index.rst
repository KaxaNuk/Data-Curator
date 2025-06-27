.. _changelog_v0:

V0 Changelog
============

0.40.2 (2025-06-05)
-------------------

Fixed
~~~~~

* Cli ``init excel`` command not creating ``.env`` file in the ``Config`` folder
* ``c_market_cap`` calculation now uses split-adjusted close price, to reduce the jumps right after a split
* ReadTheDocs integration basic setup



0.40.1 (2025-06-04)
-------------------

Fixed
~~~~~

* OS Error on cli ``init script`` because of entry script template missing from wheel data



0.40.0 (2025-06-04)
-------------------

Changed
~~~~~~~

* First public release, now on PyPI



0.39 (2025-06-03)
-----------------

Changed
~~~~~~~

* ``ticker`` has been renamed to ``main_identifier`` in Excel configuration and all related code
* ``Ticker`` entity is now a ``MainIdentifier`` entity, with ``Ticker.symbol`` becoming ``MainIdentifier.identifier``
* ``MainIdentifier.identifier`` now only validates that no whitespace is present
* ``TickerNotFoundError`` is now ``IdentifierNotFoundError``
* Configuration entity no longer validates identifiers format, as different data providers will have different required formats
* ``fi_`` prefix for income statement columns now becomes ``fis_``
* Versioning changed to remove ``1.0b`` prefix, as version 1.0 still requires quite a few features.
* ``FundamentalDataRowIncome`` entity is now ``FundamentalDataRowIncomeStatement``
* ``DataProviderInterface.init_config()`` is now ``DataProviderInterface.initialize()``
* ``DividendDataRow.adjusted_dividend`` renamed to ``DividendDataRow.split_adjusted_dividend``
* ``MarketDataDailyRow`` entity fields added for all split-adjusted and dividend-and-split-adjusted data sets
* FMP updated all endpoints to the new "stable" version
* ``FMP`` class and all related code usages now renamed to ``FinancialModelingPrep``;
* Parameters file now uses ``financial_modeling_prep`` instead of ``fmp`` as the provider name
* Renamed most calculation feature functions for consistency
* Updated Excel parameters file template to the new public API

Fixed
~~~~~

* CLI ``update excel`` command not working on dev

Removed
~~~~~~~

* ``MarketDataDailyRow.adjusted_close`` field, as it is now redundant



0.38 (2025-05-08)
-----------------

Added
~~~~~

* ``DataColumn`` comparison operators ==, !=, <, <=, >, >= for use in binary logic, filters, etc.
* ``DataColumn.boolean_and`` and ``DataColumn.boolean_or`` methods for boolean comparisons between multiple BooleanArray DataColumns
* Missing ``DataColumn`` reverse arithmetic operation tests
* ``DataColumn._replace_array_mask_with_nones`` private method to replace the mask of a DataColumn with None values
* ``DataColumn`` related exceptions

Changed
~~~~~~~

* ``DataColumn`` divisions involving decimal inputs now output as float64, as any decimal precision was lost in the division anyway and becomes innecessary



0.37 (2025-04-11)
-----------------

Added
~~~~~

* Created ``conftest.py`` with ``pytest`` fixtures to load example data from CSV files used in calculation tests.
* Helper methods in ``helpers.py`` to compute technical indicators, including Exponential Moving Average (EMA).
* Pytest coverage for ``calculations.py``, reaching 96%.

Changed
~~~~~~~

* Prefixed all functions in ``calculations.py`` with ``c_``.
* Alphabetically ordered all functions in ``calculations.py`` and their corresponding tests.
* Replaced usage of ``ta`` library for technical indicators in almost all features, except for ``c_chaikin_money_flow`` and ``c_relative_strength_index_14d``, which are still under development.
* Removed CSV-based test data fixtures previously defined directly in ``calculations_test.py``.



0.36 (2025-03-24)
-----------------

Changed
~~~~~~~

* Simplify dev installation procedure in README.md and Dockerfile 

Fixed
~~~~~

* ExcelConfigurator: Data providers returning ``validate_api_key()`` as ``None`` should not raise an error
* ColumnBuilder: Extended Fundamental entity properties fail when full data period contains no fundamental data 



0.35 (2025-03-13)
-----------------

Fixed
~~~~~

* ``DataColumn`` extended entity subclasses should now work with empty data on the first rows 



0.34 (2025-03-04)
-----------------

Added
~~~~~

* ``DataProviderInterface`` tests (incomplete)

Changed
~~~~~~~

* ``DataProviderInterface._find_first_date_before_start_date_in_descending_dates`` renamed to ``DataProviderInterface._find_first_date_before_start_date``, added ``descending_order`` parameter
* ``DataProviderInterface._find_unordered_dates_in_descending_dates`` renamed to ``DataProviderInterface._find_unordered_dates``, added ``descending_order`` parameter



0.33 (2025-02-20)
-----------------

Changed
~~~~~~~

* Feature and custom calculation function names should now always start with a ``c_`` prefix.
* Improved some error texts' legibility

Deprecated
~~~~~~~~~~

* Feature and custom calculation function names without the ``c_`` prefix will stop working in the public release version.  



0.32 (2025-02-17)
-----------------

Changed
~~~~~~~

* Breaking: ``entity_helper.fill_fields`` renamed to ``entity_helper.convert_data_row_into_entity_fields``
* ``entity_helper.convert_data_row_into_entity_fields`` now skips the type conversion logic if the value is already the expected type.



0.31 (2025-02-13)
-----------------

Changed
~~~~~~~

* Can now subclass all fundamental statement row entities to add custom data columns

Fixed
~~~~~

* Regression: Debugger initialization was not being called



0.30 (2025-01-31)
-----------------

Changed
~~~~~~~

* Python 3.13 now officially supported
* Can now subclass ``MarketDataDailyRow`` to add custom market data columns

Fixed
~~~~~

* CLI ``update`` should load the templates from the templates dir instead of the Python data dir when installed in editable mode
* templates dir not found when installed in editable mode on Windows



0.29 (2025-01-25)
-----------------

Added
~~~~~

* ``kaxanuk.data_curator.modules.extension_handler`` aliased to ``kaxanuk.data_curator.extension_handler`` for loading external extension modules

Changed
~~~~~~~

* YahooFinance is now a separate package, ``kaxanuk.data_curator_extensions.yahoo_finance`` under https://github.com/KaxaNuk/Data-Curator-Extensions_Yahoo-Finance

Removed
~~~~~~~

* YahooFinance from this main package



0.28 (2024-12-19)
-----------------

Added
~~~~~

* Sphinx documentation generator with Readthedocs support
* Creation of a .py extension for parsing the calculations file and classifying its content using the ``..category::`` directive found in the docstring
* Implementation of a structure to support ``.md`` files located outside the docs folder
* Development of a Sphinx custom template for the documentation.
* CLI ``--version`` command
* ``kaxanuk.datacurator.__package_name__``
* ``kaxanuk.datacurator.__package_title__``

Changed
~~~~~~~

* ``__version__`` and ``__parameters_format_version__`` moved from ``__version__.py`` to ``__init__.py``
* Migration of the documentation structure to the ``docs`` folder, replacing the deprecated ``Read_the_Docs`` folder.
* Parquet and CSV output handlers no longer take ``data_features_subdir`` parameter, everything is saved to the ``Output`` folder

Fixed
~~~~~

* Regression: YahooFinance not correctly loaded by Excel configuration entry script
* Editable install CLI ``init`` and ``update`` now work in any directory
* CLI catch ``OSError`` when there's a file permissions issue

Removed
~~~~~~~

* ``__version__.py`` file
* ``kaxanuk.datacurator.version``
* ``Data_and_Features`` subfolder



0.27 (2024-11-17)
-----------------

Added
~~~~~

* CLI update format 'entry_script' for updating just the entry script
* ``validate_api_key`` abstract (required) method to ``DataProviderInterface``

Changed
~~~~~~~

* ``FinancialDataProviderInterface`` is now ``DataProviderInterface`` once more
* ``ExcelConfigurator.__init__`` API changed, data_providers now receive a typed dict with ``class`` and ``api_key`` params
* Data provider API keys now are per provider
* Excel entry script now gets api keys from environment (loading from Config/.env if available)
* ``FMP`` changed ``_endpoints`` MappingProxyType for StrEnum
* ``FMP.validate_api_key`` makes a request to get AAPL company information
* Updated parameters_datacurator file version



0.26 (2024-11-13)
-----------------

Added
~~~~~

* ``Datacolumn.__neg__``
* Left a not implemented placeholder for ``Datacolumn.__pos__`` for completeness

Changed
~~~~~~~

* ``DataColumn.all_equal()`` renamed to ``DataColumn.fully_equal()``
* Updated usage instructions in README.md

Fixed
~~~~~

* DataColumn reflected operators for ``+``, ``-``, ``*``, ``/``, ``//``, ``%``



0.25 (2024-11-08)
-----------------

Added
~~~~~

* Tests to replicate reflected arithmetic operation errors.
* Missing typing on data_column methods.
* CLI interface through ``services.cli``
* click library for the CLI implementation
* ``.env`` file to templates (unused at the moment)
* ``__main__.py`` file to templates

Changed
~~~~~~~

* ``DataColumn`` arithmetic operation tests now grouped into classes.
* Docker images now install the library and use the CLI interface
* ``templates`` dir files moved into ``data_curator`` subdir so that installed data files are also installed in a subdir of the data dir
* Config templates are now in the ``templates/data_curator/Config`` subdir
* The PyCharm debugger now only depends on the ``KNDC_DEBUG_PORT`` env variable for activation

Fixed
~~~~~

* GitHub workflow broke on pull request merges

Removed
~~~~~~~

* ``Config`` dir (gets created by the CLI interface now)
* ``Output`` dir (gets created by the CLI interface now)
* ``__main__.py`` (gets created by the CLI interface now)



0.24 (2024-10-12)
-----------------

Changed
~~~~~~~

* FMP: use unadjustedVolume for ``MarketDataDailyRow.volume``
* Standardized .gitignore based on the official GitHub one

Fixed
~~~~~

* Pin Dockerfile base image to python:3.12-slim, as Python 3.13 is now a thing and Pyarrow breaks on it



0.23 (2024-09-21)
-----------------

Added
~~~~~

* The GitHub Actions pipeline now builds and pushes the container image to GitHub Packages when pushing the dev branch or a main branch tag.



0.22 (2024-09-21)
-----------------

Changed
~~~~~~~

* ``DataColumn.__getitem__`` - simplified the logic
* First steps to build the full GitHub Packages publishing pipeline

Fixed
~~~~~

* ``FinancialDataProviderInterface._request_data`` - "AttributeError: 'URLError' object has no attribute 'read'" when ``_load_ssl_context`` throws "ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host"
* ``FinancialDataProviderInterface._request_data`` - connection errors were still throwing up error trace after all connection attempts



0.21 (2024-08-04)
-----------------

Added
~~~~~

* tests: ``ColumnBuilder._process_columns_with_available_dependencies`` now fully tested

Changed
~~~~~~~

* prepended ``KNDC_`` to all env vars
* increased ``parameters_datacurator.xlsx`` version to 0.21
* ``ExcelConfigurator`` now receives the names of the api key env vars, and loads them itself, as otherwise the warnings can't really know if the keys were loaded from env vars or not
* added optional ``logger_file`` parameter to ``data_curator.main``, to enable logging to a file
* Fundamental data entity error logs now include the date, where available

Fixed
~~~~~

* ``logger_level`` parameter in ``data_curator.main`` was being ignored if ``ExcelConfigurator`` had been used beforehand, as it was not closing its own logger
* ``ExcelConfigurator`` now changes its own level to the one in the Excel file, as soon as it can



0.20 (2024-07-28)
-----------------

Changed
~~~~~~~

* *parameters.xlsx* file renamed to *parameters_datacurator.xlsx* as a workaround for Excel's mind-numbingly idiotic inability to have 2 open files with the same name as of 2024
* ``MarketDataDailyRow``: validate ``low <= high``
* ``MarketDataDailyRow``: validate ``volume`` and ``vwap`` are not negative
* ``FundamentalDataRowIncome``: validate ``weighted_average_shares_outstanding`` and ``weighted_average_shares_outstanding_diluted`` are not negative
* ``ColumnBuilder._process_columns_with_available_dependencies`` is now class method, for easier unit testing
* pytest: added ``data_column_debugger.dump_data_columns_to_csv()`` fixture to conftest.py for dumping DataColumns to a csv file for debugging tests
* more unit tests



0.19 (2024-06-30)
-----------------

Added
~~~~~

* ``FinancialDataProviderInterface._build_url_with_ticker_path_and_query_params`` method for building standard URLs
* ``calculations.annualized_volatility_5d()`` test with csv fixture

Changed
~~~~~~~

* ``features.helpers.annualized_volatility`` and ``features.helpers.indexed_rolling_window_operation`` now require kwargs as they are meant as user-facing functions
* improved ``column_builder`` typing and docstrings
* ``FinancialDataProviderInterface._request_data`` now accepts a ``url_builder`` callable argument for building the URLs, ``_build_url_with_ticker_path_and_query_params`` by default
* made ``MarketDataDailyRow.adjusted_close`` nullable
* refactored ``DataColumn.equal()`` to use both absolute and relative thresholds when comparing with ``approximate_floats=true``
* better documentation and linting

## Fixed
* Fundamental Data Provider 'none' failed regression
* ``DataColumn.equal`` failed when presented 2 NullArray arguments



0.18 (2024-05-26)
-----------------

Added
~~~~~

* ``logger_format`` param to ``ExcelConfigurator.__init__``
* ``Ticker`` value object for entities
* ``services.validator`` module, with a validator for date pattern strings
* date pattern tests for dividend and split rows
* package descriptions for each ``__init__.py``

Changed
~~~~~~~

* Started removing redundant docstring types from typehinted functions
* ``providers`` package is now ``data-providers``
* ``internals`` package is now ``services``
* entities now use ``Ticker`` instead of ``str`` for field ``ticker``
* Moved all interface modules to their respective implementation packages
* test coverage report now skips fully covered files

Fixed
~~~~~

* ``ExcelConfigurator`` missing checks
* Handle FMP throwing error 404 when symbol data not found
* ``entity_helper.detect_field_type_errors`` bug that prevented validation of all fields of an entity
* ``entity_helper`` and ``validator`` services now fully tested, full library test coverage @ 58%



0.17 (2024-05-12)
-----------------

Added
~~~~~

* pdm.lock to .gitignore (don't forget to remove on main branch!)
* ``FinancialDataProviderInterface.init_config()`` method for running code before looping through each ticker
* ``DataProviderMissingKeyError``
* YahooFinance missing interface implementation methods
* ``__all__`` to all ``__init__.py`` files, to be able to remove the ``F401`` linter ignore
* ``ConfiguratorInterface``

Changed
~~~~~~~

* Data providers now use object instances instead of classes
* Inject api keys into data providers at object initialization, removed them from individual method params
* ``data_curator.main()`` now receives individual data provider objects instead of dict with possible choices
* ``data_curator.main()`` now receives output_handlers list, runs all
* ``data_curator.main()`` now receives an int ``logger_level``
* Complete refactor of ``ExcelConfigurator``, is now an implementation of the new ``ConfiguratorInterface`` and has methods for outputting each dependency type
* ``PassedArgumentError`` Exception

Fixed
~~~~~

* SplitData error with Fundamental data provider set as None
* data_curator.py linter G004 error

Removed
~~~~~~~

* ``Configuration`` entity fields related to providers, handlers and loggers



0.16 (2024-05-02)
-----------------

Added
~~~~~

* yahoo_finance market data - yahoo!

Changed
~~~~~~~

* increased parameters file version



0.15 (2024-04-28)
-----------------

Added
~~~~~

* split data
* initial entity_helper tests

Changed
~~~~~~~

* MarketDataDailyRow.vwap now nullable
* use entity_helper.detect_field_type_errors() for all downloaded data entity validation
* entity_helper.fill_fields() now accepts null field correspondences
* updated parameters.xlsx template to add new fields, column width adjustments 



0.14 (2024-04-23)
-----------------

Added
~~~~~

* modules/data_column 100% effective test coverage!
* dividend data - had to rework some internal machinery
* entity_helper.detect_field_type_errors() for validating entity field types

Changed
~~~~~~~

* ``pdm run test`` now includes coverage term-missing report
* attribute_filler.py renamed to entity_helper.py



0.13 (2024-04-06)
-----------------

Added
~~~~~

* DataColumn.__mod__() actual implementation
* DataColumn private members tests, class coverage at 90%

Changed
~~~~~~~

* Replaced generic exceptions by custom ones

Fixed
~~~~~

* Decimal precision out of range errors on DataColumn.__add__(), .__subtract__()
* Decimal precision out of range error on DataColumn._mask_zeroes()
* Multiple DataColumn private methods minor bugs



0.12 (2024-03-24)
-----------------

Changed
~~~~~~~

* CHANGELOG.md updated Keep a Changelog link to version 1.1.0
* Fundamental data: missing cash flow or balance sheet now returns just the income statement data if available
* Improved Api server error retries handler
* Api server errors (after retries) now fully stop execution
* Publicly exposed entity classes from kaxanuk.data_curator.entities namespace
* Improved error handler for circular dependencies
* Improved error handler for missing custom calculation functions
* Improved error handler for Excel parameters file configuration errors
* Removed redundant src/kaxanuk/py.typed file

Fixed
~~~~~

* "Decimal precision out of range" error when dividing decimal columns with too many decimals



0.11 (2024-03-17)
-----------------

Added
~~~~~

* pdm run install_dev
* parquet output handler
* py.typed files to declare the whole library is typehinted

Changed
~~~~~~~

* now loading api_keys from env vars, only using templates/parameters.xlsx as fallback
* increased templates/parameters.xlsx version
* fixed templates/parameters.xlsx start date validation
* added all default output columns to templates/parameters.xlsx
* improved internal documentation in templates/parameters.xlsx
* Docker: don't uninstall pdm on dev environment
* renamed ``pdm run test_with_coverage`` to ``pdm run test``

Fixed
~~~~~

* support index tickers with ^ character
* "Decimal precision out of range" error when multiplying decimal columns with too many decimals
* ExcelConfigurator typecasting None values to 'None' string
* mypy configuration and some typehint errors



0.10 (2024-03-10)
-----------------

Added
~~~~~

* coverage tests under pytest-cov

Changed
~~~~~~~

* modules/security_calculations.py is now features/calculations.py
* moved security_calculations helper functions into features/helpers.py
* moved modules/attribute_filler.py to internals/attribute_filler.py
* moved modules/column_builder.py to internals/column_builder.py
* exposed FinancialDataProviderInterface as kaxanuk.data_curator.interfaces.FinancialDataProviderInterface
* exposed OutputHandlerInterface as kaxanuk.data_curator.interfaces.OutputHandlerInterface
* refactored DataColumn tests

Fixed
~~~~~

* Circular references error when custom calculation function parameter columns are missing from the output column list
* __main__.py now only injects src to sys.path if not loading as installed library



0.9 (2024-03-03)
----------------

Added
~~~~~

* An actual public API for the library by means of ``__all__`` and imports in ``__init__.py`` files
* DataColumn methods, fully unit tested:: ``all_equal``, ``concatenate``, ``equal``
* DataColumn property: ``type``
* Basic url request retry functionality

Changed
~~~~~~~

* CsvOutputter is now CsvOutput
* ``__main__.py`` now uses the public library API in its imports

Fixed
~~~~~

* security_calculations._indexed_rolling_window_operation was broken, so last_twelve_month... functions returned wrong data
* Improved internal documentation
* Improved/simplified some type hints
* Regression: Empty market data no longer terminates the whole process 

Removed
~~~~~~~

* Numpy no longer a dependency



0.8 (2024-02-26)
----------------

Added
~~~~~

* pytest pyarrow_helper fixture, for helper functions for testing PyArrow arrays

Changed
~~~~~~~

* No more Numpy! DataColumn, and thus security_calculations, now work on top of PyArrow!
* DataColumn public API changed, but operator overloading works the same
* security_calculations refactor as PyArrow simplifies many operations, allows easier use of pandas
* security_calculations output columns are automatically wrapped in DataColumn



0.7 (2024-02-11)
----------------

Added
~~~~~

* Add main() optional parameter logger_format, for configuring the logger

Changed
~~~~~~~

* Make entity attribute typing/casting errors more explicit
* Remove revenue>=0 checks
* Simplify entity attribute type validations
* Change the default logger format for more readable logs
* Allow floats as entity attribute type
* Moved all general helper methods from FMP to FinancialDataProviderInterface, so any provider can use them

Fixed
~~~~~

* Require Configurator.start_date and end_date to be explicitly datetime.date



0.6 (2024-02-05)
----------------

Fixed
~~~~~

* Add blank FundamentalData rows for ommited data (in case of ammendments, missing fundamentals, &c.)



0.5 (2024-02-04)
----------------

Added
~~~~~

* Custom exceptions
* New library dependency for semver version comparisons: packaging

Changed
~~~~~~~

* New parameters file template
* Now versioning the parameters file formats, and checking them in ExcelConfigurator
* Fundamental data provider can be now set independently of market data one, or even as disabled
* Separate input and config handlers into their own folders
* Rename the "quarter" period to "quarterly" in config

Fixed
~~~~~

* Missing fundamental data for a ticker will only omit that data, but keep the market data



0.4 (2024-02-03)
----------------

Changed
~~~~~~~

* Replace numpy.array with custom DataColumn to remove "where" kwarg boilerplate code.
* Inject custom calculations from entry script
* Move templates outside src



0.3 (2024-01-07)
----------------

* Restructure src to implement under organization/project_name namespace.

