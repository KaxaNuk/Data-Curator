.. _component_integrator:

Component Integrator Workflow
=============================

In “component integrator” mode, you bypass the Excel-driven Zero-Coder interface and instead write a Python script that constructs a `Configuration` object, selects or implements data providers and output handlers, and invokes the core Data Curator functionality programmatically. This mode assumes you have Python programming experience and a Direct Setup (non-container) environment. Follow these steps to install the component library, create an entry script, and (if needed) implement custom data providers or data blocks.

Install the Data Curator Component Library
------------------------------------------

Use Pip to install the library from PyPI:

.. code-block:: bash

   pip install --upgrade kaxanuk.data_curator

This command installs `kaxanuk.data_curator` along with all its dependencies (e.g., `openpyxl`, `pandas`, `pyarrow`, etc.).

Create an Entry Script
~~~~~~~~~~~~~~~~~~~~~~

Create a new Python file (e.g., ``run_data_curator.py``) in your project directory. The script must:

- Import the `kaxanuk.data_curator` package.
- Construct a `Configuration` object (from ``kaxanuk.data_curator.entities``) that mirrors the settings you would normally set in ``data_curator_parameters.xlsx``.
- Instantiate one or more data provider classes (implementing ``DataProviderInterface``).
- Instantiate the output handler(s) you wish to use (e.g., CSV or Parquet).
- Call the `kaxanuk.data_curator.main(...)` function with the assembled arguments.

Below is a minimal example:

.. code-block:: python

   import datetime
   import logging

   import kaxanuk.data_curator
   from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
   from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
   from kaxanuk.data_curator.data_providers import FinancialModelingPrep
   from kaxanuk.data_curator.entities import Configuration
   from kaxanuk.data_curator.output_handlers import ParquetOutput

   # 1. Build a Configuration object
   configuration = Configuration(
       start_date=datetime.date(2024, 1, 1),
       end_date=datetime.date(2024, 12, 31),
       period="quarterly",
       identifiers=("AAPL", "MSFT"),
       columns=("m_date", "m_close", "fis_revenues"),
   )

   # 2. Instantiate a data provider, which can supply several data blocks
   data_provider = FinancialModelingPrep(api_key="YOUR_FMP_API_KEY")

   # 3. Instantiate an output handler
   output_handler = ParquetOutput(output_base_dir="./Output")

   # 4. Call the main function, mapping each data block to its data provider
   kaxanuk.data_curator.main(
       configuration=configuration,
       data_block_providers={
           MarketDailyDataBlock: data_provider,
           FundamentalsDataBlock: data_provider,
       },
       output_handlers=[output_handler],
       custom_calculation_modules=None,     # or a list of imported Python modules
       logger_level=logging.INFO,
       logger_format="%(asctime)s %(levelname)s: %(message)s",
   )

Save the script and run it:

.. code-block:: bash

   python run_data_curator.py

Data Curator will execute according to the supplied `Configuration`, fetch data from the specified providers, apply any predefined or custom calculations, and write output files into the ``Output/`` directory.

Implement a Custom Data Provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need to integrate a data source not supported out-of-the-box, create a class that implements the data provider interface. Your class must inherit from:

``kaxanuk.data_curator.data_providers.DataProviderInterface``

and return the data of each data block it supports (e.g., market data, fundamentals, dividends, splits, etc.) from ``get_data_block_data``.

Below is a skeleton example illustrating the required structure:

.. code-block:: python

   from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
   from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
   from kaxanuk.data_curator.data_providers import DataProviderInterface
   from kaxanuk.data_curator.entities import (
       BaseDataEntity,
       Configuration,
   )

   class MyCustomProvider(DataProviderInterface):
       """
       Example custom provider that fetches data from a hypothetical REST API.
       """

       def __init__(self, api_key: str):
           self.api_key = api_key
           # Initialize any HTTP clients, base URLs, etc.

       @classmethod
       def get_data_block_endpoint_tag_map(cls):
           """
           Map each data block this provider supplies to the tags of its endpoint fields (empty if unused).
           """
           return {
               MarketDailyDataBlock: {},
           }

       def get_data_block_data(
           self,
           *,
           main_identifier: str,
           data_block: type[BaseDataBlock],
           configuration: Configuration,
       ) -> BaseDataEntity:
           """
           Fetch the data block's data for main_identifier between configuration.start_date and end_date.
           Must return the data block's main entity (e.g., MarketData for MarketDailyDataBlock).
           """
           # 1. Build API request URL
           # 2. Issue request and parse JSON or CSV
           # 3. Construct the entities, which validate their own fields
           raise NotImplementedError

       def initialize(self, *, configuration: Configuration) -> None:
           """
           Run any setup logic before processing the identifiers, like bulk downloads.
           """

       def validate_api_key(self) -> bool | None:
           """
           Return whether the API key is valid, or None if the provider doesn't use API keys.
           """
           return None

   # Example usage in your entry script:
   # custom_provider = MyCustomProvider(api_key="YOUR_API_KEY")
   # kaxanuk.data_curator.main(..., data_block_providers={MarketDailyDataBlock: custom_provider}, ...)

Implement a Custom Data Block
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A “data block” in Data Curator packages a specific type of data (e.g., market data, dividends, fundamentals) with the entities holding it and the prefixes of its columns (e.g., ``m_``). If you want to customize a data block—such as adding metrics from a third-party API—you need to:

- Create its entity classes, as frozen dataclasses inheriting from ``kaxanuk.data_curator.entities.BaseDataEntity``.
- Create a new class inheriting from ``kaxanuk.data_curator.data_blocks.base_data_block.BaseDataBlock``.

The minimal steps are:

1. Declare the entities, whose row fields become columns under their prefix (e.g., ``m_short_interest``):

   .. code-block:: python

      import dataclasses
      import datetime
      import decimal

      from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
      from kaxanuk.data_curator.entities import BaseDataEntity, MainIdentifier

      @dataclasses.dataclass(frozen=True, slots=True)
      class MyMarketDataRow(BaseDataEntity):
          date: datetime.date
          close: decimal.Decimal | None
          short_interest: decimal.Decimal | None

      @dataclasses.dataclass(frozen=True, slots=True)
      class MyMarketData(BaseDataEntity):
          main_identifier: MainIdentifier
          rows: dict[str, MyMarketDataRow]     # keyed by their 'YYYY-MM-DD' date

2. Declare the data block, mapping each column prefix to its entity:

   .. code-block:: python

      class MyMarketDailyDataBlock(BaseDataBlock):
          clock_sync_field = MyMarketDataRow.date
          grouping_identifier_field = MyMarketData.main_identifier
          main_entity = MyMarketData
          prefix_entity_map = {
              'm': MyMarketDataRow,
          }

3. Return its ``main_entity`` from your provider's ``get_data_block_data``, add it to its ``get_data_block_endpoint_tag_map``, and pass it as a ``data_block_providers`` key to ``main``.

A data block shadows any built-in one sharing its prefixes, so this one replaces ``MarketDailyDataBlock`` for the whole run, while one with new prefixes (e.g., ``alt``) adds its columns alongside the built-in ones.

Troubleshooting & Tips
~~~~~~~~~~~~~~~~~~~~~~

- **Missing Methods in Your Custom Provider?**
  Ensure your class inherits from `DataProviderInterface` and that you have implemented all abstract methods. A missing method will cause a runtime error.
- **Entity Validation Errors?**
  The built-in entities validate their fields on creation, so check your provider fills them with the expected types (e.g., ``datetime.date`` dates, ``decimal.Decimal`` prices). Custom entities only validate what their own ``__post_init__`` checks.
- **Logger Configuration**
  By default, `main(...)` uses simple logging to stderr. To write logs to a file, pass `logger_file="path/to/logfile.log"` and adjust `logger_format`.
- **Custom Modules**
  To include additional Python modules for custom calculations, pass a list of the imported modules to the `custom_calculation_modules` parameter in `main(...)`. Those modules must export functions prefixed with `c_`.
- **Runtime Dependencies**
  If your custom provider or block uses external libraries (e.g., `requests`, `numpy`), confirm they are installed in your environment before running the entry script.

Next Steps
~~~~~~~~~~

- **Explore Built-in Examples**
  Review the Excel entry script template ``templates/data_curator/__main__.py`` in the GitHub repository for patterns you can adapt.
- **Version Control**
  Commit your entry script, any custom provider/block modules, and a minimal requirements file (e.g., ``requirements.txt``) to track your integration logic.
- **Automated Testing**
  Write unit tests for your custom provider and block classes (e.g., using `pytest`) to ensure they return correctly populated entities even when data is missing or malformed.
- **Deploy in a Container**
  If you later decide to use a containerized workflow, mount your project directory to `/app` with your entry script saved as `__main__.py`, which the container runs on each start, importing your custom modules from it.

See also
--------

- :ref:`Zero-Coder Workflow <zero_coder>` for end-user installation and usage.
- :ref:`Custom Calculator Workflow <custom_calculator>` for adding Python-based features.
- :ref:`Developer/Tester Workflow <developer_tester>` for contributing code and running tests.
