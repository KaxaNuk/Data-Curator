.. _component_integrator:

Component Integrator Workflow
=============================

In “component integrator” mode, you bypass the Excel-driven Zero-Coder interface and instead write a Python script that constructs a `Configuration` object, selects or implements data providers and output handlers, and invokes the core Data Curator functionality programmatically. This mode assumes you have Python programming experience and a Direct Setup (non-container) environment. Follow these steps to install the component library, create an entry script, and (if needed) implement custom data providers or data blocks.

Install the Data Curator Component Library
------------------------------------------

Use Pip to install the library from PyPI:

.. code-block:: bash

   pip install --upgrade kaxanuk.data_curator

This command installs `kaxanuk.data_curator` along with all its dependencies (e.g., `openpyxl`, `pandas`, `pyarrow`, `pandas_ta`, etc.). :contentReference[oaicite:0]{index=0}

Create an Entry Script
~~~~~~~~~~~~~~~~~~~~~~

Create a new Python file (e.g., ``run_data_curator.py``) in your project directory. The script must:

- Import the `kaxanuk.data_curator` package.
- Construct a `Configuration` object (from ``kaxanuk.data_curator.entities``) that mirrors the settings you would normally set in ``parameters_datacurator.xlsx``.
- Instantiate one or more data provider classes (implementing the appropriate interfaces).
- Instantiate the output handler(s) you wish to use (e.g., CSV or Parquet).
- Call the `kaxanuk.data_curator.main(...)` function with the assembled arguments.

Below is a minimal example:

.. code-block:: python

   from kaxanuk.data_curator.entities import Configuration
   from kaxanuk.data_curator.data_providers import AlphaVantageProvider
   from kaxanuk.data_curator.output_handlers import ParquetOutputHandler
   from kaxanuk.data_curator import main as run_data_curator

   # 1. Build a Configuration object
   config = Configuration(
       market_data_provider_name="AlphaVantage",
       market_data_api_key="YOUR_ALPHA_VANTAGE_KEY",
       fundamental_data_provider_name="AlphaVantage",
       fundamental_data_api_key="YOUR_ALPHA_VANTAGE_KEY",
       start_date="2024-01-01",
       end_date="2024-12-31",
       period="1d",
       instruments=["AAPL", "MSFT"],
       output_format="parquet",
       logger_level="INFO"
   )

   # 2. Instantiate data providers
   #    (Alternatively, pass provider names in Configuration and let Data Curator instantiate)
   market_dp = AlphaVantageProvider(api_key=config.market_data_api_key)
   fundamental_dp = AlphaVantageProvider(api_key=config.fundamental_data_api_key)

   # 3. Instantiate an output handler
   output_handler = ParquetOutputHandler(output_dir="./Output")

   # 4. Call the main function
   run_data_curator(
       configuration=config,
       market_data_provider=market_dp,
       fundamental_data_provider=fundamental_dp,
       output_handlers=[output_handler],
       custom_calculation_modules=None,     # or a list of Python module names
       logger_level=config.logger_level,
       logger_format="%(asctime)s %(levelname)s: %(message)s"
   )

Save the script and run it:

.. code-block:: bash

   python run_data_curator.py

Data Curator will execute according to the supplied `Configuration`, fetch data from the specified providers, apply any predefined or custom calculations, and write output files into the ``Output/`` directory. :contentReference[oaicite:1]{index=1}

Implement a Custom Data Provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need to integrate a data source not supported out-of-the-box, create a class that implements the financial data provider protocol. Your class must inherit from:

``kaxanuk.data_curator.data_providers.FinancialDataProviderInterface``

and implement all required methods for each data block it supports (e.g., market data, fundamentals, dividends, splits, etc.). :contentReference[oaicite:2]{index=2}

Below is a skeleton example illustrating the required structure:

.. code-block:: python

   from datetime import date
   from typing import List, Dict
   from pandas import DataFrame
   from kaxanuk.data_curator.data_providers import FinancialDataProviderInterface

   class MyCustomProvider(FinancialDataProviderInterface):
       """
       Example custom provider that fetches data from a hypothetical REST API.
       """

       def __init__(self, api_key: str):
           self.api_key = api_key
           # Initialize any HTTP clients, base URLs, etc.

       def get_market_data(self, instrument: str, start_date: date, end_date: date, period: str) -> DataFrame:
           """
           Fetch time series market data for the given ticker between start_date and end_date.
           Must return a Pandas DataFrame with columns: 'date', 'open', 'high', 'low', 'close', 'volume', etc.
           """
           # 1. Build API request URL
           # 2. Issue request and parse JSON or CSV
           # 3. Construct a DataFrame with the required schema
           # 4. Return the DataFrame (indexed or column-based, as per Data Curator’s expectations)
           raise NotImplementedError

       def get_fundamental_data(self, instrument: str, start_date: date, end_date: date) -> DataFrame:
           """
           Fetch fundamental (income statement, balance sheet) data.
           Must return a DataFrame with one row per reporting period and columns like 'total_revenue', 'net_income', etc.
           """
           raise NotImplementedError

       def get_dividends(self, instrument: str, start_date: date, end_date: date) -> DataFrame:
           """
           Optionally fetch dividend history. Must return a DataFrame with columns: 'date', 'dividend_amount'.
           """
           return DataFrame(columns=["date", "dividend_amount"])

       def get_splits(self, instrument: str, start_date: date, end_date: date) -> DataFrame:
           """
           Optionally fetch stock split history. Must return a DataFrame with columns: 'date', 'split_ratio'.
           """
           return DataFrame(columns=["date", "split_ratio"])

       # If your provider offers additional alternative data (e.g., earnings transcripts, news),
       # implement those methods as documented in the interface.

   # Example usage in your entry script:
   # from run_data_curator import config
   # custom_provider = MyCustomProvider(api_key=config.market_data_api_key)
   # run_data_curator(..., market_data_provider=custom_provider, fundamental_data_provider=custom_provider, ...)

Implement a Custom Data Block
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A “data block” in Data Curator refers to a specific type of data (e.g., market data, dividends, fundamentals). If you want to define a new data block—such as alternative metrics from a third-party API—you need to:

- Create a new class that implements the corresponding Data Block interface (e.g., ``MarketDataBlockInterface``, ``FundamentalDataBlockInterface``) found under ``kaxanuk.data_curator.data_providers`` or ``kaxanuk.data_curator.features``.
- Register your custom block within the provider or via a plugin mechanism.

The minimal steps are:

1. Identify the interface for the block you wish to implement. For example, if you want a new “alternative factors” block, find or define:

   ``kaxanuk.data_curator.data_providers.AlternativeDataBlockInterface``
   (or create a new protocol in ``data_providers`` and add it to the provider registration logic).

2. Inherit from that interface and implement the required methods:

   .. code-block:: python

      from pandas import DataFrame
      from kaxanuk.data_curator.data_providers import AlternativeDataBlockInterface

      class MyAlternativeBlock(AlternativeDataBlockInterface):
          """
          Provides alternative financial metrics not covered by standard blocks.
          """

          def fetch(self, instrument: str, start_date, end_date) -> DataFrame:
              """
              Must return a DataFrame with an index or column labelled 'date' and one column per metric.
              """
              # 1. Call your data source
              # 2. Parse results into a DataFrame indexed by date
              # 3. Return the DataFrame
              raise NotImplementedError

   3. Modify your custom provider so that it returns an instance of your new block when Data Curator requests it. Typically, providers expose a method like:

      ``get_alternative_data_block(self, instrument, start_date, end_date) -> DataFrame``

      Ensure Data Curator’s registry (or your entry script) knows to use your block.

Once your custom block class is correctly implemented and registered, Data Curator will include those columns in the final output under a new “Alternative Data” section for each instrument.

Troubleshooting & Tips
~~~~~~~~~~~~~~~~~~~~~~

- **Missing Methods in Your Custom Provider?**
  Ensure your class inherits exactly from `FinancialDataProviderInterface` and that you have implemented all abstract methods. A missing method will cause a runtime error. :contentReference[oaicite:3]{index=3}
- **DataFrame Schema Mismatch?**
  Verify that DataFrame columns match expected names (e.g., “date”, “open”, “close”, “total_revenue”, etc.). Misnamed columns will be dropped or cause errors during merging.
- **Logger Configuration**
  By default, `main(...)` uses simple logging to stdout. To write logs to a file, pass `logger_file="path/to/logfile.log"` and adjust `logger_format`.
- **Custom Modules**
  To include additional Python modules for custom calculations, pass a list of module paths (as strings) to the `custom_calculation_modules` parameter in `main(...)`. Those modules must export functions prefixed with `c_`.
- **Runtime Dependencies**
  If your custom provider or block uses external libraries (e.g., `requests`, `numpy`), confirm they are installed in your environment before running the entry script.

Next Steps
~~~~~~~~~~

- **Explore Built-in Examples**
  Review the example entry scripts under the “examples” directory in the GitHub repository for patterns you can adapt. :contentReference[oaicite:4]{index=4}
- **Version Control**
  Commit your entry script, any custom provider/block modules, and a minimal requirements file (e.g., ``requirements.txt``) to track your integration logic.
- **Automated Testing**
  Write unit tests for your custom provider and block classes (e.g., using `pytest`) to ensure they return correctly formatted DataFrames even when data is missing or malformed.
- **Deploy in a Container**
  If you later decide to use a containerized workflow, package your custom modules into a directory that can be mounted to `/app/Config` or adjust container `PYTHONPATH` so that Data Curator recognizes your components at runtime.

See also
--------

- :ref:`Zero-Coder Workflow <zero_coder>` for end-user installation and usage.
- :ref:`Custom Calculator Workflow <custom_calculator>` for adding Python-based features.
- :ref:`Developer/Tester Workflow <developer_tester>` for contributing code and running tests.
