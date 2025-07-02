.. _zero_coder:

Zero-Coder Workflow
====================

In “zero-coder” mode, everything is configured via an Excel workbook (`Config/parameters_datacurator.xlsx`) and an optional `.env` file—no Python coding is required. Follow these steps to install, configure, and run Data Curator.

Direct Setup (No Containers)
----------------------------

Create a Python 3.12 Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use Conda or `venv` to isolate Data Curator’s dependencies.

**Conda example (Windows/macOS/Linux):**

.. code-block:: bash

   conda create --name datacurator_env python=3.12
   conda activate datacurator_env

**venv example:**

.. code-block:: bash

   python3.12 -m venv datacurator_env
   source datacurator_env/bin/activate    # macOS/Linux
   datacurator_env\Scripts\activate.bat   # Windows


Install Data Curator via pip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the virtual environment active, run:

.. code-block:: bash

   pip install --upgrade kaxanuk.data_curator

This installs Data Curator plus all dependencies specified in `pyproject.toml` (e.g., `openpyxl`, `pandas`, `pyarrow`, `pandas_ta`, etc.).


Initialize the Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Choose or create a project directory and move into it:

.. code-block:: bash

   mkdir ~/data_curator_project
   cd ~/data_curator_project

Run the initializer:

.. code-block:: bash

   kaxanuk.data_curator init excel

After this command, your directory will contain:

- ``__main__.py``
- ``Config/`` (empty configuration folder)
- ``Output/`` (empty output folder)


Configure Data Curator
----------------------

Edit the Excel workbook and, if needed, the `.env` file as follows.


Edit ``Config/parameters_datacurator.xlsx``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Open the Excel workbook and fill in these worksheets:

- **Providers**
  - ``market_data_provider``: pick a market-data vendor (e.g., “Financial Modeling Prep”, “AlphaVantage”).
  - ``market_data_api_key``: enter its API key here, or leave blank to use `.env`.
  - ``fundamental_data_provider``: pick a fundamental-data vendor.
  - ``fundamental_data_api_key``: enter its API key here, or leave blank to use `.env`.

- **Date Range**
  - ``start_date`` (YYYY-MM-DD): first date of data fetch.
  - ``end_date`` (YYYY-MM-DD): last date of data fetch.
  - ``period``: frequency (e.g., ``1d`` for daily, ``1w`` for weekly, ``1m`` for monthly).

- **Instruments**
  - List ticker symbols (one per row), e.g., ``AAPL``, ``MSFT``.

- **Output Settings**
  - ``output_format``: choose between ``csv`` or ``parquet``.
  - ``logger_level``: log verbosity (e.g., ``INFO``, ``DEBUG``).

- **Columns/Calculations**
  - Tick raw data columns you want (e.g., ``open``, ``close``, ``volume``).
  - Under **Predefined Calculations**, tick any built-in features (e.g., “Simple Moving Average 5d”, “Annualized Volatility 21d”).
  - Under **Custom Calculations**, list any function names defined in ``Config/custom_calculations.py`` (prefix with ``c_``).


Edit ``Config/.env``
~~~~~~~~~~~~~~~~~~~~~~

If you left API keys blank in Excel, create or open ``Config/.env`` and add:

.. code-block:: text

   KNDC_API_KEY_MARKET_DATA=<your_market_data_api_key>
   KNDC_API_KEY_FUNDAMENTAL_DATA=<your_fundamental_data_api_key>

- On macOS, press **Command+Shift+Period** in Finder dialogs to reveal hidden files when editing.


Run Data Curator
----------------

After saving ``parameters_datacurator.xlsx`` and (if needed) `.env`, run:

.. code-block:: bash

   python /path/to/data_curator_project

Replace ``/path/to/data_curator_project`` with the directory containing ``__main__.py``. Data Curator will:

- Load ``parameters_datacurator.xlsx`` and read your settings.
- Read any API keys from `.env`.
- Fetch market, fundamental, and optional alternative data (e.g., transcripts, news, economic).
- Apply predefined and custom calculations.
- Write one file per ticker into ``Output/``, named:

  ::

     <TICKER>_Market_and_Fundamental_Data.<csv|parquet>

  Each output file contains separate sheets/tables for:
  - **Market data** (date, open, high, low, close, volume, etc.)
  - **Fundamental data** (income statement and balance-sheet fields)
  - **Dividends** (if requested)
  - **Splits** (if requested)
  - **Calculations** (columns prefixed with ``c_``)


Container Setup (Recommended)
-----------------------------

Using a container runtime eliminates local dependency issues and guarantees a reproducible environment. This guide uses **Podman Desktop** (open-source and free).


Install Podman Desktop
~~~~~~~~~~~~~~~~~~~~~~

1. Download the installer for your OS:
   https://podman-desktop.io/downloads

2. Run the installer:
   - On **Windows**, ensure “Install WSL if not present” is checked.
   - On **macOS/Linux**, follow on-screen instructions.

3. Launch Podman Desktop; accept defaults if prompted to create a Podman Machine.

4. If Windows complains about WSL version < 1.2.5, open an elevated command prompt and run:

   .. code-block:: powershell

      wsl --update

   Then re-open Podman Desktop.


Pull the Data Curator Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In Podman Desktop’s left menu, click **Images**.
2. Click **Pull** (top-right).
3. Enter the image URI:

   .. code-block:: text

      ghcr.io/kaxanuk-community/data-curator:dev

4. Click **Pull Image**.


Run the Container for the First Time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In **Images**, locate ``data-curator:dev`` and click the triangular **Run** icon.
2. In the **Basic** tab:
   - **Container name**: ``data-curator``

   - **Volumes**:

     - **Host path**: select or create the directory where you want ``Config/`` and ``Output/`` to reside (e.g., ``~/data_curator_project``).
     - **Container path**: ``/app``

   - **Environment variables** (only if you did not set API keys in Excel):
     - ``KNDC_API_KEY_MARKET_DATA=<your_market_data_api_key>``
     - ``KNDC_API_KEY_FUNDAMENTAL_DATA=<your_fundamental_data_api_key>``

3. Leave other fields at defaults, then click **Start container**. Podman will:
   - Create a container named ``data-curator``.
   - Mount your chosen host directory to ``/app`` inside the container.
   - Run ``__main__.py`` once, which initializes Data Curator in Excel mode (equivalent to ``kaxanuk.data_curator init excel``).


Configure Inside the Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After the first run, your host directory contains:

- ``Config/`` ── contains ``parameters_datacurator.xlsx``, ``custom_calculations.py``, and (if present) ``.env``.
- ``Output/`` ── initially empty; output will be written here on subsequent runs.

Edit these files exactly as in **Configure Data Curator**:

- **Config/parameters_datacurator.xlsx**: set providers, API keys, date range, tickers, output format, and calculations.
- **Config/.env**: add API keys if not set in Excel.


Run the Fully Configured Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In Podman Desktop, click **Containers**.
2. Locate ``data-curator`` and click **Start**.
3. The container reads updated configuration and writes output to ``Output/``.


Iterate
~~~~~~~

- Modify ``Config/parameters_datacurator.xlsx`` or ``Config/custom_calculations.py``.
- In Podman Desktop’s **Containers** view, click **Stop** (if running) and then **Start** again.
- The container reruns Data Curator with the new settings, overwriting previous outputs.


Output Structure
----------------

After running (Direct or Container), inspect ``Output/``:

- **``<TICKER>_Market_and_Fundamental_Data.<csv|parquet>``**
  - **Market data**: one row per date with columns ``date``, ``open``, ``high``, ``low``, ``close``, ``volume``, ``adjusted_close``, etc.
  - **Fundamental data**: income statement and balance-sheet fields (e.g., ``total_revenue``, ``net_income``, ``total_assets``).
  - **Dividends**: ``date``, ``dividend_amount`` (if enabled).
  - **Splits**: ``date``, ``split_ratio`` (if enabled).
  - **Calculations**: columns prefixed with ``c_`` (e.g., ``c_simple_moving_average_5d``, ``c_log_returns_adjusted_close``).

- **``Earnings_Transcripts/``** (optional)
  If earnings transcripts were enabled, JSON/text files appear here.

- **``News/``** (optional)
  If news data was enabled, files per ticker or aggregate news feed appear here.

- **``Economic_Data/``** (optional)
  Contains macroeconomic series (e.g., GDP, CPI) if enabled.


See also
--------

- :ref:`Custom Calculator Workflow <custom_calculator>` for adding Python-based features.
- :ref:`Component Integrator Workflow <component_integrator>` for programmatic integration.
- :ref:`Developer/Tester Workflow <developer_tester>` for contributing code and running tests.
