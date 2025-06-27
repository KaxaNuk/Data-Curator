.. _custom_calculator:

Custom Calculator Workflow
==========================

In “custom calculator” mode, you start from a Zero-Coder installation of Data Curator (i.e., you have already installed Data Curator, run ``kaxanuk.data_curator init excel``, and populated ``Config/parameters_datacurator.xlsx`` as described in the Zero-Coder guide). Then, in addition to configuring providers, dates, tickers, and default output columns via Excel, you add one or more Python functions that generate extra columns on a per-row basis. Follow these steps to install (if you haven’t already), configure, and run Data Curator with your own calculations.

Prerequisites (Zero-Coder Setup)
--------------------------------

Before adding custom calculations, ensure you have completed the Zero-Coder steps.

Create a Python 3.12 Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use Conda or ``venv`` to isolate Data Curator’s dependencies.

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

This installs Data Curator along with its dependencies (e.g., ``openpyxl``, ``pandas``, ``pyarrow``, ``pandas_ta``, etc.).

Initialize the Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Configure Data Curator (Zero-Coder Settings)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Open ``Config/parameters_datacurator.xlsx`` and fill in the worksheets as follows:

- **Providers**
  - ``market_data_provider``: select a market-data vendor.
  - ``market_data_api_key``: enter its API key here (or leave blank to use ``.env``).
  - ``fundamental_data_provider``: select a fundamental-data vendor.
  - ``fundamental_data_api_key``: enter its API key here (or leave blank to use ``.env``).

- **Date Range**
  - ``start_date`` (YYYY-MM-DD): first date of data fetch.
  - ``end_date`` (YYYY-MM-DD): last date of data fetch.
  - ``period``: frequency (e.g., ``1d``, ``1w``, ``1m``).

- **Instruments**
  - List ticker symbols (one per row), e.g., ``AAPL``, ``MSFT``.

- **Output Settings**
  - ``output_format``: choose between ``csv`` or ``parquet``.
  - ``logger_level``: e.g., ``INFO``, ``DEBUG``.

- **Columns/Calculations**
  - Tick the raw data columns you want (e.g., ``open``, ``close``, ``volume``).
  - Under **Predefined Calculations**, tick any built-in features (e.g., “Simple Moving Average 5d”).
  - Under **Custom Calculations**, list any function names defined in ``Config/custom_calculations.py`` (each prefixed with ``c_``).

If you left any API keys blank in Excel, create or edit ``Config/.env``:

.. code-block:: text

   KNDC_API_KEY_MARKET_DATA=<your_market_data_api_key>
   KNDC_API_KEY_FUNDAMENTAL_DATA=<your_fundamental_data_api_key>

After saving ``parameters_datacurator.xlsx`` and (if needed) ``.env``, you can run:

.. code-block:: bash

   python /path/to/data_curator_project

to verify that Data Curator fetches default data and writes output into ``Output/``.

Create Your Custom Calculation Function
---------------------------------------

Data Curator looks for any Python function in ``Config/custom_calculations.py`` whose name begins with ``c_``. Each such function is applied row-wise over the assembled dataset once the raw market/fundamental data has been collected. A custom function should:

- Be defined in ``Config/custom_calculations.py``.
- Take as positional arguments the column names (as Pandas Series) needed for the computation.
- Return a Pandas Series of the same length, with ``None`` or ``NaN`` in rows where inputs are missing or the operation is undefined.

Locate the Custom Calculations File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In your project directory, open:

- ``Config/custom_calculations.py``

This file already contains template functions and import statements. At the top you’ll see helper imports such as:

.. code-block:: python

   import pandas as pd
   from datetime import datetime
   from kaxanuk.data_curator.features.helpers import (
       cumulative_return,
       log_return,
       ...
   )

Define a New Function
~~~~~~~~~~~~~~~~~~~~~

Choose a clear, snake_case name prefixed with ``c_``. For example, to compute a 10-day price difference, you might write:

.. code-block:: python

   def c_price_difference_10d(m_close: pd.Series) -> pd.Series:
       """
       Returns the difference between the close price and its value 10 trading days ago.
       Leaves first 10 rows as NaN.
       """
       # Use Pandas to shift by 10 rows
       return m_close - m_close.shift(10)

If you need multiple input columns, add them as separate parameters. For example:

.. code-block:: python

   def c_return_over_volume(m_close: pd.Series, m_volume: pd.Series) -> pd.Series:
       """
       Returns the ratio of daily log returns to volume.
       Rows with zero or missing volume will be NaN.
       """
       # Compute the log return using a helper
       log_ret = log_return(m_close)
       # Avoid division by zero
       return log_ret.where(m_volume != 0, None) / m_volume

Save your changes. Any function name not prefixed with ``c_`` will be ignored.

Best Practices for Custom Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Use only Pandas operations or existing helper functions for performance and consistency.
- Handle missing data explicitly (e.g., avoid dividing by zero; propagate ``NaN`` where appropriate).
- Document your function with a short docstring explaining inputs, outputs, and any edge-case behavior.
- If you import new libraries (e.g., ``numpy``), ensure they are already installed in your environment.

Add Your Custom Calculation to the Excel File
---------------------------------------------

After defining one or more functions in ``Config/custom_calculations.py``, you must tell Data Curator to include them in the output.

Open ``Config/parameters_datacurator.xlsx``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Switch to the **Columns/Calculations** worksheet.
2. Under the **Custom Calculations** section, add each function name (including the ``c_`` prefix) on its own row.
   For example, if your function is:

   .. code-block:: text

      def c_price_difference_10d(m_close: pd.Series) -> pd.Series: …

   then enter:

   .. code-block:: text

      c_price_difference_10d

Verify the Naming
~~~~~~~~~~~~~~~~~

- The Excel entry must exactly match the function name in ``custom_calculations.py``.
- Do **not** include parentheses or arguments—only the bare function name.

Save the Workbook
~~~~~~~~~~~~~~~~~

Once you’ve added all desired custom-calculation names, save ``parameters_datacurator.xlsx``.
If you are editing on macOS and don’t see hidden files (e.g., ``.env``), press **Command+Shift+Period** in Finder dialogs to reveal them.

Run Data Curator with Custom Calculations
-----------------------------------------

With both ``Config/custom_calculations.py`` and ``Config/parameters_datacurator.xlsx`` updated, run:

.. code-block:: bash

   python /path/to/data_curator_project

What happens under the hood:

- Data Curator loads all raw data providers and writes default columns into memory.
- It then imports ``Config/custom_calculations.py`` and looks for any functions whose names start with ``c_``.
- For each such function, it calls the function with the specified input columns (as Pandas Series).
- The returned Series is appended as a new column in the in-memory DataFrame.
- Finally, Data Curator writes one output file per ticker under ``Output/``, with separate sheets (or sections) for:
  - **Market data**
  - **Fundamental data**
  - **Dividends** (if enabled)
  - **Splits** (if enabled)
  - **Calculations** (including your custom columns prefixed ``c_``)

Troubleshooting & Tips
----------------------

**No output for your custom column?**
- Verify there are no syntax errors in ``custom_calculations.py``.
- Ensure the function name appears under **Custom Calculations** in ``parameters_datacurator.xlsx``.
- Check that the input column names you referenced (e.g., ``m_close``, ``m_volume``) match the raw-data columns exactly.

**Getting many NaNs in your new column?**
- By design, custom calculations propagate ``NaN`` for rows where inputs are missing or invalid.
- Review your logic to see if you need to “forward-fill” or otherwise handle gaps before applying the calculation.

**Want to test a function interactively?**
1. Open a Python REPL (or Jupyter Notebook) in the same virtual environment.
2. Run:

   .. code-block:: python

      import pandas as pd
      # Load a small sample of raw data to a DataFrame
      df = pd.read_parquet("Output/AAPL_Market_and_Fundamental_Data.parquet", engine="pyarrow")
      from Config.custom_calculations import c_price_difference_10d
      # Apply it to the 'm_close' column
      sample = c_price_difference_10d(df["m_close"])
      print(sample.head())

**Reordering or renaming columns**
If you need to change the column order or rename your custom columns, do so in the **Output Settings** section of the Excel file before rerunning.

(Optional) Containerized Workflow
---------------------------------

If you prefer using containers (Podman/Docker) instead of installing locally, follow these steps once you’ve added your custom functions.

Pull and Run the Data Curator Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See the Zero-Coder Container Setup under “Pull the Data Curator Image” and “Run the Container for the First Time.” Ensure your host directory (containing ``Config/`` and ``Output/``) is mounted at ``/app`` inside the container.

Edit Custom Calculations Inside the Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In ``Config/custom_calculations.py``, create or modify your functions as described above.
2. Update ``Config/parameters_datacurator.xlsx`` to reference your new ``c_``-functions.

Start the Container
~~~~~~~~~~~~~~~~~~~

In Podman Desktop or via the CLI:

.. code-block:: bash

   podman start data-curator

The container will read the updated configuration and write output (including your custom columns) into the host’s ``Output/`` folder.

Next Steps
----------

- **Organize Multiple Custom Functions**
  If you plan to maintain many custom calculations, group related helpers into separate Python modules under ``Config/`` and import them from ``custom_calculations.py``.

- **Version Control**
  Commit both ``custom_calculations.py`` and ``parameters_datacurator.xlsx`` into your git repository to track changes to your custom logic.

- **Automated Testing**
  Write small unit tests for your custom functions (e.g., using ``pytest``) to ensure they behave as expected when inputs have gaps or extreme values.

See also
--------

- :ref:`Zero-Coder Workflow <zero_coder>` for end-user installation and usage.
- :ref:`Component Integrator Workflow <component_integrator>` for programmatic integration.
- :ref:`Developer/Tester Workflow <developer_tester>` for contributing code and running tests.
