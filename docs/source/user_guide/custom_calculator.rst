.. _custom_calculator:

Custom Calculator Workflow
==========================

In “custom calculator” mode, you start from a Zero-Coder installation of Data Curator (i.e., you have already installed Data Curator, run ``kaxanuk.data_curator init excel``, and populated ``Config/data_curator_parameters.xlsx`` as described in the Zero-Coder guide). Then, in addition to configuring providers, dates, tickers, and default output columns via Excel, you add one or more Python functions that generate extra columns on a per-row basis. Follow these steps to install (if you haven’t already), configure, and run Data Curator with your own calculations.

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

This installs Data Curator along with its dependencies (e.g., ``openpyxl``, ``pandas``, ``pyarrow``, etc.).

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
- ``Config/`` (configuration files)
- ``Output/`` (empty output folder)

Configure Data Curator (Zero-Coder Settings)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Open ``Config/data_curator_parameters.xlsx`` and fill in the worksheets as follows:

- **General**
  - ``start_date`` / ``end_date``: first and last date of data fetch.
  - ``period``: fundamental data period, ``quarterly`` or ``annual``.
  - ``output_format``: choose between ``csv`` or ``parquet``.
  - ``logger_level``: e.g., ``info``, ``debug``.

- **Data_Providers**
  - Select the data provider of each data block, or ``None`` to skip an optional one.

- **Identifiers**
  - List ticker symbols (one per row) under ``main_identifier``, e.g., ``AAPL``, ``MSFT``.

- **Output_Columns**
  - List the columns you want (one per row) under ``columns``: raw data columns (e.g., ``m_close``), built-in calculations, and any functions defined in ``Config/custom_calculations.py`` (each prefixed with ``c_``).

If your data providers require API keys, set them in their ``Config/.env`` variables:

.. code-block:: text

   KNDC_API_KEY_FMP=<your_financial_modeling_prep_api_key>
   KNDC_API_KEY_LSEG=<your_lseg_workspace_api_key>
   KNDC_API_KEY_SHARADAR=<your_sharadar_api_key>

After saving ``data_curator_parameters.xlsx`` and (if needed) ``.env``, you can run from inside the project directory:

.. code-block:: bash

   kaxanuk.data_curator run

to verify that Data Curator fetches default data and writes output into ``Output/``.

Create Your Custom Calculation Function
---------------------------------------

Data Curator calls any function in ``Config/custom_calculations.py`` whose name begins with ``c_`` and is listed in the **Output_Columns** sheet, once each ticker's raw data has been collected. A custom function should:

- Be defined in ``Config/custom_calculations.py``.
- Name its parameters after the columns needed for the computation, which it receives as ``DataColumn`` objects.
- Return a column of the same length (e.g., a ``DataColumn``), with nulls in rows where inputs are missing or the operation is undefined.

Locate the Custom Calculations File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In your project directory, open:

- ``Config/custom_calculations.py``

This file already contains a template function and import statement. At the top you’ll see the helpers import:

.. code-block:: python

   # Here you'll find helper functions for calculating more complicated features:
   from kaxanuk.data_curator.features import helpers

Define a New Function
~~~~~~~~~~~~~~~~~~~~~

Choose a clear, snake_case name prefixed with ``c_``. For example, to compute a 10-day simple moving average of the close price, you might write:

.. code-block:: python

   def c_close_sma_10d(m_close):
       """
       Returns the 10-day simple moving average of the close price.
       Leaves the first 9 rows as null.
       """
       # Use a helper to calculate the moving average
       return helpers.simple_moving_average(m_close, 10)

If you need multiple input columns, add them as separate parameters. For example:

.. code-block:: python

   def c_log_returns_over_volume(m_close, m_volume):
       """
       Returns the ratio of daily log returns to volume.
       Rows with zero or missing volume will be null.
       """
       # Compute the log returns using a helper; dividing by zero returns null
       return helpers.log_returns(m_close) / m_volume

Save your changes. Any function name not prefixed with ``c_`` will be ignored.

Best Practices for Custom Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Use only ``DataColumn`` operations or existing helper functions for performance and consistency.
- Handle missing data explicitly (``DataColumn`` operations already return null for any null input).
- Document your function with a short docstring explaining inputs, outputs, and any edge-case behavior.
- If you import new libraries (e.g., ``numpy``), ensure they are already installed in your environment.

Add Your Custom Calculation to the Excel File
---------------------------------------------

After defining one or more functions in ``Config/custom_calculations.py``, you must tell Data Curator to include them in the output.

Open ``Config/data_curator_parameters.xlsx``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Switch to the **Output_Columns** worksheet.
2. Under ``columns``, add each function name (including the ``c_`` prefix) on its own row.
   For example, if your function is:

   .. code-block:: text

      def c_close_sma_10d(m_close): …

   then enter:

   .. code-block:: text

      c_close_sma_10d

Verify the Naming
~~~~~~~~~~~~~~~~~

- The Excel entry must exactly match the function name in ``custom_calculations.py``.
- Do **not** include parentheses or arguments—only the bare function name.

Save the Workbook
~~~~~~~~~~~~~~~~~

Once you’ve added all desired custom-calculation names, save ``data_curator_parameters.xlsx``.
If you are editing on macOS and don’t see hidden files (e.g., ``.env``), press **Command+Shift+Period** in Finder dialogs to reveal them.

Run Data Curator with Custom Calculations
-----------------------------------------

With both ``Config/custom_calculations.py`` and ``Config/data_curator_parameters.xlsx`` updated, run from inside the project directory:

.. code-block:: bash

   kaxanuk.data_curator run

What happens under the hood:

- The entry script imports ``Config/custom_calculations.py`` and passes it to Data Curator.
- Data Curator fetches each ticker's raw data from the selected data providers.
- For each ``c_`` column in **Output_Columns**, it calls its function with the specified input columns (as ``DataColumn`` objects).
- The returned values become a new column, which other functions can also use as input.
- Finally, Data Curator writes one output file per ticker under ``Output/``, with the **Output_Columns** columns, including your custom ``c_`` ones.

Troubleshooting & Tips
----------------------

**No output for your custom column?**
- Verify there are no syntax errors in ``custom_calculations.py``.
- Ensure the function name appears under ``columns`` in the **Output_Columns** sheet of ``data_curator_parameters.xlsx``.
- Check that the input column names you referenced (e.g., ``m_close``, ``m_volume``) match the raw-data columns exactly.

**Getting many nulls in your new column?**
- By design, ``DataColumn`` operations return null for rows where inputs are missing or invalid.
- Review your logic to see if you need to “forward-fill” or otherwise handle gaps before applying the calculation.

**Want to test a function interactively?**
1. Open a Python REPL (or Jupyter Notebook) in the same virtual environment.
2. Run:

   .. code-block:: python

      import pyarrow.parquet
      from kaxanuk.data_curator import DataColumn
      # Load a previous parquet output that includes the input columns
      table = pyarrow.parquet.read_table("Output/AAPL.parquet")
      from Config.custom_calculations import c_close_sma_10d
      # Apply it to the 'm_close' column
      sample = c_close_sma_10d(DataColumn.load(table["m_close"]))
      print(sample.to_pandas().head())

**Reordering or renaming columns**
The output columns follow the order of the **Output_Columns** sheet, so reorder its rows; to rename a custom column, rename its function.

(Optional) Containerized Workflow
---------------------------------

If you prefer using containers (Podman/Docker) instead of installing locally, follow these steps once you’ve added your custom functions.

Pull and Run the Data Curator Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See the Zero-Coder Container Setup under “Pull the Data Curator Image” and “Run the Container for the First Time.” Ensure your host directory (containing ``Config/`` and ``Output/``) is mounted at ``/app`` inside the container.

Edit Custom Calculations Inside the Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In ``Config/custom_calculations.py``, create or modify your functions as described above.
2. Update ``Config/data_curator_parameters.xlsx`` to reference your new ``c_``-functions.

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
  Commit both ``custom_calculations.py`` and ``data_curator_parameters.xlsx`` into your git repository to track changes to your custom logic.

- **Automated Testing**
  Write small unit tests for your custom functions (e.g., using ``pytest``) to ensure they behave as expected when inputs have gaps or extreme values.

See also
--------

- :ref:`Zero-Coder Workflow <zero_coder>` for end-user installation and usage.
- :ref:`Component Integrator Workflow <component_integrator>` for programmatic integration.
- :ref:`Developer/Tester Workflow <developer_tester>` for contributing code and running tests.
