.. _zero_coder:

Zero-Coder Workflow
====================

In “zero-coder” mode, everything is configured via an Excel workbook (`Config/data_curator_parameters.xlsx`) and an optional `.env` file—no Python coding is required. Follow these steps to install, configure, and run Data Curator.

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

This installs Data Curator plus all dependencies specified in `pyproject.toml` (e.g., `openpyxl`, `pandas`, `pyarrow`, etc.).


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
- ``Config/`` (configuration files)
- ``Output/`` (empty output folder)


Configure Data Curator
----------------------

Edit the Excel workbook and, if needed, the `.env` file as follows.


Edit ``Config/data_curator_parameters.xlsx``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Open the Excel workbook and fill in these worksheets:

- **General**
  - ``start_date`` / ``end_date``: first and last date of data fetch.
  - ``period``: fundamental data period, ``quarterly`` or ``annual``.
  - ``output_format``: choose between ``csv`` or ``parquet``.
  - ``logger_level``: log verbosity (e.g., ``info``, ``debug``).

- **Data_Providers**
  - Pick the data provider of each data block (e.g., ``FinancialModelingPrep``), or ``None`` to skip an optional one.

- **Identifiers**
  - List ticker symbols (one per row) under ``main_identifier``, e.g., ``AAPL``, ``MSFT``.

- **Output_Columns**
  - List the columns you want (one per row) under ``columns``: raw data columns (e.g., ``m_open``, ``m_close``), built-in calculations (e.g., ``c_daily_traded_value``), and any functions defined in ``Config/custom_calculations.py`` (prefixed with ``c_``).


Edit ``Config/.env``
~~~~~~~~~~~~~~~~~~~~~~

If your data providers require API keys, open ``Config/.env`` and set them in their variables:

.. code-block:: text

   KNDC_API_KEY_FMP=<your_financial_modeling_prep_api_key>
   KNDC_API_KEY_LSEG=<your_lseg_workspace_api_key>
   KNDC_API_KEY_SHARADAR=<your_sharadar_api_key>

- On macOS, press **Command+Shift+Period** in Finder dialogs to reveal hidden files when editing.


Run Data Curator
----------------

After saving ``data_curator_parameters.xlsx`` and (if needed) `.env`, run from inside the project directory:

.. code-block:: bash

   kaxanuk.data_curator run

Data Curator will:

- Load ``data_curator_parameters.xlsx`` and read your settings.
- Read any API keys from `.env`.
- Fetch each data block's data from its selected provider.
- Apply predefined and custom calculations.
- Write one file per ticker into ``Output/`` (see **Output Structure** below).


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

      ghcr.io/kaxanuk/data-curator:latest

4. Click **Pull Image**.


Run the Container for the First Time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In **Images**, locate ``data-curator:latest`` and click the triangular **Run** icon.
2. In the **Basic** tab:
   - **Container name**: ``data-curator``

   - **Volumes**:

     - **Host path**: select or create the directory where you want ``Config/`` and ``Output/`` to reside (e.g., ``~/data_curator_project``).
     - **Container path**: ``/app``

   - **Environment variables** (only for providers requiring API keys):
     - ``KNDC_API_KEY_FMP``, ``KNDC_API_KEY_LSEG``, or ``KNDC_API_KEY_SHARADAR``, set to your API key

3. Leave other fields at defaults, then click **Start container**. Podman will:
   - Create a container named ``data-curator``.
   - Mount your chosen host directory to ``/app`` inside the container.
   - Run ``kaxanuk.data_curator autorun``, which creates ``__main__.py``, ``Config/`` and ``Output/`` (like ``kaxanuk.data_curator init excel``) and exits, as there's nothing to run yet.


Configure Inside the Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After the first run, your host directory contains:

- ``__main__.py`` ── the entry script, run on each later start.
- ``Config/`` ── contains ``data_curator_parameters.xlsx``, ``custom_calculations.py``, and ``.env``.
- ``Output/`` ── initially empty; output will be written here on subsequent runs.

Edit these files exactly as in **Configure Data Curator**:

- **Config/data_curator_parameters.xlsx**: set date range, providers, tickers, output format, and columns.
- **Config/.env**: add your providers' API keys.


Run the Fully Configured Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In Podman Desktop, click **Containers**.
2. Locate ``data-curator`` and click **Start**.
3. The container reads updated configuration and writes output to ``Output/``.


Iterate
~~~~~~~

- Modify ``Config/data_curator_parameters.xlsx`` or ``Config/custom_calculations.py``.
- In Podman Desktop’s **Containers** view, click **Stop** (if running) and then **Start** again.
- The container reruns Data Curator with the new settings, overwriting previous outputs.


Output Structure
----------------

After running (Direct or Container), inspect ``Output/``:

- ``<TICKER>.<csv|parquet>``: one row per market date, with the columns of the **Output_Columns** sheet, each prefixed by its data type:
  - ``m_``: market data (e.g., ``m_date``, ``m_close``).
  - ``f_``, ``fis_``, ``fbs_``, ``fcf_``: fundamental data (e.g., ``fis_revenues``).
  - ``d_`` / ``s_``: dividends / splits.
  - ``c_``: calculations (e.g., ``c_daily_traded_value``).


See also
--------

- :ref:`Custom Calculator Workflow <custom_calculator>` for adding Python-based features.
- :ref:`Component Integrator Workflow <component_integrator>` for programmatic integration.
- :ref:`Developer/Tester Workflow <developer_tester>` for contributing code and running tests.
