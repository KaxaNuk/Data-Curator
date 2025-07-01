.. _quick_start:

Quick Start
=========================

Installation
------------

The system can run either on your local Python environment or on Docker.

**Requirements for Local Installation**

- Python 3.12 or 3.13

**Installing on Python**

1. Make sure you're running the required version of Python, preferably in its own virtual environment.
2. Open a terminal and run:

   .. code-block:: bash

      pip install kaxanuk.data_curator

3. If you want to use the Yahoo Finance data provider, install the extension package:

   .. code-block:: bash

      pip install kaxanuk.data_curator_extensions.yahoo_finance

4. Set the path where Data Curator should generate its configuration files

    .. code-block:: bash

        cd /path/to/your/datacurator/project

**Excel Configuration**

1. Open a terminal and run:

   .. code-block:: bash

      kaxanuk.data_curator init excel

   This will create two subdirectories: ``Config`` and ``Output``, along with the entry script ``__main__.py``.

2. Edit the ``Config/parameters_datacurator.xlsx`` file to specify your settings.

3. If any provider requires an API key, edit the ``Config/.env`` file and set the key using the variable indicated in the provider documentation. Do not add quotes or extra spaces.

   *On macOS, the `.env` file may be hidden. Use `Cmd + Shift + .` to show hidden files.*

**Usage**

You can run the tool using:

.. code-block:: bash

   kaxanuk.data_curator run

Or by running the main script directly:

.. code-block:: bash

   python __main__.py

The system will pull the data for the configured tickers and save results in the ``Output`` folder.
