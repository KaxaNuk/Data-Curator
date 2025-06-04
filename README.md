# KaxaNuk Data Curator

![Python](https://img.shields.io/badge/python-3.12%20|%203.13-blue?logo=python&logoColor=ffdd54)

[![Build Status](https://github.com/KaxaNuk/Data-Curator/actions/workflows/main.yml/badge.svg)](https://github.com/KaxaNuk/Data-Curator/actions/workflows/main.yml)

Tool for building a structured database for market, fundamental and alternative data obtained
from different financial data provider web services.

Allows for easy creation of additional calculated column functions.

# Requirements
* Python `3.12` or `3.13`
* All the dependency library versions specified in `pyproject.toml` under the `[project].dependencies` section
  (see the installation guide)


# Supported Data Providers
* Financial Modeling Prep
* Yahoo Finance (requires installing a separate extension package, and doesn't support most data types)


# Installation
1. Make sure you're running the required version of Python, preferably in its own virtual environment.
2. Open a terminal and run:
    ```
    pip install --upgrade pip
    pip install kaxanuk.data_curator
    ```

3. If you want to use the Yahoo Finance data provider, install the extension package:
    ```
    pip install kaxanuk.data_curator_extensions.yahoo_finance
    ```


# Configuration
1. Open a terminal in any directory and run the following command:
    ```
    kaxanuk.data_curator init excel
    ```
    This should create 2 subdirectories, `Config` and `Output`, as well as the entry script `__main__.py` in the current directory.
2. Open the `Config/parameters_datacurator.xlsx` file in Excel, fill out the fields in all the sheets, save the file and close it.
3. If your data provider requires an API key, open the `Config/.env` file in a text editor, and paste the key after
    the `=` sign of the provider's corresponding `API_KEY` variable. Don't add any quotes or spaces before or after the key.


# Usage
Now you can run the entry script with either:
    ```
    kaxanuk.data_curator run
    ```
or by executing the `__main__.py` script directly with Python:
    ```
    python __main__.py
    ```
The system will download the data for the tickers configured in the file, and save the data to the `Output` folder.


# Customization
The `__main__.py` entry script is customizable, so you can implement your own data providers and configuration and output
handlers, and inject them from there.

You can also create your own calculated feature functions by adding them to the `Config/custom_calculations.py` file, and
adding the function's name (which start with the `c_` prefix) to the `Columns` sheet in the `Config/parameters_datacurator.xlsx` file.

Check the API Reference to learn how to easily implement your own calculated features.
