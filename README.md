# KaxaNuk Data Curator

![Python](https://img.shields.io/badge/python-3.12-blue?logo=python&logoColor=ffdd54)

[![Build Status](https://github.com/KaxaNuk/Data-Curator/actions/workflows/main.yml/badge.svg)](https://github.com/KaxaNuk/Data-Curator/actions/workflows/main.yml)

Tool for building a structured database for market, fundamental and alternative data obtained
from different financial data provider web services.

Allows for easy creation of additional calculated column functions.

# Requirements 

* Python >= `3.12`
* All the dependency library versions specified in `pyproject.toml` under the `[project].dependencies` section
  (see the installation guide)

# Supported Data Providers 

* Financial Modeling Prep

# Installation 

## Standalone Mode 

On release the library will be on PyPI so it will be really easy to install with `pip install`. To run it currently:

1. Make sure you're running the required version of Python, preferably in its own virtual environment.
2. Download the repository to a folder in your PC.
3. Open a terminal in that folder and run the following commands:
    ```
    pip install --upgrade pip
    pip install pdm
    pdm run install_dev
    ```
   
4. Follow the Standalone Mode Configuration instructions and run it directly from `__main__.py`, or create your own
entry script.

# Configuration

## Standalone Mode

Make sure you've followed the Installation instructions before continuing with the following steps.

1. Open a terminal in any folder and run the following commands:
    ```
    kaxanuk.data_curator init excel
    ```
2. Open in Excel the `Config/parameters_datacurator.xlsx` file that should have been created in yor folder, fill out the fields, save the file and close it.

Now you can run `__main__.py` and the library will download the data for the tickers configured in the file, and save
the data to the `Output` folder.
