# KaxaNuk Data Curator

|                                                                                                                                                                                                                                                                                                                                                                                |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [![Python](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue?logo=python&logoColor=ffdd54)](https://www.python.org) [![PyPI - License](https://img.shields.io/pypi/l/kaxanuk.data-curator?color=blue)](LICENSE)                                                                                                                                                       |
| [![Build Status](https://github.com/KaxaNuk/Data-Curator/actions/workflows/main.yml/badge.svg)](https://github.com/KaxaNuk/Data-Curator/actions/workflows/main.yml) [![Read the Docs](https://app.readthedocs.org/projects/kaxanuk-data-curator/badge/?version=stable)](https://kaxanuk-data-curator.readthedocs.io/en/stable/)                                                |
| [![PyPI - Version](https://img.shields.io/pypi/v/kaxanuk.data-curator?logo=pypi)](https://pypi.org/project/kaxanuk.data-curator) [![PyPI Downloads](https://static.pepy.tech/badge/kaxanuk-data-curator)](https://pepy.tech/projects/kaxanuk-data-curator) [![Powered by KaxaNuk](https://img.shields.io/badge/powered%20by-KaxaNuk-orange?colorB=orange)](https://kaxanuk.mx) |

Component library for downloading, validating, homogenizing, and combining financial stocks' data from different data providers.
Can be run in standalone mode, configurable in Excel, or as a component of a larger Python-based system. 

Features:
* **Configurable** from an Excel file, or directly in a Python script. Docker image also available.
* Fully readable and specific **tag names**, homogenized between data providers, based on the US GAAP taxonomy. Switch between data providers without changing your code.
* Automatically validates market and fundamental data, discarding datasets that make no sense (like high price below low, etc.) or can't guarantee point-in-time validity (like amended statements).
* Easily create your own **calculated feature functions** without need for Numpy or Pandas (though you can also use those if you want to).
* **Output** to CSV or Parquet files, or to in-memory Pandas Dataframes for further processing.
* Completely **extensible architecture**: implement your own data providers, feature combinations, and output handlers on top of clear, stable interfaces.
* Readable, well-documented, and tested **code**.


## Documentation
Full documentation is available at [kaxanuk-data-curator.readthedocs.io](https://kaxanuk-data-curator.readthedocs.io/en/stable/).

## Requirements
The system can run either on your local Python (versions `3.12` or `3.13`) or on Docker.


## Supported Data Providers
* Financial Modeling Prep (free and discounted plans available through [our referral link](https://site.financialmodelingprep.com/pricing-plans?couponCode=xss2L2sI))
* Yahoo Finance (requires installing a separate extension package, and doesn't support most data types)


## Running on Local Python
### Installation
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


### Configuration
1. Open a terminal in any directory and run the following command:
    ```
    kaxanuk.data_curator init excel
    ```
    This should create 2 subdirectories, `Config` and `Output`, as well as the entry script `__main__.py` in the current directory.
2. Open the `Config/parameters_datacurator.xlsx` file in Excel, fill out the fields in all the sheets, save the file and close it.
3. If your data provider requires an API key, open the `Config/.env` file in a text editor, and paste the key after
    the `=` sign of the provider's corresponding `API_KEY` variable. Don't add any quotes or spaces before or after the key.

*_If on MacOS, the `.env` file will be hidden in Finder by default. Just use the keys `Command` + `Shift` + `.` to toggle
the visibility of hidden files._


### Usage
Now you can run the entry script with either:
```
kaxanuk.data_curator run
```
or by executing the `__main__.py` script directly with Python:
```
python __main__.py
```
The system will download the data for the tickers configured in the file, and save the data to the `Output` folder.


## Running on Docker
### Pull the Docker image:
```
docker pull ghcr.io/kaxanuk/data-curator:latest
```

### Docker Configuration
#### Volumes
You need to mount the following volume to the container:
* Path on the host: (select the directory on your PC where you want the Data Curator configuration and output files to be created)
* Path inside the container: `/app`

#### Environment Variables
If your data provider requires an API key, you need to pass it as an environment variable when running the container.
* Name: `KNDC_API_KEY_FMP`
* Value: API key for the Financial Modeling Prep data provider, as a string.

#### Running the Container
1. On the first run, the container will create the `Config` and `Output` subdirectories in the mounted volume, as well as
the entry script `__main__.py`.
2. Open the `Config/parameters_datacurator.xlsx` file in Excel, fill out the fields in all the sheets, save the file and close it.

Now that the configuration is set up, each time you run the container again, it will download the data for the tickers/identifiers
as configured in the parameters file, and save it to the `Output` folder.


## Customization
The `__main__.py` entry script is customizable, so you can implement your own data providers and configuration and output
handlers, and inject them from there.

You can also create your own calculated feature functions by adding them to the `Config/custom_calculations.py` file,
and adding their function name to the `Columns` sheet in the `Config/parameters_datacurator.xlsx` file.
As long as the names start with the `c_` prefix, the system will use them as any other feature.

Check the [API Reference](https://kaxanuk-data-curator.readthedocs.io/en/stable/api_reference/index.html) to learn how to easily implement your own calculated features.


## The Road to v1.0
We believe in the need for a stable API, and have expended considerable effort into finalizing the API as much as 
possible before the first public release. We plan to avoid any changes that severely break backwards compatibility
before version 1.0, with one major exception: The Data Blocks functionality.

Data Blocks will generalize the link between the data providers and the feature column prefixes, which will allow users
to create their own data providers and feature columns for any type of data from any source without having to modify
the core code of the Data Curator. This will open the door to calculated features that incorporate all kinds of data,
like economic indicators, alternative data, financial indices and benchmarks, etc.

Once Data Blocks are implemented, we will rapidly make any necessary adjustments to the public API, and when we're
happy with it, we will work on finalizing the version 1.0 release.
