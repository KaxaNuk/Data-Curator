# KaxaNuk Data Curator

Component library for downloading, validating, homogenizing, and combining financial stocks' data from different data providers.
Can be run in standalone mode, configurable in Excel, or as a component of a larger Python-based system.


## Stack
- Python >=3.12,<3.15
- PyArrow for all the basic data structures and processing
- Click for CLI
- Sphinx for documentation generation
- Readthedocs for documentation hosting
- Microsoft APM for AI scaffolding
- GitHub Actions for automated testing and deployment
- Python docstring format: Numpy


## Directory Structure
```
data-curator/
├── .apm/                           # AI scaffolding primitives installable through MIcrosoft APM
├── .devcontainer/                  # Development container configuration
├── .github/                        # Automated build and test pipelines for GitHub Actions
├── docs/                           # Source for building Sphinx documentation, deployed to Readthedocs 
├── src/                            # Library code
│   └── kaxanuk/                    # Main organization namespace
│       └── data_curator/           # Data Curator namespace
│           ├── config_handlers/    # Modules for handling runtime configuration
│           ├── data_blocks/        # Packages for implementing each specific data type functionality
│           ├── data_providers/     # Connectors to external data sources
│           ├── entities/           # Entities for packaging and validating each data type
│           ├── exceptions/         # Custom exceptions
│           ├── features/           # Modules for feature calculation
│           ├── modules/            # Reusable modules that might be later extracted into their own libraries
│           ├── output_handlers/    # Modules for handling output in different formats 
│           ├── services/           # Services for common functionality specific to the library
│           ├── __init__.py         # Public Python API
│           └── data_curator.py     # Main library module
├── templates/                      # Templates for initializing user projects
│   └── data_curator/               # This folder gets copied to the Python environment's data directory on install 
│       ├── Config/                 # Templates for user configuration files
│       └── __main__.py             # Template for the Excel-configurable entry script
├── tests/                          # Automated tests
├── .readthedocs.yml                # Readthedocs build configuration
├── Dockerfile                      # Main Dockerfile for both development and production environments
├── pyproject.toml                  # All dependencies and project tool configurations are managed here
└── README.md                       # Main readme file
```


## Dependencies Reference
Consult these whenever you need to check a dependency's latest API or general documentation.

### Libraries
- [click API](https://click.palletsprojects.com/en/stable/api/)
- [lseg-data API](https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python/documentation)
- [networkx API](https://networkx.org/documentation/stable/reference/index.html)
- [openpyxl API](https://openpyxl.readthedocs.io/en/stable/api/openpyxl.html)
- [packaging API](https://packaging.pypa.io/en/stable/)
- [pandas API](https://pandas.pydata.org/docs/reference/index.html)
- [pyarrow API](https://arrow.apache.org/docs/python/api.html)
- [python-dotenv API](https://saurabh-kumar.com/python-dotenv/)

### Web services
- [Financial Modeling Prep API](https://site.financialmodelingprep.com/developer/docs)
