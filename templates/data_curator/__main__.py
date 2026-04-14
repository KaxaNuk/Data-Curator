"""
Example entry script implementation that reads configuration from an Excel file and outputs to csv or parquet files.

It initializes a Configuration object through our ExcelConfigurator module, selects its possible data provider
modules and output handler, and passes all these dependencies to data_curator.main().

Environment Variables
---------------------
KNDC_API_KEY_FMP : str
    Api key for the Financial Modeling Prep data provider
KNDC_API_KEY_LSEG : str
    Api key for the LSEG Workspace data provider
"""

import os
import pathlib

import kaxanuk.data_curator


# Load the user's environment variables from Config/.env, including data provider API keys
kaxanuk.data_curator.load_config_env()

# Load user's custom calculations module, if exists in Config dir
custom_calculations_file = 'Config/custom_calculations.py'
if pathlib.Path(custom_calculations_file).is_file():
    from Config import custom_calculations
    custom_calculation_modules = [custom_calculations]
else:
    custom_calculation_modules = []

output_base_dir = 'Output'

# Load the configuration from the file
parameters_excel_file = 'Config/data_curator_parameters.xlsx'
configurator = kaxanuk.data_curator.config_handlers.ExcelConfigurator(
    file_path=parameters_excel_file,
    data_providers={
        'financial_modeling_prep': {
            'class': kaxanuk.data_curator.data_providers.FinancialModelingPrep,
            'api_key': os.getenv('KNDC_API_KEY_FMP'),   # set this up in the Config/.env file
        },
        'lseg_workspace': {
            'class': kaxanuk.data_curator.data_providers.LsegWorkspace,
            'api_key': os.getenv('KNDC_API_KEY_LSEG'), # set this up in the Config/.env file
        },
        'yahoo_finance': {
            'class': kaxanuk.data_curator.load_data_provider_extension(
                extension_name='yahoo_finance',
                extension_class_name='YahooFinance',
            ),
            'api_key': None     # this provider doesn't use API key
        },
    },
    output_handlers={
       'csv': kaxanuk.data_curator.output_handlers.CsvOutput(
            output_base_dir=output_base_dir,
       ),
        'parquet': kaxanuk.data_curator.output_handlers.ParquetOutput(
            output_base_dir=output_base_dir,
       ),
    },
)

# Run this puppy!
kaxanuk.data_curator.main(
    configuration=configurator.get_configuration(),
    market_data_provider=configurator.get_market_data_provider(),
    fundamental_data_provider=configurator.get_fundamental_data_provider(),
    output_handlers=[configurator.get_output_handler()],
    custom_calculation_modules=custom_calculation_modules,  # Optional
    logger_level=configurator.get_logger_level(),           # Optional
)
