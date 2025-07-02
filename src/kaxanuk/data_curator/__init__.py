__version__ = '0.41.0'
__parameters_format_version__ = '0.40.0'

__package_name__ = 'kaxanuk.data_curator'
__package_title__ = 'KaxaNuk Data Curator'


# add these definitions to our base namespace
from kaxanuk.data_curator.data_curator import main
from kaxanuk.data_curator.modules import debugger
from kaxanuk.data_curator.modules.data_column import DataColumn
from kaxanuk.data_curator.modules.dotenv_loader import load_config_env
from kaxanuk.data_curator.modules.extension_handler import (
    load_data_provider_extension
)
from kaxanuk.data_curator.services.cli import cli

# make these modules part of the public API of the base namespace
from kaxanuk.data_curator import config_handlers
from kaxanuk.data_curator import data_providers
from kaxanuk.data_curator import entities
from kaxanuk.data_curator import exceptions
from kaxanuk.data_curator import features
from kaxanuk.data_curator import output_handlers


# this defines the public API; just don't use `import *`, mmkay?
__all__ = [
    'DataColumn',
    '__package_name__',
    '__package_title__',
    '__parameters_format_version__',
    '__version__',
    'cli',
    'config_handlers',
    'data_providers',
    'debugger',
    'entities',
    'exceptions',
    'features',
    'load_config_env',
    'load_data_provider_extension',
    'main',
    'output_handlers',
]
