"""
Package containing the interface and implementations of Configuration entity factories.
"""

# make these modules part of the public API of the base namespace
from kaxanuk.data_curator.config_handlers.configurator_interface import ConfiguratorInterface
from kaxanuk.data_curator.config_handlers.excel_configurator import ExcelConfigurator


__all__ = [
    'ConfiguratorInterface',
    'ExcelConfigurator',
]
