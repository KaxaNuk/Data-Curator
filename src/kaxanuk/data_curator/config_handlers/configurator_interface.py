"""
Interface for classes creating Configuration entities and related dependencies.
"""

import abc
import logging
import types

from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.data_providers import DataProviderInterface
from kaxanuk.data_curator.output_handlers import OutputHandlerInterface


class ConfiguratorInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_configuration(self) -> Configuration:
        ...

    @abc.abstractmethod
    def get_fundamental_data_provider(self) -> DataProviderInterface:
        ...

    @abc.abstractmethod
    def get_logger_level(self) -> int:
        ...

    @abc.abstractmethod
    def get_market_data_provider(self) -> DataProviderInterface:
        ...

    @abc.abstractmethod
    def get_output_handler(self) -> OutputHandlerInterface:
        ...

    CONFIGURATION_LOGGER_LEVELS = types.MappingProxyType({
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    })
    CONFIGURATION_PROVIDER_NONE = 'none'
    CONFIGURATION_PROVIDERS_FUNDAMENTAL = (
        'financial_modeling_prep',
        CONFIGURATION_PROVIDER_NONE,
    )
    CONFIGURATION_PROVIDERS_MARKET = (
        'financial_modeling_prep',
        'yahoo_finance'
    )
