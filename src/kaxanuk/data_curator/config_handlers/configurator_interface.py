"""
Interface for classes creating Configuration entities and related dependencies.
"""

import abc
import enum
import logging

from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.data_providers import DataProviderInterface
from kaxanuk.data_curator.output_handlers import OutputHandlerInterface


class ConfigurationLoggerLevel(enum.StrEnum):
    """
    The logger levels selectable in a configuration, each named after its logging module level.
    """
    CRITICAL = 'critical'
    DEBUG = 'debug'
    ERROR = 'error'
    INFO = 'info'
    WARNING = 'warning'

    @property
    def logger_level(self) -> int:
        """
        The value of the logging module level of this configuration logger level.
        """
        return logging.getLevelNamesMapping()[self.name]


class ConfiguratorInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_configuration(self) -> Configuration:
        ...

    @abc.abstractmethod
    def get_data_block_providers(self) -> dict[type[BaseDataBlock], DataProviderInterface]:
        ...

    @abc.abstractmethod
    def get_logger_level(self) -> int:
        ...

    @abc.abstractmethod
    def get_output_handler(self) -> OutputHandlerInterface:
        ...
