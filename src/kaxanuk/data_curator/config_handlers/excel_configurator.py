"""
Loads and returns a Configuration entity from an Excel file.

Functions
---------
init:
    Loads and returns Configuration
"""

import logging
import pathlib
import re
import sys
import typing

import openpyxl
import openpyxl.cell
import openpyxl.utils.exceptions
import openpyxl.worksheet.cell_range
import openpyxl.worksheet.worksheet
import packaging.version

from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
# @todo: remove these built-in data block imports once we drop the deprecated parameter and file format support:
from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import (
    ConfigurationError,
    ConfigurationHandlerError
)
from kaxanuk.data_curator.config_handlers.configurator_interface import (
    ConfigurationLoggerLevel,
    ConfiguratorInterface,
)
from kaxanuk.data_curator.output_handlers import OutputHandlerInterface
from kaxanuk.data_curator.data_providers import (
    DataProviderInterface,
    NotFoundDataProvider,
)
from kaxanuk.data_curator import __parameters_format_version__


class ExcelConfigurator(ConfiguratorInterface):
    # the keys required in the parameters of each injected data provider:
    DATA_PROVIDER_PARAMETER_KEYS : typing.Final = (
        'class',
        'api_key',
    )
    # the sheet mapping each data block to the data provider that will supply its data:
    DATA_PROVIDERS_SHEET : typing.Final = 'Data_Providers'
    # the headers of the data block and data provider columns of the data providers sheet:
    DATA_PROVIDERS_SHEET_HEADERS : typing.Final = (
        'data_block',
        'data_provider',
    )
    # @todo: remove all the following DEPRECATED_ constants once we drop the deprecated parameter and file formats
    # the data blocks each deprecated General sheet data provider key supplied:
    DEPRECATED_DATA_BLOCKS_BY_PROVIDER_KEY : typing.Final = {
        'fundamental_data_provider': (
            DividendsDataBlock,
            FundamentalsDataBlock,
            SplitsDataBlock,
        ),
        'market_data_provider': (
            MarketDailyDataBlock,
        ),
    }
    # the data blocks assumed when the entry script doesn't inject any, replicating the deprecated hardcoded behavior:
    DEPRECATED_DEFAULT_DATA_BLOCKS : typing.Final = (
        DividendsDataBlock,
        FundamentalsDataBlock,
        MarketDailyDataBlock,
        SplitsDataBlock,
    )
    # the oldest parameters file format version still supported, through the deprecated General sheet provider keys:
    DEPRECATED_PARAMETERS_FORMAT_VERSION : typing.Final = '0.47.0'
    NONE_DATA_PROVIDER = 'none'
    SHEET_KEY_VALUES : typing.Final = {
        'General': (
            'start_date',
            'end_date',
            'period',
            'logger_level',
            'output_format',
            'parameters_format_version'
        ),
    }
    SHEET_COLUMNS : typing.Final = {
        'Identifiers': (
            'main_identifier',
        ),
        'Output_Columns': (
            'columns',
        ),
    }

    # the structure of the parameters for each injected data provider:
    DataProviderParameter = typing.TypedDict(
        'DataProviderParameter',
        {
            'class': type[DataProviderInterface],
            'api_key': str | None
        }
    )

    def __init__(
        self,
        *,
        file_path: str,
        data_providers: dict[
            str,
            DataProviderParameter,
        ],
        output_handlers: dict[str, OutputHandlerInterface],
        logger_format: str = "[%(levelname)s] %(message)s",
        # @todo: make this parameter required, and move it right after file_path, once we drop the deprecated
        # entry script support:
        data_blocks: list[type[BaseDataBlock]] | None = None,
    ):
        """
        Initialize configuration, data providers and output handlers based on a configuration Excel file.

        Parameters
        ----------
        file_path
            The path to the Excel configuration file
        data_providers
            All the data provider options that the configuration file will choose from, along with their API keys if any
        output_handlers
            All the output handlers options that the configuration file will choose from
        logger_format
            The format for the logger messages. will be injected to logging.basicConfig()
        data_blocks
            All the data block classes that the configuration file will choose from, each identified in the file
            by its class name. Defaults to all the built-in data blocks, which is deprecated behavior that will be
            removed in the next version, when the parameter becomes required
        """
        # not using logging.basicConfig as we need to close it, without affecting any existing root logger
        logger = logging.getLogger(__name__)
        # set the logging level to info by default, as we haven't loaded the file configuration yet
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(logger_format)
        )
        logger.addHandler(handler)

        try:
            # @todo: remove this deprecated fallback once the data_blocks parameter becomes required
            if data_blocks is None:
                msg = " ".join([
                    "No data_blocks were injected into ExcelConfigurator, so all the built-in data blocks were",
                    "assumed. This is deprecated and will become incompatible in the next version, so please",
                    "update your entry script and parameters file by running:",
                    "kaxanuk.data_curator update entry_script",
                    "and",
                    "kaxanuk.data_curator update excel",
                ])
                logger.warning(msg)
                selected_data_blocks = list(self.DEPRECATED_DEFAULT_DATA_BLOCKS)
            else:
                selected_data_blocks = data_blocks

            invalid_data_block_descriptions = [
                repr(data_block)
                for data_block in selected_data_blocks
                if not isinstance(data_block, type)
                or not issubclass(data_block, BaseDataBlock)
            ]
            if len(invalid_data_block_descriptions) > 0:
                msg = " ".join([
                    "The following data blocks injected into ExcelConfigurator aren't BaseDataBlock subclasses:",
                    ", ".join(invalid_data_block_descriptions),
                ])

                raise ConfigurationHandlerError(msg)

            invalid_provider_descriptions = []
            for (provider_name, provider_parameters) in data_providers.items():
                if not isinstance(provider_parameters, dict):
                    invalid_provider_descriptions.append(
                        f"{provider_name} parameters aren't a dict: {provider_parameters!r}"
                    )

                    continue

                missing_parameter_keys = [
                    parameter_key
                    for parameter_key in self.DATA_PROVIDER_PARAMETER_KEYS
                    if parameter_key not in provider_parameters
                ]
                if len(missing_parameter_keys) > 0:
                    invalid_provider_descriptions.append(
                        f"{provider_name} parameters are missing the keys: {', '.join(missing_parameter_keys)}"
                    )

                    continue

                if not isinstance(provider_parameters['class'], type):
                    invalid_provider_descriptions.append(
                        f"{provider_name} class isn't a class: {provider_parameters['class']!r}"
                    )
                elif not issubclass(provider_parameters['class'], DataProviderInterface):
                    invalid_provider_descriptions.append(
                        " ".join([
                            f"{provider_name} class {provider_parameters['class'].__name__}",
                            "doesn't implement DataProviderInterface",
                        ])
                    )

                if (
                    provider_parameters['api_key'] is not None
                    and not isinstance(provider_parameters['api_key'], str)
                ):
                    invalid_provider_descriptions.append(
                        f"{provider_name} api_key is neither a string nor None"
                    )

            if len(invalid_provider_descriptions) > 0:
                msg = " ".join([
                    "The following data providers injected into ExcelConfigurator are incorrectly structured:",
                    "; ".join(invalid_provider_descriptions),
                ])

                raise ConfigurationHandlerError(msg)

            workbook = self._load_file(file_path)
            sheet_key_values = self._extract_workbook_key_values_by_schema(
                workbook,
                self.SHEET_KEY_VALUES,
            )
            sheet_columns = self._extract_workbook_columns_by_schema(
                workbook,
                self.SHEET_COLUMNS,
            )

            self._logger_level = self._get_logger_level_from_name(
                sheet_key_values['General']['logger_level']
            )
            logger.setLevel(self._logger_level)

            current_parameters_format_version = str(sheet_key_values['General']['parameters_format_version'])
            # @todo: compare against __parameters_format_version__ once we drop the deprecated file format support
            if (
                len(current_parameters_format_version) < 1
                or (
                    packaging.version.parse(current_parameters_format_version)
                    < packaging.version.parse(self.DEPRECATED_PARAMETERS_FORMAT_VERSION)
                )
            ):
                msg = " ".join([
                    "Excel configuration file uses an unsupported old format, please update it by running:",
                    "kaxanuk.data_curator update excel",
                ])

                raise ConfigurationHandlerError(msg)

            # @todo: remove this deprecated file format handling once we only support the latest format
            is_deprecated_parameters_format = (
                packaging.version.parse(current_parameters_format_version)
                < packaging.version.parse(__parameters_format_version__)
            )
            if is_deprecated_parameters_format:
                msg = " ".join([
                    "Your Excel configuration file and entry script are using deprecated parameters which will stop",
                    "working in the next version. Please update them by running",
                    "`kaxanuk.data_curator update excel`",
                    "and",
                    "`kaxanuk.data_curator update entry_script`",
                ])
                logger.warning(msg)

            data_blocks_by_name = {
                data_block.__name__: data_block
                for data_block in selected_data_blocks
            }

            # @todo: replace this whole block with the _extract_sheet_key_value_rows call of its else clause, once
            # we drop the deprecated file format support
            if is_deprecated_parameters_format:
                file_provider_names_by_data_block_name = self._extract_deprecated_provider_names(
                    workbook,
                    data_blocks_by_name.keys(),
                )
            else:
                file_provider_names_by_data_block_name = self._extract_sheet_key_value_rows(
                    workbook,
                    self.DATA_PROVIDERS_SHEET,
                    self.DATA_PROVIDERS_SHEET_HEADERS,
                )

            # @todo: use file_provider_names_by_data_block_name directly once we drop the deprecated snake_case
            # data provider names of the previous parameters file format
            provider_names_by_data_block_name = self._resolve_deprecated_provider_names(
                file_provider_names_by_data_block_name,
                data_providers.keys(),
            )

            unknown_data_block_names = [
                data_block_name
                for data_block_name in provider_names_by_data_block_name
                if data_block_name not in data_blocks_by_name
            ]
            if len(unknown_data_block_names) > 0:
                msg = " ".join([
                    f"The following data blocks selected in the {self.DATA_PROVIDERS_SHEET} sheet are unavailable:",
                    ", ".join(unknown_data_block_names),
                ])

                raise ConfigurationError(msg)

            # the same data provider can supply several data blocks, but only gets instantiated once
            selected_provider_names = []
            for provider_name in provider_names_by_data_block_name.values():
                if (
                    provider_name.lower() == self.NONE_DATA_PROVIDER
                    or provider_name in selected_provider_names
                ):
                    continue

                selected_provider_names.append(provider_name)

            missing_provider_names = [
                provider_name
                for provider_name in selected_provider_names
                if provider_name not in data_providers
            ]
            if len(missing_provider_names) > 0:
                msg = " ".join([
                    f"The following data providers selected in the {self.DATA_PROVIDERS_SHEET} sheet are unavailable:",
                    ", ".join(missing_provider_names),
                ])

                raise ConfigurationError(msg)

            uninstalled_provider_names = [
                provider_name
                for provider_name in selected_provider_names
                if issubclass(
                    data_providers[provider_name]['class'],
                    NotFoundDataProvider
                )
            ]
            if len(uninstalled_provider_names) > 0:
                extension_install_commands = [
                    "".join([
                        "pip install kaxanuk.data_curator_extensions.",
                        self._convert_class_name_to_extension_name(provider_name),
                    ])
                    for provider_name in uninstalled_provider_names
                ]
                msg = " ".join([
                    "The following data providers were not found on your system:",
                    f"{', '.join(uninstalled_provider_names)}.",
                    "If they're officially supported providers you should be able to install them by running:\n",
                    "\n".join(extension_install_commands),
                ])

                raise ConfigurationError(msg)

            unsupplied_data_block_descriptions = []
            for (data_block_name, provider_name) in provider_names_by_data_block_name.items():
                if provider_name.lower() == self.NONE_DATA_PROVIDER:
                    continue

                supplied_data_blocks = data_providers[provider_name]['class'].get_data_block_endpoint_tag_map()
                if data_blocks_by_name[data_block_name] in supplied_data_blocks:
                    continue

                unsupplied_data_block_descriptions.append(
                    f"{data_block_name}: {provider_name}"
                )

            if len(unsupplied_data_block_descriptions) > 0:
                msg = " ".join([
                    f"The following {self.DATA_PROVIDERS_SHEET} sheet data blocks can't be supplied by the data",
                    "provider selected for them:",
                    ", ".join(unsupplied_data_block_descriptions),
                ])

                raise ConfigurationError(msg)

            data_provider_instances = {}
            for provider_name in selected_provider_names:
                if data_providers[provider_name]['api_key'] is not None:
                    data_provider_params = {'api_key': data_providers[provider_name]['api_key']}
                else:
                    data_provider_params = {}

                # noinspection PyArgumentList
                data_provider_instances[provider_name] = data_providers[provider_name]['class'](
                    **data_provider_params
                )

            for data_provider in data_provider_instances.values():
                is_api_key_valid = data_provider.validate_api_key()
                if is_api_key_valid:
                    msg = f"API key validation succeeded for {data_provider.__class__.__name__}"
                    logging.getLogger(__name__).info(msg)
                elif is_api_key_valid is not None:
                    msg = f"Invalid API key for {data_provider.__class__.__name__}"

                    raise ConfigurationError(msg)

            self._data_block_providers = {
                data_blocks_by_name[data_block_name]: data_provider_instances[provider_name]
                for (data_block_name, provider_name) in provider_names_by_data_block_name.items()
                if provider_name.lower() != self.NONE_DATA_PROVIDER
            }

            self._output_handler = output_handlers[
                sheet_key_values['General']['output_format']
            ]

            self._configuration = Configuration(
                start_date=sheet_key_values['General']['start_date'].date(),
                end_date=sheet_key_values['General']['end_date'].date(),
                period=sheet_key_values['General']['period'],
                identifiers=sheet_columns['Identifiers']['main_identifier'],
                columns=sheet_columns['Output_Columns']['columns']
            )

            # remove this logger
            logger.handlers.clear()
        except (
            ConfigurationError,
            ConfigurationHandlerError
        ) as error:
            msg = f"An error was encountered when parsing your configuration file: {error!s}"
            logging.getLogger(__name__).critical(msg)
            sys.exit()

    def get_configuration(self) -> Configuration:
        return self._configuration

    def get_data_block_providers(self) -> dict[type[BaseDataBlock], DataProviderInterface]:
        return self._data_block_providers

    # @todo: remove this deprecated method once we drop the deprecated entry script support
    def get_fundamental_data_provider(self) -> DataProviderInterface | None:
        """
        Get the data provider assigned to the built-in fundamentals data block.

        Deprecated, use get_data_block_providers instead.

        Returns
        -------
        The data provider instance, or None if no provider was assigned to the fundamentals data block
        """
        msg = " ".join([
            "The ExcelConfigurator.get_fundamental_data_provider method is deprecated and will be removed in the",
            "next version. Please update your entry script by running: kaxanuk.data_curator update entry_script",
        ])
        logging.getLogger(__name__).warning(msg)

        return self._data_block_providers.get(FundamentalsDataBlock)

    def get_logger_level(self) -> int:
        return self._logger_level

    # @todo: remove this deprecated method once we drop the deprecated entry script support
    def get_market_data_provider(self) -> DataProviderInterface | None:
        """
        Get the data provider assigned to the built-in market daily data block.

        Deprecated, use get_data_block_providers instead.

        Returns
        -------
        The data provider instance, or None if no provider was assigned to the market daily data block
        """
        msg = " ".join([
            "The ExcelConfigurator.get_market_data_provider method is deprecated and will be removed in the",
            "next version. Please update your entry script by running: kaxanuk.data_curator update entry_script",
        ])
        logging.getLogger(__name__).warning(msg)

        return self._data_block_providers.get(MarketDailyDataBlock)

    def get_output_handler(self) -> OutputHandlerInterface:
        return self._output_handler

    @staticmethod
    def _convert_class_name_to_extension_name(class_name: str) -> str:
        """
        Convert a data provider class name into the snake_case name of its extension module.

        Follows the naming convention of our officially supported extensions, where the YahooFinance class
        lives in the yahoo_finance extension module.

        Parameters
        ----------
        class_name
            The name of the data provider class

        Returns
        -------
        The snake_case name of the extension module implementing the class
        """
        return re.sub(
            r'(?<!^)(?=[A-Z])',
            '_',
            class_name
        ).lower()

    @staticmethod
    def _extract_cell_value(cell: openpyxl.cell.cell.Cell) -> str | None:
        """
        Extract the value of an openpyxl cell as a string.

        Parameters
        ----------
        cell
            The cell object to extract the value from.

        Returns
        -------
        The stripped value of the cell. If the cell is empty returns None.
        """
        if (
            cell is not None
            and cell.value is not None
        ):

            return str(cell.value).strip()

        else:

            return None

    @classmethod
    def _extract_column_values(
        cls,
        column: tuple[openpyxl.cell.cell.Cell, ...]
    ) -> list[str]:
        """
        Extract the values of an openpyxl column as a string list.

        Parameters
        ----------
        column : openpyxl.worksheet.cell_range.CellRange
            The column object to extract the values from.

        Returns
        -------
        list[str]
            The values as strings.
        """
        values = filter(
            None,
            (
                cls._extract_cell_value(i)
                for i in column
            )
        )

        return list(values)

    # @todo: remove this deprecated method once we drop the deprecated file format support
    @classmethod
    def _extract_deprecated_provider_names(
        cls,
        workbook: openpyxl.workbook.workbook.Workbook,
        data_block_names: typing.Collection[str],
    ) -> dict[str, str]:
        """
        Extract the data provider of each data block from the deprecated General sheet data provider keys.

        Each deprecated key supplied a fixed group of built-in data blocks, so its value gets assigned to each
        one of those data blocks that is also available in data_block_names.

        Parameters
        ----------
        workbook
            The workbook to search
        data_block_names
            The names of the data blocks available to the configuration file

        Returns
        -------
        The name of the data provider selected for each available data block name

        Raises
        ------
        ConfigurationHandlerError
        """
        deprecated_key_values = cls._extract_workbook_key_values_by_schema(
            workbook,
            {
                'General': tuple(cls.DEPRECATED_DATA_BLOCKS_BY_PROVIDER_KEY),
            },
        )
        provider_names_by_data_block_name = {}
        keys_without_value = []
        for (provider_key, supplied_data_blocks) in cls.DEPRECATED_DATA_BLOCKS_BY_PROVIDER_KEY.items():
            selected_data_block_names = [
                data_block.__name__
                for data_block in supplied_data_blocks
                if data_block.__name__ in data_block_names
            ]
            if len(selected_data_block_names) < 1:
                continue

            provider_name = deprecated_key_values['General'][provider_key]
            if provider_name is None:
                keys_without_value.append(provider_key)

                continue

            for data_block_name in selected_data_block_names:
                provider_names_by_data_block_name[data_block_name] = str(provider_name).strip()

        if len(keys_without_value) > 0:
            msg = " ".join([
                "The following General sheet keys of the Configuration file have no value:",
                ", ".join(keys_without_value),
            ])

            raise ConfigurationHandlerError(msg)

        return provider_names_by_data_block_name

    @classmethod
    def _extract_sheet_key_value_rows(
        cls,
        workbook: openpyxl.workbook.workbook.Workbook,
        sheet_name: str,
        headers: tuple[str, str],
        header_row: int = 1,
        key_column: str = 'A',
    ) -> dict[str, str]:
        """
        Extract all the key/value row pairs of a sheet, whose keys aren't known in advance.

        Parameters
        ----------
        workbook
            The workbook to search
        sheet_name
            The name of the sheet holding the key/value rows
        headers
            The expected headers of the key and value columns, used to validate the sheet's format
        header_row
            The number of the row holding the column headers, with the key/value rows starting right below it
        key_column
            The letter identifier of the column holding the keys

        Returns
        -------
        Each key of the sheet mapped to its value

        Raises
        ------
        ConfigurationHandlerError
        """
        if sheet_name not in workbook.sheetnames:
            msg = f"The following sheet is missing from the Configuration file: {sheet_name}"

            raise ConfigurationHandlerError(msg)

        sheet = workbook[sheet_name]
        value_column = cls._increment_column_identifier(key_column)
        found_headers = (
            cls._extract_cell_value(sheet[f'{key_column}{header_row}']),
            cls._extract_cell_value(sheet[f'{value_column}{header_row}']),
        )
        if found_headers != headers:
            msg = " ".join([
                f"The {sheet_name} sheet of the Configuration file requires the headers",
                f"{', '.join(headers)}, but instead has:",
                ", ".join(
                    str(found_header)
                    for found_header in found_headers
                ),
            ])

            raise ConfigurationHandlerError(msg)

        key_values = {}
        keys_without_value = []
        for row in range(header_row + 1, sheet.max_row + 1):
            key = cls._extract_cell_value(sheet[f'{key_column}{row}'])
            if key is None:
                continue

            value = cls._extract_cell_value(sheet[f'{value_column}{row}'])
            if value is None:
                keys_without_value.append(key)
            else:
                key_values[key] = value

        if len(keys_without_value) > 0:
            msg = " ".join([
                f"The following {sheet_name} sheet rows of the Configuration file have no",
                f"{headers[1]} value:",
                ", ".join(keys_without_value),
            ])

            raise ConfigurationHandlerError(msg)

        return key_values

    @classmethod
    def _extract_workbook_columns_by_schema(
        cls,
        workbook: openpyxl.workbook.workbook.Workbook,
        schema: typing.Mapping[str, tuple[str, ...]],
        header_row: int=1
    ) -> dict[str, dict[str, tuple[typing.Any, ...]]]:
        """
        Extract the values of particular columns specified in the schema from a workbook's sheets.

        Parameters
        ----------
        workbook
            The workbook to search
        schema
            The schema indicating the required sheets and the columns to be extracted from them
        header_row
            The number of the row that will include the column headings

        Returns
        -------
        The extracted values, in the same arrangement as the schema

        Raises
        ------
        ConfigurationHandlerError
        """
        missing_sheets = set(schema.keys()).difference(
            set(workbook.sheetnames)
        )
        if len(missing_sheets) > 0:
            msg = " ".join([
                "The following sheets are missing from the Configuration file:",
                ", ".join(missing_sheets)
            ])

            raise ConfigurationHandlerError(msg)

        sheets = {}
        missing_sheet_columns = {}
        for sheet_name in schema:
            sheets[sheet_name] = {}
            missing_columns = []
            for column_name in schema[sheet_name]:
                column = cls._find_sheet_column_by_row_value(
                    workbook[sheet_name],
                    header_row,
                    column_name
                )
                if column is None:
                    missing_columns.append(column_name)
                else:
                    sheets[sheet_name][column_name] = cls._extract_column_values(
                        workbook[sheet_name][column][header_row:]
                    )
            if len(missing_columns) > 0:
                missing_sheet_columns[sheet_name] = missing_columns

        if len(missing_sheet_columns) > 0:
            msg = " ".join([
                "The following sheet columns are missing from the Configuration file:",
                "; ".join([
                    f"{sheet}: " + ", ".join(fields)
                    for sheet, fields in missing_sheet_columns.items()
                ])
            ])

            raise ConfigurationHandlerError(msg)

        return sheets

    @classmethod
    def _extract_workbook_key_values_by_schema(
        cls,
        workbook: openpyxl.workbook.workbook.Workbook,
        schema: typing.Mapping[str, tuple[str, ...]],
        key_column: str='A',
    ) -> dict[str, dict[str, typing.Any]]:
        """
        Extract the values of particular key values specified in the schema from a workbook's sheets.

        Parameters
        ----------
        workbook
            The workbook to search
        schema
            The schema indicating the required sheets and the key values to be extracted from them
        key_column
            The letter identifier of the column to search for the key

        Returns
        -------
        The extracted values, in the same arrangement as the schema

        Raises
        ------
        ConfigurationHandlerError
        """
        value_column = cls._increment_column_identifier(key_column)
        missing_sheets = set(schema.keys()).difference(
            set(workbook.sheetnames)
        )
        if len(missing_sheets) > 0:
            msg = " ".join([
                "The following sheets are missing from the Configuration file:",
                ", ".join(missing_sheets)
            ])

            raise ConfigurationHandlerError(msg)

        sheets = {}
        missing_sheet_fields = {}
        for sheet_name in schema:
            sheets[sheet_name] = {}
            missing_fields = []
            for field_name in schema[sheet_name]:
                row = cls._find_sheet_row_by_column_value(
                    workbook[sheet_name],
                    key_column,
                    field_name
                )
                if row is None:
                    missing_fields.append(field_name)
                else:
                    sheets[sheet_name][field_name] = workbook[sheet_name][f'{value_column}{row}'].value
            if len(missing_fields) > 0:
                missing_sheet_fields[sheet_name] = missing_fields

        if len(missing_sheet_fields) > 0:
            msg = " ".join([
                "The following sheet params are missing from the Configuration file:",
                "; ".join([
                    f"{sheet}: " + ", ".join(fields)
                    for sheet, fields in missing_sheet_fields.items()
                ])
            ])

            raise ConfigurationHandlerError(msg)

        return sheets

    @staticmethod
    def _find_sheet_column_by_row_value(
        sheet: openpyxl.worksheet.worksheet.Worksheet,
        row: int,
        search_value: str
    ) -> int | None:
        """
        Find the column location of the search_value in the row.

        Parameters
        ----------
        sheet
            The sheet in which to search

        row
            The number of the row where to search

        search_value
            the value to be searched for in the row

        Returns
        -------
        The letter identifier of the column where the value was found, or None if not found
        """
        found_column = None

        for column_number in range(1, sheet.max_column + 1):
            column = chr(
                ord('A') + column_number - 1
            )
            cell_name = f"{column}{row}"
            if (
                sheet[cell_name].value is not None
                and sheet[cell_name].value.strip() == search_value
            ):
                found_column = column
                break

        return found_column

    @staticmethod
    def _find_sheet_row_by_column_value(
        sheet: openpyxl.worksheet.worksheet.Worksheet,
        column: str,
        search_value: str
    ) -> int | None:
        """
        Find the row location of the search_value in the column.

        Parameters
        ----------
        sheet
            The sheet in which to search
        column
            The letter identofoer of the column where to search
        search_value
            the value to be searched for in the column

        Returns
        -------
        The number of the row where the value was found, or None if not found
        """
        found_row = None

        for row in range(1, sheet.max_row + 1):
            cell_name = f"{column}{row}"
            if (
                sheet[cell_name].value is not None
                and sheet[cell_name].value.strip() == search_value
            ):
                found_row = row
                break

        return found_row

    @staticmethod
    def _get_logger_level_from_name(
        level_name: str
    ) -> int:
        """
        Get the logger level value corresponding to a Configuration.logger_level name.

        Parameters
        ----------
        level_name
            The Configuration.logger_level name

        Returns
        -------
        The level value, as set in the logger module

        Raises
        ------
        ConfigurationHandlerError
        """
        try:
            configuration_logger_level = ConfigurationLoggerLevel(level_name)
        except ValueError as error:
            msg = " ".join([
                f"Invalid logger_level in the Configuration file: {level_name}.",
                "The valid logger levels are:",
                ", ".join(ConfigurationLoggerLevel),
            ])

            raise ConfigurationHandlerError(msg) from error

        return configuration_logger_level.logger_level

    @classmethod
    def _increment_column_identifier(
        cls,
        column_identifier: str
    ) -> str:
        """
        Get the letter identifier of the next column after column_identifier.

        Uses Excel's column identifier scheme of successive uppercase letters. AA follows Z, and so on...

        Parameters
        ----------
        column_identifier
            The column identifier to increment

        Returns
        -------
        The next column letter identifier
        """
        if column_identifier == 'Z':

            return 'AA'

        elif column_identifier[-1] == 'Z':

            return cls._increment_column_identifier(column_identifier[:-1]) + 'A'

        else:
            next_character = chr(
                ord(column_identifier[-1])
                + 1
            )

            return column_identifier[:-1] + next_character

    @staticmethod
    def _load_file(file_path: str) -> openpyxl.Workbook:
        """
        Load an Excel file.

        Parameters
        ----------
        file_path
            The path of the Excel file to load

        Returns
        -------
        The openpyxl Workbook corresponding to the file

        Raises
        ------
        ConfigurationHandlerError
        """
        if not pathlib.Path(file_path).is_file():
            msg = f"Configuration file not found in path: {file_path}"

            raise ConfigurationHandlerError(msg)

        try:
            workbook = openpyxl.load_workbook(file_path)
        except openpyxl.utils.exceptions.InvalidFileException as error:
            msg = f"Invalid Configuration file in path: {file_path}"

            raise ConfigurationHandlerError(msg) from error

        return workbook

    # @todo: remove this deprecated method once we drop the deprecated file format support
    @classmethod
    def _resolve_deprecated_provider_names(
        cls,
        provider_names_by_data_block_name: typing.Mapping[str, str],
        data_provider_names: typing.Collection[str],
    ) -> dict[str, str]:
        """
        Match each data provider name selected in the configuration file to the name of an injected data provider.

        The previous parameters file format identified the data providers by their deprecated snake_case aliases
        instead of their class names, so any name without an exact match in the injected data providers gets
        matched against them by comparing their snake_case forms. Names that still don't match are left
        untouched, so that they get reported as unavailable further down the line.

        Parameters
        ----------
        provider_names_by_data_block_name
            The name of the data provider selected in the configuration file for each data block name
        data_provider_names
            The names of the data providers injected into the configurator

        Returns
        -------
        The name of the injected data provider matching each data block name's selection
        """
        injected_names_by_snake_case_name = {
            cls._convert_class_name_to_extension_name(data_provider_name): data_provider_name
            for data_provider_name in data_provider_names
        }
        resolved_provider_names_by_data_block_name = {}
        for (data_block_name, provider_name) in provider_names_by_data_block_name.items():
            snake_case_provider_name = cls._convert_class_name_to_extension_name(provider_name)
            if (
                provider_name in data_provider_names
                or provider_name.lower() == cls.NONE_DATA_PROVIDER
                or snake_case_provider_name not in injected_names_by_snake_case_name
            ):
                resolved_provider_names_by_data_block_name[data_block_name] = provider_name
            else:
                resolved_provider_names_by_data_block_name[data_block_name] = (
                    injected_names_by_snake_case_name[snake_case_provider_name]
                )

        return resolved_provider_names_by_data_block_name
