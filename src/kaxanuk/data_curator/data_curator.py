"""
KaxaNuk Data Curator: Request, combine and save financial data from different provider web services.

Requires an entry script that injects the required dependencies
cf. __main__.py on the GitHub repository root

Functions
---------
main:
    Receives injected dependencies and runs the system
"""

import inspect
import logging
import os
import types
import warnings

from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.fundamentals import FundamentalsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
from kaxanuk.data_curator.entities import (
    BaseDataEntity,
    Configuration,
)
from kaxanuk.data_curator.exceptions import (
    ApiEndpointError,
    ColumnBuilderCircularDependenciesError,
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderUnavailableEntityFieldError,
    DataBlockRowEntityErrorGroup,
    DataProviderPaymentError,
    EntityProcessingError,
    InjectedDependencyError,
    PassedArgumentError,
    IdentifierNotFoundError,
)
from kaxanuk.data_curator.data_providers import DataProviderInterface
from kaxanuk.data_curator.features import calculations
from kaxanuk.data_curator.output_handlers import OutputHandlerInterface
from kaxanuk.data_curator.services.column_builder import ColumnBuilder


def main(
    *,  # Force user to call function with keyword arguments
    configuration: Configuration,
    output_handlers: list[OutputHandlerInterface],
    custom_calculation_modules: list[types.ModuleType]|None = None,
    logger_level: int = logging.WARNING,
    logger_format: str = "[%(levelname)s] %(message)s",
    logger_file: str | os.PathLike | None = None,
    # @todo make this parameter required when we remove the deprecated ones:
    data_block_providers: dict[type[BaseDataBlock], DataProviderInterface] | None = None,
    master_clock_data_block: type[BaseDataBlock] | None = None,
    # deprecated parameters:
    market_data_provider: DataProviderInterface | None = None,
    fundamental_data_provider: DataProviderInterface | None = None,
) -> None:
    """
    Run the data curator system.

    Parameters
    ----------
    configuration
        Assembled Configuration entity containing the user's selected configurations
    output_handlers
        Objects that will handle the columnar data output, will be run one by one per each main_identifier
    custom_calculation_modules
        List of modules containing custom column calculation functions. Modules will be searched in order,
        with the function taken from the first module that declares it. If not found, the function will be
        searched in kaxanuk.data_curator.features.calculations
    logger_level
        All logs of priority logger_level or higher will be printed to stderr
    logger_format
        The format for the logger messages. will be injected to logging.basicConfig()
    logger_file
        An optional logger file to write the logging messages to. Accepts the same argument types as `os.fspath`
    data_block_providers
        Map of each data block class to the data provider object instance that will supply its data.
        The same data provider can be assigned to any number of data blocks. Any custom data block shadows
        each built-in data block sharing its prefixes, replacing it for the whole run.
    master_clock_data_block
        The data block whose dates all the other data blocks' rows will be synced against, and which must be one
        of the data_block_providers keys. Defaults to the built-in market daily data block, or to the custom data
        block that shadowed it.
    market_data_provider
        Deprecated, use data_block_providers instead. The market data provider object instance
    fundamental_data_provider
        Deprecated, use data_block_providers instead. The fundamental data provider object instance

    Returns
    -------
    None
    """
    if not isinstance(configuration, Configuration):
        msg = "Incorrect Configuration passed to main"

        raise InjectedDependencyError(msg)

    if not _is_valid_log_level(logger_level):
        msg = "Incorrect logger_level passed to main"

        raise PassedArgumentError(msg)

    logging.basicConfig(
        format=logger_format,
        level=logger_level,
        filename=logger_file
    )

    if market_data_provider is not None:
        warnings.warn(
            "The main() market_data_provider parameter is deprecated and will be removed in a future version."
            " Use data_block_providers instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    if fundamental_data_provider is not None:
        warnings.warn(
            "The main() fundamental_data_provider parameter is deprecated and will be removed in a future version."
            " Use data_block_providers instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    # @todo remove this once we remove the deprecated parameters
    if (
        data_block_providers is not None
        and (
            market_data_provider is not None
            or fundamental_data_provider is not None
        )
    ):
        msg = " ".join([
            "The deprecated market_data_provider and fundamental_data_provider parameters can't be passed to main",
            "together with data_block_providers, as they'd be ignored. Assign their data blocks in",
            "data_block_providers instead.",
        ])

        raise PassedArgumentError(msg)

    if data_block_providers is None:
        data_block_providers = {}
        if not isinstance(market_data_provider, DataProviderInterface):
            msg = "Market data provider passed to main doesn't implement DataProviderInterface"

            raise InjectedDependencyError(msg)

        data_block_providers[MarketDailyDataBlock] = market_data_provider

        if fundamental_data_provider is not None:
            if not isinstance(fundamental_data_provider, DataProviderInterface):
                msg = "Fundamental data provider passed to main doesn't implement DataProviderInterface"

                raise InjectedDependencyError(msg)

            data_block_providers[DividendsDataBlock] = fundamental_data_provider
            data_block_providers[FundamentalsDataBlock] = fundamental_data_provider
            data_block_providers[SplitsDataBlock] = fundamental_data_provider

    if not isinstance(data_block_providers, dict):
        msg = " ".join([
            "The data_block_providers passed to main is not a dict of data block classes to data providers:",
            repr(data_block_providers),
        ])

        raise PassedArgumentError(msg)

    if len(data_block_providers) < 1:
        msg = "The data_block_providers passed to main is empty, so there's no data to process"

        raise PassedArgumentError(msg)

    invalid_data_block_names = []
    for data_block in data_block_providers:
        if (
            inspect.isclass(data_block)
            and issubclass(data_block, BaseDataBlock)
        ):
            continue

        if inspect.isclass(data_block):
            invalid_data_block_names.append(data_block.__name__)
        else:
            invalid_data_block_names.append(
                repr(data_block)
            )

    if len(invalid_data_block_names) > 0:
        msg = " ".join([
            "The following data_block_providers keys passed to main are not data block classes:",
            ", ".join(invalid_data_block_names),
        ])

        raise PassedArgumentError(msg)

    if master_clock_data_block is not None:
        if (
            not inspect.isclass(master_clock_data_block)
            or not issubclass(master_clock_data_block, BaseDataBlock)
        ):
            msg = " ".join([
                "The master_clock_data_block passed to main is not a data block class:",
                repr(master_clock_data_block),
            ])

            raise PassedArgumentError(msg)

        if master_clock_data_block not in data_block_providers:
            passed_data_block_names = ", ".join(
                passed_data_block.__name__
                for passed_data_block in data_block_providers
            )
            msg = " ".join([
                f"The master_clock_data_block passed to main, {master_clock_data_block.__name__}, needs a data",
                "provider assigned to it in data_block_providers, which only has these data blocks:",
                passed_data_block_names,
            ])

            raise PassedArgumentError(msg)

    invalid_provider_descriptions = []
    for (data_block, data_provider) in data_block_providers.items():
        if isinstance(data_provider, DataProviderInterface):
            continue

        invalid_provider_descriptions.append(
            f"{data_block.__name__}: {type(data_provider).__name__}"
        )

    if len(invalid_provider_descriptions) > 0:
        msg = " ".join([
            "The following data_block_providers data providers passed to main",
            "don't implement DataProviderInterface:",
            ", ".join(invalid_provider_descriptions),
        ])

        raise InjectedDependencyError(msg)

    unsupplied_data_block_descriptions = []
    for (data_block, data_provider) in data_block_providers.items():
        if data_block in data_provider.get_data_block_endpoint_tag_map():
            continue

        unsupplied_data_block_descriptions.append(
            f"{data_block.__name__}: {type(data_provider).__name__}"
        )

    if len(unsupplied_data_block_descriptions) > 0:
        msg = " ".join([
            "The following data_block_providers data blocks passed to main can't be supplied by the data",
            "providers they were assigned to:",
            ", ".join(unsupplied_data_block_descriptions),
        ])

        raise PassedArgumentError(msg)

    if (
        len(output_handlers) < 1
        or not all(
            isinstance(output_handler, OutputHandlerInterface)
            for output_handler in output_handlers
        )
    ):
        msg = "One or more output handlers passed to main don't implement OutputHandlerInterface"

        raise InjectedDependencyError(msg)

    if custom_calculation_modules is None:
        custom_calculation_modules = []

    calculation_modules = [
        *custom_calculation_modules,
        calculations
    ]

    # @todo: make async using asyncio
    try:
        ColumnBuilder.initialize_data_blocks(
            calculation_modules=calculation_modules,
            configuration=configuration,
            data_blocks=list(data_block_providers.keys()),
            master_clock_data_block=master_clock_data_block,
        )

        # a data provider assigned to several data blocks only needs to be initialized once
        unique_data_providers = set(data_block_providers.values())
        for data_provider in unique_data_providers:
            data_provider.initialize(configuration=configuration)

        for main_identifier in configuration.identifiers:
            logging.getLogger(__name__).info(
                "Loading data for: %s",
                main_identifier
            )
            try:
                full_data_entities: list[BaseDataEntity] = [
                    data_provider.get_data_block_data(
                        main_identifier=main_identifier,
                        data_block=data_block,
                        configuration=configuration,
                    )
                    for (data_block, data_provider) in data_block_providers.items()
                ]
            except IdentifierNotFoundError as error:
                msg = "\n  ".join([
                    f"{main_identifier} skipping output as it presented the following error during data retrieval:",
                    str(error)
                ])
                logging.getLogger(__name__).error(msg)

                continue
            except EntityProcessingError as error:
                error_messages = _get_nested_exception_messages(error)
                msg = "\n  ".join([
                    f"{main_identifier} skipping output as it presented the following error during data assembly:",
                    ": ".join(error_messages)
                ])
                logging.getLogger(__name__).error(msg)

                continue
            except DataProviderPaymentError as error:
                msg = "\n  ".join([
                    f"{main_identifier} skipping output as it presented the following data provider error:",
                    str(error)
                ])
                logging.getLogger(__name__).error(msg)

                continue
            except DataBlockRowEntityErrorGroup as error_group:
                msg = "\n  ".join([
                    f"{main_identifier} skipping output as it presented the following errors during data assembly:",
                    str(error_group),
                    *[
                        str(error)
                        for error in error_group.exceptions
                    ]
                ])
                logging.getLogger(__name__).error(msg)

                continue

            column_builder = ColumnBuilder(
                data_entities=full_data_entities,
            )
            output_columns = column_builder.process_columns(configuration.columns)

            for output_handler in output_handlers:
                output_handler.output_data(
                    main_identifier=main_identifier,
                    columns=output_columns
                )

            logging.getLogger(__name__).info(
                "Output processed for: %s",
                main_identifier
            )
    except (
        ApiEndpointError,
        ColumnBuilderCircularDependenciesError,
        ColumnBuilderCustomFunctionNotFoundError,
        ColumnBuilderUnavailableEntityFieldError,
    ) as error:
        logging.getLogger(__name__).critical(str(error))
    else:
        logging.getLogger(__name__).info("Finished processing data!")


def _get_nested_exception_messages(
    nested_exception: Exception
) -> list[str]:
    """
    Unravels the nested exception, and creates a flat list of all the nested exception messages.

    Parameters
    ----------
    nested_exception
        A nested exception

    Returns
    -------
    The nested exception messages in a flat list
    """
    messages = []
    remaining_exception: Exception | BaseException | None = nested_exception
    while remaining_exception:
        messages.append(
            str(remaining_exception)
        )
        remaining_exception = remaining_exception.__cause__

    return messages


def _is_valid_log_level(level: int) -> bool:
    """
    Check if the received log level is valid.

    Parameters
    ----------
    level
        The level to check

    Returns
    -------
    Whether the received log level is valid
    """
    level_name = logging.getLevelName(level)

    return not level_name.startswith('Level ')
