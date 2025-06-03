"""
KaxaNuk Data Curator: Request, combine and save financial data from different provider web services.

Requires an entry script that injects the required dependencies
cf. __main__.py on the GitHub repository root

Functions
---------
main:
    Receives injected dependencies and runs the system
"""

import logging
import os
import types

from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    FundamentalData,
    SplitData,
    MainIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    ApiEndpointError,
    ColumnBuilderCircularDependenciesError,
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderUnavailableEntityFieldError,
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
    market_data_provider: DataProviderInterface,
    fundamental_data_provider: DataProviderInterface | None,
    output_handlers: list[OutputHandlerInterface],
    custom_calculation_modules: list[types.ModuleType]|None = None,
    logger_level: int = logging.WARNING,
    logger_format: str = "[%(levelname)s] %(message)s",
    logger_file: str | bytes | os.PathLike | None = None,
) -> None:
    """
    Run the data curator system.

    Parameters
    ----------
    configuration
        Assembled Configuration entity containing the user's selected configurations
    market_data_provider
        The market data provider object instance
    fundamental_data_provider
        The fundamental data provider object instance
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

    Returns
    -------
    None
    """
    if not isinstance(configuration, Configuration):
        raise InjectedDependencyError("Incorrect Configuration passed to main")

    if not _is_valid_log_level(logger_level):
        raise PassedArgumentError("Incorrect logger_level passed to main")

    logging.basicConfig(
        format=logger_format,
        level=logger_level,
        filename=logger_file
    )

    if not isinstance(market_data_provider, DataProviderInterface):
        raise InjectedDependencyError(
            "Market data provider passed to main doesn't implement FinancialDataProviderInterface"
        )

    if (
        fundamental_data_provider is not None
        and not isinstance(fundamental_data_provider, DataProviderInterface)
    ):
        raise InjectedDependencyError(
            "Fundamental data provider passed to main doesn't implement FinancialDataProviderInterface"
        )

    if (
        len(output_handlers) < 1
        or not all(
            isinstance(output_handler, OutputHandlerInterface)
            for output_handler in output_handlers
        )
    ):
        raise InjectedDependencyError(
            "One or more output handlers passed to main don't implement OutputHandlerInterface"
        )

    if custom_calculation_modules is None:
        custom_calculation_modules = []

    calculation_modules = [
        *custom_calculation_modules,
        calculations
    ]

    # @todo: make async using asyncio
    try:
        market_data_provider.initialize(configuration=configuration)

        if fundamental_data_provider is not None:
            fundamental_data_provider.initialize(configuration=configuration)

        for main_identifier in configuration.identifiers:
            logging.getLogger(__name__).info(
                "Loading data for: %s",
                main_identifier
            )
            try:
                full_market_data = market_data_provider.get_market_data(
                    main_identifier=main_identifier,
                    start_date=configuration.start_date,
                    end_date=configuration.end_date,
                )
                if fundamental_data_provider is not None:
                    full_fundamental_data = fundamental_data_provider.get_fundamental_data(
                        main_identifier=main_identifier,
                        period=configuration.period,
                        start_date=configuration.start_date,
                        end_date=configuration.end_date,
                    )
                    full_dividend_data = fundamental_data_provider.get_dividend_data(
                        main_identifier=main_identifier,
                        start_date=configuration.start_date,
                        end_date=configuration.end_date,
                    )
                    full_split_data = fundamental_data_provider.get_split_data(
                        main_identifier=main_identifier,
                        start_date=configuration.start_date,
                        end_date=configuration.end_date,
                    )
                else:
                    full_fundamental_data = FundamentalData(
                        main_identifier=MainIdentifier(main_identifier),
                        rows={}
                    )
                    full_dividend_data = DividendData(
                        main_identifier=MainIdentifier(main_identifier),
                        rows={}
                    )
                    full_split_data = SplitData(
                        main_identifier=MainIdentifier(main_identifier),
                        rows={}
                    )
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

            column_builder = ColumnBuilder(
                calculation_modules=calculation_modules,
                configuration=configuration,
                dividend_data=full_dividend_data,
                fundamental_data=full_fundamental_data,
                market_data=full_market_data,
                split_data=full_split_data,
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
