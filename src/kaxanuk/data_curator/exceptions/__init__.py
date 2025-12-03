"""
Package containing all our custom Exceptions.
"""
import datetime
import typing

import pyarrow


class DataCuratorError(Exception):
    """Base class for all Data Curator exceptions."""


class DataCuratorUnhandledError(DataCuratorError):
    """Base class for exceptions that are not meant to be handled, but should crash the system."""


class ApiEndpointError(DataCuratorError):
    pass


class CalculationError(DataCuratorError):
    pass


class CalculationHelperError(CalculationError):
    pass


class ColumnBuilderCircularDependenciesError(DataCuratorError):
    pass


class ColumnBuilderCustomFunctionNotFoundError(DataCuratorError):
    pass

class ColumnBuilderUnavailableEntityFieldError(DataCuratorError):
    pass

class ColumnBuilderNoDatesToInfillError(DataCuratorError):
    pass


class ConfigurationError(DataCuratorError):
    pass


class ConfigurationHandlerError(DataCuratorError):
    pass


class DataBlockEmptyError(DataCuratorError):
    pass


class DataBlockError(DataCuratorError):
    pass


class DataBlockIncorrectMappingTypeError(DataCuratorError):
    pass


class DataBlockEntityPackingError(DataCuratorError):
    def __init__(
        self,
        entity_name: str,
        clock_sync_value: datetime.date,
    ):
        self.entity_name = entity_name
        self.clock_sync_value = clock_sync_value
        super().__init__(
            f"Error during entity packing for {entity_name} @ {clock_sync_value}"
        )


class DataBlockIncorrectPackingStructureError(DataCuratorUnhandledError):
    pass


class DataBlockTypeConversionError(DataCuratorError):
    pass


class DataBlockTypeConversionRuntimeError(DataCuratorError):
    def __init__(
        self,
        conversion_type: str,
        original_value: typing.Any
    ):
        self.conversion_type = conversion_type
        self.original_value = original_value
        super().__init__(
            f"Error during value -> type conversion for {original_value} -> {conversion_type}"
        )


class DataBlockTypeConversionNotImplementedError(DataBlockTypeConversionError):
    def __init__(
        self,
        conversion_type: str,
        original_value: typing.Any
    ):
        self.conversion_type = conversion_type
        self.original_value = original_value
        super().__init__(
            f"Unsupported value -> type conversion for {original_value} -> {conversion_type}"
        )


class DataColumnError(DataCuratorError):
    pass


class DataColumnParameterError(DataColumnError):
    pass


class DataProviderMissingKeyError(DataCuratorError):
    pass


class DataProviderConnectionError(DataCuratorError):
    pass


class DataProviderIncorrectMappingTypeError(DataCuratorError):
    pass


class DataProviderMultiEndpointCommonDataDiscrepancyError(DataCuratorError):
    def __init__(
        self,
        discrepant_columns: set[str],
        discrepancies_table: pyarrow.Table,
        key_column_names: list[str],
    ):
        self.discrepant_columns = discrepant_columns
        self.discrepancies_table = discrepancies_table
        self.key_column_names = key_column_names
        super().__init__(
            "Discrepancies found between common columns across multiple endpoints."
        )


class DataProviderMultiEndpointCommonDataOrderError(DataCuratorError):
    pass


class DataProviderParsingError(DataCuratorError):
    pass


class DataProviderPaymentError(DataProviderConnectionError):
    pass


class DataProviderToolkitError(DataCuratorError):
    pass


class DataProviderToolkitArgumentError(DataProviderToolkitError):
    pass


class DataProviderToolkitNoDataError(DataProviderToolkitError):
    pass


class DataProviderToolkitRuntimeError(DataProviderToolkitError):
    pass


class DividendDataEmptyError(DataCuratorError):
    pass


class DividendDataRowError(DataCuratorError):
    pass


class EntityFieldTypeError(DataCuratorError):
    pass


class EntityProcessingError(DataCuratorError):
    pass


class EntityTypeError(DataCuratorError):
    pass


class EntityValueError(DataCuratorError):
    pass


class ExtensionFailedError(DataCuratorError):
    pass


class ExtensionNotFoundError(DataCuratorError):
    pass


class FileNameError(DataCuratorError):
    pass


class FundamentalDataNoIncomeError(DataCuratorError):
    def __init__(self):
        msg = "No income data obtained for the selected period"
        super().__init__(msg)


class FundamentalDataNonChronologicalStatementWithoutOriginalDateError(DataCuratorError):
    def __init__(self):
        msg = "Non-chronological (possible ammendment) statement found without original date"
        super().__init__(msg)


class FundamentalDataUnsortedRowDatesError(DataCuratorError):
    def __init__(self):
        msg = " ".join([
                "FundamentalData.rows are not correctly sorted by date,",
                "this usually indicates missing or amended statements"
            ])
        super().__init__(msg)


class IdentifierNotFoundError(DataCuratorError):
    pass


class InjectedDependencyError(DataCuratorError):
    pass


class MarketDataEmptyError(DataCuratorError):
    pass


class MarketDataRowError(DataCuratorError):
    pass


class OutputHandlerError(DataCuratorError):
    pass


class PassedArgumentError(DataCuratorError):
    pass


class SplitDataEmptyError(DataCuratorError):
    pass


class SplitDataRowError(DataCuratorError):
    pass


class DataCuratorErrorGroup(ExceptionGroup):
    pass


class DataBlockRowEntityErrorGroup(DataCuratorErrorGroup):
    pass
