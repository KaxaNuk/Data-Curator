"""
Package containing all our custom Exceptions.
"""


class DataCuratorError(Exception):
    pass


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


class DataColumnError(DataCuratorError):
    pass


class DataColumnParameterError(DataColumnError):
    pass


class DataProviderMissingKeyError(DataCuratorError):
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
