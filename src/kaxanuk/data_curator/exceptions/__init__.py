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


class LSEGProviderError(DataCuratorError):
    """
    Base class for all LSEG provider exceptions.

    Provides common attributes for LSEG API error classification
    and retry decision-making.

    Parameters
    ----------
    http_code : int | None
        The HTTP or LSEG-specific error code, if available.
    message : str
        Detailed description of the error.
    retryable : bool
        Whether this error type can be retried.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    retryable : bool
        Whether this error type is suitable for retry.
    """

    def __init__(
        self,
        http_code: int | None,
        message: str,
        *,
        retryable: bool,
    ) -> None:
        self.http_code = http_code
        self.retryable = retryable
        msg = "LSEG error"
        if http_code is not None:
            msg += f" (code {http_code})"
        msg += f": {message}"
        super().__init__(msg)


class LSEGDataNotFoundError(LSEGProviderError):
    """
    Raised when requested data is not available for the given tickers.

    This exception is raised when the API returns successfully but contains
    no data for the requested instruments.

    Parameters
    ----------
    tickers : list[str]
        List of ticker symbols (RICs) for which data was not found.
    message : str, optional
        Custom error message. If not provided, a default message listing
        the tickers will be generated.

    Attributes
    ----------
    tickers : list[str]
        The ticker symbols that had no data available.
    """

    def __init__(
        self,
        tickers: list[str],
        message: str | None = None
    ) -> None:
        self.tickers = tickers
        msg = message or f"No data found for tickers: {', '.join(tickers)}"
        super().__init__(http_code=None, message=msg, retryable=False)


class LSEGFatalError(LSEGProviderError):
    """
    Raised on fatal errors that should not be retried.

    Certain LSEG API errors indicate fundamental issues that cannot be resolved
    by retrying, such as invalid field parameters (error code 207), bad request
    parameters (error code 400) or unrecognized PERIOD values.

    Parameters
    ----------
    error_code : int | None
        The LSEG error code, if available.
    message : str
        Detailed description of the fatal error.

    Attributes
    ----------
    error_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, error_code: int | None, message: str) -> None:
        self.error_code = error_code
        super().__init__(http_code=error_code, message=message, retryable=False)


class LSEGAuthenticationError(LSEGProviderError):
    """
    Raised on authentication failures requiring token refresh.

    Triggered by HTTP 401 responses or 400 responses containing
    authentication-related error messages (e.g., ``invalid_grant``,
    ``invalid_client``).

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 401 or 400).
    message : str
        Detailed description of the authentication failure.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class LSEGSessionQuotaError(LSEGAuthenticationError):
    """
    Raised when the LSEG session quota has been exceeded.

    This is a specific authentication-related error triggered by
    400 responses containing "session quota" in the error message.

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 400).
    message : str
        Detailed description of the session quota error.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message)


class LSEGAuthorizationError(LSEGProviderError):
    """
    Raised on authorization failures that cannot be resolved by retrying.

    Triggered by HTTP 403 responses indicating the user does not have
    permission to access the requested resource.

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 403).
    message : str
        Detailed description of the authorization failure.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=False)


class LSEGRateLimitError(LSEGProviderError):
    """
    Raised when the API rate limit has been exceeded.

    Triggered by HTTP 429 or 503 responses. The caller should wait
    before retrying the same request.

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 429 or 503).
    message : str
        Detailed description of the rate limit error.
    retry_after : float | None
        Suggested wait time in seconds before retrying, if provided
        by the API response.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    retry_after : float | None
        Suggested wait time in seconds before retrying.
    """

    def __init__(
        self,
        http_code: int | None,
        message: str,
        *,
        retry_after: float | None = None,
    ) -> None:
        self.retry_after = retry_after
        super().__init__(http_code=http_code, message=message, retryable=True)


class LSEGPlatformOverloadError(LSEGProviderError):
    """
    Raised when the LSEG backend is overloaded.

    Triggered by 400 responses containing "Backend error" in the message.
    This error indicates the request should be retried with a smaller
    ticker batch (ticker chunking).

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 400).
    message : str
        Detailed description of the backend overload error.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class LSEGGatewayTimeoutError(LSEGProviderError):
    """
    Raised on LSEG gateway timeout errors.

    Triggered by error code 2504. This error indicates the request
    should be retried with a smaller ticker batch (ticker chunking).

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 2504).
    message : str
        Detailed description of the gateway timeout error.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class LSEGServerError(LSEGProviderError):
    """
    Raised on generic retryable server errors.

    Triggered by HTTP 500, 408, or other unclassified error codes.
    The caller should retry with standard exponential backoff.

    Parameters
    ----------
    http_code : int | None
        The HTTP error code.
    message : str
        Detailed description of the server error.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the LSEG API.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class LSEGDataTruncationError(LSEGProviderError):
    """
    Raised when silent data truncation is detected in the API response.

    This occurs when the API returns data for fewer tickers than
    were requested, without reporting an error.

    Parameters
    ----------
    missing_tickers : list[str]
        List of ticker symbols that were requested but not returned.
    message : str, optional
        Custom error message. If not provided, a default message listing
        the missing tickers will be generated.

    Attributes
    ----------
    missing_tickers : list[str]
        The ticker symbols that were silently dropped from the response.
    """

    def __init__(
        self,
        missing_tickers: list[str],
        message: str | None = None,
    ) -> None:
        self.missing_tickers = missing_tickers
        msg = message or (
            f"Response truncated: missing {len(missing_tickers)} tickers: "
            f"{', '.join(missing_tickers)}"
        )
        super().__init__(http_code=None, message=msg, retryable=True)
