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


class ColumnBuilderNoDatesToInfillError(DataCuratorError):
    pass


class ColumnBuilderUnavailableEntityFieldError(DataCuratorError):
    pass


class ConfigurationError(DataCuratorError):
    pass


class ConfigurationHandlerError(DataCuratorError):
    pass


class DataBlockEmptyError(DataCuratorError):
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


class DataBlockError(DataCuratorError):
    pass


class DataBlockIncorrectMappingTypeError(DataCuratorError):
    pass


class DataBlockIncorrectPackingStructureError(DataCuratorUnhandledError):
    pass


class DataBlockTypeConversionError(DataCuratorError):
    pass


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


class DataColumnError(DataCuratorError):
    pass


class DataColumnParameterError(DataColumnError):
    pass


class DataCuratorErrorGroup(ExceptionGroup):
    pass


class DataBlockRowEntityErrorGroup(DataCuratorErrorGroup):
    pass


class DataProviderApiError(DataCuratorError):
    """
    Base class for data provider API exceptions with error classification.

    Provides common attributes for API error classification and retry
    decision-making. Any data provider can use this hierarchy to represent
    typed API errors.

    Parameters
    ----------
    http_code : int | None
        The HTTP or provider-specific error code, if available.
    message : str
        Detailed description of the error.
    retryable : bool
        Whether this error type can be retried.

    Attributes
    ----------
    http_code : int | None
        The numeric error code from the API.
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
        msg = "Data provider error"
        if http_code is not None:
            msg += f" (code {http_code})"
        msg += f": {message}"
        super().__init__(msg)


class DataProviderAuthenticationError(DataProviderApiError):
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
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class DataProviderAuthorizationError(DataProviderApiError):
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
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=False)


class DataProviderConnectionError(DataCuratorError):
    pass


class DataProviderDataNotFoundError(DataProviderApiError):
    """
    Raised when requested data is not available for the given tickers.

    The API returned successfully but contains no data for the
    requested instruments.

    Parameters
    ----------
    tickers : list[str]
        List of ticker symbols for which data was not found.
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


class DataProviderDataTruncationError(DataProviderApiError):
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


class DataProviderFatalError(DataProviderApiError):
    """
    Raised on fatal errors that should not be retried.

    Indicates fundamental issues that cannot be resolved by retrying,
    such as invalid field parameters, bad request parameters, or
    unrecognized query values.

    Parameters
    ----------
    error_code : int | None
        The provider error code, if available.
    message : str
        Detailed description of the fatal error.

    Attributes
    ----------
    error_code : int | None
        The numeric error code from the provider API.
    """

    def __init__(self, error_code: int | None, message: str) -> None:
        self.error_code = error_code
        super().__init__(http_code=error_code, message=message, retryable=False)


class DataProviderGatewayTimeoutError(DataProviderApiError):
    """
    Raised on gateway timeout errors.

    Indicates the upstream server did not respond in time. The request
    should be retried with a smaller ticker batch.

    Parameters
    ----------
    http_code : int | None
        The HTTP or provider-specific timeout error code.
    message : str
        Detailed description of the gateway timeout error.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class DataProviderIncorrectMappingTypeError(DataCuratorError):
    pass


class DataProviderMissingKeyError(DataCuratorError):
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


class DataProviderOverloadError(DataProviderApiError):
    """
    Raised when the data provider backend is overloaded.

    Triggered by responses indicating backend capacity issues.
    The request should be retried with a smaller ticker batch.

    Parameters
    ----------
    http_code : int | None
        The HTTP error code.
    message : str
        Detailed description of the backend overload error.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class DataProviderParsingError(DataCuratorError):
    pass


class DataProviderPaymentError(DataProviderConnectionError):
    pass


class DataProviderRateLimitError(DataProviderApiError):
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


class DataProviderServerError(DataProviderApiError):
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
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message, retryable=True)


class DataProviderSessionQuotaError(DataProviderAuthenticationError):
    """
    Raised when the provider session quota has been exceeded.

    A specific authentication-related error triggered by responses
    indicating the maximum number of concurrent sessions has been reached.

    Parameters
    ----------
    http_code : int | None
        The HTTP error code (typically 400).
    message : str
        Detailed description of the session quota error.
    """

    def __init__(self, http_code: int | None, message: str) -> None:
        super().__init__(http_code=http_code, message=message)


class DataProviderToolkitError(DataCuratorError):
    pass


class DataProviderToolkitArgumentError(DataProviderToolkitError):
    pass


class DataProviderToolkitNoDataError(DataProviderToolkitError):
    pass


class DataProviderToolkitRuntimeError(DataProviderToolkitError):
    pass


class DataProviderTooManyTickersError(ApiEndpointError):
    """
    Raised when the data provider cannot process the requested ticker list
    even after internal chunking and retries.

    This signals to the orchestrator (via ApiEndpointError) that the
    user should reduce the number of tickers in their configuration.

    Parameters
    ----------
    total : int
        Total number of tickers that were requested.
    failed : int
        Number of tickers that failed after all retry attempts.
    message : str, optional
        Custom error message.
    """

    def __init__(self, total: int, failed: int, message: str | None = None) -> None:
        self.total = total
        self.failed = failed
        msg = message or (
            f"Data provider failed for {failed}/{total} tickers after internal "
            f"chunking. Reduce the number of tickers in your configuration."
        )
        super().__init__(msg)


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
