"""
Interface for all financial data providers.
"""

import abc
import datetime
import http
import logging
import ssl
import time
import typing
import urllib.error
import urllib.parse
import urllib.request

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
    DataProviderPaymentError,
    IdentifierNotFoundError,
    InjectedDependencyError,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataBlockEndpointTagMap


class DataProviderInterface(metaclass=abc.ABCMeta):
    """
    Interface for all the data providers.

    Any data provider implementing this interface MUST implement all of the methods below marked with
    the @abc.abstractmethod decorator.

    After the abstract method definitions we have some concrete helpers and attributes that can be
    optionally used inside any method that requires them, or they can be overriden by the implementing
    class.
    """

    # Abstract methods that need to be implemented by any provider class:

    @classmethod
    @abc.abstractmethod
    def get_data_block_endpoint_tag_map(cls) -> DataBlockEndpointTagMap:
        """
        Return the endpoint field map that supplies each of the data blocks this provider can serve.

        The keys are the authoritative list of the data blocks this provider can supply, so a data block this
        provider serves without using the data provider toolkit still needs to be a key, even if its endpoint
        field map is empty.

        Returns
        -------
        Each data block class mapped to the provider tags of the endpoint fields that populate its entity fields
        """

    # @todo make this method abstract and remove its whole body once all the data providers implement it
    def get_data_block_data(
        self,
        *,
        main_identifier: str,
        data_block: type[BaseDataBlock],
        configuration: Configuration,
    ) -> BaseDataEntity:
        """
        Return the `data_block` data of `main_identifier`.

        DEPRECATED IMPLEMENTATION: data providers that haven't implemented this method yet fall back to the
        data block specific methods they used to implement, which only cover the built-in data blocks.

        Parameters
        ----------
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        data_block
            The data block class whose data entity we're returning
        configuration
            The Configuration entity with all the currently injected settings

        Returns
        -------
        The main data entity of `data_block`, containing the data

        Raises
        ------
        InjectedDependencyError
        """
        if data_block is DividendsDataBlock:

            return self.get_dividend_data(
                main_identifier=main_identifier,
                start_date=configuration.start_date,
                end_date=configuration.end_date,
            )

        if data_block is FundamentalsDataBlock:

            return self.get_fundamental_data(
                main_identifier=main_identifier,
                period=configuration.period,
                start_date=configuration.start_date,
                end_date=configuration.end_date,
            )

        if data_block is MarketDailyDataBlock:

            return self.get_market_data(
                main_identifier=main_identifier,
                start_date=configuration.start_date,
                end_date=configuration.end_date,
            )

        if data_block is SplitsDataBlock:

            return self.get_split_data(
                main_identifier=main_identifier,
                start_date=configuration.start_date,
                end_date=configuration.end_date,
            )

        msg = " ".join([
            f"The {type(self).__name__} data provider can only supply the built-in data blocks,",
            f"so it can't supply {data_block.__name__} until it implements get_data_block_data",
        ])

        raise InjectedDependencyError(msg)

    @abc.abstractmethod
    def initialize(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        """
        Run any required setup logic here before the identifiers' processing loop.

        Data providers that allow bulk downloads can download and cache all the data here, and then just format it
        for every identifier in the corresponding get data type method.

        Parameters
        ----------
        configuration
            The Configuration entity with all the currently injected settings

        Returns
        -------
        None
        """

    @abc.abstractmethod
    def validate_api_key(
        self,
    ) -> bool | None:
        """
        Validate that the API key used to init the class is valid.

        If the provider doesn't use API keys, simply return None

        Returns
        -------
        Whether `api_key` is valid
        """

    # Concrete helper attributes and methods:

    _ssl_context = None
    _MAX_CONNECTION_RETRIES = 5
    _REQUEST_RETRY_TIME = 0.250  # seconds

    @staticmethod
    def _build_url_with_identifier_path_and_query_params(
        endpoint_url: str,
        main_identifier: str,
        params: dict[str, str]
    ) -> str:
        """
        Build a URL with the `main_identifier` at the end of the path and `params` as the query parameters.

        This is a common way of structuring identifier (ticker, etc.) data web service APIs

        Parameters
        ----------
        endpoint_url
            The base URL of the endpoint
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        params
            The parameters that will be used as the query key-values

        Returns
        -------
        The assembled URL string
        """
        consolidated_params = (
            f'{key}={urllib.parse.quote(str(value))}'
            for (key, value) in params.items()
        )
        url_suffix = '&'.join(consolidated_params)
        identifier_encoded = urllib.parse.quote(main_identifier)

        return f'{endpoint_url}/{identifier_encoded}?{url_suffix}'

    @staticmethod
    def _build_url_with_query_params(
        endpoint_url: str,
        main_identifier: str,
        params: dict[str, str]
    ) -> str:
        """
        Build a URL with `params` as the query parameters.

        This is a common way of structuring identifier (ticker, etc.) data web service APIs

        Parameters
        ----------
        endpoint_url
            The base URL of the endpoint
        main_identifier
            Ignored
        params
            The parameters that will be used as the query key-values

        Returns
        -------
        The assembled URL string
        """
        consolidated_params = (
            f'{key}={urllib.parse.quote(str(value))}'
            for (key, value) in params.items()
        )
        url_suffix = '&'.join(consolidated_params)

        return f'{endpoint_url}?{url_suffix}'

    @staticmethod
    def _find_first_date_before_start_date(
        dates: list[str | datetime.date | None],
        start_date: str | datetime.date,
        *,
        descending_order: bool = False,
    ) -> str | datetime.date:
        """
        Find the first date before start_date in an iterable of date strings in format 'YYYY-MM-DD'.

        Parameters
        ----------
        dates
            datetime dates or strings in 'YYYY-MM-DD' format, can include Nones
        descending_order
            whether the dates are in descending order or not
        start_date
            datetime date or string in 'YYYY-MM-DD' format

        Returns
        -------
        the first date string before start_date, or start_date if no dates found before it

        """
        descending_dates = dates if descending_order else reversed(dates)

        return next(
            (
                d
                for d in descending_dates
                if (
                    d is not None
                    and d < start_date
            )
            ),
            start_date
        )

    @staticmethod
    def _find_unordered_dates(
        dates: list[str | datetime.date | None],
        *,
        descending_order: bool = False,
    ) -> list[str | datetime.date]:
        """
        Find the date strings in 'YYYY-MM-DD' format in descending order that are out of order.

        Parameters
        ----------
        dates
            datetime dates or strings in 'YYYY-MM-DD' format, can include Nones
        descending_order
            whether the dates are in descending order or not

        Returns
        -------
        the dates that are out of order
        """
        descending_dates = dates if descending_order else reversed(dates)

        unordered_dates: list[str] = []
        last_ordered_date = None
        for desc_date in descending_dates:
            if (
                last_ordered_date is not None
                and desc_date is not None
                and desc_date > last_ordered_date
            ):
                unordered_dates.append(desc_date)
            elif desc_date is not None:
                last_ordered_date = desc_date

        if descending_order:
            return unordered_dates
        else:
            return list(
                reversed(unordered_dates)
            )

    @classmethod
    def _load_ssl_context(cls) -> ssl.SSLContext:
        """
        Load an SSL context for HTTP requests.

        Returns
        -------
        The loaded SSL context
        """
        if cls._ssl_context is not None:
            return cls._ssl_context

        cls._ssl_context = ssl.create_default_context()
        cls._ssl_context.check_hostname = True
        cls._ssl_context.verify_mode = ssl.CERT_REQUIRED

        return cls._ssl_context

    @classmethod
    def _request_data(
        cls,
        endpoint_id: str,
        endpoint_url: str,
        main_identifier: str,
        params: dict[str, str],
        url_builder: typing.Callable[
            [str, str, dict[str, str]],
            str
        ] = _build_url_with_query_params
    ) -> str | None:
        """
        Return the raw data from the webservice endpoint, with the URL assembled by `url_builder`.

        Parameters
        ----------
        endpoint_id
            the internal name of the endpoint, for error logging purposes
        endpoint_url
            the base URL of the endpoint
        main_identifier
            The security's main identifier (ticker, etc.) used by the data provider
        params
            parameters to append to the url
        url_builder
            The function that will be used to assemble the final URL.
            `cls._build_url_with_identifier_path_and_query_params` by default

        Returns
        -------
        The raw data from the webservice endpoint, or None on error
        """
        url = url_builder(
            endpoint_url,
            main_identifier,
            params
        )
        attempt_number = 0
        response = None

        while attempt_number < cls._MAX_CONNECTION_RETRIES:
            attempt_number += 1
            try:
                # @todo: extract network connections to module, for unit testing etc.
                http_request = urllib.request.Request(url)
                with urllib.request.urlopen(
                    http_request,
                    context=cls._load_ssl_context()
                ) as http_response:
                    response = http_response.read().decode('utf-8')

                if response:
                    break
            except (
                urllib.error.URLError,
                urllib.error.HTTPError,
                urllib.error.ContentTooShortError
            ) as error:
                error_message = str(error)
                if hasattr(error, 'code'):
                    if (
                        error.code == http.HTTPStatus.NOT_FOUND.value
                        and "No data found" in error_message
                    ):
                        msg = f"API Error accessing endpoint {endpoint_id}, returned error {error_message}"

                        raise IdentifierNotFoundError(msg) from error
                    elif error.code == http.HTTPStatus.PAYMENT_REQUIRED.value:
                        detailed_error_message = error.read().decode('utf-8')
                        if len(detailed_error_message) < 1:
                            detailed_error_message = error_message

                        raise DataProviderPaymentError(detailed_error_message) from error

                    if (
                        error.code < http.HTTPStatus.INTERNAL_SERVER_ERROR.value # client error, so no point in retrying
                    ):
                        msg = " ".join([
                            f"Data provider server error accessing endpoint {endpoint_id},",
                            f"returned HTTP code {error.code}",
                            (
                                f"with message {error_message}"
                                if len(error_message) > 0
                                else ""
                            )
                        ])

                        raise ApiEndpointError(msg) from error

                if (
                    attempt_number == (cls._MAX_CONNECTION_RETRIES - 1)  # last attempt
                ):
                    msg = " ".join([
                        f"Data provider server error accessing endpoint {endpoint_id}",
                        (
                            f"with message {error_message}"
                            if len(error_message) > 0
                            else ""
                        )
                    ])

                    raise ApiEndpointError(msg) from error
                else:
                    msg = f"API Server error on endpoint {endpoint_id}, retrying request attempt {attempt_number}"
                    logging.getLogger(__name__).warning(msg)
                    time.sleep(cls._REQUEST_RETRY_TIME)

        return response
