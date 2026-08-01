import datetime

from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    FundamentalData,
    MarketData,
    SplitData,
)
from kaxanuk.data_curator.services.data_provider_toolkit import DataBlockEndpointTagMap

class NotFoundDataProvider(DataProviderInterface):
    """
    Non-functional data provider returned by the extension handler when a data provider is not found.

    Used so Configruation handlers can display the correct error message when a data provider extension was not found.
    """

    @classmethod
    def get_data_block_endpoint_tag_map(cls) -> DataBlockEndpointTagMap:
        raise NotImplementedError

    def get_dividend_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        raise NotImplementedError

    def get_fundamental_data(
        self,
        *,
        main_identifier: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        raise NotImplementedError

    def get_market_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        raise NotImplementedError

    def get_split_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        raise NotImplementedError

    def initialize(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        raise NotImplementedError

    def validate_api_key(
        self,
    ) -> bool | None:
        raise NotImplementedError
