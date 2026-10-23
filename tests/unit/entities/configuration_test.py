import datetime

import pytest

from kaxanuk.data_curator.entities import Configuration
from kaxanuk.data_curator.exceptions import ConfigurationError


class TestDunderPostInit:
    @pytest.mark.parametrize(
        'column',
        [
            'm_close',
            'fis_revenues',
            'c_test',
        ]
    )
    def test_post_init_built_in_prefix_column(self, column):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 12, 31),
            period='quarterly',
            identifiers=('AAPL',),
            columns=(column,),
        )

        assert configuration.columns == (column,)

    @pytest.mark.parametrize(
        'column',
        [
            'alt_short_interest',
            'x2_metric_5d',
        ]
    )
    def test_post_init_custom_prefix_column(self, column):
        configuration = Configuration(
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 12, 31),
            period='quarterly',
            identifiers=('AAPL',),
            columns=(column,),
        )

        assert configuration.columns == (column,)

    @pytest.mark.parametrize(
        'column',
        [
            'close',
            '_close',
            'M_close',
            '2m_close',
            'm_',
            'm_close price',
        ]
    )
    def test_post_init_incorrectly_formatted_column(self, column):
        with pytest.raises(ConfigurationError):
            Configuration(
                start_date=datetime.date(2024, 1, 1),
                end_date=datetime.date(2024, 12, 31),
                period='quarterly',
                identifiers=('AAPL',),
                columns=(column,),
            )
