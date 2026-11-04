import pytest

from kaxanuk.data_curator.entities import MainIdentifier
from kaxanuk.data_curator.exceptions import EntityValueError


class TestDunderPostInit:
    @pytest.mark.parametrize(
        'symbol',
        [
            'F',
            'TSLA',
            'nvda',
            'ORR.AX',
            '^GSPC',
            'ETHMUSD',
            'SUNPHARMA*',
            'IBM-B',
            '9744',
        ]
    )
    def test_correct_tickers(
        self,
        symbol,
    ):
        MainIdentifier(symbol)

    @pytest.mark.parametrize(
        'symbol',
        [
            '',
            'HAZ SPACE',
            25.33
        ]
    )
    def test_incorrect_tickers(
        self,
        symbol,
    ):
        with pytest.raises(EntityValueError):
            MainIdentifier(symbol)
