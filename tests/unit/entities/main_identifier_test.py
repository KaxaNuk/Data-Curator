import pytest

from kaxanuk.data_curator.entities import MainIdentifier
from kaxanuk.data_curator.exceptions import EntityValueError


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
def test_correct_tickers(symbol):
    MainIdentifier(symbol)

@pytest.mark.parametrize(
    'symbol',
    [
        '',
        'HAZ SPACE',
        25.33
    ]
)
def test_incorrect_tickers(symbol):
    with pytest.raises(EntityValueError):
        MainIdentifier(symbol)
