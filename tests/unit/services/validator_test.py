import pytest

from kaxanuk.data_curator.services import validator


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ("2022-01-01", True),
        ("1954-12-31", True),
        ("", False),
        (24, False),
        ("2022/01/01", False),
        ("01-01-2022", False),
        ("2022-1-7", False),
    ]
)
def test_is_date_pattern(value, expected):
    assert validator.is_date_pattern(value) == expected
