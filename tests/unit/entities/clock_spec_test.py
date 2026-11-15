import pytest

from kaxanuk.data_curator import ReferencePeriods
from kaxanuk.data_curator.entities import ClockSpec
from kaxanuk.data_curator.exceptions import (
    EntityTypeError,
    EntityValueError,
)


class TestDunderPostInit:
    def test_valid_clock_spec(self):
        result = ClockSpec(
            reference_period=ReferencePeriods.MONTH,
            periods_per_row=3,
        )

        assert result.periods_per_row == 3

    def test_reference_period_that_is_not_reference_period_member(self):
        with pytest.raises(EntityTypeError):
            ClockSpec(
                reference_period='month',
                periods_per_row=3,
            )

    def test_float_periods_per_row(self):
        with pytest.raises(EntityTypeError):
            ClockSpec(
                reference_period=ReferencePeriods.MONTH,
                periods_per_row=1.5,
            )

    def test_bool_periods_per_row(self):
        with pytest.raises(EntityTypeError):
            ClockSpec(
                reference_period=ReferencePeriods.MONTH,
                periods_per_row=True,
            )

    def test_periods_per_row_below_one(self):
        with pytest.raises(EntityValueError):
            ClockSpec(
                reference_period=ReferencePeriods.MONTH,
                periods_per_row=0,
            )
