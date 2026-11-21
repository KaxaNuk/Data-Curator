import types

import pytest

from kaxanuk.data_curator.data_curator import _format_calculation_error_message
from kaxanuk.data_curator.exceptions import ColumnBuilderCalculationError
from kaxanuk.data_curator.features import calculations


CUSTOM_CALCULATION_MODULE = types.ModuleType('custom_calculations')
CUSTOM_CALCULATION_MODULE.__file__ = 'Config/custom_calculations.py'


def failing_calculation() -> None:
    """
    Raise the error of a failing calculation.
    """
    msg = "Test calculation error"

    raise ValueError(msg)


def raise_calculation_error(calculation_module: types.ModuleType) -> None:
    """
    Call the failing calculation, reporting its error like the ColumnBuilder does.
    """
    try:
        failing_calculation()
    except ValueError as error:
        msg = "Calculation c_failing_calculation raised ValueError: Test calculation error"

        raise ColumnBuilderCalculationError(
            msg,
            calculation_module=calculation_module,
            column='c_failing_calculation',
        ) from error


@pytest.fixture
def built_in_calculation_error():
    with pytest.raises(ColumnBuilderCalculationError) as error_info:
        raise_calculation_error(calculations)

    return error_info.value


@pytest.fixture
def custom_calculation_error():
    with pytest.raises(ColumnBuilderCalculationError) as error_info:
        raise_calculation_error(CUSTOM_CALCULATION_MODULE)

    return error_info.value


class TestPrivateFormatCalculationErrorMessage:
    def test_custom_calculation(self, custom_calculation_error):
        result = _format_calculation_error_message(
            custom_calculation_error,
            custom_calculation_modules=[CUSTOM_CALCULATION_MODULE],
            main_identifier='TEST',
        )
        expected = " ".join([
            "Custom calculation c_failing_calculation in Config/custom_calculations.py",
            "failed for TEST, stopping the run:",
        ])

        assert result.startswith(expected)

    def test_built_in_calculation(self, built_in_calculation_error):
        result = _format_calculation_error_message(
            built_in_calculation_error,
            custom_calculation_modules=[CUSTOM_CALCULATION_MODULE],
            main_identifier='TEST',
        )

        assert result.startswith("Built-in calculation c_failing_calculation in ")

    def test_traceback_from_calculation_function(self, custom_calculation_error):
        result = _format_calculation_error_message(
            custom_calculation_error,
            custom_calculation_modules=[CUSTOM_CALCULATION_MODULE],
            main_identifier='TEST',
        )

        assert 'in failing_calculation' in result

    def test_traceback_without_calling_frame(self, custom_calculation_error):
        # The frame calling the calculation is the Data Curator's own
        result = _format_calculation_error_message(
            custom_calculation_error,
            custom_calculation_modules=[CUSTOM_CALCULATION_MODULE],
            main_identifier='TEST',
        )

        assert 'in raise_calculation_error' not in result

    def test_calculation_error_message(self, custom_calculation_error):
        result = _format_calculation_error_message(
            custom_calculation_error,
            custom_calculation_modules=[CUSTOM_CALCULATION_MODULE],
            main_identifier='TEST',
        )

        assert result.endswith("ValueError: Test calculation error")

    def test_calculation_output_error_without_traceback(self):
        # The calculation returned something that can't be a column, so it raised no error of its own
        error = ColumnBuilderCalculationError(
            "Calculation c_wrong_length_calculation returned 2 rows",
            calculation_module=CUSTOM_CALCULATION_MODULE,
            column='c_wrong_length_calculation',
        )
        result = _format_calculation_error_message(
            error,
            custom_calculation_modules=[CUSTOM_CALCULATION_MODULE],
            main_identifier='TEST',
        )

        assert 'Traceback' not in result
