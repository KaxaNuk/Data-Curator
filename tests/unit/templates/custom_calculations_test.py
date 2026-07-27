"""
Tests for the share count normalization shipped in the custom calculations template.

The template is copied into every new user project, so it is the versioned home of this logic and
needs the same protection as library code.

Vendors restate reported share counts for later corporate events inconsistently across period
types: Intrinio restates its annual periods but leaves its quarterly ones as reported, so
consecutive periods can differ by a whole event factor even though no shares changed hands. The
normalization puts every period back on one basis, using the point-in-time share count as its
reference, and declines to guess where nothing fits.
"""

import importlib.util
import pathlib

import numpy
import pytest

from kaxanuk.data_curator import DataColumn


_TEMPLATE_PATH = (
    pathlib.Path(__file__).parents[3]
    / 'templates' / 'data_curator' / 'Config' / 'custom_calculations.py'
)


def _load_template():
    spec = importlib.util.spec_from_file_location('template_custom_calculations', _TEMPLATE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


custom_calculations = _load_template()


# a 7-for-1 event on row 2 and a 4-for-1 on row 4, so share counts multiply by 7 then by 4
SPLIT_NUMERATORS = numpy.array([numpy.nan, numpy.nan, 1.0, numpy.nan, 1.0, numpy.nan])
SPLIT_DENOMINATORS = numpy.array([numpy.nan, numpy.nan, 7.0, numpy.nan, 4.0, numpy.nan])
# the clean point-in-time anchor, already on the current basis throughout
ANCHOR = numpy.array([2.63e10, 2.63e10, 2.00e10, 2.00e10, 1.75e10, 1.75e10])


def _normalize(reported, anchor=None):
    return custom_calculations._normalize_share_counts(
        numpy.array(reported, dtype='float64'),
        ANCHOR if anchor is None else numpy.array(anchor, dtype='float64'),
        SPLIT_NUMERATORS,
        SPLIT_DENOMINATORS,
    )


class TestNormalizeShareCounts:
    def test_restates_each_period_by_the_events_it_is_missing(self) -> None:
        # row 0 as reported, row 1 already restated for the 7-for-1 only, rows 2-3 post-7-for-1,
        # rows 4-5 already on the current basis
        result = _normalize([9.40e8, 6.58e9, 5.00e9, 5.00e9, 1.75e10, 1.75e10])

        assert result == pytest.approx([2.632e10, 2.632e10, 2.0e10, 2.0e10, 1.75e10, 1.75e10])

    def test_corrects_a_figure_restated_one_time_too_many(self) -> None:
        """A vendor can apply an adjustment on top of an already adjusted figure."""
        result = _normalize([9.40e8, 6.58e9 * 7 * 4, 5.00e9, 5.00e9, 1.75e10, 1.75e10])

        assert result == pytest.approx([2.632e10, 2.632e10, 2.0e10, 2.0e10, 1.75e10, 1.75e10])

    def test_corrects_a_period_reported_in_millions(self) -> None:
        """Intrinio expresses some periods in thousands or millions rather than units."""
        result = _normalize([9.40e8, 6.58e9, 5.00e9, 2.00e4, 1.75e10, 1.75e10])

        assert result == pytest.approx([2.632e10, 2.632e10, 2.0e10, 2.0e10, 1.75e10, 1.75e10])

    def test_leaves_a_figure_nothing_explains_out_rather_than_guessing(self) -> None:
        result = _normalize([9.40e8, 1.23e5, 5.00e9, 5.00e9, 1.75e10, 1.75e10])

        assert numpy.isnan(result[1])
        assert not numpy.isnan(result[0])
        assert not numpy.isnan(result[2])

    def test_falls_back_to_continuity_without_any_anchor(self) -> None:
        """Where the provider covers no market cap there is no anchor, only the newest period."""
        result = _normalize(
            [9.40e8, 6.58e9, 5.00e9, 5.00e9, 1.75e10, 1.75e10],
            anchor=[numpy.nan] * 6,
        )

        assert result == pytest.approx([2.632e10, 2.632e10, 2.0e10, 2.0e10, 1.75e10, 1.75e10])

    def test_resolves_a_reported_period_to_one_value_across_all_its_rows(self) -> None:
        """
        A period's figure is one number, so an event inside its rows must not step it.

        Resolving row by row would restate the days before an intra-period event and not the days
        after it, splitting one reported figure into two.
        """
        # a single reported period spanning the 7-for-1 event on row 2
        result = _normalize([5.00e9, 5.00e9, 5.00e9, 5.00e9, 1.75e10, 1.75e10])

        assert len(set(result[:4])) == 1

    def test_carries_nulls_through_untouched(self) -> None:
        result = _normalize([numpy.nan, 6.58e9, numpy.nan, 5.00e9, 1.75e10, 1.75e10])

        assert numpy.isnan(result[0])
        assert numpy.isnan(result[2])
        assert not numpy.isnan(result[1])


class TestNormalizedSharesColumn:
    def test_emits_nulls_rather_than_nans(self) -> None:
        """
        A NaN reaching the output is written literally as `nan` instead of an empty cell.

        The normalization works in NumPy, where a missing value is a NaN, so the column has to
        hand those to PyArrow as a null mask.
        """
        column = custom_calculations._normalized_shares_column(
            DataColumn.load([None, 6.58e9, 5.00e9, 5.00e9, 1.75e10, 1.75e10]),
            DataColumn.load(list(ANCHOR)),
            DataColumn.load(list(SPLIT_NUMERATORS)),
            DataColumn.load(list(SPLIT_DENOMINATORS)),
        )
        values = column.to_pylist()

        assert values[0] is None
        assert not any(
            value is not None and numpy.isnan(value)
            for value in values
        )


class TestCalculationFunctions:
    @pytest.mark.parametrize(
        'name',
        [
            'c_normalized_weighted_average_basic_shares_outstanding',
            'c_normalized_weighted_average_diluted_shares_outstanding',
            'c_normalized_adjusted_weighted_average_basic_shares_outstanding',
            'c_normalized_adjusted_weighted_average_diluted_shares_outstanding',
            'c_normalized_adjusted_weighted_average_basic_and_diluted_shares_outstanding',
        ],
    )
    def test_the_template_exposes_each_normalized_column(self, name: str) -> None:
        function = getattr(custom_calculations, name, None)

        assert callable(function)
        # the curator resolves a calculation's inputs from its argument names
        argument_names = function.__code__.co_varnames[:function.__code__.co_argcount]
        assert 'm_shares_outstanding' in argument_names
        assert 's_split_date_numerator' in argument_names
        assert 's_split_date_denominator' in argument_names
