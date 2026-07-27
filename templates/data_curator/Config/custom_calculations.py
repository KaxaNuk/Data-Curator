"""
Create your own custom Data Curator feature calculation functions.

To add a custom calculation, you should have this file in your own project's Config folder.

Each function needs to start with c_ as a prefix, and the rest of the name can be anything as
long as it's a valid Python function name.

Each function declares as arguments the names of each column it needs as input, which
are provided to it in our custom DataColumn objects. DataColumn acts as apyarrow.Array wrapper
but with neat features like:
- operator overloading (so you can directly perform arithmetic operations between columns,
like in pandas)
- automatically treating the result of any operation involving NaN or null elements as null, since
we consider any null a missing value

Each function needs to return an iterable supported by pyarrow.array(), of the same length
(preferably another DataColumn, a pyarrow.Array, a pandas.Series, or a 1D numpy.ndarray).
The result will automatically be wrapped in a DataColumn for any successive functions that use
that as input. Yes, you can absolutely chain functions together and are encouraged to do so!

Once you've added your function to the file, you need to add its name to the Output_Columns
sheet of the data_curator_parameters.xlsx file. Don't forget that your function name needs to
start with c_ as a prefix!

See more examples of how to program custom functions by checking our built-in calculations at
https://github.com/KaxaNuk/Data-Curator/blob/main/src/kaxanuk/data_curator/features/calculations.py
"""

import numpy
import pyarrow

# Here you'll find helper functions for calculating more complicated features:
from kaxanuk.data_curator.features import helpers


# a candidate share count is accepted as matching the point-in-time anchor when it lands within
# this factor of it; weighted averages legitimately differ from the count outstanding on a date by
# a few percent, while the wrong candidate is off by a whole split factor, so the gap is wide
_ANCHOR_TOLERANCE = 1.35

# the scales a reported figure may be expressed at: Intrinio reports some periods in thousands or
# millions rather than in units
_UNIT_SCALES = (1.0, 1e3, 1e-3, 1e6, 1e-6)


def _split_ratios(split_numerators, split_denominators):
    """
    Reduce the split columns to the ordered share multipliers they represent.

    A split row carries the price ratio (a 7-for-1 split is a numerator of 1 over a denominator
    of 7), so share counts move by its reciprocal.

    Parameters
    ----------
    split_numerators : numpy.ndarray
        The split numerator of each row, NaN where the row is not a split date.
    split_denominators : numpy.ndarray
        The split denominator of each row, NaN where the row is not a split date.

    Returns
    -------
    numpy.ndarray
        The share multiplier of each split, in date order.
    """
    is_split = (
        ~numpy.isnan(split_numerators)
        & ~numpy.isnan(split_denominators)
        & (split_numerators > 0)
        & (split_denominators > 0)
        & (split_numerators != split_denominators)
    )

    return split_denominators[is_split] / split_numerators[is_split]


def _candidate_multipliers(split_ratios):
    """
    List every factor a reported share count could be off by.

    A vendor restates a reported share count for some of the splits around its period and not
    others, so the figure can sit on any split basis, not just its own: too low when later splits
    were left out, and too high when a restatement was applied on top of an already restated
    figure. The correction is therefore the product of some contiguous run of splits, or the
    reciprocal of one.

    Intrinio also expresses some periods in thousands or millions rather than units, so those
    scales are candidates in their own right.

    The two kinds are deliberately not combined. Every extra candidate is another chance to
    "explain" a figure that is simply wrong, and the cross product is wide enough to land near
    almost any reference; a scale break and a restatement have not been seen together.

    Parameters
    ----------
    split_ratios : numpy.ndarray
        The share multiplier of every split in the series, in date order.

    Returns
    -------
    list of float
        The candidate factors, including 1.0 for a figure already on the current basis.
    """
    candidates = {*_UNIT_SCALES}
    for start in range(len(split_ratios)):
        product = 1.0
        for ratio in split_ratios[start:]:
            product *= ratio
            candidates.add(product)
            candidates.add(1 / product)

    return sorted(candidates)


def _value_runs(reported_shares):
    """
    Group the rows into the runs of equal reported values that each fiscal period occupies.

    A reported share count is carried unchanged across every daily row of its period, and its
    restatement is a property of the figure rather than of any one row, so the whole run has to
    resolve to a single value. Resolving per row would instead step in the middle of a period
    whenever a split fell inside it.

    Parameters
    ----------
    reported_shares : numpy.ndarray
        The vendor's reported share counts, in ascending date order, NaN where absent.

    Yields
    ------
    tuple of int
        The first and last row index of each run of equal values, newest run first.
    """
    is_present = ~numpy.isnan(reported_shares) & (reported_shares > 0)
    runs = []
    run_end = None
    for row_index in range(len(reported_shares) - 1, -1, -1):
        if not is_present[row_index]:
            if run_end is not None:
                runs.append((row_index + 1, run_end))
                run_end = None
            continue

        if run_end is None:
            run_end = row_index
        elif reported_shares[row_index] != reported_shares[row_index + 1]:
            runs.append((row_index + 1, run_end))
            run_end = row_index

    if run_end is not None:
        runs.append((0, run_end))

    return runs


def _normalize_share_counts(
    reported_shares,
    anchor_shares,
    split_numerators,
    split_denominators,
):
    """
    Restate a reported share count series onto a single, current split basis.

    Vendors restate share counts for later splits inconsistently across period types: Intrinio
    restates its annual periods but leaves its quarterly ones as reported, so consecutive periods
    can differ by a whole split factor even though no shares were issued or bought back. That
    makes the raw series unusable for anything that multiplies it by a price.

    Each reported period is resolved once (see `_value_runs`) by testing every factor its figure
    could be off by (see `_candidate_multipliers`) and keeping the one that agrees with a
    reference. The reference is the point-in-time share count where the provider covers it, and
    otherwise the last period already resolved, walking from the newest period backwards so every
    one is compared against something already on the current basis.

    A period whose figure no candidate brings close to its reference is left null rather than
    guessed at, so that a value that is wrong for some other reason is not silently dressed up as
    a corrected one.

    Parameters
    ----------
    reported_shares : numpy.ndarray
        The vendor's reported share counts, in ascending date order, NaN where absent.
    anchor_shares : numpy.ndarray
        The point-in-time share counts to trust as reference, NaN where uncovered.
    split_numerators : numpy.ndarray
        The split numerator of each row, NaN where the row is not a split date.
    split_denominators : numpy.ndarray
        The split denominator of each row, NaN where the row is not a split date.

    Returns
    -------
    numpy.ndarray
        The share counts on the latest split basis, NaN wherever the input was.
    """
    split_ratios = _split_ratios(split_numerators, split_denominators)
    multipliers = _candidate_multipliers(split_ratios)
    normalized = numpy.full(len(reported_shares), numpy.nan)
    reference = numpy.nan

    for (first_row, last_row) in _value_runs(reported_shares):
        reported = reported_shares[last_row]
        run_anchors = anchor_shares[first_row:last_row + 1]
        run_anchors = run_anchors[~numpy.isnan(run_anchors) & (run_anchors > 0)]
        # the median shrugs off any single odd anchor day within the period
        run_reference = numpy.median(run_anchors) if len(run_anchors) else reference

        if numpy.isnan(run_reference):
            # nothing resolved yet and no anchor: the newest period can only be on the current
            # basis already, since no split has happened since it
            resolved = reported
        else:
            resolved = min(
                (reported * multiplier for multiplier in multipliers),
                key=lambda candidate: abs(numpy.log(candidate / run_reference)),
            )
            if not (
                1 / _ANCHOR_TOLERANCE <= resolved / run_reference <= _ANCHOR_TOLERANCE
            ):
                # nothing lands near the reference, so the figure is wrong for some reason this
                # cannot correct; leave it out rather than publish a guess
                continue

        normalized[first_row:last_row + 1] = resolved
        reference = resolved

    return normalized


def _normalized_shares_column(
    reported_shares,
    m_shares_outstanding,
    s_split_date_numerator,
    s_split_date_denominator,
):
    """
    Adapt the curator's columns to `_normalize_share_counts` and back.

    Parameters
    ----------
    reported_shares : kaxanuk.data_curator.DataColumn
    m_shares_outstanding : kaxanuk.data_curator.DataColumn
    s_split_date_numerator : kaxanuk.data_curator.DataColumn
    s_split_date_denominator : kaxanuk.data_curator.DataColumn

    Returns
    -------
    pyarrow.Array
        The normalized counts, with every unresolved row an actual null.

    Notes
    -----
    The normalization works in NumPy, where a missing value is a NaN, but a NaN reaching the
    output would be written literally as `nan` rather than as an empty cell. The NaNs are
    therefore handed to PyArrow as a null mask, so an unresolved row is null like any other
    missing value in the output.
    """
    normalized = _normalize_share_counts(
        reported_shares.to_pandas().astype('float64').to_numpy(),
        m_shares_outstanding.to_pandas().astype('float64').to_numpy(),
        s_split_date_numerator.to_pandas().astype('float64').to_numpy(),
        s_split_date_denominator.to_pandas().astype('float64').to_numpy(),
    )

    return pyarrow.array(
        normalized,
        type=pyarrow.float64(),
        mask=numpy.isnan(normalized),
    )


def c_test(m_open_split_adjusted, m_close_split_adjusted):
    """
    Example feature calculation function.

    Receives the market open and market close columns, and returns a column with their difference.

    For this function to generate an output column, you need to:
    1. Make sure it's in your project's Config/custom_calculations.py file.
    2. Add c_test to the Output_Columns sheet in your Config/data_curator_parameters.xlsx file.

    Parameters
    ----------
    m_open_split_adjusted : kaxanuk.data_curator.DataColumn
    m_close_split_adjusted : kaxanuk.data_curator.DataColumn

    Returns
    -------
    kaxanuk.data_curator.DataColumn
    """
    # we're just doing a subtraction here, but you can implement any logic
    # just remember to return the same number of rows in a single column!
    return m_close_split_adjusted - m_open_split_adjusted


def c_normalized_weighted_average_basic_shares_outstanding(
    fis_weighted_average_basic_shares_outstanding,
    m_shares_outstanding,
    s_split_date_numerator,
    s_split_date_denominator,
):
    """
    Restate the reported weighted average basic shares outstanding onto the current split basis.

    Leaves the vendor's own column untouched; see `_normalize_share_counts` for why the raw
    series is not directly usable.

    Parameters
    ----------
    fis_weighted_average_basic_shares_outstanding : kaxanuk.data_curator.DataColumn
    m_shares_outstanding : kaxanuk.data_curator.DataColumn
    s_split_date_numerator : kaxanuk.data_curator.DataColumn
    s_split_date_denominator : kaxanuk.data_curator.DataColumn

    Returns
    -------
    pyarrow.Array
        The normalized counts, with every unresolved row null.
    """
    return _normalized_shares_column(
        fis_weighted_average_basic_shares_outstanding,
        m_shares_outstanding,
        s_split_date_numerator,
        s_split_date_denominator,
    )


def c_normalized_weighted_average_diluted_shares_outstanding(
    fis_weighted_average_diluted_shares_outstanding,
    m_shares_outstanding,
    s_split_date_numerator,
    s_split_date_denominator,
):
    """
    Restate the reported weighted average diluted shares outstanding onto the current split basis.

    Parameters
    ----------
    fis_weighted_average_diluted_shares_outstanding : kaxanuk.data_curator.DataColumn
    m_shares_outstanding : kaxanuk.data_curator.DataColumn
    s_split_date_numerator : kaxanuk.data_curator.DataColumn
    s_split_date_denominator : kaxanuk.data_curator.DataColumn

    Returns
    -------
    pyarrow.Array
        The normalized counts, with every unresolved row null.
    """
    return _normalized_shares_column(
        fis_weighted_average_diluted_shares_outstanding,
        m_shares_outstanding,
        s_split_date_numerator,
        s_split_date_denominator,
    )


def c_normalized_adjusted_weighted_average_basic_shares_outstanding(
    fis_adjusted_weighted_average_basic_shares_outstanding,
    m_shares_outstanding,
    s_split_date_numerator,
    s_split_date_denominator,
):
    """
    Restate the adjusted weighted average basic shares outstanding onto the current split basis.

    Parameters
    ----------
    fis_adjusted_weighted_average_basic_shares_outstanding : kaxanuk.data_curator.DataColumn
    m_shares_outstanding : kaxanuk.data_curator.DataColumn
    s_split_date_numerator : kaxanuk.data_curator.DataColumn
    s_split_date_denominator : kaxanuk.data_curator.DataColumn

    Returns
    -------
    pyarrow.Array
        The normalized counts, with every unresolved row null.
    """
    return _normalized_shares_column(
        fis_adjusted_weighted_average_basic_shares_outstanding,
        m_shares_outstanding,
        s_split_date_numerator,
        s_split_date_denominator,
    )


def c_normalized_adjusted_weighted_average_diluted_shares_outstanding(
    fis_adjusted_weighted_average_diluted_shares_outstanding,
    m_shares_outstanding,
    s_split_date_numerator,
    s_split_date_denominator,
):
    """
    Restate the adjusted weighted average diluted shares outstanding onto the current split basis.

    Parameters
    ----------
    fis_adjusted_weighted_average_diluted_shares_outstanding : kaxanuk.data_curator.DataColumn
    m_shares_outstanding : kaxanuk.data_curator.DataColumn
    s_split_date_numerator : kaxanuk.data_curator.DataColumn
    s_split_date_denominator : kaxanuk.data_curator.DataColumn

    Returns
    -------
    pyarrow.Array
        The normalized counts, with every unresolved row null.
    """
    return _normalized_shares_column(
        fis_adjusted_weighted_average_diluted_shares_outstanding,
        m_shares_outstanding,
        s_split_date_numerator,
        s_split_date_denominator,
    )


def c_normalized_adjusted_weighted_average_basic_and_diluted_shares_outstanding(
    fis_adjusted_weighted_average_basic_and_diluted_shares_outstanding,
    m_shares_outstanding,
    s_split_date_numerator,
    s_split_date_denominator,
):
    """
    Restate the adjusted weighted average basic & diluted shares onto the current split basis.

    Parameters
    ----------
    fis_adjusted_weighted_average_basic_and_diluted_shares_outstanding : kaxanuk.data_curator.DataColumn
    m_shares_outstanding : kaxanuk.data_curator.DataColumn
    s_split_date_numerator : kaxanuk.data_curator.DataColumn
    s_split_date_denominator : kaxanuk.data_curator.DataColumn

    Returns
    -------
    pyarrow.Array
        The normalized counts, with every unresolved row null.
    """
    return _normalized_shares_column(
        fis_adjusted_weighted_average_basic_and_diluted_shares_outstanding,
        m_shares_outstanding,
        s_split_date_numerator,
        s_split_date_denominator,
    )
