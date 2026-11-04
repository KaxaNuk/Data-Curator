import pyarrow
import pytest

from kaxanuk.data_curator import clock_sync_methods


@pytest.fixture
def master_row_indices():
    # Rows 0 to 3 belong to master row 0, row 4 to master row 2, and row 5 to none of them
    return pyarrow.array(
        [0, 0, 0, 0, 2, None],
        type=pyarrow.int64(),
    )


@pytest.fixture
def reference_array():
    return pyarrow.array([None, 5, 2, None, 3, 7])


class TestFirst:
    def test_first_valid_row_of_each_master_row(
        self,
        master_row_indices,
        reference_array,
    ):
        result = clock_sync_methods.ConsolidationMethods.first(
            reference_array,
            master_row_indices,
            3,
        )
        expected = pyarrow.array([5, None, 3])

        assert result.equals(expected)

    def test_interleaved_master_rows(self):
        result = clock_sync_methods.ConsolidationMethods.first(
            pyarrow.array([1, 2, 3, 4]),
            pyarrow.array(
                [0, 1, 0, 1],
                type=pyarrow.int64(),
            ),
            2,
        )
        expected = pyarrow.array([1, 2])

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods.first(
            pyarrow.nulls(6),
            master_row_indices,
            3,
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)


class TestLast:
    def test_last_valid_row_of_each_master_row(
        self,
        master_row_indices,
        reference_array,
    ):
        result = clock_sync_methods.ConsolidationMethods.last(
            reference_array,
            master_row_indices,
            3,
        )
        expected = pyarrow.array([2, None, 3])

        assert result.equals(expected)

    def test_interleaved_master_rows(self):
        result = clock_sync_methods.ConsolidationMethods.last(
            pyarrow.array([1, 2, 3, 4]),
            pyarrow.array(
                [0, 1, 0, 1],
                type=pyarrow.int64(),
            ),
            2,
        )
        expected = pyarrow.array([3, 4])

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods.last(
            pyarrow.nulls(6),
            master_row_indices,
            3,
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)


class TestMax:
    def test_maximum_of_each_master_row(
        self,
        master_row_indices,
        reference_array,
    ):
        result = clock_sync_methods.ConsolidationMethods.max(
            reference_array,
            master_row_indices,
            3,
        )
        expected = pyarrow.array([5, None, 3])

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods.max(
            pyarrow.nulls(6),
            master_row_indices,
            3,
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)


class TestMean:
    def test_mean_of_each_master_row(
        self,
        master_row_indices,
        reference_array,
    ):
        result = clock_sync_methods.ConsolidationMethods.mean(
            reference_array,
            master_row_indices,
            3,
        )
        expected = pyarrow.array([3.5, None, 3.0])

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods.mean(
            pyarrow.nulls(6),
            master_row_indices,
            3,
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)


class TestMin:
    def test_minimum_of_each_master_row(
        self,
        master_row_indices,
        reference_array,
    ):
        result = clock_sync_methods.ConsolidationMethods.min(
            reference_array,
            master_row_indices,
            3,
        )
        expected = pyarrow.array([2, None, 3])

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods.min(
            pyarrow.nulls(6),
            master_row_indices,
            3,
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)


class TestSum:
    def test_sum_of_each_master_row(
        self,
        master_row_indices,
        reference_array,
    ):
        result = clock_sync_methods.ConsolidationMethods.sum(
            reference_array,
            master_row_indices,
            3,
        )
        expected = pyarrow.array([7, None, 3])

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods.sum(
            pyarrow.nulls(6),
            master_row_indices,
            3,
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)


class TestPrivateAggregate:
    @pytest.mark.parametrize(
        (
            'aggregation',
            'expected_list',
        ),
        [
            ('first', [5, None, 3]),
            ('last', [2, None, 3]),
            ('max', [5, None, 3]),
            ('mean', [3.5, None, 3.0]),
            ('min', [2, None, 3]),
            ('sum', [7, None, 3]),
        ]
    )
    def test_aggregation(
        self,
        master_row_indices,
        reference_array,
        aggregation,
        expected_list,
    ):
        result = clock_sync_methods.ConsolidationMethods._aggregate(
            aggregation=aggregation,
            master_row_count=3,
            master_row_indices=master_row_indices,
            reference_array=reference_array,
        )
        expected = pyarrow.array(expected_list)

        assert result.equals(expected)

    @pytest.mark.parametrize(
        (
            'aggregation',
            'expected_list',
        ),
        [
            ('first', [1, 2]),
            ('last', [3, 4]),
        ]
    )
    def test_ordered_aggregation_with_interleaved_master_rows(
        self,
        aggregation,
        expected_list,
    ):
        result = clock_sync_methods.ConsolidationMethods._aggregate(
            aggregation=aggregation,
            master_row_count=2,
            master_row_indices=pyarrow.array(
                [0, 1, 0, 1],
                type=pyarrow.int64(),
            ),
            reference_array=pyarrow.array([1, 2, 3, 4]),
        )
        expected = pyarrow.array(expected_list)

        assert result.equals(expected)

    def test_null_array(
        self,
        master_row_indices,
    ):
        result = clock_sync_methods.ConsolidationMethods._aggregate(
            aggregation='sum',
            master_row_count=3,
            master_row_indices=master_row_indices,
            reference_array=pyarrow.nulls(6),
        )
        expected = pyarrow.nulls(3)

        assert result.equals(expected)

    def test_reference_rows_outside_all_master_rows(self):
        result = clock_sync_methods.ConsolidationMethods._aggregate(
            aggregation='sum',
            master_row_count=2,
            master_row_indices=pyarrow.array(
                [None, None],
                type=pyarrow.int64(),
            ),
            reference_array=pyarrow.array([1, 2]),
        )
        expected = pyarrow.array(
            [None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)

    def test_empty_reference_array(self):
        result = clock_sync_methods.ConsolidationMethods._aggregate(
            aggregation='sum',
            master_row_count=2,
            master_row_indices=pyarrow.array(
                [],
                type=pyarrow.int64(),
            ),
            reference_array=pyarrow.array(
                [],
                type=pyarrow.int64(),
            ),
        )
        expected = pyarrow.array(
            [None, None],
            type=pyarrow.int64(),
        )

        assert result.equals(expected)
