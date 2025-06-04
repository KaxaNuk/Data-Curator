import datetime

from kaxanuk.data_curator.data_providers import DataProviderInterface


class TestPrivateBuildUrlWithTickerPathAndQueryParams:
    def test_build_simple_url(self):
        endpoint_url = 'https://my.site'
        ticker = 'AAPL'
        query_params = {
            'period': 'annual',
            'start_date': '2020-01-01',
        }
        expected = 'https://my.site/AAPL?period=annual&start_date=2020-01-01'
        actual = DataProviderInterface._build_url_with_identifier_path_and_query_params(
            endpoint_url,
            ticker,
            query_params
        )

        assert actual == expected


class TestPrivateFindFirstDateBeforeStartDate:
    def test_find_in_date_strings_ascending(self):
        rows = [
            '2020-01-01',
            '2020-04-01',
            '2020-07-01',
            '2021-01-05',
            '2021-10-08',
            '2023-01-09',
            '2023-04-10',
        ]
        start_date = '2022-01-04'
        expected = '2021-10-08'
        actual = DataProviderInterface._find_first_date_before_start_date(rows, start_date)

        assert actual == expected

    def test_find_in_date_strings_descending(self):
        rows = [
            '2023-04-10',
            '2023-01-09',
            '2021-10-08',
            '2021-01-05',
            '2020-07-01',
            '2020-04-01',
            '2020-01-01',
        ]
        start_date = '2022-01-04'
        expected = '2021-10-08'
        actual = DataProviderInterface._find_first_date_before_start_date(
            rows,
            start_date,
            descending_order=True
        )

        assert actual == expected

    def test_find_in_datetime_dates_ascending(self):
        rows = [
            datetime.date.fromisoformat('2020-01-01'),
            datetime.date.fromisoformat('2020-04-01'),
            datetime.date.fromisoformat('2020-07-01'),
            datetime.date.fromisoformat('2021-01-05'),
            datetime.date.fromisoformat('2021-10-08'),
            datetime.date.fromisoformat('2023-01-09'),
            datetime.date.fromisoformat('2023-04-10'),
        ]
        start_date = datetime.date.fromisoformat('2022-01-04')
        expected = datetime.date.fromisoformat('2021-10-08')
        actual = DataProviderInterface._find_first_date_before_start_date(rows, start_date)

        assert actual == expected


    def test_find_in_datetime_dates_descending(self):
        rows = [
            datetime.date.fromisoformat('2023-04-10'),
            datetime.date.fromisoformat('2023-01-09'),
            datetime.date.fromisoformat('2021-10-08'),
            datetime.date.fromisoformat('2021-01-05'),
            datetime.date.fromisoformat('2020-07-01'),
            datetime.date.fromisoformat('2020-04-01'),
            datetime.date.fromisoformat('2020-01-01'),
        ]
        start_date = datetime.date.fromisoformat('2022-01-04')
        expected = datetime.date.fromisoformat('2021-10-08')
        actual = DataProviderInterface._find_first_date_before_start_date(
            rows,
            start_date,
            descending_order=True
        )

        assert actual == expected


class TestPrivateFindUnorderedDatesInDescendingDates:
    def test_find_unordered_date_strings_ascending(self):
        rows = [
            None,
            '2020-01-01',
            '2020-04-01',
            '2020-07-01',
            '2022-01-04',
            '2021-01-05',
            '2022-10-06',
            '2022-10-07',
            '2021-10-08',
            '2023-01-09',
            '2023-04-10',
        ]
        expected = [
            '2022-01-04',
            '2022-10-06',
            '2022-10-07',
        ]
        actual = DataProviderInterface._find_unordered_dates(rows)

        assert actual == expected

    def test_find_unordered_date_strings_descending(self):
        rows = [
            '2023-04-10',
            '2023-01-09',
            '2021-10-08',
            '2022-10-07',
            '2022-10-06',
            '2021-01-05',
            '2022-01-04',
            '2020-07-01',
            '2020-04-01',
            '2020-01-01',
            None,
        ]
        expected = [
            '2022-10-07',
            '2022-10-06',
            '2022-01-04',
        ]
        actual = DataProviderInterface._find_unordered_dates(
            rows,
            descending_order=True
        )

        assert actual == expected

    def test_find_unordered_datetime_dates_ascending(self):
        rows = [
            datetime.date.fromisoformat('2020-01-01'),
            datetime.date.fromisoformat('2020-04-01'),
            datetime.date.fromisoformat('2020-07-01'),
            datetime.date.fromisoformat('2022-01-04'),
            datetime.date.fromisoformat('2021-01-05'),
            datetime.date.fromisoformat('2022-10-06'),
            datetime.date.fromisoformat('2022-10-07'),
            datetime.date.fromisoformat('2021-10-08'),
            datetime.date.fromisoformat('2023-01-09'),
            datetime.date.fromisoformat('2023-04-10'),
        ]
        expected = [
            datetime.date.fromisoformat('2022-01-04'),
            datetime.date.fromisoformat('2022-10-06'),
            datetime.date.fromisoformat('2022-10-07'),
        ]
        actual = DataProviderInterface._find_unordered_dates(rows)

        assert actual == expected

    def test_find_unordered_datetime_dates_descending(self):
        rows = [
            datetime.date.fromisoformat('2023-04-10'),
            datetime.date.fromisoformat('2023-01-09'),
            datetime.date.fromisoformat('2021-10-08'),
            datetime.date.fromisoformat('2022-10-07'),
            datetime.date.fromisoformat('2022-10-06'),
            datetime.date.fromisoformat('2021-01-05'),
            datetime.date.fromisoformat('2022-01-04'),
            datetime.date.fromisoformat('2020-07-01'),
            datetime.date.fromisoformat('2020-04-01'),
            datetime.date.fromisoformat('2020-01-01'),
        ]
        expected = [
            datetime.date.fromisoformat('2022-10-07'),
            datetime.date.fromisoformat('2022-10-06'),
            datetime.date.fromisoformat('2022-01-04'),
        ]
        actual = DataProviderInterface._find_unordered_dates(
            rows,
            descending_order=True
        )

        assert actual == expected
