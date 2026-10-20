"""
Stand-in for `SharadarClient`, serving the recorded Sharadar responses in this folder instead of the live API.

Each fixture is the JSON envelope Sharadar's query API answers with for one instrument, so `fetch_pages` returns
the same records the real client would, filtered the way the API filters them. Only the methods that reach the
network are replaced; everything else, like the ticker batching, is the real client's.
"""

import pathlib
import pickle

from kaxanuk.data_curator.data_providers import sharadar


_FIXTURE_FILES = {
    'actions': 'corporate_actions.pkl',
    'fundamentals': 'fundamentals.pkl',
    'funds': 'fund_prices.pkl',
    'stocks': 'stock_prices.pkl',
    'tickers': 'tickers_metadata.pkl',
}
_TABLE_RECORDS = {}
for _table, _file_name in _FIXTURE_FILES.items():
    with (pathlib.Path(__file__).parent / _file_name).open('rb') as _fixture_file:
        _TABLE_RECORDS[_table] = pickle.load(_fixture_file)['data']    # noqa: S301  repository fixtures


class SharadarClient(sharadar.SharadarClient):
    def fetch_pages(
        self,
        *,
        table,
        tickers=None,
        start_date=None,
        end_date=None,
        extra_params=None,
    ):
        # every recorded response fits in a single page
        dimension = (extra_params or {}).get('dimension')

        yield [
            record
            for record in _TABLE_RECORDS[table]
            if (tickers is None or record['ticker'] in tickers)
            and (dimension is None or record.get('dimension') == dimension)
            and (start_date is None or 'date' not in record or record['date'] >= start_date)
            and (end_date is None or 'date' not in record or record['date'] <= end_date)
        ]

    def validate_api_key(
        self,
    ):
        return True
