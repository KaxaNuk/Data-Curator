import pytest

from . import example_columns


class TestLoadExampleColumns:
    @pytest.mark.parametrize(
        'file_contents',
        [
            # An empty file
            '',
            # Only the headers
            'm_close,c_market_cap\n',
        ]
    )
    def test_file_without_rows(
        self,
        tmp_path,
        file_contents,
    ):
        file_path = tmp_path / 'example.csv'
        file_path.write_text(file_contents)

        with pytest.raises(RuntimeError):
            example_columns.load_example_columns(
                'example.csv',
                directory=tmp_path,
            )
