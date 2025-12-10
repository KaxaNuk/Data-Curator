# conftest.py
import pytest
import csv
import pathlib


@pytest.fixture(scope="module")
def example_adjusted_price_ratio_calculation():
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/adjusted_price_ratio_calculation.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }
        if not columns:
            msg = 'No data in adjusted_price_ratio_calculation.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_chakin_money_flow_calculation():
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/chakin_money_flow.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in chakin_money_flow.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_log_returns_and_annualized_volatility():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/log_returns_annualized_volatility.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in log_returns_annualized_volatility.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_relative_strength_index():
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/relative_strength_index.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }
        if not columns:
            msg = 'No data in relative_strength_index'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_sales_to_price():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/sales_to_price.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in sales_to_price.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_adjusted_price_ratio():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/adjusted_price_ratio.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in adjusted_price_ratio.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_moving_average():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/moving_average.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in moving_average.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_average_daily_traded_value():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/average_daily_traded_value.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in average_daily_traded_value.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_logarithmic_difference_high_low():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/logarithmic_difference_high_low.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in logarithmic_difference_high_low.csv'

            raise RuntimeError(msg)

        return columns


@pytest.fixture(scope="module")
def example_market_cap():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/market_cap.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in market_cap.csv'

            raise RuntimeError(msg)

        return columns

@pytest.fixture(scope="module")
def example_book_to_price():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/book_to_price.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in book_to_price.csv'

            raise RuntimeError(msg)

        return columns

@pytest.fixture(scope="module")
def example_last_twelve_months_revenue_per_share():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/last_twelve_months_revenue_per_share.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in last_twelve_months_revenue_per_share.csv'

            raise RuntimeError(msg)

        return columns

@pytest.fixture(scope="module")
def example_earnings_to_price():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/earnings_to_price.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in earnings_to_price.csv'

            raise RuntimeError(msg)

        return columns

@pytest.fixture(scope="module")
def example_moving_average_convergence_divergence():
    # Get the absolute path of the directory of the current file
    base_dir = pathlib.Path(__file__).parent
    relative_path = f'{base_dir}/fixtures/moving_average_convergence_divergence.csv'
    with pathlib.Path(relative_path).open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True
        )
        data= list(reader)
        columns = {
            header: [
                float(row[header]) if len(row[header]) else None
                for row in data
            ]
            for header in reader.fieldnames
        }

        if not columns:
            msg = 'No data in moving_average_convergence_divergence.csv'

            raise RuntimeError(msg)

        return columns
