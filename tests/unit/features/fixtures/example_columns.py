"""
Loader for the example CSV files in this folder, holding the inputs and expected outputs of the feature calculations.
"""

import csv
import pathlib


FIXTURES_DIRECTORY = pathlib.Path(__file__).parent


def load_example_columns(
    file_name: str,
    directory: pathlib.Path = FIXTURES_DIRECTORY,
) -> dict[str, list[float | None]]:
    file_path = directory / file_name
    with file_path.open() as example_file:
        reader = csv.DictReader(
            example_file,
            skipinitialspace=True,
        )
        rows = list(reader)

    if not rows:
        msg = f"No data in {file_name}"

        raise RuntimeError(msg)

    return {
        header: [
            float(row[header]) if len(row[header]) else None
            for row in rows
        ]
        for header in reader.fieldnames
    }
