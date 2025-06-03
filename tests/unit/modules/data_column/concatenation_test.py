import pyarrow

from kaxanuk.data_curator import DataColumn


def test_concatenate():
    # Test case: concatenate two strings
    assert (
        DataColumn.concatenate("Hello ", "World").as_py()
        == "Hello World"
    )

    # Test case: concatenate a DataColumn with strings and no Nones to a string
    column_with_strings = DataColumn(pyarrow.array(["Hello", "Hey", "Hi"]))
    assert (
        DataColumn.concatenate(column_with_strings, " World").to_pyarrow().to_pylist()
        == ["Hello World", "Hey World", "Hi World"]
    )

    # Test case: concatenate a DataColumn with strings and a None to a string
    column_with_strings_and_none = DataColumn(pyarrow.array(["Hello", None, "Hi"]))
    assert (
        DataColumn.concatenate(column_with_strings_and_none, " World").to_pyarrow().to_pylist()
        == ["Hello World", " World", "Hi World"]
    )

    # Test case: concatenate a string to a DataColumn with strings and a None
    assert (
        DataColumn.concatenate("Hello ", column_with_strings_and_none).to_pyarrow().to_pylist()
        == ["Hello Hello", "Hello ", "Hello Hi"]
    )

    # Test case: concatenate a DataColumn with strings and no Nones to another DataColumn with strings and no Nones
    another_column_with_strings = DataColumn(pyarrow.array([" World", "! How are you", "?"]))
    assert (
        DataColumn.concatenate(column_with_strings, another_column_with_strings).to_pyarrow().to_pylist()
        == ["Hello World", "Hey! How are you", "Hi?"]
    )

    # Test case: concatenate a DataColumn with strings and no Nones to another DataColumn with strings and a None
    column_with_strings_and_none = DataColumn(pyarrow.array([" World", None, "?"]))
    assert (
        DataColumn.concatenate(column_with_strings, column_with_strings_and_none).to_pyarrow().to_pylist()
        == ["Hello World", "Hey", "Hi?"]
    )

    # Test case: concatenate a DataColumn with strings a None and to another DataColumn with strings and no Nones
    column_with_strings_and_none = DataColumn(pyarrow.array(["Hello", None, "Hi"]))
    assert (
        DataColumn.concatenate(column_with_strings_and_none, another_column_with_strings).to_pyarrow().to_pylist()
        == ["Hello World", "! How are you", "Hi?"]
    )

    # Test case: concatenate a DataColumn with strings and a None to another DataColumn with strings and a None
    column_with_strings_and_none_1 = DataColumn(pyarrow.array(["Hello", None, "Hi"]))
    column_with_strings_and_none_2 = DataColumn(pyarrow.array([" World", None, "?"]))
    assert (
        DataColumn.concatenate(column_with_strings_and_none_1, column_with_strings_and_none_2).to_pyarrow().to_pylist()
        == ["Hello World", "", "Hi?"]
    )

    # Test case: concatenate a DataColumn with ints and no Nones to a string
    column_with_ints = DataColumn(pyarrow.array([1, 2, 3]))
    assert (
        DataColumn.concatenate(column_with_ints, " Cats").to_pyarrow().to_pylist()
        == ["1 Cats", "2 Cats", "3 Cats"]
    )

    # Test case: concatenate a DataColumn with ints and a None to a string
    column_with_ints_and_none = DataColumn(pyarrow.array([1, None, 3]))
    assert (
        DataColumn.concatenate(column_with_ints_and_none, " Cats").to_pyarrow().to_pylist()
        == ["1 Cats", " Cats", "3 Cats"]
    )

    # Test case: concatenate a DataColumn with floats and no Nones to a string
    column_with_floats = DataColumn(pyarrow.array([1.1, 2.4, 3.6]))
    assert (
        DataColumn.concatenate(column_with_floats, " Dogs").to_pyarrow().to_pylist()
        == ["1.1 Dogs", "2.4 Dogs", "3.6 Dogs"]
    )

    # Test case: concatenate a DataColumn with floats and a None to a string
    column_with_floats_and_none = DataColumn(pyarrow.array([1.4, None, 3.6]))
    assert (
        DataColumn.concatenate(column_with_floats_and_none, " Dogs").to_pyarrow().to_pylist()
        == ["1.4 Dogs", " Dogs", "3.6 Dogs"]
    )

    # Test case: concatenate a DataColumn with floats and a NaN to a string
    column_with_floats_and_none = DataColumn(pyarrow.array([1.1, float('nan'), 3.6]))
    assert (
        DataColumn.concatenate(column_with_floats_and_none, " Dogs").to_pyarrow().to_pylist()
        == ["1.1 Dogs", "nan Dogs", "3.6 Dogs"]
    )
