import pyarrow
import pyarrow.compute


class ArrayPreprocessors:
    @staticmethod
    def convert_millions_to_units(array: pyarrow.StringArray) -> pyarrow.StringArray:
        # @todo: logic for automatically determining needed precision
        numeric_array = array.cast(
            pyarrow.decimal128(36, 12)
        )
        converted_array = pyarrow.compute.multiply_checked(
            numeric_array,
            pyarrow.scalar(1_000_000)
        )
        result_array = converted_array.cast(
            pyarrow.string()
        )

        return result_array
