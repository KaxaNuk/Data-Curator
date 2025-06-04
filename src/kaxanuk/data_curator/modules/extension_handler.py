import importlib
import inspect

from kaxanuk.data_curator.data_providers import DataProviderInterface
from kaxanuk.data_curator.data_providers.not_found import NotFoundDataProvider
from kaxanuk.data_curator.exceptions import (
    ExtensionFailedError,
    ExtensionNotFoundError
)


def load_data_provider_extension(
    *,
    extension_name: str,
    extension_class_name: str,
) -> type[DataProviderInterface]:
    """
    Load the external data provider class if exists, NotFoundDataProvider otherwise.

    Parameters
    ----------
    extension_name
        the name of the data provider extension module to load
    extension_class_name
        the class of the data provider extension module to return

    Returns
    -------
    DataProviderInterface
        The data provider class if available, NotFoundDataProvider otherwise

    Raises
    ------
    ExtensionFailedError
    """
    try:
        extension = load_extension(
            extension_name=extension_name,
            extension_class_name=extension_class_name,
        )
        if not issubclass(extension, DataProviderInterface):

            raise ExtensionFailedError(
                f"Extension '{extension_name}' does not implement DataProviderInterface"
            )

        return extension

    except ExtensionNotFoundError:

        return NotFoundDataProvider


def load_extension(
    *,
    extension_name: str,
    extension_class_name: str,
) -> type:
    """
    Attempt to load the extension extension_class from the extension module extension_name.

    All extensions are assumed to be in the kaxanuk.data_curator_extensions namespace.

    Parameters
    ----------
    extension_name
        the name of the extension module to load
    extension_class_name
        the class of the extension module to return

    Returns
    -------
    type
        The loaded Python class or None if not available

    Raises
    ------
    ExtensionFailedError
    ExtensionNotFoundError
    """
    try:
        module = importlib.import_module(
            f'kaxanuk.data_curator_extensions.{extension_name}'
        )
        attribute = getattr(module, extension_class_name)

        if not inspect.isclass(attribute):

            raise AttributeError

        return attribute

    except ModuleNotFoundError as e:
        msg = f"Extension '{extension_name}' not found"

        raise ExtensionNotFoundError(msg) from e

    except AttributeError as e:
        msg = f"Class '{extension_class_name}' not found in extension '{extension_name}'"

        raise ExtensionNotFoundError(msg) from e

    except Exception as e:
        msg = f"Error loading extension '{extension_name}'"

        raise ExtensionFailedError(msg) from e
