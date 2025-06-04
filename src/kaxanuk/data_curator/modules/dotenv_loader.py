"""
Module for loading env variables from .env files
"""

import pathlib

import dotenv


def load_config_env() -> bool:
    """
    Load the environment variables in the Config/.env file.

    This will not overwrite existing environment variables.

    Returns
    -------
    Whether the operation was successful
    """
    dotenv_path = pathlib.Path.cwd() / 'Config' / '.env'

    if not dotenv_path.exists():
        return False

    return dotenv.load_dotenv(
        dotenv_path
    )
