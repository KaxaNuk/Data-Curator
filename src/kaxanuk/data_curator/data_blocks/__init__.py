"""
Package containing the built-in data blocks
"""

__all__ = [
    'BaseDataBlock',
    'dividends',
    'fundamentals',
    'market_daily',
    'splits',
]


# make the base class and each self-enclosed data block package part of the public API of the package
from kaxanuk.data_curator.data_blocks.base_data_block import BaseDataBlock
from kaxanuk.data_curator.data_blocks import dividends
from kaxanuk.data_curator.data_blocks import fundamentals
from kaxanuk.data_curator.data_blocks import market_daily
from kaxanuk.data_curator.data_blocks import splits
