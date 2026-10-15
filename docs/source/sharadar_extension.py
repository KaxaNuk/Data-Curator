"""
Sphinx extension that generates the Sharadar data provider page from the provider's own field maps.

The tables of the page are read off the endpoint field maps of the Sharadar class at build time, so the page
always lists the tags the code actually maps, and the text around them is kept here.
"""

import contextlib
import importlib.util
import inspect
import types
from pathlib import Path
from typing import Any

from sphinx.application import Sphinx
from sphinx.util import logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = (Path(__file__).parent / '..' / '..').resolve()
SHARADAR_PATH = PROJECT_ROOT / 'src' / 'kaxanuk' / 'data_curator' / 'data_providers' / 'sharadar.py'
SHARADAR_MODULE = 'kaxanuk.data_curator.data_providers.sharadar'

SECTION_ORDER = [
    "Market Data",
    "Dividends",
    "Splits",
    "Fundamentals",
    "Income",
    "Balance Sheet",
    "Cash Flow",
]

# Sharadar serves every statement of a filing from one table, so the section of a field comes from the entity
# it belongs to rather than from its endpoint
ENTITY_SECTIONS = {
    "DividendDataRow": ("Dividends", "d_"),
    "FundamentalDataRow": ("Fundamentals", "f_"),
    "FundamentalDataRowBalanceSheet": ("Balance Sheet", "fbs_"),
    "FundamentalDataRowCashFlow": ("Cash Flow", "fcf_"),
    "FundamentalDataRowIncomeStatement": ("Income", "fis_"),
    "MarketDataDailyRow": ("Market Data", "m_"),
    "SplitDataRow": ("Splits", "s_"),
}

# The columns the provider joins into Sharadar's rows, which are not fields of the table they are listed under
DERIVED_COLUMNS = {
    'currency': 'currency, from the tickers table',
}

INTRO_TEXT = """
.. _sharadar:

Sharadar
=============================

**Sharadar** publishes end of day prices, fundamentals and corporate actions for US listed stocks and funds,
including delisted ones, which makes it suited to point in time research free of survivorship bias.

- Visit their `official documentation <https://sharadar.com/docs/intro>`_.

- View available plans: `Sharadar Pricing Plans <https://sharadar.com/subscribe>`_.

Sharadar Features
------------------

Market Data
~~~~~~~~~~~~~~~~~

Daily prices of **stocks** and of **funds** (ETFs, closed-end funds and ETNs). Which of the two tables holds
an identifier is read from Sharadar's tickers table.

- **Split adjusted**: open, high, low, close and volume
- **Unadjusted**: close only
- **Dividend and split adjusted**: close only

Fundamentals
~~~~~~~~~~~~~~~~~

Income statement, balance sheet and cash flow of each filing, in the **as-reported** dimensions only (ARQ for
quarterly runs, ARY for annual ones). These are point in time: each filing is dated on the day it was filed and
its figures are not revised by later filings. Any other dimension Sharadar returns is dropped. Filings are read
from their whole history, so the first day of a run already carries the filing in effect on it.

**Share counts and per-share values are adjusted for later splits**, even in the as-reported dimensions: AAPL's
filing of January 2020 carries 17.7 billion shares and an EPS of 1.26, where Apple reported about 4.4 billion and
4.99 before its 2020 split. Combine them with split adjusted prices, never with unadjusted ones.

The reporting currency is taken from the fundamentals row of the tickers table, since the fundamentals table
carries none; its stocks row holds the trading currency instead. Companies filing in a foreign currency report
their fundamentals in it, like Toyota in yen, while their prices are in US dollars.

Dividends and Splits
~~~~~~~~~~~~~~~~~~~~~

Both come from Sharadar's actions table.

- Sharadar publishes each dividend **only adjusted for the splits after it**. That value fills the split
  adjusted dividend, and the dividend as paid is left empty: AAPL's 0.77 of 2019 reads as 0.1925, after its
  2020 4 for 1 split. Deriving the paid amount, if needed, belongs in a custom calculation.

Technical Details
~~~~~~~~~~~~~~~~~

- Every table is read through Sharadar's query API, 30 identifiers per request.
- Rate limited requests wait for the reset Sharadar announces, and network or server errors are retried for
  about four minutes.
- An identifier that needs a table the subscription doesn't cover, like a fund on a stocks-only plan, is
  skipped and logged, and the rest of the run carries on. A rejected API key stops the run.
- Sharadar reuses ticker symbols: when a company is delisted or renamed, its old symbol may get a suffix and
  later belong to another company. Keep the identifiers of a run current with Sharadar's tickers table.

Authentication is managed using an API key placed in the `.env` file via the variable:

.. code-block:: ini

   KNDC_API_KEY_SHARADAR=your_key_here
""".strip()


# --- LOADER FUNCTIONS ---

def load_sharadar_module() -> types.ModuleType | None:
    try:
        spec = importlib.util.spec_from_file_location(SHARADAR_MODULE, SHARADAR_PATH)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            logger.info("Successfully loaded Sharadar module: %s", module.__file__)
            return module
        return None
    except FileNotFoundError:
        logger.error("Could not find file %s", SHARADAR_PATH)
        return None
    except (ImportError, SyntaxError, AttributeError) as e:
        logger.error("Error loading sharadar.py: %s", e)
        return None


def build_entity_field_map(sharadar_module: types.ModuleType) -> dict[Any, tuple[str, str]]:
    """Map every field of the target entities to its entity name and field name."""
    field_map = {}
    for entity_name in ENTITY_SECTIONS:
        cls = getattr(sharadar_module, entity_name, None)
        if cls:
            for name, value in inspect.getmembers(cls):
                with contextlib.suppress(TypeError):
                    field_map[value] = (entity_name, name)
    return field_map


# --- CORE LOGIC ---

def get_field_info(value_obj: Any) -> tuple[str | tuple[str, ...], bool]:
    """Return the Sharadar field a tag reads, and whether it goes through a preprocessor."""
    if isinstance(value_obj, str):
        return value_obj, False

    if hasattr(value_obj, 'tags'):
        tags = value_obj.tags
        if isinstance(tags, list):
            return tuple(str(t) for t in tags), True

    return str(value_obj), False


def extract_all_fields() -> tuple[dict[str, dict[str, dict[Any, Any]]], dict[str, str]]:
    sharadar_module = load_sharadar_module()
    if sharadar_module is None:
        return {}, {}

    sharadar_class = getattr(sharadar_module, "Sharadar", None)
    if sharadar_class is None:
        logger.error("Could not find Sharadar class.")
        return {}, {}

    field_map = build_entity_field_map(sharadar_module)
    endpoint_tables = getattr(sharadar_class, '_endpoint_tables', {})

    # Structure: { Section: { Tag: { RawContent: { is_list, tables } } } }
    sections_data: dict[str, dict[str, dict[Any, Any]]] = {
        section: {} for section in SECTION_ORDER
    }

    for endpoint_maps in sharadar_class.get_data_block_endpoint_tag_map().values():
        for endpoint, field_mapping in endpoint_maps.items():
            table_name = endpoint_tables.get(endpoint, str(endpoint))
            for key_obj, value_obj in field_mapping.items():
                if key_obj not in field_map:
                    continue

                entity_name, internal_name = field_map[key_obj]
                section, prefix = ENTITY_SECTIONS[entity_name]
                tag = f"{prefix}{internal_name}"
                raw_content, is_list = get_field_info(value_obj)

                tag_definitions = sections_data[section].setdefault(tag, {})
                definition = tag_definitions.setdefault(raw_content, {"is_list": is_list, "tables": set()})
                definition["tables"].add(table_name)

    return sections_data, DERIVED_COLUMNS


# --- GENERATOR ---

def format_definition(
        raw_content: str | tuple[str, ...],
        is_list: bool,
        tables: set[str],
        derived_columns: dict[str, str],
) -> str:
    table_tooltip = " | ".join(sorted(tables))

    if is_list:
        inner_items = [f":abbr:`{item} ({table_tooltip})`" for item in raw_content]
        return f"[ {', '.join(inner_items)} ] `* <sharadar_preprocessed_legend_>`_"

    if raw_content in derived_columns:
        return f":abbr:`{derived_columns[raw_content]} ({table_tooltip})` `† <sharadar_derived_legend_>`_"

    return f":abbr:`{raw_content} ({table_tooltip})`"


def generate_sharadar_fields_rst(app: Sphinx) -> None:
    sections_data, derived_columns = extract_all_fields()
    if not sections_data:
        logger.warning("sharadar.rst will not be generated.")
        return

    rst_file_path = Path(app.srcdir) / 'data_providers' / 'sharadar.rst'
    logger.info("Generating sharadar.rst at: %s", rst_file_path)

    with rst_file_path.open('w', encoding='utf-8') as f:
        f.write(INTRO_TEXT + "\n\n")

        for section in SECTION_ORDER:
            tags_dict = sections_data.get(section)
            if not tags_dict:
                continue

            f.write(f"{section}\n")
            f.write(f"{'-' * len(section)}\n\n")

            f.write(".. list-table::\n")
            f.write("   :header-rows: 1\n\n")
            f.write("   * - Data Curator Tag\n")
            f.write("     - Sharadar Field\n")

            for tag in sorted(tags_dict):
                definitions = sorted(tags_dict[tag].items(), key=lambda item: str(item[0]))
                lines = [
                    format_definition(raw_content, info['is_list'], info['tables'], derived_columns)
                    for raw_content, info in definitions
                ]

                f.write(f"   * - {tag}\n")
                if len(lines) == 1:
                    f.write(f"     - {lines[0]}\n")
                else:
                    f.write(f"     - - {lines[0]}\n")
                    for line in lines[1:]:
                        f.write(f"       - {line}\n")

            f.write("\n")

        f.write("\n\n|\n\n")

        legend_title = "Data Processing"
        f.write(f"{legend_title}\n")
        f.write(f"{'-' * len(legend_title)}\n\n")
        f.write("Hover a field to see the Sharadar table it is read from. Tags Sharadar doesn't publish are not "
                "listed, and are left empty.\n\n")
        f.write(".. _sharadar_preprocessed_legend:\n\n")
        f.write(
            r"\* Fields enclosed in brackets [ ... ] with an asterisk indicate preprocessed tags." + "\n\n"
        )
        f.write(".. _sharadar_derived_legend:\n\n")
        f.write(r"† Values computed by the provider rather than read as published." + "\n")


def setup(app: Sphinx) -> dict[str, Any]:
    app.connect('builder-inited', generate_sharadar_fields_rst)
    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
