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

# The columns the provider joins into Sharadar's rows from another table: their label, and the table they are read from
DERIVED_COLUMNS = {
    'currency': ('currency, from the tickers table', 'tickers'),
}

INTRO_TEXT = """
.. _sharadar:

Sharadar
=============================

**Sharadar** is a provider of end of day US market data, fundamentals and corporate actions, covering both
active and delisted companies, which makes it suited to point in time research free of survivorship bias.
This library integrates Sharadar's query API to access historical prices, financial statements and corporate
actions for supported instruments.

- Visit their `official documentation <https://sharadar.com/docs/intro>`_.

- View available plans: `Sharadar Pricing Plans <https://sharadar.com/subscribe>`_.

Sharadar Features
------------------

Sharadar publishes its data as tables, all read through the same query API.
Below is an overview of the types of data available through this integration.

Market Data
~~~~~~~~~~~~~~~~~

Sharadar provides historical end of day **time-series data** for:

- **Stocks**
- **Funds** (ETFs, closed-end funds and ETNs)

Each instrument supports:

- **Price fields**:

  - Open, High, Low, Close, Volume

- **Adjustment types**:

  - Split-adjusted, for every price field
  - Raw (unadjusted), for the close only
  - Dividend & split-adjusted, for the close only

Fundamentals
~~~~~~~~~~~~~~~~~

Sharadar offers **as-reported financial statements** for public companies, available in:

- **Quarterly**
- **Annual**

The supported statement types include:

- Income Statements
- Balance Sheets
- Cash Flow Statements

Statements are point in time: each filing is dated on the day it was filed and is not revised by later filings,
so every day of a run carries the latest filing available on it.

Corporate Actions
~~~~~~~~~~~~~~~~~

- **Dividends**: ex-dividend date and amount, published only adjusted for later splits. The amount as paid can
  be derived in a custom calculation.
- **Splits**: date, numerator and denominator of the split ratio.

Technical Details
~~~~~~~~~~~~~~~~~

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


def extract_all_fields() -> tuple[dict[str, dict[str, dict[Any, Any]]], dict[str, tuple[str, str]]]:
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
        *,
        is_list: bool,
        tables: set[str],
        derived_columns: dict[str, tuple[str, str]],
) -> str:
    table_tooltip = " | ".join(sorted(tables))

    if is_list:
        inner_items = [f":abbr:`{item} ({table_tooltip})`" for item in raw_content]
        return f"[ {', '.join(inner_items)} ] `* <sharadar_preprocessed_legend_>`_"

    if raw_content in derived_columns:
        derived_label, derived_table = derived_columns[raw_content]
        return f":abbr:`{derived_label} ({derived_table})` `† <sharadar_derived_legend_>`_"

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
                    format_definition(
                        raw_content,
                        is_list=info['is_list'],
                        tables=info['tables'],
                        derived_columns=derived_columns,
                    )
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
        f.write(r"† Fields read from another Sharadar table than the rest of their section." + "\n")


def setup(app: Sphinx) -> dict[str, Any]:
    app.connect('builder-inited', generate_sharadar_fields_rst)
    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
