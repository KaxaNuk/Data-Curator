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
FMP_PATH = PROJECT_ROOT / 'src' / 'kaxanuk' / 'data_curator' / 'data_providers' / 'financial_modeling_prep.py'
FMP_MODULE = 'kaxanuk.data_curator.data_providers.financial_modeling_prep'

SECTION_ORDER = [
    "Market Data",
    "Dividends",
    "Splits",
    "Fundamentals",
    "Income",
    "Balance Sheet",
    "Cash Flow",
]

COMMON_FUNDAMENTAL_FIELDS = {
    "accepted_date",
    "filing_date",
    "fiscal_period",
    "fiscal_year",
    "period_end_date",
    "reported_currency",
}


# --- LOADER FUNCTIONS ---

def load_fmp_module() -> types.ModuleType | None:
    try:
        spec = importlib.util.spec_from_file_location(FMP_MODULE, FMP_PATH)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            logger.info("Successfully loaded FMP module: %s", module.__file__)
            return module
        return None
    except FileNotFoundError:
        logger.error("Could not find file %s", FMP_PATH)
        return None
    except (ImportError, SyntaxError, AttributeError) as e:
        logger.error("Error loading financial_modeling_prep.py: %s", e)
        return None


def build_entity_field_name_map(fmp_module: types.ModuleType) -> dict[Any, str]:
    reverse_map = {}
    target_entities = [
        "DividendDataRow",
        "FundamentalDataRow",
        "FundamentalDataRowBalanceSheet",
        "FundamentalDataRowCashFlow",
        "FundamentalDataRowIncomeStatement",
        "MarketDataDailyRow",
        "SplitDataRow"
    ]

    for entity_name in target_entities:
        cls = getattr(fmp_module, entity_name, None)
        if cls:
            for name, value in inspect.getmembers(cls):
                with contextlib.suppress(TypeError):
                    reverse_map[value] = name
    return reverse_map


# --- CORE LOGIC ---

def resolve_tag_name(key_obj: Any, field_name_map: dict[Any, str]) -> str:
    if key_obj in field_name_map:
        return field_name_map[key_obj]
    elif hasattr(key_obj, 'name'):
        return str(key_obj.name)
    return str(key_obj)


def classify_field(
        internal_name: str,
        original_section: str,
        default_prefix: str
) -> tuple[str, str]:
    if (
            internal_name in COMMON_FUNDAMENTAL_FIELDS
            and original_section in ["Income", "Balance Sheet", "Cash Flow"]
    ):
        return "Fundamentals", "f_"
    return original_section, default_prefix


def get_field_info(value_obj: Any) -> tuple[str | tuple[str, ...], bool]:
    """
    Analyzes the source object.
    Returns: (raw_content, is_list)
    - raw_content: string if simple, tuple of strings if preprocessed list
    - is_list: boolean
    """
    if isinstance(value_obj, str):
        return value_obj, False

    # Check for PreprocessedFieldMapping via 'tags' attribute
    if hasattr(value_obj, 'tags'):
        tags = value_obj.tags
        if isinstance(tags, list):
            # Return as tuple to be hashable for dictionary keys later
            return tuple(str(t) for t in tags), True

    return str(value_obj), False


def process_endpoint_map(
        data_map: dict[Any, Any],
        endpoint_config: dict[Any, tuple[str, str]],
        field_name_map: dict[Any, str]
) -> list[tuple[str, str, str | tuple[str, ...], bool, str]]:
    extracted_entries = []

    for endpoint, field_mapping in data_map.items():
        if endpoint not in endpoint_config:
            continue

        original_section, default_prefix = endpoint_config[endpoint]

        # Get the literal URL from the Enum value
        endpoint_url = getattr(endpoint, 'value', str(endpoint))

        for key_obj, value_obj in field_mapping.items():
            internal_name = resolve_tag_name(key_obj, field_name_map)
            section, prefix = classify_field(
                internal_name, original_section, default_prefix
            )
            final_tag = f"{prefix}{internal_name}"

            # Get raw data
            raw_content, is_list = get_field_info(value_obj)

            extracted_entries.append(
                (section, final_tag, raw_content, is_list, endpoint_url)
            )

    return extracted_entries


# --- ORCHESTRATOR ---

def extract_all_fields() -> dict[str, dict[str, dict[Any, Any]]]:
    fmp_module = load_fmp_module()
    if fmp_module is None:
        return {}

    fmp_class = getattr(fmp_module, "FinancialModelingPrep", None)
    if fmp_class is None:
        logger.error("Could not find FinancialModelingPrep class.")
        return {}

    endpoints_enum = getattr(fmp_class, "Endpoints", None)
    if endpoints_enum is None:
        logger.error("Could not find Endpoints enum.")
        return {}

    field_name_map = build_entity_field_name_map(fmp_module)

    maps_config = [
        (
            "_market_data_endpoint_map",
            {
                endpoints_enum.MARKET_DATA_DAILY_UNADJUSTED: ("Market Data", "m_"),
                endpoints_enum.MARKET_DATA_DAILY_SPLIT_ADJUSTED: ("Market Data", "m_"),
                endpoints_enum.MARKET_DATA_DAILY_DIVIDEND_AND_SPLIT_ADJUSTED: ("Market Data", "m_"),
            }
        ),
        (
            "_dividend_data_endpoint_map",
            {
                endpoints_enum.STOCK_DIVIDEND: ("Dividends", "d_")
            }
        ),
        (
            "_fundamental_data_endpoint_map",
            {
                endpoints_enum.INCOME_STATEMENT: ("Income", "fis_"),
                endpoints_enum.BALANCE_SHEET_STATEMENT: ("Balance Sheet", "fbs_"),
                endpoints_enum.CASH_FLOW_STATEMENT: ("Cash Flow", "fcf_"),
            }
        ),
        (
            "_split_data_endpoint_map",
            {
                endpoints_enum.STOCK_SPLIT: ("Splits", "s_")
            }
        )
    ]

    # Structure: { Section: { Tag: { RawContent: { is_list, urls } } } }
    sections_data: dict[str, dict[str, dict[Any, Any]]] = {
        section: {} for section in SECTION_ORDER
    }

    for map_attr, endpoint_config in maps_config:
        data_map = getattr(fmp_class, map_attr, {})
        entries = process_endpoint_map(data_map, endpoint_config, field_name_map)

        for section, tag, raw_content, is_list, url in entries:
            if section in sections_data:
                if tag not in sections_data[section]:
                    sections_data[section][tag] = {}

                # Group by the content definition
                if raw_content not in sections_data[section][tag]:
                    sections_data[section][tag][raw_content] = {
                        "is_list": is_list,
                        "urls": set()
                    }

                sections_data[section][tag][raw_content]["urls"].add(url)

    return sections_data


# --- GENERATOR ---

def generate_fmp_fields_rst(app: Sphinx) -> None:
    sections_data = extract_all_fields()
    if not sections_data:
        logger.warning("financial_modeling_prep.rst will not be generated.")
        return

    rst_file_path = Path(app.srcdir) / 'data_providers' / 'financial_modeling_prep.rst'
    logger.info("Generating financial_modeling_prep.rst at: %s", rst_file_path)

    intro_text = """
.. _fmp:

Financial Modeling Prep
=============================

**FMP** is a trusted provider of stock market and financial data,
offering a wide range of standardized and audited financial information.
This library integrates FMP's API to access historical market prices and
core financial statements for supported instruments.

- Visit their `official documentation <https://site.financialmodelingprep.com/developer/docs/stable>`_.
- View available plans through our referral link: `FMP Pricing Plans`_

.. _FMP Pricing Plans: https://site.financialmodelingprep.com/pricing-plans?couponCode=xss2L2sI


FMP Features
-------------

FMP offers a wide range of financial and market data through a unified REST API.
Below is an overview of the types of data available through this integration.

Market Data
~~~~~~~~~~~~~~~~~

FMP provides historical **time-series data** for multiple asset classes:

- **Stocks**
- **ETFs**
- **Indexes**
- **Cryptocurrencies**
- **Commodities**
- **Forex**

Each asset class supports:

- **Price fields**:
  - Open, High, Low, Close, Volume, VWAP
- **Adjustment types**:
  - Raw (unadjusted)
  - Split-adjusted
  - Dividend & split-adjusted

Fundamentals
~~~~~~~~~~~~~~~~~

FMP offers **audited and standardized financial statements** for public companies, available in:

- **Quarterly**
- **Annual**

The supported statement types include:

- Income Statements
- Balance Sheets
- Cash Flow Statements

Technical Details
~~~~~~~~~~~~~~~~~

This library uses FMP's REST endpoints to fetch:

- Time series for historical price data.
- Standardized fundamentals via financial statements.
- Fully configurable columns and date ranges.

Authentication is managed using an API key placed in the `.env` file via the variable:

.. code-block:: ini

   KNDC_API_KEY_FMP=your_key_here
""".strip()

    with rst_file_path.open('w', encoding='utf-8') as f:
        f.write(intro_text + "\n\n")

        for section in SECTION_ORDER:
            tags_dict = sections_data.get(section)
            if not tags_dict:
                continue

            f.write(f"{section}\n")
            f.write(f"{'-' * len(section)}\n\n")

            f.write(".. list-table::\n")
            f.write("   :header-rows: 1\n\n")
            f.write("   * - Data Curator Tag\n")
            f.write("     - FMP Tag\n")

            sorted_tags = sorted(tags_dict.keys())

            for tag in sorted_tags:
                definitions = []
                for raw_content, info in tags_dict[tag].items():
                    definitions.append((raw_content, info['is_list'], info['urls']))

                definitions.sort(key=lambda x: str(x[0]))

                f.write(f"   * - {tag}\n")

                def format_line(defn: tuple[str | tuple[str, ...], bool, set[str]]) -> str:
                    raw_content, is_list, urls = defn

                    # Create the tooltip string: URL | URL
                    url_tooltip = " | ".join(sorted(urls))

                    if is_list:
                        # Format: [ :abbr:`tag (url)` , :abbr:`tag (url)` ]
                        inner_items = [
                            f":abbr:`{item} ({url_tooltip})`" for item in raw_content
                        ]

                        joined_content = ", ".join(inner_items)
                        # Spaces inside brackets
                        content_str = f"[ {joined_content} ]"

                        # Add asterisk link for preprocessed items
                        content_str += " `* <preprocessed_legend_>`_"
                    else:
                        content_str = f":abbr:`{raw_content} ({url_tooltip})`"

                    return content_str

                if len(definitions) == 1:
                    f.write(f"     - {format_line(definitions[0])}\n")
                else:
                    f.write("     - ")
                    first = True
                    for defn in definitions:
                        if first:
                            f.write(f"- {format_line(defn)}\n")
                            first = False
                        else:
                            f.write(f"       - {format_line(defn)}\n")

            f.write("\n")

        # Notation section at the end
        f.write("\n\n")
        f.write("|\n\n")

        legend_title = "Data Processing"
        f.write(f"{legend_title}\n")
        f.write(f"{'-' * len(legend_title)}\n\n")

        f.write(".. _preprocessed_legend:\n\n")
        f.write(
            r"\* Fields enclosed in brackets [ ... ] with an asterisk indicate "
            r"preprocessed tags (hover to see source endpoint)." + "\n"
        )


def setup(app: Sphinx) -> dict[str, Any]:
    app.connect('builder-inited', generate_fmp_fields_rst)
    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
