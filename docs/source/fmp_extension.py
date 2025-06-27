import importlib.util
import inspect
from pathlib import Path
from sphinx.util import logging
import types

logger = logging.getLogger(__name__)

PROJECT_ROOT = (Path(__file__).parent / '..' / '..').resolve()
FMP_PATH = PROJECT_ROOT / 'src' / 'kaxanuk' / 'data_curator' / 'data_providers' / 'financial_modeling_prep.py'
FMP_MODULE = 'kaxanuk.data_curator.data_providers.financial_modeling_prep'

FIELD_NAMES = {
    "_fields_dividend_data_rows": "Dividends",
    "_fields_split_data_rows": "Splits",
    "_fields_fundamental_income_data_rows": "Income",
    "_fields_fundamental_balance_sheet_data_rows": "Balance Sheet",
    "_fields_fundamental_cash_flow_data_rows": "Cash Flow",
    "_fields_fundamental_common_data_rows": "Fundamentals",
}

FIELD_PREFIXES = {
    "_fields_fundamental_balance_sheet_data_rows": "fbs_",
    "_fields_fundamental_cash_flow_data_rows": "fcf_",
    "_fields_fundamental_income_data_rows": "fis_",
    "_fields_fundamental_common_data_rows": "f_",
    "_fields_market_data_daily_rows_unadjusted": "m_",
    "_fields_market_data_daily_rows_split_adjusted": "m_",
    "_fields_market_data_daily_rows_dividend_and_split_adjusted": "m_",
    "_fields_dividend_data_rows": "d_",
    "_fields_split_data_rows": "s_",
}

SECTION_ORDER = [
    "Market Data",
    "_fields_dividend_data_rows",
    "_fields_split_data_rows",
    "_fields_fundamental_common_data_rows",
    "_fields_fundamental_income_data_rows",
    "_fields_fundamental_balance_sheet_data_rows",
    "_fields_fundamental_cash_flow_data_rows",
]

MARKET_SECTIONS = [
    "_fields_market_data_daily_rows_unadjusted",
    "_fields_market_data_daily_rows_split_adjusted",
    "_fields_market_data_daily_rows_dividend_and_split_adjusted",
]


def load_fmp_module():
    try:
        spec = importlib.util.spec_from_file_location(FMP_MODULE, FMP_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        logger.info("Successfully loaded FMP module: %s", module.__file__)
        return module
    except FileNotFoundError:
        logger.error("Could not find file %s", FMP_PATH)
        return None
    except ImportError as e:
        logger.error("Error loading financial_modeling_prep.py: %s", e)
        return None


def extract_mapped_fields():
    fmp_module = load_fmp_module()
    if fmp_module is None:
        logger.error("Failed to load financial_modeling_prep.py.")
        return {}

    fmp_class = None
    for _, obj in inspect.getmembers(fmp_module):
        if isinstance(obj, type) and obj.__name__ == "FinancialModelingPrep":
            fmp_class = obj
            break

    if fmp_class is None:
        logger.error("Could not find FinancialModelingPrep class in financial_modeling_prep.py.")
        return {}

    mapped_fields = {}
    for attr_name, attr_obj in inspect.getmembers(fmp_class):
        if attr_name.startswith("_fields_") and isinstance(attr_obj, types.MappingProxyType):
            mapped_fields[attr_name] = dict(attr_obj)
            logger.info("Mapping found: %s (%d elements)", attr_name, len(attr_obj))

    return mapped_fields


def generate_fmp_fields_rst(app):
    mapped_fields = extract_mapped_fields()
    if not mapped_fields:
        logger.warning("financial_modeling_prep.rst will not be generated because no mappings found.")
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

        # Market Data — maintain original field order
        seen_keys = set()
        all_market_fields = []
        for section in MARKET_SECTIONS:
            prefix = FIELD_PREFIXES.get(section, "")
            for local_key, api_key in mapped_fields.get(section, {}).items():
                tag = f"{prefix}{local_key}"
                if api_key and tag not in seen_keys:
                    seen_keys.add(tag)
                    all_market_fields.append((tag, api_key))

        if all_market_fields:
            f.write("Market Data\n")
            f.write("------------\n\n")
            f.write(".. list-table::\n")
            f.write("   :header-rows: 1\n\n")
            f.write("   * - Data Curator Tag\n")
            f.write("     - FMP Tag\n")
            for tag, api_key in all_market_fields:
                f.write(f"   * - {tag}\n")
                f.write(f"     - {api_key}\n")
            f.write("\n")

        # Other sections
        for section in SECTION_ORDER:
            if section == "Market Data":
                continue  # already written

            fields = mapped_fields.get(section)
            if not fields:
                continue

            display_name = FIELD_NAMES.get(section, section)
            f.write(f"{display_name}\n")
            f.write(f"{'-' * len(display_name)}\n\n")
            f.write(".. list-table::\n")
            f.write("   :header-rows: 1\n\n")
            f.write("   * - Data Curator Tag\n")
            f.write("     - FMP Tag\n")

            prefix = FIELD_PREFIXES.get(section, "")
            for local_key, api_key in fields.items():
                f.write(f"   * - {prefix}{local_key}\n")
                f.write(f"     - {api_key}\n")

            f.write("\n")


def setup(app):
    app.connect('builder-inited', generate_fmp_fields_rst)
    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
