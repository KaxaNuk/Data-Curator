import contextlib
import importlib.util
import inspect
import re
import sys
import types
from pathlib import Path
from typing import Any

from sphinx.application import Sphinx
from sphinx.util import logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = (Path(__file__).parent / '..' / '..').resolve()
LSEG_PATH = PROJECT_ROOT / 'src' / 'kaxanuk' / 'data_curator' / 'data_providers' / 'lseg_workspace.py'
LSEG_MODULE = 'kaxanuk.data_curator.data_providers.lseg_workspace'

SECTION_ORDER = [
    "Market Data",
    "Dividends",
    "Splits",
    "Fundamentals",
    "Income",
    "Balance Sheet",
    "Cash Flow",
]

ENTITY_CLASS_TO_SECTION: dict[str, tuple[str, str]] = {
    "FundamentalDataRow":                ("Fundamentals",  "f_"),
    "FundamentalDataRowIncomeStatement": ("Income",        "fis_"),
    "FundamentalDataRowBalanceSheet":    ("Balance Sheet", "fbs_"),
    "FundamentalDataRowCashFlow":        ("Cash Flow",     "fcf_"),
}

# Maps each LSEG API response column name to its position inside the
# corresponding *_FIELDS_LIST on LsegWorkspace.
# None = field is computed internally by the Data Curator; no LSEG TR field.
# Update this dict when a new column is added to lseg_workspace.py.
_COLUMN_FIELDS_LIST_POS: dict[str, tuple[str, int] | None] = {

    # ── Market Data ───────────────────────────────────────────────────────────
    # Source: MARKET_DATA_FIELDS_LIST
    "Date": ("MARKET_DATA_FIELDS_LIST", 0),
    "Open Price": ("MARKET_DATA_FIELDS_LIST", 1),
    "High Price": ("MARKET_DATA_FIELDS_LIST", 2),
    "Low Price": ("MARKET_DATA_FIELDS_LIST", 3),
    "Close Price": ("MARKET_DATA_FIELDS_LIST", 4),
    "Open Price_split": ("MARKET_DATA_FIELDS_LIST", 5),
    "High Price_split": ("MARKET_DATA_FIELDS_LIST", 6),
    "Low Price_split": ("MARKET_DATA_FIELDS_LIST", 7),
    "Close Price_split": ("MARKET_DATA_FIELDS_LIST", 8),
    "Volume": ("MARKET_DATA_FIELDS_LIST", 9),
    "VWAP": ("MARKET_DATA_FIELDS_LIST", 10),
    "Open Price_div_split": None,
    "High Price_div_split": None,
    "Low Price_div_split": None,
    "Close Price_div_split": None,

    # ── Dividends ─────────────────────────────────────────────────────────────
    # Source: DIVIDEND_FIELDS_LIST
    "Dividend Ex Date": ("DIVIDEND_FIELDS_LIST", 0),
    "Dividend Pay Date": ("DIVIDEND_FIELDS_LIST", 1),
    "Dividend Record Date": ("DIVIDEND_FIELDS_LIST", 2),
    "Dividend Announcement Date": ("DIVIDEND_FIELDS_LIST", 3),
    "Gross Dividend Amount": ("DIVIDEND_FIELDS_LIST", 4),
    "Adjusted Gross Dividend Amount": ("DIVIDEND_FIELDS_LIST", 5),

    # ── Splits ────────────────────────────────────────────────────────────────
    # Source: SPLIT_FIELDS_LIST
    "Capital Change Ex Date": ("SPLIT_FIELDS_LIST", 0),
    "Terms New Shares": ("SPLIT_FIELDS_LIST", 1),
    "Terms Old Shares": ("SPLIT_FIELDS_LIST", 2),

    # ── Fundamentals — row-level ──────────────────────────────────────────────
    # Source: INCOME_STATEMENT_FIELDS_LIST[0:2]
    "Original Announcement Date Time": ("INCOME_STATEMENT_FIELDS_LIST", 0),
    "Period End Date": ("INCOME_STATEMENT_FIELDS_LIST", 1),
    "Fiscal Period": None,
    "Fiscal Year": None,
    "Currency": None,

    # ── Income Statement ──────────────────────────────────────────────────────
    # Source: INCOME_STATEMENT_FIELDS_LIST
    "Earnings before Interest & Taxes (EBIT)":
        ("INCOME_STATEMENT_FIELDS_LIST", 2),
    "Earnings before Interest Taxes Depreciation & Amortization":
        ("INCOME_STATEMENT_FIELDS_LIST", 3),
    "EPS - Basic - incl Extraordinary Items, Common - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 4),
    "EPS - Diluted - incl Extraordinary Items, Common - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 5),
    "Income Available to Common Shares":
        ("INCOME_STATEMENT_FIELDS_LIST", 6),
    "Net Income after Tax":
        ("INCOME_STATEMENT_FIELDS_LIST", 7),
    "Interest & Dividend Income/(Expense) - Net - Finance":
        ("INCOME_STATEMENT_FIELDS_LIST", 8),
    "Operating Profit before Non-Recurring Income/Expense":
        ("INCOME_STATEMENT_FIELDS_LIST", 9),
    "Revenue from Business Activities - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 10),
    "Shares used to calculate Basic EPS - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 11),
    "Normalized Net Income from Continuing Operations":
        ("INCOME_STATEMENT_FIELDS_LIST", 12),
    "Operating Expenses - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 13),
    "Cost of Operating Revenue":
        ("INCOME_STATEMENT_FIELDS_LIST", 14),
    "Depreciation & Amortization":
        ("INCOME_STATEMENT_FIELDS_LIST", 15),
    "Discontinued Operations Net - Total - Income/(Expense)":
        ("INCOME_STATEMENT_FIELDS_LIST", 16),
    "Selling General & Administrative Expenses":
        ("INCOME_STATEMENT_FIELDS_LIST", 17),
    "Gross Revenue from Business Activities - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 18),
    "Income before Taxes":
        ("INCOME_STATEMENT_FIELDS_LIST", 19),
    "Excise Tax Expense":
        ("INCOME_STATEMENT_FIELDS_LIST", 20),
    "Interest Expense":
        ("INCOME_STATEMENT_FIELDS_LIST", 21),
    "Interest & Dividend Income - Finance - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 22),
    "Earnings Adjustments to Net Income - Other Expense/(Income)":
        ("INCOME_STATEMENT_FIELDS_LIST", 23),
    "Other Non-Operating Income/(Expense) - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 24),
    "Operating Expenses":
        ("INCOME_STATEMENT_FIELDS_LIST", 25),
    "Other Operating Expense":
        ("INCOME_STATEMENT_FIELDS_LIST", 26),
    "Adjustments to Net Income - Other":
        ("INCOME_STATEMENT_FIELDS_LIST", 27),
    "Research & Development Expense":
        ("INCOME_STATEMENT_FIELDS_LIST", 28),
    "Advertising Expense":
        ("INCOME_STATEMENT_FIELDS_LIST", 29),
    "Selling General & Administrative Expenses - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 30),
    "Shares used to calculate Diluted EPS - Total":
        ("INCOME_STATEMENT_FIELDS_LIST", 31),

    # ── Balance Sheet ─────────────────────────────────────────────────────────
    # Source: BALANCE_SHEET_FIELDS_LIST
    "Total Assets":
        ("BALANCE_SHEET_FIELDS_LIST", 0),
    "Total Liabilities":
        ("BALANCE_SHEET_FIELDS_LIST", 1),
    "Common Equity - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 2),
    "Retained Earnings - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 3),
    "Total Current Assets":
        ("BALANCE_SHEET_FIELDS_LIST", 4),
    "Total Current Liabilities":
        ("BALANCE_SHEET_FIELDS_LIST", 5),
    "Debt - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 6),
    "Net Debt":
        ("BALANCE_SHEET_FIELDS_LIST", 7),
    "Debt - Long-Term - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 8),
    "Preferred Shareholders Equity":
        ("BALANCE_SHEET_FIELDS_LIST", 9),
    "Comprehensive Income - Accumulated - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 10),
    "Common Stock - Additional Paid in Capital":
        ("BALANCE_SHEET_FIELDS_LIST", 11),
    "Capitalized Lease Obligations - Long-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 12),
    "Capital Lease Obligations - Long-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 12),
    "Cash & Cash Equivalents - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 13),
    "Cash & Short Term Investments - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 14),
    "Trade Accounts Payable & Accruals - Short-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 15),
    "Accounts & Notes Receivable - Trade - Net":
        ("BALANCE_SHEET_FIELDS_LIST", 16),
    "Accrued Expenses":
        ("BALANCE_SHEET_FIELDS_LIST", 17),
    "Capitalized Leases - Current Portion":
        ("BALANCE_SHEET_FIELDS_LIST", 18),
    "Loans & Receivables - Net - Short-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 19),
    "Income Taxes - Payable - Short-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 20),
    "Deferred Revenue - Long-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 21),
    "Goodwill - Net":
        ("BALANCE_SHEET_FIELDS_LIST", 22),
    "Investments - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 23),
    "Investments - Long-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 24),
    "Intangible Assets - excluding Goodwill - Net - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 25),
    "Intangible Assets - Total - Net":
        ("BALANCE_SHEET_FIELDS_LIST", 26),
    "Inventories - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 27),
    "Property Plant & Equipment - Net - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 28),
    "Minority Interests/Non-Controlling Interests - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 29),
    "Total Non-Current Assets":
        ("BALANCE_SHEET_FIELDS_LIST", 30),
    "Deferred Tax - Asset - Long-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 31),
    "Deferred Tax Liabilities - Long-Term":
        ("BALANCE_SHEET_FIELDS_LIST", 32),
    "Total Non-Current Liabilities":
        ("BALANCE_SHEET_FIELDS_LIST", 33),
    "Other Assets - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 34),
    "Other Current Assets - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 35),
    "Other Current Liabilities - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 36),
    "Other Liabilities - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 37),
    "Other Non-Current Assets - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 38),
    "Other Non-Current Liabilities - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 39),
    "Other Payables - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 40),
    "Receivables - Other - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 41),
    "Equity - Other":
        ("BALANCE_SHEET_FIELDS_LIST", 42),
    "Prepaid Expenses - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 43),
    "Short-Term Debt - Financial Sector - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 44),
    "Short-Term Investments - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 45),
    "Total Shareholders Equity":
        ("BALANCE_SHEET_FIELDS_LIST", 47),
    "Total Liabilities & Equity":
        ("BALANCE_SHEET_FIELDS_LIST", 48),
    "Accounts Payable":
        ("BALANCE_SHEET_FIELDS_LIST", 49),
    "Common Shares - Treasury - Total":
        ("BALANCE_SHEET_FIELDS_LIST", 50),

    # ── Cash Flow ─────────────────────────────────────────────────────────────
    # Source: CASH_FLOW_FIELDS_LIST
    "Non-GAAP Free Cash Flow - Company Reported":
        ("CASH_FLOW_FIELDS_LIST", 0),
    "Net Cash Flow from Operating Activities":
        ("CASH_FLOW_FIELDS_LIST", 1),
    "Common Stock Buyback - Net":
        ("CASH_FLOW_FIELDS_LIST", 2),
    "Accounts Payable - Increase/(Decrease) - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 3),
    "Accounts Receivables - Decrease/(Increase) - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 4),
    "Capital Expenditures - Net - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 5),
    "Net Change in Cash - Total":
        ("CASH_FLOW_FIELDS_LIST", 6),
    "Foreign Exchange Effects - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 7),
    "Dividends - Common - Cash Paid":
        ("CASH_FLOW_FIELDS_LIST", 8),
    "Stock - Common - Issued/Sold - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 9),
    "Deferred Inc Taxes & Income Tax Credits - CF - to Reconcile":
        ("CASH_FLOW_FIELDS_LIST", 10),
    "Dividends Paid - Cash - Total - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 11),
    "Interest Paid - Cash":
        ("CASH_FLOW_FIELDS_LIST", 12),
    "Inventories - Decrease/(Increase) - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 13),
    "Investment Securities - Sold/Matured - Unclassified - CF":
        ("CASH_FLOW_FIELDS_LIST", 14),
    "Investment Securities - Purchased - Unclassified - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 15),
    "Acquisition of Business - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 16),
    "Net Cash Flow from Investing Activities":
        ("CASH_FLOW_FIELDS_LIST", 17),
    "Net Cash Flow from Financing Activities":
        ("CASH_FLOW_FIELDS_LIST", 18),
    "Stock - Common - Issuance/(Retirement) - Net - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 19),
    "Debt - Issued - Long-Term & Short-Term - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 20),
    "Profit/(Loss) - Starting Line - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 21),
    "Income Taxes - Paid/(Reimbursed) - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 22),
    "Debt - Issued - Long-Term - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 23),
    "Debt - Issued - Short-Term - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 24),
    "Stock - Common Preferred & Other - Issued/Sold - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 25),
    "Other Financing Cash Flow - Increase/(Decrease)":
        ("CASH_FLOW_FIELDS_LIST", 26),
    "Other Investing Cash Flow - Decrease/(Increase)":
        ("CASH_FLOW_FIELDS_LIST", 27),
    "Other Non-Cash Items & adjustments - CF - to Reconcile":
        ("CASH_FLOW_FIELDS_LIST", 28),
    "Other Assets & Liabilities - Increase/(Decrease) - Net - CF":
        ("CASH_FLOW_FIELDS_LIST", 29),
    "Net Cash - Ending Balance":
        ("CASH_FLOW_FIELDS_LIST", 30),
    "Net Cash - Beginning Balance":
        ("CASH_FLOW_FIELDS_LIST", 31),
    "Dividends - Preferred - Cash Paid":
        ("CASH_FLOW_FIELDS_LIST", 32),
    "Stock - Preferred - Issued/Sold - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 33),
    "Property Plant & Equipment - Purchased - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 34),
    "Share Based Payments - Cash Flow - to Reconcile":
        ("CASH_FLOW_FIELDS_LIST", 35),
    "Working Capital - Increase/(Decrease) - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 36),
    "Depreciation Depletion & Amortization - Cash Flow":
        ("CASH_FLOW_FIELDS_LIST", 37),
}


# --- LOADER FUNCTIONS ---

def _setup_lseg_mocks() -> list[str]:
    to_mock = ['lseg', 'lseg.data', 'lseg.data.errors']
    mocked: list[str] = []
    for name in to_mock:
        if name not in sys.modules:
            mock_mod = types.ModuleType(name)
            if name == 'lseg.data.errors':
                mock_mod.LDError = type('LDError', (Exception,), {})  # type: ignore[attr-defined]
            sys.modules[name] = mock_mod
            mocked.append(name)
    lseg_mod = sys.modules.get('lseg')
    if lseg_mod is not None and not hasattr(lseg_mod, 'data'):
        lseg_mod.data = sys.modules['lseg.data']  # type: ignore[attr-defined]
    lseg_data_mod = sys.modules.get('lseg.data')
    if lseg_data_mod is not None and not hasattr(lseg_data_mod, 'errors'):
        lseg_data_mod.errors = sys.modules['lseg.data.errors']  # type: ignore[attr-defined]
    return mocked


def _teardown_lseg_mocks(mocked: list[str]) -> None:
    for name in mocked:
        sys.modules.pop(name, None)


def load_lseg_module() -> types.ModuleType | None:
    mocked = _setup_lseg_mocks()
    try:
        spec = importlib.util.spec_from_file_location(LSEG_MODULE, LSEG_PATH)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            logger.info("Successfully loaded LSEG module: %s", module.__file__)
            return module
        return None
    except FileNotFoundError:
        logger.error("Could not find file %s", LSEG_PATH)
        return None
    except (ImportError, SyntaxError, AttributeError) as e:
        logger.error("Error loading lseg_workspace.py: %s", e)
        return None
    finally:
        _teardown_lseg_mocks(mocked)


# --- FIELD INTROSPECTION ---

def build_entity_field_name_map(
    lseg_module: types.ModuleType,
) -> dict[Any, tuple[str, str]]:
    """entity_field_value -> (field_name, class_name). First definition wins."""
    field_map: dict[Any, tuple[str, str]] = {}
    target_entities = [
        "DividendDataRow",
        "FundamentalDataRow",
        "FundamentalDataRowBalanceSheet",
        "FundamentalDataRowCashFlow",
        "FundamentalDataRowIncomeStatement",
        "MarketDataDailyRow",
        "SplitDataRow",
    ]
    for entity_name in target_entities:
        cls = getattr(lseg_module, entity_name, None)
        if cls is None:
            continue
        for attr_name, attr_value in inspect.getmembers(cls):
            with contextlib.suppress(TypeError):
                if attr_value not in field_map:
                    field_map[attr_value] = (attr_name, entity_name)
    return field_map


def _simplify_tr_tag(template: str | None) -> str | None:
    """
    Strip date-range parameters (SDate, EDate, Frq) from a TR field template,
    keeping all semantic parameters (Adjusted, CAEventType, Period, ...).

    'TR.CLOSEPRICE(SDate={s},Frq=D,EDate={e},Adjusted=0)' -> 'TR.CLOSEPRICE(Adjusted=0)'
    'TR.CLOSEPRICE(SDate={s},Frq=D,EDate={e}).date'       -> 'TR.CLOSEPRICE.date'
    'TR.F.EBIT(Period=FQ0,SDate={s},EDate={e},Frq=FQ)'    -> 'TR.F.EBIT(Period=FQ0)'
    'TR.Volume(SDate={s},Frq=D,EDate={e})'                -> 'TR.Volume'
    """
    if template is None:
        return None
    m = re.match(r'^(.+?)(\([^)]*\))(\.\w+)?$', template)
    if not m:
        return template
    base, params_str, suffix = m.groups()
    suffix = suffix or ''
    skip = {'SDate', 'EDate', 'Frq'}
    kept = [
        p.strip() for p in params_str[1:-1].split(',')
        if p.strip() and not ('=' in p and p.split('=')[0].strip() in skip)
    ]
    return f"{base}({',' .join(kept)}){suffix}" if kept else f"{base}{suffix}"


def _build_column_tr_map(lseg_class: Any) -> dict[str, str | None]:
    """
    Build column_name -> simplified TR tag by reading the *_FIELDS_LIST
    attributes from lseg_class at runtime.

    The TR strings come directly from the FIELDS_LIST items, so any change to
    those strings in lseg_workspace.py is automatically reflected in the docs.
    """
    result: dict[str, str | None] = {}
    for col_name, pos in _COLUMN_FIELDS_LIST_POS.items():
        if pos is None:
            result[col_name] = None          # calculated internally
            continue
        list_attr, idx = pos
        fields_list: list[str] = getattr(lseg_class, list_attr, [])
        if idx < len(fields_list):
            result[col_name] = _simplify_tr_tag(fields_list[idx])
        else:
            logger.warning(
                "Index %d out of range for %s (len=%d); column %r will be omitted.",
                idx, list_attr, len(fields_list), col_name,
            )
    return result


def get_field_info(
    value_obj: Any,
    column_tr_map: dict[str, str | None],
) -> tuple[str | None | tuple[str | None, ...], bool]:
    """
    Return the simplified TR tag for an endpoint-map value using column_tr_map.

    - Plain string / StrEnum  ->  (tr_tag_or_None, False)
    - PreprocessedFieldMapping (has .tags list)  ->  (tuple of tr_tags, True)
    """
    if isinstance(value_obj, str):
        col_name = str(value_obj)
        if col_name not in column_tr_map:
            logger.warning("Column %r not in _COLUMN_FIELDS_LIST_POS; field omitted.", col_name)
            return None, False
        return column_tr_map[col_name], False

    if hasattr(value_obj, 'tags') and isinstance(value_obj.tags, list):
        tags: list[str | None] = []
        for t in value_obj.tags:
            col_name = str(t)
            if col_name not in column_tr_map:
                logger.warning("Column %r not in _COLUMN_FIELDS_LIST_POS; field omitted.", col_name)
            tags.append(column_tr_map.get(col_name))
        return tuple(tags), True

    col_name = str(value_obj)
    if col_name not in column_tr_map:
        logger.warning("Column %r not in _COLUMN_FIELDS_LIST_POS; field omitted.", col_name)
        return None, False
    return column_tr_map[col_name], False


# --- CORE LOGIC ---

# section -> tag -> set of TR tags (None = calculated internally)
SectionsData = dict[str, dict[str, set[str | None]]]


def _add_entry(
    sections_data: SectionsData,
    section: str,
    tag: str,
    raw_content: str | None | tuple[str | None, ...],
    *,
    is_list: bool,
) -> None:
    if section not in sections_data:
        return
    existing = sections_data[section].setdefault(tag, set())
    if isinstance(raw_content, tuple):
        existing.update(raw_content)
    else:
        existing.add(raw_content)


def extract_all_fields(lseg_module: types.ModuleType) -> SectionsData:
    lseg_class = getattr(lseg_module, "LsegWorkspace", None)
    if lseg_class is None:
        logger.error("Could not find LsegWorkspace class.")
        return {}

    if getattr(lseg_class, "Endpoints", None) is None:
        logger.error("Could not find Endpoints enum inside LsegWorkspace.")
        return {}

    # Build the mapping once from the live FIELDS_LISTs in lseg_class
    column_tr_map = _build_column_tr_map(lseg_class)
    field_name_map = build_entity_field_name_map(lseg_module)
    sections_data: SectionsData = {section: {} for section in SECTION_ORDER}

    # -- Market data ---------------------------------------------------------
    market_map = getattr(lseg_class, "_market_data_endpoint_map", {})
    for field_mapping in market_map.values():
        for key_obj, value_obj in field_mapping.items():
            if key_obj not in field_name_map:
                continue
            field_name, _ = field_name_map[key_obj]
            raw_content, is_list = get_field_info(value_obj, column_tr_map)
            _add_entry(sections_data, "Market Data", f"m_{field_name}",
                       raw_content, is_list=is_list)

    # -- Dividend data -------------------------------------------------------
    dividend_map = getattr(lseg_class, "_dividend_data_endpoint_map", {})
    for field_mapping in dividend_map.values():
        for key_obj, value_obj in field_mapping.items():
            if key_obj not in field_name_map:
                continue
            field_name, _ = field_name_map[key_obj]
            raw_content, is_list = get_field_info(value_obj, column_tr_map)
            _add_entry(sections_data, "Dividends", f"d_{field_name}",
                       raw_content, is_list=is_list)

    # -- Split data ----------------------------------------------------------
    split_map = getattr(lseg_class, "_split_data_endpoint_map", {})
    for field_mapping in split_map.values():
        for key_obj, value_obj in field_mapping.items():
            if key_obj not in field_name_map:
                continue
            field_name, _ = field_name_map[key_obj]
            raw_content, is_list = get_field_info(value_obj, column_tr_map)
            _add_entry(sections_data, "Splits", f"s_{field_name}",
                       raw_content, is_list=is_list)

    # -- Fundamental data (income / balance sheet / cash flow / common) ------
    fundamental_map = getattr(lseg_class, "_fundamental_data_endpoint_map", {})
    for field_mapping in fundamental_map.values():
        for key_obj, value_obj in field_mapping.items():
            if key_obj not in field_name_map:
                continue
            field_name, entity_class = field_name_map[key_obj]
            section_info = ENTITY_CLASS_TO_SECTION.get(entity_class)
            if section_info is None:
                continue
            section, prefix = section_info
            raw_content, is_list = get_field_info(value_obj, column_tr_map)
            _add_entry(sections_data, section, f"{prefix}{field_name}",
                       raw_content, is_list=is_list)

    return sections_data


# --- GENERATOR ---

_INTRO_TEXT = """\
.. _lseg:

LSEG Workspace
==============

`LSEG Workspace <https://www.lseg.com/en/data-analytics/products/workspace>`_ (London Stock Exchange Group) is a
professional financial data platform providing access to real-time and historical market data, financial statements,
analytics, and news across global asset classes, offering integration exclusively to active subscribers who possess
a valid subscription and a registered App Key.

This library integrates LSEG through the
`LSEG Data Library for Python <https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python/documentation>`_
(``lseg-data`` package), which connects to the platform either through the LSEG Workspace
desktop application or directly through the LSEG Data Platform.

.. admonition:: Workspace must be running

   The LSEG Workspace desktop application must be open and running on the same machine
   as your Python code when using the Data Curator with this provider.
   If Workspace is closed, all data requests will fail.


LSEG Features
-------------

Market Data
~~~~~~~~~~~

Historical end-of-day time series for a wide range of global instruments, including equities
(stocks, ETFs), indexes, fixed income, foreign exchange, commodities, and funds.

Price fields available per instrument: Open, High, Low, Close, Volume, and VWAP.
Prices are provided in two variants: unadjusted and split-adjusted.
See the :ref:`market-data-table` below for the exact TR tags corresponding to each variant.

Corporate Actions
~~~~~~~~~~~~~~~~~

- **Dividends**: ex-date, pay date, record date, declaration date, gross and adjusted amounts.
- **Splits**: ex-date, numerator and denominator of the split ratio.

Fundamentals
~~~~~~~~~~~~

Standardized quarterly financial statements for public companies:

- Income Statements
- Balance Sheets
- Cash Flow Statements

All fundamental TR fields use ``Period=FQ0``.
See the :ref:`fundamentals-table` below for the exact TR tags.


.. _lseg_setup:

Setup
-----

**Requirements**: An active LSEG Workspace subscription.

**Step 1 - Generate your App Key**

The App Key is generated from inside LSEG Workspace:

1. Open the LSEG Workspace application.
2. In the search bar, type **App Key Generator** and open it.
3. In the **App Key Generator**, enter a display name for your application and click **Register New App**.
4. Select (at minimum) the first three API checkboxes to enable data access.
5. Copy the generated App Key.

.. note::

   The App Key Generator was previously called **App Creator** in earlier versions of
   LSEG Workspace / Eikon. The functionality is equivalent.

**Step 2 - Add the App Key to your** ``.env`` **file**

In your project's ``Config/.env`` file, add:

.. code-block:: ini

   KNDC_APP_KEY_LSEG=your_app_key_here

**Step 3 - Keep Workspace open**

When running Data Curator with the LSEG provider, ensure the LSEG Workspace desktop
application is open and you are signed in before executing any data requests.
"""

_CALCULATED_MARKER = "*(calculated internally)*"


def _format_cell(tr_tags: set[str | None]) -> str:
    sorted_tags = sorted(tr_tags, key=lambda t: (t is None, t or ""))
    parts: list[str] = []
    parts = [_CALCULATED_MARKER if tag is None else f"``{tag}``" for tag in sorted_tags]
    return " | ".join(parts)


def generate_lseg_fields_rst(app: Sphinx) -> None:
    lseg_module = load_lseg_module()
    if lseg_module is None:
        logger.warning("lseg_workspace.rst will not be regenerated: module load failed.")
        return

    sections_data = extract_all_fields(lseg_module)
    if not sections_data:
        logger.warning("lseg_workspace.rst will not be regenerated: no field data extracted.")
        return

    rst_path = Path(app.srcdir) / 'data_providers' / 'lseg_workspace.rst'
    logger.info("Generating lseg_workspace.rst at: %s", rst_path)

    with rst_path.open('w', encoding='utf-8') as f:
        f.write(_INTRO_TEXT.strip() + "\n\n")

        for section in SECTION_ORDER:
            tags_dict = sections_data.get(section)
            if not tags_dict:
                continue
            if section == "Market Data":
                f.write("\n.. _market-data-table:\n\n")
            if section == "Fundamentals":
                f.write("\n.. _fundamentals-table:\n\n")
            f.write(f"\n{section}\n")
            f.write(f"{'-' * len(section)}\n\n")

            f.write(".. list-table::\n")
            f.write("   :header-rows: 1\n\n")
            f.write("   * - Data Curator Tag\n")
            f.write("     - LSEG Field\n")
            for tag in sorted(tags_dict.keys()):
                tr_tags = tags_dict[tag]
                if not tr_tags or all(t is None for t in tr_tags):
                    continue  # omit calculated-only fields from the table
                f.write(f"   * - {tag}\n")
                f.write(f"     - {_format_cell(tr_tags)}\n")
            f.write("\n")


def setup(app: Sphinx) -> dict[str, Any]:
    app.connect('builder-inited', generate_lseg_fields_rst)
