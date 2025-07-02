import importlib.util
import inspect
import shutil
from sphinx.util import logging
from pathlib import Path
from docutils.parsers.rst import Directive
from docutils import nodes

logger = logging.getLogger(__name__)

PROJECT_ROOT = (Path(__file__).parent / '..' / '..').resolve()
CALCULATIONS_PATH = PROJECT_ROOT / 'src' / 'kaxanuk' / 'data_curator' / 'features' / 'calculations.py'
CALCULATIONS_MODULE = 'kaxanuk.data_curator.features.calculations'


def load_calculations_module():
    try:
        spec = importlib.util.spec_from_file_location(CALCULATIONS_MODULE, CALCULATIONS_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except (ImportError, FileNotFoundError, AttributeError, SyntaxError) as e:
        logger.error("Failed to load calculations module: %s", e)
        return None


def extract_functions_by_category():
    mod = load_calculations_module()
    if mod is None:
        return {}

    category_marker = ".. category::"
    grouped = {}

    for name, func in inspect.getmembers(mod, inspect.isfunction):
        if func.__module__ != CALCULATIONS_MODULE:
            continue

        docstring = inspect.getdoc(func) or ""
        lines = docstring.splitlines()
        first_line = next((line.strip() for line in lines if line.strip()), "")
        category = "Uncategorized"

        for line in lines:
            if line.strip().startswith(category_marker):
                category = line.split(category_marker, 1)[-1].strip()
                break

        grouped.setdefault(category, []).append((name, first_line))

    return grouped


def generate_features_rst(app, config):
    functions_by_category = extract_functions_by_category()
    if not functions_by_category:
        logger.warning("No functions found in calculations.py.")
        return

    root = Path(app.srcdir) / "api_reference"
    rst_file = root / "features.rst"
    api_dir = root / "api"

    if api_dir.exists():
        shutil.rmtree(api_dir)
        logger.info("Deleted stale dir: %s", api_dir)
    api_dir.mkdir(parents=True)

    if rst_file.exists():
        rst_file.unlink()
        logger.info("Deleted old file: %s", rst_file)

    logger.info("Generating new features.rst at %s", rst_file)

    with rst_file.open('w', encoding='utf-8') as f:
        f.write(""".. _features:

Features
========

.. currentmodule:: kaxanuk.data_curator.features.calculations

Available Calculation Functions
-------------------------------

This section lists all the **predefined calculation functions** provided by Data Curator.
Each function corresponds to a feature that can be used as an output column in your Excel configuration file.

To use one of these features:

- Reference its name (without the ``c_`` prefix) in the ``Output_Columns`` sheet.
- The system will automatically match it to the Python function ``c_<name>``.

All functions operate on `DataColumn` inputs and return iterable values compatible with our internal data.

""")

        for category in sorted(functions_by_category):
            entries = sorted(functions_by_category[category], key=lambda x: x[0])

            f.write(f"{category}\n")
            f.write(f"{'~' * len(category)}\n\n")
            f.write(".. list-table::\n")
            f.write("   :header-rows: 1\n")
            f.write("   :widths: 100\n\n")
            f.write("   * - Function\n")

            for name, _ in entries:
                f.write(f"   * - :ref:`{name} <{name}_ref>`\n")

            f.write("\n")

        f.write(".. toctree::\n")
        f.write("   :hidden:\n")
        f.write("   :maxdepth: 1\n\n")
        for category in sorted(functions_by_category):
            cat_dir = category.replace(" ", "_").lower()
            f.write(f"   api/{cat_dir}/index\n")

    for category in functions_by_category:
        cat_dir = category.replace(" ", "_").lower()
        category_path = api_dir / cat_dir
        category_path.mkdir(parents=True, exist_ok=True)

        index_path = category_path / "index.rst"
        with index_path.open("w", encoding="utf-8") as index_file:
            index_file.write(f"{category}\n")
            index_file.write(f"{'=' * len(category)}\n\n")

            index_file.write(".. list-table::\n")
            index_file.write("   :header-rows: 1\n")
            index_file.write("   :widths: 100\n\n")
            index_file.write("   * - Function\n")

            for name, _ in sorted(functions_by_category[category], key=lambda x: x[0]):
                index_file.write(f"   * - :ref:`{name} <{name}_ref>`\n")

            index_file.write("\n.. toctree::\n")
            index_file.write("   :hidden:\n\n")
            for name, _ in sorted(functions_by_category[category], key=lambda x: x[0]):
                index_file.write(f"   {name}\n")

        for name, _ in functions_by_category[category]:
            path = category_path / f"{name}.rst"
            with path.open("w", encoding="utf-8") as out:
                out.write(f".. _{name}_ref:\n\n")
                out.write(f"{name}\n")
                out.write(f"{'=' * len(name)}\n\n")
                out.write(f".. currentmodule:: {CALCULATIONS_MODULE}\n\n")
                out.write(f".. autofunction:: {name}\n")
                out.write("   :no-index:\n")


class CategoryDirective(Directive):
    has_content = True
    def run(self):
        return [nodes.comment()]


def setup(app):
    app.add_directive("category", CategoryDirective)
    app.connect("config-inited", generate_features_rst)
    return {
        "version": "1.2",
        "parallel_read_safe": True,
        "parallel_write_safe": True
    }
