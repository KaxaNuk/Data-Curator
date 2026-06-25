import importlib.util
import inspect
import shutil
import types
from pathlib import Path
from typing import Any

from docutils import nodes
from docutils.parsers.rst import Directive
from sphinx.application import Sphinx
from sphinx.util import logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = (Path(__file__).parent / '..' / '..').resolve()
HELPERS_PATH = PROJECT_ROOT / 'src' / 'kaxanuk' / 'data_curator' / 'features' / 'helpers.py'
HELPERS_MODULE = 'kaxanuk.data_curator.features.helpers'


def load_helpers_module() -> types.ModuleType | None:
    try:
        spec = importlib.util.spec_from_file_location(HELPERS_MODULE, HELPERS_PATH)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        return None
    except (ImportError, FileNotFoundError, AttributeError, SyntaxError) as e:
        logger.error("Failed to load helpers module: %s", e)
        return None


def extract_functions_by_category() -> dict[str, list[tuple[str, str]]]:
    mod = load_helpers_module()
    if mod is None:
        return {}

    category_marker = ".. category::"
    grouped: dict[str, list[tuple[str, str]]] = {}

    for name, func in inspect.getmembers(mod, inspect.isfunction):
        if func.__module__ != HELPERS_MODULE:
            continue

        docstring = inspect.getdoc(func) or ""
        lines = docstring.splitlines()
        first_line = next((line.strip() for line in lines if line.strip()), "")
        category = "Available Helper Functions"

        for line in lines:
            if line.strip().startswith(category_marker):
                category = line.split(category_marker, 1)[-1].strip()
                break

        grouped.setdefault(category, []).append((name, first_line))

    return grouped


def generate_helpers_rst(app: Sphinx, config: Any) -> None:
    functions_by_category = extract_functions_by_category()
    if not functions_by_category:
        logger.warning("No functions found in helpers.py.")
        return

    root = Path(app.srcdir) / "api_reference"
    rst_file = root / "helpers.rst"
    api_dir = root / "helpers_api"

    if api_dir.exists():
        shutil.rmtree(api_dir)
        logger.info("Deleted stale dir: %s", api_dir)
    api_dir.mkdir(parents=True)

    if rst_file.exists():
        rst_file.unlink()
        logger.info("Deleted old file: %s", rst_file)

    logger.info("Generating new helpers.rst at %s", rst_file)
    with rst_file.open('w', encoding='utf-8') as f:
        f.write(f""".. _helpers:

Helpers
=======

.. currentmodule:: {HELPERS_MODULE}

Calculation Helpers
--------------------------




This section lists all the **underlying calculation helper functions** provided by Data Curator.
These functions act as the mathematical and statistical engine for the main feature indicators.

To use one of these helpers:

- Import and call them directly within your custom calculations or when developing new feature modules.
- Unlike main features, they are not typically called directly from the Excel configuration file.

All functions operate on ``DataColumn`` inputs and return new ``DataColumn`` objects,
ensuring compatibility with our internal data.

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
                f.write(f"   * - :ref:`{name} <{name}_helper_ref>`\n")

            f.write("\n")

        f.write(".. toctree::\n")
        f.write("   :hidden:\n")
        f.write("   :maxdepth: 1\n\n")
        for category in sorted(functions_by_category):
            cat_dir = category.replace(" ", "_").lower()
            f.write(f"   helpers_api/{cat_dir}/index\n")

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
                index_file.write(f"   * - :ref:`{name} <{name}_helper_ref>`\n")

            index_file.write("\n.. toctree::\n")
            index_file.write("   :hidden:\n\n")
            for name, _ in sorted(functions_by_category[category], key=lambda x: x[0]):
                index_file.write(f"   {name}\n")

        for name, _ in functions_by_category[category]:
            path = category_path / f"{name}.rst"
            with path.open("w", encoding="utf-8") as out:
                out.write(f".. _{name}_helper_ref:\n\n")
                out.write(f"{name}\n")
                out.write(f"{'=' * len(name)}\n\n")
                out.write(f".. currentmodule:: {HELPERS_MODULE}\n\n")
                out.write(f".. autofunction:: {name}\n")
                out.write("   :no-index:\n")


class CategoryDirective(Directive):
    has_content = True

    def run(self) -> list[nodes.Node]:
        return [nodes.comment()]


def setup(app: Sphinx) -> dict[str, Any]:
    app.connect("config-inited", generate_helpers_rst)
    return {
        "version": "1.2",
        "parallel_read_safe": True,
        "parallel_write_safe": True
    }
