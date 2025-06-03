import importlib.util
import inspect
from sphinx.util import logging
from pathlib import Path
from docutils.parsers.rst import Directive
from docutils import nodes

logger = logging.getLogger(__name__)

# Path to the calculations.py file, adjusted to your project's structure.
PROJECT_ROOT = (Path(__file__).parent/'..'/'..').resolve()
CALCULATIONS_PATH = Path(PROJECT_ROOT)/'src'/'kaxanuk'/'data_curator'/'features'/'calculations.py'
CALCULATIONS_MODULE = 'kaxanuk.data_curator.features.calculations'

def load_calculations_module():
    """
    Dynamically loads the calculations module.
    """
    try:
        spec = importlib.util.spec_from_file_location(CALCULATIONS_MODULE, CALCULATIONS_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except FileNotFoundError:
        logger.error("Could not find the file %s", CALCULATIONS_PATH)
        return None
    except ImportError as e:
        logger.error("Error loading calculations module: %s", e)
        return None

def extract_functions_and_categories():
    """
    Extracts functions and their categories from the calculations module.
    """
    calculations_module = load_calculations_module()

    if calculations_module is None:
        return {}

    functions_by_category = {}

    # Category marker used in the function docstring
    category_marker = ".. category::"

    for name, func in inspect.getmembers(calculations_module, inspect.isfunction):
        docstring = inspect.getdoc(func)

        if docstring and category_marker in docstring:
            # Extract the category from the docstring
            lines = docstring.splitlines()
            for line in lines:
                if line.strip().startswith(category_marker):
                    current_category = line.split(category_marker)[-1].strip()
                    break
            else:
                current_category = 'Uncategorized'  # Fallback if no category is found
        else:
            current_category = 'Uncategorized'

        if current_category not in functions_by_category:
            functions_by_category[current_category] = []
        functions_by_category[current_category].append(name)

        logger.info("Function '%s' assigned to category '%s'.", name, current_category)

    return functions_by_category

def generate_category_rst(app):
    """
    Generates the features.rst file based on the extracted categories and functions.
    """
    functions_by_category = extract_functions_and_categories()

    if not functions_by_category:
        logger.warning("No categories or functions found in calculations.py.")
        return

    # Path where the features.rst file will be generated
    features_rst_path = Path(app.srcdir)/'features.rst'

    logger.info("Generating features.rst at %s", features_rst_path)

    # Introductory text to be added at the beginning of the file
    introductory_text = """
Here you can find how we calculate all features from market, fundamental and alternative data.
You can implement your own columns by injecting a custom_calculations module into data_curator.main().
Each output column corresponds to a function with the exact same name but prepended with "c_".
For example column "my_feature" corresponds to function "c_my_feature".

Every function defines as its parameters the names of the columns it uses as input, which are passed
to it as a DataColumn object wrapping a pyarrow array. You can use basic arithmetic on the DataColumn's
and any rows with nulls, divisions by zero, etc. will return null.

Every function must return an iterable of the same length as the input columns that is compatible with
pyarrow arrays, including pandas.Series, numpy 1-dimensional ndarray and our own DataColumns. The result
will be automatically wrapped into a DataColumn.

* If you need to use pandas methods in your functions, you can convert any DataColumn to pandas.Series
  with the .to_pandas() method.

* If you need to use pyarrow.compute methods in your functions, you need to use the .to_pyarrow() method
  on the columns.

There's examples of both approaches in the following functions.
"""

    with Path.open(features_rst_path, 'w', encoding='utf-8') as f:
        f.write("Features\n")
        f.write("====================\n\n")
        f.write(introductory_text.strip() + "\n\n")  # Add the introductory text

        for category, functions in functions_by_category.items():
            f.write(f"{category}\n")
            f.write(f"{'-' * len(category)}\n\n")
            for func in functions:
                # Generate the 'autofunction' directive for each function
                f.write(f".. autofunction:: calculations.{func}\n")
            f.write("\n")

class CategoryDirective(Directive):
    has_content = True

    def run(self):
        return [nodes.comment()]

def setup(app):
    app.add_directive("category", CategoryDirective)
    app.connect('builder-inited', generate_category_rst)
