# Configuration file for the Sphinx documentation builder.

# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import sys
from pathlib import Path

sys.path.insert(0, str(Path().resolve()))
sys.path.insert(1, str((Path('..') / '..' / 'templates'/ 'data_curator'/'Config').resolve()))
sys.path.insert(2, str((Path('..') / '..' / 'src').resolve()))
sys.path.insert(3, str((Path('..') / '..' / 'src' / 'kaxanuk').resolve()))
sys.path.insert(4, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator').resolve()))
sys.path.insert(5, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'config_handlers').resolve()))
sys.path.insert(6, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'data_providers').resolve()))
sys.path.insert(7, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'entities').resolve()))
sys.path.insert(8, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'exceptions').resolve()))
sys.path.insert(9, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'features').resolve()))
sys.path.insert(10, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'modules').resolve()))
sys.path.insert(11, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'output_handlers').resolve()))
sys.path.insert(12, str((Path('..') / '..' / 'src' / 'kaxanuk' / 'data_curator' / 'services').resolve()))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Data Curator'
project_copyright = 'KaxaNuk - Kaxan means Seek and Find, and Nuuk Answer in Mayan'
author = 'KaxaNuk'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    "myst_parser",
    "sphinx_ext"
]

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# This function is used to load the KaxaNuk documentation template.
def setup(app):
    app.add_css_file('sidebar.css')
    app.add_css_file('general.css')
    app.add_css_file('content.css')

