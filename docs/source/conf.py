# Configuration file for the Sphinx documentation builder.

import sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path().resolve()))
sys.path.insert(1, str((Path('..') / '..' / 'templates' / 'data_curator' / 'Config').resolve()))
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
project = 'Data Curator'
author = 'KaxaNuk'
copyright = (
    f"{datetime.now(ZoneInfo('America/Mexico_City')).year}, "
    "KaxaNuk - Kaxan means Seek and Find, and Nuuk Answer in Mayan"
)
release = '0.40.2'

# -- General configuration ---------------------------------------------------
extensions = [
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinx_copybutton',
    'sphinx_design',
    'sphinxcontrib.mermaid',
    "features_extension",
    "fmp_extension",
    "changelog_extension",
    'sphinx_click',
]

add_module_names = False
autosummary_generate = True
napoleon_numpy_docstring = True
napoleon_google_docstring = False

myst_enable_extensions = [
    'amsmath',
    'attrs_inline',
    'deflist',
    'dollarmath',
    'html_admonition',
    'html_image',
    'linkify',
    'strikethrough',
    'substitution',
    'tasklist',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
html_theme = 'pydata_sphinx_theme'

html_theme_options = {
    "logo": {
        "image_light": "_static/Imagotipo_Kaxanuk.png",
        "image_dark": "_static/Imagotipo_Kaxanuk_dark.png",
    },
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/KaxaNuk/Data-Curator",
            "icon": "fa-brands fa-square-github",
        },
    ],
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
}

html_static_path = ['_static']

mermaid_output_format = 'svg'

html_context = {
    "github_user": "tu-usuario",
    "github_repo": "tu-repo",
    "github_version": "main",
    "doc_path": "docs/source",
}

# -- Custom CSS and JS --------------------------------------------------------
def setup(app):
    app.add_css_file('sidebar.css')
    app.add_css_file('general.css')
    app.add_css_file('content.css')
