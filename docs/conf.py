# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import pathlib
import sys

import sphinx_rtd_theme

sys.path.insert(0, os.path.abspath("../src"))


project = "enlyze"
copyright = "2025, ENLYZE GmbH"
author = "ENLYZE GmbH"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_tabs.tabs",
    "sphinxcontrib.spelling",
    "sphinx.ext.viewcode",
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", ".ipynb_checkpoints"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_static_path = ["_static"]

html_theme = "sphinx_rtd_theme"

nitpicky = True
nitpick_ignore_regex = [
    ("py:class", r".*\.T"),
    ("py:.*", r"httpx\..*"),
]
nitpick_ignore = [
    ("py:class", "ComputedFieldInfo"),
]

autodoc_default_options = {"exclude-members": "__weakref__, __init__, __new__"}
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented_params"
autodoc_typehints_format = "short"

rst_epilog = pathlib.Path("substitutions.txt").read_text()

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
}
