# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
sys.path.insert(0, os.path.abspath('../../acs_sdk_python'))  # Add your project root
import sphinx_rtd_theme  # Added import for the theme

project = 'Python SDK'
copyright = '2025, Accelerated Cloud Storage'
author = 'Accelerated Cloud Storage'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # For Google-style docstrings
]

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'exclude-members': 'CHUNK_SIZE, COMPRESSION_THRESHOLD, SERVER_ADDRESS',
}

html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'searchbox.html'
    ],
}

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'  # Use Read the Docs theme
html_static_path = ['_static']
