# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here.
import pathlib
import sys
sys.path.insert(0, pathlib.Path(__file__).parents[2].resolve().as_posix()+"/pandlas/")

import os
# Set the PYTHONNET_RUNTIME environment variable
os.environ['PYTHONNET_RUNTIME'] = 'coreclr'

# Set the PYTHONNET_CORECLR_RUNTIME_CONFIG environment variable
os.environ['PYTHONNET_CORECLR_RUNTIME_CONFIG'] = r'C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.Atlas.Host.runtimeconfig.json'

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Pandlas'
copyright = '2024, Owen Foo'
author = 'Owen Foo'
release = '0.2.1.a4'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
]

autosummary_generate = True
templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']


# useful article to generate class methods
# https://stackoverflow.com/questions/2701998/automatically-document-all-modules-recursively-with-sphinx-autodoc/62613202#62613202
