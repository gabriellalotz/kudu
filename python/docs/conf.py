# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Minimal Sphinx configuration for Kudu Python client API documentation POC.
"""

import os
import sys

# Add the parent directory to the path so Sphinx can find the kudu module
# This assumes the kudu module has been built in-place
sys.path.insert(0, os.path.abspath('..'))

# Project information
project = 'Kudu Python Client API'
copyright = '2026, The Apache Software Foundation'
author = 'Apache Kudu Developers'

# Try to get version from kudu
try:
    from kudu.version import version as kudu_version
    release = kudu_version
    version = '.'.join(kudu_version.split('.')[:2])
except ImportError:
    version = 'dev'
    release = 'dev'

# Sphinx extensions
extensions = [
    'sphinx.ext.autodoc',      # Auto-generate docs from docstrings
    'sphinx.ext.napoleon',     # Support for NumPy/Google style docstrings
    'sphinx.ext.viewcode',     # Add links to source code
    'sphinx.ext.intersphinx',  # Link to other project docs
]

# Autodoc settings
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'undoc-members': True,
    'show-inheritance': True,
}

# Napoleon settings for Google/NumPy style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

# Intersphinx mapping to link to Python docs
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
}

# HTML output settings
html_theme = 'alabaster'  # Simple built-in theme, no extra dependencies
html_static_path = []
html_title = f"{project} {version}"

# Output options
templates_path = []
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
