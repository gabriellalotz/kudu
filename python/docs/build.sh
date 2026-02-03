#!/bin/bash
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

# Production build script for Kudu Python Client API documentation
# This script can be called directly; make_site.sh may invoke it.

set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PYTHON_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

echo "=== Building Kudu Python Client API Documentation ==="
echo ""

# Step 1: Check if kudu module is built
echo "Checking if kudu module is built..."
cd "$PYTHON_DIR"
if ! python3 -c "import kudu.client" 2>/dev/null; then
    echo "Building Python module..."
    python3 setup.py build_ext --inplace
fi
echo "✓ Kudu module ready"
echo ""

# Step 2: Ensure Sphinx is installed
echo "Checking for Sphinx..."
if ! python3 -c "import sphinx" 2>/dev/null; then
    if [ -n "$KUDU_DOCS_ALLOW_PIP" ]; then
        echo "Installing Sphinx (KUDU_DOCS_ALLOW_PIP=1)..."
        pip3 install --user sphinx
    else
        echo "Sphinx not found. Install it or set KUDU_DOCS_ALLOW_PIP=1"
        echo "to allow auto-install into your user environment."
        exit 1
    fi
fi
echo "✓ Sphinx ready"
echo ""

# Step 3: Build the docs
echo "Building documentation..."
cd "$SCRIPT_DIR"
export PATH="$HOME/.local/bin:$PATH"
rm -rf _build
sphinx-build -b html . _build/html

echo ""
echo "=== Documentation Build Complete! ==="
echo ""
echo "Output: $SCRIPT_DIR/_build/html/"
echo ""
