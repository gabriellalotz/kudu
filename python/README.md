<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License. -->

# kudu-python: Python interface to the Apache Kudu C++ Client API

Using this package requires that you install the Kudu C++ client libraries and
headers. See https://kudu.apache.org for more.

**Note: It is recommended to use a Python virtual environment to avoid conflicts with system packages.**

## Setting up a Virtual Environment
```bash
# Create a virtual environment
# You can use any Python version supported by kudu-python (check https://pypi.org/project/kudu-python/)
virtualenv venv -p 3.8

# Activate the virtual environment
source venv/bin/activate

# To deactivate later:
deactivate
```

## Installing from PyPI

```bash
pip install kudu-python
```

## Installing from Source

**Note: Make sure you are in the `kudu/python` directory where the requirements files are located.**

```bash
cd /path/to/kudu/python  # Navigate to the python directory if not already there
pip install -r requirements.txt
python setup.py sdist
pip install dist/kudu-python-*.tar.gz
```

## Building for Development

### Setting up the KUDU_HOME Environment Variable

Before building for development, you need to set the `KUDU_HOME` environment variable to point to the root directory of your Kudu git repository:

```bash
export KUDU_HOME=/path/to/kudu
```

This variable is required by various scripts and tools in the project. Make sure it's set in your environment before running any Kudu-related commands.


**Note: Make sure you are in the `kudu/python` directory where the requirements files are located.**

```bash
cd /path/to/kudu/python  # Navigate to the python directory if not already there
pip install -r requirements.txt
pip install -r requirements_dev.txt
python setup.py build_ext
```

## Run All Tests

```bash
python setup.py test
```

## Run Single Test

```bash
python -m unittest kudu.tests.test_client.TestClient.test_list_tables
```
