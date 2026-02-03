.. Licensed to the Apache Software Foundation (ASF) under one
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
   under the License.

Kudu Python Client API Documentation
=====================================

The Kudu Python client provides a programmatic interface to interact with Apache Kudu,
a columnar storage engine designed for fast analytics on fast data.

Installation
============

Install the Kudu Python client from PyPI::

    pip install kudu-python

Or build from source::

    cd $KUDU_HOME/python
    python setup.py build_ext --inplace

Quick Start
===========

Connect to a Kudu cluster::

    import kudu
    client = kudu.connect(host='localhost', port=7051)

API Documentation
=================

Core Module
-----------

.. automodule:: kudu
   :members: connect, timedelta, schema_builder
   :undoc-members:

Client and Table Operations
---------------------------

.. autoclass:: kudu.Client
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.Table
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.Scanner
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.ScanTokenBuilder
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.ScanToken
   :members:
   :undoc-members:
   :show-inheritance:

Data Modification
-----------------

.. autoclass:: kudu.client.Session
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.Insert
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.Update
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.Upsert
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: kudu.client.Delete
   :members:
   :undoc-members:
   :show-inheritance:

Schema and Types
----------------

.. automodule:: kudu.schema
   :members:
   :undoc-members:
   :show-inheritance:

Errors
------

.. automodule:: kudu.errors
   :members:
   :undoc-members:
   :show-inheritance:

Examples
========

Insert, update, and delete operations::

    table = client.table('my_table')
    session = client.new_session()

    op = table.new_insert()
    op['key'] = 1
    op['name'] = 'Alice'
    op['value'] = 100
    session.apply(op)

    op = table.new_update()
    op['key'] = 1
    op['value'] = 200
    session.apply(op)

    op = table.new_delete()
    op['key'] = 1
    session.apply(op)

    session.flush()

Scanning with predicates::

    scanner = table.scanner()
    scanner.add_predicate(table['value'] >= 100)
    scanner.add_predicate(table['name'] == 'Alice')

    for row in scanner:
        print(row['key'], row['name'], row['value'])

Indices and Search
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
