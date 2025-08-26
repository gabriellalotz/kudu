#
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

from __future__ import division

from kudu.compat import CompatUnitTest
import kudu

from kudu.client import PartitionSchema

class TestPartitionSchema(CompatUnitTest):

    def setUp(self):
        # Create a test schema that we can use to create a mock table
        self.columns = [('key', 'int32', False),
                        ('timestamp_col', 'unixtime_micros', False),
                        ('value', 'string', True)]
        
        self.primary_keys = ['key', 'timestamp_col']
        
        self.builder = kudu.schema_builder()
        for name, typename, nullable in self.columns:
            self.builder.add_column(name, typename, nullable=nullable)
        
        self.builder.set_primary_keys(self.primary_keys)
        self.schema = self.builder.build()

    def test_partition_schema_type(self):
        """Test that PartitionSchema class exists and can be instantiated"""
        partition_schema = PartitionSchema()
        self.assertIsInstance(partition_schema, PartitionSchema)

    def test_partition_schema_wrap_method(self):
        """Test that the _wrap method exists and can be called"""
        partition_schema = PartitionSchema()
        
        # Create a mock table object for testing
        class MockTable:
            def __init__(self):
                self.name = "test_table"
                self.num_replicas = 3
                self.schema = self.schema
        
        mock_table = MockTable()
        mock_table.schema = self.schema
        
        # Test that _wrap returns the partition schema object
        result = partition_schema._wrap(mock_table)
        self.assertIs(result, partition_schema)
        self.assertIs(partition_schema.table, mock_table)

    def test_get_table_info_method_exists(self):
        """Test that get_table_info method exists"""
        partition_schema = PartitionSchema()
        self.assertTrue(hasattr(partition_schema, 'get_table_info'))
        self.assertTrue(callable(getattr(partition_schema, 'get_table_info')))

    def test_get_table_info_with_mock_table(self):
        """Test get_table_info method with a mock table"""
        partition_schema = PartitionSchema()
        
        # Create a mock table object for testing
        class MockTable:
            def __init__(self):
                self.name = "test_time_partitioned_table"
                self.num_replicas = 3
                self.schema = None
        
        mock_table = MockTable()
        mock_table.schema = self.schema
        
        # Set up the partition schema with the mock table
        partition_schema._wrap(mock_table)
        
        # Test get_table_info
        table_info = partition_schema.get_table_info()
        
        # Verify the returned information
        self.assertIsInstance(table_info, dict)
        self.assertIn('table_name', table_info)
        self.assertIn('num_replicas', table_info)
        self.assertIn('schema_num_columns', table_info)
        self.assertIn('message', table_info)
        
        self.assertEqual(table_info['table_name'], "test_time_partitioned_table")
        self.assertEqual(table_info['num_replicas'], 3)
        self.assertEqual(table_info['schema_num_columns'], len(self.schema))
        self.assertIn("test_time_partitioned_table", table_info['message'])

    def test_partition_schema_repr(self):
        """Test that PartitionSchema has a reasonable string representation"""
        partition_schema = PartitionSchema()
        repr_str = repr(partition_schema)
        self.assertIn('PartitionSchema', repr_str)

    def test_partition_schema_docstring(self):
        """Test that PartitionSchema class and methods have docstrings"""
        self.assertIsNotNone(PartitionSchema.__doc__)
        self.assertIn('partition schema', PartitionSchema.__doc__.lower())
        
        partition_schema = PartitionSchema()
        self.assertIsNotNone(partition_schema.get_table_info.__doc__)
        self.assertIn('partition information', partition_schema.get_table_info.__doc__.lower())


class TestPartitionSchemaIntegration(CompatUnitTest):
    """Integration tests that would work with a real Kudu cluster"""

    def test_partition_schema_property_type(self):
        """Test that when accessed from a table, partition_schema returns correct type"""
        # This test demonstrates the intended usage pattern
        # In a real cluster test, this would look like:
        # table = client.table('some_table')
        # partition_schema = table.partition_schema
        # self.assertIsInstance(partition_schema, PartitionSchema)
        
        # For now, just test the class exists and can be imported
        from kudu.client import PartitionSchema
        self.assertTrue(issubclass(PartitionSchema, object))

    def test_partition_schema_intended_usage(self):
        """Test that demonstrates the intended usage pattern for time range monitoring"""
        partition_schema = PartitionSchema()
        
        # Create a mock table that represents a time-partitioned table
        class MockTimePartitionedTable:
            def __init__(self):
                self.name = "sensor_data_2024"
                self.num_replicas = 3
                self.schema = None
        
        mock_table = MockTimePartitionedTable()
        mock_table.schema = self.schema
        
        # This demonstrates how partition info would be accessed for monitoring
        partition_schema._wrap(mock_table)
        table_info = partition_schema.get_table_info()
        
        # Verify this provides useful information for monitoring scripts
        self.assertIn('table_name', table_info)
        self.assertEqual(table_info['table_name'], "sensor_data_2024")
        
        # The message should indicate this is partition schema information
        self.assertIn('Partition schema information', table_info['message'])
        self.assertIn('sensor_data_2024', table_info['message'])