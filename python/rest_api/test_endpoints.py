from kudu.compat import CompatUnitTest
from common import KuduTestBase


class TestEndpoints(KuduTestBase, CompatUnitTest):
    def setUp(self):
        pass

    def test_get_tables(self):
        expected_tables = {'tables': ['example-table']}
        response = self.rest_api_client.get('/api/v1/tables')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), expected_tables)

    def test_get_table(self):
        expected_table = {'name': 'example-table', 'table_schema': 
            {'columns': [{'name': 'key', 'type ': 'int32', 'nullable': False, 'primary_key': True, 'precision': None, 'scale': None, 'length': None, 'comment': 'key_comment'},
                         {'name': 'int_val', 'type ': 'int32', 'nullable': True, 'primary_key': False, 'precision': None, 'scale': None, 'length': None, 'comment': ''}, 
                         {'name': 'string_val', 'type ': 'string', 'nullable': True, 'primary_key': False, 'precision': None, 'scale': None, 'length': None, 'comment': ''}, 
                         {'name': 'unixtime_micros_val', 'type ': 'unixtime_micros', 'nullable': True, 'primary_key': False, 'precision': None, 'scale': None, 'length': None, 'comment': ''}]}}
        response = self.rest_api_client.get('/api/v1/tables/example-table')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), expected_table)