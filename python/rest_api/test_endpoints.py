from kudu.compat import CompatUnitTest
from common import KuduTestBase
import json


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
        with open('example/example-table.json') as f:
            expected_table = json.load(f)

        response = self.rest_api_client.get('/api/v1/tables/example-table')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assert_table_object(response.json(), expected_table)

    def test_post_table(self):
        with open('example/post-example-table.json') as f:
            table = json.load(f)
        response = self.rest_api_client.post('/api/v1/tables', json=table)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        # self.assert_table_object(response.json(), table)
        response = self.rest_api_client.delete('/api/v1/tables/post-example-table')

    def test_delete_table(self):
        with open('example/post-example-table.json') as f:
            table = json.load(f)
        response = self.rest_api_client.post('/api/v1/tables', json=table)
        response = self.rest_api_client.delete('/api/v1/tables/post-example-table')
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
