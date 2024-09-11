from kudu.compat import CompatUnitTest
from common import KuduTestBase
import json

class TestEndpointsNegative(KuduTestBase, CompatUnitTest):
    def setUp(self):
        pass
    
    def test_get_table_not_found(self):
        response = self.rest_api_client.get('/api/v1/tables/non-existent-table')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Table does not exist.'})
        
    def test_post_table_already_exists(self):
        with open('example/example-table.json') as f:
            table = json.load(f)
        response = self.rest_api_client.post('/api/v1/tables', json=table)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Table already exists.'})