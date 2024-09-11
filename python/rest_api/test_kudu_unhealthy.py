from kudu.compat import CompatUnitTest
from fastapi.testclient import TestClient
from main import app
import json

class TestKuduUnhealthy(CompatUnitTest):
    def setUp(self):
        self.rest_api_client = TestClient(app)
        
    def test_get_tables(self):
        response = self.rest_api_client.get('/api/v1/tables')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Kudu is not available.'})
        
    def test_get_table(self):
        response = self.rest_api_client.get('/api/v1/tables/example-table')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Kudu is not available.'})
        
    def test_get_tablet_servers(self):
        response = self.rest_api_client.get('/api/v1/tabletServers')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Kudu is not available.'})
        
    def test_put_table(self):
        with open('example/put-example-table.json') as f:
            table = json.load(f)
        response = self.rest_api_client.put('/api/v1/tables/put-example-table', json=table)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Kudu is not available.'})
        
    def test_post_table(self):
        with open('example/example-table.json') as f:
            table = json.load(f)
        response = self.rest_api_client.post('/api/v1/tables', json=table)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Kudu is not available.'})
        
    def test_delete_table(self):
        response = self.rest_api_client.delete('/api/v1/tables/example-table')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), {'detail': 'Kudu is not available.'})
        