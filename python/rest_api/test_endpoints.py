from kudu.compat import CompatUnitTest
from .common import KuduTestBase

class TestEndpoints(KuduTestBase, CompatUnitTest):
    def setUp(self):
        pass
    
    def test_get_tables(self):
        expected_tables = {'tables': ['example-table']}
        response = self.rest_api_client.get('/api/v1/tables')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), expected_tables)