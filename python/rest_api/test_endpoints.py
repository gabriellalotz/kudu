from kudu.compat import CompatUnitTest
from kudu.tests.common import KuduTestBase

class TestEndpoints(KuduTestBase, CompatUnitTest):
    def setUp(self):
        pass
    
    def test_get_tables(self):
        expected_tables = ['table1', 'table2', 'table3']
        response = self.rest_api_client.get('/api/v1/tables')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.json(), expected_tables)