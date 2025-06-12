#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/rest_catalog_test_base.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/curl_util.h"  // EasyCurl
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/client/client.h"
#include "kudu/util/test_util.h"  // For ASSERT_EVENTUALLY
#include "kudu/gutil/macros.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/rpc/rpc_controller.h"
#include <gtest/gtest.h>
#include <memory>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/faststring.h"
#include "kudu/master/master.proxy.h"  // For MasterServiceProxy

using kudu::EasyCurl;
using kudu::faststring;
using kudu::HostPort;
using kudu::MonoDelta;
using kudu::Status;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::master::MasterServiceProxy;
using kudu::master::RefreshAuthzCacheRequestPB;
using kudu::master::RefreshAuthzCacheResponsePB;
using kudu::ranger::ActionPB;
using kudu::ranger::AuthorizationPolicy;
using kudu::ranger::MiniRanger;
using kudu::ranger::PolicyItem;
using kudu::rpc::RpcController;
using kudu::rpc::UserCredentials;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace authz_util {

void RefreshAuthzPolicies(const std::unique_ptr<ExternalMiniCluster>& cluster) {
  RefreshAuthzCacheRequestPB req;
  RefreshAuthzCacheResponsePB resp;

  kudu::AssertEventually([&] {  // Explicitly qualify AssertEventually with the kudu namespace
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cluster->master_proxy()->RefreshAuthzCache(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
  });
}

void GrantCreateTablePrivilege(const string& db_name,
                               const string& user_name,
                               const unique_ptr<ExternalMiniCluster>& cluster) {
  // Ensure MiniRanger is not null and is started before adding a policy.
  auto* ranger = cluster->ranger();
  ASSERT_NE(ranger, nullptr)
      << "MiniRanger instance is null. Ensure the cluster is properly initialized.";
  ASSERT_OK(ranger->Start());

  AuthorizationPolicy policy;
  policy.databases.emplace_back(db_name);
  policy.tables.emplace_back("*");
  policy.items.emplace_back(PolicyItem({user_name}, {ActionPB::CREATE}, false));
  ASSERT_OK(ranger->AddPolicy(std::move(policy)));

  RefreshAuthzPolicies(cluster);
}

void GrantDropTablePrivilege(const string& db_name,
                             const string& table_name,
                             const string& user_name,
                             const unique_ptr<ExternalMiniCluster>& cluster) {
  AuthorizationPolicy policy;
  policy.databases.emplace_back(db_name);
  policy.tables.emplace_back(table_name);
  policy.items.emplace_back(PolicyItem({user_name}, {ActionPB::DROP}, false));
  ASSERT_OK(cluster->ranger()->AddPolicy(std::move(policy)));
  RefreshAuthzPolicies(cluster);
}

// Add other Grant*Privilege methods as needed...

}  // namespace authz_util

namespace kudu {
namespace master {

class SpnegoWebUiITest : public RestCatalogTestBase {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    opts.enable_kerberos = true;
    opts.extra_master_flags.push_back("--enable_rest_api");
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(KuduClientBuilder()
                  .add_master_server_addr(cluster_->master()->bound_rpc_addr().ToString())
                  .Build(&client_));
  }

 protected:
  std::unique_ptr<ExternalMiniCluster> cluster_;
};

TEST_F(SpnegoWebUiITest, TestTableIsolationBetweenUsersWithRestApi) {
  // Create table with User A, verify isolation: User A creates a new table.
  // Verify that User B cannot access it using PUT or DELETE endpoints.
  ASSERT_OK(CreateTestTable("test-admin"));

  // Grant privileges to test-admin for creating tables.
  authz_util::GrantCreateTablePrivilege("default", "test-admin", cluster_);

  ASSERT_OK(cluster_->kdc()->Kinit("joe-interloper"));
  string table_id;
  ASSERT_OK(GetTableId("test_table", &table_id));
  EasyCurl c;
  c.set_verbose(true);
  faststring buf;
  c.set_auth(CurlAuthType::SPNEGO);
  c.set_custom_method("DELETE");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->master()->bound_http_hostport().ToString(),
                                   table_id),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 403");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Not authorized: Unauthorized action\"}");
  c.set_custom_method("PUT");
  s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                             cluster_->master()->bound_http_hostport().ToString(),
                             table_id),
                  R"({
                      "table": {
                        "table_name": "test_table"
                      },
                      "alter_schema_steps": [
                        {
                          "type": "ADD_COLUMN",
                          "add_column": {
                            "schema": {
                              "name": "new_column",
                              "type": "STRING",
                              "is_nullable": true
                            }
                          }
                        }
                      ]
                    }
                    )",
                  &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 403");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Not authorized: Unauthorized action\"}");
}

TEST_F(SpnegoWebUiITest, TestListTablesIsolationBetweenUsersWithRestApi) {
  // User A and User B create separate tables. When performing
  // a GET /api/v1/tables, each user should only see the tables
  // they created
  ASSERT_OK(cluster_->kdc()->Kinit("test-user"));
  authz_util::GrantCreateTablePrivilege("default", "test-user", cluster_);

  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  c.set_auth(CurlAuthType::SPNEGO);
  c.set_verbose(true);
  ASSERT_OK(c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->master()->bound_http_hostport().ToString()),
      R"({
        "name": "test_table",
        "schema": {
          "columns": [
            {"name": "key", "type": "INT32", "is_nullable": false, "is_key": true},
            {"name": "int_val", "type": "INT32", "is_nullable": false, "is_key": false}
          ]
        },
        "partition_schema": {
          "range_schema": {
            "columns": [{"name": "key"}]
          }
        },
        "num_replicas": 1
      })",
      &buf));
  c.set_custom_method("GET");
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->master()->bound_http_hostport().ToString()),
      &buf));
  string table_id;
  ASSERT_OK(GetTableId("test_table", &table_id));
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"tables\":[{\"table_id\":\"$0\",\"table_name\":\"test_table\"}]}", table_id));

  ASSERT_OK(cluster_->kdc()->Kinit("joe-interloper"));
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->master()->bound_http_hostport().ToString()),
      &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"tables\":[]}");
}

}  // namespace master
}  // namespace kudu