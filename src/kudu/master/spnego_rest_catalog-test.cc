// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::KuduPartialRow;
using kudu::Status;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

class SpnegoRestCatalogTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    const vector<string> kExtraFlags = {
      "--enable_rest_api"
    };

    cluster_opts_.enable_kerberos = true;
    cluster_opts_.extra_master_flags = kExtraFlags;
  }

  Status StartCluster() {
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    return cluster_->Start();
  }

  Status CreateTestTable() {
    string kTableName = "test_table";
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    KUDU_CHECK_OK(b.Build(&schema));
    vector<string> columnNames;
    columnNames.emplace_back("key");

    // Set the schema and range partition columns.
    KuduTableCreator* tableCreator = client_->NewTableCreator();
    tableCreator->table_name(kTableName).schema(&schema).set_range_partition_columns(columnNames);

    // Generate and add the range partition splits for the table.
    int32_t increment = 1000 / 10;
    for (int32_t i = 1; i < 10; i++) {
      KuduPartialRow* row = schema.NewRow();
      KUDU_CHECK_OK(row->SetInt32(0, i * increment));
      tableCreator->add_range_partition_split(row);
    }
    tableCreator->num_replicas(1);
    Status s = tableCreator->Create();
    delete tableCreator;
    return s;
  }

  string GetTableId(const string& table_name) {
    shared_ptr<KuduTable> table;
    Status s = client_->OpenTable(table_name, &table);
    if (!s.ok()) {
      LOG(ERROR) << "OpenTable failed: " << s.ToString();
      return "";
    }
    if (table->name() == table_name) {
      return table->id();
    }
    return "";
  }

 protected:
    unique_ptr<ExternalMiniCluster> cluster_;
    shared_ptr<KuduClient> client_;
    ExternalMiniClusterOptions cluster_opts_;
};

TEST_F(SpnegoRestCatalogTest, TestGetTablesOneTable) {
  ASSERT_OK(StartCluster());
  ASSERT_OK(cluster_->kdc()->Kinit("test-admin"));
  cluster_->CreateClient(nullptr, &client_);
  ASSERT_OK(CreateTestTable());
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->master(0)->bound_http_hostport().ToString()),
      &buf));
  string table_id = GetTableId("test_table");
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"tables\":[{\"table_id\":\"$0\",\"table_name\":\"test_table\"}]}", table_id));
}

} // namespace master
} // namespace kudu
