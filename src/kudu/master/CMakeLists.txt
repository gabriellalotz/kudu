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

KRPC_GENERATE(
  MASTER_KRPC_SRCS MASTER_KRPC_HDRS MASTER_KRPC_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES master.proto)
set(MASTER_KRPC_LIBS
  consensus_metadata_proto
  krpc
  protobuf
  rpc_header_proto
  tablet_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(master_proto
  SRCS ${MASTER_KRPC_SRCS}
  DEPS ${MASTER_KRPC_LIBS}
  NONLINK_DEPS ${MASTER_KRPC_TGTS})

KRPC_GENERATE(
  TXN_MANAGER_KRPC_SRCS TXN_MANAGER_KRPC_HDRS TXN_MANAGER_KRPC_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES txn_manager.proto)
set(TXN_MANAGER_KRPC_LIBS
  consensus_metadata_proto
  krpc
  protobuf
  transactions_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(txn_manager_proto
  SRCS ${TXN_MANAGER_KRPC_SRCS}
  DEPS ${TXN_MANAGER_KRPC_LIBS}
  NONLINK_DEPS ${TXN_MANAGER_KRPC_TGTS})

set(MASTER_SRCS
  authz_provider.cc
  auto_rebalancer.cc
  auto_leader_rebalancer.cc
  catalog_manager.cc
  hms_notification_log_listener.cc
  location_cache.cc
  master.cc
  master_cert_authority.cc
  master_options.cc
  master_path_handlers.cc
  master_runner.cc
  master_service.cc
  placement_policy.cc
  ranger_authz_provider.cc
  rest_catalog_path_handlers.cc
  sys_catalog.cc
  table_locations_cache.cc
  table_locations_cache_metrics.cc
  table_metrics.cc
  tablet_loader.cc
  ts_descriptor.cc
  ts_manager.cc
  txn_manager.cc
  txn_manager_service.cc)

# The mini_master is used only for tests.
if (KUDU_CLI_TEST_TOOL_ENABLED)
  set(MASTER_SRCS ${MASTER_SRCS} mini_master.cc)
endif()

add_library(master ${MASTER_SRCS})
target_link_libraries(master
  clock
  gutil
  krpc
  kserver
  kudu_common
  kudu_hms
  kudu_ranger
  kudu_subprocess
  kudu_thrift
  kudu_util
  master_proto
  rebalance
  rpc_header_proto
  security
  server_process
  tablet
  token_proto
  txn_manager_proto
  transactions
  tserver)

#########################################
# kudu-master
#########################################

add_executable(kudu-master master_main.cc)
target_link_libraries(kudu-master
  ${SANITIZER_OPTIONS_OVERRIDE}
  ${KRB5_REALM_OVERRIDE}
  master
  ${KUDU_BASE_LIBS})

option(KUDU_MASTER_INSTALL "Whether to install the Kudu Master executable" ON)
if(KUDU_MASTER_INSTALL)
  install(TARGETS kudu-master RUNTIME DESTINATION ${CMAKE_INSTALL_SBINDIR})
else()
  message(STATUS "Skipping install rule for the Kudu Master executable")
endif()

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(
  kudu_client
  kudu_curl_util
  itest_util
  master
  master_proto
  mini_cluster
  mini_hms
  mini_kdc)

ADD_KUDU_TEST(auto_rebalancer-test)
ADD_KUDU_TEST(auto_leader_rebalancer-test)
ADD_KUDU_TEST(catalog_manager-test)
ADD_KUDU_TEST(dynamic_multi_master-test NUM_SHARDS 6)
ADD_KUDU_TEST(hms_notification_log_listener-test)
ADD_KUDU_TEST(location_cache-test DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(master_options-test)
ADD_KUDU_TEST(master-test RESOURCE_LOCK "master-web-port"
                          DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(mini_master-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(placement_policy-test)
ADD_KUDU_TEST(rest_catalog-test)
ADD_KUDU_TEST(spnego_rest_catalog-test)
ADD_KUDU_TEST(sys_catalog-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(ts_descriptor-test DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(ts_state-test)
ADD_KUDU_TEST(txn_manager-test RESOURCE_LOCK "master-web-port")
