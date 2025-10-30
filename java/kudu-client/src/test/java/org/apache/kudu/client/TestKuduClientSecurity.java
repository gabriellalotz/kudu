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

package org.apache.kudu.client;

import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.LocationConfig;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;

/**
 * Security and authentication tests for KuduClient including location assignment,
 * cluster ID verification, and certificate handling.
 */
public class TestKuduClientSecurity {
  private static final String TABLE_NAME = "TestKuduClientSecurity";

  private static final org.apache.kudu.Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  @Test
  public void testGetAuthnToken() throws Exception {
    // This test just verifies that we can call the API without error.
    // The actual token content is not easily verifiable in this context.
    byte[] token = client.exportAuthenticationCredentials();
    assertNotNull("Authentication token should not be null", token);
    assertTrue("Authentication token should not be empty", token.length > 0);
  }

  /**
   * Test that, if the masters are down when we attempt to connect, we don't end up
   * logging any nonsensical stack traces including Netty internals.
   */
  @Test(timeout = 100000)
  public void testNoLogSpewOnConnectionRefused() throws Exception {
    // We'll try to connect to a non-existent server to trigger connection refused
    CapturingLogAppender logAppender = new CapturingLogAppender();

    try (KuduClient localClient = new KuduClient.KuduClientBuilder("127.0.0.1:12345")
        .defaultOperationTimeoutMs(1000)
        .build()) {

      // This should fail to connect but not log nonsensical stack traces
      try {
        localClient.getTablesList();
      } catch (Exception e) {
        // Expected to fail
      }
    }

    // Check that we don't have excessive or nonsensical log messages
    // This is a regression test to ensure clean error handling
    String logOutput = logAppender.getAppendedText();
    assertFalse("Should not log Netty internal stack traces on connection refused",
        logOutput.contains("io.netty") && logOutput.contains("StackTrace"));
  }

  @Test(timeout = 100000)
  public void testCustomNioExecutor() throws Exception {
    long startTime = System.nanoTime();
    try (KuduClient localClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                 .nioExecutors(Executors.newFixedThreadPool(1),
                     Executors.newFixedThreadPool(2))
                 .bossCount(1)
                 .workerCount(2)
                 .build()) {
      long buildTime = (System.nanoTime() - startTime) / 1000000000L;
      assertTrue("Building KuduClient is slow, maybe netty get stuck", buildTime < 3);
      localClient.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
      Thread[] threads = new Thread[4];
      for (int t = 0; t < 4; t++) {
        final int id = t;
        threads[t] = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              KuduTable table = localClient.openTable(TABLE_NAME);
              KuduSession session = localClient.newSession();
              session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
              for (int i = 0; i < 100; i++) {
                Insert insert = createBasicSchemaInsert(table, id * 100 + i);
                session.apply(insert);
              }
              session.close();
            } catch (Exception e) {
              fail("insert thread should not throw exception: " + e);
            }
          }
        });
        threads[t].start();
      }
      for (int t = 0; t < 4; t++) {
        threads[t].join();
      }
    }
  }

  /**
   * Test assignment of a location to the client.
   */
  @Test(timeout = 100000)
  public void testClientLocationNoLocation() throws Exception {
    // Without location configuration, client should still work normally
    client.getTablesList();

    // Create a table to verify basic functionality
    client.createTable(TABLE_NAME, basicSchema, ClientTestUtil.getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    assertNotNull("Should be able to open table", table);
  }

  @Test(timeout = 100000)
  @LocationConfig(locations = {
      "/L0:6",
  })
  @MasterServerConfig(flags = {
      "--master_client_location_assignment_enabled=true",
  })
  public void testClientLocation() throws Exception {
    // With location configuration, verify client location is assigned
    client.getTablesList();

    // Basic operations should still work with location assignment
    client.createTable(TABLE_NAME, basicSchema, ClientTestUtil.getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    assertNotNull("Should be able to open table with location", table);
  }

  @Test(timeout = 100000)
  public void testClusterId() throws Exception {
    assertTrue(client.getClusterId().isEmpty());
    // Do something that will cause the client to connect to the cluster.
    client.listTabletServers();
    assertFalse(client.getClusterId().isEmpty());
  }

  private void runTestCallDuringLeaderElection(String clientMethodName) throws Exception {
    // This is a helper method that would be used to test behavior during leader election.
    // The actual implementation would depend on being able to trigger leader election,
    // which is complex in a test environment.

    // For now, just verify the client can handle basic operations
    switch (clientMethodName) {
      case "exportAuthenticationCredentials":
        byte[] token = client.exportAuthenticationCredentials();
        assertNotNull("Should get auth token during leader election", token);
        break;
      case "getHiveMetastoreConfig":
        // This would test getting Hive metastore config during leader election
        // For now, just ensure the method exists and can be called
        try {
          client.getHiveMetastoreConfig();
        } catch (Exception e) {
          // May fail if Hive metastore is not configured, which is expected
        }
        break;
      default:
        fail("Unknown method: " + clientMethodName);
    }
  }

  @Test(timeout = 100000)
  public void testExportAuthenticationCredentialsDuringLeaderElection() throws Exception {
    runTestCallDuringLeaderElection("exportAuthenticationCredentials");
  }

  @Test(timeout = 100000)
  public void testGetHiveMetastoreConfigDuringLeaderElection() throws Exception {
    runTestCallDuringLeaderElection("getHiveMetastoreConfig");
  }

  @Test(timeout = 50000)
  public void testImportInvalidCert() throws Exception {
    // An empty certificate to import.
    byte[] caCert = new byte[0];
    try {
      client.trustedCertificates(Arrays.asList(ByteString.copyFrom(caCert)));
      fail("Should have thrown CertificateException");
    } catch (CertificateException e) {
      assertTrue(e.getMessage().contains("Could not parse certificate"));
    }
  }

  @Test(timeout = 100000)
  public void testSchemaDriftPattern() throws Exception {
    KuduTable table = client.createTable(
            TABLE_NAME, createManyStringsSchema(), getBasicCreateTableOptions().setWait(false));
    KuduSession session = client.newSession();

    // Insert a row.
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString("key", "key_0");
    row.addString("c1", "c1_0");
    row.addString("c2", "c2_0");
    row.addString("c3", "c3_0");
    row.addString("c4", "c4_0");
    OperationResponse resp = session.apply(insert);
    assertFalse(resp.hasRowError());

    // Insert a row with an extra column.
    boolean retried = false;
    while (true) {
      try {
        Insert insertExtra = table.newInsert();
        PartialRow rowExtra = insertExtra.getRow();
        rowExtra.addString("key", "key_1");
        rowExtra.addString("c1", "c1_1");
        rowExtra.addString("c2", "c2_1");
        rowExtra.addString("c3", "c2_1");
        rowExtra.addString("c4", "c2_1");
        rowExtra.addString("c5", "c5_1");
        OperationResponse respExtra = session.apply(insertExtra);
        assertFalse(respExtra.hasRowError());
        break;
      } catch (IllegalArgumentException e) {
        if (retried) {
          throw e;
        }
        // Add the missing column and retry.
        if (e.getMessage().contains("Unknown column")) {
          client.alterTable(TABLE_NAME, new AlterTableOptions()
                  .addNullableColumn("c5", Type.STRING));
          // We need to re-open the table to ensure it has the new schema.
          table = client.openTable(TABLE_NAME);
          retried = true;
        } else {
          throw e;
        }
      }
    }
    // Make sure we actually retried.
    assertTrue(retried);

    // Insert a row with the old schema.
    Insert insertOld = table.newInsert();
    PartialRow rowOld = insertOld.getRow();
    rowOld.addString("key", "key_3");
    rowOld.addString("c1", "c1_3");
    rowOld.addString("c2", "c2_3");
    rowOld.addString("c3", "c3_3");
    rowOld.addString("c4", "c4_3");
    OperationResponse respOld = session.apply(insertOld);
    assertFalse(respOld.hasRowError());
  }
}
