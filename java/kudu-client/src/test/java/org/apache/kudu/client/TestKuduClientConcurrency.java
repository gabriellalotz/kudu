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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.Schema;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;

/**
 * Concurrency and reliability tests for KuduClient including concurrent operations,
 * read-your-writes consistency, and close behavior.
 */
public class TestKuduClientConcurrency {
  private static final String TABLE_NAME = "TestKuduClientConcurrency";

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  /**
   * Creates a local client that we auto-close while buffering one row, then makes sure that after
   * closing that we can read the row.
   */
  @Test(timeout = 100000)
  public void testAutoClose() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);

    // Insert a row using an auto-closeable client.
    try (KuduClient localClient = new KuduClient.KuduClientBuilder(
        harness.getMasterAddressesAsString()).build()) {

      KuduTable localTable = localClient.openTable(TABLE_NAME);
      KuduSession session = localClient.newSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      session.apply(createBasicSchemaInsert(localTable, 0));
      // The session should be auto-closed which will flush the row.
    }

    // Now verify we can read it back.
    KuduScanner scanner = client.newScannerBuilder(table).build();
    int rowCount = 0;
    while (scanner.hasMoreRows()) {
      rowCount += scanner.nextRows().getNumRows();
    }
    assertEquals(1, rowCount);
  }

  /**
   * Regression test for some log spew which occurred in short-lived client instances which
   * had outbound connections.
   */
  @Test(timeout = 100000)
  public void testCloseShortlyAfterOpen() throws Exception {
    try (KuduClient localClient = new KuduClient.KuduClientBuilder(
        harness.getMasterAddressesAsString()).build()) {
      // Just open and close quickly.
    }
    // Should not have any issues or log spew.
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentInsert() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());

    final KuduTable table = client.openTable(TABLE_NAME);
    final int numThreads = 4;
    final int insertsPerThread = 25;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);

    List<Future<Void>> futures = new ArrayList<>();

    // Create concurrent insert tasks
    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      Future<Void> future = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          startLatch.await();

          KuduSession session = client.newSession();
          session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

          for (int j = 0; j < insertsPerThread; j++) {
            int key = threadId * insertsPerThread + j;
            session.apply(createBasicSchemaInsert(table, key));
          }
          return null;
        }
      });
      futures.add(future);
    }

    // Start all threads
    startLatch.countDown();

    // Wait for all to complete
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();

    // Verify all rows were inserted
    KuduScanner scanner = client.newScannerBuilder(table).build();
    int rowCount = 0;
    while (scanner.hasMoreRows()) {
      rowCount += scanner.nextRows().getNumRows();
    }
    assertEquals(numThreads * insertsPerThread, rowCount);
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentAlter() throws Exception {
    // Kick off an asynchronous table creation.
    Deferred<KuduTable> d = asyncClient.createTable(TABLE_NAME,
        createManyStringsSchema(), getBasicCreateTableOptions());

    // Rename the table that's being created to make sure it doesn't interfere
    // with the "wait for all tablets to be created" behavior of createTable().
    //
    // We have to retry this in a loop because we might run before the table
    // actually exists.
    while (true) {
      try {
        client.alterTable(TABLE_NAME,
            new AlterTableOptions().renameTable("foo"));
        break;
      } catch (KuduException e) {
        if (!e.getStatus().isNotFound()) {
          throw e;
        }
      }
    }

    // If createTable() was disrupted by the alterTable(), this will throw.
    d.join();
  }

  // This is a test that verifies, when multiple clients run
  // simultaneously, a client can get read-your-writes and
  // read-your-reads session guarantees using READ_YOUR_WRITES
  // scan mode, from leader replica. In this test writes are
  // performed in AUTO_FLUSH_SYNC (single operation) flush modes.
  @Test(timeout = 100000)
  public void testReadYourWritesSyncLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
                   ReplicaSelection.LEADER_ONLY);
  }

  // Similar test as above but scan from the closest replica.
  @Test(timeout = 100000)
  public void testReadYourWritesSyncClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
                   ReplicaSelection.CLOSEST_REPLICA);
  }

  // Similar to testReadYourWritesSyncLeaderReplica, but in this
  // test writes are performed in MANUAL_FLUSH (batches) flush modes.
  @Test(timeout = 100000)
  public void testReadYourWritesBatchLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
                   ReplicaSelection.LEADER_ONLY);
  }

  // Similar test as above but scan from the closest replica.
  @Test(timeout = 100000)
  public void testReadYourWritesBatchClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
                   ReplicaSelection.CLOSEST_REPLICA);
  }

  private void readYourWrites(final SessionConfiguration.FlushMode flushMode,
                              final ReplicaSelection replicaSelection)
          throws Exception {
    final int NUM_ROWS_PER_THREAD = 100;
    final int NUM_THREADS = 4;

    client.createTable(TABLE_NAME, basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), NUM_THREADS)
                                .setNumReplicas(3));
    final KuduTable table = client.openTable(TABLE_NAME);

    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Void>> futures = new ArrayList<>();

    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      Future<Void> future = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try (KuduClient localClient = new KuduClient.KuduClientBuilder(
              harness.getMasterAddressesAsString()).build()) {

            KuduTable localTable = localClient.openTable(TABLE_NAME);
            KuduSession session = localClient.newSession();
            session.setFlushMode(flushMode);

            // Write rows
            for (int j = 0; j < NUM_ROWS_PER_THREAD; j++) {
              int key = threadNum * NUM_ROWS_PER_THREAD + j;
              session.apply(createBasicSchemaInsert(localTable, key));
            }

            if (flushMode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
              session.flush();
            }

            // Read back what we wrote using READ_YOUR_WRITES mode
            KuduScanner scanner = localClient.newScannerBuilder(localTable)
                .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                .replicaSelection(replicaSelection)
                .build();

            int rowCount = 0;
            while (scanner.hasMoreRows()) {
              rowCount += scanner.nextRows().getNumRows();
            }

            // We should be able to read at least what we wrote
            assertTrue("Should read at least " + NUM_ROWS_PER_THREAD + " rows, got " + rowCount,
                       rowCount >= NUM_ROWS_PER_THREAD);
          }
          return null;
        }
      });
      futures.add(future);
    }

    // Wait for all threads to complete
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();

    // Final verification - count total rows
    KuduScanner scanner = client.newScannerBuilder(table).build();
    int totalRows = 0;
    while (scanner.hasMoreRows()) {
      totalRows += scanner.nextRows().getNumRows();
    }
    assertEquals(NUM_THREADS * NUM_ROWS_PER_THREAD, totalRows);
  }

  /**
   * This is a test scenario to reproduce conditions described in KUDU-3277.
   * The scenario was failing before the fix:
   *   ** 'java.lang.AssertionError: This Deferred was already called' was
   *       encountered multiple times with the stack exactly as described in
   *       KUDU-3277
   *   ** some flusher threads were unable to join since KuduSession.flush()
   *      would hang (i.e. would not return)
   */
  @MasterServerConfig(flags = {
      "--table_locations_ttl_ms=500",
  })
  @Test(timeout = 100000)
  public void testConcurrentFlush() throws Exception {
    final int numThreads = 8;
    final int numRowsPerThread = 10;

    client.createTable(TABLE_NAME, basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 4)
                                .setNumReplicas(1));
    final KuduTable table = client.openTable(TABLE_NAME);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<Void>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final int threadNum = i;
      Future<Void> future = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          startLatch.await();

          KuduSession session = client.newSession();
          session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

          for (int j = 0; j < numRowsPerThread; j++) {
            int key = threadNum * numRowsPerThread + j;
            session.apply(createBasicSchemaInsert(table, key));
          }

          // Multiple threads flushing simultaneously
          session.flush();
          return null;
        }
      });
      futures.add(future);
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all to complete
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();

    // Verify all rows were inserted
    KuduScanner scanner = client.newScannerBuilder(table).build();
    int rowCount = 0;
    while (scanner.hasMoreRows()) {
      rowCount += scanner.nextRows().getNumRows();
    }
    assertEquals(numThreads * numRowsPerThread, rowCount);
  }

  @Test(timeout = 100000)
  public void testSessionOnceClosed() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = createBasicSchemaInsert(table, 0);
    session.apply(insert);
    session.close();
    assertTrue(session.isClosed());

    insert = createBasicSchemaInsert(table, 1);
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      session.apply(insert);
    }
    String loggedText = cla.getAppendedText();
    assertTrue("Missing warning:\n" + loggedText,
               loggedText.contains("this is unsafe"));
  }
}
