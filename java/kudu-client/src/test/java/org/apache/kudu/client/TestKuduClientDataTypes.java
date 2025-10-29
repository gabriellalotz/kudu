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

import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.createManyVarcharsSchema;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithBinaryColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDateColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDecimalColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithTimestampColumns;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.DecimalUtil;
import org.apache.kudu.util.TimestampUtil;

/**
 * Data type specific tests for KuduClient including strings, binary, timestamps,
 * dates, decimals, and other data types.
 */
public class TestKuduClientDataTypes {
  private static final String TABLE_NAME = "TestKuduClientDataTypes";

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  /**
   * Test creating a table with columns with different combinations of NOT NULL and
   * default values, inserting rows, and checking the results are as expected.
   * Regression test for KUDU-180.
   */
  @Test(timeout = 100000)
  public void testTableWithDefaults() throws Exception {
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
             .key(true)
             .build());
    // nullable with no default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING)
             .nullable(true)
             .build());
    // nullable with default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING)
             .nullable(true)
             .defaultValue("def")
             .build());
    // not null with no default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING)
             .nullable(false)
             .build());
    // not null with default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING)
             .nullable(false)
             .defaultValue("def")
             .build());
    Schema schema = new Schema(cols);
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    // Insert various rows. '-' indicates leaving the row unset in the insert.
    List<String> rows = ImmutableList.of(
        "r1,a,b,c,d",
        "r2,NULL,NULL,c,d",
        "r3,-,-,c,-",
        "fail_1,a,b,c,NULL",
        "fail_2,a,b,NULL,d");
    for (String row : rows) {
      String[] split = row.split(",");
      boolean shouldSucceed = !split[0].startsWith("fail");
      try {
        Insert insert = table.newInsert();
        PartialRow partialRow = insert.getRow();
        partialRow.addString("key", split[0]);
        for (int i = 1; i < split.length; i++) {
          String value = split[i];
          String colName = "c" + i;
          if (value.equals("-")) {
            // don't set anything
          } else if (value.equals("NULL")) {
            partialRow.setNull(colName);
          } else {
            partialRow.addString(colName, value);
          }
        }
        session.apply(insert);

        if (!shouldSucceed) {
          org.junit.Assert.fail(
              "Expected IllegalArgumentException for setting NULL on non-nullable column");
        }
      } catch (IllegalArgumentException e) {
        // Expected for fail_1 and fail_2
        assertTrue("Exception should mention cannot set to null",
                   e.getMessage().matches("c[34] cannot be set to null"));
      }
    }
    session.flush();

    // Check that we got the results we expected.
    List<String> expectedStrings = ImmutableList.of(
        "STRING key=r1, STRING c1=a, STRING c2=b, STRING c3=c, STRING c4=d",
        "STRING key=r2, STRING c1=NULL, STRING c2=NULL, STRING c3=c, STRING c4=d",
        "STRING key=r3, STRING c1=NULL, STRING c2=def, STRING c3=c, STRING c4=def");
    List<String> rowStrings = scanTableToStrings(table);
    Collections.sort(rowStrings);
    assertArrayEquals(rowStrings.toArray(new String[0]),
                      expectedStrings.toArray(new String[0]));
  }

  /**
   * Test inserting and retrieving VARCHAR columns.
   */
  @Test(timeout = 100000)
  public void testVarchars() throws Exception {
    Schema schema = createManyVarcharsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addVarchar("key", String.format("key_%02d", i));
      row.addVarchar("c1", "c1_" + i);
      row.addVarchar("c2", "c2_" + i);
      if (i % 2 == 1) {
        row.addVarchar("c3", "c3_" + i);
      } else {
        row.setNull("c3");
      }
      row.addVarchar("c4", "c4_" + i);
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    assertEquals(
        "VARCHAR key(10)=key_03, VARCHAR c1(10)=c1_3, VARCHAR c2(10)=c2_3," +
        " VARCHAR c3(10)=c3_3, VARCHAR c4(10)=c4_3", rowStrings.get(3));
    assertEquals(
        "VARCHAR key(10)=key_04, VARCHAR c1(10)=c1_4, VARCHAR c2(10)=c2_4," +
        " VARCHAR c3(10)=NULL, VARCHAR c4(10)=c4_4", rowStrings.get(4));

    KuduScanner scanner = client.newScannerBuilder(table).build();

    assertTrue("Scanner should have returned row", scanner.hasMoreRows());

    RowResultIterator rows = scanner.nextRows();
    final RowResult next = rows.next();

    // Do negative testing on string type.
    try {
      next.getString("key");
    } catch (IllegalArgumentException ignored) {
      // ignored.
    }
  }

  /**
   * Test inserting and retrieving string columns.
   */
  @Test(timeout = 100000)
  public void testStrings() throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c1", "c1_" + i);
      row.addString("c2", "c2_" + i);
      if (i % 2 == 1) {
        row.addString("c3", "c3_" + i);
      } else {
        row.setNull("c3");
      }
      row.addString("c4", "c4_" + i);
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    assertEquals(
        "STRING key=key_03, STRING c1=c1_3, STRING c2=c2_3, STRING c3=c3_3, STRING c4=c4_3",
        rowStrings.get(3));
    assertEquals(
        "STRING key=key_04, STRING c1=c1_4, STRING c2=c2_4, STRING c3=NULL, STRING c4=c4_4",
        rowStrings.get(4));

    KuduScanner scanner = client.newScannerBuilder(table).build();

    assertTrue("Scanner should have returned row", scanner.hasMoreRows());

    RowResultIterator rows = scanner.nextRows();
    final RowResult next = rows.next();

    // Do negative testing on string type.
    try {
      next.getVarchar("key");
    } catch (IllegalArgumentException ignored) {
      // ignored.
    }
  }

  /**
   * Test to verify that we can write in and read back UTF8.
   */
  @Test(timeout = 100000)
  public void testUTF8() throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduTable table = client.openTable(TABLE_NAME);
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString("key", "กขฃคฅฆง"); // some thai
    row.addString("c1", "✁✂✃✄✆"); // some icons

    row.addString("c2", "hello"); // some normal chars
    row.addString("c4", "🐱"); // supplemental plane
    KuduSession session = client.newSession();
    session.apply(insert);
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
        "STRING key=กขฃคฅฆง, STRING c1=✁✂✃✄✆, STRING c2=hello, STRING c3=NULL, STRING c4=🐱",
        rowStrings.get(0));
  }

  /**
   * Test inserting and retrieving binary columns.
   */
  @Test(timeout = 100000)
  public void testBinaryColumns() throws Exception {
    Schema schema = createSchemaWithBinaryColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    byte[] testArray = new byte[] {1, 2, 3, 4, 5, 6 ,7, 8, 9};

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      // Schema: key=BINARY, c1=STRING, c2=DOUBLE, c3=BINARY(nullable)
      row.addBinary("key",
          String.format("key_%02d", i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      row.addString("c1", "✁✂✃✄✆");
      row.addDouble("c2", i);
      if (i % 2 == 1) {
        row.addBinary("c3", testArray);
      } else {
        row.setNull("c3");
      }
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("BINARY key=\"key_%02d\"", i));
      expectedRow.append(", STRING c1=✁✂✃✄✆");
      expectedRow.append(String.format(", DOUBLE c2=%.1f", (double) i));
      if (i % 2 == 1) {
        expectedRow.append(", BINARY c3=");
        expectedRow.append(Bytes.pretty(testArray));
      } else {
        expectedRow.append(", BINARY c3=NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test inserting and retrieving timestamp columns.
   */
  @Test(timeout = 100000)
  public void testTimestampColumns() throws Exception {
    Schema schema = createSchemaWithTimestampColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    List<Long> timestamps = new ArrayList<>();

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    long lastTimestamp = 0;
    for (int i = 0; i < 100; i++) {
      long ts = System.currentTimeMillis() * 1000;
      if (ts <= lastTimestamp) {
        ts = lastTimestamp + 1;
      }
      lastTimestamp = ts;
      timestamps.add(ts);
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addLong("key", ts);  // key is UNIXTIME_MICROS type
      if (i % 2 == 1) {
        row.addLong("c1", ts);
      } else {
        row.setNull("c1");
      }
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append("UNIXTIME_MICROS key=")
          .append(TimestampUtil.timestampToString(timestamps.get(i)));
      if (i % 2 == 1) {
        expectedRow.append(", UNIXTIME_MICROS c1=")
            .append(TimestampUtil.timestampToString(timestamps.get(i)));
      } else {
        expectedRow.append(", UNIXTIME_MICROS c1=NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test inserting and retrieving date columns.
   */
  @Test(timeout = 100000)
  public void testDateColumns() throws Exception {
    Schema schema = createSchemaWithDateColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    List<Integer> dates = new ArrayList<>();

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      // Just use i as the date for simplicity.
      int dateValue = i;
      dates.add(dateValue);
      row.addDate("key", DateUtil.epochDaysToSqlDate(dateValue));  // key is DATE type
      if (i % 2 == 1) {
        row.addDate("c1", DateUtil.epochDaysToSqlDate(dateValue));
      } else {
        row.setNull("c1");
      }
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      String sdate = DateUtil.epochDaysToDateString(dates.get(i));
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append("DATE key=").append(sdate);
      if (i % 2 == 1) {
        expectedRow.append(", DATE c1=").append(sdate);
      } else {
        expectedRow.append(", DATE c1=NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test inserting and retrieving decimal columns.
   */
  @Test(timeout = 100000)
  public void testDecimalColumns() throws Exception {
    Schema schema = createSchemaWithDecimalColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    // Verify ColumnTypeAttributes
    assertEquals(DecimalUtil.MAX_DECIMAL128_PRECISION,
        table.getSchema().getColumn("c1").getTypeAttributes().getPrecision());

    for (int i = 0; i < 9; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addDecimal("key", BigDecimal.valueOf(i));  // key is DECIMAL type
      if (i % 2 == 1) {
        row.addDecimal("c1", BigDecimal.valueOf(i));
      } else {
        row.setNull("c1");
      }
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(9, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append("DECIMAL key(18, 0)=").append(i);
      if (i % 2 == 1) {
        expectedRow.append(", DECIMAL c1(38, 0)=").append(i);
      } else {
        expectedRow.append(", DECIMAL c1(38, 0)=NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }
}
