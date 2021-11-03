/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.datafusion.plugin.sap.table.source;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapColumn;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author sankalpbapat
 *
 */
public class SapTableToStructuredSchemaTransformerTest {

  private static final String FIELD_NAME = "fieldName";

  private static SapTableToStructuredSchemaTransformer schemaTransformer;

  private static List<Schema.Field> expectedSchemaFields;
  private static List<SapFieldMetadata> sapColumnList;
  private static SapObjectMetadata tableMetadata;

  private enum SapColMetadata {
    C_CHAR(FIELD_NAME + 1, 5, 0, "C", true),
    N_CHAR(FIELD_NAME + 2, 6, 0, "N", true),
    G_CHAR(FIELD_NAME + 3, 7, 0, "G", false),
    STRING_CHAR(FIELD_NAME + 4, 8, 0, "STRING", false),
    B_INT(FIELD_NAME + 5, 4, 0, "B", false),
    S_INT(FIELD_NAME + 6, 6, 0, "S", false),
    I_INT(FIELD_NAME + 7, 11, 0, "I", false),
    INT8_LONG(FIELD_NAME + 8, 20, 0, "8", false),
    F_DOUBLE(FIELD_NAME + 9, 13, 0, "F", false),
    X_BYTES(FIELD_NAME + 10, 14, 0, "X", false),
    Y_BYTES(FIELD_NAME + 11, 15, 0, "Y", false),
    XSTRING_BYTES(FIELD_NAME + 12, 16, 0, "XSTRING", false),
    P_DECIMAL(FIELD_NAME + 13, 6, 3, "P", false),
    A_DECIMAL("field/Name_14", 8, 5, "A", false),
    E_DECIMAL(FIELD_NAME + 15, 12, 8, "E", false),
    D_DATE(FIELD_NAME + 16, 8, 0, "D", false),
    T_TIME(FIELD_NAME + 17, 6, 0, "T", false),
    UTCL_TIMESTAMP(FIELD_NAME + 18, 27, 0, "UTCL", false);

    private String name;
    private int length;
    private int decimals;
    private String abapType;
    private boolean isKey;

    private SapColMetadata(String name, int length, int decimals, String abapType, boolean isKey) {
      this.name = name;
      this.length = length;
      this.decimals = decimals;
      this.abapType = abapType;
      this.isKey = isKey;
    }

    public String getName() {
      return name;
    }

    public int getLength() {
      return length;
    }

    public int getDecimals() {
      return decimals;
    }

    public String getAbapType() {
      return abapType;
    }

    public boolean isKey() {
      return isKey;
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    schemaTransformer = new SapTableToStructuredSchemaTransformer();

    sapColumnList = new ArrayList<>();

    int offset = 0;

    for (SapColMetadata colMetadata : SapColMetadata.values()) {
      int length = colMetadata.getLength();
      int decimals = colMetadata.getDecimals();

      // For decimal type columns, metadata RFM gives partial length and number of
      // decimals must be added to get real length
      SapFieldMetadata column = new SapColumn(colMetadata.getName(), null, null, colMetadata.ordinal() + 1, offset,
        length + decimals, decimals, colMetadata.toString(), colMetadata.getAbapType(), colMetadata.isKey());

      sapColumnList.add(column);

      offset += length + decimals;
    }
    tableMetadata = new SapObjectMetadata(sapColumnList);

    expectedSchemaFields = new ArrayList<>();
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 1, Schema.of(Schema.Type.STRING)));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 2, Schema.of(Schema.Type.STRING)));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 3, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 4, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 5, Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 6, Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 7, Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 8, Schema.nullableOf(Schema.of(Schema.Type.LONG))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 9, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 10, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 11, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 12, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 13, Schema.nullableOf(Schema.decimalOf(9, 3))));
    expectedSchemaFields.add(Schema.Field.of("field__Name_14", Schema.nullableOf(Schema.decimalOf(13, 5))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 15, Schema.nullableOf(Schema.decimalOf(20, 8))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 16, Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));
    expectedSchemaFields
      .add(Schema.Field.of(FIELD_NAME + 17, Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))));

    expectedSchemaFields
      .add(Schema.Field.of(FIELD_NAME + 18, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #createSchemaFields(java.util.List)}.
   */
  @Test
  public void testCreateSchemaFieldsAll() {
    List<Schema.Field> actualResult = schemaTransformer.createSchemaFields(sapColumnList);
    Assert.assertEquals("testCreateSchemaFieldsAll length does not match", expectedSchemaFields.size(),
      actualResult.size());

    Assert.assertEquals(expectedSchemaFields.toString(), actualResult.toString());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #createSchemaFields(java.util.List)}.
   */
  @Test
  public void testCreateSchemaFieldsSkipped() {
    SapFieldMetadata column = new SapColumn(".HIDDEN", null, null, 0, 0, 0, 0, ".NODE", ".NODE", false);
    // Simulate a meta field coming in response from SAP, which is of no consequence
    // is actual data so must not be used in schema
    List<SapFieldMetadata> tableColumnMetaList = new ArrayList<>(sapColumnList);
    tableColumnMetaList.add(0, column);

    List<Schema.Field> actualResult = schemaTransformer.createSchemaFields(tableColumnMetaList);
    Assert.assertNotEquals("testCreateSchemaFieldsSkipped length match", tableColumnMetaList.size(),
      actualResult.size());

    Assert.assertEquals(expectedSchemaFields.toString(), actualResult.toString());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   */
  @Test
  public void testReadFieldsSuccessful() throws IOException {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));
    String rawRecord = " chr  00256abapG  string      44444-2147483647 8888888888888888888 7777777.77777"
      + "AAAAAAAAAAAAx=                               -1234.567123456.78901-12345678901.2345678920191130034259"
      + "1991-12-31 23:59:59.0005671";

    StructuredRecord actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    Assert.assertEquals(" chr", actualResult.get(sapColumnList.get(0).getName()));

    Assert.assertEquals(" 00256", actualResult.get(sapColumnList.get(1).getName()));

    Assert.assertEquals("abapG", actualResult.get(sapColumnList.get(2).getName()));

    Assert.assertEquals("string", actualResult.get(sapColumnList.get(3).getName()));

    Assert.assertNull(actualResult.get(sapColumnList.get(4).getName()));

    Assert.assertEquals(new Integer(-44444), actualResult.get(sapColumnList.get(5).getName()));

    Assert.assertEquals(new Integer(2147483647), actualResult.get(sapColumnList.get(6).getName()));

    Assert.assertEquals(new Long(8888888888888888888L), actualResult.get(sapColumnList.get(7).getName()));

    Assert.assertEquals(new Double(7777777.77777), actualResult.get(sapColumnList.get(8).getName()));

    Assert.assertNull(actualResult.get(sapColumnList.get(10).getName()));

    byte[] decimalVal = (byte[]) actualResult.get(sapColumnList.get(12).getName());
    Assert.assertEquals(new BigDecimal("-1234.567"), new BigDecimal(new BigInteger(decimalVal), 3));

    decimalVal = (byte[]) actualResult.get(expectedSchemaFields.get(13).getName());
    Assert.assertEquals(new BigDecimal("-123456.78901"), new BigDecimal(new BigInteger(decimalVal), 5));

    decimalVal = (byte[]) actualResult.get(sapColumnList.get(14).getName());
    Assert.assertEquals(new BigDecimal("12345678901.23456789"), new BigDecimal(new BigInteger(decimalVal), 8));

    int dateVal = (int) actualResult.get(sapColumnList.get(15).getName());
    Assert.assertEquals("2019-11-30", LocalDate.ofEpochDay(dateVal).toString());

    long timeVal = (long) actualResult.get(sapColumnList.get(16).getName());
    Assert.assertEquals("03:42:59", LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(timeVal)).toString());

    long utcVal = (long) actualResult.get(sapColumnList.get(17).getName());
    String actualUtcDateTime = ZonedDateTime
      .ofInstant(Instant.ofEpochMilli(utcVal / 1000).plus(utcVal % 1000, ChronoUnit.MICROS), ZoneId.of("Z")).toString();

    Assert.assertEquals("1991-12-31T23:59:59.000567Z", actualUtcDateTime);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.source.SapToStructuredSchemaTransformer
   * #readColumns(java.lang.String, java.util.List, io.cdap.cdap.api.data.schema.Schema)}.
   * @throws IOException
   */
  @Test
  public void testReadColumnsDiffSchemaTypeSuccessful() throws IOException {
    SapFieldMetadata dateCol = sapColumnList.get(15);
    SapColumn dateAsStringCol = new SapColumn(dateCol.getName(), dateCol.getLabel(), null, dateCol.getPosition(),
      dateCol.getOffset(), dateCol.getLength(), 0, dateCol.getDataType(), "C", dateCol.isKey());

    SapFieldMetadata timeCol = sapColumnList.get(16);
    SapColumn timeAsStringCol = new SapColumn(timeCol.getName(), timeCol.getLabel(), null, timeCol.getPosition(),
      timeCol.getOffset(), timeCol.getLength(), 0, timeCol.getDataType(), "C", timeCol.isKey());

    SapFieldMetadata timestampCol = sapColumnList.get(17);
    SapColumn timestampAsStringCol =
      new SapColumn(timestampCol.getName(), timestampCol.getLabel(), null, timestampCol.getPosition(),
        timestampCol.getOffset(), timestampCol.getLength(), 0, timestampCol.getDataType(), "C", timestampCol.isKey());

    List<SapFieldMetadata> diffTypeFields = new ArrayList<>(sapColumnList);
    diffTypeFields.remove(17);
    diffTypeFields.remove(16);
    diffTypeFields.remove(15);

    diffTypeFields.add(dateAsStringCol);
    diffTypeFields.add(timeAsStringCol);
    diffTypeFields.add(timestampAsStringCol);

    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(diffTypeFields));

    String rawRecord = " chr  00256abapG  string      44444-2147483647 8888888888888888888 7777777.77777"
      + "AAAAAAAAAAAAx=                               -1234.567123456.78901-12345678901.2345678920191130034259"
      + "1991-12-31 23:59:59.0005671";

    StructuredRecord actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    Assert.assertEquals(" chr", actualResult.get(diffTypeFields.get(0).getName()));

    Assert.assertEquals(" 00256", actualResult.get(diffTypeFields.get(1).getName()));

    Assert.assertEquals("abapG", actualResult.get(diffTypeFields.get(2).getName()));

    Assert.assertEquals("string", actualResult.get(diffTypeFields.get(3).getName()));

    Assert.assertNull(actualResult.get(diffTypeFields.get(4).getName()));

    Assert.assertEquals(new Integer(-44444), actualResult.get(diffTypeFields.get(5).getName()));

    Assert.assertEquals(new Integer(2147483647), actualResult.get(diffTypeFields.get(6).getName()));

    Assert.assertEquals(new Long(8888888888888888888L), actualResult.get(diffTypeFields.get(7).getName()));

    Assert.assertEquals(new Double(7777777.77777), actualResult.get(diffTypeFields.get(8).getName()));

    Assert.assertNull(actualResult.get(diffTypeFields.get(10).getName()));

    byte[] decimalVal = (byte[]) actualResult.get(diffTypeFields.get(12).getName());
    Assert.assertEquals(new BigDecimal("-1234.567"), new BigDecimal(new BigInteger(decimalVal), 3));

    decimalVal = (byte[]) actualResult.get(expectedSchemaFields.get(13).getName());
    Assert.assertEquals(new BigDecimal("-123456.78901"), new BigDecimal(new BigInteger(decimalVal), 5));

    decimalVal = (byte[]) actualResult.get(diffTypeFields.get(14).getName());
    Assert.assertEquals(new BigDecimal("12345678901.23456789"), new BigDecimal(new BigInteger(decimalVal), 8));

    Assert.assertEquals("20191130", actualResult.get(diffTypeFields.get(15).getName()));

    Assert.assertEquals("034259", actualResult.get(diffTypeFields.get(16).getName()));

    Assert.assertEquals("1991-12-31 23:59:59.0005671", actualResult.get(diffTypeFields.get(17).getName()));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   */
  @Test
  public void testReadFieldsStrAllSpaceSuccessful() throws IOException {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));
    String rawRecord = "      \n           ";

    StructuredRecord actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    Assert.assertEquals("     ", actualResult.get(sapColumnList.get(0).getName()));

    Assert.assertEquals(" \n    ", actualResult.get(sapColumnList.get(1).getName()));

    Assert.assertEquals("       ", actualResult.get(sapColumnList.get(2).getName()));

    Assert.assertNull(actualResult.get(sapColumnList.get(3).getName()));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   */
  @Test
  public void testReadColumnsDefaultDateTimeSuccess() throws IOException {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));

    String rawRecord = "                                                                        "
      + "                                                                                       "
      + "        00000000000000                           ";

    StructuredRecord actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    Assert.assertNull(actualResult.get(sapColumnList.get(15).getName()));

    long timeVal = (long) actualResult.get(sapColumnList.get(16).getName());
    Assert.assertEquals("00:00", LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(timeVal)).toString());

    Assert.assertNull(actualResult.get(sapColumnList.get(17).getName()));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   */
  @Test
  public void testReadFieldsDefaultNumSuccess() throws IOException {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));

    String rawRecord = "                                                                   0000000.00000";

    StructuredRecord actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    Assert.assertNull(actualResult.get(sapColumnList.get(6).getName()));

    Assert.assertNull(actualResult.get(sapColumnList.get(7).getName()));

    Assert.assertEquals(new Double(0.0), actualResult.get(sapColumnList.get(8).getName()));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   */
  @Test
  public void testReadFieldsInvalidDateFail() {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));

    String rawRecord = "                                                                        "
      + "                                                                                               20191100";

    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(15).getName(),
        "20191100", Schema.LogicalType.DATE.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }

    rawRecord = "                                                                        "
      + "                                                                                               201911  ";

    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(15).getName(),
        "201911  ", Schema.LogicalType.DATE.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.source.SapToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   * @throws IOException
   */
  @Test
  public void testReadColumnsInvalidTimeSuccess() throws IOException {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));

    String rawRecord = "                                                                        "
      + "                                                                                "
      + "                       235950";

    StructuredRecord actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    long timeVal = (long) actualResult.get(sapColumnList.get(16).getName());
    Assert.assertEquals("23:59:50", LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(timeVal)).toString());

    rawRecord = "                                                                        "
      + "                                                                                       "
      + "                240000";

    actualResult = schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);

    timeVal = (long) actualResult.get(sapColumnList.get(16).getName());
    Assert.assertEquals("00:00", LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(timeVal)).toString());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.source.SapToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   * @throws IOException
   */
  @Test
  public void testReadColumnsInvalidTimeFail() throws IOException {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));

    String rawRecord = "                                                                        "
      + "                                                                                       "
      + "                253274";

    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(16).getName(),
        "253274", Schema.LogicalType.TIME_MICROS.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }

    rawRecord = "                                                                        "
      + "                                                                                       "
      + "                8:00:0";

    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(16).getName(),
        "8:00:0", Schema.LogicalType.TIME_MICROS.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer
   * #readFields(java.lang.String, com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata,
   *  io.cdap.cdap.api.data.schema.Schema)}.
   */
  @Test
  public void testReadFieldsInvalidNumFail() {
    Schema pluginSchema = Schema.recordOf("columnMetadata", schemaTransformer.createSchemaFields(sapColumnList));

    String rawRecord = "                              444.44-2147483647   ";
    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(5).getName(),
        "444.44", Schema.Type.INT.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }

    rawRecord = "                              44444-21474 3647   ";
    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(6).getName(),
        "21474 3647 ", Schema.Type.INT.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }

    rawRecord = "                              44444-2147483647 8,888,888,888,888,8";
    try {
      schemaTransformer.readFields(rawRecord, tableMetadata, pluginSchema);
    } catch (IOException e) {
      String goldErr = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(sapColumnList.get(7).getName(),
        "8,888,888,888,888,8", Schema.Type.LONG.toString());

      Assert.assertEquals(goldErr, e.getMessage());
    }
  }
}
