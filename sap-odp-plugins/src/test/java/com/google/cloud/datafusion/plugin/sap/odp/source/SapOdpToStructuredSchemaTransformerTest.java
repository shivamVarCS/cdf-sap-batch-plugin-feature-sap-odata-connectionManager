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

package com.google.cloud.datafusion.plugin.sap.odp.source;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasourceField;
import com.sap.conn.jco.JCoRecord;

import io.cdap.cdap.api.data.schema.Schema;

import mockit.Mocked;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @author sankalpbapat
 */

public class SapOdpToStructuredSchemaTransformerTest {

  private static final String FIELD_NAME = "fieldName";

  private static SapOdpToStructuredSchemaTransformer schemaTransformer;

  private static List<Schema.Field> expectedSchemaFields;
  private static List<SapFieldMetadata> sapFieldMetadataList;

  @Mocked
  JCoRecord jCoRecord;

  private enum SapColMetadata {
    C_CHAR(FIELD_NAME + 1, 5, 0, "C", true),
    N_CHAR(FIELD_NAME + 2, 6, 0, "N", true),
    G_CHAR(FIELD_NAME + 3, 7, 0, "G", false),
    STRING_CHAR(FIELD_NAME + 4, 8, 0, "STRING", false),
    B_INT(FIELD_NAME + 5, 4, 0, "B", false),
    S_INT("/field/Name6", 6, 0, "S", false),
    I_INT(FIELD_NAME + 7, 11, 0, "I", false),
    INT8_LONG(FIELD_NAME + 8, 20, 0, "8", false),
    F_DOUBLE(FIELD_NAME + 9, 13, 0, "F", false),
    X_BYTES(FIELD_NAME + 10, 14, 0, "X", false),
    Y_BYTES(FIELD_NAME + 11, 15, 0, "Y", false),
    XSTRING_BYTES(FIELD_NAME + 12, 16, 0, "XSTRING", false),
    P_DECIMAL(FIELD_NAME + 13, 6, 3, "P", false),
    A_DECIMAL(FIELD_NAME + 14, 8, 5, "A", false),
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
    schemaTransformer = new SapOdpToStructuredSchemaTransformer();

    sapFieldMetadataList = new ArrayList<>();

    for (SapColMetadata colMetadata : SapColMetadata.values()) {
      int decimals = colMetadata.getDecimals();

      // For decimal type columns, metadata RFM gives partial length and number of
      // decimals must be added to get real length
      SapFieldMetadata sapFieldMetadata =
        new SapOdpDatasourceField(colMetadata.getName(), null, null, colMetadata.getLength() + decimals, decimals,
          colMetadata.toString(), colMetadata.getAbapType(), colMetadata.isKey(), false, false);

      sapFieldMetadataList.add(sapFieldMetadata);
    }

    expectedSchemaFields = new ArrayList<>();
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 1, Schema.of(Schema.Type.STRING)));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 2, Schema.of(Schema.Type.STRING)));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 3, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 4, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 5, Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of("__field__Name6", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 7, Schema.nullableOf(Schema.of(Schema.Type.INT))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 8, Schema.nullableOf(Schema.of(Schema.Type.LONG))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 9, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 10, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 11, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 12, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 13, Schema.nullableOf(Schema.decimalOf(9, 3))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 14, Schema.nullableOf(Schema.decimalOf(13, 5))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 15, Schema.nullableOf(Schema.decimalOf(20, 8))));
    expectedSchemaFields.add(Schema.Field.of(FIELD_NAME + 16, Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));
    expectedSchemaFields
      .add(Schema.Field.of(FIELD_NAME + 17, Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))));

    expectedSchemaFields
      .add(Schema.Field.of(FIELD_NAME + 18, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
  }

  /**
   * Test method for
   * {@link SapOdpToStructuredSchemaTransformer#readFields(JCoRecord, SapObjectMetadata, Schema, boolean)}
   */
  @Test
  public void testCreateSchemaFieldsAll() {
    List<Schema.Field> actualResult = schemaTransformer.createSchemaFields(sapFieldMetadataList);

    Assert.assertEquals("testCreateSchemaFieldsAll length does not match", expectedSchemaFields.size(),
      actualResult.size());

    Assert.assertEquals(expectedSchemaFields.toString(), actualResult.toString());
  }

  /**
   * Test method for
   * {@link SapOdpToStructuredSchemaTransformer#readFields(JCoRecord, SapObjectMetadata, Schema, boolean)}
   */
  @Test
  public void testCreateSchemaFieldsSkipped() {
    SapFieldMetadata sapFieldMetadata =
      new SapOdpDatasourceField(".NODE", "Net Value", "Net transfer", Integer.parseInt(
        "000006"), Integer.parseInt("000000"), ".NODE", ".NODE", true, true, true);

    // Simulate a meta field coming in response from SAP, which is of no consequence
    // in actual data so must not be used in schema
    List<SapFieldMetadata> tableFieldMetaList = new ArrayList<>(sapFieldMetadataList);
    tableFieldMetaList.add(0, sapFieldMetadata);

    List<Schema.Field> actualResult = schemaTransformer.createSchemaFields(tableFieldMetaList);
    Assert.assertNotEquals("testCreateSchemaFieldsSkipped length match", tableFieldMetaList.size(),
      actualResult.size());

    Assert.assertEquals(expectedSchemaFields.toString(), actualResult.toString());
  }
}
