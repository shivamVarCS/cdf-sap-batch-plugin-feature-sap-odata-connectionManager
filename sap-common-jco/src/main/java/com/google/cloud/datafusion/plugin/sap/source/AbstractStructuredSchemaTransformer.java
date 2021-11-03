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

package com.google.cloud.datafusion.plugin.sap.source;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * @author sankalpbapat
 *
 */
public abstract class AbstractStructuredSchemaTransformer {

  public static final char FWD_SLASH = '/';
  public static final char HYPHEN = '-';

  public static final String EMPTY_STRING = "";

  // Mapping of Abap type as key and its corresponding CDF type as value
  private static final Map<String, Schema> SAP_CDF_DATA_TYPE_MAPPING;

  static {
    Map<String, Schema> dataTypeMap = new HashMap<>();
    dataTypeMap.put("C", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("N", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("G", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("STRING", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("I", Schema.of(Schema.Type.INT));
    dataTypeMap.put("B", Schema.of(Schema.Type.INT));
    dataTypeMap.put("S", Schema.of(Schema.Type.INT));
    dataTypeMap.put("8", Schema.of(Schema.Type.LONG));
    dataTypeMap.put("F", Schema.of(Schema.Type.DOUBLE));
    dataTypeMap.put("X", Schema.of(Schema.Type.BYTES));
    dataTypeMap.put("Y", Schema.of(Schema.Type.BYTES));
    dataTypeMap.put("XSTRING", Schema.of(Schema.Type.BYTES));
    dataTypeMap.put("P", Schema.decimalOf(1, 1));
    dataTypeMap.put("A", Schema.decimalOf(1, 1));
    dataTypeMap.put("E", Schema.decimalOf(1, 1));
    dataTypeMap.put("D", Schema.of(Schema.LogicalType.DATE));
    dataTypeMap.put("T", Schema.of(Schema.LogicalType.TIME_MICROS));
    dataTypeMap.put("UTCL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    dataTypeMap.put("UTCLONG", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));

    SAP_CDF_DATA_TYPE_MAPPING = Collections.unmodifiableMap(dataTypeMap);
  }

  /**
   * Generates a {@link Schema.Field} element for every field of SAP object. This
   * standard implementation only handles flat structure of fields in the SAP
   * object.<br/>
   * Any hierarchy in the fields of native SAP object must be implemented by the
   * sub classes.
   * 
   * @param nativeFieldMetaList List of column metadata
   * @return List of {@link Schema.Field}
   */
  public List<Schema.Field> createSchemaFields(List<SapFieldMetadata> nativeFieldMetaList) {
    List<Schema.Field> fields = new ArrayList<>();
    for (SapFieldMetadata sapNativeField : nativeFieldMetaList) {
      String abapIntType = sapNativeField.getAbapType().toUpperCase();
      Schema sapMappedSchema = SAP_CDF_DATA_TYPE_MAPPING.get(abapIntType);
      // If any SAP data type mapping with CDF is missing, then skip that column.
      // '.NODE' is one such example. This data type based hidden column is listed in
      // column metadata response for SAP views but does not hold any actual data
      if (sapMappedSchema != null) {
        Schema fieldSchema = sapMappedSchema;
        if (sapMappedSchema.getLogicalType() == Schema.LogicalType.DECIMAL) {
          fieldSchema = Schema.decimalOf(sapNativeField.getLength(), sapNativeField.getDecimals());
        }

        // SAP column names may contain '/' char, which is NOT allowed by CDF, so encode
        // '/' to '__'. Single underscore is often used in column names in SAP.
        String encodedColName = sapNativeField.getName().replace("/", "__");

        fields
          .add(Schema.Field.of(encodedColName, sapNativeField.isKey() ? fieldSchema : Schema.nullableOf(fieldSchema)));
      }
    }

    return fields;
  }

  /**
   * Builds the {@code record} using the specified raw SAP record and column
   * metadata corresponding to SAP object.<br/>
   * <br/>
   * 
   * @param rawRecord       SAP response record for the table
   * @param runtimeObjMetadata column metadata helps to get individual column values
   *                        from {@code rawRecord}
   * @param outputSchema    Plugin schema as a string
   * @throws IOException
   */
  public StructuredRecord readFields(String rawRecord, SapObjectMetadata runtimeObjMetadata, Schema outputSchema)
    throws IOException {

    StructuredRecord.Builder recBuilder = StructuredRecord.builder(outputSchema);
    List<Schema.Field> schemaFields = outputSchema.getFields();
    // Iterate over fields as defined in the schema
    for (int i = 0; i < schemaFields.size(); i++) {
      Schema.Field field = schemaFields.get(i);
      String encodedFieldName = field.getName();

      String fieldValStr = getFieldNativeValue(runtimeObjMetadata, rawRecord, i);

      Schema fieldSchema = field.getSchema();
      // Get Non-nullable schema object for the current field
      Schema nonNullSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

      boolean isBlank = handleBlankVal(recBuilder, encodedFieldName, fieldValStr,
        nonNullSchema.getType() == Schema.Type.STRING);

      // If value from SAP is NOT null or empty then transform acc. to the field's
      // data type
      if (!isBlank) {
        processValue(nonNullSchema, recBuilder, encodedFieldName, fieldValStr);
      }
    }

    return recBuilder.build();
  }

  public abstract String getFieldNativeValue(SapObjectMetadata runtimeMetadata, String rawRecord, int fieldIdx);

  /**
   * Checks and sets null or original (whitespace filled) string as value in the
   * {@code StructuredRecord.Builder} for field represented by {@code fieldName}
   * 
   * @param recBuilder
   * @param fieldName
   * @param fieldValStr
   * @param isTypeString
   * @return True, if native value is null or empty. False, otherwise.
   */
  protected final boolean handleBlankVal(StructuredRecord.Builder recBuilder, String fieldName,
                                         @Nullable String fieldValStr, boolean isTypeString) {

    if (fieldValStr == null || fieldValStr.trim().isEmpty()) {
      // If no non-whitespace char is present in value and field type is String, then
      // set original value (consisting of only whitespace) to CDF field
      recBuilder.set(fieldName, isTypeString ? fieldValStr : null);
      return true;
    }

    return false;
  }

  /**
   * Processes field native value according to their type based on Schema Simple
   * Type or Logical Type
   * 
   * @param nonNullSchema
   * @param recBuilder
   * @param encodedFieldName
   * @param fieldValStr
   * @throws IOException
   */
  protected final void processValue(Schema nonNullSchema, StructuredRecord.Builder recBuilder, String encodedFieldName,
                                    String fieldValStr)
    throws IOException {

    try {
      if (nonNullSchema.getLogicalType() != null) {
        processLogicalTypeVal(nonNullSchema, recBuilder, encodedFieldName, fieldValStr.trim());
      } else {
        processTypeVal(nonNullSchema.getType(), recBuilder, encodedFieldName, fieldValStr);
      }
    } catch (Exception e) {
      handleConversionException(nonNullSchema.getLogicalType() != null ? nonNullSchema.getLogicalType().toString()
        : nonNullSchema.getType().toString(), encodedFieldName, fieldValStr, e);
    }
  }

  /**
   * Process the value for field which is mapped to a {@code Schema.LogicalType}
   * and set into the {@code StructuredRecord.Builder}.
   * 
   * @param nonNullSchema non nullable Schema
   * @param recBuilder    Structured record builder
   * @param fieldName     SAP objects's field name (may be encoded to remove CDF
   *                      unsupported chars)
   * @param fieldValTrim  trimmed value corresponding to an SAP object's field
   *                      name
   */
  private void processLogicalTypeVal(Schema nonNullSchema, StructuredRecord.Builder recBuilder, String fieldName,
                                     String fieldValTrim) {

    switch (nonNullSchema.getLogicalType()) {
      case DECIMAL:
        fieldValTrim = handleMinusAtEnd(fieldValTrim);
        recBuilder.setDecimal(fieldName, new BigDecimal(fieldValTrim).setScale(nonNullSchema.getScale()));
        break;

      case DATE:
        LocalDate localDate = null;
        // Date field in SAP has default/uninitialized value of 00000000
        if (!fieldValTrim.startsWith("0000")) {
          localDate = LocalDate.parse(fieldValTrim, DateTimeFormatter.BASIC_ISO_DATE);
        }
        recBuilder.setDate(fieldName, localDate);
        break;

      case TIME_MICROS:
        // Fix for bug 190464284 - handle missing characters in time value (Jira GCB-218)
        // Handle invalid time values = 240000. It is invalid and must be rolled over to
        // 000000. Any other invalid values > 235959, must simply throw an error
        if (fieldValTrim.equals("240000")) {
          fieldValTrim = "000000";
        }
        StringBuilder timeValBuilder = new StringBuilder(fieldValTrim);

        // Time field in SAP has values in format HHmmss, so add colon separators to
        // make it parseable
        timeValBuilder.insert(4, ':').insert(2, ':');
        LocalTime localTime = LocalTime.parse(timeValBuilder, DateTimeFormatter.ISO_LOCAL_TIME);
        recBuilder.setTime(fieldName, localTime);
        break;

      case TIMESTAMP_MICROS:
        ZonedDateTime zonedDateTime = null;
        // Check if UTCLONG string having format yyyy-MM-dd HH:mm:ss.SSSSSSS does not
        // start with default date value part 0000
        if (!fieldValTrim.startsWith("0000")) {
          String parseableTimestamp = fieldValTrim.replace(' ', 'T');

          String dateTimePart = parseableTimestamp + "+00:00";
          String nanoSecPart = "0";
          int nanoSecPartIdx = parseableTimestamp.indexOf('.');
          if (nanoSecPartIdx > 0) {
            dateTimePart = parseableTimestamp.substring(0, nanoSecPartIdx) + "+00:00";
            // nano second part is precise only to 1/10th of nanosecond so need to append 2
            // more zeros
            nanoSecPart = parseableTimestamp.substring(nanoSecPartIdx + 1) + "00";
          }
          zonedDateTime = ZonedDateTime.parse(dateTimePart, DateTimeFormatter.ISO_DATE_TIME);
          zonedDateTime = zonedDateTime.plus(Long.parseLong(nanoSecPart), ChronoUnit.NANOS);
        }
        recBuilder.setTimestamp(fieldName, zonedDateTime);
        break;

      default:
        recBuilder.set(fieldName, fieldValTrim);
        break;
    }
  }

  /**
   * Process the value for field which is mapped to a {@code Schema.Type} and set
   * into the {@code StructuredRecord.Builder}.
   * 
   * @param fieldType   Schema field logical type
   * @param recBuilder  Structured record builder
   * @param fieldName   SAP object's field name (may be encoded to remove CDF
   *                    unsupported chars)
   * @param fieldValStr value corresponding to an SAP object's field
   */
  private void processTypeVal(Schema.Type fieldType, StructuredRecord.Builder recBuilder, String fieldName,
                              String fieldValStr) {

    String colValTrim = fieldValStr.trim();
    switch (fieldType) {
      case INT:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Integer.parseInt(colValTrim));
        break;

      case LONG:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Long.parseLong(colValTrim));
        break;

      case FLOAT:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Float.parseFloat(colValTrim));
        break;

      case DOUBLE:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Double.parseDouble(colValTrim));
        break;

      case BYTES:
        recBuilder.set(fieldName, Bytes.toBytesBinary(colValTrim));
        break;

      case STRING:
        char[] colValArr = fieldValStr.toCharArray();
        int length = fieldValStr.length();
        // Right trim string
        for (int i = length - 1; i > -1; i--) {
          if (!Character.isWhitespace(colValArr[i])) {
            break;
          }
          length--;
        }
        recBuilder.set(fieldName, fieldValStr.substring(0, length));
        break;

      case NULL:
        recBuilder.set(fieldName, null);
        break;

      default:
        // shouldn't ever get here
        String err =
          ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(fieldName, fieldValStr, "any Schema type");

        throw new UnexpectedFormatException(err);
    }
  }

  /**
   * Removes minus sign from the end of the string and puts it at the beginning.
   * This handling is required for number values because some profile settings in
   * SAP may result in the minus sign being put at the end of the number like
   * {@code 12345-}.
   * 
   * @param fieldValTrim Trimmed field native value
   * @return String with minus appended at the beginning if it was at the end.
   *         Else returns the same string
   */
  private String handleMinusAtEnd(String fieldValTrim) {
    char lastChar = fieldValTrim.charAt(fieldValTrim.length() - 1);
    if (lastChar == HYPHEN) {
      StringBuilder sb = new StringBuilder(fieldValTrim);
      sb.deleteCharAt(sb.length() - 1).insert(0, lastChar);
      fieldValTrim = sb.toString();
    }

    return fieldValTrim;
  }

  private void handleConversionException(String schemaTypeString, String fieldName, String fieldVal, Exception e)
    throws IOException {

    String err = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(fieldName, fieldVal, schemaTypeString);
    throw new IOException(err, e);
  }
}
