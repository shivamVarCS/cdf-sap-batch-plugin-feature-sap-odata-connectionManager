/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.google.cloud.datafusion.plugin.sap.odata.source.transform;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataDeltaFeed;
import org.apache.olingo.odata2.core.ep.entry.ODataEntryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class SapODataTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataTransformer.class);

  private final List<ODataEntry> oDataEntryList;
  private final Schema recordSchema;
  private final int totalRowCount;
  private int nextRow;

  public SapODataTransformer(Schema recordSchema, List<ODataEntry> oDataEntryList) {
    this.recordSchema = recordSchema;
    this.oDataEntryList = oDataEntryList;
    totalRowCount = oDataEntryList.size();
  }

  public StructuredRecord buildCurrentCDFRecord() {
    final ODataEntry oDataEntry = oDataEntryList.get(nextRow);
    nextRow += 1;

    return buildCDFStructure(recordSchema, oDataEntry);
  }

  public boolean containsNextRow() {
    return nextRow < totalRowCount;
  }

  private StructuredRecord buildCDFStructure(final Schema recordSchema, ODataEntry oDataEntry) {
    StructuredRecord.Builder record = StructuredRecord.builder(recordSchema);
    List<Schema.Field> cdfFields = recordSchema.getFields();
    cdfFields.forEach(field -> {
      String fieldName = field.getName();
      Object value = oDataEntry.getProperties().get(fieldName);
      Schema childSchema = field.getSchema();
//      LOGGER.info("FieldName: {}", fieldName);
//      LOGGER.info("Value: {}", value);
//      LOGGER.info("Schema: {}", childSchema);


      if (value instanceof ODataEntryImpl) {
        value = buildCDFStructure(childSchema, (ODataEntry) value);
      }
      if (value instanceof ODataDeltaFeed) {
        value = readInternalDeltaFeed(childSchema, (ODataDeltaFeed) value);
      }

      if (value != null) {
        processSchemaTypeValue(childSchema, record, fieldName, value);
      }
    });

    LOGGER.info("record: {}", record);
    return record.build();
  }


  private List<StructuredRecord> readInternalDeltaFeed(Schema recordSchema, ODataDeltaFeed oDataEntryList) {
//    List innerRecList = new ArrayList<StructuredRecord>();
    List<ODataEntry> entryList = oDataEntryList.getEntries();
    if (entryList.isEmpty()) {
      LOGGER.info("No records found in {}.", recordSchema.getDisplayName());
    }

//    for (ODataEntry oDataEntry : entryList) {
////      LOGGER.info("Display Name: {}", recordSchema.getDisplayName());
////      LOGGER.info("Record Name: {}", recordSchema.getRecordName());
////      LOGGER.info("Component Display Name: {}", recordSchema.getComponentSchema().getDisplayName());
////      LOGGER.info("Component Record Name: {}", recordSchema.getComponentSchema().getRecordName());
////      LOGGER.info(recordSchema.getComponentSchema().toString());
////      innerRecList.add(buildCDFStructure(recordSchema.getComponentSchema(), oDataEntry));
//      val deltaRec = buildCDFStructure(recordSchema.getComponentSchema(), oDataEntry);
//      innerRecList.add(deltaRec);
//    }

//    innerRecList = entryList.stream().map(oDataEntry -> buildCDFStructure(recordSchema.getComponentSchema(),
//    oDataEntry))
//      .collect(Collectors.toList());
//    if (innerRecList == null) {
//      innerRecList = new ArrayList<StructuredRecord>();
//    }
//    return innerRecList;

    return entryList.stream().map(oDataEntry -> buildCDFStructure(recordSchema.getComponentSchema(), oDataEntry))
      .collect(Collectors.toList());
  }

  private void processSchemaTypeValue(Schema fieldSchema, StructuredRecord.Builder recordBuilder,
                                      String fieldName,
                                      Object fieldValue) {

    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType == null) {
      recordBuilder.set(fieldName, fieldValue);
    } else if (logicalType == Schema.LogicalType.DECIMAL) {
      recordBuilder
        .setDecimal(fieldName, new BigDecimal(String.valueOf(fieldValue)).setScale(fieldSchema.getScale()));

    } else if (logicalType == Schema.LogicalType.TIME_MICROS) {
      LocalTime localTime = ((GregorianCalendar) fieldValue).toZonedDateTime().toLocalTime();
      recordBuilder.setTime(fieldName, localTime);

    } else if (logicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      ZonedDateTime zonedDateTime = ((GregorianCalendar) fieldValue).toZonedDateTime();
      recordBuilder.setTimestamp(fieldName, zonedDateTime);
    }
  }
}
