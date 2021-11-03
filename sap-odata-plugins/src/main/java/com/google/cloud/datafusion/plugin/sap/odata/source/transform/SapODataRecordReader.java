/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.google.cloud.datafusion.plugin.sap.odata.source.transform;

import com.google.cloud.datafusion.plugin.sap.odata.source.SapODataService;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SapODataRecordReader extends RecordReader<LongWritable, StructuredRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataRecordReader.class);

  public static final String SKIPS_COUNT = "skip";
  public static final String FETCH_COUNT = "top";

  private final Schema pluginSchema;
  private final SapODataPluginConfig pluginConfig;

  private final long start;
  private final long end;
  private final long packageSize;
  private final String encodedMetadataString;
  private final String encodedX509;

  private long numRowsProcessed;
  private long totalRowCount;
  private LongWritable key;
  private SapODataTransformer valueConverter;
  private SapODataService oDataServices;

  private Edm edmData;

  private Map<String, Long> operProps;


  public SapODataRecordReader(final SapODataPluginConfig pluginConfig,
                              final Schema pluginSchema,
                              final String encodedMetadataString,
                              final String encodedX509, long start, long end, long packageSize) {

    this.pluginSchema = pluginSchema;
    this.pluginConfig = pluginConfig;
    this.encodedMetadataString = encodedMetadataString;
    this.encodedX509 = encodedX509;
    this.start = start;
    this.end = end;
    this.packageSize = packageSize;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taContext) throws IOException {

    LOGGER.info("inside initialize");
    SapX509Manager x509Manager = new SapX509Manager(encodedX509, pluginConfig.getCertPassphrase());
    SapODataTransporter transporter = new SapODataTransporter(pluginConfig.getUsername(),
      pluginConfig.getPassword(), x509Manager);
    oDataServices = new SapODataService(pluginConfig, transporter);

    key = new LongWritable();
    operProps = new HashMap<>(2);

    try {
      edmData = oDataServices.getODataServiceEdm(encodedMetadataString);
    } catch (ODataServiceException oce) {
      String errorMsg = String.format("Unable to convert encoded entity metadata string of '%s' to actual EDM type.",
        pluginConfig.getEntityName());
      throw new IOException(errorMsg, oce);
    }

    calculateSkipAndFetchCount(numRowsProcessed);
    LOGGER.info("data fetch request status: {}", pullODataRecords());

    LOGGER.info("end of initialize");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    key.set(numRowsProcessed);
    //check if existing batch set has records left to read
    if (valueConverter.containsNextRow()) {
      return true;
    }
    //check if next batch call is required if the existing batch set contains no records
    if (isNextCallReqd(numRowsProcessed)) {
      calculateSkipAndFetchCount(numRowsProcessed);
      return pullODataRecords();
    }
    return false;
  }

//  @Override
//  public boolean nextKeyValue() throws IOException, InterruptedException {
//    key.set(pos);
//    if (!valueConverter.containsNextRow()) {
//      if (isNextCallReqd(pos)) {
//        calculateSkipAndFetchCount(pos);
//        boolean status = pullODataRecords();
//        LOGGER.info("data fetch request status: {}", status);
//        return status;
//      } else {
//        return false;
//      }
//    }
//    return true;
//  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    LOGGER.info("calling data for Record: {}", (key.get() + start));
    return key;
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
    numRowsProcessed++;
    return valueConverter.buildCurrentCDFRecord();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return numRowsProcessed / (float) totalRowCount;
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  private boolean isNextCallReqd(long rowsDone) {
//    return getLength() - rowsDone > packageSize;
    return getLength() - rowsDone > 0;
  }

  private void calculateSkipAndFetchCount(long rowsDone) {
    long rowSkip = rowsDone + start - 1;
    long remain = getLength() - rowsDone;
    long rowCount = Math.min(remain, packageSize);

    operProps.put(SKIPS_COUNT, rowSkip);
    operProps.put(FETCH_COUNT, rowCount);
  }

  private long getLength() {
    return end - start + 1;
  }

  private boolean pullODataRecords() throws IOException {
    Long rowsToSkip = operProps.get(SKIPS_COUNT);
    Long rowsToFetch = operProps.get(FETCH_COUNT);

    List<ODataEntry> entryList;
    try {
      entryList = oDataServices.getEntityListForGivenMetadata(edmData, rowsToSkip, rowsToFetch);
    } catch (ODataServiceException | TransportException | InterruptedException ex) {
      String errorMsg = ResourceConstants.ERR_RECORD_PULL
        .getMsgForKeyWithCode(pluginConfig.getEntityName(), rowsToSkip, rowsToFetch);

      throw new IOException(errorMsg, ex);
    }
    if (!entryList.isEmpty()) {
      totalRowCount = entryList.size();
      valueConverter = new SapODataTransformer(pluginSchema, entryList);
      return true;
    } else {
      LOGGER.info("No records found in '{}' for given split indexes (skip: {} & top: {})",
        pluginConfig.getEntityName(), rowsToSkip, rowsToFetch);
      return false;
    }
  }
}
