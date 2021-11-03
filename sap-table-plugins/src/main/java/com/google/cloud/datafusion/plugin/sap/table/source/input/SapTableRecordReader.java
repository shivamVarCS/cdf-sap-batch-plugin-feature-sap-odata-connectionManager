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

package com.google.cloud.datafusion.plugin.sap.table.source.input;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.SapInterface;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.source.AbstractStructuredSchemaTransformer;
import com.google.cloud.datafusion.plugin.sap.table.connection.out.SapTableInterfaceImpl;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.table.source.SapTableToStructuredSchemaTransformer;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.sap.conn.jco.ext.DestinationDataProvider;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sankalpbapat
 *
 */
public class SapTableRecordReader extends RecordReader<LongWritable, StructuredRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapTableRecordReader.class);

  private static final int MAX_RETRIES = 3;
  private static final int INITIAL_WAIT_TIME_MILLIS = 5000;

  public static final String ROW_SKIPS = "rowSkips";
  public static final String ROW_COUNT = "rowCount";

  private final Map<String, String> jcoConnProps;
  private final String tableName;
  private final List<String> filterOptions;
  private final SapInterface sapInterface;
  private final Schema pluginSchema;

  private final long start;
  private final long end;
  private final long packageSize;
  private final String totalWorkProcessCount;

  private SapConnection sapConn;

  private SapObjectMetadata runtimeMetadata;

  private long pos;
  private LongWritable key;
  private StructuredRecord value;
  private AbstractStructuredSchemaTransformer valueConverter;

  private Map<String, String> operProps;
  private SapTableRuntimeOutput sapTableOutput;

  public SapTableRecordReader(Map<String, String> jcoProps, String tableName, List<String> filterOptions,
                              String schemaStr, long start, long end, long packageSize, String totalWorkProcessCount) {

    this.jcoConnProps = new HashMap<>(jcoProps);
    this.tableName = tableName;

    this.filterOptions = new ArrayList<>(filterOptions);

    this.pluginSchema = getSchema(schemaStr);
    this.start = start;
    this.end = end;
    this.packageSize = packageSize;
    this.totalWorkProcessCount = totalWorkProcessCount;
    this.sapInterface = new SapTableInterfaceImpl();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taContext) throws IOException, InterruptedException {
    createConn(jcoConnProps);
    operProps = new HashMap<>();
    sapTableOutput = new SapTableRuntimeOutput(null, null);
    key = new LongWritable();
    valueConverter = new SapTableToStructuredSchemaTransformer();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // First time into this method or all rows exhausted, call RFM
    if (sapTableOutput.getOutputDataTable() == null || sapTableOutput.getOutputDataTable().isEmpty()) {
      calcSkipAndRowCount(pos);
      sapTableOutput = executeRequest();
      // If no rows are returned in current batch, then no point in making further
      // calls
      if (sapTableOutput.getOutputDataTable() == null || sapTableOutput.getOutputDataTable().isEmpty()) {
        LOGGER.info(ResourceConstants.INFO_NO_RECORDS.getMsgForKey(tableName));

        return false;
      }
      if (runtimeMetadata == null) {
        runtimeMetadata = sapTableOutput.getObjectMetadata();
      }
    } else if (!sapTableOutput.getOutputDataTable().nextRow()) {
      sapTableOutput.getOutputDataTable().deleteAllRows();
      // Check if any more network call is needed in the same split. If yes, keep the
      // process on, else return false
      return isNextCallReqd(pos) ? nextKeyValue() : Boolean.FALSE.booleanValue();
    }

    long rowNum = pos + start;
    // Set the key field value as the output key value
    key.set(rowNum);
    value =
      valueConverter.readFields(sapTableOutput.getOutputDataTable().getString("WA"), runtimeMetadata, pluginSchema);

    pos++;

    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return pos / (float) getLength();
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  private boolean isNextCallReqd(long rowsDone) {
    return getLength() - rowsDone > 0;
  }

  private void calcSkipAndRowCount(long rowsDone) {
    long rowSkip = rowsDone + start - 1;
    long remain = getLength() - rowsDone;
    long rowCount = remain > packageSize ? packageSize : remain;

    operProps.put(ROW_SKIPS, String.valueOf(rowSkip));
    operProps.put(ROW_COUNT, String.valueOf(rowCount));
  }

  private SapTableRuntimeOutput executeRequest() throws IOException, InterruptedException {
    int retryCount = 1;
    int waitTimeMillis = INITIAL_WAIT_TIME_MILLIS;
    while (true) {
      try {
        return (SapTableRuntimeOutput) sapInterface.getSourceData(tableName, filterOptions, operProps, sapConn);
      } catch (ConnectorException e) {
        long batchStart = Long.parseLong(operProps.get(ROW_SKIPS));
        long batchEnd = batchStart + Long.parseLong(operProps.get(ROW_COUNT));

        if (retryCount > MAX_RETRIES) {
          LOGGER.error(ResourceConstants.ERR_FAILED_PACKAGE_EXTRACT.getMsgForKeyWithCode(batchStart + 1, batchEnd,
            retryCount - 1));

          throw new IOException(e);
        }

        LOGGER.warn(ResourceConstants.WARN_RETRY_PACKAGE_EXTRACT.getMsgForKey(batchStart + 1, batchEnd, retryCount,
          waitTimeMillis / 1000));

        Thread.sleep(waitTimeMillis);
        // Exponentially increase timeout for every retry attempt
        waitTimeMillis *= 2;
        retryCount++;
      }
    }
  }

  private void createConn(Map<String, String> pluginProps) throws IOException {
    // JCo connection pool and expiration timeout properties, to avoid batch and
    // pipeline failures due to large requests which take more than 60 sec. to
    // respond (default timeout)
    // Max. allowed JCo connections to this SAP system is determined by PEAK_LIMIT,
    // and is set to total number of Dialog work processes in SAP
    pluginProps.put(DestinationDataProvider.JCO_PEAK_LIMIT, totalWorkProcessCount);
    pluginProps.put(DestinationDataProvider.JCO_POOL_CAPACITY, "5");
    pluginProps.put(DestinationDataProvider.JCO_EXPIRATION_TIME, "180000");
    pluginProps.put(DestinationDataProvider.JCO_EXPIRATION_PERIOD, "180000");
    pluginProps.put(DestinationDataProvider.JCO_MAX_GET_TIME, "180000");

    sapConn = new SapConnection(pluginProps);
    try {
      sapConn.initDestination();
    } catch (ConnectorException e) {
      throw new IOException(e);
    }
  }

  private long getLength() {
    return end - start + 1;
  }

  /**
   * Parses the string to schema
   * 
   * @param schema Schema in string format
   * @return schema Parsed schema
   */
  private Schema getSchema(String schemaStr) {
    try {
      return Util.isNotNullOrEmpty(schemaStr) ? Schema.parseJson(schemaStr) : null;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
    }
  }
}
