/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.google.cloud.datafusion.plugin.sap.odata.source.input;

import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.transform.SapODataRecordReader;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static com.google.cloud.datafusion.plugin.sap.odata.source.SapODataRuntimeHelper.ENCODED_ENTITY_METADATA_STRING;
import static com.google.cloud.datafusion.plugin.sap.odata.source.SapODataRuntimeHelper.ENCODED_X509_CERTIFICATE;
import static com.google.cloud.datafusion.plugin.sap.odata.source.SapODataRuntimeHelper.OUTPUT_SCHEMA;
import static com.google.cloud.datafusion.plugin.sap.odata.source.SapODataRuntimeHelper.PARTITIONS_PROPERTY;
import static com.google.cloud.datafusion.plugin.sap.odata.source.SapODataRuntimeHelper.SAP_ODATA_PLUGIN_PROPERTIES;

/**
 *
 */
public class SapODataInputFormat extends InputFormat<LongWritable, StructuredRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataInputFormat.class);

  private static final Gson gson = new Gson();

  private static final Type INPUT_SPLIT_TYPE = new TypeToken<List<SapODataInputSplit>>() {

  }.getType();

  @Override
  public List<InputSplit> getSplits(JobContext jContext) throws IOException, InterruptedException {

    LOGGER.info("inside getSplits");
    Configuration configuration = jContext.getConfiguration();
    List<InputSplit> splits = new ArrayList<>();

    // Deserialize partitions from Hadoop Configuration
    List<SapODataInputSplit> partitions = new Gson().fromJson(configuration.get(PARTITIONS_PROPERTY), INPUT_SPLIT_TYPE);

    for (SapODataInputSplit partition : partitions) {
      splits.add(partition);
    }

    LOGGER.info("end of getSplits");

    return splits;
  }


  @Override
  public RecordReader<LongWritable, StructuredRecord> createRecordReader(InputSplit split,
                                                                         TaskAttemptContext taContext)
    throws IOException, InterruptedException {

    LOGGER.info("inside createRecordReader");
    SapODataInputSplit inputSplit = (SapODataInputSplit) split;

    SapODataPluginConfig pluginConfig = gson.fromJson(taContext.getConfiguration()
      .get(SAP_ODATA_PLUGIN_PROPERTIES), SapODataPluginConfig.class);

    LOGGER.info("Raw Schema: {}", taContext.getConfiguration().get(OUTPUT_SCHEMA));

    Schema outputSchema = Schema.parseJson(taContext.getConfiguration().get(OUTPUT_SCHEMA));

    String encodedMetadataString = taContext.getConfiguration().get(ENCODED_ENTITY_METADATA_STRING);

    String encodedX509 = taContext.getConfiguration().get(ENCODED_X509_CERTIFICATE);

    SapODataRecordReader reader = new SapODataRecordReader(pluginConfig, outputSchema, encodedMetadataString,
      encodedX509,
      inputSplit.getStart(),
      inputSplit.getEnd(),
      inputSplit.getPackageSize());

    LOGGER.info("end of createRecordReader");
    return reader;
  }
}
