/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.connector.SAPODataConnectorConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.input.SapODataInputFormat;
import com.google.cloud.datafusion.plugin.sap.odata.source.input.SapODataInputSplit;
import com.google.cloud.datafusion.plugin.sap.odata.source.input.SapODataPartitionBuilder;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class SapODataRuntimeHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataRuntimeHelper.class);

  public static final String SAP_ODATA_PLUGIN_PROPERTIES = "SAP_ODATA_PLUGIN_PROPERTIES";
  public static final String PARTITIONS_PROPERTY = "PARTITIONS_PROPERTY";
  public static final String OUTPUT_SCHEMA = "OUTPUT_SCHEMA";
  public static final String ENCODED_ENTITY_METADATA_STRING = "ENTITY_METADATA";
  public static final String ENCODED_X509_CERTIFICATE = "X509_CERTIFICATE";

  private static final Gson GSON = new Gson();

  private final SapODataPluginConfig pluginConfig;
  private final SapODataService oDataService;
  private final SapX509Manager x509Manager;

  public SapODataRuntimeHelper(SapODataPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;

    x509Manager = new SapX509Manager(pluginConfig.getGcpProjectId(),
      pluginConfig.getCertGcsPath(),
      pluginConfig.getCertPassphrase());

    SapODataTransporter transporter = new SapODataTransporter(pluginConfig.getUsername(),
      pluginConfig.getPassword(), x509Manager);

    oDataService = new SapODataService(pluginConfig, transporter);
  }

  public void configureJob(BatchSourceContext context, Schema outputSchema)
    throws ODataServiceException, TransportException, IOException {
    LOGGER.info("inside setJob.");

    long skipRowCount = pluginConfig.getSkipRowCount() != null ? pluginConfig.getSkipRowCount() : 0L;
    long fetchRowCount = pluginConfig.getNumRowsToFetch() != null ? pluginConfig.getNumRowsToFetch() : 0L;
    int splitCount = pluginConfig.getSplitCount() != null ? pluginConfig.getSplitCount() : 0;
    long packageSize = pluginConfig.getBatchSize() != null ? pluginConfig.getBatchSize() : 0L;

    long availableRowCount = oDataService.getTotalAvailableRowCount();
    LOGGER.info("Total available row count:  {}", availableRowCount);

    if (availableRowCount <= 0) {
      throw new IllegalArgumentException(
        ResourceConstants.ERR_NO_RECORD_FOUND.getMsgForKeyWithCode(pluginConfig.getEntityName()));
    }

    if (context.isPreviewEnabled()) {
      long previewRecordCount = context.getMaxPreviewRecords();
      if (previewRecordCount <= 0) {
        fetchRowCount = 100L;
      }
      fetchRowCount = Math.min(previewRecordCount, Math.min(fetchRowCount, availableRowCount));
    }

    SapODataPartitionBuilder partitionBuilder = new SapODataPartitionBuilder();
    List<SapODataInputSplit> partitions = partitionBuilder.buildSplit(availableRowCount,
      fetchRowCount, skipRowCount, splitCount, packageSize);

    setJobForDataRead(context, outputSchema, partitions);
    LOGGER.info("end of setJob");
  }

  private void setJobForDataRead(BatchSourceContext context, Schema outputSchema, List<SapODataInputSplit> partitions)
    throws IOException, ODataServiceException, TransportException {

    LOGGER.info("inside setJobForDataRead");
    Configuration jobConfiguration;
    Job job = JobUtils.createInstance();
    jobConfiguration = job.getConfiguration();

//    try (Job job = JobUtils.createInstance()) {
//      jobConfiguration = job.getConfiguration();
//    }

    // Set properties in Hadoop Job's configuration
    jobConfiguration.set(SAP_ODATA_PLUGIN_PROPERTIES, GSON.toJson(pluginConfig));

    // Serialize the list of partitions to save in Hadoop Configuration
    String partitionString = new Gson().toJson(partitions);
    jobConfiguration.set(PARTITIONS_PROPERTY, partitionString);

    jobConfiguration.set(OUTPUT_SCHEMA, outputSchema.toString());

    String metadataString = oDataService.getEncodedServiceMetadata();
    jobConfiguration.set(ENCODED_ENTITY_METADATA_STRING, metadataString);

    String x509CertEncodedString = x509Manager.getX509AsBase64EncodedString();
    jobConfiguration.set(ENCODED_X509_CERTIFICATE, x509CertEncodedString);

    emitLineage(context, outputSchema, pluginConfig.getEntityName());

    SourceInputFormatProvider inputFormat = new SourceInputFormatProvider(SapODataInputFormat.class, jobConfiguration);
    context.setInput(Input.of(pluginConfig.getReferenceName(), inputFormat));

    LOGGER.info("end of setJobForDataRead");
  }

  private void emitLineage(BatchSourceContext context, Schema schema, String entity) {

    LOGGER.info("inside emitLineage");
    LineageRecorder lineageRecorder = new LineageRecorder(context, pluginConfig.getReferenceName());
    lineageRecorder.createExternalDataset(schema);

    if (schema.getFields() != null) {
      String operationDesc = String.format("Read '%s' from SAP OData service '%s'",
        entity, pluginConfig.getServiceName());

      lineageRecorder.recordRead("Read", operationDesc,
        schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }

    LOGGER.info("end of emitLineage");
  }
}
