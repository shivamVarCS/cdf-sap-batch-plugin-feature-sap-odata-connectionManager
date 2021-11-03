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

import com.google.cloud.ServiceOptions;
import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.SapInterface;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.odp.connection.out.SapOdpInterfaceImpl;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpInputSplit;
import com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder;
import com.google.cloud.datafusion.plugin.sap.odp.util.Base62Encoder;
import com.google.cloud.datafusion.plugin.sap.odp.util.SapRfmExecutionHelper;
import com.google.cloud.datafusion.plugin.sap.source.AbstractStructuredSchemaTransformer;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.google.common.annotations.VisibleForTesting;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;

import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Batch Source that reads data from an ODP datasource in SAP ECC or S/4 HANA OP
 * system.
 * <p>
 * LongWritable is the first parameter because that is the key used by Hadoop's
 * {@code TextInputFormat}. Similarly, Text is the second parameter because that
 * is the value used by Hadoop's {@code TextInputFormat}.
 * {@code StructuredRecord} is the third parameter because that is what the
 * source will output. All the plugins included with Hydrator operate on
 * StructuredRecord.
 */
public class SapOdpBatchSource extends BatchSource<LongWritable, StructuredRecord, StructuredRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapOdpBatchSource.class);

  /**
   * <ul>
   * <li><b>\b</b> checks for word boundaries eg: \bstatic\b will match "static"
   * but not "ecstatic" or "_static".
   * <li><b>[1-9]</b> checks for one number digit, denoting the version of JCo
   * library in message.
   * <li><b>\.</b> checks for a literal . (dot), denoting the path.
   * </ul>
   */
  private static final String PATTERN_MISSING_JCO_LINK = ".*\\bsapjco[1-9]\\b in java\\.library\\.path.*";
  private static final String PATTERN_JCO_VERSION_MISMATCH = ".*com\\.sap\\.conn\\.jco\\.rt\\.JCoRuntimeFactory.*";
  // JVM may complain for any class in this package based on its first usage, so
  // class name should not be checked for
  private static final String MISSING_JCO_JAR = "com/sap/conn/jco/";

  public static final String OUTPUT_SCHEMA_NAME = "columnMetadata";
  public static final String PARTITIONS_PROPERTY = "sapPartitions";
  public static final String TOTAL_WORK_PROCESS = "totalWorkProcess";
  public static final String REPLICATION_POINTER = "replicationPointer";
  public static final String HIERARCHICAL_FLAG = "isHierarchical";

  public static final String MAX_PACKAGE_SIZE = "I_MAXPACKAGESIZE";
  public static final String EXTRACTION_MODE = "I_EXTRACTION_MODE";
  public static final String SUBSCRIBER_NAME = "I_SUBSCRIBER_NAME";
  public static final String SYNC_MODE = "I_SYNC";
  public static final String EXTRACTION_MODE_FULL = "F";
  public static final String EXTRACTION_MODE_DELTA = "D";
  public static final String EXTRACTION_MODE_RECOVERY = "R";
  public static final String EXTRACTION_MODE_DELTA_LABEL = "Delta";
  public static final String EXTRACTION_MODE_RECOVERY_LABEL = "Recovery of last Delta";
  public static final String SUCCESS = "S";
  public static final String SUCCESS_LABEL = "successful";
  public static final String FAILURE = "E";
  public static final String FAILURE_LABEL = "failed";
  public static final String NO_REQUEST_FOR_DATASOURCE = "N";
  public static final String INFO = "I";

  private final SapOdpPluginConfig config;

  // SapConnection is not cached and is short lived only for validate call
  private SapConnection sapConn;

  public SapOdpBatchSource(SapOdpPluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    LOGGER.trace("Initiating SAP ODP batch source configure pipeline");
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    try {
      // If any connection param is macro, no need to create connection instance
      if (config.isConnectionReqd()) {
        SapInterface sapInterface = new SapOdpInterfaceImpl();
        createConnAndPing(sapInterface);

        if (!config.containsMacro(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE)) {
          String inputDataSource = config.getSapSource();
          SapObjectMetadata odpSourceMetadata = sapInterface.getSourceMetadata(inputDataSource, sapConn);
          config.validateFilterOptions(collector, odpSourceMetadata.getFieldMetadata());

          AbstractStructuredSchemaTransformer schemaTransformer = new SapOdpToStructuredSchemaTransformer();
          String schemaRecName = inputDataSource.replace("/", "__");
          Schema outputSchema = Schema.recordOf(OUTPUT_SCHEMA_NAME + "_" + schemaRecName,
            schemaTransformer.createSchemaFields(odpSourceMetadata.getFieldMetadata()));

          if (outputSchema != null) {
            pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
          }
        }
      }
    } catch (LinkageError e) {
      createMissingJCoLibException(e, collector);
      // If error is raised due to missing JCo jars, then suppress it at configure
      // time. Else throw it.
    } catch (ConnectorException e) {
      configureDatasourceException(e, collector);
      collector.addFailure(e.getMessage(), null);
      collector.getOrThrowException();
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException, InstantiationException {
    LOGGER.trace("Preparing SAP ODP batch source pipeline for run");
    FailureCollector collector = context.getFailureCollector();

    try {
      if (context.isPreviewEnabled()) {
        throw new ConnectorException(ResourceConstants.ERR_PREVIEW_NOT_SUPPORTED.getCode(),
          ResourceConstants.ERR_PREVIEW_NOT_SUPPORTED.getMsgForKeyWithCode());
      }

      SapOdpInterfaceImpl sapOdpInterface = new SapOdpInterfaceImpl();
      createConnAndPing(sapOdpInterface);

      String inputDataSource = config.getSapSource().toUpperCase();
      Schema outputSchema = context.getOutputSchema();
      if (outputSchema == null) {
        // Get column metadata from SAP if macros were used at configure time
        SapObjectMetadata odpSourceMetadata = sapOdpInterface.getSourceMetadata(inputDataSource, sapConn);
        config.validateFilterOptions(collector, odpSourceMetadata.getFieldMetadata());

        AbstractStructuredSchemaTransformer schemaTransformer = new SapOdpToStructuredSchemaTransformer();
        String schemaRecName = inputDataSource.replace("/", "__");
        outputSchema = Schema.recordOf(OUTPUT_SCHEMA_NAME + "_" + schemaRecName,
          schemaTransformer.createSchemaFields(odpSourceMetadata.getFieldMetadata()));
      }

      String subscriberName = getFormattedSubscriberName(context);

      // Convert user provided package size (in KB) to bytes for SAP consumption
      long packageSizeBytes = config.getPackageSizeKB() * 1024;

      SapOdpRuntimeConfigInfo previousExtractInfo =
        getPreviousExtractInfo(sapOdpInterface, inputDataSource, subscriberName);

      String previousExtractType = previousExtractInfo.getPreviousExtractType();
      String previousExtractStatus = previousExtractInfo.getPreviousExtractStatus();
      boolean isDeltaSupported = previousExtractInfo.isDeltaSupported();
      String extractMode = setExtractionMode(inputDataSource, subscriberName, previousExtractType,
        previousExtractStatus, isDeltaSupported);

      // Get extractable package count based on Filter Options condition along with
      // replication pointer, available work process count and max memory allowed for
      // a work process from SAP
      SapOdpRuntimeConfigInfo odpRuntimeConfig = sapOdpInterface.getRuntimeConfigParams(inputDataSource,
        config.getFormattedFilterOptions(), false, extractMode, subscriberName, packageSizeBytes, sapConn);

      long extractablePackageCount = odpRuntimeConfig.getRuntimePackageCount();
      LOGGER.info(ResourceConstants.INFO_FOUND_NUM_PACKAGES.getMsgForKey(extractablePackageCount));
      LOGGER.info(ResourceConstants.INFO_FOUND_MAX_MEMORY_FOR_WP.getMsgForKey(odpRuntimeConfig.getWpMaxMemory()));

      SapOdpPartitionBuilder partitionBuilder = new SapOdpPartitionBuilder();
      List<SapOdpInputSplit> partitions =
        partitionBuilder.build(odpRuntimeConfig, config.getNumSplits(), packageSizeBytes);
      // Set Hadoop Job and configuration params
    } catch (LinkageError e) {
      // If error is raised due to missing JCo jars then raise soft exception, else
      // throw the actual error.
      createMissingJCoLibException(e, collector);
      collector.getOrThrowException();
    } catch (ConnectorException e) {
      collector.addFailure(e.getMessage(), null);
      collector.getOrThrowException();
    }
  }

  private void createConnAndPing(SapInterface sapInterface) throws ConnectorException {
    sapConn = new SapConnection(config.getConnPropsByType());
    sapConn.initDestination();

    LOGGER.trace("Initiating SAP ODP connection ping test");
    sapInterface.ping(sapConn);
  }

  /**
   * Checks whether the input {@code LinkageError} is due to missing JCo
   * libraries. If yes, then wraps a custom user friendly message in soft
   * exception and sets it to FaulireCollector. Otherwise, throws the
   * {@code LinkageError} as is, after logging.
   *
   * @param e                {@link LinkageError}
   * @param failureCollector {@link FailureCollector}
   */
  private void createMissingJCoLibException(LinkageError e, FailureCollector failureCollector) {
    if (e.getMessage().contains(MISSING_JCO_JAR) || e.getMessage().matches(PATTERN_MISSING_JCO_LINK)
      || e.getMessage().matches(PATTERN_JCO_VERSION_MISMATCH)) {

      String err = ResourceConstants.ERR_JCOLIB_MISSING.getMsgForKeyWithCode();
      String action = ResourceConstants.ERR_JCO_MISSING_ACTION.getMsgForKey();
      // Just add failure, but don't throw here
      failureCollector.addFailure(err, action);
    } else {
      throw e;
    }
  }

  /**
   * Configures the error message to be displayed on UI, if input datasource is
   * missing or is the structure for datasource is missing in SAP or a generic
   * exception message.
   *
   * @param e                {@link ConnectorException}
   * @param failureCollector {@link FailureCollector}
   */
  private void configureDatasourceException(ConnectorException e, FailureCollector failureCollector) {
    if (e.getCode().equalsIgnoreCase(SapRfmExecutionHelper.ERR_DATA_SOURCE_NOT_EXIST)
      || e.getCode().equalsIgnoreCase(SapRfmExecutionHelper.ERR_DATA_SOURCE_NOT_EXPOSED)
      || e.getCode().equalsIgnoreCase(SapRfmExecutionHelper.ERR_STRUCTURE_NOT_FOUND)) {

      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE);
    } else {
      failureCollector.addFailure(e.getMessage(), null);
    }
  }

  /**
   * Provides subscriber name. If not specified by user, generates the encoded
   * subscriber name for default subscriber name
   *
   * @param context
   * @return formatted subscriber name
   */
  private String getFormattedSubscriberName(BatchSourceContext context) {
    String subscriberName = config.getSubscriberName();
    if (Util.isNullOrEmpty(subscriberName)) {
      subscriberName =
        ServiceOptions.getDefaultProjectId() + "/" + context.getNamespace() + "/" + context.getPipelineName();

      String encodedShortString = Base62Encoder.getEncodedShortString(subscriberName.replace("-", "_"));

      LOGGER.info("SAP ODP default subscriber name '{}', and shortened/encoded subscriber name '{}'", subscriberName,
        encodedShortString);

      subscriberName = encodedShortString;
    } else {
      LOGGER.info("SAP ODP user defined subscriber name '{}'", subscriberName);
    }

    return subscriberName;
  }

  /**
   * Gets the previous extract type and status to help decide the extract mode
   * 
   * @param sapOdpInterface
   * @param inputDataSource data source name
   * @param subscName       ODP subscriber name
   * @return ODP runtime info
   * @throws ConnectorException
   */
  private SapOdpRuntimeConfigInfo getPreviousExtractInfo(SapOdpInterfaceImpl sapOdpInterface, String inputDataSource,
                                                         String subscName)
    throws ConnectorException {

    if (SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE.equalsIgnoreCase(config.getExtractType())) {
      // Get last execution mode and its status, to automatically determine the
      // execution mode for this execution
      return sapOdpInterface.getRuntimeConfigParams(inputDataSource, config.getFormattedFilterOptions(), true,
        EXTRACTION_MODE_FULL, subscName, 0L, sapConn);
    }

    return SapOdpRuntimeConfigInfo.builder().build();
  }

  @VisibleForTesting
  String setExtractionMode(String inputDatasource, String subscriberName, String previousExtractType,
                           String previousExtractStatus, boolean isDeltaSupported) {

    // User selects 'Full' on UI, no need to check previousExtractType and status
    if (config.getExtractType().equalsIgnoreCase(SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE)) {
      return EXTRACTION_MODE_FULL;
    }

    LOGGER.info("SAP ODP data extraction mode is selected as 'Sync'");

    String extractMode = "";
    // User selects 'Sync' on UI and extraction from this datasource having this
    // subscriber name is happening for the first time.
    if (NO_REQUEST_FOR_DATASOURCE.equals(previousExtractType) && INFO.equals(previousExtractStatus)) {
      extractMode = EXTRACTION_MODE_FULL;
      LOGGER.info(ResourceConstants.INFO_SYNC_LAST_EXTRACT_NOT_FOUND.getMsgForKey(subscriberName, inputDatasource,
        SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE));

    } else if (EXTRACTION_MODE_FULL.equals(previousExtractType) && SUCCESS.equals(previousExtractStatus)) {
      // If Datasource doesn't support delta, then keep executing it in Full mode
      extractMode = isDeltaSupported ? EXTRACTION_MODE_DELTA : EXTRACTION_MODE_FULL;
      LOGGER.info(ResourceConstants.INFO_SYNC_LAST_EXTRACT.getMsgForKey(
        SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE, SUCCESS_LABEL, subscriberName, inputDatasource,
        isDeltaSupported ? EXTRACTION_MODE_DELTA_LABEL : SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE));

    } else if (EXTRACTION_MODE_FULL.equals(previousExtractType) && FAILURE.equals(previousExtractStatus)) {
      extractMode = EXTRACTION_MODE_FULL;
      LOGGER
        .info(ResourceConstants.INFO_SYNC_LAST_EXTRACT.getMsgForKey(SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE,
          FAILURE_LABEL, subscriberName, inputDatasource, SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE));

    } else if (EXTRACTION_MODE_DELTA.equals(previousExtractType) && SUCCESS.equals(previousExtractStatus)) {
      extractMode = EXTRACTION_MODE_DELTA;
      LOGGER.info(ResourceConstants.INFO_SYNC_LAST_EXTRACT.getMsgForKey(EXTRACTION_MODE_DELTA_LABEL, SUCCESS_LABEL,
        subscriberName, inputDatasource, EXTRACTION_MODE_DELTA_LABEL));

    } else if (EXTRACTION_MODE_DELTA.equals(previousExtractType) && FAILURE.equals(previousExtractStatus)) {
      // If previous delta failed, the plugin config's extract type (D) will have to
      // be explicitly updated to 'R'
      extractMode = EXTRACTION_MODE_RECOVERY;
      LOGGER.info(ResourceConstants.INFO_SYNC_LAST_EXTRACT.getMsgForKey(EXTRACTION_MODE_DELTA_LABEL, FAILURE_LABEL,
        subscriberName, inputDatasource, EXTRACTION_MODE_RECOVERY_LABEL));

    } else {
      throw new IllegalStateException(
        "Error while auto configuring Extraction Mode based on previous Extraction Mode and Status");
    }

    return extractMode;
  }
}
