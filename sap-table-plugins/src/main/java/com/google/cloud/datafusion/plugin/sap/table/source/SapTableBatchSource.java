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

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.SapInterface;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.source.AbstractStructuredSchemaTransformer;
import com.google.cloud.datafusion.plugin.sap.source.SapJcoBatchSourceWrapper;
import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.table.connection.out.SapTableInterfaceImpl;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.table.source.config.SapTablePluginConfig;
import com.google.cloud.datafusion.plugin.sap.table.source.config.SapTablePluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.table.source.input.SapTableInputFormat;
import com.google.cloud.datafusion.plugin.sap.table.source.input.SapTableInputSplit;
import com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Batch Source that reads Table data from an SAP ECC or S/4 HANA OP system.
 *
 * LongWritable is the first parameter because that is the key used by Hadoop's
 * {@code TextInputFormat}. Similarly, Text is the second parameter because that
 * is the value used by Hadoop's {@code TextInputFormat}.
 * {@code StructuredRecord} is the third parameter because that is what the
 * source will output. All the plugins included with Hydrator operate on
 * StructuredRecord.
 */
public class SapTableBatchSource extends BatchSource<LongWritable, StructuredRecord, StructuredRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapTableBatchSource.class);

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

  // Only these logical types are allowed to be changed to 'String' in the schema
  public static final Set<Schema.LogicalType> MODIFIABLE_LOGICAL_TYPES =
    ImmutableSet.of(Schema.LogicalType.DATE, Schema.LogicalType.TIME_MICROS, Schema.LogicalType.TIMESTAMP_MICROS);

  private final SapTablePluginConfig config;

  // SapConnection is not cached and is short lived only for validate call
  private SapConnection sapConn;

  public SapTableBatchSource(SapTablePluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    LOGGER.trace("Initiating SAP Table batch source configure pipeline");
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    try {
      // If any connection param is macro, no need to create connection instance
      if (config.isConnectionReqd()) {
        SapInterface sapInterface = new SapTableInterfaceImpl();
        createConnAndPing(sapInterface);

        if (!config.containsMacro(SapTablePluginConfigWrapper.EXTRACT_TABLE_VALUE)) {
          String inputTable = config.getSapTable();
          SapObjectMetadata tableMetadata = sapInterface.getSourceMetadata(inputTable, sapConn);

          AbstractStructuredSchemaTransformer schemaTransformer = new SapTableToStructuredSchemaTransformer();
          String schemaRecName = inputTable.replace("/", "__");
          Schema outputSchema = Schema.recordOf(OUTPUT_SCHEMA_NAME + "_" + schemaRecName,
            schemaTransformer.createSchemaFields(tableMetadata.getFieldMetadata()));

          // Fix for bug 190464284 - handle missing characters in time value (Jira GCB-218)
          validateSchemaFields(outputSchema, config.getSchema(), collector);
          collector.getOrThrowException();
          if (config.getSchema() != null) {
            outputSchema = config.getSchema();
          }

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
      configureTableException(e, collector);
      collector.addFailure(e.getMessage(), null);
      collector.getOrThrowException();
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException, InstantiationException {
    LOGGER.trace("Preparing SAP Table batch source pipeline for run");
    FailureCollector collector = context.getFailureCollector();

    try {
      SapTableInterfaceImpl sapTableInterface = new SapTableInterfaceImpl();
      createConnAndPing(sapTableInterface);

      String tableName = config.getSapTable().toUpperCase();
      Schema outputSchema = context.getOutputSchema();
      if (outputSchema == null) {
        // Get column metadata from SAP if macros were used at configure time
        SapObjectMetadata tableMetadata = sapTableInterface.getSourceMetadata(tableName, sapConn);

        AbstractStructuredSchemaTransformer schemaTransformer = new SapTableToStructuredSchemaTransformer();
        String schemaRecName = tableName.replace("/", "__");
        outputSchema = Schema.recordOf(OUTPUT_SCHEMA_NAME + "_" + schemaRecName,
          schemaTransformer.createSchemaFields(tableMetadata.getFieldMetadata()));
      }

      // Fix for bug 190464284 - handle missing characters in time value (Jira GCB-218)
      validateSchemaFields(outputSchema, config.getSchema(), collector);
      collector.getOrThrowException();
      if (config.getSchema() != null) {
        outputSchema = config.getSchema();
      }

      // Get extractable record count based on Filter Options condition along
      // with avg. record size, available work process count and max memory allowed
      // for a work process from SAP
      SapTableRuntimeConfigInfo runtimeTableInfo =
        sapTableInterface.getRuntimeConfigParams(tableName, config.getFormattedFilterOptions(), sapConn);

      long extractableRowCount = runtimeTableInfo.getRuntimeTableRecCount();
      LOGGER.info(ResourceConstants.INFO_FOUND_NUM_RECORDS.getMsgForKey(extractableRowCount));

      if (context.isPreviewEnabled()) {
        // Only supported from CDAP v6.3.0 onwards
        long maxPreviewRecords = context.getMaxPreviewRecords();
        // If user entered preview count = 0
        if (maxPreviewRecords < 1) {
          maxPreviewRecords = 100L;
        }
        extractableRowCount = maxPreviewRecords <= extractableRowCount ? maxPreviewRecords : extractableRowCount;
      }

      LOGGER.info(ResourceConstants.INFO_FOUND_MAX_MEMORY_FOR_WP.getMsgForKey(runtimeTableInfo.getWpMaxMemory()));

      SapTablePartitionBuilder partitionBuilder = new SapTablePartitionBuilder();
      List<SapTableInputSplit> partitions = partitionBuilder.build(runtimeTableInfo, extractableRowCount,
        config.getRowsToFetch(), config.getNumSplits(), config.getPackageSize());

      setJobForDataRead(context, outputSchema, partitions, runtimeTableInfo.getTotalWorkProcCount());
    } catch (LinkageError e) {
      // If error is raised due to missing JCo jars then raise soft exception, else
      // throw the actual error.
      createMissingJCoLibException(e, collector);
      collector.getOrThrowException();
    } catch (ConnectorException | IOException e) {
      collector.addFailure(e.getMessage(), null);
      collector.getOrThrowException();
    }
  }

  private void createConnAndPing(SapInterface sapInterface) throws ConnectorException {
    sapConn = new SapConnection(config.getConnPropsByType());
    sapConn.initDestination();

    LOGGER.trace("Initiating SAP Table connection ping test");
    sapInterface.ping(sapConn);
  }

  /**
   * Validates if UI updates to schema are allowed and expected when compared to
   * plugin auto generated schema
   *
   * @param autoGeneratedSchema
   * @param uiSchema
   * @param failureCollector
   * @return
   */
  private boolean validateSchemaFields(Schema autoGeneratedSchema, @Nullable Schema uiSchema,
                                       FailureCollector failureCollector) {

    if (uiSchema == null || autoGeneratedSchema.equals(uiSchema)) {
      return true;
    }

    List<Schema.Field> autoGenSchemaFields = autoGeneratedSchema.getFields();
    List<Schema.Field> uiSchemaFields = uiSchema.getFields();
    if (autoGenSchemaFields.size() != uiSchemaFields.size()) {
      String err = ResourceConstants.ERR_SCHEMA_FIELD_COUNT_MISMATCH.getMsgForKey();
      String action = ResourceConstants.ERR_SCHEMA_FIELD_INVALID_ACTION.getMsgForKey();
      failureCollector.addFailure(err, action);
    }

    // Iterate over fields of UI Schema
    for (int i = 0; i < uiSchemaFields.size(); i++) {
      Schema.Field uiField = uiSchemaFields.get(i);
      String uiEncodedColName = uiField.getName();
      Schema uiFieldSchema = uiField.getSchema();

      Schema.Field autoGenField = autoGenSchemaFields.get(i);
      String autoGenEncodedColName = autoGenField.getName();
      Schema autoGenFieldSchema = autoGenField.getSchema();

      Schema autoGenFieldNonNullSchema =
        autoGenFieldSchema.isNullable() ? autoGenFieldSchema.getNonNullable() : autoGenFieldSchema;

      String allowedDataType = getAllowedDataTypeName(autoGenFieldNonNullSchema);
      boolean isAutoGenFieldModifiable = MODIFIABLE_LOGICAL_TYPES.contains(autoGenFieldNonNullSchema.getLogicalType());

      if (isAutoGenFieldModifiable) {
        allowedDataType += " or 'string'";
      }

      // Either column order is changed or existing column name is updated on UI
      if (!uiEncodedColName.equals(autoGenEncodedColName)) {
        // Throw error as column order is different than plugin generated as per SAP
        String err = ResourceConstants.ERR_SCHEMA_FIELD_INVALID.getMsgForKey(uiEncodedColName, autoGenEncodedColName,
          allowedDataType);

        String action = ResourceConstants.ERR_SCHEMA_FIELD_INVALID_ACTION.getMsgForKey();
        failureCollector.addFailure(err, action).withOutputSchemaField(uiEncodedColName);
        break;
      }

      // If a field is found to be nullable by plguin auto generated schema, then it
      // must not be allowed to be made non-nullable on UI, because SAP considers the
      // field as nullable and may have null values in the table. Deliberately setting
      // it non-nullable will cause unnecessary failures.
      if (!uiFieldSchema.isNullable() && autoGenFieldSchema.isNullable()) {
        String err = ResourceConstants.ERR_SCHEMA_FIELD_NON_NULLABLE.getMsgForKey(uiEncodedColName);
        failureCollector.addFailure(err, null).withOutputSchemaField(uiEncodedColName);
        continue;
      }

      Schema uiFieldNonNullSchema = uiFieldSchema.isNullable() ? uiFieldSchema.getNonNullable() : uiFieldSchema;
      // Field on UI updated to a type different than its plugin auto generated type
      if (!uiFieldNonNullSchema.equals(autoGenFieldNonNullSchema)
        // Auto generated types Decimal, String, Int, Long, Double and Bytes, but on UI
        // updated to any other type, is not allowed
        && (!isAutoGenFieldModifiable
        // Auto generated types Date, Time, Timestamp but on UI updated to NON String
        // (like int, map etc.), is not allowed
        || (isAutoGenFieldModifiable && uiFieldNonNullSchema.getType() != Schema.Type.STRING))) {

        String err = ResourceConstants.ERR_SCHEMA_FIELD_TYPE_INVALID.getMsgForKey(uiEncodedColName, allowedDataType);
        failureCollector.addFailure(err, null).withOutputSchemaField(uiEncodedColName);
      }
    }

    return false;
  }

  private String getAllowedDataTypeName(Schema autoGenFieldNonNullSchema) {
    Schema.LogicalType logicalType = autoGenFieldNonNullSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return "'date'";

        case TIME_MILLIS: // FALLTHROUGH

        case TIME_MICROS:
          return "'time'";

        case TIMESTAMP_MILLIS: // FALLTHROUGH

        case TIMESTAMP_MICROS:
          return "'timestamp'";

        case DECIMAL:
          return String.format("'decimal' with precision %d and scale %d", autoGenFieldNonNullSchema.getPrecision(),
            autoGenFieldNonNullSchema.getScale());
      }
    }

    return "'" + autoGenFieldNonNullSchema.getType().name().toLowerCase() + "'";
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
   * Configures the error message to be displayed on UI, if input table is missing
   * in SAP or not valid or a generic exception message.
   *
   * @param e                {@link ConnectorException}
   * @param failureCollector {@link FailureCollector}
   */
  private void configureTableException(ConnectorException e, FailureCollector failureCollector) {
    if (e.getCode().equalsIgnoreCase("NOT_FOUND") || e.getCode().equalsIgnoreCase("INVALID_TABLE")) {
      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(SapTablePluginConfigWrapper.EXTRACT_TABLE_VALUE);
    } else {
      failureCollector.addFailure(e.getMessage(), null);
    }
  }

  private void setJobForDataRead(BatchSourceContext context, Schema outputSchema, List<SapTableInputSplit> partitions,
                                 int totalWorkProcessCount)
    throws IOException {

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> pluginConfigProps = new HashMap<>(config.getConnPropsByType());
    conf.setStrings(SapJcoBatchSourceWrapper.PLUGIN_SOURCE_JCO_PROPERTY_NAMES,
      pluginConfigProps.keySet().toArray(new String[pluginConfigProps.size()]));

    pluginConfigProps.put(Constants.Reference.REFERENCE_NAME, config.getReferenceName());
    pluginConfigProps.put(SapJcoPluginConfigWrapper.CONNECTION_TYPE, config.getConnType());

    String inputTable = config.getSapTable().toUpperCase();
    pluginConfigProps.put(SapTablePluginConfigWrapper.EXTRACT_TABLE_VALUE, inputTable);

    Gson gson = new GsonBuilder().create();
    String filterOptionStr = gson.toJson(config.getFormattedFilterOptions());
    pluginConfigProps.put(SapTablePluginConfigWrapper.FILTER_OPTIONS, filterOptionStr);

    if (Util.isNotNullOrEmpty(config.getProject())) {
      pluginConfigProps.put(SapJcoPluginConfigWrapper.GCP_PROJECT_ID, config.getProject());
    }

    pluginConfigProps.put(SapJcoPluginConfigWrapper.GCS_PATH, config.getGcsPathString());

    // Set properties in Hadoop Job's configuration
    for (Entry<String, String> entry : pluginConfigProps.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    // Serialize the list of partitions to save in Hadoop Configuration
    String partitionString = gson.toJson(partitions);
    conf.set(PARTITIONS_PROPERTY, partitionString);

    // Save total work process count in configuration to use as peak limit for JCo
    // connection
    conf.set(TOTAL_WORK_PROCESS, String.valueOf(totalWorkProcessCount));

    conf.set(OUTPUT_SCHEMA_NAME, outputSchema.toString());
    emitLineage(context, outputSchema, inputTable);

    context
      .setInput(Input.of(config.getReferenceName(), new SourceInputFormatProvider(SapTableInputFormat.class, conf)));
  }

  private void emitLineage(BatchSourceContext context, Schema schema, String table) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);

    if (schema.getFields() != null) {
      lineageRecorder.recordRead("Read", String.format("Read '%s' from SAP.", table),
        schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
