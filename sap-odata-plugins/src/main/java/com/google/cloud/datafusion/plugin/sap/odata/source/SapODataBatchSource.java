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

package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.connector.SAPODataConnectorConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import com.google.cloud.datafusion.plugin.sap.odata.source.util.ExceptionParser;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.LongWritable;

import java.net.HttpURLConnection;
import javax.annotation.Nullable;

/**
 * Batch Source that reads data from an SAP ECC or S/4 HANA system which are exposed as OData service.
 * <p>
 * LongWritable is the first parameter because that is the key used by Hadoop's
 * {@code TextInputFormat}. Similarly, Text is the second parameter because that
 * is the value used by Hadoop's {@code TextInputFormat}.
 * {@code StructuredRecord} is the third parameter because that is what the
 * source will output. All the plugins included with Hydrator operate on
 * StructuredRecord.
 */

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapODataBatchSource.NAME)
@Description("Reads the data which is exposed as OData services from SAP. " +
        "The SAP source systems include: SAP S4HANA 1909.")
public class SapODataBatchSource extends BatchSource<LongWritable, StructuredRecord, StructuredRecord> {

  public static final String NAME = "SapOData";

  private final SapODataPluginConfig pluginConfig;

  public SapODataBatchSource(SapODataPluginConfig sapODataPluginConfig) {
    this.pluginConfig = sapODataPluginConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    pluginConfig.validatePluginParameters(failureCollector);

    if (pluginConfig.isSchemaBuildRequired()) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(getOutputSchema(failureCollector));
    } else {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Schema outputSchema = context.getOutputSchema();
    if (outputSchema == null) {
      outputSchema = getOutputSchema(context.getFailureCollector());
    }
    if (outputSchema == null) {
      throw new IllegalArgumentException(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode());
    }

    SapODataRuntimeHelper runtimeService = new SapODataRuntimeHelper(pluginConfig);
    runtimeService.configureJob(context, outputSchema);
  }

  /**
   * Gets the appropriate Schema basis the provided plugin parameters and also
   * sets the appropriate error messages in case any error is identified while preparing the Schema.
   *
   * @param failureCollector {@code FailureCollector}
   * @return {@code Schema}
   */
  @Nullable
  private Schema getOutputSchema(FailureCollector failureCollector) {

    SapX509Manager x509Manager = new SapX509Manager(pluginConfig.getGcpProjectId(),
            pluginConfig.getCertGcsPath(),
            pluginConfig.getCertPassphrase());

    SapODataTransporter transporter = new SapODataTransporter(pluginConfig.getUsername(),
            pluginConfig.getPassword(),
            x509Manager);

    SapODataService sapODataServices = new SapODataService(pluginConfig, transporter);
    try {
      //validate if the given parameters form a valid OData URL.
      sapODataServices.checkODataURL();

      return sapODataServices.buildOutputSchema();
    } catch (TransportException te) {
      String errorMsg = ExceptionParser.buildTransportError(te);
      switch (te.getErrorType()) {
        case TransportException.IO_ERROR:
          failureCollector.addFailure(errorMsg, null)
                  .withConfigProperty(SAPODataConnectorConfig.BASE_URL);
          break;

        case TransportException.X509_CERT_PATH_ERROR:
          failureCollector.addFailure(te.getMessage(), null)
                  .withConfigProperty(SapODataPluginConfig.CERT_GCS_PATH);
          break;

        default:
          failureCollector.addFailure(
                  ResourceConstants.ERR_ODATA_SERVICE_CALL.getMsgForKeyWithCode(errorMsg), null);
      }
    } catch (ODataServiceException ose) {
      attachFieldWithError(ose, failureCollector);
    }

    failureCollector.getOrThrowException();

    return null;
  }

  /**
   * Checks and attaches the UI fields with its relevant error message.
   *
   * @param ose              {@code ODataServiceException}
   * @param failureCollector {@code FailureCollector}
   */
  private void attachFieldWithError(ODataServiceException ose, FailureCollector failureCollector) {
    String errMsg = ExceptionParser.buildODataServiceError(ose);

    switch (ose.getErrorCode()) {
      case HttpURLConnection.HTTP_UNAUTHORIZED: {
        failureCollector.addFailure(errMsg, null).withConfigProperty(SAPODataConnectorConfig.UNAME);
        failureCollector.addFailure(errMsg, null).withConfigProperty(SAPODataConnectorConfig.PASSWORD);
      }
      break;
      case HttpURLConnection.HTTP_FORBIDDEN:
      case ODataServiceException.INVALID_SERVICE:
        failureCollector.addFailure(errMsg, null).withConfigProperty(SapODataPluginConfig.SERVICE_NAME);
        break;
      case ExceptionParser.NO_VERSION_FOUND:
      case ExceptionParser.INVALID_VERSION_FOUND:
        failureCollector.addFailure(errMsg, null).withConfigProperty(SapODataPluginConfig.ENTITY_NAME);
        break;
      case HttpURLConnection.HTTP_NOT_FOUND:
        failureCollector.addFailure(ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode(errMsg),
                ResourceConstants.ERR_NOT_FOUND.getMsgForKey());
        break;
      case HttpURLConnection.HTTP_BAD_REQUEST:
        failureCollector.addFailure(ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode(errMsg),
                ResourceConstants.ERR_CHECK_ADVANCED_PARAM.getMsgForKey());
        break;

      default:
        failureCollector.addFailure(
                ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode(errMsg), null);
    }
  }
}
