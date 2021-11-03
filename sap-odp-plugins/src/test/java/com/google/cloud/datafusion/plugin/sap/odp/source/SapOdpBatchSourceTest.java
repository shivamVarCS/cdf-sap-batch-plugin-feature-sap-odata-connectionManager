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

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.out.SapDestinationDataProvider;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.odp.connection.out.SapOdpInterfaceImpl;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasource;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasourceField;
import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.odp.util.SapRfmExecutionHelper;
import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;
import com.sap.conn.jco.rt.DefaultDestinationManager;
import com.sap.conn.jco.rt.RfcDestination;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sankalpbapat
 *
 */
@RunWith(JMockit.class)
public class SapOdpBatchSourceTest {

  private static SapOdpDatasource sapOdpDatasource;

  private MockPipelineConfigurer pipelineConfigurer;

  @Tested
  private SapOdpBatchSource odpBatchSource;

  @Mocked
  SapOdpPluginConfig sapOdpPluginConfig;

  @Mocked
  SapOdpInterfaceImpl sapOdpInterface;

  @Mocked
  SapRfmExecutionHelper sapRfmExecutionHelper;

  @Mocked
  DestinationDataEventListener eventListener;

  @Mocked
  DefaultDestinationManager jcoDestMgr;

  @Mocked
  RfcDestination jcoDest;

  /**
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    List<SapFieldMetadata> sapFieldMetadataList = new ArrayList<>();
    SapFieldMetadata sapFieldMetadata = new SapOdpDatasourceField("TEST_COLUMN1", "Net Value", "Net transfer",
      Integer.parseInt("000006"), Integer.parseInt("000000"), "Char", "C", true, true, true);

    sapFieldMetadataList.add(sapFieldMetadata);
    sapOdpDatasource = new SapOdpDatasource(false, sapFieldMetadataList);
  }

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    odpBatchSource = new SapOdpBatchSource(sapOdpPluginConfig);
    pipelineConfigurer = new MockPipelineConfigurer(null);

    new Expectations(Environment.class) {
      {
        Environment.registerDestinationDataProvider((DestinationDataProvider) any);
        minTimes = 0;

        Environment.unregisterDestinationDataProvider((DestinationDataProvider) any);
        minTimes = 0;
      }
    };

    new Expectations(jcoDestMgr) {
      {
        JCoDestinationManager.getDestination(anyString);
        result = jcoDest;
        minTimes = 0;
      }
    };
  }

  /**
   * @throws Exception
   */
  @After
  public void tearDown() throws Exception {
    SapDestinationDataProvider destDataProv = SapDestinationDataProvider.getInstance();
    destDataProv.setDestinationDataEventListener(eventListener);
    SapConnection sapConn = new SapConnection(sapOdpPluginConfig.getConnPropsByType());
    destDataProv.removeDefinition(sapConn.getDestName());
    Environment.unregisterDestinationDataProvider(destDataProv);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testConfigurePipelineWOConnMacro() throws ConnectorException {
    new Expectations() {
      {
        sapOdpPluginConfig.isConnectionReqd();
        result = true;
        minTimes = 0;

        sapOdpPluginConfig.getSapSource();
        result = "testSourceObj";
        minTimes = 0;

        sapOdpInterface.getSourceMetadata(anyString, null);
        result = sapOdpDatasource;
        minTimes = 0;
      }
    };

    odpBatchSource.configurePipeline(pipelineConfigurer);
    Schema actualSchema = pipelineConfigurer.getOutputSchema();
    Assert.assertNotNull("Generated schema during pipeline configuration is null", actualSchema);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testConfigurePipelineWConnMacro() {
    new Expectations() {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapJcoPluginConfigWrapper.JCO_SYSNR.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    odpBatchSource.configurePipeline(pipelineConfigurer);
    Schema actualSchema = pipelineConfigurer.getOutputSchema();
    Assert.assertNull("Generated schema during pipeline configuratin is null", actualSchema);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testConfigurePipelineWTableMacro() throws ConnectorException {
    new Expectations() {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    odpBatchSource.configurePipeline(pipelineConfigurer);
    Schema actualSchema = pipelineConfigurer.getOutputSchema();
    Assert.assertNull("Generated schema during pipeline configuratin is null", actualSchema);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testConfigurePipelineWMissingDS() throws ConnectorException {
    String dataSourceName = "Test1234";
    String errCode = "DATA_SOURCE_NOT_EXIST";
    String errMsg = ResourceConstants.ERR_SOURCE_OBJ_MISSING.getMsgForKeyWithCode(dataSourceName);
    new Expectations() {
      {
        sapOdpPluginConfig.isConnectionReqd();
        result = true;
        minTimes = 0;

        sapOdpInterface.getSourceMetadata(anyString, null);
        result = new ConnectorException(errCode, errMsg);
        minTimes = 0;
      }
    };

    try {
      odpBatchSource.configurePipeline(pipelineConfigurer);
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testConfigurePipelineWMissingDS Failures size does not match", 2, failures.size());

      String goldErr = ResourceConstants.ERR_SOURCE_OBJ_MISSING.getMsgForKeyWithCode(dataSourceName);

      Assert.assertEquals("testConfigurePipelineWMissingDS datasource failure does not match", goldErr,
        failures.get(0).getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testConfigurePipelineWMissingStructForDS() throws ConnectorException {
    String dataSourceName = "Test1234";
    String errCode = "STRUCT_NOT_FOUND";
    String errMsg = ResourceConstants.ERR_STRUCT_FOR_DATA_SOURCE_MISSING.getMsgForKeyWithCode(dataSourceName);
    new Expectations() {
      {
        sapOdpPluginConfig.isConnectionReqd();
        result = true;
        minTimes = 0;

        sapOdpInterface.getSourceMetadata(anyString, null);
        result = new ConnectorException(errCode, errMsg);
        minTimes = 0;
      }
    };

    try {
      odpBatchSource.configurePipeline(pipelineConfigurer);
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testConfigurePipelineWMissingStructForDS Failures size does not match", 2, failures.size());

      String goldErr = ResourceConstants.ERR_STRUCT_FOR_DATA_SOURCE_MISSING.getMsgForKeyWithCode(dataSourceName);

      Assert.assertEquals("testConfigurePipelineWMissingStructForDS datasource structure failure does not match",
        goldErr, failures.get(0).getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWFullSuccessful() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE;
        minTimes = 0;
      }
    };

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName", null, null, false);
    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_FULL, actualExtractMode);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWSyncWLastTypeNAndStatusI() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE;
        minTimes = 0;
      }
    };

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName",
      SapOdpBatchSource.NO_REQUEST_FOR_DATASOURCE, SapOdpBatchSource.INFO, false);

    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_FULL, actualExtractMode);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWSyncWLastTypeFAndStatusSWDelta() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE;
        minTimes = 0;
      }
    };

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName",
      SapOdpBatchSource.EXTRACTION_MODE_FULL, SapOdpBatchSource.SUCCESS, true);

    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_DELTA, actualExtractMode);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWSyncWLastTypeFAndStatusSWoDelta() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE;
        minTimes = 0;
      }
    };

    Map<String, String> operationProps = new HashMap<>();
    operationProps.put(SapOdpBatchSource.SYNC_MODE, "X");

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName",
      SapOdpBatchSource.EXTRACTION_MODE_FULL, SapOdpBatchSource.SUCCESS, false);

    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_FULL, actualExtractMode);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWSyncWLastTypeFAndStatusE() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE;
        minTimes = 0;
      }
    };

    Map<String, String> operationProps = new HashMap<>();
    operationProps.put(SapOdpBatchSource.SYNC_MODE, "X");

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName",
      SapOdpBatchSource.EXTRACTION_MODE_FULL, SapOdpBatchSource.FAILURE, false);

    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_FULL, actualExtractMode);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWSyncWLastTypeDAndStatusS() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE;
        minTimes = 0;
      }
    };

    Map<String, String> operationProps = new HashMap<>();
    operationProps.put(SapOdpBatchSource.SYNC_MODE, "X");

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName",
      SapOdpBatchSource.EXTRACTION_MODE_DELTA, SapOdpBatchSource.SUCCESS, false);

    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_DELTA, actualExtractMode);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource
   * #configurePipeline(io.cdap.cdap.etl.api.PipelineConfigurer)}.
   */
  @Test
  public void testSetExtractionModeWSyncWLastTypeDAndStatusE() {
    new Expectations() {
      {
        sapOdpPluginConfig.getExtractType();
        result = SapOdpPluginConfigWrapper.EXTRACT_TYPE_SYNC_VALUE;
        minTimes = 0;
      }
    };

    Map<String, String> operationProps = new HashMap<>();
    operationProps.put(SapOdpBatchSource.SYNC_MODE, "X");

    String actualExtractMode = odpBatchSource.setExtractionMode("testSapObject", "subscName",
      SapOdpBatchSource.EXTRACTION_MODE_DELTA, SapOdpBatchSource.FAILURE, false);

    Assert.assertEquals(SapOdpBatchSource.EXTRACTION_MODE_RECOVERY, actualExtractMode);
  }
}
