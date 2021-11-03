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

package com.google.cloud.datafusion.plugin.sap.odp.connection.out;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.out.SapDestinationDataProvider;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasource;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasourceField;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.odp.util.SapRfmExecutionHelper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;
import com.sap.conn.jco.rt.AbapFunction;
import com.sap.conn.jco.rt.DefaultDestinationManager;
import com.sap.conn.jco.rt.DefaultTable;
import com.sap.conn.jco.rt.RfcDestination;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sankalpbapat
 */
@RunWith(JMockit.class)
public class SapOdpInterfaceImplTest {
  private static Map<String, String> connParams;

  private List<SapFieldMetadata> sapFieldMetadataList;

  @Tested
  SapOdpInterfaceImpl sapInterface;

  SapConnection conn;

  @Mocked
  DestinationDataEventListener eventListener;

  @Mocked
  DefaultDestinationManager jcoDestMgr;

  @Mocked
  RfcDestination jcoDest;

  @Mocked
  AbapFunction jcoFunction;

  @Mocked
  DefaultTable jcoTable;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    connParams = new HashMap<>();
    connParams.put(SapOdpPluginConfigWrapper.JCO_CLIENT, "003");
    connParams.put(SapOdpPluginConfigWrapper.JCO_LANG, "EN");
    connParams.put(SapOdpPluginConfigWrapper.JCO_USER, "sapUser");
    connParams.put(SapOdpPluginConfigWrapper.JCO_PASSWD, "sapPaswd");
    connParams.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "VBAK");
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    sapFieldMetadataList = new ArrayList<>();

    new Expectations(Environment.class) {
      {
        Environment.registerDestinationDataProvider((DestinationDataProvider) any);
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

    conn = new SapConnection(connParams);
    conn.initDestination();
    sapInterface = new SapOdpInterfaceImpl();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    SapDestinationDataProvider destDataProv = SapDestinationDataProvider.getInstance();
    destDataProv.setDestinationDataEventListener(eventListener);
    destDataProv.removeDefinition(conn.getDestName());
    Environment.unregisterDestinationDataProvider(destDataProv);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.connection.out.SapOdpInterfaceImpl
   * #ping(com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   *
   * @throws ConnectorException
   */
  @Test
  public void testPingSuccess() throws ConnectorException {
    Assert.assertTrue("SapInterface ping failed", sapInterface.ping(conn));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.connection.out.SapOdpInterfaceImpl
   * #ping(com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   *
   * @throws JCoException
   */
  @Test
  public void testPingFail() throws JCoException {
    String jcoErrCode = "JCO_ERROR_ILLEGAL_ARGUMENT";
    String jcoErrMsg = "Mocked DEST_PING Exception";

    new Expectations() {
      {
        jcoDest.ping();
        result = new JCoException(131, jcoErrCode, jcoErrMsg);
        minTimes = 0;
      }
    };

    try {
      sapInterface.ping(conn);
      Assert.fail("Interface ping expected to fail but passed");
    } catch (ConnectorException e) {
      String errMsg = ResourceConstants.ERR_SAP_PING.getMsgForKeyWithCode() + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + jcoErrCode + " - " + jcoErrMsg;

      Assert.assertEquals(errMsg, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceMetadata(java.lang.String,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   *
   * @throws ConnectorException
   */
  @Test
  public void testGetSourceMetadataSuccessful(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapFieldMetadata sapFieldMetadata = new SapOdpDatasourceField("TEST_COLUMN", "Net Value", "Net transfer",
      Integer.parseInt("000006"), Integer.parseInt("000000"), "Char", "C", true, true, true);

    sapFieldMetadataList.add(sapFieldMetadata);

    new Expectations() {
      {
        rfmHelper.getColumnMetadata(anyString, null);
        result = new SapOdpDatasource(false, sapFieldMetadataList);
        minTimes = 0;
      }
    };

    String dataSourceName = "2L_ITS_MM";
    SapObjectMetadata sapObjectMetadata = sapInterface.getSourceMetadata(dataSourceName, conn);
    Assert.assertEquals("Column Metadata map size is 1", sapFieldMetadataList.size(),
      sapObjectMetadata.getFieldMetadata().size());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceMetadata(java.lang.String,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   *
   * @throws JCoException
   */
  @Test
  public void testGetSourceMetadataWNoStructFoundFail() throws JCoException {
    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result = new AbapException("STRUCT_NOT_FOUND", "Mocked STRUCT_NOT_FOUND exception for executeFunction()");
      }
    };

    String dataSourceName = "2L_ITS_OL";
    try {
      sapInterface.getSourceMetadata(dataSourceName, conn);
      Assert.fail(
        "SapInterface getSourceMetadata expected to fail with ERR_STRUCT_FOR_DATA_SOURCE_MISSING error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals(
        "SapInterface getSourceMetadata exception code does not match ERR_STRUCT_FOR_DATA_SOURCE_MISSING",
        ResourceConstants.ERR_STRUCT_FOR_DATA_SOURCE_MISSING.getMsgForKey(dataSourceName), e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceMetadata(java.lang.String,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   *
   * @throws JCoException
   */
  @Test
  public void testGetSourceMetadataNotFoundErrFail() throws JCoException {
    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result =
          new AbapException("DATA_SOURCE_NOT_EXIST", "Mocked DATA_SOURCE_NOT_EXIST exception for executeFunction()");
      }
    };

    String dataSourceName = "2L_ITS";
    try {
      sapInterface.getSourceMetadata(dataSourceName, conn);
      Assert.fail("SapInterface getSourceMetadata expected to fail with DATA_SOURCE_NOT_EXIST error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals("SapInterface getSourceMetadata exception code does not match DATA_SOURCE_NOT_EXIST",
        ResourceConstants.ERR_SOURCE_OBJ_MISSING.getMsgForKey(dataSourceName), e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface
   * #getRuntimeConfigParams(java.lang.String,
   * java.util.List, java.util.Map, com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}
   */
  @Test
  public void testGetRuntimeConfigParamsSuccessful(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapOdpRuntimeConfigInfo.Builder rtOdpInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    rtOdpInfoBuilder.setRuntimePackageCount(29L);

    SapOdpRuntimeConfigInfo.Builder odpConfigBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigBuilder.setTotalWorkProcCount(50).setAvailableWorkProcCount(30);

    new Expectations() {
      {
        rfmHelper.getRuntimePackageCount(anyString, null, anyBoolean, anyString, anyString, anyLong, null);
        result = rtOdpInfoBuilder;
        minTimes = 0;

        rfmHelper.getAvailableDialogWorkProcesses(conn);
        result = odpConfigBuilder;
        minTimes = 0;

        rfmHelper.getAvailableMemory(conn);
        result = 400000000L;
        minTimes = 0;
      }
    };

    SapRuntimeConfigInfo sapOdpRuntimeConfigInfo = sapInterface.getRuntimeConfigParams("2LIS_02_ITM",
      Arrays.asList("TEST_COLUMN:'Sample Test Value'"), false, "F", "SAP_ODP_SUBSCRIBER", 50000L, conn);

    Assert.assertNotNull("Generated sap odp configuration is null", sapOdpRuntimeConfigInfo);
    Assert.assertEquals(29L, ((SapOdpRuntimeConfigInfo) sapOdpRuntimeConfigInfo).getRuntimePackageCount());
    Assert.assertEquals(50, sapOdpRuntimeConfigInfo.getTotalWorkProcCount());
    Assert.assertEquals(30, sapOdpRuntimeConfigInfo.getAvailableWorkProcCount());
    Assert.assertEquals(400000000L, sapOdpRuntimeConfigInfo.getWpMaxMemory());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface
   * #getRuntimeConfigParams(java.lang.String,
   * java.util.List, java.util.Map, com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}
   */
  @Test
  public void testGetRuntimeConfigParamsWNotAuthDS(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapOdpRuntimeConfigInfo.Builder odpConfigBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigBuilder.setTotalWorkProcCount(50).setAvailableWorkProcCount(30);

    String sourceName = "0MATERIAL_ATRS";

    String errMsg = ResourceConstants.ERR_NOT_AUTHORIZED_FOR_DATASOURCE.getMsgForKeyWithCode(sourceName);

    new Expectations() {
      {
        rfmHelper.getRuntimePackageCount(anyString, null, anyBoolean, anyString, anyString, anyLong, null);
        result = new ConnectorException(ResourceConstants.ERR_NOT_AUTHORIZED_FOR_DATASOURCE.getCode(), errMsg);
        minTimes = 0;

        rfmHelper.getAvailableDialogWorkProcesses(conn);
        result = odpConfigBuilder;
        minTimes = 0;

        rfmHelper.getAvailableMemory(conn);
        result = 400000000L;
        minTimes = 0;
      }
    };

    try {
      sapInterface.getRuntimeConfigParams(sourceName, Arrays.asList("TEST_COLUMN:'Sample Test Value'"), false, "F",
        "SAP_ODP_SUBSCRIBER", 50000L, conn);
    } catch (ConnectorException e) {
      Assert.assertEquals(errMsg, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface
   * #getRuntimeConfigParams(java.lang.String,
   * java.util.List, java.util.Map, com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}
   */
  @Test
  public void testGetRuntimeConfigParamsWInactiveDS(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapOdpRuntimeConfigInfo.Builder odpConfigBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigBuilder.setTotalWorkProcCount(50).setAvailableWorkProcCount(30);

    String sourceName = "0MATERIAL_ATRS";

    String errMsg = ResourceConstants.ERR_SOURCE_OBJ_NOT_EXPOSED.getMsgForKeyWithCode(sourceName);

    new Expectations() {
      {
        rfmHelper.getRuntimePackageCount(anyString, null, anyBoolean, anyString, anyString, anyLong, null);
        result = new ConnectorException(ResourceConstants.ERR_SOURCE_OBJ_NOT_EXPOSED.getCode(), errMsg);
        minTimes = 0;

        rfmHelper.getAvailableDialogWorkProcesses(conn);
        result = odpConfigBuilder;
        minTimes = 0;

        rfmHelper.getAvailableMemory(conn);
        result = 400000000L;
        minTimes = 0;
      }
    };

    try {
      sapInterface.getRuntimeConfigParams(sourceName, Arrays.asList("TEST_COLUMN:'Sample Test Value'"), false, "F",
        "SAP_ODP_SUBSCRIBER", 50000L, conn);
    } catch (ConnectorException e) {
      Assert.assertEquals(errMsg, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface
   * #getRuntimeConfigParams(java.lang.String,
   * java.util.List, java.util.Map, com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}
   */
  @Test
  public void testGetRuntimeConfigParamsWMissingDS(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapOdpRuntimeConfigInfo.Builder odpConfigBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigBuilder.setTotalWorkProcCount(50).setAvailableWorkProcCount(30);

    String sourceName = "0MATERIAL_ATRS";

    String errMsg = ResourceConstants.ERR_SOURCE_OBJ_MISSING.getMsgForKey(sourceName);

    new Expectations() {
      {
        rfmHelper.getRuntimePackageCount(anyString, null, anyBoolean, anyString, anyString, anyLong, null);
        result = new ConnectorException("DATA_SOURCE_NOT_EXIST", errMsg);
        minTimes = 0;

        rfmHelper.getAvailableDialogWorkProcesses(conn);
        result = odpConfigBuilder;
        minTimes = 0;

        rfmHelper.getAvailableMemory(conn);
        result = 400000000L;
        minTimes = 0;
      }
    };

    try {
      sapInterface.getRuntimeConfigParams(sourceName, Arrays.asList("TEST_COLUMN:'Sample Test Value'"), false, "F",
        "SAP_ODP_SUBSCRIBER", 50000L, conn);
    } catch (ConnectorException e) {
      Assert.assertEquals(errMsg, e.getMessage());
      Assert.assertEquals("DATA_SOURCE_NOT_EXIST", e.getCode());
    }
  }
}
