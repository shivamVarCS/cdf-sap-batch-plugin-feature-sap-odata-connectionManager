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

package com.google.cloud.datafusion.plugin.sap.table.connection.out;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.out.SapDestinationDataProvider;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapColumn;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.table.source.config.SapTablePluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.table.source.input.SapTableRecordReader;
import com.google.cloud.datafusion.plugin.sap.table.util.SapRfmExecutionHelper;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sankalpbapat
 */
@RunWith(JMockit.class)
public class SapTableInterfaceImplTest {
  private static Map<String, String> connParams;
  private static Map<String, String> operationsProps;

  private List<SapFieldMetadata> sapColumnList;

  @Tested
  SapTableInterfaceImpl sapTableInterface;

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
    connParams.put(SapTablePluginConfigWrapper.JCO_CLIENT, "003");
    connParams.put(SapTablePluginConfigWrapper.JCO_LANG, "EN");
    connParams.put(SapTablePluginConfigWrapper.JCO_USER, "sapUser");
    connParams.put(SapTablePluginConfigWrapper.JCO_PASSWD, "sapPaswd");
    connParams.put(SapTablePluginConfigWrapper.EXTRACT_TABLE_VALUE, "VBAK");
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    sapColumnList = new ArrayList<>();
    operationsProps = new HashMap<>();

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
    sapTableInterface = new SapTableInterfaceImpl();
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
   * {@link com.google.cloud.datafusion.plugin.sap.table.connection.out.SapTableInterfaceImpl
   * #ping(com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   * @throws ConnectorException
   */
  @Test
  public void testPingSuccess() throws ConnectorException {
    Assert.assertTrue("SapInterface ping failed", sapTableInterface.ping(conn));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.connection.out.SapTableInterfaceImpl
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
      sapTableInterface.ping(conn);
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
    SapFieldMetadata column = new SapColumn("ERZET", null, null, 0, 0, 10, 0, "CHAR", "C", false);
    sapColumnList.add(column);
    SapObjectMetadata tableMetadata = new SapObjectMetadata(sapColumnList);

    new Expectations() {
      {
        rfmHelper.getColumnMetadata(anyString, null);
        result = tableMetadata;
        minTimes = 0;
      }
    };

    String sapTable = "VBAK";
    SapObjectMetadata actualObjMetadata = sapTableInterface.getSourceMetadata(sapTable, conn);
    Assert.assertEquals("Column Metadata map size is 1", sapColumnList.size(),
      actualObjMetadata.getFieldMetadata().size());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceMetadata(java.lang.String,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   *
   * @throws JCoException
   */
  @Test
  public void testGetTableMetadataNotFoundFail() throws JCoException {
    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result = new AbapException("NOT_FOUND", "Mocked NOT_FOUND exception for executeFunction()");
      }
    };

    String sapTable = "VBAK";
    try {
      sapTableInterface.getSourceMetadata(sapTable, conn);
      Assert.fail("SapInterface getTableMetadata expected to fail with ERR_TABLE_VIEW_MISSING error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals(ResourceConstants.ERR_TABLE_VIEW_MISSING.getMsgForKey(sapTable), e.getMessage());
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
  public void testGetTableMetadataInternalErrorFail() throws JCoException {
    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result = new AbapException("INTERNAL_ERROR", "Mocked INTERNAL_ERROR exception for executeFunction()");
      }
    };

    try {
      String sapTable = "VBAK";
      sapTableInterface.getSourceMetadata(sapTable, conn);
      Assert.fail("SapInterface getTableMetadata expected to fail with ERR_EXEC_FUNC error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals("SapInterface getTableMetadata exception code does not match ERR_EXEC_FUNC",
        ResourceConstants.ERR_EXEC_FUNC.getCode(), e.getCode());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getRuntimeConfigParams(java.lang.String,
   * java.util.List<String>,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   * @throws ConnectorException
   */
  @Test
  public void testGetRuntimeConfigParamsSuccessful(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapTableRuntimeConfigInfo.Builder rtTableInfoBuilder = SapTableRuntimeConfigInfo.builder();
    rtTableInfoBuilder.setRuntimeTableRecCount(50000L).setRecordSize(2500);

    SapTableRuntimeConfigInfo.Builder tableConfigBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigBuilder.setTotalWorkProcCount(50).setAvailableWorkProcCount(30);

    new Expectations() {
      {
        rfmHelper.getRuntimeTableRecordCount(anyString, null, null);
        result = rtTableInfoBuilder;
        minTimes = 0;

        rfmHelper.getAvailableDialogWorkProcesses(conn);
        result = tableConfigBuilder;
        minTimes = 0;

        rfmHelper.getAvailableMemory(conn);
        result = 400000000L;
        minTimes = 0;
      }
    };

    String sapTable = "VBAK";
    SapRuntimeConfigInfo sapTableRuntimeConfigInfo =
      sapTableInterface.getRuntimeConfigParams(sapTable, Arrays.asList("TEST_COLUMN = 'Sample Test Value'"), conn);

    Assert.assertNotNull("Generated sap table configuration is null", sapTableRuntimeConfigInfo);
    Assert.assertEquals(50000L, ((SapTableRuntimeConfigInfo) sapTableRuntimeConfigInfo).getRuntimeTableRecCount());
    Assert.assertEquals(50, sapTableRuntimeConfigInfo.getTotalWorkProcCount());
    Assert.assertEquals(30, sapTableRuntimeConfigInfo.getAvailableWorkProcCount());
    Assert.assertEquals(400000000L, sapTableRuntimeConfigInfo.getWpMaxMemory());
    Assert.assertEquals(2500, ((SapTableRuntimeConfigInfo) sapTableRuntimeConfigInfo).getRecordSize());
  }

  /**
   * Test method for {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface
   * #getSourceData(java.lang.String, java.util.List<String>,
   * java.util.Map<String, String>,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   * @throws ConnectorException
   */
  @Test
  public void testGetSourceDataSuccessful(@Mocked SapRfmExecutionHelper rfmHelper) throws ConnectorException {
    SapFieldMetadata column = new SapColumn("ERZET", null, null, 0, 0, 10, 0, "CHAR", "C", false);
    sapColumnList.add(column);
    SapObjectMetadata objMetadata = new SapObjectMetadata(sapColumnList);

    SapTableRuntimeOutput rtTableInfo = new SapTableRuntimeOutput(objMetadata, jcoTable);

    new Expectations() {
      {
        rfmHelper.getTableRecords(anyString, anyLong, anyLong, null, null);
        result = rtTableInfo;
        minTimes = 0;
      }
    };

    String sapTable = "VBAK";
    operationsProps.put(SapTableRecordReader.ROW_SKIPS, "0");
    operationsProps.put(SapTableRecordReader.ROW_COUNT, "10");
    SapObjectRuntimeOutput sapTableRuntimeOutput = sapTableInterface.getSourceData(sapTable,
      Arrays.asList("TEST_COLUMN = 'Sample Test Value'"), operationsProps, conn);

    Assert.assertEquals("Column Metadata map size is not 1", sapColumnList.size(),
      sapTableRuntimeOutput.getObjectMetadata().getFieldMetadata().size());

    Assert.assertNotNull(((SapTableRuntimeOutput) sapTableRuntimeOutput).getOutputDataTable());
  }

  /**
   * Test method for {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface
   * #getSourceData(java.lang.String, java.util.List<String>,
   * java.util.Map<String, String>,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   * @throws ConnectorException
   */
  @Test
  public void testGetSourceDataRowCountLessThanOneSuccessful() throws ConnectorException {
    String sapTable = "VBAK";
    operationsProps.put(SapTableRecordReader.ROW_SKIPS, "0");
    operationsProps.put(SapTableRecordReader.ROW_COUNT, "0");
    SapObjectRuntimeOutput sapTableRuntimeOutput = sapTableInterface.getSourceData(sapTable,
      Arrays.asList("TEST_COLUMN = 'Sample Test Value'"), operationsProps, conn);

    Assert.assertNull(sapTableRuntimeOutput.getObjectMetadata());
    Assert.assertNull(((SapTableRuntimeOutput) sapTableRuntimeOutput).getOutputDataTable());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceData(java.lang.String,
   * java.util.List<String>, java.util.Map<String, String>,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   * 
   * @throws JCoException
   */
  @Test
  public void testGetSourceDataDataBufferExcdFail() throws JCoException {
    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result =
          new AbapException("DATA_BUFFER_EXCEEDED", "Mocked DATA_BUFFER_EXCEEDED exception for executeFunction()");
      }
    };

    String sapTable = "VBAK";
    try {
      operationsProps.put(SapTableRecordReader.ROW_SKIPS, "0");
      operationsProps.put(SapTableRecordReader.ROW_COUNT, "10");
      sapTableInterface.getSourceData(sapTable, Arrays.asList("TEST_COLUMN = 'Sample Test Value'"), operationsProps,
        conn);

      Assert.fail("SapInterface getTableRecords expected to fail with DATA_BUFFER_EXCEEDED error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals("SapInterface getTableRecords exception code does not match DATA_BUFFER_EXCEEDED",
        ResourceConstants.ERR_DATA_BUFFER_EXCEEDED.getMsgForKeyWithCode(sapTable), e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceData(java.lang.String,
   * java.util.List<String>, java.util.Map<String, Long>,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)}.
   * 
   * @throws JCoException
   */
  @Test
  public void testGetSourceDataSQLFail() throws JCoException {
    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result = new AbapException("SQL_FAILURE", "Mocked SQL_FAILURE exception for executeFunction()");
      }
    };

    String sapTable = "VBAK";
    try {
      operationsProps.put(SapTableRecordReader.ROW_SKIPS, "0");
      operationsProps.put(SapTableRecordReader.ROW_COUNT, "10");
      sapTableInterface.getSourceData(sapTable, Arrays.asList("TEST_COLUMN = 'Sample Test Value'"), operationsProps,
        conn);

      Assert.fail("SapInterface getTableRecords expected to fail with SQL_FAILURE error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals("SapInterface getTableRecords exception code does not match SQL_FAILURE",
        ResourceConstants.ERR_SQL_FAILURE.getMsgForKeyWithCode(sapTable), e.getMessage());
    }

    try {
      operationsProps.put(SapTableRecordReader.ROW_SKIPS, "0");
      operationsProps.put(SapTableRecordReader.ROW_COUNT, "10");
      sapTableInterface.getSourceData(sapTable, Collections.emptyList(), operationsProps, conn);
      Assert.fail("SapInterface getTableRecords expected to fail with SQL_FAILURE error but succeeded.");
    } catch (ConnectorException e) {
      Assert.assertEquals("SapInterface getTableRecords exception code does not match SQL_FAILURE",
        ResourceConstants.ERR_DB_FAILURE.getMsgForKeyWithCode(sapTable), e.getMessage());
    }
  }
}
