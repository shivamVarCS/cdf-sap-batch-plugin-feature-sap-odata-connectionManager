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

package com.google.cloud.datafusion.plugin.sap.connection;

import com.google.cloud.datafusion.plugin.sap.connection.out.SapDestinationDataProvider;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.util.PayloadHelper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;
import com.sap.conn.jco.rt.AbapFunction;
import com.sap.conn.jco.rt.BasicRepository;
import com.sap.conn.jco.rt.DefaultDestinationManager;
import com.sap.conn.jco.rt.RfcDestination;

import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

/**
 * @author sankalpbapat
 *
 */
@RunWith(JMockit.class)
public class SapConnectionTest {

  private static final String COLUMN_METADATA_RFM = "DDIF_FIELDINFO_GET";

  static Map<String, String> connParams;

  SapConnection sapConn;

  @Mocked
  DestinationDataEventListener eventListener;

  @Mocked
  DefaultDestinationManager jcoDestMgr;

  @Mocked
  RfcDestination jcoDest;

  @Mocked
  BasicRepository jcoRepo;

  @Mocked
  AbapFunction jcoFunction;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    connParams = PayloadHelper.loadProperties("connection/connParams_direct.properties");
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
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
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    SapDestinationDataProvider destDataProv = SapDestinationDataProvider.getInstance();
    destDataProv.setDestinationDataEventListener(eventListener);
    destDataProv.removeDefinition(sapConn.getDestName());
    Environment.unregisterDestinationDataProvider(destDataProv);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#SapConnection(java.util.Map)}.
   */
  @Test
  public void testSapConnectionSuccessfulWODestReg() {
    new Expectations(Environment.class) {
      {
        Environment.isDestinationDataProviderRegistered();
        result = true;
        minTimes = 0;
      }
    };

    sapConn = new SapConnection(connParams);
    Assert.assertNotNull("SapConnection instance is null", sapConn);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#SapConnection(java.util.Map)}.
   * 
   * @throws JCoException
   */
  @Test
  public void testInitDestinationFail() throws JCoException {
    String getDestErrCode = "JCO_ERROR_RESOURCE";
    String getErrDestMsg = "Mocked JCO_ERROR_RESOURCE exception for getDestination()";

    new Expectations(jcoDestMgr) {
      {
        JCoDestinationManager.getDestination(anyString);
        result = new JCoException(106, getDestErrCode, getErrDestMsg);
        minTimes = 0;
      }
    };

    sapConn = new SapConnection(connParams);
    try {
      sapConn.initDestination();
      Assert.fail("testInitDestinationFail expected to fail but succeeded");
    } catch (Throwable e) {
      String errMsg = ResourceConstants.ERR_GET_DEST_FROM_MGR.getMsgForKeyWithCode(COLUMN_METADATA_RFM) + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + getDestErrCode + " - " + getErrDestMsg;

      Assert.assertEquals("testGetJCoFunctionFail error message does not match", errMsg, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#ping()}.
   */
  @Test
  public void testPingSuccessful() {
    sapConn = new SapConnection(connParams);
    try {
      sapConn.initDestination();
      sapConn.ping();
    } catch (Exception e) {
      Assert.fail("SAPConnection ping failed");
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#getConnParam(java.lang.String)}.
   */
  @Test
  public void testGetConnParamBlank() {
    sapConn = new SapConnection(connParams);
    final String actualParam = sapConn.getConnParam(null);

    Assert.assertNull(actualParam);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#getConnParam(java.lang.String)}.
   */
  @Test
  public void testGetConnParamPaswd() {
    sapConn = new SapConnection(connParams);
    final String actualVal = sapConn.getConnParam(DestinationDataProvider.JCO_PASSWD);
    final String goldVal = "<secret>";

    Assert.assertEquals(goldVal, actualVal);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#getConnParam(java.lang.String)}.
   */
  @Test
  public void testGetConnParamClient() {
    sapConn = new SapConnection(connParams);
    final String actualVal = sapConn.getConnParam(DestinationDataProvider.JCO_CLIENT);
    final String goldVal = (String) connParams.get(DestinationDataProvider.JCO_CLIENT);

    Assert.assertEquals(goldVal, actualVal);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#refreshDestination(boolean)}.
   */
  @Test
  public void testRefreshDestinationSuccessful() {
    new Expectations() {
      {
        jcoDest.isValid();
        result = true;
        minTimes = 0;
      }
    };

    sapConn = new SapConnection(connParams);

    try {
      sapConn.initDestination();
      sapConn.refreshDestination(true);
    } catch (ConnectorException e) {
      Assert.fail("JCoDestination refresh failed");
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#getJCoFunction(java.lang.String)}.
   */
  @Test
  public void testGetJCoFunctionSuccessful() {
    sapConn = new SapConnection(connParams);
    JCoFunction jcoFunc = null;
    try {
      sapConn.initDestination();
      jcoFunc = sapConn.getJCoFunction(COLUMN_METADATA_RFM);
    } catch (ConnectorException e) {
      Assert.fail("JCoFunction could NOT be instantiated");
    }

    if (jcoFunc == null) {
      Assert.fail("JCoFunction instance is null");
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection#getJCoFunction(java.lang.String)}.
   * 
   * @throws JCoException
   */
  @Test
  public void testGetJCoFunctionFail() throws JCoException {
    int getFuncErrCode = 102;
    String getFuncErrMsg = "Mocked exception for get JCoFunction. " + COLUMN_METADATA_RFM + " is not defined in SAP";

    new Expectations() {
      {
        jcoRepo.getFunction(anyString);
        result = new JCoException(getFuncErrCode, getFuncErrMsg);
        minTimes = 0;
      }
    };

    sapConn = new SapConnection(connParams);
    try {
      sapConn.initDestination();
      sapConn.getJCoFunction(COLUMN_METADATA_RFM);
      Assert.fail("testGetJCoFunctionFail expected to fail due to but passed");
    } catch (ConnectorException e) {
      String errMsg = ResourceConstants.ERR_GET_FUNC_FROM_REPO.getMsgForKeyWithCode(COLUMN_METADATA_RFM) + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + "JCO_ERROR_COMMUNICATION" + " - " + getFuncErrMsg;

      Assert.assertEquals("testGetJCoFunctionFail error message does not match", errMsg, e.getMessage());
    }

    int getRepoErrCode = 150;
    String getRepoErrMsg = "Mocked exception for allocate repository from destination";

    new Expectations() {
      {
        jcoDest.getRepository();
        result = new JCoException(getRepoErrCode, getRepoErrMsg);
        minTimes = 0;
      }
    };

    try {
      sapConn.getJCoFunction(COLUMN_METADATA_RFM);
      Assert.fail("Get JCoFunction expected to fail due to repository allocation but passed");
    } catch (ConnectorException e) {
      String errMsg = ResourceConstants.ERR_GET_REPO_FROM_DEST.getMsgForKeyWithCode() + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + "JCO_ERROR_DSR_LOAD_ERROR" + " - " + getRepoErrMsg;

      Assert.assertEquals("testGetJCoFunctionFail error message does not match", errMsg, e.getMessage());
    }

    String jcoErrCode = "JCO_ERROR_RESOURCE";
    String jcoErrMsg = "Mocked JCO_ERROR_RESOURCE exception for getDestination()";

    new Expectations(JCoDestinationManager.class) {
      {
        JCoDestinationManager.getDestination(anyString);
        result = new JCoException(106, jcoErrCode, jcoErrMsg);
        minTimes = 0;
      }
    };

    try {
      sapConn.getJCoFunction(COLUMN_METADATA_RFM);
      Assert.fail("Get JCoFunction expected to fail due to destination undefined but passed");
    } catch (ConnectorException e) {
      String errMsg = ResourceConstants.ERR_GET_DEST_FROM_MGR.getMsgForKeyWithCode() + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + jcoErrCode + " - " + jcoErrMsg;

      Assert.assertEquals("testGetJCoFunctionFail error message does not match", errMsg, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection
   * #executeFunction(com.sap.conn.jco.JCoFunction)}.
   */
  @Test
  public void testExecuteFunctionSuccessful() {
    sapConn = new SapConnection(connParams);
    try {
      sapConn.initDestination();
      sapConn.executeFunction(jcoFunction);
    } catch (Exception e) {
      Assert.fail("JCoFunction execute expected to succeed but failed with error: " + e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapConnection
   * #executeFunction(com.sap.conn.jco.JCoFunction)}.
   * 
   * @throws JCoException
   */
  @Test
  public void testExecuteFunctionFail() throws JCoException {
    String jcoErrCode = "JCO_ERROR_RESOURCE";
    String jcoErrMsg = "Mocked JCO_ERROR_RESOURCE exception for getDestination()";

    new Expectations() {
      {
        jcoFunction.execute((JCoDestination) any);
        result = new JCoException(106, jcoErrCode, jcoErrMsg);
        minTimes = 0;
      }
    };

    sapConn = new SapConnection(connParams);
    try {
      sapConn.initDestination();
      sapConn.executeFunction(jcoFunction);
      Assert.fail("testExecuteFunctionFail expected to fail but passed");
    } catch (JCoException e) {
      Assert.assertEquals("testConfigurePipelineWPingFail exception key does not match", jcoErrCode, e.getKey());
      Assert.assertEquals("testConfigurePipelineWPingFail exception message does not match", jcoErrMsg, e.getMessage());
    } catch (ConnectorException e) {
      Assert.fail("testConfigurePipelineWPingFail expected to fail with JCoException, but failed with error: "
        + e.getMessage());
    }
  }
}
