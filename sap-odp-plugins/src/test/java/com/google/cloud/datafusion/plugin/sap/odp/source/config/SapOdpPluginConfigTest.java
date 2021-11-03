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
package com.google.cloud.datafusion.plugin.sap.odp.source.config;

import com.google.cloud.ServiceOptions;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasourceField;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JMockit.class)
public class SapOdpPluginConfigTest {
  private static Map<String, String> pluginConfigProps;

  private static List<SapFieldMetadata> sapOdpDatasourceList;

  private SapOdpPluginConfig sapOdpPluginConfig;

  private MockFailureCollector failureCollector;

  @Mocked
  private PluginConfig pluginConfig;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    pluginConfigProps = new HashMap<>();
    pluginConfigProps.put(Constants.Reference.REFERENCE_NAME, "sapBatchRefName");
    pluginConfigProps.put("client", "003");
    pluginConfigProps.put("lang", "EN");
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE, SapOdpPluginConfigWrapper.CONN_TYPE_DIRECT_VALUE);
    pluginConfigProps.put("user", "sapUser");
    pluginConfigProps.put("paswd", "sapPaswd");
    pluginConfigProps.put(SapOdpPluginConfigWrapper.GCP_PROJECT_ID, SapOdpPluginConfigWrapper.AUTO_DETECT);
    pluginConfigProps.put(SapOdpPluginConfigWrapper.GCS_PATH, "gs://cdf-sap-files");

    sapOdpDatasourceList = new ArrayList<>();

    SapFieldMetadata sapFieldMetadataObj1 = new SapOdpDatasourceField("TEST_COLUMN1", "Net Value", "Net transfer",
      Integer.parseInt("000006"), Integer.parseInt("000000"), "Char", "C", true, true, true);

    SapFieldMetadata sapFieldMetadataObj2 = new SapOdpDatasourceField("TEST_COLUMN2", "Net Value", "Net transfer",
      Integer.parseInt("000006"), Integer.parseInt("000000"), "Char", "C", true, true, true);

    sapOdpDatasourceList.add(sapFieldMetadataObj1);
    sapOdpDatasourceList.add(sapFieldMetadataObj2);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsEqSuccessful() throws Exception {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE,
      "TEST_COLUMN1:'Sample Value1'," + "TEST_COLUMN2:'Sample Value2'");

    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);
    failureCollector = new MockFailureCollector();
    sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateFilterOptionsEqSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsEqFail() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "2L_ITS");
    String testColumn = "TEST_COLUMN";
    pluginConfigProps.put(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE,
      testColumn + ":'Sample " + "Value1',TEST_COLUMN2:'Sample Value2'");

    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);
    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
      ValidationException e = failureCollector.getOrThrowException();
      Assert.assertEquals("testValidateFilterOptionsEqFail", "Errors were encountered during validation. ",
        e.getMessage());
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());

      String goldErr = ResourceConstants.ERR_FILTER_EQ_FIELD.getMsgForKey(testColumn,
        pluginConfigProps.get(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE));

      Assert.assertEquals("Error message for filterOptionsEq does not match", goldErr, failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE,
        failures.get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsBwSuccessful() throws Exception {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE,
      "TEST_COLUMN1:'Sample Val1' AND " + "'Sample Val2',TEST_COLUMN2:'Sample Val3' AND 'Sample Val4'");

    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);
    failureCollector = new MockFailureCollector();
    sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateFilterOptionsBwSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsBwFail() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "2L_ITS");
    String testColumn = "TEST_COLUMN";
    pluginConfigProps.put(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE,
      testColumn + ":'Sample Val1' AND " + "'Sample Val2',TEST_COLUMN2:'Sample Val3' AND 'Sample Val4'");

    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);
    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
      ValidationException e = failureCollector.getOrThrowException();
      Assert.assertEquals("testValidateFilterOptionsBwFail", "Errors were encountered during validation. ",
        e.getMessage());
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());

      String goldErr = ResourceConstants.ERR_FILTER_RANGE_FIELD.getMsgForKey(testColumn,
        pluginConfigProps.get(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE));

      Assert.assertEquals("Error message for filterOptionsRange does not match", goldErr, failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE,
        failures.get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsEqMacroSuccessful() throws Exception {
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateFilterOptionsEqMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsBwMacroSuccessful() throws Exception {
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateFilterOptionsBwMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsEqMacroFail() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "2L_ITS");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);
    String testColumn = "TEST_COLUMN";

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
      ValidationException e = failureCollector.getOrThrowException();
      Assert.assertEquals("testValidateFilterOptionsEqMacroFail", "Errors were encountered during validation. ",
        e.getMessage());
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());

      String goldErr = ResourceConstants.ERR_FILTER_EQ_FIELD.getMsgForKey(testColumn,
        pluginConfigProps.get(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE));

      Assert.assertEquals("Error message for filterOptionsEq does not match", goldErr, failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE,
        failures.get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig
   * #validateFilterOptions(FailureCollector, List)}.
   */
  @Test
  public void testValidateFilterOptionsBwMacroFail() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "2L_ITS");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);
    String testColumn = "TEST_COLUMN";

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfig.validateFilterOptions(failureCollector, sapOdpDatasourceList);
      ValidationException e = failureCollector.getOrThrowException();
      Assert.assertEquals("testValidateFilterOptionsBwMacroFail", "Errors were encountered during validation. ",
        e.getMessage());
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());

      String goldErr = ResourceConstants.ERR_FILTER_RANGE_FIELD.getMsgForKey(testColumn,
        pluginConfigProps.get(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE));

      Assert.assertEquals("Error message for filterOptionsEq does not match", goldErr, failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE,
        failures.get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#getProject()}.
   */
  @Test
  public void testGetProjectAutoDetectSuccessful() throws Exception {
    // Set GcpProjectId as auto-detect
    pluginConfigProps.put(SapOdpPluginConfigWrapper.GCP_PROJECT_ID, SapOdpPluginConfigWrapper.AUTO_DETECT);
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(ServiceOptions.class) {
      {
        ServiceOptions.getDefaultProjectId();
        result = "mockDefaultGCPProjectId";
        minTimes = 0;
      }
    };

    String actualResult = sapOdpPluginConfig.getProject();
    Assert.assertEquals("mockDefaultGCPProjectId", actualResult);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#getProject()}.
   */
  @Test
  public void testGetProjectFail() {
    // No GcpProjectId
    pluginConfigProps.put(SapOdpPluginConfigWrapper.GCP_PROJECT_ID, null);
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(ServiceOptions.class) {
      {
        ServiceOptions.getDefaultProjectId();
        result = "mockDefaultGCPProjectId";
        minTimes = 0;
      }
    };

    try {
      sapOdpPluginConfig.getProject();
      Assert.fail("testValidateGcpProjectFail expected to fail with validation error, but succeeded");
    } catch (Exception e) {
      Assert.assertEquals(
        "Could not detect Google Cloud Project ID from the environment." + " Please specify a Project ID.",
        e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#getConnPropsByType()}.
   */
  @Test
  public void testGetConnPropsByTypeDirect() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE, SapOdpPluginConfigWrapper.CONN_TYPE_DIRECT_VALUE);
    // Mandatory direct connection params
    pluginConfigProps.put("ashost", "10.10.10.10");
    pluginConfigProps.put("sysnr", "09");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    Map<String, String> actualResult = sapOdpPluginConfig.getConnPropsByType();
    Assert.assertEquals("testGetConnPropsByTypeDirect client does not match", pluginConfigProps.get("client"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_CLIENT));

    Assert.assertEquals("testGetConnPropsByTypeDirect lang does not match", pluginConfigProps.get("lang"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_LANG));

    Assert.assertEquals("testGetConnPropsByTypeDirect user does not match", pluginConfigProps.get("user"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_USER));

    Assert.assertEquals("testGetConnPropsByTypeDirect paswd does not match", pluginConfigProps.get("paswd"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_PASSWD));

    Assert.assertEquals("testGetConnPropsByTypeDirect ashost does not match", pluginConfigProps.get("ashost"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_ASHOST));

    Assert.assertEquals("testGetConnPropsByTypeDirect sysnr does not match", pluginConfigProps.get("sysnr"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_SYSNR));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#getConnPropsByType()}.
   */
  @Test
  public void testGetConnPropsByTypeLoadBalanced() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE,
      SapOdpPluginConfigWrapper.CONN_TYPE_LOAD_BALANCED_VALUE);

    // Mandatory load balanced connection params
    pluginConfigProps.put("mshost", "gcp.sap-mshost.com");
    pluginConfigProps.put("msserv", "sapms09");
    pluginConfigProps.put("r3name", "SAPEHP8");
    pluginConfigProps.put("group", "PUBLIC");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    Map<String, String> actualResult = sapOdpPluginConfig.getConnPropsByType();
    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced client does not match", pluginConfigProps.get("client"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_CLIENT));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced lang does not match", pluginConfigProps.get("lang"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_LANG));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced user does not match", pluginConfigProps.get("user"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_USER));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced paswd does not match", pluginConfigProps.get("paswd"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_PASSWD));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced mshost does not match", pluginConfigProps.get("mshost"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_MSHOST));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced msserv does not match", pluginConfigProps.get("msserv"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_MSSERV));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced r3name does not match", pluginConfigProps.get("r3name"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_R3NAME));

    Assert.assertEquals("testGetConnPropsByTypeLoadBalanced group does not match", pluginConfigProps.get("group"),
      actualResult.get(SapOdpPluginConfigWrapper.JCO_GROUP));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#isConnectionReqd()}.
   */
  @Test
  public void testIsConnectionReqdDirect() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE, SapOdpPluginConfigWrapper.CONN_TYPE_DIRECT_VALUE);
    // Set ASHost, SysNr
    pluginConfigProps.put("ashost", "10.10.10.10");
    pluginConfigProps.put("sysnr", "09");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    Assert.assertTrue(sapOdpPluginConfig.isConnectionReqd());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#isConnectionReqd()}.
   */
  @Test
  public void testIsConnectionReqdDirectMacro() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE, SapOdpPluginConfigWrapper.CONN_TYPE_DIRECT_VALUE);
    // SysNr macro
    pluginConfigProps.put("ashost", "10.10.10.10");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.JCO_SYSNR.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    Assert.assertFalse(sapOdpPluginConfig.isConnectionReqd());

    // SysNr and optional param SapRouter macro
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.JCO_SYSNR.equals(fieldName)
              || SapOdpPluginConfigWrapper.JCO_SAPROUTER.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    Assert.assertFalse(sapOdpPluginConfig.isConnectionReqd());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#isConnectionReqd()}.
   */
  @Test
  public void testIsConnectionReqdLoadBalanced() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE,
      SapOdpPluginConfigWrapper.CONN_TYPE_LOAD_BALANCED_VALUE);

    // Mandatory load balanced connection params
    pluginConfigProps.put("mshost", "gcp.sap-mshost.com");
    pluginConfigProps.put("msserv", "sapms09");
    pluginConfigProps.put("r3name", "SAPEHP8");
    pluginConfigProps.put("group", "PUBLIC");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    Assert.assertTrue(sapOdpPluginConfig.isConnectionReqd());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig#isConnectionReqd()}.
   */
  @Test
  public void testIsConnectionReqdLoadBalancedMacro() {
    pluginConfigProps.put(SapOdpPluginConfigWrapper.CONNECTION_TYPE,
      SapOdpPluginConfigWrapper.CONN_TYPE_LOAD_BALANCED_VALUE);

    // MSServ, R3Name, Group macro
    pluginConfigProps.put("mshost", "gcp.sap-mshost.com");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.JCO_MSSERV.equals(fieldName)
              || SapOdpPluginConfigWrapper.JCO_R3NAME.equals(fieldName)
              || SapOdpPluginConfigWrapper.JCO_GROUP.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    Assert.assertFalse(sapOdpPluginConfig.isConnectionReqd());

    // R3Name, Group macro
    pluginConfigProps.put("mshost", "gcp.sap-mshost.com");
    pluginConfigProps.put("msserv", "sapms09");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.JCO_R3NAME.equals(fieldName)
              || SapOdpPluginConfigWrapper.JCO_GROUP.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    Assert.assertFalse(sapOdpPluginConfig.isConnectionReqd());

    // Group macro
    pluginConfigProps.put("mshost", "gcp.sap-mshost.com");
    pluginConfigProps.put("msserv", "sapms09");
    pluginConfigProps.put("r3name", "SAPEH8");
    sapOdpPluginConfig = new SapOdpPluginConfig(pluginConfig, pluginConfigProps);

    new Expectations(SapOdpPluginConfig.class) {
      {
        sapOdpPluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.JCO_GROUP.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    Assert.assertFalse(sapOdpPluginConfig.isConnectionReqd());
  }
}
