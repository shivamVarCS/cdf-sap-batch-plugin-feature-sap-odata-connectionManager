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

import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JMockit.class)
public class SapOdpPluginConfigWrapperTest {
  private static Map<String, String> pluginProps;

  @Tested
  private SapOdpPluginConfigWrapper sapOdpPluginConfigWrapper;

  private MockFailureCollector failureCollector;

  @Mocked
  private PluginConfig config;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    pluginProps = new HashMap<>();
    pluginProps.put(Constants.Reference.REFERENCE_NAME, "sapBatchRefName");
    pluginProps.put("client", "003");
    pluginProps.put("lang", "EN");
    pluginProps.put(SapJcoPluginConfigWrapper.CONNECTION_TYPE, SapJcoPluginConfigWrapper.CONN_TYPE_DIRECT_VALUE);
    pluginProps.put("user", "sapUser");
    pluginProps.put("paswd", "sapPaswd");
    pluginProps.put(SapJcoPluginConfigWrapper.GCP_PROJECT_ID, SapJcoPluginConfigWrapper.AUTO_DETECT);
    pluginProps.put(SapJcoPluginConfigWrapper.GCS_PATH, "gs://cdf-sap-files");
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateOptionalPropsSuccessful() throws Exception {
    // No subscriberName, numSplits, packageSize
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No subscriberName, numSplits
    pluginProps.put(SapOdpPluginConfigWrapper.PACKAGE_SIZE, "10000");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No numSplits
    pluginProps.put(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME, "Test_Identifier");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // All suscriberName, numSplits, packageSize set
    pluginProps.put(SapOdpPluginConfigWrapper.NUM_SPLITS, "16");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateOptionalPropsMacroSuccessful() throws Exception {
    // No subscriberName
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.SUBSCRIBER_NAME.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No numSplits
    pluginProps.put(SapOdpPluginConfigWrapper.PACKAGE_SIZE, "85000");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.NUM_SPLITS.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No packageSize
    pluginProps.put(SapOdpPluginConfigWrapper.PACKAGE_SIZE, "0");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.PACKAGE_SIZE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();

    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No subscriberName, packageSize, numSplits
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.PACKAGE_SIZE.equals(fieldName)
              || SapOdpPluginConfigWrapper.NUM_SPLITS.equals(fieldName)
              || SapOdpPluginConfigWrapper.SUBSCRIBER_NAME.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateOptionalPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateOptionalPropsFail() {
    // susbcriberName, numSplits, packageSize
    String subscriberName = "Test_@Name";
    pluginProps.put(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME, subscriberName);
    pluginProps.put(SapOdpPluginConfigWrapper.NUM_SPLITS, "-8");
    pluginProps.put(SapOdpPluginConfigWrapper.PACKAGE_SIZE, "-50000");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateOptionalPropsFail expected to fail with validation error, but succeeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateOptionalPropsFail Failures size does not match", 3, failures.size());

      String goldErr = ResourceConstants.ERR_INVALID_SUBSCRIBER_NAME.getMsgForKey(subscriberName);

      Assert.assertEquals("testValidateOptionalPropsFail subscriberName failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME,
        failures.get(0).getCauses().get(0).getAttribute("stageConfig"));

      goldErr =
        ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.NUM_SPLITS_TO_GENERATE);

      Assert.assertEquals("testValidateOptionalPropsFail numSplits failure does not match", goldErr,
        failures.get(1).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.NUM_SPLITS,
        failures.get(1).getCauses().get(0).getAttribute("stageConfig"));

      goldErr = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.PACKAGE_SIZE_LABEL);

      Assert.assertEquals("testValidateOptionalPropsFail packageSize failure does not match", goldErr,
        failures.get(2).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.PACKAGE_SIZE, failures.get(2).getCauses().get(0).getAttribute(
        "stageConfig"));
    }

    // -ve packageSize & invalid subscriberName
    pluginProps.put(SapOdpPluginConfigWrapper.NUM_SPLITS, "0");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateOptionalPropsFail expected to fail with validation error, but suceeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateOptionalPropsFail Failures size does not match", 2, failures.size());

      String goldErr = ResourceConstants.ERR_INVALID_SUBSCRIBER_NAME.getMsgForKey(subscriberName);

      Assert.assertEquals("testValidateOptionalPropsFail subscriberName failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME, failures.get(0).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.PACKAGE_SIZE_LABEL);

      Assert.assertEquals("testValidateOptionalPropsFail packageSize failure does not match", goldErr,
        failures.get(1).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.PACKAGE_SIZE, failures.get(1).getCauses().get(0).getAttribute(
        "stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateOptionalPropsMacroFail() {
    // invalid subscriberName, -ve numSplits and macro packageSize
    String subscriberName = "Test_@Name";
    pluginProps.put(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME, subscriberName);
    pluginProps.put(SapOdpPluginConfigWrapper.NUM_SPLITS, "-8");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.PACKAGE_SIZE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateOptionalPropsMacroFail expected to fail with validation error, but suceeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateOptionalPropsMacroFail Failures size does not match", 2, failures.size());

      String goldErr = ResourceConstants.ERR_INVALID_SUBSCRIBER_NAME.getMsgForKey(subscriberName);

      Assert.assertEquals("testValidateOptionalPropsFail subscriberName failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME, failures.get(0).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr =
        ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.NUM_SPLITS_TO_GENERATE);

      Assert.assertEquals("testValidateOptionalPropsMacroFail numSplits failure does not match", goldErr,
        failures.get(1).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.NUM_SPLITS, failures.get(1).getCauses().get(0).getAttribute(
        "stageConfig"));
    }

    // -ve packageSize and macro numSplits
    pluginProps.put(SapOdpPluginConfigWrapper.NUM_SPLITS, "0");
    pluginProps.put(SapOdpPluginConfigWrapper.PACKAGE_SIZE, "-1000");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.NUM_SPLITS.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateOptionalProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateOptionalPropsMacroFail expected to fail with validation error, but suceeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateOptionalPropsMacroFail Failures size does not match", 2, failures.size());

      String goldErr = ResourceConstants.ERR_INVALID_SUBSCRIBER_NAME.getMsgForKey(subscriberName);

      Assert.assertEquals("testValidateOptionalPropsFail subscriberName failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME, failures.get(0).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.PACKAGE_SIZE_LABEL);

      Assert.assertEquals("testValidateOptionalPropsMacroFail packageSize failure does not match", goldErr,
        failures.get(1).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.PACKAGE_SIZE, failures.get(1).getCauses().get(0).getAttribute(
        "stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateMandatoryPropsSuccessful(@Mocked SapJcoPluginConfigWrapper sapJcoPluginConfigWrapper)
    throws Exception {

    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "2L_ITS_ITM");
    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_TYPE, SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE);
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);
    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateMandatoryPropsSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateMandatoryPropsMacroSuccessful(@Mocked SapJcoPluginConfigWrapper sapJcoPluginConfigWrapper)
    throws Exception {

    // No sapDataSource
    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_TYPE, SapOdpPluginConfigWrapper.EXTRACT_TYPE_FULL_VALUE);
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
    ValidationException e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateMandatoryPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No sapDataSource extract type
    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_TYPE, null);
    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, "2L_ITS_IM");
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.EXTRACT_TYPE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateMandatoryPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());

    // No sapDataSource and extract type
    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_TYPE, null);
    pluginProps.put(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, null);
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.EXTRACT_TYPE.equals(fieldName)
              || SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
    e = failureCollector.getOrThrowException();
    Assert.assertEquals("testValidateMandatoryPropsMacroSuccessful", "Errors were encountered during validation. ",
      e.getMessage());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateMandatoryPropsFail() {
    // No referenceName, client, language, user, password, sapDatasource, and
    // extractType
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateMandatoryPropsFail expected to fail with validation error, but succeeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateMandatoryPropsFail Failures size does not match", 7, failures.size());

      String goldErr =
        ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.REFERENCE_NAME);

      Assert.assertEquals("testValidateMandatoryPropsFail reference name failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert.assertEquals("referenceName", failures.get(0).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.SAP_CLIENT);

      Assert.assertEquals("testValidateMandatoryPropsFail sapClient failure does not match", goldErr,
        failures.get(1).getMessage());

      Assert.assertEquals(SapJcoPluginConfigWrapper.JCO_CLIENT, failures.get(1).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.SAP_LANGUAGE);

      Assert.assertEquals("testValidateMandatoryPropsFail language failure does not match", goldErr,
        failures.get(2).getMessage());

      Assert.assertEquals(SapJcoPluginConfigWrapper.JCO_LANG, failures.get(2).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.SAP_USERNAME);

      Assert.assertEquals("testValidateMandatoryPropsFail user failure does not match", goldErr,
        failures.get(3).getMessage());

      Assert.assertEquals(SapJcoPluginConfigWrapper.JCO_USER, failures.get(3).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.SAP_PASSWD);

      Assert.assertEquals("testValidateMandatoryPropsFail password failure does not match", goldErr,
        failures.get(4).getMessage());

      Assert.assertEquals(SapJcoPluginConfigWrapper.JCO_PASSWD, failures.get(4).getCauses().get(0).getAttribute(
        "stageConfig"));

      goldErr = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.SAP_SOURCE_NAME);

      Assert.assertEquals("testValidateMandatoryPropsFail sap data source failure does not match", goldErr,
        failures.get(5).getMessage());

      Assert
        .assertEquals(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, failures.get(5).getCauses().get(0).getAttribute(
          "stageConfig"));

      goldErr = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.EXTRACT_TYPE_LABEL);

      Assert.assertEquals("testValidateMandatoryPropsFail extract type failure does not match", goldErr,
        failures.get(6).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.EXTRACT_TYPE, failures.get(6).getCauses().get(0).getAttribute(
        "stageConfig"));
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper
   * #validateOptionalProps(io.cdap.cdap.etl.api.FailureCollector)}.
   */
  @Test
  public void testValidateMandatoryPropsMacroFail(@Mocked SapJcoPluginConfigWrapper sapJcoPluginConfigWrapper) {
    // no sapDataSource
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.EXTRACT_TYPE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateMandatoryPropsMacroFail expected to fail with validation error, but suceeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateOptionalPropsMacroFail Failures size does not match", 1, failures.size());

      String goldErr =
        ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.SAP_SOURCE_NAME);

      Assert.assertEquals("testValidateMandatoryPropsMacroFail sap data source failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert
        .assertEquals(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE, failures.get(0).getCauses().get(0).getAttribute(
          "stageConfig"));
    }

    // no extractType
    sapOdpPluginConfigWrapper = new SapOdpPluginConfigWrapper(pluginProps);

    new Expectations(PluginConfig.class) {
      {
        config.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE.equals(fieldName);
          }
        };
        minTimes = 0;
      }
    };

    failureCollector = new MockFailureCollector();
    try {
      sapOdpPluginConfigWrapper.validateMandatoryProps(failureCollector);
      failureCollector.getOrThrowException();
      Assert.fail("testValidateMandatoryPropsMacroFail expected to fail with validation error, but suceeded");
    } catch (ValidationException e) {
      List<ValidationFailure> failures = e.getFailures();
      Assert.assertEquals("testValidateMandatoryPropsMacroFail Failures size does not match", 1, failures.size());

      String goldErr =
        ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SapOdpPluginConfigWrapper.EXTRACT_TYPE_LABEL);

      Assert.assertEquals("testValidateMandatoryPropsMacroFail packageSize failure does not match", goldErr,
        failures.get(0).getMessage());

      Assert.assertEquals(SapOdpPluginConfigWrapper.EXTRACT_TYPE, failures.get(0).getCauses().get(0).getAttribute(
        "stageConfig"));
    }
  }
}
