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

/*
package com.google.cloud.datafusion.plugin.sap.odata.source.config;

import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import mockit.Delegate;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class SapODataPluginConfigTest {

  private MockFailureCollector failureCollector;
  private SapODataPluginConfig.Builder pluginConfigBuilder;

  @Before
  public void setUp() {
    failureCollector = new MockFailureCollector();

    pluginConfigBuilder = SapODataPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("http://localhost")
      .serviceName("service name")
      .entityName("entity name")
      .username("username")
      .password("password");
  }

  @Test
  public void testSchemaBuildRequired() {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();
    Assert.assertTrue(pluginConfig.isSchemaBuildRequired());
  }

  @Test
  public void testSchemaBuildNotRequired() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();
    new Expectations(SapODataPluginConfig.class) {
      {
        pluginConfig.containsMacro(anyString);
        result = new Delegate<String>() {
          @SuppressWarnings("unused")
          boolean containsMacro(String fieldName) {
            return SapODataPluginConfig.BASE_URL.equals(fieldName);
          }
        };
      }
    };

    Assert.assertFalse(pluginConfig.isSchemaBuildRequired());
  }

  @Test
  public void testValidateMandatoryPluginParameters() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .entityName(null)
      .username(null)
      .password(null)
      .build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 3, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Entity Name"),
        failures.get(0).getMessage());
    }
  }

  @Test
  public void testInValidBaseURL() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .baseURL("INVALID-URL")
      .build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey(), failures.get(0).getMessage());
    }
  }

  @Test
  public void testBaseURLWithEmbeddedSpaces() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .baseURL("http://localhost/         /        /        ")
      .build();

    Assert.assertEquals("http://localhost///", pluginConfig.getBaseURL());
  }

  @Test
  public void testInValidGcpCertPath() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .gcpProjectId("project-id")
      .certGcsPath("invalid path")
      .certPassphrase("secret")
      .build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());
      Assert.assertTrue(failures.get(0).getMessage().contains("Invalid bucket name in path 'invalid path'."));
    }
  }

  @Test
  public void testValidGcpCertPathWithNullPassphrase() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .gcpProjectId("project-id")
      .certGcsPath("valid-path")
      .certPassphrase(null)
      .build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Passphrase"),
        failures.get(0).getMessage());
    }
  }

  @Test
  public void testValidateNumberTypePluginParameters() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .splitCount(-21)
      .batchSize(0L)
      .build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX
        .getMsgForKey("Number of Splits to Generate"), failures.get(0).getMessage());
    }
  }

  @Test
  public void testValidateEntityForKeyBasedExtraction() {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder.entityName("Products(1)").build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_FEATURE_NOT_SUPPORTED.getMsgForKey(), failures.get(0).getMessage());
    }
  }

  @Test
  public void testRefactoredPluginPropertyValues() {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .baseURL("  http://localhost:5000   ")
      .entityName("entity-name    ")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ")
      .build();

    Assert.assertEquals("Base URL not trimmed", "http://localhost:5000", pluginConfig.getBaseURL());
    Assert.assertEquals("Entity name not trimmed", "entity-name", pluginConfig.getEntityName());
    Assert.assertEquals("Select option not trimmed", "col1,col2,parent/col1,col3", pluginConfig.getSelectOption());
  }
}
*/
