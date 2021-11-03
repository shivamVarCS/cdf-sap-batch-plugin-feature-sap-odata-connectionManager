package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.connector.SAPODataConnectorConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import mockit.Delegate;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SapODataBatchSourceTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());
    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private SapODataPluginConfig.Builder pluginConfigBuilder;
    private SapODataBatchSource sapODataBatchSource;

    private MockPipelineConfigurer pipelineConfigurer;

      @Before
      public void setUp() {
       pluginConfigBuilder = SapODataPluginConfig.builder()
         .referenceName("REF001")
          .baseURL("http://localhost:" + wireMockRule.port())
          .serviceName("odata/v2")
          .entityName("C_GLAccountHierarchyNode")
          .username("test")
          .password("secret");


        sapODataBatchSource = new SapODataBatchSource(pluginConfigBuilder.build());
        pipelineConfigurer = new MockPipelineConfigurer(null);
      }


    @Test
    public void testConfigurePipelineWithValidSchema() {
        String expectedBody = (TestUtil.convertInputStreamToString(TestUtil.readResource("sap-metadata.xml")));

        SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

        WireMock.stubFor(WireMock.get("/odata/v2/C_GLAccountHierarchyNode?%24top=0")
                .withBasicAuth(pluginConfig.getUsername(), pluginConfig.getPassword())
                .willReturn(WireMock.ok()
                        .withHeader(SapODataTransporter.SERVICE_VERSION, "2.0")));

        WireMock.stubFor(WireMock.get("/odata/v2/$metadata")
                .withBasicAuth(pluginConfig.getUsername(), pluginConfig.getPassword())
                .willReturn(WireMock.ok()
                        .withHeader(SapODataTransporter.SERVICE_VERSION, "2.0")
                        .withBody(expectedBody)));


        Assert.assertNull(pipelineConfigurer.getOutputSchema());
        sapODataBatchSource.configurePipeline(pipelineConfigurer);

        Assert.assertNotNull("Plugin output schema is null", pipelineConfigurer.getOutputSchema());
    }

    @Test
    public void testConfigurePipelineForNonSupportedVersion() {
        SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

        WireMock.stubFor(WireMock.get("/odata/v2/C_GLAccountHierarchyNode?%24top=0")
                .withBasicAuth(pluginConfig.getUsername(), pluginConfig.getPassword())
                .willReturn(WireMock.ok()
                        .withHeader(SapODataTransporter.SERVICE_VERSION, "1.0")));

        exceptionRule.expect(ValidationException.class);
        exceptionRule.expectMessage(ResourceConstants.ERR_UNSUPPORTED_VERSION.getMsgForKey("1.0", "2.0"));

        sapODataBatchSource.configurePipeline(pipelineConfigurer);
    }

    @Test
    public void testConfigurePipelineWithMacros() {
        SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

        new Expectations(SapODataPluginConfig.class) {
            {
                pluginConfig.containsMacro(anyString);
                result = new Delegate<String>() {
                    @SuppressWarnings("unused")
                    boolean containsMacro(String fieldName) {
                        return SAPODataConnectorConfig.BASE_URL.equals(fieldName);
                    }
                };
            }
        };

        sapODataBatchSource.configurePipeline(pipelineConfigurer);

        Assert.assertNull("Plugin output schema is not null.", pipelineConfigurer.getOutputSchema());
    }
}
