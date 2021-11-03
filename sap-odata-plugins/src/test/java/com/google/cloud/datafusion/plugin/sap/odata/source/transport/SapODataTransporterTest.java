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

package com.google.cloud.datafusion.plugin.sap.odata.source.transport;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.cloud.datafusion.plugin.sap.odata.source.TestUtil;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import javax.ws.rs.core.MediaType;


public class SapODataTransporterTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private SapODataPluginConfig.Builder pluginConfigBuilder;
  private SapX509Manager x509Manager;
  private SapODataTransporter transporter;

  private SapODataUrlContainer oDataURL;

  @Before
  public void setUp() {
    pluginConfigBuilder = SapODataPluginConfig.builder()
      .baseURL("http://localhost:" + wireMockRule.port())
      .serviceName("odata/v2")
      .entityName("ODataEntity")
      .username("test")
      .password("secret");

    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    oDataURL = new SapODataUrlContainer(pluginConfig);

    x509Manager = new SapX509Manager(null, null, null);
    transporter = new SapODataTransporter(pluginConfig.getUsername(), pluginConfig.getPassword(), x509Manager);
  }

  @Test
  public void testCallSapOData() throws TransportException {
    String expectedBody = "{\"d\": [{\"ID\": 0,\"Name\": \"Bread\"}}]}";
    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    WireMock.stubFor(WireMock.get("/odata/v2/ODataEntity?%24top=0")
      .withBasicAuth(pluginConfig.getUsername(), pluginConfig.getPassword())
      .willReturn(WireMock.ok()
        .withHeader(SapODataTransporter.SERVICE_VERSION, "2.0")
        .withBody(expectedBody)));

    SapODataResponseContainer response = transporter
      .callSapOData(oDataURL.getTesterURL(), MediaType.APPLICATION_JSON, "TEST");

    Assert.assertEquals("SAP OData Service data version is not same.",
      "2.0",
      response.getDataServiceVersion());

    Assert.assertEquals("HTTP status code is not same.",
      HttpURLConnection.HTTP_OK,
      response.getHttpStatusCode());

    Assert.assertEquals("HTTP response body is not same.",
      expectedBody,
      TestUtil.convertInputStreamToString(response.getResponseStream()));
  }

  @Test
  public void testUnAuthorized() throws TransportException {
    WireMock.stubFor(WireMock.get("/odata/v2/$metadata")
      .willReturn(WireMock.unauthorized()));

    SapODataResponseContainer response = transporter
      .callSapOData(oDataURL.getMetadataURL(), MediaType.APPLICATION_XML, "METADATA");

    WireMock.verify(1, WireMock.getRequestedFor(WireMock.urlEqualTo("/odata/v2/$metadata")));

    Assert.assertEquals("HTTP status code is not matching.",
      HttpURLConnection.HTTP_UNAUTHORIZED,
      response.getHttpStatusCode());
  }

  @Test
  public void testInvalidHost() throws TransportException {
    oDataURL = new SapODataUrlContainer(pluginConfigBuilder.baseURL("http://INVALID-HOST").build());

    exception.expectCause(CoreMatchers.isA(UnknownHostException.class));

    transporter.callSapOData(oDataURL.getMetadataURL(), MediaType.APPLICATION_XML, "METADATA");
  }

  @Test
  public void testConnectionTimeout() throws TransportException {
    WireMock.stubFor(WireMock.get("/odata/v2/ODataEntity?%24top=0")
      .willReturn(WireMock.aResponse()
        .withFixedDelay(11_000)));

    exception.expectMessage(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey("TEST"));
    exception.expectCause(CoreMatchers.isA(SocketTimeoutException.class));

    transporter.callSapOData(oDataURL.getTesterURL(), MediaType.APPLICATION_JSON, "TEST");
  }
}
