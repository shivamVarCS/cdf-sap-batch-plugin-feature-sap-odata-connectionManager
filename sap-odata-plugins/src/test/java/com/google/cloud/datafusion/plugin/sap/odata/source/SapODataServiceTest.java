package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.metadata.SapODataEntityProvider;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataUrlContainer;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

public class SapODataServiceTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

  private SapODataPluginConfig.Builder pluginConfigBuilder;
  private SapX509Manager x509Manager;
  private SapODataTransporter transporter;

  private SapODataUrlContainer oDataURL;

  private SapODataService oDataService;

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
    oDataService = new SapODataService(pluginConfig, transporter);
  }


  @Test
  public void test() throws TransportException, ODataServiceException, EntityProviderException, IOException {

    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    InputStream metadata = TestUtil.readResource("sap-metadata.xml");

    InputStream metaDataStream = metadata;
    byte[] bytes = new byte[metaDataStream.available()];
    metaDataStream.read(bytes);
//      System.out.println(Base64.getEncoder().encodeToString(bytes));
    String str = Base64.getEncoder().encodeToString(bytes);

    byte[] bytes2 = Base64.getDecoder().decode(str);

    Edm data = EntityProvider.readMetadata(new ByteArrayInputStream(bytes2), false);
    new SapODataEntityProvider(data).getEdmMetadata();
  }

  @Test
  public void t1() {
    String str = ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode("errorMsg");
    System.out.println(str);
  }

}
