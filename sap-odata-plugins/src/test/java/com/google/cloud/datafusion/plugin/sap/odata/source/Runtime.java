package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataUrlContainer;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;

public class Runtime {

  @Test
  public void test() throws IOException {
    Path localTempDirPath = Files.createTempDirectory("_sap.cert");
    Path path = Paths.get(localTempDirPath.toString(), UUID.randomUUID().toString());

    System.out.println(path);
    Files.createFile(path);

    try (Stream<Path> pathStream = Files.walk(path.getParent())) {
      pathStream.sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .map(File::delete)
        .forEach(deleteFlag -> {
          if (deleteFlag) {
            System.out.println("Unable to delete downloaded content at path '{}'");
          }
        });
    }
  }

//  @Test
//  public void test2() {
//    String error = " {\n" +
//      "    \"error\": {\n" +
//      "      \"code\": \"005056A509B11EE1B9A8FEC11C21D78E\",\n" +
//      "      \"message\": {\n" +
//      "        \"lang\": \"en\",\n" +
//      "        \"value\": \"Resource not found for the segment 'Address2'.\"\n" +
//      "      },\n" +
//      "      \"innererror\": {\n" +
//      "        \"transactionid\": \"C83CB3D2A1420000E00609D31E196BD4\",\n" +
//      "        \"timestamp\": \"20210524082515.9921880\",\n" +
//      "        \"Error_Resolution\": {\n" +
//      "          \"SAP_Transaction\": \"For backend administrators: use ADT feed reader
//      \\\"SAP Gateway Error Log\\\"\n" +
//      "          or run transaction /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries
//      with the timestamp\n" +
//      "          above for more details\",\n" +
//      "          \"SAP_Note\": \"See SAP Note 1797736 for error analysis
//      (https://service.sap.com/sap/support/notes/1797736)\"\n" +
//      "        }\n" +
//      "      }\n" +
//      "    }\n" +
//      "  }";
//
//    SapODataResponseContainer container = SapODataResponseContainer.builder()
//      .dataServiceVersion("2.0")
//      .httpStatusCode(404)
//      .responseStream(()->new ByteArrayInputStream(error.getBytes()))
//      .build();
//
//    try {
//      ExceptionParser.checkAndThrowException("test", container);
//    } catch (ODataServiceException e) {
//      System.out.println(e.getODataError().getError().getMessage().getValue());
//    }
//  }

  @Test
  public void test3() {
    String str = "<html><head></html>";

    System.out.println(str.startsWith("<html>"));
  }

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

  @Test
  public void test4() {

    SapODataPluginConfig.Builder pluginConfigBuilder = SapODataPluginConfig.builder()
      .baseURL("http://localhost:" + wireMockRule.port())
      .serviceName("odata/v2")
      .entityName("ODataEntity");
      //.username("test")
      //.password("secret");

    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    SapODataUrlContainer oDataURL = new SapODataUrlContainer(pluginConfig);

    SapX509Manager x509Manager = new SapX509Manager(null, null, null);
    SapODataTransporter transporter = new SapODataTransporter(pluginConfig.getUsername(), pluginConfig.getPassword(),
      x509Manager);

    SapODataService s = new SapODataService(pluginConfig, transporter);

    String str = "<html><head></html>";

    WireMock.stubFor(WireMock.get("/odata/v2/ODataEntity?%24top=0")
      .willReturn(WireMock.notFound()
        .withHeader(SapODataTransporter.SERVICE_VERSION, "2.0")
        .withBody(str)));

    try {
      s.checkODataURL();
    } catch (TransportException | ODataServiceException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void cleanUpResource() throws IOException {
    Path localTempDirPath = Files.createTempDirectory("_sap.cert");
    Path path = Paths.get(localTempDirPath.toString(), UUID.randomUUID().toString());
//
//    Path p = Files.createFile(path);
//    Path path = Paths.get("/Users/anupksingh/projects/SAP Plugins/cs/odata/cdf-sap-batch-plugin/sap-odata-plugins" +
//      "/target/_sap.cert3875887781028760335/7c4681d3-320f-4adc-8437-b493e9a3baea");
    try (Stream<Path> pathStream = Files.walk(path.getParent())) {
      pathStream
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    System.out.println(path);
  }
}
