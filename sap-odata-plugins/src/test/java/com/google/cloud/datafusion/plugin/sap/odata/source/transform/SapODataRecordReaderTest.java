package com.google.cloud.datafusion.plugin.sap.odata.source.transform;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.cloud.datafusion.plugin.sap.odata.source.TestUtil;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.input.SapODataInputSplit;
import com.google.cloud.datafusion.plugin.sap.odata.source.input.SapODataPartitionBuilder;
import com.google.cloud.datafusion.plugin.sap.odata.source.metadata.SapODataEntityProvider;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MediaType;

import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

public class SapODataRecordReaderTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());
  @Rule
  public final ExpectedException exceptionRule = ExpectedException.none();

  private SapODataPluginConfig.Builder pluginConfigBuilder;
  private Schema pluginSchema;

  private String encodedMetadataString;

  @Before
  public void setUp() throws Exception {
    pluginConfigBuilder = SapODataPluginConfig.builder()
      .baseURL("http://localhost:" + wireMockRule.port())
      .serviceName("odata/v2")
      .entityName("C_GLAccountHierarchyNode")
      .username("test")
      .password("secret")
      .numRowsToFetch(0L)
      .skipRowCount(0L)
      .splitCount(0)
      .batchSize(0L);

    String metadataString = TestUtil.convertInputStreamToString(TestUtil.readResource("sap-metadata.xml"));
    encodedMetadataString = Base64.getEncoder().encodeToString(metadataString.getBytes(StandardCharsets.UTF_8));

    pluginSchema = getPluginSchema();
  }

  @Test
  public void runPipelineWithDefaultValues() throws IOException, InterruptedException {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    long availableRowCount = 10;

    List<SapODataInputSplit> partitionList = new SapODataPartitionBuilder().buildSplit(availableRowCount,
      pluginConfig.getNumRowsToFetch(), pluginConfig.getSkipRowCount(), pluginConfig.getSplitCount(),
      pluginConfig.getBatchSize());

    for (SapODataInputSplit inputSplit : partitionList) {
      prepareStubForRun(pluginConfig);

      SapODataRecordReader sapODataRecordReader = new SapODataRecordReader(pluginConfig, pluginSchema,
        encodedMetadataString, null, inputSplit.getStart(), inputSplit.getEnd(), inputSplit.getPackageSize());

      sapODataRecordReader.initialize(null, null);

      List<StructuredRecord> recordList = new ArrayList<>();
      while (sapODataRecordReader.nextKeyValue()) {
        recordList.add(sapODataRecordReader.getCurrentValue());
      }

      long expectedRecordsToPull = (inputSplit.getEnd() - inputSplit.getStart()) + 1;
      String msg = String.format("Total record count for split (start: %s & end: %s) is not matching",
        inputSplit.getStart(), inputSplit.getEnd());

      Assert.assertEquals(msg, expectedRecordsToPull, recordList.size());

      int expectedNetworkCallCount = (int) (expectedRecordsToPull / inputSplit.getPackageSize());

      System.out.println(expectedRecordsToPull);
      System.out.println(expectedNetworkCallCount);

      verify(expectedNetworkCallCount, getRequestedFor(WireMock.urlPathMatching("/odata/v2" +
        "/C_GLAccountHierarchyNode(\\?.*)?")));
    }

  }

  @Test
  public void verifyDataExtractionError() throws IOException {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    WireMock.stubFor(WireMock.get(WireMock.urlPathMatching("/odata/v2/C_GLAccountHierarchyNode(\\?.*)?"))
      .withBasicAuth(pluginConfig.getUsername(), pluginConfig.getPassword())
      .willReturn(WireMock.notFound()
        .withHeader(SapODataTransporter.SERVICE_VERSION, "2.0")
        .withBody("Provided entity name not found.")));

    long availableRowCount = 10;

    exceptionRule.expect(IOException.class);
    exceptionRule.expectMessage("CDF_SAP_ODATA_01536 - Failed to pull records from 'C_GLAccountHierarchyNode' " +
      "for given split indexes (skip: '0' & top: '10')");

    List<SapODataInputSplit> partitionList = new SapODataPartitionBuilder().buildSplit(availableRowCount,
      pluginConfig.getNumRowsToFetch(), pluginConfig.getSkipRowCount(), pluginConfig.getSplitCount(),
      pluginConfig.getBatchSize());

    for (SapODataInputSplit inputSplit : partitionList) {
      SapODataRecordReader sapODataRecordReader = new SapODataRecordReader(pluginConfig, pluginSchema,
        encodedMetadataString, null, inputSplit.getStart(), inputSplit.getEnd(), inputSplit.getPackageSize());

      sapODataRecordReader.initialize(null, null);
    }

  }

  @Test
  public void verifyFailToDecodeMetadataString() throws IOException {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    long availableRowCount = 10;

    exceptionRule.expect(IOException.class);
    exceptionRule.expectMessage("Unable to convert encoded entity metadata string of " +
      "'C_GLAccountHierarchyNode' to actual EDM type.");

    List<SapODataInputSplit> partitionList = new SapODataPartitionBuilder().buildSplit(availableRowCount,
      pluginConfig.getNumRowsToFetch(), pluginConfig.getSkipRowCount(), pluginConfig.getSplitCount(),
      pluginConfig.getBatchSize());

    for (SapODataInputSplit inputSplit : partitionList) {
      SapODataRecordReader sapODataRecordReader = new SapODataRecordReader(pluginConfig, pluginSchema,
        "encodedMetadataString", null, inputSplit.getStart(), inputSplit.getEnd(), inputSplit.getPackageSize());

      sapODataRecordReader.initialize(null, null);
    }

  }

  @Test
  public void verifyTotalRecordFetchAndNetworkCalls() throws IOException, InterruptedException {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder
      .splitCount(10)
      .batchSize(10L)
      .build();

    long availableRowCount = 200;

    List<SapODataInputSplit> partitionList = new SapODataPartitionBuilder().buildSplit(availableRowCount,
      pluginConfig.getNumRowsToFetch(), pluginConfig.getSkipRowCount(), pluginConfig.getSplitCount(),
      pluginConfig.getBatchSize());


    for (SapODataInputSplit inputSplit : partitionList) {
      prepareStubForRun(pluginConfig);

      SapODataRecordReader sapODataRecordReader = new SapODataRecordReader(pluginConfig, pluginSchema,
        encodedMetadataString, null, inputSplit.getStart(), inputSplit.getEnd(), inputSplit.getPackageSize());

      sapODataRecordReader.initialize(null, null);

      List<StructuredRecord> recordList = new ArrayList<>();
      while (sapODataRecordReader.nextKeyValue()) {
        recordList.add(sapODataRecordReader.getCurrentValue());
      }

      long expectedRecordsToPull = (inputSplit.getEnd() - inputSplit.getStart()) + 1;
      String msg = String.format("Total record count for split (start: %s & end: %s) is not matching",
        inputSplit.getStart(), inputSplit.getEnd());

      System.out.println(msg);

      Assert.assertEquals(msg, expectedRecordsToPull, recordList.size());

      int expectedNetworkCallCount = (int) (expectedRecordsToPull / inputSplit.getPackageSize());

      System.out.println(expectedRecordsToPull);
      System.out.println(expectedNetworkCallCount);

      verify(expectedNetworkCallCount, getRequestedFor(WireMock.urlPathMatching("/odata/v2" +
        "/C_GLAccountHierarchyNode(\\?.*)?")));
    }
  }

  @Test
  public void verifyDataCorrectness() throws IOException, InterruptedException, EdmException, EntityProviderException {
    SapODataPluginConfig pluginConfig = pluginConfigBuilder.build();

    long availableRowCount = 10;

    List<SapODataInputSplit> partitionList = new SapODataPartitionBuilder().buildSplit(availableRowCount,
      pluginConfig.getNumRowsToFetch(), pluginConfig.getSkipRowCount(), pluginConfig.getSplitCount(),
      pluginConfig.getBatchSize());

    for (SapODataInputSplit inputSplit : partitionList) {
      prepareStubForRun(pluginConfig);

      SapODataRecordReader sapODataRecordReader = new SapODataRecordReader(pluginConfig, pluginSchema,
        encodedMetadataString, null, inputSplit.getStart(), inputSplit.getEnd(), inputSplit.getPackageSize());

      sapODataRecordReader.initialize(null, null);

      List<StructuredRecord> recordList = new ArrayList<>();
      while (sapODataRecordReader.nextKeyValue()) {
        recordList.add(sapODataRecordReader.getCurrentValue());
      }

      long expectedRecordsToPull = (inputSplit.getEnd() - inputSplit.getStart()) + 1;
      String msg = String.format("Total record count for split (start: %s & end: %s) is not matching",
        inputSplit.getStart(), inputSplit.getEnd());

      Assert.assertEquals(msg, expectedRecordsToPull, recordList.size());

      ODataFeed oDataFeed = prepareODataFeed(pluginConfig);
      for (int i = 0; i < oDataFeed.getEntries().size(); i++) {
        ODataEntry oDataEntry = oDataFeed.getEntries().get(i);
        StructuredRecord structuredRecord = recordList.get(i);
        pluginSchema.getFields().forEach(field -> {
          String fieldName = field.getName();

          Assert.assertEquals(field.getName() + " value is not equal.",
            processSchemaTypeValue(field.getSchema(), oDataEntry.getProperties().get(fieldName)),
            structuredRecord.get(field.getName()));
        });
      }
    }
  }

  private Object processSchemaTypeValue(Schema fieldSchema, Object fieldValue) {

    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType == Schema.LogicalType.DECIMAL) {
      // below change is done on the plugin level
      BigDecimal pluginValue = new BigDecimal(String.valueOf(fieldValue)).setScale(fieldSchema.getScale());
      // below update is done by the StructuredRecord.Builder#setDecimal
      return pluginValue.unscaledValue().toByteArray();
    } else if (logicalType == Schema.LogicalType.TIME_MICROS) {
      // below change is done on the plugin level
      LocalTime pluginValue = ((GregorianCalendar) fieldValue).toZonedDateTime().toLocalTime();
      // below update is done by the StructuredRecord.Builder#setTime
      long nanos = pluginValue.toNanoOfDay();
      return TimeUnit.NANOSECONDS.toMicros(nanos);
    } else if (logicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      // below change is done on the plugin level
      ZonedDateTime pluginValue = ((GregorianCalendar) fieldValue).toZonedDateTime();
      // below update is done by the StructuredRecord.Builder#setTimestamp
      Instant instant = pluginValue.toInstant();
      long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond());
      return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
    }
    return fieldValue;
  }

  private ODataFeed prepareODataFeed(SapODataPluginConfig pluginConfig) throws EntityProviderException, EdmException {
    try (InputStream metadataStream = TestUtil.readResource("sap-metadata.xml");
         InputStream responseStream = TestUtil.readResource("sap-data.json")) {

      SapODataEntityProvider serviceHelper =
        new SapODataEntityProvider(EntityProvider.readMetadata(metadataStream, false));
      EdmEntitySet entity = serviceHelper.getEntitySet(pluginConfig.getEntityName());

      return EntityProvider
        .readFeed(MediaType.APPLICATION_JSON, entity, responseStream, EntityProviderReadProperties.init().build());
    } catch (IOException e) {
      return null;
    }
  }

  private void prepareStubForRun(SapODataPluginConfig pluginConfig) {
    WireMock.reset();

    String expectedBody = TestUtil.convertInputStreamToString(TestUtil.readResource("sap-data.json"));
    WireMock.stubFor(WireMock.get(WireMock.urlPathMatching("/odata/v2/C_GLAccountHierarchyNode(\\?.*)?"))
      .withBasicAuth(pluginConfig.getUsername(), pluginConfig.getPassword())
      .willReturn(WireMock.ok()
        .withHeader(SapODataTransporter.SERVICE_VERSION, "2.0")
        .withBody(expectedBody)));
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"ODataColumnMetadata\"," +
      "\"fields\":[{\"name\":\"GLAccountHierarchy\",\"type\":\"string\"},{\"name\":\"HierarchyNode\"," +
      "\"type\":\"string\"},{\"name\":\"ValidityEndDate\",\"type\":[{\"type\":\"long\",\"logicalType\":" +
      "\"timestamp-micros\"},\"null\"]},{\"name\":\"ParentNode\",\"type\":[\"string\",\"null\"]},{\"name\":" +
      "\"HierarchyVersion\",\"type\":[\"string\",\"null\"]},{\"name\":\"ValidityStartDate\",\"type\":[{\"type\":" +
      "\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"ChartOfAccounts\",\"type\":[\"string\"," +
      "\"null\"]},{\"name\":\"GLAccount\",\"type\":[\"string\",\"null\"]},{\"name\":\"SequenceNumber\",\"type\":" +
      "[\"string\",\"null\"]},{\"name\":\"HierarchyNodeLevel\",\"type\":[\"string\",\"null\"]}," +
      "{\"name\":\"NodeType\",\"type\":[\"string\",\"null\"]},{\"name\":\"SemanticTag\"," +
      "\"type\":[\"string\",\"null\"]},{\"name\":\"SACAccountType\",\"type\":[\"string\",\"null\"]}]}";

    return Schema.parseJson(schemaString);
  }
}
