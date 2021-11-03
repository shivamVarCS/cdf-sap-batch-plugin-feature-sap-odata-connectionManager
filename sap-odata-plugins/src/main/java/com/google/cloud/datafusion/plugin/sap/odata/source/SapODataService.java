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

package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.metadata.SapODataEntityProvider;
import com.google.cloud.datafusion.plugin.sap.odata.source.metadata.SapODataSchemaGenerator;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataResponseContainer;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataUrlContainer;
import com.google.cloud.datafusion.plugin.sap.odata.source.util.ExceptionParser;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;


/**
 * This {@code SapODataService} contains all the SAP OData relevant service call implementations
 * - check the correctness of the formed OData URL
 * - CDF compliant schema with non-navigation default properties
 * - CDF compliant schema with default and given expanded navigation properties
 * - CDF compliant schema with given selective properties
 */
public class SapODataService {

  public static final String TEST = "TEST";
  public static final String METADATA = "METADATA";
  public static final String COUNT = "COUNT";
  public static final String DATA = "DATA";

  private final SapODataPluginConfig pluginConfig;
  private final SapODataTransporter oDataHttpClient;
  private final SapODataUrlContainer  urlContainer;

  public SapODataService(SapODataPluginConfig pluginConfig, SapODataTransporter oDataHttpClient) {
    this.pluginConfig = pluginConfig;
    this.oDataHttpClient = oDataHttpClient;
    urlContainer = new SapODataUrlContainer(pluginConfig);
  }

  /**
   * Calls to check the OData URL correctness.
   *
   * @throws TransportException    any http client exceptions are wrapped under it.
   * @throws ODataServiceException any OData service based exception is wrapped under it.
   */
  public void checkODataURL() throws TransportException, ODataServiceException {

    SapODataResponseContainer responseContainer =
      oDataHttpClient.callSapOData(urlContainer.getTesterURL(), MediaType.APPLICATION_JSON, TEST);

    ExceptionParser.checkAndThrowException(ResourceConstants.ERR_FAILED_SERVICE_VALIDATION.getMsgForKey(),
      responseContainer);
  }

  /**
   * Prepares output schema based on the provided plugin config parameters.
   * e.g.
   * - builds schema with given selective properties
   * - builds schema with default and given expanded navigation properties
   * - builds schema with non-navigation default properties
   * <p>
   * For more detail please refer {@code SapODataSchemaGenerator}
   *
   * @return {@code Schema}
   * @throws TransportException    any http client exceptions are wrapped under it.
   * @throws ODataServiceException any OData service based exception is wrapped under it.
   */
  public Schema buildOutputSchema()  throws ODataServiceException, TransportException {

    SapODataEntityProvider edmData = fetchServiceMetadata();
    SapODataSchemaGenerator oDataSchemaGenerator = new SapODataSchemaGenerator(edmData);

    if (Util.isNotNullOrEmpty(pluginConfig.getSelectOption())) {
      return oDataSchemaGenerator.buildSelectOutputSchema(pluginConfig.getEntityName(), pluginConfig.getSelectOption());
    } else if (Util.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
      return oDataSchemaGenerator.buildExpandOutputSchema(pluginConfig.getEntityName(), pluginConfig.getExpandOption());
    } else {
      return oDataSchemaGenerator.buildDefaultOutputSchema(pluginConfig.getEntityName());
    }
  }

  /**
   * Calls the SAP OData Service and returns the {@code Edm} instance.
   *
   * @return {@code SapODataEntityProvider}
   * @throws TransportException    any http client exceptions are wrapped under it.
   * @throws ODataServiceException any OData service based exception is wrapped under it.
   */
  private SapODataEntityProvider fetchServiceMetadata() throws ODataServiceException, TransportException {
    try (InputStream metadataStream = callEntityMetadata()) {
      return fetchServiceMetadata(metadataStream);
    } catch (IOException e) {
      String errMsg = ResourceConstants.ERR_READING_METADATA.getMsgForKey(pluginConfig.getServiceName());
      throw new ODataServiceException(errMsg, e);
    }
  }

  /**
   * Converts the OData {@code InputStream} to {@code Edm} instance.
   * This will be used mainly in the Runtime.
   *
   * @return {@code Edm} object
   * @throws ODataServiceException
   */
  private SapODataEntityProvider fetchServiceMetadata(InputStream metadataStream) throws ODataServiceException {
    try {
      Edm metadata = EntityProvider.readMetadata(metadataStream, false);
      return new SapODataEntityProvider(metadata);
    } catch (EntityProviderException e) {
      String errMsg = ResourceConstants.ERR_READING_METADATA.getMsgForKey(pluginConfig.getServiceName());
      throw new ODataServiceException(errMsg, e);
    }
  }

  /**
   * Calls the SAP OData catalog service to fetch Service Entity metadata
   *
   * @return {@code InputStream}
   * @throws TransportException    any http client exceptions are wrapped under it.
   * @throws ODataServiceException any OData service based exception is wrapped under it.
   */
  private InputStream callEntityMetadata() throws TransportException, ODataServiceException {
    SapODataResponseContainer responseContainer = oDataHttpClient
      .callSapOData(urlContainer.getMetadataURL(), MediaType.APPLICATION_XML, METADATA);

    String errMsg = ResourceConstants.ERR_METADATA_CALL.getMsgForKey(pluginConfig.getServiceName());
    ExceptionParser.checkAndThrowException(errMsg, responseContainer);

    return responseContainer.getResponseStream();
  }

  /**
   * Calls the OData service to fetch the Entity data
   *
   * @throws ODataServiceException
   */
  private InputStream callEntityData(Long skip, Long top)
    throws ODataServiceException, TransportException, InterruptedException {
    URL dataURL = urlContainer.getDataFetchURL(skip, top);
    SapODataResponseContainer responseContainer = oDataHttpClient.callSapODataWithRetry(dataURL,
      MediaType.APPLICATION_JSON, DATA);

    String errMsg = ResourceConstants.ERR_ENTITY_DATA_CALL.getMsgForKey(pluginConfig.getEntityName());
    ExceptionParser.checkAndThrowException(errMsg, responseContainer);
    return responseContainer.getResponseStream();
  }

  private InputStream callEntityDataCount() throws ODataServiceException, TransportException {
    SapODataResponseContainer responseContainer = oDataHttpClient
      .callSapOData(urlContainer.getTotalRecordCountURL(), MediaType.TEXT_PLAIN, COUNT);

    String errMsg = ResourceConstants.ERR_ENTITY_DATA_CALL.getMsgForKey(pluginConfig.getEntityName());
    ExceptionParser.checkAndThrowException(errMsg, responseContainer);
    return responseContainer.getResponseStream();
  }

  public Integer getTotalAvailableRowCount() throws IOException, ODataServiceException, TransportException {

    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
      callEntityDataCount(), StandardCharsets.UTF_8))) {
      String raw = bufferedReader.lines().collect(Collectors.joining(""));
      return Integer.parseInt(raw);
    }
  }


  public ODataFeed getODataFeedForGivenMetadata(final Edm edm, Long skip, Long top)
    throws ODataServiceException, TransportException, InterruptedException {
    try {
      InputStream responseStream = callEntityData(skip, top);
      SapODataEntityProvider serviceHelper = new SapODataEntityProvider(edm);
      EdmEntitySet entity = serviceHelper.getEntitySet(pluginConfig.getEntityName());
      if (entity != null) {
        return EntityProvider
          .readFeed(MediaType.APPLICATION_JSON, entity, responseStream, EntityProviderReadProperties.init().build());
      } else {
        throw new ODataServiceException(
          "No record for the given '" + pluginConfig.getEntityName() + "' entity.");
      }
    } catch (ODataServiceException | TransportException ex) {
      throw ex;
    } catch (EdmException | EntityProviderException ex) {
      throw new ODataServiceException("Failed to read data from SAP OData service.", ex);
    }
  }

  public List<ODataEntry> getEntityListForGivenMetadata(final Edm edm, Long skip, Long top)
    throws ODataServiceException, TransportException, InterruptedException {
    return getODataFeedForGivenMetadata(edm, skip, top).getEntries();
  }

  public Edm getODataServiceEdm(String encodedMetadata) throws ODataServiceException {
    try {
      byte[] bytes = Base64.getDecoder().decode(encodedMetadata);
      try (ByteArrayInputStream metadataStream = new ByteArrayInputStream(bytes)) {
        return fetchServiceMetadata(metadataStream).getEdmMetadata();
      }
    } catch (IOException | IllegalArgumentException e) {
      String errMsg = String.format("Failed to decode the metadata from the the given encoded string for " +
        "service (%s)", pluginConfig.getServiceName());
      throw new ODataServiceException(errMsg, e);
    }
  }

  public String getEncodedServiceMetadata() throws ODataServiceException, TransportException {
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int numRead = 0;
    try (InputStream metaDataStream = callEntityMetadata()) {
      while ((numRead = metaDataStream.read(buffer)) > -1) {
        output.write(buffer, 0, numRead);
      }

      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (IOException ioe) {
      throw new ODataServiceException("Error while converting OData service metadata to string.", ioe);
    }
  }
}
