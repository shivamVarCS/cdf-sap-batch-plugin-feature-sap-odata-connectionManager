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

import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.connector.SAPODataConnectorConfig;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import okhttp3.HttpUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * This {@code SapODataRequestContainer} contains the implementation of different SAP OData url:
 * * Test url
 * * Metadata url
 * * Available record count url
 * * Data url
 */
public class SapODataUrlContainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataUrlContainer.class);
  
  private static final String SKIP_OPTION = "$skip";
  private static final String TOP_OPTION = "$top";

  private final SapODataPluginConfig pluginConfig;
  private SAPODataConnectorConfig connection;

  public SapODataUrlContainer(SapODataPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
  }

  /**
   * Construct tester URL.
   *
   * @return tester URL.
   */
  public URL getTesterURL() {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegments(pluginConfig.getServiceName())
      .addPathSegment(pluginConfig.getEntityName());

    URL testerURL = buildQueryOptions(builder)
      .addQueryParameter(TOP_OPTION, "0")
      .build()
      .url();

    LOGGER.debug(ResourceConstants.DEBUG_TEST_ENDPOINT.getMsgForKey(testerURL));

    return testerURL;
  }

  /**
   * Construct tester URL.
   *
   * @return tester URL.
   */
  public URL getConnectorTesterURL() {
    HttpUrl.Builder builder = HttpUrl.parse(connection.getBaseURL())
            .newBuilder()
            .addPathSegments(pluginConfig.getServiceName())
            .addPathSegment(pluginConfig.getEntityName());

    URL testerURL = buildQueryOptions(builder)
            .addQueryParameter(TOP_OPTION, "0")
            .build()
            .url();

    LOGGER.debug(ResourceConstants.DEBUG_TEST_ENDPOINT.getMsgForKey(testerURL));

    return testerURL;
  }

  /**
   * Constructs metadata URL.
   *
   * @return metadata URL.
   */
  public URL getMetadataURL() {
    URL metadataURL = HttpUrl.parse(connection.getBaseURL())
      .newBuilder()
      .addPathSegments(pluginConfig.getServiceName())
      .addPathSegment("$metadata")
      .build()
      .url();

    LOGGER.debug(ResourceConstants.DEBUG_METADATA_ENDPOINT.getMsgForKey(metadataURL));

    return metadataURL;
  }

  /**
   * Constructs total available record count URL.
   *
   * @return total available record count URL.
   */
  public URL getTotalRecordCountURL() {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegments(pluginConfig.getServiceName())
      .addPathSegment(pluginConfig.getEntityName())
      .addPathSegment("$count");

    if (Util.isNotNullOrEmpty(pluginConfig.getFilterOption())) {
      builder.addQueryParameter("$filter", pluginConfig.getFilterOption());
    }
    URL recordCountURL = builder.build().url();

    LOGGER.debug(ResourceConstants.DEBUG_DATA_COUNT_ENDPOINT.getMsgForKey(recordCountURL));

    return recordCountURL;
  }

  /**
   * Constructs data URL with provided '$skip' and '$top' parameters.
   *
   * @param skip records to skip.
   * @param top  records to fetch.
   * @return data URL with provided '$skip' and '$top' parameters.
   */
  public URL getDataFetchURL(Long skip, Long top) {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegments(pluginConfig.getServiceName())
      .addPathSegment(pluginConfig.getEntityName());

    buildQueryOptions(builder);
    if (skip != null && skip != 0) {
      builder.addQueryParameter(SKIP_OPTION, String.valueOf(skip));
    }
    if (top != null) {
      builder.addQueryParameter(TOP_OPTION, String.valueOf(top));
    }
    URL dataURL = builder.build().url();

    LOGGER.debug(ResourceConstants.DEBUG_DATA_ENDPOINT.getMsgForKey(dataURL));

    return dataURL;
  }

  /**
   * Adds Query option parameters in {@code HttpUrl.Builder} as per the given sequence.
   * Sequence:
   * 1. $filter
   * 2. $select
   * 3. $expand
   *
   * @param urlBuilder
   * @return initialize the passed {@code HttpUrl.Builder} with the provided query options
   * in {@code SapODataPluginConfig} and return it.
   */
  private HttpUrl.Builder buildQueryOptions(HttpUrl.Builder urlBuilder) {
    if (Util.isNotNullOrEmpty(pluginConfig.getFilterOption())) {
      urlBuilder.addQueryParameter("$filter", pluginConfig.getFilterOption());
    }
    if (Util.isNotNullOrEmpty(pluginConfig.getSelectOption())) {
      urlBuilder.addQueryParameter("$select", pluginConfig.getSelectOption());
    }
    if (Util.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
      urlBuilder.addQueryParameter("$expand", pluginConfig.getExpandOption());
    }

    return urlBuilder;
  }
}
