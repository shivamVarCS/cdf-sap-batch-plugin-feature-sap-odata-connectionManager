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

import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;


/**
 * This {@code SapODataTransporter} class is used to
 * make a rest web service call to the SAP OData exposed services.
 */
public class SapODataTransporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataTransporter.class);

  public static final String SERVICE_VERSION = "dataserviceversion";

  private final String username;
  private final String password;

  private final SapX509Manager x509Manager;

  public SapODataTransporter(String username,
                             String password,
                             SapX509Manager x509Manager) {
    this.username = username;
    this.password = password;
    this.x509Manager = x509Manager;
  }

  public SapODataResponseContainer callSapOData(URL endpoint, String mediaType, String fetchType)
    throws TransportException {

    Response res;
    try {
      LOGGER.debug(ResourceConstants.DEBUG_CALL_SERVICE_START.getMsgForKey(fetchType));
      res = transport(endpoint, mediaType);
    } catch (IOException ioe) {
      throw new TransportException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(), ioe);
    }

    LOGGER.debug(ResourceConstants.DEBUG_CALL_SERVICE_END.getMsgForKey(fetchType));

    return prepareResponseContainer(res);
  }

  public SapODataResponseContainer callSapODataWithRetry(URL endpoint, String mediaType, String fetchType)
    throws TransportException, InterruptedException {

    LOGGER.debug(ResourceConstants.DEBUG_CALL_SERVICE_START.getMsgForKey(fetchType));

    Response res = retrySapTransportCall(endpoint, mediaType, fetchType);

    LOGGER.debug(ResourceConstants.DEBUG_CALL_SERVICE_END.getMsgForKey(fetchType));

    return prepareResponseContainer(res);
  }

  private Response retrySapTransportCall(URL endpoint, String mediaType, String fetchType)
    throws TransportException, InterruptedException {

    //Maximum amount of retries.
    int maxTries = 3;

    //Base delay between retry calls.
    int baseDelayMillis = 100;

    int retryCount = 1;
    Response res = null;
    while (true) {
      try {
        res = transport(endpoint, mediaType);

        if (res.code() < HttpURLConnection.HTTP_INTERNAL_ERROR || retryCount >= maxTries) {
          return res;
        }
      } catch (IOException ioe) {
        if (retryCount >= maxTries) {
          LOGGER.warn("Retry limit exceed. Stopping retry and throwing TransportException.");
          throw new TransportException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(fetchType), ioe);
        }
      }

      String retryMsg = ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(fetchType)
        .concat(ResourceConstants.INFO_RETRY_ON_FAILURE.getMsgForKey(retryCount, baseDelayMillis));
      LOGGER.debug(retryMsg);

      TimeUnit.MILLISECONDS.sleep(baseDelayMillis);

      retryCount++;
    }
  }

  private Response transport(URL endpoint, String mediaType) throws IOException, TransportException {
    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    Request req = buildRequest(endpoint, mediaType);

    return enhancedOkHttpClient.newCall(req).execute();
  }

  private SapODataResponseContainer prepareResponseContainer(Response res) {
    return SapODataResponseContainer.builder()
      .httpStatusCode(res.code())
      .httpStatusMsg(res.message())
      .dataServiceVersion(res.header(SERVICE_VERSION))
      .responseStream(() -> (res.body() != null ? res.body().byteStream() : null))
      .build();
  }

  /**
   * Prepares request for metadata and data calls.
   *
   * @param mediaType supported types 'application/json' & 'application/xml'
   * @return Request
   */
  private Request buildRequest(URL endpoint, String mediaType) {
    return new Request.Builder()
      .addHeader("Authorization", getAuthenticationKey())
      .addHeader("Accept", mediaType)
      .get()
      .url(endpoint)
      .build();
  }

  /**
   * Builds the {@code OkHttpClient.Builder} with following optimized configuration parameters
   * Connection Timeout in seconds: 10
   * Read Timeout in seconds: 10
   * Write Timeout in seconds: 10
   *
   * @return {@code OkHttpClient.Builder}
   */
  private OkHttpClient.Builder getConfiguredClient() throws TransportException {

    OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder()
      .readTimeout(10, TimeUnit.SECONDS)
      .writeTimeout(10, TimeUnit.SECONDS)
      .connectTimeout(10, TimeUnit.SECONDS);

    try {
      x509Manager.configureSSLLayer(httpClientBuilder);
    } catch (CertificateException | IOException | KeyStoreException
      | NoSuchAlgorithmException | KeyManagementException | IllegalStateException ex) {

      throw new TransportException(ResourceConstants.ERR_FAILED_SSL_CONFIGURATION.getMsgForKey(),
        TransportException.X509_CERT_ERROR, ex);
    } catch (IllegalArgumentException iae) {
      throw new TransportException(iae.getMessage(), TransportException.X509_CERT_PATH_ERROR, iae);
    }

    return httpClientBuilder;
  }

  /**
   * Builds the Base64 encoded key for given Basic authorization parameters.
   *
   * @return returns the Base64 encoded username:password
   */
  private String getAuthenticationKey() {
    return "Basic " + Base64.getEncoder()
      .encodeToString(username
        .concat(":")
        .concat(password)
        .getBytes(StandardCharsets.UTF_8)
      );
  }
}
