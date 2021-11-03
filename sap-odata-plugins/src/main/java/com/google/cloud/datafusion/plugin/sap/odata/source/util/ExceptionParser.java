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

package com.google.cloud.datafusion.plugin.sap.odata.source.util;

import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto.ODataError;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataResponseContainer;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * This {@code ExceptionParser} parse and forms the relevant error messages.
 */
public class ExceptionParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionParser.class);

  public static final int NO_VERSION_FOUND = 1;
  public static final int INVALID_VERSION_FOUND = 2;

  private static final Gson GSON = new Gson();

  private ExceptionParser() {
  }

  /**
   * Checks the {@code SapODataResponseContainer} status code and build appropriate error message
   * and throws {@code ODataClientException}
   *
   * @param msg               stage wise error message for example the caller is
   *                          - failure while testing OData URL
   *                          - failure while reading metadata
   *                          - failure while reading total available record count
   *                          - failure while reading actual data
   * @param responseContainer {@code SapODataResponseContainer} contains the http response details after calling
   *                          SAP OData service
   * @throws ODataServiceException in case of any error scenario, it prepares and throw this exception.
   */
  public static void checkAndThrowException(String msg, SapODataResponseContainer responseContainer)
    throws ODataServiceException {

    String failureMessage = msg;
    ODataError error = null;

    if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      failureMessage += ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey()
        .concat(ResourceConstants.ERR_INVALID_CREDENTIAL.getMsgForKey());

      throw new ODataServiceException(failureMessage, responseContainer.getHttpStatusCode());
    }

    if (responseContainer.getHttpStatusCode() != HttpURLConnection.HTTP_OK) {
      String rawResponseString = "";
      InputStream rawStream = responseContainer.getResponseStream();
      if (rawStream != null) {
        try (BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(rawStream, StandardCharsets.UTF_8))) {

          rawResponseString = bufferedReader.lines().collect(Collectors.joining(" "));
        } catch (IOException ioe) {
          // no-ops
        }
      }

      LOGGER.error("HTTP Code: {}", responseContainer.getHttpStatusCode());
      LOGGER.error("Detailed error message: {} {}", msg, rawResponseString);

      try {
        error = GSON.fromJson(rawResponseString, ODataError.class);
      } catch (JsonSyntaxException | JsonIOException je) {
        // html errors are only found in case of invalid SAP OData service namespace
        if (rawResponseString.startsWith("<html>")) {
          failureMessage += ResourceConstants.ERR_INVALID_SERVICE_NAME.getMsgForKey();
          throw new ODataServiceException(failureMessage, ODataServiceException.INVALID_SERVICE);
        }

        // this exception may occur when the rawResponseString contains non-json format such as text | html and in
        // OData it is possible to receive text | html format response based on the type of errors.
        failureMessage += ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey().concat(rawResponseString);
      }
      throw new ODataServiceException(failureMessage, responseContainer.getHttpStatusCode(), error);
    }

    if (Util.isNullOrEmpty(responseContainer.getDataServiceVersion())) {
      failureMessage += ResourceConstants.ERR_MISSING_DATASERVICE_VERSION.getMsgForKey();
      throw new ODataServiceException(failureMessage, NO_VERSION_FOUND);
    }

    if (!responseContainer.getDataServiceVersion().equals("2.0")) {
      failureMessage += ResourceConstants.ERR_UNSUPPORTED_VERSION
        .getMsgForKey(responseContainer.getDataServiceVersion(), "2.0");

      throw new ODataServiceException(failureMessage, INVALID_VERSION_FOUND);
    }
  }

  /**
   * Builds a user friendly error message for any {@code TransportException} exception
   *
   * @param te {@code TransportException}
   * @return user friendly error message
   */
  public static String buildTransportError(TransportException te) {
    StringBuilder errorDetails = new StringBuilder()
      .append(te.getMessage())
      .append(" ")
      .append(ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey());

    if (te.getCause() instanceof SocketTimeoutException) {
      errorDetails.append("Connection timeout. Please verify that the given base URL is up and running.");
    } else {
      errorDetails.append(te.getCause().getMessage());
    }

    return errorDetails.toString();
  }

  /**
   * Builds a user friendly error message for any {@code ODataServiceException} exception
   *
   * @param ose {@code ODataServiceException}
   * @return user friendly error message
   */
  public static String buildODataServiceError(ODataServiceException ose) {
    StringBuilder errorDetails = new StringBuilder()
      .append(ose.getMessage())
      .append(" ");

    if (ose.getCause() != null) {
      errorDetails.append(ose.getCause().getMessage());
    }
    if (ose.getODataError() != null && ose.getODataError().getError() != null) {
      errorDetails
        .append(ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey())
        .append(ose.getODataError().getError().getMessage().getValue());
    }

    return errorDetails.toString();
  }
}
