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

package com.google.cloud.datafusion.plugin.sap.odata.source.exception;

/**
 * This {@code TransportException} class is used to capture all the errors that are related to the calls to SAP
 * OData services.
 * i.e. invalid host, retry error or any IOException
 */
public class TransportException extends Exception {

  public static final int IO_ERROR = 0;
  public static final int X509_CERT_PATH_ERROR = 1;
  public static final int X509_CERT_ERROR = 2;

  private int errorType = IO_ERROR;

  public TransportException(String message) {
    super(message);
  }

  public TransportException(String message, int errorType, Throwable cause) {
    super(message, cause);
    this.errorType = errorType;
  }

  public TransportException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportException(Throwable cause) {
    super(cause);
  }

  protected TransportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public int getErrorType() {
    return errorType;
  }
}
