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

import com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto.ODataError;

/**
 * This {@code ODataServiceException} class is used to capture all the errors that are related to SAP OData
 * service issues.
 * e.g. Resource Not found, forbidden access, invalid query options etc.
 */

public class ODataServiceException extends Exception {

  public static final int INVALID_SERVICE = 0;
  private ODataError oDataError;
  private Integer errorCode;

  public ODataServiceException(String message) {
    this(message, null, null, null);
  }

  public ODataServiceException(String message, Integer errorCode) {
    this(message, errorCode, null, null);
  }

  public ODataServiceException(String message, Integer errorCode, ODataError oDataError) {
    this(message, errorCode, oDataError, null);
  }

  public ODataServiceException(String message, ODataError oDataError) {
    this(message, null, oDataError, null);
  }

  public ODataServiceException(String message, Throwable cause) {
    this(message, null, null, cause);
  }

  public ODataServiceException(String message, Integer errorCode, ODataError oDataError, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
    this.oDataError = oDataError;
  }

  public ODataError getODataError() {
    return this.oDataError;
  }

  public Integer getErrorCode() {
    return this.errorCode;
  }
}
