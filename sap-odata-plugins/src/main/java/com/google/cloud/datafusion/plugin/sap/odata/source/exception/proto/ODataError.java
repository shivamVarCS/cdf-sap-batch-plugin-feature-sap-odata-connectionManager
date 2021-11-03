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

package com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto;

import javax.annotation.Nullable;

/**
 * This {@code ODataError} class contains SAP S4/Hana 1909 error response structure.
 * SAP OData service responds error in following structure.
 * Sample:
 * {
 *   "error": {
 *     "code": "005056A509B11EE1B9A8FEC11C21D78E",
 *     "message": {
 *       "lang": "en",
 *       "value": "Resource not found for the segment 'Address2'."
 *     },
 *     "innererror": {
 *       "transactionid": "C83CB3D2A1420000E00609D31E196BD4",
 *       "timestamp": "20210524082515.9921880",
 *       "Error_Resolution": {
 *         "SAP_Transaction": "For backend administrators: use ADT feed reader \"SAP Gateway Error Log\"
 *         or run transaction /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries with the timestamp
 *         above for more details",
 *         "SAP_Note": "See SAP Note 1797736 for error analysis (https://service.sap.com/sap/support/notes/1797736)"
 *       }
 *     }
 *   }
 * }
 */
public class ODataError {

  @Nullable
  private final ErrorDetail error;

  public ODataError(@Nullable ErrorDetail error) {
    this.error = error;
  }

  @Nullable
  public ErrorDetail getError() {
    return error;
  }
}
