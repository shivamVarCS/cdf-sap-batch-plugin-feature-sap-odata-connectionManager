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

/**
 * This {@code ErrorDetail} class contains SAP S4/Hana 1909 error details.
 * Sample:
 * {
 *   "code": "005056A509B11EE1B9A8FEC11C21D78E",
 *   "message": {
 *     "lang": "en",
 *     "value": "Resource not found for the segment 'Address2'."
 *   },
 *   "innererror": {
 *     "transactionid": "C83CB3D2A1420000E00609D31E196BD4",
 *     "timestamp": "20210524082515.9921880",
 *     "Error_Resolution": {
 *       "SAP_Transaction": "For backend administrators: use ADT feed reader \"SAP Gateway Error Log\"
 *       or run transaction /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries with the timestamp
 *       above for more details",
 *       "SAP_Note": "See SAP Note 1797736 for error analysis (https://service.sap.com/sap/support/notes/1797736)"
 *     }
 *   }
 * }
 *
 * Any 'innererror' is collected as {@code Map<String, Object>} which will be used to render in the logs for detailed
 * error reference. This 'innererror' structure varies with different SAP OData 'Entity' as well as different HTTP
 * statue code.
 * e.g.
 * - the above sample is in case of unknown property name found in given SAP OData entity, HTTP code is 404
 * - for invalid SAP OData service name, HTTP status code is 403 and 'innererror' structure is
 *  {
 *   "application": {
 *     "component_id": "",
 *     "service_namespace": "/SAP/",
 *     "service_id": "ZPURCHASEORDER_SRV_X",
 *     "service_version": "0001"
 *   },
 *   "transactionid": "C83CB3D2A1420280E0060C1BA869E3A7",
 *   "timestamp": "20210722055302.8528500",
 *   "Error_Resolution": {
 *     "SAP_Transaction": "For backend administrators: use ADT feed reader \"SAP Gateway Error Log\"
 *     or run transaction /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries with the timestamp
 *     above for more details",
 *     "SAP_Note": "See SAP Note 1797736 for error analysis (https://service.sap.com/sap/support/notes/1797736)"
 *   },
 *   "errordetails": []
 * }
 */
public class ErrorDetail {
  private final String code;
  private final Message message;
  private final InnerError innererror;

  public ErrorDetail(String code, Message message, InnerError innererror) {
    this.code = code;
    this.message = message;
    this.innererror = innererror;
  }

  public String getCode() {
    return code;
  }

  public Message getMessage() {
    return message;
  }

  public InnerError getInnerError() {
    return innererror;
  }
}
