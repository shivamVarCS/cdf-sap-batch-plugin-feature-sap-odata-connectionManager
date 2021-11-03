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
 * This {@code Message} class contains SAP S4/Hana 1909 error message.
 * Sample:
 * {
 *   "lang": "en",
 *   "value": "Resource not found for the segment 'Address2'."
 * }
 */
public class Message {
  private final String lang;
  private final String value;

  public Message(String lang, String value) {
    this.lang = lang;
    this.value = value;
  }

  public String getLang() {
    return lang;
  }

  public String getValue() {
    return value;
  }
}
