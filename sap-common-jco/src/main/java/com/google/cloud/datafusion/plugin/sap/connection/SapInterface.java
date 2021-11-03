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

package com.google.cloud.datafusion.plugin.sap.connection;

import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectRuntimeOutput;

import java.util.List;
import java.util.Map;

/**
 * Exposes the APIs to connect with and execute programs in SAP
 * 
 * @author sankalpbapat
 */
public interface SapInterface {

  /**
   * Tests the connectivity with SAP via a ping, using provided parameters.
   *
   * @param conn SapConnection
   * @return true, if ping succeeds
   * @throws ConnectorException if ping fails
   */
  default boolean ping(SapConnection conn) throws ConnectorException {
    conn.ping();

    return true;
  }

  /**
   * Returns the metadata of table(s) having the provided name(s).
   *
   * @param sourceName
   * @param conn       SapConnection
   * @return POJO wrapping the SAP native object level metadata
   * @throws ConnectorException
   */
  SapObjectMetadata getSourceMetadata(String sourceName, SapConnection conn) throws ConnectorException;

  /**
   * Retrieves the data from SAP source.
   *
   * @param sourceName
   * @param operationsProps operation properties map containing specific
   *                        properties specific to implementation
   * @param conn            SapConnection
   * @return SapObjectRuntimeOutput
   * @throws ConnectorException
   */
  SapObjectRuntimeOutput getSourceData(String sourceName, List<String> filterOptions,
                                       Map<String, String> operationsProps, SapConnection conn)
    throws ConnectorException;
}
