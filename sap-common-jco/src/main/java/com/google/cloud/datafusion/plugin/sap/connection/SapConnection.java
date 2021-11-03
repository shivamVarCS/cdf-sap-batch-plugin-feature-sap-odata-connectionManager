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
import com.google.cloud.datafusion.plugin.sap.exception.ExceptionHandler;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.ext.DestinationDataProvider;

import java.util.Map;

/**
 * The SAP connection wrapper containing parameters along with credentials to
 * establish connection with SAP and a non-exposed connector instance to carry
 * out the tasks in SAP.
 * 
 * @author sankalpbapat
 *
 */
public class SapConnection {

  private final String destName;
  private final SapDefinition definition;
  private final SapConnector connector;

  public SapConnection(Map<String, String> connParams) {
    definition = SapDefinition.builder(connParams).build();
    destName = definition.getProviderKey(false);
    connector = new SapConnector(definition);
  }

  public void initDestination() throws ConnectorException {
    connector.initDestination(destName);
  }

  public String getDestName() {
    return destName;
  }

  /**
   * Tests connectivity with SAP on this connection
   * 
   * @throws ConnectorException
   */
  public void ping() throws ConnectorException {
    try {
      connector.ping();
    } catch (JCoException e) {
      String errMsg = ResourceConstants.ERR_SAP_PING.getMsgForKeyWithCode() + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + e.getKey() + " - " + ExceptionHandler.getRootMessage(e);

      throw new ConnectorException(ResourceConstants.ERR_SAP_PING.getCode(), errMsg, e);
    }
  }

  /**
   * Retrieves the value of a connection parameter by name (except SAP logon
   * password)
   * 
   * @param name parameter name
   * @return Value for the named parameter
   */
  public String getConnParam(String name) {
    boolean isNameNotBlank = Util.isNotNullOrEmpty(name);
    if (isNameNotBlank && !DestinationDataProvider.JCO_PASSWD.equals(name)) {
      return (String) definition.getProperties().get(name);
    } else if (isNameNotBlank) {
      return SapDefinition.REPLACEMENT_JCO_PASWD;
    }

    return null;
  }

  /**
   * Refreshes the {@link JCoDestination} explicitly
   * 
   * @param forceRefresh
   * @throws ConnectorException
   */
  public void refreshDestination(boolean forceRefresh) throws ConnectorException {
    connector.refreshDestination(destName, forceRefresh);
  }

  /**
   * Retrieves the {@link JCoFunction} for an SAP RFM having {@code rfmName}
   * 
   * @param rfmName
   * @return JCoFunction instance
   * @throws ConnectorException
   */
  public JCoFunction getJCoFunction(String rfmName) throws ConnectorException {
    return connector.getJCoFunction(rfmName, destName);
  }

  /**
   * Executes an RFM in SAP via JCoFunction
   * 
   * @param function JCoFunction for an RFM populated with input
   * @throws ConnectorException
   */
  public void executeFunction(JCoFunction function) throws JCoException {
    connector.executeFunction(function);
  }
}
