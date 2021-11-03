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

import com.google.cloud.datafusion.plugin.sap.connection.out.SapDestinationDataProvider;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.exception.ExceptionHandler;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connector wrapper containing the required JCo objects and is used for low
 * level operations in SAP.
 * 
 * @author sankalpbapat
 *
 */
public final class SapConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapConnector.class);

  private static final int MAX_RETRIES = 1;

  private JCoDestination jcoDest;

  /**
   * Sets up Destination Provider with JCo RT environment
   * 
   * @param def
   * @param destName
   * @throws ConnectorException
   */
  public SapConnector(SapDefinition def) {
    SapDestinationDataProvider destinationDataProvider = SapDestinationDataProvider.getInstance();
    destinationDataProvider.register(def);
  }

  /**
   * Initializes the JCoDestination for use.
   * 
   * @param destName
   * @throws JCoException
   */
  public void initDestination(String destName) throws ConnectorException {
    LOGGER.debug("JCoDestination init requested, for destination = {}", destName);
    try {
      jcoDest = JCoDestinationManager.getDestination(destName);
    } catch (JCoException e) {
      String errMsg = ResourceConstants.ERR_GET_DEST_FROM_MGR.getMsgForKeyWithCode() + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + e.getKey() + " - " + ExceptionHandler.getRootMessage(e);

      throw new ConnectorException(ResourceConstants.ERR_GET_DEST_FROM_MGR.getCode(), errMsg, e);
    }
  }

  /**
   * Pings the SAP destination
   * 
   * @throws JCoException
   */
  public void ping() throws JCoException {
    jcoDest.ping();
  }

  /**
   * Explicitly refreshes the {@link JCoDestination} identified by
   * {@code destName}, if it is no more valid or {@code forceRefresh} flag is
   * {@code true}.
   * 
   * @param destName
   * @param forceRefresh boolean
   * @throws ConnectorException
   */
  public void refreshDestination(String destName, boolean forceRefresh) throws ConnectorException {
    LOGGER.debug("JCoDestination refresh requested. isValid = {}; forceRefresh = {}", jcoDest.isValid(), forceRefresh);
    if (!jcoDest.isValid() || forceRefresh) {
      initDestination(destName);
    }
  }

  /**
   * Retrieves the {@link JCoFunction} for an SAP RFM having {@code rfmName}, from
   * the SAP destination {@code destName}.
   * 
   * @param rfmName
   * @param destName
   * @return JCoFunction instance
   * @throws ConnectorException
   */
  public JCoFunction getJCoFunction(String rfmName, String destName) throws ConnectorException {
    JCoRepository jcoRepo = initRepository(destName);
    try {
      LOGGER.debug("JCoFunction requested for RFM = {}, in destination = {}", rfmName, destName);
      JCoFunction jcoFunc = jcoRepo.getFunction(rfmName);
      // No Exception from JCo is thrown, so create custom and throw
      if (jcoFunc == null) {
        throw new ConnectorException(ResourceConstants.ERR_FUNC_MISSING.getCode(),
          ResourceConstants.ERR_FUNC_MISSING.getMsgForKeyWithCode(rfmName));
      }
      return jcoFunc;
    } catch (JCoException e) {
      String errMsg = ResourceConstants.ERR_GET_FUNC_FROM_REPO.getMsgForKeyWithCode(rfmName) + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + e.getKey() + " - " + ExceptionHandler.getRootMessage(e);

      throw new ConnectorException(ResourceConstants.ERR_GET_FUNC_FROM_REPO.getCode(), errMsg, e);
    }
  }

  /**
   * Initializes the {@code JCoRepository} from destination. During init,
   * {@code JCoDestination} may be stale and any subsequent operation on it would
   * result in a {@code JCoException}. In such case, the destination needs to be
   * refreshed before attempting the repository initialization again.
   * 
   * @param destName
   * @return JCoRepository instance
   * @throws ConnectorException
   */
  private JCoRepository initRepository(String destName) throws ConnectorException {
    LOGGER.debug("JCoRepository init, for destination = {}", destName);
    int retryCount = 1;
    while (true) {
      try {
        return jcoDest.getRepository();
      } catch (JCoException e) {
        if (retryCount > MAX_RETRIES) {
          String errMsg = ResourceConstants.ERR_GET_REPO_FROM_DEST.getMsgForKeyWithCode() + "\n"
            + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + e.getKey() + " - " + ExceptionHandler.getRootMessage(e);

          throw new ConnectorException(ResourceConstants.ERR_GET_REPO_FROM_DEST.getCode(), errMsg, e);
        }
        LOGGER.warn("JCoRepository init retry attempt {}, for destination = {}", retryCount, destName);
        refreshDestination(destName, false);
        retryCount++;
      }
    }
  }

  /**
   * Executes an RFM in SAP via JCoFunction
   * 
   * @param function JCoFunction for an RFM populated with input
   * @throws JCoException
   */
  public void executeFunction(JCoFunction function) throws JCoException {
    LOGGER.debug("Executing JCoFunction for RFM = {}", function.getName());
    function.execute(jcoDest);
  }
}
