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

package com.google.cloud.datafusion.plugin.sap.connection.out;

import com.google.cloud.datafusion.plugin.sap.connection.SapDefinition;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Implements the JCo Destination Data Provider to initialize and utilize JCo
 * Runtime for connecting to SAP, without the requirement of a physical
 * connection file.
 * 
 * @author sankalpbapat
 */
public class SapDestinationDataProvider implements DestinationDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapDestinationDataProvider.class);

  private static final SapDestinationDataProvider DEST_DATA_PROVIDER = new SapDestinationDataProvider();

  private final Map<String, SapDefinition> defs;

  private DestinationDataEventListener eventListener;

  private SapDestinationDataProvider() {
    this.defs = new HashMap<>();
  }

  public static final SapDestinationDataProvider getInstance() {
    return DEST_DATA_PROVIDER;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.sap.conn.jco.ext.DestinationDataProvider#getDestinationProperties(java.
   * lang.String)
   */
  @Override
  public Properties getDestinationProperties(String name) {
    SapDefinition def = this.defs.get(name);
    if (def != null && def.getProviderKey(false).equalsIgnoreCase(name)) {
      return def.getProperties();
    }

    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.sap.conn.jco.ext.DestinationDataProvider#setDestinationDataEventListener(
   * com.sap.conn.jco.ext.DestinationDataEventListener)
   */
  @Override
  public void setDestinationDataEventListener(DestinationDataEventListener eventListener) {
    this.eventListener = eventListener;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.sap.conn.jco.ext.DestinationDataProvider#supportsEvents()
   */
  @Override
  public boolean supportsEvents() {
    return true;
  }

  /**
   * Registers the {@link DestinationDataProvider} in SAP JCo Runtime Environment,
   * and updates connection parameters definition. Expected to be called for every
   * new SapConnection.
   * 
   * @param def Connection parameters
   */
  public void register(SapDefinition def) {
    LOGGER.debug("Destination Data Provider already registered? {}", Environment.isDestinationDataProviderRegistered());
    synchronized (this) {
      // This check passes only the first time upon JVM start.
      if (this.defs.isEmpty() && !Environment.isDestinationDataProviderRegistered()) {
        LOGGER.debug("Registering Destination Data Provider in JCo environment");
        // This activity is needed once during the whole JVM life cycle, before the
        // first SAP call over network.
        Environment.registerDestinationDataProvider(this);
      }

      String destKey = def.getProviderKey(false);
      SapDefinition previousDef = this.defs.put(destKey, def);
      if (previousDef != null) {
        LOGGER.debug("Updating Destination Data Event Listener to the latest destination name '{}'", destKey);
        eventListener.updated(destKey);
      }
    }
  }

  /**
   * Removes connection parameters definition from JCo destination manager.
   * 
   * @param destKey Connection parameters identifier
   */
  public void removeDefinition(String destKey) {
    synchronized (this) {
      if (this.defs != null && !defs.isEmpty()) {
        SapDefinition def = this.defs.remove(destKey);
        if (def != null) {
          LOGGER.debug("Deleting the destination name '{}' from Destination Data Event Listener", destKey);
          eventListener.deleted(destKey);
        }
      }
    }
  }
}
