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

package com.google.cloud.datafusion.plugin.sap.odp.connection.out;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.SapInterface;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasource;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.odp.util.SapRfmExecutionHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of APIs to connect with and execute operations in SAP via JCo
 * (Application Layer)
 *
 * @author sankalpbapat
 */
public class SapOdpInterfaceImpl implements SapInterface {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapOdpInterfaceImpl.class);

  private final SapRfmExecutionHelper rfmHelper;

  public SapOdpInterfaceImpl() {
    rfmHelper = new SapRfmExecutionHelper();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceMetadata(java.
   * lang.String, com.google.cloud.datafusion.plugin.sap.connection.SapConnection)
   */
  @Override
  public SapOdpDatasource getSourceMetadata(String dataSourceName, SapConnection conn) throws ConnectorException {
    LOGGER.info("Initiate retrieve schema for SAP ODP datasource {}", dataSourceName);

    SapOdpDatasource odpSourceMetadata = rfmHelper.getColumnMetadata(dataSourceName, conn);
    if (odpSourceMetadata == null) {
      odpSourceMetadata = new SapOdpDatasource(false, Collections.emptyList());
    }

    return odpSourceMetadata;
  }

  /**
   * Retrieves the runtime SAP configuration like available work process count and
   * max memory allowed for work process.
   * 
   * @param sourceName    SAP object name
   * @param filterOptions Filter option as a list
   * @param conn          SapConnection
   * @return SapOdpRuntimeConfigInfo
   * @throws ConnectorException
   */
  public SapOdpRuntimeConfigInfo
    getRuntimeConfigParams(String sourceName, List<String> filterOptions, boolean isSyncMode, String extractMode,
                           String subcriberName, long packageSize, SapConnection conn)
      throws ConnectorException {

    LOGGER.trace("Initiate read package count, work process count and max memory for work process");

    SapOdpRuntimeConfigInfo.Builder rtOdpInfoBuilder = rfmHelper.getRuntimePackageCount(sourceName, filterOptions,
      isSyncMode, extractMode, subcriberName, packageSize, conn);

    SapOdpRuntimeConfigInfo.Builder sapConfigInfoBuilder = rfmHelper.getAvailableDialogWorkProcesses(conn);
    SapOdpRuntimeConfigInfo configInfo = sapConfigInfoBuilder.build();

    long wpMaxMemory = rfmHelper.getAvailableMemory(conn);
    rtOdpInfoBuilder.setTotalWorkProcCount(configInfo.getTotalWorkProcCount());
    rtOdpInfoBuilder.setAvailableWorkProcCount(configInfo.getAvailableWorkProcCount());
    rtOdpInfoBuilder.setWpMaxMemory(wpMaxMemory);

    return rtOdpInfoBuilder.build();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.google.cloud.datafusion.plugin.sap.connection.SapInterface#getSourceData(java.lang
   * .String, java.util.Collection<String>, java.util.Map<String, String>,
   * com.google.cloud.datafusion.plugin.sap.connection.SapConnection)
   */
  @Override
  public SapObjectRuntimeOutput getSourceData(String sourceName, List<String> filterOptions,
                                              Map<String, String> operationsProps, SapConnection conn)
    throws ConnectorException {

    return null;
  }
}
