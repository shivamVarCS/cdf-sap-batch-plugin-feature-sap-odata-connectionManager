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

package com.google.cloud.datafusion.plugin.sap.table.connection.out;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.connection.SapInterface;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.table.source.input.SapTableRecordReader;
import com.google.cloud.datafusion.plugin.sap.table.util.SapRfmExecutionHelper;

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
public class SapTableInterfaceImpl implements SapInterface {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapTableInterfaceImpl.class);

  private final SapRfmExecutionHelper rfmHelper;

  public SapTableInterfaceImpl() {
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
  public SapObjectMetadata getSourceMetadata(String tableName, SapConnection conn) throws ConnectorException {
    LOGGER.info("Initiate retrieve schema for SAP table {}", tableName);

    SapObjectMetadata tableSourceMetadata = rfmHelper.getColumnMetadata(tableName, conn);
    if (tableSourceMetadata == null) {
      tableSourceMetadata = new SapObjectMetadata(Collections.emptyList());
    }

    return tableSourceMetadata;
  }

  /**
   * Retrieves the runtime SAP configuration like available work process count and
   * max memory allowed for work process.
   * 
   * @param sapTable      SAP Table name
   * @param filterOptions Filter option as a list
   * @param conn          SapConnection
   * @return SapTableRuntimeConfigInfo
   * @throws ConnectorException
   */
  public SapTableRuntimeConfigInfo getRuntimeConfigParams(String sapTable, List<String> filterOptions,
                                                          SapConnection conn)
    throws ConnectorException {

    LOGGER.debug("Initiate read table record count, work process count and max memory for work process");

    SapTableRuntimeConfigInfo.Builder rtTableInfoBuilder =
      rfmHelper.getRuntimeTableRecordCount(sapTable, filterOptions, conn);

    SapTableRuntimeConfigInfo.Builder sapConfigInfoBuilder = rfmHelper.getAvailableDialogWorkProcesses(conn);
    SapTableRuntimeConfigInfo configInfo = sapConfigInfoBuilder.build();

    long wpMaxMemory = rfmHelper.getAvailableMemory(conn);
    rtTableInfoBuilder.setTotalWorkProcCount(configInfo.getTotalWorkProcCount());
    rtTableInfoBuilder.setAvailableWorkProcCount(configInfo.getAvailableWorkProcCount());
    rtTableInfoBuilder.setWpMaxMemory(wpMaxMemory);

    return rtTableInfoBuilder.build();
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
  public SapObjectRuntimeOutput getSourceData(String sapTable, List<String> filterOptions,
                                              Map<String, String> operationsProps, SapConnection conn)
    throws ConnectorException {

    LOGGER.debug("Initiate read table records");
    long rowSkips = Long.parseLong(operationsProps.get(SapTableRecordReader.ROW_SKIPS));
    long rowCount = Long.parseLong(operationsProps.get(SapTableRecordReader.ROW_COUNT));
    if (rowCount < 1) {
      return new SapTableRuntimeOutput(null, null);
    }

    return rfmHelper.getTableRecords(sapTable, rowSkips, rowCount, filterOptions, conn);
  }
}
