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

package com.google.cloud.datafusion.plugin.sap.table.util;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.exception.ExceptionHandler;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapColumn;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeOutput;
import com.google.cloud.datafusion.plugin.sap.table.source.config.SapTablePluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.table.source.input.SapTableRecordReader;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Helper class to encapsulate SAP RFM specific network calls and logic.
 *
 * @author sankalpbapat
 */
public class SapRfmExecutionHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SapRfmExecutionHelper.class);

  private static final String METADATA_UTIL_RFM = "DDIF_FIELDINFO_GET";
  private static final String TABLE_READER_RFM = "/GOOG/RFC_READ_TABLE";
  private static final String WORK_PROCESS_RFM = "TH_WPINFO";
  private static final String MEMORY_SUMMARY_RFM = "SAPTUNE_GET_SUMMARY_STATISTIC";
  private static final String QUERY_TABLE_PARAM = "QUERY_TABLE";
  private static final String MARK_FIELD_TRUE_VALUE = "X";

  /**
   * Retrieves the columns and associated attributes like name, label, data type,
   * position in table, length etc. for the table identified by {@code tabName}
   *
   * @param tabName table name
   * @param conn    SapConnection
   * @return SapObjectMetadata pojo wrapper on column level metadata attributes
   * @throws ConnectorException
   */
  @Nullable
  public SapObjectMetadata getColumnMetadata(String tabName, SapConnection conn) throws ConnectorException {
    List<SapFieldMetadata> columnMetaList = new ArrayList<>();
    SapObjectMetadata tableMetadata = null;

    JCoFunction function = conn.getJCoFunction(METADATA_UTIL_RFM);
    function.getImportParameterList().setValue("TABNAME", tabName);

    try {
      conn.executeFunction(function);

      String objectType = function.getExportParameterList().getString("DDOBJTYPE");
      // Object is valid, but not a table or view
      if ("INTTAB".equals(objectType) || "APPEND".equals(objectType)) {
        String errMsg = ResourceConstants.ERR_TABLE_VIEW_INVALID.getMsgForKeyWithCode(tabName);
        throw new ConnectorException("INVALID_TABLE", errMsg);
      }

      JCoTable fieldMetaTable = function.getTableParameterList().getTable("DFIES_TAB");
      for (int j = 0; j < fieldMetaTable.getNumRows(); j++) {
        fieldMetaTable.setRow(j);
        String fieldName = fieldMetaTable.getString("FIELDNAME");
        String label = fieldMetaTable.getString("SCRTEXT_L");
        String desc = fieldMetaTable.getString("FIELDTEXT");
        if (Util.isNullOrEmpty(label)) {
          label = desc;
        }

        String position = fieldMetaTable.getString("POSITION");
        String decimals = fieldMetaTable.getString("DECIMALS");
        String dataType = fieldMetaTable.getString("DATATYPE");
        // SAP Views may have columns defined as key, but still be nullable
        String keyFlag =
          "VIEW".equals(objectType) || "STOB".equals(objectType) ? "" : fieldMetaTable.getString("KEYFLAG");

        boolean isUtcLong = "UTCL".equalsIgnoreCase(dataType) || "UTCLONG".equalsIgnoreCase(dataType);
        // For UTCLONG data type, length is incorrectly defined in SAP metadata
        String length = isUtcLong ? "27" : fieldMetaTable.getString("LENG");
        // UTCLONG data type even though internally represented as decimal in SAP, can
        // only be mapped to a character type for external communication
        String dataIntType = isUtcLong ? dataType : fieldMetaTable.getString("INTTYPE");

        int decimalInt = Integer.parseInt(decimals);
        // For decimal type columns, metadata RFM gives partial length and number of
        // decimals must be added to get real length
        SapFieldMetadata column =
          new SapColumn(fieldName, label, desc, Integer.parseInt(position), 0, Integer.parseInt(length) + decimalInt,
            decimalInt, dataType, dataIntType, MARK_FIELD_TRUE_VALUE.equals(keyFlag));

        columnMetaList.add(column);
      }
      tableMetadata = new SapObjectMetadata(columnMetaList);
    } catch (AbapException e) {
      if (e.getKey().equalsIgnoreCase("NOT_FOUND")) {
        String errMsg = ResourceConstants.ERR_TABLE_VIEW_MISSING.getMsgForKey(tabName);
        throw new ConnectorException(e.getKey(), errMsg, e);
      }
      handleGenericException(e, METADATA_UTIL_RFM);
    } catch (JCoException e) {
      handleGenericException(e, METADATA_UTIL_RFM);
    }

    return tableMetadata;
  }

  /**
   * Retrieves the columns and associated attributes like fieldName, offset,
   * length, data type, fieldText in SAP table
   *
   * @param sapTable      table name
   * @param filterOptions filterOptions
   * @param conn          SapConnection
   * @return SapTableRuntimeConfigInfo.Builder
   * @throws ConnectorException
   */
  public SapTableRuntimeConfigInfo.Builder getRuntimeTableRecordCount(String sapTable, List<String> filterOptions,
                                                                      SapConnection conn)
    throws ConnectorException {

    SapTableRuntimeConfigInfo.Builder rtTableInfoBuilder = SapTableRuntimeConfigInfo.builder();

    JCoFunction function = conn.getJCoFunction(TABLE_READER_RFM);
    function.getImportParameterList().setValue(QUERY_TABLE_PARAM, sapTable);
    function.getImportParameterList().setValue("NO_DATA", MARK_FIELD_TRUE_VALUE);
    function.getImportParameterList().setValue("IM_REC_COUNT", MARK_FIELD_TRUE_VALUE);
    JCoTable tabList =
      function.getTableParameterList().getTable(SapTablePluginConfigWrapper.FILTER_OPTIONS.toUpperCase());

    for (String filterOption : filterOptions) {
      tabList.appendRow();
      tabList.setValue("TEXT", filterOption);
    }

    try {
      conn.executeFunction(function);

      long recordCount = Long.parseLong(function.getExportParameterList().getString("EX_COUNT"));
      rtTableInfoBuilder.setRuntimeTableRecCount(recordCount);

      JCoTable fieldMetaTable = function.getTableParameterList().getTable("FIELDS");
      fieldMetaTable.setRow(fieldMetaTable.getNumRows() - 1);
      int offset = Integer.parseInt(fieldMetaTable.getString("OFFSET"));
      int length = Integer.parseInt(fieldMetaTable.getString("LENGTH"));
      rtTableInfoBuilder.setRecordSize(offset + length);
    } catch (AbapException e) {
      if (e.getKey().equalsIgnoreCase("OPTION_NOT_VALID")) {
        String errMsg = ResourceConstants.ERR_OPTION_NOT_VALID.getMsgForKeyWithCode();
        throw new ConnectorException(ResourceConstants.ERR_OPTION_NOT_VALID.getCode(), errMsg, e);
      } else if (e.getKey().equalsIgnoreCase("NOT_AUTHORIZED")) {
        String errMsg = ResourceConstants.ERR_NOT_AUTHORIZED_FOR_TABLE.getMsgForKeyWithCode(sapTable);
        throw new ConnectorException(ResourceConstants.ERR_NOT_AUTHORIZED_FOR_TABLE.getCode(), errMsg, e);
      } else if (e.getKey().equalsIgnoreCase("SQL_FAILURE")) {
        String errMsg = ResourceConstants.ERR_SQL_FAILURE.getMsgForKeyWithCode(sapTable);
        throw new ConnectorException(ResourceConstants.ERR_SQL_FAILURE.getCode(), errMsg, e);
      }

      handleGenericException(e, TABLE_READER_RFM);
    } catch (JCoException e) {
      handleGenericException(e, TABLE_READER_RFM);
    }

    return rtTableInfoBuilder;
  }

  /**
   * Retrieves the number of available work processes
   * 
   * @param conn
   * @return
   * @throws ConnectorException
   */
  public SapTableRuntimeConfigInfo.Builder getAvailableDialogWorkProcesses(SapConnection conn)
    throws ConnectorException {

    LOGGER.debug("Initiate get available dialog work processes");
    SapTableRuntimeConfigInfo.Builder tableConfigBuilder = SapTableRuntimeConfigInfo.builder();

    JCoFunction function = conn.getJCoFunction(WORK_PROCESS_RFM);

    try {
      conn.executeFunction(function);

      int totalDialogWp = 0;
      int availableDialogWp = 0;

      JCoTable wpDetailTable = function.getTableParameterList().getTable("WPLIST");
      for (int j = 0; j < wpDetailTable.getNumRows(); j++) {
        wpDetailTable.setRow(j);
        if ("DIA".equals(wpDetailTable.getString("WP_TYP"))) {
          // Dialog work process but may be in use
          totalDialogWp++;

          if ("Waiting".equals(wpDetailTable.getString("WP_STATUS"))) {
            // Dialog work process that is available at the moment
            availableDialogWp++;
          }
        }
      }
      tableConfigBuilder.setTotalWorkProcCount(totalDialogWp);
      tableConfigBuilder.setAvailableWorkProcCount(availableDialogWp);
    } catch (JCoException e) {
      handleGenericException(e, TABLE_READER_RFM);
    }

    return tableConfigBuilder;
  }

  /**
   * Retrieves the available memory
   *
   * @param conn
   * @return
   * @throws ConnectorException
   */
  public long getAvailableMemory(SapConnection conn) throws ConnectorException {
    LOGGER.debug("Initiate get available memory for work process");
    long maxMemoryForWp = 0L;

    JCoFunction function = conn.getJCoFunction(MEMORY_SUMMARY_RFM);

    try {
      conn.executeFunction(function);

      JCoTable memoryDetailTable = function.getTableParameterList().getTable("ALLOC_PROCEDURE_DIA");
      for (int j = 0; j < memoryDetailTable.getNumRows(); j++) {
        memoryDetailTable.setRow(j);
        String memType = memoryDetailTable.getString("MEMTYPE");
        String amount = memoryDetailTable.getString("AMOUNT");

        if ("1".equals(memType)) {
          maxMemoryForWp = Long.parseLong(amount);
        }
      }
    } catch (JCoException e) {
      handleGenericException(e, TABLE_READER_RFM);
    }

    return maxMemoryForWp;
  }

  /**
   * Executes RFM to get SAP table records
   *
   * @param sapTable
   * @param rowSkips
   * @param rowSkips
   * @param rowCount
   * @param filterOptions
   * @param conn
   * @return jcoTable
   * @throws ConnectorException
   */
  @Nullable
  public SapTableRuntimeOutput getTableRecords(String sapTable, long rowSkips, long rowCount,
                                               List<String> filterOptions, SapConnection conn)

    throws ConnectorException {

    LOGGER.debug("Initiate read sap table data");
    JCoFunction function = conn.getJCoFunction(TABLE_READER_RFM);
    function.getImportParameterList().setValue(QUERY_TABLE_PARAM, sapTable);
    function.getImportParameterList().setValue((SapTableRecordReader.ROW_SKIPS).toUpperCase(), rowSkips);
    function.getImportParameterList().setValue((SapTableRecordReader.ROW_COUNT).toUpperCase(), rowCount);
    JCoTable tabList =
      function.getTableParameterList().getTable(SapTablePluginConfigWrapper.FILTER_OPTIONS.toUpperCase());

    for (String filterOption : filterOptions) {
      tabList.appendRow();
      tabList.setValue("TEXT", filterOption);
    }

    try {
      conn.executeFunction(function);

      List<SapFieldMetadata> colMetaList = new ArrayList<>();
      SapObjectMetadata tableMetadata = new SapObjectMetadata(colMetaList);

      JCoTable fieldMetaTable = function.getTableParameterList().getTable("FIELDS");
      for (int j = 0; j < fieldMetaTable.getNumRows(); j++) {
        fieldMetaTable.setRow(j);
        String fieldName = fieldMetaTable.getString("FIELDNAME");
        String desc = fieldMetaTable.getString("FIELDTEXT");
        String offset = fieldMetaTable.getString("OFFSET");
        String length = fieldMetaTable.getString("LENGTH");
        String dataIntType = fieldMetaTable.getString("TYPE");

        SapFieldMetadata column = new SapColumn(fieldName, null, desc, 0, Integer.valueOf(offset),
          Integer.valueOf(length), 0, null, dataIntType, false);

        colMetaList.add(column);
      }

      String outputTable = function.getExportParameterList().getString("OUT_TABLE");
      // Return JCoTable identified by OUT_TABLE parameter of RFM
      return new SapTableRuntimeOutput(tableMetadata, function.getTableParameterList().getTable(outputTable));
    } catch (AbapException e) {
      if (e.getKey().equalsIgnoreCase("DATA_BUFFER_EXCEEDED")) {
        String errMsg = ResourceConstants.ERR_DATA_BUFFER_EXCEEDED.getMsgForKeyWithCode(sapTable);
        throw new ConnectorException(ResourceConstants.ERR_DATA_BUFFER_EXCEEDED.getCode(), errMsg, e);
      } else if (e.getKey().equalsIgnoreCase("SQL_FAILURE")) {
        if (filterOptions.isEmpty()) {
          String errMsg = ResourceConstants.ERR_DB_FAILURE.getMsgForKeyWithCode(sapTable);
          throw new ConnectorException(ResourceConstants.ERR_DB_FAILURE.getCode(), errMsg, e);
        } else {
          String errMsg = ResourceConstants.ERR_SQL_FAILURE.getMsgForKeyWithCode(sapTable);
          throw new ConnectorException(ResourceConstants.ERR_SQL_FAILURE.getCode(), errMsg, e);
        }
      }

      handleGenericException(e, TABLE_READER_RFM);
    } catch (JCoException e) {
      handleGenericException(e, TABLE_READER_RFM);
    }

    return null;
  }

  /**
   * Handles generic JCoException thrown from SAP while executing RFM
   *
   * @param e
   * @param funcName
   * @throws ConnectorException
   */
  private void handleGenericException(JCoException e, String funcName) throws ConnectorException {
    String errMsg = ResourceConstants.ERR_EXEC_FUNC.getMsgForKeyWithCode(funcName) + "\n"
      + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + e.getKey() + " - " + ExceptionHandler.getRootMessage(e);

    throw new ConnectorException(ResourceConstants.ERR_EXEC_FUNC.getCode(), errMsg, e);
  }
}
