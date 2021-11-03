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

package com.google.cloud.datafusion.plugin.sap.odp.util;

import com.google.cloud.datafusion.plugin.sap.connection.SapConnection;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.exception.ExceptionHandler;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasource;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpDatasourceField;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSource;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Helper class to encapsulate SAP RFM specific network calls and logic.
 *
 * @author sankalpbapat
 */
public class SapRfmExecutionHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapRfmExecutionHelper.class);

  private static final String METADATA_UTIL_RFM = "/GOOG/ODP_DS_METADATA";
  private static final String ODP_PACKAGE_BUILDER_RFM = "/GOOG/ODP_DS_EXTRACT_DATA";
  private static final String WORK_PROCESS_RFM = "TH_WPINFO";
  private static final String MEMORY_SUMMARY_RFM = "SAPTUNE_GET_SUMMARY_STATISTIC";

  private static final String DATASOURCE_NAME = "I_OLTPSOURCE";
  private static final String SUBSCRIBER_PROCESS = "I_SUBSCRIBER_PROCESS";

  private static final String MARK_FIELD_TRUE_VALUE = "X";

  private static final String COLON = ":";

  private static final Pattern PATTERN_RANGE_FILTER_OPTION = Pattern.compile("(?i)(\\S+)\\s+AND\\s+(\\S+)");

  public static final String ERR_DATA_SOURCE_NOT_EXIST = "DATA_SOURCE_NOT_EXIST";
  public static final String ERR_DATA_SOURCE_NOT_EXPOSED = "DATA_SOURCE_NOT_EXPOSE_ODP";
  public static final String ERR_STRUCTURE_NOT_FOUND = "STRUCT_NOT_FOUND";

  /**
   * Retrieves the columns and associated attributes like name, label, data type,
   * position, length etc. for the datasource identified by {@code dataSourceName}
   *
   * @param dataSourceName ODP Datasource name
   * @param conn           SapConnection
   * @return Map of name as key and map of column name and wrapped object for the
   *         column attributes, as value
   * @throws ConnectorException
   */
  @Nullable
  public SapOdpDatasource getColumnMetadata(String dataSourceName, SapConnection conn) throws ConnectorException {
    List<SapFieldMetadata> fieldMetaList = new ArrayList<>();
    SapOdpDatasource odpSourceMetadata = null;

    JCoFunction function = conn.getJCoFunction(METADATA_UTIL_RFM);
    function.getImportParameterList().setValue(DATASOURCE_NAME, dataSourceName.toUpperCase());

    try {
      conn.executeFunction(function);

      boolean isHierarchical = MARK_FIELD_TRUE_VALUE.equals(function.getExportParameterList().getString("E_IS_HIER"));

      JCoTable fieldTypeTable = function.getTableParameterList().getTable("T_TYPEKIND");
      JCoTable fieldMetaTable = function.getTableParameterList().getTable("T_FIELDS");

      Map<String, String> fieldTypeMap = getFieldTypeMap(fieldTypeTable);

      for (int j = 0; j < fieldMetaTable.getNumRows(); j++) {
        fieldMetaTable.setRow(j);

        String fieldName = fieldMetaTable.getString("NAME");
        String dataType = fieldMetaTable.getString("TYPE");
        String decimals = fieldMetaTable.getString("DECIMALS");
        String desc = fieldMetaTable.getString("DESCRIPTION");
        String keyFlag = fieldMetaTable.getString("KEYFLAG");
        String equalFlag = fieldMetaTable.getString("SEL_EQUAL");
        String betweenFlag = fieldMetaTable.getString("SEL_BETWEEN");

        boolean isUtcLong = "UTCL".equalsIgnoreCase(dataType) || "UTCLONG".equalsIgnoreCase(dataType);
        // For UTCLONG data type, length is incorrectly defined in SAP metadata
        String length = isUtcLong ? "27" : fieldMetaTable.getString("LENGTH");
        // UTCLONG data type even though internally represented as decimal in SAP, can
        // only be mapped to a character type for external communication
        String dataIntType = isUtcLong ? dataType : fieldTypeMap.get(fieldName);

        int decimalInt = Integer.parseInt(decimals);
        // For decimal type columns, metadata RFM gives partial length and number of
        // decimals must be added to get real length
        SapFieldMetadata odpSourceField =
          new SapOdpDatasourceField(fieldName, null, desc, Integer.parseInt(length) + decimalInt, decimalInt, dataType,
            dataIntType, MARK_FIELD_TRUE_VALUE.equals(keyFlag), MARK_FIELD_TRUE_VALUE.equals(equalFlag),
            MARK_FIELD_TRUE_VALUE.equals(betweenFlag));

        fieldMetaList.add(odpSourceField);
      }

      odpSourceMetadata = new SapOdpDatasource(isHierarchical, fieldMetaList);
    } catch (AbapException e) {
      if (e.getKey().equalsIgnoreCase(ERR_DATA_SOURCE_NOT_EXIST)) {
        String errMsg = ResourceConstants.ERR_SOURCE_OBJ_MISSING.getMsgForKey(dataSourceName);
        throw new ConnectorException(e.getKey(), errMsg);
      } else if (e.getKey().equalsIgnoreCase(ERR_DATA_SOURCE_NOT_EXPOSED)) {
        String errMsg = ResourceConstants.ERR_SOURCE_OBJ_NOT_EXPOSED.getMsgForKey(dataSourceName);
        throw new ConnectorException(e.getKey(), errMsg, e);
      } else if (e.getKey().equalsIgnoreCase(ERR_STRUCTURE_NOT_FOUND)) {
        String errMsg = ResourceConstants.ERR_STRUCT_FOR_DATA_SOURCE_MISSING.getMsgForKey(dataSourceName);
        throw new ConnectorException(e.getKey(), errMsg);
      }

      handleGenericException(e, METADATA_UTIL_RFM);
    } catch (JCoException e) {
      handleGenericException(e, METADATA_UTIL_RFM);
    }

    return odpSourceMetadata;
  }

  /**
   * Maps the data type with its corresponding field name
   *
   * @param fieldTypeTable Data type table
   * @return map contains field and its type mapping
   */
  private Map<String, String> getFieldTypeMap(JCoTable fieldTypeTable) {
    Map<String, String> fieldTypeMap = new HashMap<>();
    for (int j = 0; j < fieldTypeTable.getNumRows(); j++) {
      fieldTypeTable.setRow(j);
      String fieldName = fieldTypeTable.getString("NAME");
      String dataIntType = fieldTypeTable.getString("TYPE_KIND");
      fieldTypeMap.put(fieldName, dataIntType);
    }
    return fieldTypeMap;
  }

  /**
   * Executes RFM to get runtime configuration information like total packages,
   * last extract mode, last extract status, delta supported flag, replication
   * pointer, & hierarchical flag
   * 
   * @param sourceName     datasource name
   * @param filterOptions  List of filter options
   * @param isSyncMode     whether extraction mode auto-detect should be on
   * @param extractMode    type of extraction like F (Full), D (Delta), R
   *                       (Recovery)
   * @param subscriberName ODP subscriber name
   * @param packageSize    package size to be used for each package
   * @param conn           SAP connection
   * @return SapOdpRuntimeConfigInfo.Builder
   * @throws ConnectorException
   */
  public SapOdpRuntimeConfigInfo.Builder
    getRuntimePackageCount(String sourceName, List<String> filterOptions, boolean isSyncMode, String extractMode,
                           String subscriberName, long packageSize, SapConnection conn)
      throws ConnectorException {

    SapOdpRuntimeConfigInfo.Builder rtDataSourceInfoBuilder = SapOdpRuntimeConfigInfo.builder();

    JCoFunction function = conn.getJCoFunction(ODP_PACKAGE_BUILDER_RFM);
    function.getImportParameterList().setValue(DATASOURCE_NAME, sourceName);
    function.getImportParameterList().setValue(SapOdpBatchSource.EXTRACTION_MODE, extractMode);
    function.getImportParameterList().setValue(SapOdpBatchSource.SUBSCRIBER_NAME, subscriberName);
    function.getImportParameterList().setValue(SUBSCRIBER_PROCESS, subscriberName);

    // Sync flag must only be set when querying the previous execution mode and
    // status
    if (isSyncMode) {
      function.getImportParameterList().setValue(SapOdpBatchSource.SYNC_MODE, MARK_FIELD_TRUE_VALUE);
    }

    function.getImportParameterList().setValue("I_SUBSCRIBER_RUN", "REQ_CDF_" + System.currentTimeMillis());
    function.getImportParameterList().setValue(SapOdpBatchSource.MAX_PACKAGE_SIZE, packageSize);

    setFilterOptions(filterOptions, function);

    try {
      conn.executeFunction(function);

      String previousExtractType = function.getExportParameterList().getString("E_LAST_EXTARCT_MODE");
      rtDataSourceInfoBuilder.setPreviousExtractType(previousExtractType);

      String previousExtractStatus = function.getExportParameterList().getString("E_LAST_EXTRACT_STATUS");
      rtDataSourceInfoBuilder.setPreviousExtractStatus(previousExtractStatus);

      boolean isDeltaSupported =
        MARK_FIELD_TRUE_VALUE.equals(function.getExportParameterList().getString("E_FLAG_DELTA"));

      rtDataSourceInfoBuilder.setDeltaSupported(isDeltaSupported);

      long totalPackageCount = Long.parseLong(function.getExportParameterList().getString("E_TOT_PACKAGES"));
      rtDataSourceInfoBuilder.setRuntimePackageCount(totalPackageCount);

      String replicationPointer = function.getExportParameterList().getString("E_POINTER");
      rtDataSourceInfoBuilder.setReplicationPointer(replicationPointer);

      String jobId = function.getExportParameterList().getString("E_JOBCOUNT");
      rtDataSourceInfoBuilder.setExtractJobId(jobId);

      String jobName = function.getExportParameterList().getString("E_JOBNAME");
      rtDataSourceInfoBuilder.setExtractJobName(jobName);

      boolean isJobStatusFinished =
        SapOdpBatchSource.EXTRACTION_MODE_FULL.equals(function.getExportParameterList().getString("E_JOB_STATUS"));

      rtDataSourceInfoBuilder.setExtractJobStatusFinished(isJobStatusFinished);

      boolean isHierarchical = MARK_FIELD_TRUE_VALUE.equals(function.getExportParameterList().getString("E_IS_HIER"));
      rtDataSourceInfoBuilder.setHierarchical(isHierarchical);

      handleRuntimeException(function, ODP_PACKAGE_BUILDER_RFM, "T_RETURN");

    } catch (AbapException e) {
      if (e.getKey().equalsIgnoreCase(ERR_DATA_SOURCE_NOT_EXIST)) {
        String errMsg = ResourceConstants.ERR_SOURCE_OBJ_MISSING.getMsgForKey(sourceName);
        throw new ConnectorException(e.getKey(), errMsg, e);
      } else if (e.getKey().equalsIgnoreCase(ERR_DATA_SOURCE_NOT_EXPOSED)) {
        String errMsg = ResourceConstants.ERR_SOURCE_OBJ_NOT_EXPOSED.getMsgForKey(sourceName);
        throw new ConnectorException(e.getKey(), errMsg, e);
      } else if (e.getKey().equalsIgnoreCase("NOT_AUTHORIZED")) {
        String errMsg = ResourceConstants.ERR_NOT_AUTHORIZED_FOR_DATASOURCE.getMsgForKeyWithCode(sourceName);
        throw new ConnectorException(ResourceConstants.ERR_NOT_AUTHORIZED_FOR_DATASOURCE.getCode(), errMsg, e);
      }

      handleGenericException(e, ODP_PACKAGE_BUILDER_RFM);
    } catch (JCoException e) {
      handleGenericException(e, ODP_PACKAGE_BUILDER_RFM);
    }

    return rtDataSourceInfoBuilder;
  }

  /**
   * Configures the JCoTable with filter options
   *
   * @param filterOptions List of filter options
   * @param function
   */
  private void setFilterOptions(List<String> filterOptions, JCoFunction function) {
    if (filterOptions.isEmpty()) {
      return;
    }

    JCoTable tabList = function.getTableParameterList().getTable("T_FILTER");

    for (String filterOption : filterOptions) {
      String[] filterOptionsKVArr = filterOption.split(COLON);
      String fieldName = filterOptionsKVArr[0];
      String fieldVal = filterOptionsKVArr[1];

      tabList.appendRow();
      tabList.setValue("FIELDNAME", fieldName.toUpperCase());
      tabList.setValue("SIGN", "I");

      Matcher matcher = PATTERN_RANGE_FILTER_OPTION.matcher(fieldVal);
      if (matcher.find()) {
        String lowVal = matcher.group(1);
        String highVal = matcher.group(2);
        tabList.setValue("OPT", "BT");
        tabList.setValue("LOW", lowVal.trim());
        tabList.setValue("HIGH", highVal.trim());
      } else {
        tabList.setValue("OPT", "EQ");
        tabList.setValue("LOW", fieldVal);
      }
    }
  }

  /**
   * Retrieves the number of available work processes
   *
   * @param conn
   * @return
   * @throws ConnectorException
   */
  public SapOdpRuntimeConfigInfo.Builder getAvailableDialogWorkProcesses(SapConnection conn) throws ConnectorException {
    LOGGER.debug("Initiate get available dialog work processes");
    SapOdpRuntimeConfigInfo.Builder odpRuntimeConfigBuilder = SapOdpRuntimeConfigInfo.builder();

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
      odpRuntimeConfigBuilder.setTotalWorkProcCount(totalDialogWp);
      odpRuntimeConfigBuilder.setAvailableWorkProcCount(availableDialogWp);
    } catch (JCoException e) {
      handleGenericException(e, ODP_PACKAGE_BUILDER_RFM);
    }

    return odpRuntimeConfigBuilder;
  }

  /**
   * Retrieves the available memory
   *
   * @param conn
   * @return
   * @throws ConnectorException
   */
  public long getAvailableMemory(SapConnection conn) throws ConnectorException {
    LOGGER.trace("Initiate get available memory for work process");
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
      handleGenericException(e, ODP_PACKAGE_BUILDER_RFM);
    }

    return maxMemoryForWp;
  }

  /**
   * Handles exception thrown from SAP while executing RFM
   *
   * @param function
   * @param rfmName
   * @param tableParam
   * @throws ConnectorException
   */
  private void handleRuntimeException(JCoFunction function, String rfmName, String tableParam)
    throws ConnectorException {

    JCoTable fieldReturnTable = function.getTableParameterList().getTable(tableParam);
    if (!fieldReturnTable.isEmpty()) {
      List<String> errMsgList = new ArrayList<>();
      for (int j = 0; j < fieldReturnTable.getNumRows(); j++) {
        fieldReturnTable.setRow(j);
        // SAP RETURN table standard for error is 'E' (Error) and for fatal is 'A'
        // (Abort)
        if (fieldReturnTable.getString("TYPE").equals("E") || fieldReturnTable.getString("TYPE").equals("A")) {
          String message = fieldReturnTable.getString("MESSAGE");
          errMsgList.add(message);
        }
      }

      String errMsg = ResourceConstants.ERR_EXEC_FUNC.getMsgForKeyWithCode(rfmName) + "\n"
        + ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey() + errMsgList;

      throw new ConnectorException(ResourceConstants.ERR_EXEC_FUNC.getCode(), errMsg);
    }
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
