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

package com.google.cloud.datafusion.plugin.sap.table.source.config;

import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * SAP plugin UI configuration parameters and validation wrapper
 *
 * @author sankalpbapat
 *
 */
public class SapTablePluginConfigWrapper extends SapJcoPluginConfigWrapper {
  private static final long serialVersionUID = 8895910429655430670L;

  // Widget UI labels
  public static final String SAP_TABLE_NAME = "SAP Table/View Name";
  public static final String NUM_ROWS_TO_FETCH = "Number of Rows to Fetch";
  public static final String NUM_SPLITS_TO_GENERATE = "Number of Splits to Generate";
  public static final String PACKAGE_SIZE_LABEL = "Package Size";

  // Widgets UI fields' backend values
  public static final String EXTRACT_TABLE_VALUE = "sapTable";
  public static final String FILTER_OPTIONS = "options";
  public static final String ROWS_TO_FETCH = "rowsToFetch";
  public static final String NUM_SPLITS = "numSplits";
  public static final String PACKAGE_SIZE = "packageSize";

  @Macro
  @Description("A valid table/view name (case insensitive) existing in SAP. "
    + "All columns for this table/view will be extracted.")
  private String sapTable;

  @Nullable
  @Macro
  @Description("WHERE clause of the SELECT. For example KEY6 LT '25' AND KEY4 EQ '93'")
  private String options;

  @Nullable
  @Macro
  @Description("Total number of rows to be extracted (accounts for conditions specified in Options). "
    + "0 or no input means no restriction (all rows).")
  private Long rowsToFetch;

  @Nullable
  @Macro
  @Description("The number of splits used to partition the input data. More partitions will increase "
    + "the level of parallelism, but will require more resources and overhead. If 0 or not specified, "
    + "the execution framework will pick an appropriate value.")
  private Integer numSplits;

  @Nullable
  @Macro
  @Description("Number of rows to fetch in each network call to SAP. Smaller size will cause frequent network calls "
    + "repeating the associated overhead. A large size (> 100K) may slow down data retrieval & cause excessive "
    + "resource usage in SAP. If 0 or not specified, the execution framework will pick an appropriate value.")
  private Long packageSize;

  @Nullable
  @Macro
  private String schema;

  // No-arg constructor used by framework
  public SapTablePluginConfigWrapper() {
    super();
  }

  // Package private, only used for Junits
  SapTablePluginConfigWrapper(Map<String, String> pluginProps) {
    this.sapTable = pluginProps.get(EXTRACT_TABLE_VALUE);
    this.options = pluginProps.get(FILTER_OPTIONS);
  }

  /**
   * Validates all UI mandatory parameters in case framework doesn't throw a
   * validation error.
   * 
   * @param failureCollector
   */
  @Override
  protected void validateMandatoryProps(FailureCollector failureCollector) {
    String action = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    super.validateMandatoryProps(failureCollector);

    if (Util.isNullOrEmpty(sapTable) && !containsMacro(EXTRACT_TABLE_VALUE)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_TABLE_NAME);
      failureCollector.addFailure(errMsg, action).withConfigProperty(EXTRACT_TABLE_VALUE);
    }
  }

  /**
   * Validates whether correct values for optional parameters are filled on the
   * UI. If not, then sets a friendly error message to {@code failureCollector}
   * 
   * @param failureCollector
   */
  @Override
  public void validateOptionalProps(FailureCollector failureCollector) {
    String action = ResourceConstants.ERR_NEGATIVE_PARAM_ACTION.getMsgForKey();

    if (rowsToFetch != null && !containsMacro(ROWS_TO_FETCH) && rowsToFetch < 0) {
      String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(NUM_ROWS_TO_FETCH);
      failureCollector.addFailure(errMsg, action).withConfigProperty(ROWS_TO_FETCH);
    }

    if (numSplits != null && !containsMacro(NUM_SPLITS) && numSplits < 0) {
      String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(NUM_SPLITS_TO_GENERATE);
      failureCollector.addFailure(errMsg, action).withConfigProperty(NUM_SPLITS);
    }

    if (packageSize != null && !containsMacro(PACKAGE_SIZE) && packageSize < 0) {
      String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey(PACKAGE_SIZE_LABEL);
      failureCollector.addFailure(errMsg, action).withConfigProperty(PACKAGE_SIZE);
    }
  }
}
