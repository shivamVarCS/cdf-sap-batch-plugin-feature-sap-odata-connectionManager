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

package com.google.cloud.datafusion.plugin.sap.odp.source.config;

import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * SAP plugin UI configuration parameters and validation wrapper
 *
 * @author sankalpbapat
 */
public class SapOdpPluginConfigWrapper extends SapJcoPluginConfigWrapper {
  private static final long serialVersionUID = 8895910429655430670L;

  private static final Pattern SUBSCRIBER_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_/]+");

  // Widget UI labels
  public static final String SAP_SOURCE_NAME = "SAP ODP Datasource Name";
  public static final String EXTRACT_TYPE_LABEL = "Extract Type";
  public static final String EXTRACT_TYPE_FULL = "Full (All Data)";
  public static final String NUM_SPLITS_TO_GENERATE = "Number of Splits to Generate";
  public static final String PACKAGE_SIZE_LABEL = "Package Size";

  // Widgets UI fields' backend values
  public static final String EXTRACT_SOURCE_VALUE = "sapSourceObjName";
  public static final String FILTER_OPTIONS = "options";
  public static final String EXTRACT_TYPE = "extractType";
  public static final String EXTRACT_TYPE_FULL_VALUE = "Full";
  public static final String EXTRACT_TYPE_SYNC_VALUE = "Sync";
  public static final String FILTER_OPTION_EQUAL_VALUE = "filterOptionsEq";
  public static final String FILTER_OPTION_RANGE_VALUE = "filterOptionsRange";
  public static final String SUBSCRIBER_NAME = "subscriberName";
  public static final String NUM_SPLITS = "numSplits";
  public static final String PACKAGE_SIZE = "packageSize";

  @Macro
  @Description("ODP Datasource name (case insensitive) existing in SAP. "
    + "All fields for this Datasource will be extracted.")
  private String sapSourceObjName;

  @Macro
  @Description("Type of extract from Datasource, like Full or Sync")
  private String extractType;

  @Nullable
  @Macro
  @Description("Name of the ODP queue target subscriber used when extracting data from a Datasource. "
    + "This name must be unique for different pipelines extracting data from same datasources. This "
    + "name cannot have whitespace characters and must not exceed 32 characters. If not specified, "
    + "a subscriber name will be generated using the GCP project id, pipeline namespace, and pipeline name.")
  private String subscriberName;

  @Nullable
  @Macro
  @Description("List of metadata field names and values to use as filter options. This is a comma-separated list "
    + "of key-value pairs, where each pair is separated by a colon ':' and specifies the filter condition to apply "
    + "when reading data from datasource. Only records that satisfy the filters will be extracted. The filter key "
    + "corresponds to a field in the schema and must be a simple type (not an array, map, record, union, or enum). "
    + "The filter value indicates what value that field must be equal to in order to be read.")
  private String filterOptionsEq;

  @Nullable
  @Macro
  @Description("List of metadata field names and values to use as filter options. This is a comma-separated list "
    + "of key-value pairs, where each pair is separated by a colon ':' and specifies the filter condition to apply "
    + "when reading data from datasource. Only records that satisfy the filters will be extracted. The filter key "
    + "corresponds to a field in the schema and must be a simple type (not an array, map, record, union, or enum). "
    + "The filter value indicates what value that field must be equal to in order to be read. The filter value "
    + "indicates the range of low and high bounds in which that field's value must lie in order to be read, and "
    + "has the format 'low' AND 'high'.")
  private String filterOptionsRange;

  @Nullable
  @Macro
  @Description("Number of splits used to partition the input data. More partitions will increase "
    + "the level of parallelism, but will require more resources and overhead. If 0 or not specified, "
    + "the execution framework will pick an appropriate value.")
  private Long numSplits;

  @Nullable
  @Macro
  @Description("Size of each package of data in kilobytes to fetch in every network call to SAP. A smaller package "
    + "size will cause frequent network calls and greater overhead. A very large package size (> 100MB) may slow "
    + "down data retrieval. If no value is given, a default of 50MB is used.")
  private Long packageSize;

  @Nullable
  @Macro
  private String schema;

  // No-arg constructor used by framework
  public SapOdpPluginConfigWrapper() {
    super();
  }

  // Package private, only used for Junits
  SapOdpPluginConfigWrapper(Map<String, String> pluginProps) {
    this.sapSourceObjName = pluginProps.get(EXTRACT_SOURCE_VALUE);
    this.extractType = pluginProps.get(EXTRACT_TYPE);
    this.subscriberName = pluginProps.get(SUBSCRIBER_NAME);
    this.filterOptionsEq = pluginProps.get(FILTER_OPTION_EQUAL_VALUE);
    this.filterOptionsRange = pluginProps.get(FILTER_OPTION_RANGE_VALUE);

    this.numSplits = pluginProps.get(NUM_SPLITS) != null ? Long.parseLong(pluginProps.get(NUM_SPLITS)) : 0L;
    this.packageSize = pluginProps.get(PACKAGE_SIZE) != null ? Long.parseLong(pluginProps.get(PACKAGE_SIZE)) : 0L;
  }

  /**
   * Validates all UI mandatory parameters in case framework doesn't throw a
   * validation error.
   *
   * @param failureCollector
   */
  @Override
  protected void validateMandatoryProps(FailureCollector failureCollector) {
    super.validateMandatoryProps(failureCollector);

    String action = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    if (Util.isNullOrEmpty(sapSourceObjName) && !containsMacro(EXTRACT_SOURCE_VALUE)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_SOURCE_NAME);
      failureCollector.addFailure(errMsg, action).withConfigProperty(EXTRACT_SOURCE_VALUE);
    }

    if (Util.isNullOrEmpty(extractType) && !containsMacro(EXTRACT_TYPE)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(EXTRACT_TYPE_LABEL);
      failureCollector.addFailure(errMsg, action).withConfigProperty(EXTRACT_TYPE);
    } else if (Util.isNotNullOrEmpty(extractType) && !EXTRACT_TYPE_FULL_VALUE.equalsIgnoreCase(extractType)
      && !EXTRACT_TYPE_SYNC_VALUE.equalsIgnoreCase(extractType)) {

      String errMsg = ResourceConstants.ERR_INVALID_EXTRACT_TYPE.getMsgForKey(extractType);
      failureCollector.addFailure(errMsg, null).withConfigProperty(EXTRACT_TYPE);
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
    if (Util.isNotNullOrEmpty(subscriberName) && !containsMacro(SUBSCRIBER_NAME)
      && (!SUBSCRIBER_NAME_PATTERN.matcher(subscriberName).matches() || subscriberName.trim().length() > 32)) {

      String errMsg = ResourceConstants.ERR_INVALID_SUBSCRIBER_NAME.getMsgForKey(subscriberName);
      failureCollector.addFailure(errMsg, null).withConfigProperty(SUBSCRIBER_NAME);
    }

    String action = ResourceConstants.ERR_NEGATIVE_PARAM_ACTION.getMsgForKey();

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
