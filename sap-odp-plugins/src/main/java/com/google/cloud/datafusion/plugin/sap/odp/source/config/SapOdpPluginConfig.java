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

import com.google.cloud.ServiceOptions;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;
import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * A version of {@link SapOdpPluginConfigWrapper} that works with the classloader hack
 * performed in {@link com.google.cloud.datafusion.plugin.sap.odp.source.SapOdpBatchSourceWrapper}.
 *
 * @author sankalpbapat
 */
public class SapOdpPluginConfig extends PluginConfig {
  private static final long serialVersionUID = -1355262032766665388L;

  private static final String PATTERN_LIST_BRACKET = "(^\\[|\\]$)";
  private static final Pattern PATTERN_FILTER_OPTION_RANGE = Pattern.compile("(?i)\\S+\\s+AND\\s+\\S+");
  private static final String COMMA = ",";

  private final PluginConfig original;
  private final String referenceName;
  private final String client;
  private final String lang;
  private final String connType;
  private final String ashost;
  private final String sysnr;
  private final String sapRouter;
  private final String mshost;
  private final String msserv;
  private final String r3name;
  private final String group;
  private final String user;
  private final String paswd;
  private final String sapSource;
  private final String extractType;
  private final String subscriberName;
  private final String filterOptionsEq;
  private final String filterOptionsRange;
  private final Long numSplits;
  private final Long packageSize;
  private final String gcpProjectId;
  private final String gcsPath;
  private String schema;

  // only used by the classloader hack
  public SapOdpPluginConfig(PluginConfig pluginConfig) {
    this.original = pluginConfig;
    Map<String, String> properties = pluginConfig.getProperties().getProperties();
    this.referenceName = properties.get(Constants.Reference.REFERENCE_NAME);
    this.client = properties.get(SapJcoPluginConfigWrapper.JCO_CLIENT);
    this.lang = properties.get(SapJcoPluginConfigWrapper.JCO_LANG);
    this.connType = properties.get(SapJcoPluginConfigWrapper.CONNECTION_TYPE);
    this.ashost = properties.get(SapJcoPluginConfigWrapper.JCO_ASHOST);
    this.sysnr = properties.get(SapJcoPluginConfigWrapper.JCO_SYSNR);
    this.sapRouter = properties.get(SapJcoPluginConfigWrapper.JCO_SAPROUTER);
    this.mshost = properties.get(SapJcoPluginConfigWrapper.JCO_MSHOST);
    this.msserv = properties.get(SapJcoPluginConfigWrapper.JCO_MSSERV);
    this.r3name = properties.get(SapJcoPluginConfigWrapper.JCO_R3NAME);
    this.group = properties.get(SapJcoPluginConfigWrapper.JCO_GROUP);
    this.user = properties.get(SapJcoPluginConfigWrapper.JCO_USER);
    this.paswd = properties.get(SapJcoPluginConfigWrapper.JCO_PASSWD);
    this.sapSource = properties.get(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE);
    this.extractType = properties.get(SapOdpPluginConfigWrapper.EXTRACT_TYPE);
    this.subscriberName = properties.get(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME);
    this.filterOptionsEq = properties.get(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE);
    this.filterOptionsRange = properties.get(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE);

    this.numSplits = properties.containsKey(SapOdpPluginConfigWrapper.NUM_SPLITS)
      ? Long.parseLong(properties.get(SapOdpPluginConfigWrapper.NUM_SPLITS))
      : null;

    this.packageSize = properties.containsKey(SapOdpPluginConfigWrapper.PACKAGE_SIZE)
      ? Long.parseLong(properties.get(SapOdpPluginConfigWrapper.PACKAGE_SIZE))
      : null;

    this.schema = properties.get("schema");
    this.gcpProjectId = properties.get(SapJcoPluginConfigWrapper.GCP_PROJECT_ID);
    this.gcsPath = properties.get(SapJcoPluginConfigWrapper.GCS_PATH);
  }

  // Package private, only used for Junits
  SapOdpPluginConfig(PluginConfig pluginConfig, Map<String, String> pluginProps) {
    this.original = pluginConfig;
    this.referenceName = pluginProps.get(Constants.Reference.REFERENCE_NAME);
    this.client = pluginProps.get("client");
    this.lang = pluginProps.get("lang");
    this.connType = pluginProps.get(SapJcoPluginConfigWrapper.CONNECTION_TYPE);
    this.ashost = pluginProps.get("ashost");
    this.sysnr = pluginProps.get("sysnr");
    this.sapRouter = pluginProps.get("sapRouter");

    this.mshost = pluginProps.get("mshost");
    this.msserv = pluginProps.get("msserv");
    this.r3name = pluginProps.get("r3name");
    this.group = pluginProps.get("group");

    this.user = pluginProps.get("user");
    this.paswd = pluginProps.get("paswd");

    this.sapSource = pluginProps.get(SapOdpPluginConfigWrapper.EXTRACT_SOURCE_VALUE);
    this.extractType = pluginProps.get(SapOdpPluginConfigWrapper.EXTRACT_TYPE);
    this.subscriberName = pluginProps.get(SapOdpPluginConfigWrapper.SUBSCRIBER_NAME);
    this.filterOptionsEq = pluginProps.get(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE);
    this.filterOptionsRange = pluginProps.get(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE);
    this.numSplits = pluginProps.get(SapOdpPluginConfigWrapper.NUM_SPLITS) != null
      ? Long.parseLong(pluginProps.get(SapOdpPluginConfigWrapper.NUM_SPLITS))
      : 0L;

    this.packageSize = pluginProps.get(SapOdpPluginConfigWrapper.PACKAGE_SIZE) != null
      ? Long.parseLong(pluginProps.get(SapOdpPluginConfigWrapper.PACKAGE_SIZE))
      : 0L;

    this.gcpProjectId = pluginProps.get(SapJcoPluginConfigWrapper.GCP_PROJECT_ID);
    this.gcsPath = pluginProps.get(SapJcoPluginConfigWrapper.GCS_PATH);
  }

  @Override
  public boolean containsMacro(String fieldName) {
    return original.containsMacro(fieldName);
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getClient() {
    return client;
  }

  public String getLang() {
    return lang;
  }

  public String getConnType() {
    return connType;
  }

  @Nullable
  public String getAshost() {
    return ashost;
  }

  @Nullable
  public String getSysnr() {
    return sysnr;
  }

  @Nullable
  public String getSapRouter() {
    return sapRouter;
  }

  @Nullable
  public String getMshost() {
    return mshost;
  }

  @Nullable
  public String getMsserv() {
    return msserv;
  }

  @Nullable
  public String getR3name() {
    return r3name;
  }

  @Nullable
  public String getGroup() {
    return group;
  }

  public String getUser() {
    return user;
  }

  public String getPaswd() {
    return paswd;
  }

  public String getSapSource() {
    return sapSource;
  }

  public String getExtractType() {
    return extractType;
  }

  @Nullable
  public String getSubscriberName() {
    return subscriberName;
  }

  @Nullable
  public String getFilterOptionsEq() {
    return filterOptionsEq;
  }

  @Nullable
  public String getFilterOptionsRange() {
    return filterOptionsRange;
  }

  public List<String> getFormattedFilterOptions() {
    List<String> filterOptionsList = new ArrayList<>();

    if (Util.isNotNullOrEmpty(filterOptionsEq)) {
      String[] filterOptionsEqCheckArr = filterOptionsEq.split(COMMA);
      filterOptionsList.addAll(Arrays.asList(filterOptionsEqCheckArr));
    }

    if (Util.isNotNullOrEmpty(filterOptionsRange)) {
      String[] filterOptionsRangeCheckArr = filterOptionsRange.split(COMMA);
      filterOptionsList.addAll(Arrays.asList(filterOptionsRangeCheckArr));
    }

    return filterOptionsList;
  }

  public long getNumSplits() {
    if (numSplits == null) {
      return 0L;
    }

    return numSplits;
  }

  public long getPackageSizeKB() {
    if (packageSize == null) {
      return 0L;
    }

    return packageSize;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Util.isNotNullOrEmpty(schema) ? Schema.parseJson(schema) : null;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
    }
  }

  public String getGcsPathString() {
    return gcsPath;
  }

  public String getProject() {
    String projectId = tryGetProject();
    if (projectId == null && !containsMacro(SapJcoPluginConfigWrapper.GCP_PROJECT_ID)) {
      throw new IllegalArgumentException(
        "Could not detect Google Cloud Project ID from the environment. Please specify a Project ID.");
    }

    return projectId;
  }

  @Nullable
  private String tryGetProject() {
    if (SapJcoPluginConfigWrapper.AUTO_DETECT.equals(gcpProjectId)) {
      return ServiceOptions.getDefaultProjectId();
    }

    return gcpProjectId;
  }

  /**
   * Keeps connection parameters only for the user selected connection, security.
   *
   */
  public Map<String, String> getConnPropsByType() {
    Map<String, String> jcoPropMap = new HashMap<>();
    jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_CLIENT, client);
    jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_LANG, lang);
    jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_USER, user);
    jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_PASSWD, paswd);
    // Default or User entered values in fields persist and are populated to backend
    // even if the fields themselves are hidden due to a dependent filter
    if (SapJcoPluginConfigWrapper.CONN_TYPE_LOAD_BALANCED_VALUE.equals(connType)) {
      jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_MSHOST, mshost);
      jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_MSSERV, msserv);
      jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_R3NAME, r3name);
      jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_GROUP, group);
    } else {
      jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_ASHOST, ashost);
      jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_SYSNR, sysnr);
      if (Util.isNotNullOrEmpty(sapRouter)) {
        jcoPropMap.put(SapJcoPluginConfigWrapper.JCO_SAPROUTER, sapRouter);
      }
    }

    return jcoPropMap;
  }

  public boolean isConnectionReqd() {
    Map<String, String> jcoPropMap = getConnPropsByType();
    for (String jcoPropName : jcoPropMap.keySet()) {
      if (containsMacro(jcoPropName)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Validates the filter options for equal (EQ) and range (BW) check
   *
   * @param fieldMetaList    list of sapColumnDetails
   * @param failureCollector
   */
  public void validateFilterOptions(FailureCollector failureCollector, List<SapFieldMetadata> fieldMetaList) {
    // Filter options with equal check validation against list of sapColumnDetails
    // having isEqualFlag set as true
    if (Util.isNotNullOrEmpty(filterOptionsEq)) {
      List<String> missingEqFilteredFieldsList = new ArrayList<>();

      // Filtered list based on true value for isEqualFlag from sapColumnDetailsList
      List<SapFieldMetadata> equalFlagSapColumnDetailsList =
        fieldMetaList.stream().filter(SapFieldMetadata::isEqualFilterAllowed).collect(Collectors.toList());

      // Split key-value pair using , separator
      String[] filterOptionsEqCheckArr = filterOptionsEq.split(COMMA);

      for (String filterOptionKVPair : filterOptionsEqCheckArr) {
        // Split key-value pair using : delimiter
        String[] filterOptionArr = filterOptionKVPair.split(":");
        // Check whether user entered key as fieldName exists in the filtered list of
        // sapColumnsDetails list
        boolean isFieldPresent =
          equalFlagSapColumnDetailsList.stream().anyMatch(e -> e.getName().equalsIgnoreCase(filterOptionArr[0]));

        if (!isFieldPresent) {
          missingEqFilteredFieldsList.add(filterOptionArr[0]);
        }
      }

      if (!missingEqFilteredFieldsList.isEmpty()) {
        String err = ResourceConstants.ERR_FILTER_EQ_FIELD
          .getMsgForKey(missingEqFilteredFieldsList.toString().replaceAll(PATTERN_LIST_BRACKET, ""), sapSource);

        failureCollector.addFailure(err, null).withConfigProperty(SapOdpPluginConfigWrapper.FILTER_OPTION_EQUAL_VALUE);
      }
    }

    // Filter options with range validation against list of sapColumnDetails having
    // isBetweenFlag set as true
    if (Util.isNotNullOrEmpty(filterOptionsRange)) {
      List<String> missingBwFilteredFieldsList = new ArrayList<>();
      List<String> invalidValBwFilteredFieldsList = new ArrayList<>();

      // Filtered list based on true value for isBetweenFlag from sapColumnDetailsList
      List<SapFieldMetadata> betweenFlagSapColumnDetailsList =
        fieldMetaList.stream().filter(SapFieldMetadata::isBetweenFilterAllowed).collect(Collectors.toList());

      // Split key-value pair using , separator
      String[] filterOptionsRangeCheckArr = filterOptionsRange.split(COMMA);

      for (String filterOptionKVPair : filterOptionsRangeCheckArr) {
        // Split key-value pair using : delimiter
        String[] filterOptionArr = filterOptionKVPair.split(":");
        // Check whether user entered key as fieldName exists in the filtered list of
        // sapColumnsDetails list
        boolean isFieldPresent =
          betweenFlagSapColumnDetailsList.stream().anyMatch(e -> e.getName().equalsIgnoreCase(filterOptionArr[0]));

        if (!isFieldPresent) {
          missingBwFilteredFieldsList.add(filterOptionArr[0]);
        }

        Matcher matcher = PATTERN_FILTER_OPTION_RANGE.matcher(filterOptionArr[1]);
        if (!matcher.find()) {
          invalidValBwFilteredFieldsList.add(filterOptionArr[0]);
        }
      }

      if (!missingBwFilteredFieldsList.isEmpty()) {
        String err = ResourceConstants.ERR_FILTER_RANGE_FIELD
          .getMsgForKey(missingBwFilteredFieldsList.toString().replaceAll(PATTERN_LIST_BRACKET, ""), sapSource);

        failureCollector.addFailure(err, null).withConfigProperty(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE);
      }

      if (!invalidValBwFilteredFieldsList.isEmpty()) {
        String err = ResourceConstants.ERR_FILTER_RANGE_VALUE
          .getMsgForKey(invalidValBwFilteredFieldsList.toString().replaceAll(PATTERN_LIST_BRACKET, ""), sapSource);

        failureCollector.addFailure(err, null).withConfigProperty(SapOdpPluginConfigWrapper.FILTER_OPTION_RANGE_VALUE);
      }
    }
  }
}
