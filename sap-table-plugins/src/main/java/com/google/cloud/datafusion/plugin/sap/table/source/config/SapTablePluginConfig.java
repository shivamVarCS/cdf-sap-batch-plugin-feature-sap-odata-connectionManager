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

import com.google.cloud.ServiceOptions;
import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.GCSPath;
import com.google.cloud.datafusion.plugin.util.Util;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A version of {@link SapTablePluginConfigWrapper} that works with the
 * classloader hack performed in
 * {@link com.google.cloud.datafusion.plugin.sap.table.source.SapTableBatchSourceWrapper}.
 * 
 * @author sankalpbapat
 */
public class SapTablePluginConfig extends PluginConfig {
  private static final long serialVersionUID = -1355262032766665388L;

  private static final int QUERY_SPLIT_OFFSET = 255;

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
  private final String sapTable;
  private final String options;
  private final Long rowsToFetch;
  private final Integer numSplits;
  private final Long packageSize;
  private final String gcpProjectId;
  private final String gcsPath;
  private String schema;

  // only used by the classloader hack
  public SapTablePluginConfig(PluginConfig pluginConfig) {
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
    this.sapTable = properties.get(SapTablePluginConfigWrapper.EXTRACT_TABLE_VALUE);
    this.options = properties.get(SapTablePluginConfigWrapper.FILTER_OPTIONS);
    this.rowsToFetch = properties.containsKey(SapTablePluginConfigWrapper.ROWS_TO_FETCH)
      ? Long.parseLong(properties.get(SapTablePluginConfigWrapper.ROWS_TO_FETCH))
      : null;

    this.numSplits = properties.containsKey(SapTablePluginConfigWrapper.NUM_SPLITS)
      ? Integer.parseInt(properties.get(SapTablePluginConfigWrapper.NUM_SPLITS))
      : null;

    this.packageSize = properties.containsKey(SapTablePluginConfigWrapper.PACKAGE_SIZE)
      ? Long.parseLong(properties.get(SapTablePluginConfigWrapper.PACKAGE_SIZE))
      : null;

    this.schema = properties.get("schema");
    this.gcpProjectId = properties.get(SapJcoPluginConfigWrapper.GCP_PROJECT_ID);
    this.gcsPath = properties.get(SapJcoPluginConfigWrapper.GCS_PATH);
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

  public String getSapTable() {
    return sapTable;
  }

  @Nullable
  public String getOptions() {
    return options;
  }

  public List<String> getFormattedFilterOptions() {
    if (Util.isNotNullOrEmpty(options)) {
      return splitStringForJco(options, QUERY_SPLIT_OFFSET);
    } else {
      return Collections.emptyList();
    }
  }

  @Nullable
  public Long getRowsToFetch() {
    if (rowsToFetch == null) {
      return 0L;
    }

    return rowsToFetch;
  }

  @Nullable
  public Integer getNumSplits() {
    if (numSplits == null) {
      return 0;
    }

    return numSplits;
  }

  @Nullable
  public Long getPackageSize() {
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

  public GCSPath getGcsPath() {
    return GCSPath.from(gcsPath);
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
   * Splits the user input query string after every {@code querySplitOffset}
   * character, for the query validation RFM to use it as input.
   *
   * @param str         string
   * @param splitOffset offset to split the string at
   * @return List of original string's split fragments
   */
  private List<String> splitStringForJco(String str, int splitOffset) {
    List<String> frags;
    int iterCount = 1 + (str.length() - 1) / splitOffset;
    frags = new ArrayList<>(iterCount);
    // If query fragment is only 1, get value until the string length and not offset
    int endIdx = iterCount == 1 ? str.length() : splitOffset;
    for (int i = 0; i < iterCount; i++) {
      String frag = str.substring(i * splitOffset, endIdx);
      frags.add(frag);
      // Prepare end index for next iteration, if current is last but one iteration
      // (hence iterCount - 2), then end index for next (last iteration) is only till
      // string length. Else, add splitOffset
      endIdx = (i == iterCount - 2) ? str.length() : endIdx + splitOffset;
    }

    return frags;
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
}
