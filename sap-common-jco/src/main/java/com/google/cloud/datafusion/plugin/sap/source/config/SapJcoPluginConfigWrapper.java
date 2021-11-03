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

package com.google.cloud.datafusion.plugin.sap.source.config;

import com.google.cloud.ServiceOptions;
import com.google.cloud.datafusion.plugin.util.GCSPath;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * SAP plugin JCo specific UI configuration parameters and validation wrapper
 *
 * @author sankalpbapat
 *
 */
public class SapJcoPluginConfigWrapper extends PluginConfig {
  private static final long serialVersionUID = 8895910429655430670L;

  // Widget UI labels
  public static final String REFERENCE_NAME = "Reference Name";
  public static final String SAP_CLIENT = "SAP Client";
  public static final String SAP_LANGUAGE = "SAP Language";
  public static final String SAP_USERNAME = "SAP Logon Username";
  public static final String SAP_PASSWD = "SAP Logon Password";
  public static final String CONN_TYPE_DIRECT = "Direct (via SAP Application Server)";
  public static final String CONN_TYPE_LOAD_BALANCED = "Load Balanced (via SAP Message Server)";
  public static final String CONN_DIRECT_ASHOST = "SAP Application Server Host";
  public static final String CONN_DIRECT_SYSNR = "SAP System Number";
  public static final String CONN_LOAD_BALANCED_MSHOST = "SAP Message Server Host";
  public static final String CONN_LOAD_BALANCED_MSSERV = "SAP Message Server Service or Port Number";
  public static final String CONN_LOAD_BALANCED_R3NAME = "SAP System ID (SID)";
  public static final String CONN_LOAD_BALANCED_GROUP = "SAP Logon Group Name";
  public static final String GCP_PROJECT_ID_LABEL = "GCP Project ID";
  public static final String JCO_BUCKET_PATH_LABEL = "SAP JCo Library GCS Path";

  // Widgets UI fields' backend values
  public static final String JCO_CLIENT = "jco.client.client";
  public static final String JCO_LANG = "jco.client.lang";
  public static final String JCO_ASHOST = "jco.client.ashost";
  public static final String JCO_SYSNR = "jco.client.sysnr";
  public static final String JCO_MSHOST = "jco.client.mshost";
  public static final String JCO_MSSERV = "jco.client.msserv";
  public static final String JCO_R3NAME = "jco.client.r3name";
  public static final String JCO_GROUP = "jco.client.group";
  public static final String JCO_USER = "jco.client.user";
  public static final String JCO_PASSWD = "jco.client.passwd";
  public static final String JCO_SAPROUTER = "jco.client.saprouter";
  public static final String CONNECTION_TYPE = "connType";
  public static final String CONN_TYPE_DIRECT_VALUE = "directClient";
  public static final String CONN_TYPE_LOAD_BALANCED_VALUE = "msgServer";
  public static final String GCP_PROJECT_ID = "gcpProjectId";
  public static final String GCS_PATH = "gcsPath";

  public static final String AUTO_DETECT = "auto-detect";

  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Macro
  @Name(JCO_CLIENT)
  @Description("SAP Client")
  private String client;

  @Macro
  @Name(JCO_LANG)
  @Description("SAP Logon language")
  private String lang;

  // The @Name annotation tells CDAP what the property name is. It is optional,
  // and defaults to the variable name.
  // Note: only primitives (including boxed types) and string are the types that
  // are supported
  @Description("The type of connection with SAP")
  private String connType;

  @Nullable
  @Macro
  @Name(JCO_ASHOST)
  @Description("SAP Application Server Hostname or IP")
  private String ashost;

  @Nullable
  @Macro
  @Name(JCO_SYSNR)
  @Description("SAP System Number")
  private String sysnr;

  @Nullable
  @Macro
  @Name(JCO_SAPROUTER)
  @Description("SAP Router string to use for networks being protected by a firewall")
  private String sapRouter;

  @Nullable
  @Macro
  @Name(JCO_MSHOST)
  @Description("SAP Message Server Hostname or IP")
  private String mshost;

  @Nullable
  @Macro
  @Name(JCO_MSSERV)
  @Description("SAP Message Server service or port number")
  private String msserv;

  @Nullable
  @Macro
  @Name(JCO_R3NAME)
  @Description("System ID of the SAP system, the so-called SID")
  private String r3name;

  @Nullable
  @Macro
  @Name(JCO_GROUP)
  @Description("Logon group name of SAP application servers")
  private String group;

  @Macro
  @Name(JCO_USER)
  @Description("SAP Logon User")
  private String user;

  @Macro
  @Name(JCO_PASSWD)
  @Description("SAP Logon Password")
  private String paswd;

  @Macro
  @Description("Google Cloud Project ID, which uniquely identifies a project. "
    + "It can be found on the Dashboard in the Google Cloud Platform Console.")
  private String gcpProjectId;

  @Macro
  @Description("The Google Cloud Storage path which contains the user uploaded SAP JCo library files.")
  private String gcsPath;

  public SapJcoPluginConfigWrapper() {
    Map<String, String> properties = getProperties().getProperties();
    this.referenceName = properties.get(Constants.Reference.REFERENCE_NAME);
    this.client = properties.get(JCO_CLIENT);
    this.lang = properties.get(JCO_LANG);

    this.connType = properties.get(CONNECTION_TYPE);
    this.ashost = properties.get(JCO_ASHOST);
    this.sysnr = properties.get(JCO_SYSNR);
    this.sapRouter = properties.get(JCO_SAPROUTER);

    this.mshost = properties.get(JCO_MSHOST);
    this.msserv = properties.get(JCO_MSSERV);
    this.r3name = properties.get(JCO_R3NAME);
    this.group = properties.get(JCO_GROUP);

    this.user = properties.get(JCO_USER);
    this.paswd = properties.get(JCO_PASSWD);

    this.gcpProjectId = properties.get(GCP_PROJECT_ID);
    this.gcsPath = properties.get(GCS_PATH);
  }

  // Package private, only used for Junits
  SapJcoPluginConfigWrapper(Map<String, String> pluginProps) {
    this.referenceName = pluginProps.get("referenceName");
    this.client = pluginProps.get("client");
    this.lang = pluginProps.get("lang");
    this.connType = pluginProps.get(CONNECTION_TYPE);
    this.ashost = pluginProps.get("ashost");
    this.sysnr = pluginProps.get("sysnr");
    this.sapRouter = pluginProps.get("sapRouter");

    this.mshost = pluginProps.get("mshost");
    this.msserv = pluginProps.get("msserv");
    this.r3name = pluginProps.get("r3name");
    this.group = pluginProps.get("group");

    this.user = pluginProps.get("user");
    this.paswd = pluginProps.get("paswd");

    this.gcpProjectId = pluginProps.get(GCP_PROJECT_ID);
    this.gcsPath = pluginProps.get(GCS_PATH);
  }

  public String getGcsPathString() {
    return gcsPath;
  }

  public GCSPath getGcsPath() {
    return GCSPath.from(getGcsPathString());
  }

  public String getProject() {
    String projectId = tryGetProject();
    if (Util.isNullOrEmpty(projectId) && !containsMacro(GCP_PROJECT_ID)) {
      throw new IllegalArgumentException(
        "Could not detect Google Cloud Project ID from the environment. Please specify a Project ID.");
    }

    return projectId;
  }

  @Nullable
  private String tryGetProject() {
    if (AUTO_DETECT.equals(gcpProjectId)) {
      return ServiceOptions.getDefaultProjectId();
    }

    return gcpProjectId;
  }

  public void validateGcpParams(FailureCollector failureCollector) {
    String action = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    if (Util.isNullOrEmpty(gcpProjectId) && !containsMacro(GCP_PROJECT_ID)) {
      String err = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(GCP_PROJECT_ID_LABEL);
      failureCollector.addFailure(err, action).withConfigProperty(GCP_PROJECT_ID);
    }

    if (Util.isNullOrEmpty(gcsPath) && !containsMacro(GCS_PATH)) {
      String err = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(JCO_BUCKET_PATH_LABEL);
      failureCollector.addFailure(err, action).withConfigProperty(GCS_PATH);
    }
  }

  private void validateMsgServerParams(FailureCollector failureCollector) {
    String action = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    if (Util.isNullOrEmpty(mshost) && !containsMacro(JCO_MSHOST)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_FOR_CONN_PREFIX.getMsgForKey(CONN_LOAD_BALANCED_MSHOST,
        CONN_TYPE_LOAD_BALANCED);

      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_MSHOST);
    }

    if (Util.isNullOrEmpty(msserv) && !containsMacro(JCO_MSSERV)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_FOR_CONN_PREFIX.getMsgForKey(CONN_LOAD_BALANCED_MSSERV,
        CONN_TYPE_LOAD_BALANCED);

      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_MSSERV);
    }

    if (Util.isNullOrEmpty(r3name) && !containsMacro(JCO_R3NAME)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_FOR_CONN_PREFIX.getMsgForKey(CONN_LOAD_BALANCED_R3NAME,
        CONN_TYPE_LOAD_BALANCED);

      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_R3NAME);
    }

    if (Util.isNullOrEmpty(group) && !containsMacro(JCO_GROUP)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_FOR_CONN_PREFIX.getMsgForKey(CONN_LOAD_BALANCED_GROUP,
        CONN_TYPE_LOAD_BALANCED);

      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_GROUP);
    }
  }

  private void validateDirectParams(FailureCollector failureCollector) {
    String action = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    if (Util.isNullOrEmpty(ashost) && !containsMacro(JCO_ASHOST)) {
      String errMsg =
        ResourceConstants.ERR_MISSING_PARAM_FOR_CONN_PREFIX.getMsgForKey(CONN_DIRECT_ASHOST, CONN_TYPE_DIRECT);

      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_ASHOST);
    }

    if (Util.isNullOrEmpty(sysnr) && !containsMacro(JCO_SYSNR)) {
      String errMsg =
        ResourceConstants.ERR_MISSING_PARAM_FOR_CONN_PREFIX.getMsgForKey(CONN_DIRECT_SYSNR, CONN_TYPE_DIRECT);

      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_SYSNR);
    }
  }

  /**
   * Validates all UI mandatory parameters in case framework doesn't throw a
   * validation error.
   * 
   * @param failureCollector
   */
  protected void validateMandatoryProps(FailureCollector failureCollector) {
    String action = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    if (Util.isNullOrEmpty(referenceName)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(REFERENCE_NAME);
      String woMacroAction = ResourceConstants.ERR_MISSING_PARAM_ACTION.getMsgForKey();
      failureCollector.addFailure(errMsg, woMacroAction).withConfigProperty(Constants.Reference.REFERENCE_NAME);
    }

    if (Util.isNullOrEmpty(client) && !containsMacro(JCO_CLIENT)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_CLIENT);
      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_CLIENT);
    }

    if (Util.isNullOrEmpty(lang) && !containsMacro(JCO_LANG)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_LANGUAGE);
      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_LANG);
    }

    if (Util.isNullOrEmpty(user) && !containsMacro(JCO_USER)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_USERNAME);
      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_USER);
    }

    if (Util.isNullOrEmpty(paswd) && !containsMacro(JCO_PASSWD)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_PASSWD);
      failureCollector.addFailure(errMsg, action).withConfigProperty(JCO_PASSWD);
    }
  }

  /**
   * Validates whether correct connection/security parameters are filled on the
   * UI, corresponding to the selected connection and security type. If not, then
   * sets a friendly error message to {@code failureCollector}
   * 
   * @param failureCollector
   */
  public void validateConnProps(FailureCollector failureCollector) {
    validateMandatoryProps(failureCollector);
    // Validate mandatory params for Load Balanced or Direct connection
    if (CONN_TYPE_LOAD_BALANCED_VALUE.equals(connType)) {
      validateMsgServerParams(failureCollector);
    } else {
      validateDirectParams(failureCollector);
    }
  }

  /**
   * Validates whether correct values for optional parameters are filled on the
   * UI. If not, then sets a friendly error message to {@code failureCollector}
   * 
   * @param failureCollector
   */
  public void validateOptionalProps(FailureCollector failureCollector) {
    // No optional property to validate here, but child classes may want to define
    // some.
  }

  public boolean isClassLoadingReqd() {
    return !containsMacro(GCP_PROJECT_ID) && !containsMacro(GCS_PATH);
  }
}
