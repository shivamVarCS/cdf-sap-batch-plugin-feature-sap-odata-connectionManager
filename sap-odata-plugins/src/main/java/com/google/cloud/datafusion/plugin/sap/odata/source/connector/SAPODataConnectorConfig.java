package com.google.cloud.datafusion.plugin.sap.odata.source.connector;

import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import okhttp3.HttpUrl;

import javax.annotation.Nullable;

/**
 * ODataConnectorConfig
 */

public class SAPODataConnectorConfig extends PluginConfig {

    private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    public static final String BASE_URL = "baseURL";
    public static final String UNAME = "username";
    public static final String PASSWORD = "password";

    @Macro
    @Description("SAP Gateway OData Base URL.")
    protected String baseURL;

    @Description("Option to change the OData version to v2.")
    protected String oDataVersion;

    @Description("Basic SAP Username-Password credentials.")
    protected String securityType;

    @Macro
    @Description("SAP Logon user ID.")
    protected String username;

    @Macro
    @Description("SAP Logon password for user authentication.")
    protected String password;

    public SAPODataConnectorConfig(){}

    public SAPODataConnectorConfig(String baseURL, String oDataVersion, String securityType,
                                String username, String password){
        this.baseURL = baseURL;
        this.oDataVersion = oDataVersion;
        this.securityType = securityType;
        this.username = username;
        this.password=password;
    }


    @Nullable
    public String getBaseURL() {
        return this.baseURL;
    }

    @Nullable
    public String getODataVersion() {
        return this.oDataVersion;
    }

    @Nullable
    public String getSecurityType() {
        return this.securityType;
    }

    @Nullable
    public String getUsername() {
        return this.username;
    }

    @Nullable
    public String getPassword() {
        return this.password;
    }

    public String getProject() {
        String baseURL = tryGetProject();
        if (baseURL == null) {
            throw new IllegalArgumentException(
                    "Could not detect SAP OData Base URL. Please specify a correct URL.");
        }
        return baseURL;
    }

    @Nullable
    public String tryGetProject() {

        if (containsMacro(BASE_URL) && Strings.isNullOrEmpty(baseURL)) {
            return null;
        }
        String projectURL = baseURL;
        if (Strings.isNullOrEmpty(baseURL)) {
            projectURL = null;
        }
        return projectURL;
    }

    /**
     * Validates the parameters.
     *
     * @param failureCollector {@code FailureCollector}
     */
    public void validateBasicCredentials(FailureCollector failureCollector) {

        if (Util.isNullOrEmpty(getUsername()) && !containsMacro(UNAME)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP Logon Username");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(UNAME);
        }
        if (Util.isNullOrEmpty(getPassword()) && !containsMacro(PASSWORD)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP Logon Password");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(PASSWORD);
        }
    }

    /**
     * Validates the mandatory parameters.
     *
     * @param failureCollector {@code FailureCollector}
     */
    public void validateMandatoryParameters(FailureCollector failureCollector) {

        if (Util.isNullOrEmpty(getBaseURL()) && !containsMacro(SAPODataConnectorConfig.BASE_URL)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP OData Base URL");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(SAPODataConnectorConfig.BASE_URL);
        }
        if (Util.isNotNullOrEmpty(getBaseURL()) && !containsMacro(SAPODataConnectorConfig.BASE_URL)) {
            if (HttpUrl.parse(getBaseURL()) == null) {
                String errMsg = ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey("SAP OData Base URL");
                failureCollector.addFailure(errMsg, null).withConfigProperty(SAPODataConnectorConfig.BASE_URL);
            }
        }
    }

}
