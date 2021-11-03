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

package com.google.cloud.datafusion.plugin.sap.odata.source.config;

import com.google.cloud.datafusion.plugin.sap.odata.source.SapODataService;
import com.google.cloud.datafusion.plugin.sap.odata.source.connector.SAPODataConnectorConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import com.google.cloud.datafusion.plugin.sap.odata.source.util.ExceptionParser;
import com.google.cloud.datafusion.plugin.util.GCSPath;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferencePluginConfig;
import okhttp3.HttpUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * This {@code SapODataPluginConfig} contains all SAP OData plugin UI configuration parameters.
 */

public class SapODataPluginConfig extends ReferencePluginConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SapODataPluginConfig.class);

    private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

    //public static final String BASE_URL = "baseURL";
    public static final String SERVICE_NAME = "serviceName";
    public static final String ENTITY_NAME = "entityName";
    //public static final String UNAME = "username";
    //public static final String PASSWORD = "password";
    public static final String GCP_PROJECT_ID = "gcpProjectId";
    public static final String CERT_GCS_PATH = "certGcsPath";
    public static final String CERT_PASSPHRASE = "certPassphrase";
    public static final String SELECT_OPTION = "selectOption";
    public static final String SKIP_ROW_COUNT = "skipRowCount";
    public static final String NUM_ROWS_TO_FETCH = "numRowsToFetch";
    public static final String SPLIT_COUNT = "splitCount";
    public static final String BATCH_SIZE = "batchSize";
    public static final String NAME_CONNECTION = "connection";
    public static final String NAME_USE_CONNECTION = "useConnection";


    /**
     * Basic parameters.
     */
    /*@Macro
    @Description("SAP Gateway OData Base URL.")
    private String baseURL;

    @Description("Option to change the OData version to v2.")
    private String oDataVersion;

     */

    @Macro
    @Description("Name of the SAP OData service from which the user wants to extract an Entity.")
    private String serviceName;

    @Macro
    @Description("Name of the Entity which is being extracted.")
    private String entityName;

      /**
       * Credentials parameters
       */
    /*@Description("Basic SAP Username-Password credentials.")
    private String securityType;

    @Macro
    @Description("SAP Logon user ID.")
    private String username;

    @Macro
    @Description("SAP Logon password for user authentication.")
    private String password;
    */
    @Nullable
    @Macro
    @Description("Google Cloud Project ID, which uniquely identifies a project. "
            + "It can be found on the Dashboard in the Google Cloud Platform Console." +
            "This is mandatory in case 'X.509 Certificate GCS Path' field contains any non-macro value." +
            "Default: auto-detect")
    private String gcpProjectId;

    @Nullable
    @Macro
    @Description("Google Cloud Storage path which contains the user uploaded X.509 certificate " +
            "corresponding to the SAP application server for secure calls.")
    private String certGcsPath;

    @Nullable
    @Macro
    @Description("Passphrase corresponding to the provided X.509 certificate.")
    private String certPassphrase;

    /**
     * Advanced parameters
     */
    @Nullable
    @Macro
    @Description("Filter condition to restrict the output data volume e.g. Price gt 200")
    private String filterOption;

    @Nullable
    @Macro
    @Description("Fields to be preserved in the extracted data e.g.: Category,Price,Name,Supplier/Address")
    private String selectOption;

    @Nullable
    @Macro
    @Description("List of complex fields to be expanded in the extracted output data e.g.: Products,Products/Suppliers")
    private String expandOption;

    @Nullable
    @Macro
    @Description("Rows to skip e.g.: 10. Values such as 0 or no input means no restriction (all rows).")
    private Long skipRowCount;

    @Nullable
    @Macro
    @Description("Total number of rows to be extracted (accounts for conditions specified in Filter Options ($filter))."
            + "Value such as 0 or no input means no restriction (all rows).")
    private Long numRowsToFetch;

    @Nullable
    @Macro
    @Description("The number of splits used to partition the input data. More partitions will increase the level of " +
            "parallelism, but will require more resources and overhead. Value such as 0 or no input means data " +
            "extraction" +
            " will happen on system calculated optimized split count, i.e. total number of records to extract /" +
            " batch size.")
    private Integer splitCount;

    @Nullable
    @Macro
    @Description("Number of rows to fetch in each network call to SAP. Smaller size will cause frequent " +
            "network calls repeating the associated overhead. A large size may slow down data retrieval & cause " +
            "excessive resource usage in SAP. Value such as 0 or no input means, default (1000) records will be " +
            "extracted. " +
            "Default: 1000 & Max: 5000")
    private Long batchSize;

    @Name(NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(NAME_CONNECTION)
    @Nullable
    @Macro
    @Description("The existing connection to use.")
    private SAPODataConnectorConfig connection;

    public SapODataPluginConfig(
                              String referenceName,
                              /*String baseURL,
                              String oDataVersion,
                              String securityType,
                              @Nullable String username,
                              @Nullable String password,*/
                              String serviceName,
                              String entityName,

                                @Nullable String gcpProjectId,
                                @Nullable String certGcsPath,
                                @Nullable String certPassphrase,
                                @Nullable String filterOption,
                                @Nullable String selectOption,
                                @Nullable String expandOption,
                                @Nullable Long skipRowCount,
                                @Nullable Long numRowsToFetch,
                                @Nullable Integer splitCount,
                                @Nullable Long batchSize,
                                SAPODataConnectorConfig connection) {

        super(referenceName);
    /*   connection.getBaseURL() = baseURL;
    /*this.oDataVersion = oDataVersion;
    this.securityType = securityType;
    this.username = username;
    this.password = password;*/
        this.serviceName = serviceName;
        this.entityName = entityName;
        this.gcpProjectId = gcpProjectId;
        this.certGcsPath = certGcsPath;
        this.certPassphrase = certPassphrase;
        this.filterOption = filterOption;
        this.selectOption = selectOption;
        this.expandOption = expandOption;
        this.skipRowCount = skipRowCount;
        this.numRowsToFetch = numRowsToFetch;
        this.splitCount = splitCount;
        this.batchSize = batchSize;
        this.connection = connection;
    }

    @Nullable
    public String getReferenceName() {
        return this.referenceName;
    }

    @Nullable
    public String getBaseURL() {
        return trim(connection.getBaseURL());
    }

    @Nullable
    public String getODataVersion() {
        return trim(connection.getODataVersion());
    }

    @Nullable
    public String getServiceName() { return trim(this.serviceName);
    }

    @Nullable
    public String getEntityName() { return trim(this.entityName);
    }

    @Nullable
    public String getSecurityType() {
        return connection.getSecurityType();
    }

    @Nullable
    public String getUsername() { return trim(connection.getUsername());
    }

    @Nullable
    public String getPassword() { return (connection.getPassword());
    }

    @Nullable
    public String getGcpProjectId() {
        return trim(this.gcpProjectId);
    }

    @Nullable
    public String getCertGcsPath() {
        return trim(this.certGcsPath);
    }

    @Nullable
    public String getCertPassphrase() {
        return this.certPassphrase;
    }

    @Nullable
    public String getFilterOption() {
        // Plugin UI field is 'textarea' so the user can input multiline filter statement
        // and as line break are not supported in URI
        // so any line break is removed from the filter option.
        return removeLinebreak(this.filterOption);
    }

    @Nullable
    public String getSelectOption() {
        // Plugin UI field is 'textarea' so the user can input multiline select statement
        // and as line break are not supported in URI and any select column name with extra
        // spaces are considered as actual column by SAP OData services and results in 'Not Found'
        // so to avoid the 'Not Found' scenarios
        // any line break and extra spaces is removed from the select option.
        // e.g.
        //    $select = col1, col2,
        //                col3,col4
        //    will be convert to col1,col2,col3,col4
        return removeWhitespace(this.selectOption);
    }

    @Nullable
    public String getExpandOption() {
        return removeWhitespace(this.expandOption);
    }

    @Nullable
    public Long getSkipRowCount() {
        return this.skipRowCount;
    }

    @Nullable
    public Long getNumRowsToFetch() {
        return this.numRowsToFetch;
    }

    @Nullable
    public Integer getSplitCount() {
        return this.splitCount;
    }

    @Nullable
    public Long getBatchSize() {
        return this.batchSize;
    }

    public SAPODataConnectorConfig getConnection() {
        return connection;
    }

    /**
     * Checks if the call to SAP OData service is required for metadata creation.
     * condition parameters: ['host' | 'serviceName' | 'entityName' | 'username' | 'password']
     * - any parameter is 'macro' then it returns 'false'
     *
     * @return boolean flag as per the check
     */
    public boolean isSchemaBuildRequired() {
        LOGGER.debug("Checking output schema creation is required or not.");
        if (containsMacro(SAPODataConnectorConfig.BASE_URL) || containsMacro(SERVICE_NAME) || containsMacro(ENTITY_NAME)) {
            return false;
        }

        return !containsMacro(SAPODataConnectorConfig.UNAME) && !containsMacro(SAPODataConnectorConfig.PASSWORD);
    }

    /**
     * Validates the given {@code SapODataPluginConfig} and throws the relative error messages.
     *
     * @param failureCollector {@code FailureCollector}
     */
    public void validatePluginParameters(FailureCollector failureCollector) {

        LOGGER.debug("Validating mandatory parameters.");
        validateMandatoryParameters(failureCollector);

        LOGGER.debug("Validating the Basic Security Type parameters.");
        validateBasicCredentials(failureCollector);

        LOGGER.debug("Validating the SAP X.509 Details.");
        validateGcpParams(failureCollector);

        LOGGER.debug("Validating the advanced parameters.");
        validateAdvanceParameters(failureCollector);

        validateEntityParameter(failureCollector);

        failureCollector.getOrThrowException();
    }

    /**
     * Validates the mandatory parameters.
     *
     * @param failureCollector {@code FailureCollector}
     */
    private void validateMandatoryParameters(FailureCollector failureCollector) {

        if (Util.isNullOrEmpty(getReferenceName())) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Reference Name");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(Constants.Reference.REFERENCE_NAME);
        } else if (!Util.isValidDatasetId(getReferenceName())) {
            String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("Reference Name");
            String refAction = ResourceConstants.ERR_INVALID_REFERENCE_PARAM_ACTION.getMsgForKey();
            failureCollector.addFailure(errMsg, refAction).withConfigProperty(Constants.Reference.REFERENCE_NAME);
        }
        if (Util.isNullOrEmpty(connection.getBaseURL()) && !containsMacro(SAPODataConnectorConfig.BASE_URL)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP OData Base URL");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(SAPODataConnectorConfig.BASE_URL);
        }
        if (Util.isNotNullOrEmpty(connection.getBaseURL()) && !containsMacro(SAPODataConnectorConfig.BASE_URL)) {
            if (HttpUrl.parse(connection.getBaseURL()) == null) {
                String errMsg = ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey("SAP OData Base URL");
                failureCollector.addFailure(errMsg, null).withConfigProperty(SAPODataConnectorConfig.BASE_URL);
            }
        }
        if (Util.isNullOrEmpty(getServiceName()) && !containsMacro(SERVICE_NAME)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Service Name");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(SERVICE_NAME);
        }
        if (Util.isNullOrEmpty(getEntityName()) && !containsMacro(ENTITY_NAME)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Entity Name");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(ENTITY_NAME);
        }
    }

    /**
     * Validates the credentials parameters.
     *
     * @param failureCollector {@code FailureCollector}
     */
    public void validateBasicCredentials(FailureCollector failureCollector) {

        if (Util.isNullOrEmpty(connection.getUsername()) && !containsMacro(SAPODataConnectorConfig.UNAME)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP Logon Username");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(SAPODataConnectorConfig.UNAME);
        }
        if (Util.isNullOrEmpty(connection.getPassword()) && !containsMacro(SAPODataConnectorConfig.PASSWORD)) {
            String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP Logon Password");
            failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(SAPODataConnectorConfig.PASSWORD);
        }
    }

    /**
     * Validates the SAP X.509 details parameters.
     *
     * @param failureCollector {@code FailureCollector}
     */
    private void validateGcpParams(FailureCollector failureCollector) {

        if (containsMacro(CERT_GCS_PATH) || Util.isNotNullOrEmpty(getCertGcsPath())) {

            if (!containsMacro(GCP_PROJECT_ID) && Util.isNullOrEmpty(getGcpProjectId())) {
                String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("GCP Project ID");
                failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(GCP_PROJECT_ID);
            }

            if (!containsMacro(CERT_GCS_PATH)) {
                try {
                    GCSPath.from(getCertGcsPath());
                } catch (IllegalArgumentException iae) {
                    String errMsg = iae.getMessage();
                    failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(CERT_GCS_PATH);
                }

            }
            if (Util.isNullOrEmpty(getCertPassphrase()) && !containsMacro(CERT_PASSPHRASE)) {
                String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Passphrase");
                failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(CERT_PASSPHRASE);
            }
        }
    }

    /**
     * Validates the advance parameters.
     *
     * @param failureCollector {@code FailureCollector}
     */
    private void validateAdvanceParameters(FailureCollector failureCollector) {

        String action = ResourceConstants.ERR_NEGATIVE_PARAM_ACTION.getMsgForKey();

        if (getSkipRowCount() != null && !containsMacro(SKIP_ROW_COUNT) && getSkipRowCount() < 0) {
            String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("Number of Rows to Skip");
            failureCollector.addFailure(errMsg, action).withConfigProperty(SKIP_ROW_COUNT);
        }

        if (getNumRowsToFetch() != null && !containsMacro(NUM_ROWS_TO_FETCH) && getNumRowsToFetch() < 0) {
            String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("Number of Rows to Fetch");
            failureCollector.addFailure(errMsg, action).withConfigProperty(NUM_ROWS_TO_FETCH);
        }

        if (getSplitCount() != null && !containsMacro(SPLIT_COUNT) && getSplitCount() < 0) {
            String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("Number of Splits to Generate");
            failureCollector.addFailure(errMsg, action).withConfigProperty(SPLIT_COUNT);
        }

        if (getBatchSize() != null && !containsMacro(BATCH_SIZE) && getBatchSize() < 0) {
            String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("Batch Size");
            failureCollector.addFailure(errMsg, action).withConfigProperty(BATCH_SIZE);
        }
    }

    /**
     * Checks if the Entity field contains any 'Key' values e.g Products(2). Then throws the error as this is not
     * supported.
     *
     * @param failureCollector {@code FailureCollector}
     */
    private void validateEntityParameter(FailureCollector failureCollector) {
        if (Util.isNotNullOrEmpty(getEntityName()) && !containsMacro(getEntityName())) {
            Pattern pattern = Pattern.compile("\\(.*\\)");
            if (pattern.matcher(getEntityName()).find()) {
                failureCollector.addFailure(ResourceConstants.ERR_FEATURE_NOT_SUPPORTED.getMsgForKey(), null)
                        .withConfigProperty(ENTITY_NAME);
            }
        }
    }

    /**
     * Removes any line breaks from the given string.
     *
     * @param rawString
     * @return refactored String or null
     */
    private String removeLinebreak(String rawString) {
        if (Util.isNotNullOrEmpty(rawString)) {
            return rawString.replaceAll("[\n\r]", " ");
        }
        return rawString;
    }

    /**
     * Removes any whitespace character (spaces, tabs, line breaks) from the given string.
     *
     * @param rawString
     * @return refactored String or null
     */
    private String removeWhitespace(String rawString) {
        if (Util.isNotNullOrEmpty(rawString)) {
            return rawString.replaceAll("\\s", "");
        }
        return rawString;
    }

    /**
     * Trim whitespace from the beginning and end of a string.
     *
     * @param //rawString
     * @return trimmed String or null
     */
    private String trim(String rawString) {
        if (Util.isNotNullOrEmpty(rawString)) {
            return removeWhitespace(rawString);
        }
        return rawString;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Helper class to simplify {@link SapODataPluginConfig} class creation.
     */
    public static class Builder {
        private String referenceName;
        private String baseURL;
        private String oDataVersion;
        private String serviceName;
        private String entityName;
        private String securityType;
        private String username;
        private String password;
        private String gcpProjectId;
        private String certGcsPath;
        private String certPassphrase;
        private String filterOption;
        private String selectOption;
        private String expandOption;
        private Long skipRowCount;
        private Long numRowsToFetch;
        private Integer splitCount;
        private Long batchSize;
        private SAPODataConnectorConfig connection;


        public Builder referenceName(String referenceName) {
            this.referenceName = referenceName;
            return this;
        }

        public Builder baseURL(String host) {
            this.baseURL = host;
            return this;
        }

        public Builder oDataVersion(String oDataVersion) {
            this.oDataVersion = oDataVersion;
            return this;
        }

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder securityType(String securityType) {
            this.securityType = securityType;
            return this;
        }

        public Builder username(@Nullable String username) {
            this.username = username;
            return this;
        }

        public Builder password(@Nullable String password) {
            this.password = password;
            return this;
        }

        public Builder gcpProjectId(@Nullable String gcpProjectId) {
            this.gcpProjectId = gcpProjectId;
            return this;
        }

        public Builder certGcsPath(@Nullable String certGcsPath) {
            this.certGcsPath = certGcsPath;
            return this;
        }

        public Builder certPassphrase(@Nullable String certPassphrase) {
            this.certPassphrase = certPassphrase;
            return this;
        }

        public Builder filterOption(@Nullable String filterOption) {
            this.filterOption = filterOption;
            return this;
        }

        public Builder selectOption(@Nullable String selectOption) {
            this.selectOption = selectOption;
            return this;
        }

        public Builder expandOption(@Nullable String expandOption) {
            this.expandOption = expandOption;
            return this;
        }

        public Builder skipRowCount(@Nullable Long skipRowCount) {
            this.skipRowCount = skipRowCount;
            return this;
        }

        public Builder numRowsToFetch(@Nullable Long numRowsToFetch) {
            this.numRowsToFetch = numRowsToFetch;
            return this;
        }

        public Builder splitCount(@Nullable Integer splitCount) {
            this.splitCount = splitCount;
            return this;
        }

        public Builder batchSize(@Nullable Long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder connection(SAPODataConnectorConfig connection) {
            this.connection = connection;
            return this;
        }

        public SapODataPluginConfig build() {
            return new SapODataPluginConfig(this.serviceName, this.entityName,this.referenceName, this.gcpProjectId, this.certGcsPath,
                    this.certPassphrase,
                    this.filterOption, this.selectOption, this.expandOption, this.skipRowCount, this.numRowsToFetch,
                    this.splitCount, this.batchSize, this.connection);
        }

    }
}
