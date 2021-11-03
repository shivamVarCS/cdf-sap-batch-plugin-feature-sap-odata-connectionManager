package com.google.cloud.datafusion.plugin.sap.odata.source.util;

import com.google.cloud.datafusion.plugin.sap.odata.source.SapODataService;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.connector.SAPODataConnectorConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.sap.odata.source.exception.TransportException;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapODataTransporter;
import com.google.cloud.datafusion.plugin.sap.odata.source.transport.SapX509Manager;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.annotation.Nullable;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

public class SAPOdataUtil {

    private SapODataPluginConfig pluginConfig;
    private SAPODataConnectorConfig config;

    public SAPOdataUtil(SAPODataConnectorConfig config) {
        this.config = config;
    }

    @Nullable
    public Schema getSchema(FailureCollector collector) {
        try {
            //    String path = "/Users/varshneys/Downloads/schema.json";
            //    Schema schema = new Schema(Files.readAllBytes(Paths.get(path)));

            //  JSONParser parser = new JSONParser();
            //  FileReader reader = new FileReader("/Users/varshneys/Downloads/schema.json");
            //  Object object = parser.parse(reader);
            //  System.out.println(object);
            // JSONObject jsonObject = (JSONObject) parser.parse(reader);
            // System.out.println(String.valueOf(jsonObject));

            String schema = "{\"type\":\"record\",\"name\":\"columnMetadata\",\"fields\":[{\"name\":\"fieldName1\",\"type\":\"string\"},{\"name\":\"fieldName2\",\"type\":\"string\"},{\"name\":\"fieldName3\",\"type\":[\"string\",\"null\"]},{\"name\":\"fieldName4\",\"type\":[\"string\",\"null\"]},{\"name\":\"fieldName5\",\"type\":[\"int\",\"null\"]},{\"name\":\"fieldName6\",\"type\":[\"int\",\"null\"]},{\"name\":\"fieldName7\",\"type\":[\"int\",\"null\"]},{\"name\":\"fieldName8\",\"type\":[\"long\",\"null\"]},{\"name\":\"fieldName9\",\"type\":[\"double\",\"null\"]},{\"name\":\"fieldName10\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"fieldName11\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"fieldName12\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"fieldName13\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":3},\"null\"]},{\"name\":\"field__Name_14\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":13,\"scale\":5},\"null\"]},{\"name\":\"fieldName15\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":8},\"null\"]},{\"name\":\"fieldName16\",\"type\":[\"string\",\"null\"]},{\"name\":\"fieldName17\",\"type\":[\"string\",\"null\"]},{\"name\":\"fieldName18\",\"type\":[\"string\",\"null\"]}]}";

            return Schema.parseJson(schema);
        } catch (IOException e) {
            collector.addFailure("File location not Found " + e.getMessage(), "Please check the file location");
        }
         /* catch( ParseException e) {
            collector.addFailure("Invalid schema: " + e.getMessage(), "Please upload valid schema");
        }*/
        // if there was an error that was added, it will throw an exception, otherwise,
        // this statement will not be executed
        throw collector.getOrThrowException();
    }

    /**
     * Gets the appropriate Schema basis the provided plugin parameters and also
     * sets the appropriate error messages in case any error is identified while preparing the Schema.
     *
     * @param failureCollector {@code FailureCollector}
     * @return {@code Schema}
     */
    @Nullable
    private Schema getOutputSchema(FailureCollector failureCollector) {

        SapX509Manager x509Manager = new SapX509Manager("auto-detect",
                pluginConfig.getCertGcsPath(),
                pluginConfig.getCertPassphrase());

        SapODataTransporter transporter = new SapODataTransporter(pluginConfig.getUsername(),
                pluginConfig.getPassword(),
                x509Manager);

        SapODataService sapODataServices = new SapODataService(pluginConfig, transporter);
        try {
            //validate if the given parameters form a valid OData URL.
            sapODataServices.checkODataURL();

            return sapODataServices.buildOutputSchema();
        } catch (TransportException te) {
            String errorMsg = ExceptionParser.buildTransportError(te);
            switch (te.getErrorType()) {
                case TransportException.IO_ERROR:
                    failureCollector.addFailure(errorMsg, null)
                            .withConfigProperty(SAPODataConnectorConfig.BASE_URL);
                    break;

                case TransportException.X509_CERT_PATH_ERROR:
                    failureCollector.addFailure(te.getMessage(), null)
                            .withConfigProperty(SapODataPluginConfig.CERT_GCS_PATH);
                    break;

                default:
                    failureCollector.addFailure(
                            ResourceConstants.ERR_ODATA_SERVICE_CALL.getMsgForKeyWithCode(errorMsg), null);
            }
        } catch (ODataServiceException ose) {
            attachFieldWithError(ose, failureCollector);
        }

        failureCollector.getOrThrowException();

        return null;
    }

    /**
     * Checks and attaches the UI fields with its relevant error message.
     *
     * @param ose              {@code ODataServiceException}
     * @param failureCollector {@code FailureCollector}
     */
    private void attachFieldWithError(ODataServiceException ose, FailureCollector failureCollector) {
        String errMsg = ExceptionParser.buildODataServiceError(ose);

        switch (ose.getErrorCode()) {
            case HttpURLConnection.HTTP_UNAUTHORIZED: {
                failureCollector.addFailure(errMsg, null).withConfigProperty(SAPODataConnectorConfig.UNAME);
                failureCollector.addFailure(errMsg, null).withConfigProperty(SAPODataConnectorConfig.PASSWORD);
            }
            break;
            case HttpURLConnection.HTTP_FORBIDDEN:
            case ODataServiceException.INVALID_SERVICE:
                failureCollector.addFailure(errMsg, null).withConfigProperty(SapODataPluginConfig.SERVICE_NAME);
                break;
            case ExceptionParser.NO_VERSION_FOUND:
            case ExceptionParser.INVALID_VERSION_FOUND:
                failureCollector.addFailure(errMsg, null).withConfigProperty(SapODataPluginConfig.ENTITY_NAME);
                break;
            case HttpURLConnection.HTTP_NOT_FOUND:
                failureCollector.addFailure(ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode(errMsg),
                        ResourceConstants.ERR_NOT_FOUND.getMsgForKey());
                break;
            case HttpURLConnection.HTTP_BAD_REQUEST:
                failureCollector.addFailure(ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode(errMsg),
                        ResourceConstants.ERR_CHECK_ADVANCED_PARAM.getMsgForKey());
                break;

            default:
                failureCollector.addFailure(
                        ResourceConstants.ERR_ODATA_SERVICE_FAILURE.getMsgForKeyWithCode(errMsg), null);
        }
    }

}

