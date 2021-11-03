package com.google.cloud.datafusion.plugin.sap.odata.source.connector;

import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.odata.source.SapODataBatchSource;
import com.google.cloud.datafusion.plugin.sap.odata.source.config.SapODataPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odata.source.connection.ODataInterface;
import com.google.cloud.datafusion.plugin.sap.odata.source.connection.ODataInterfaceImpl;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.Entities;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.Services;
import com.google.cloud.datafusion.plugin.sap.odata.source.util.SAPOdataUtil;
import com.google.cloud.datafusion.plugin.util.Util;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.*;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OData Connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(SapODataConnector.NAME)
@Description("This connector enables browsing feature to fetch the service and entity information from SAP")
public class SapODataConnector implements DirectConnector {
    public static final String NAME = "SAPOData";
    //private static final String NAME_SPACE = "namespace";

    private final SAPODataConnectorConfig config;
    private SAPOdataUtil util;

    private static final ODataInterface odataInterface = new ODataInterfaceImpl();

    public SapODataConnector(SAPODataConnectorConfig config) {
        this.config = config;
    }

    @Override
    public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
            throws IOException {
        /*SapODataPath path = new SapODataPath(sampleRequest.getPath());
        String entity = path.getEntity();
        if (entity == null) {
            throw new IllegalArgumentException("Path should contain both service and entity.");
        }
        String service = path.getService();*/
        /*return getTableData(getBigQuery(config.getProject()), config.getDatasetProject(), service, entity,
                sampleRequest.getLimit());*/
        return Collections.emptyList();
    }

    @Override
    public void test(ConnectorContext context) throws ValidationException {
        FailureCollector failureCollector = context.getFailureCollector();
        String baseURL = config.getProject();
        if (Util.isNullOrEmpty(baseURL)) {
            failureCollector
                    .addFailure("Could not detect SAP BASE_URL from the environment.",
                            "Please specify a correct URL.");
        }
        try {
            config.validateMandatoryParameters(failureCollector);
        } catch (Exception e) {
            failureCollector
                    .addFailure("Could not detect SAP BASE_URL from the environment.",
                            "Please specify a correct URL.");
        }
        try {
            config.validateBasicCredentials(failureCollector);
        } catch (Exception e) {
            //failureCollector.getOrThrowException();
            failureCollector.addFailure(String.format("SAP credentials are not not valid: %s", e.getMessage()),
                    "Please provide connection properties.");
            //failureCollector.getOrThrowException();
        }
        try {
            odataInterface.listServices(config.tryGetProject());
        } catch (Exception e) {
            failureCollector.addFailure(String.format("Could not connect to SAP: %s", e.getMessage()),
                    "Please specify correct connection properties.");
        }
    }

    @Override
    public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
        SapODataPath path = new SapODataPath(browseRequest.getPath());

        try {
        /*String nameSpace = path.getNameSpace();
        if (nameSpace == null) {
             browse project to list all Servi\ces
            return listNameSpace(path,browseRequest.getLimit());
        }*/
            String sapServices = path.getService();
            if (sapServices == null) {
                return listServices(path, browseRequest.getLimit());
            }

            String sapEntities = path.getEntity();
            if (sapEntities == null) {
                return listEntities(path, browseRequest.getLimit());
            }
        } catch (ConnectorException e) {
            e.printStackTrace();
        }
        return BrowseDetail.builder().setTotalCount(0).build();
    }

    @Override
    public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
            throws IOException {
        SapODataPath path = new SapODataPath(connectorSpecRequest.getPath());
        ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
        properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
        String service = path.getService();
        if (service != null) {
            properties.put(SapODataPluginConfig.SERVICE_NAME, service);
        }
        String entity = path.getEntity();
        if (entity != null) {
            properties.put(SapODataPluginConfig.ENTITY_NAME, entity);
        }
        util = new SAPOdataUtil(config);
        Schema schema = util.getSchema(null);
        specBuilder.setSchema(schema);
        return specBuilder.addRelatedPlugin(new PluginSpec(SapODataBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties))
                .build();
    }

    private BrowseDetail listServices(SapODataPath path, Integer limit) throws IOException, ConnectorException {
        int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
        int count = 0;
        BrowseDetail.Builder builder = BrowseDetail.builder();
        // String parentPath = String.format("/%s/", path.getNameSpace());
        List<Services> locationList = odataInterface.listServices(config.tryGetProject());
        for (Services service : locationList) {
            if (count >= countLimit) {
                break;
            }
            builder.addEntity(
                    BrowseEntity.builder(service.getServiceName(), "/" + service.getServiceName(), SapODataPluginConfig.SERVICE_NAME)
                            .canSample(true).canBrowse(true).build());
            count++;
        }
        return builder.setTotalCount(count).build();
    }

    private BrowseDetail listEntities(SapODataPath path, Integer limit) throws IOException, ConnectorException {
        int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
        int count = 0;
        String parentPath = String.format("/%s/%s/", path.getService(), "entity");
        BrowseDetail.Builder builder = BrowseDetail.builder();
        List<Entities> entitiesList = odataInterface.listEntities(config.tryGetProject());
        for (Entities entity : entitiesList) {
            if (count >= countLimit) {
                break;
            }
            builder.addEntity(
                    BrowseEntity.builder(entity.getEntityName(), parentPath + entity.getEntityName(), SapODataPluginConfig.ENTITY_NAME)
                            .canSample(true).build());
            count++;
        }
        return builder.setTotalCount(count).build();
    }
}
