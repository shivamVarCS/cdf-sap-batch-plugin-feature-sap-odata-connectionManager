package com.google.cloud.datafusion.plugin.sap.odata.source.connection;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.Entities;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.NameSpace;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.Services;

import java.io.IOException;
import java.util.List;

public interface ODataInterface {

    List<Services> listServices(String service) throws ConnectorException, IOException;

    List<NameSpace> listNameSpace(String nameSpace) throws ConnectorException, IOException;

    List<Entities> listEntities(String projectId) throws ConnectorException;
}
