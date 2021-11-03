package com.google.cloud.datafusion.plugin.sap.odata.source.connection;

import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.Entities;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.NameSpace;
import com.google.cloud.datafusion.plugin.sap.odata.source.model.Services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ODataInterfaceImpl implements ODataInterface{
    @Override
    public List<Services> listServices(String nameSpace) throws IOException, ConnectorException {
        List<Services> serviceList= new ArrayList<>();
        Services service1 = new Services("sap/opu/odata/sap/ZACDOCA_CDS");
        Services service2 = new Services("Service_2");
        Services service3 = new Services("Service_3");

        serviceList.add(service1); serviceList.add(service2); serviceList.add(service3);
        return serviceList;
    }

    @Override
    public List<NameSpace> listNameSpace(String nameSpace) throws ConnectorException, IOException {
        NameSpace root = new NameSpace("sap/opu/odata/sap");
        List<NameSpace> rootList = new ArrayList<>();
        rootList.add(root);
        return rootList;
    }

    @Override
    public List<Entities> listEntities(String projectId) throws ConnectorException {
        Entities entity1 = new Entities("ZACDOCA");
        Entities entity2 = new Entities("Entity_2");

        List<Entities> entitiesList = new ArrayList<>();
        entitiesList.add(entity1);
        entitiesList.add(entity2);

        return entitiesList;
    }
}
