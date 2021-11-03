package com.google.cloud.datafusion.plugin.sap.odata.source.model;

public class Services {

    public String serviceName;

    public Services(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
