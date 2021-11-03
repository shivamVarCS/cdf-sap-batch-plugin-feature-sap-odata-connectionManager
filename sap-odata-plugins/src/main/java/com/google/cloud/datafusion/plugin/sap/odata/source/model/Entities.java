package com.google.cloud.datafusion.plugin.sap.odata.source.model;

public class Entities {

    public String entityName;

    public Entities(String entityName) {
        this.entityName = entityName;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }
}
