package com.google.cloud.datafusion.plugin.sap.odata.source.model;

public class NameSpace {

    private String namespace ;

    public NameSpace(String namespace) {
        this.namespace = namespace;
    }

    public String getNameSpace() {
        return namespace;
    }

    public void setNameSpace(String namespace) {
        this.namespace = namespace;
    }
}
