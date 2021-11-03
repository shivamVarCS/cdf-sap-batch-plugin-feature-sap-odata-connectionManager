package com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto;

/**
 * This {@code Application} class contains SAP S4/Hana 1909 application details.
 * Sample:
 * {
 *   "component_id": "",
 *   "service_namespace": "/SAP/",
 *   "service_id": "ZPURCHASEORDER_SRV_X",
 *   "service_version": "0001"
 * }
 */

public class Application {

  private final String component_id;
  private final String service_namespace;
  private final String service_id;
  private final String service_version;

  public Application(String component_id, String service_namespace, String service_id, String service_version) {

    this.component_id = component_id;
    this.service_namespace = service_namespace;
    this.service_id = service_id;
    this.service_version = service_version;
  }

  public String getComponentId() {
    return component_id;
  }

  public String getServiceNamespace() {
    return service_namespace;
  }

  public String getServiceId() {
    return service_id;
  }

  public String getServiceVersion() {
    return service_version;
  }
}
