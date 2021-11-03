package com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto;

/**
 * This {@code ErrorProperty} class contains SAP S4/Hana 1909 inner error properties.
 * Sample:
 * {
 *   "code":"/IWBEP/CM_TEA/002",
 *   "message":"'TEAM_012345678' is not a valid ID.",
 *   "propertyref":"Team/Team_Identifier",
 *   "severity":"error"
 * }
 */
public class ErrorProperty {

  private String code;
  private String message;
  private String propertyref;
  private String severity;
  private String target;

  public String getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

  public String getPropertyRef() {
    return propertyref;
  }

  public String getSeverity() {
    return severity;
  }

  public String getTarget() {
    return target;
  }

  public ErrorProperty(String code, String message, String propertyref, String severity, String target) {
    this.code = code;
    this.message = message;
    this.propertyref = propertyref;
    this.severity = severity;
    this.target = target;
  }
}
