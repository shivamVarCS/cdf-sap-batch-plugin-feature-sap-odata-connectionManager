package com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto;

/**
 * This {@code InnerError} class contains SAP S4/Hana 1909 in-depth error details.
 * Sample:
 * {
 *   "transactionid":"C83CB3D2A1420000E00609D31E196BD4",
 *   "timestamp":"20210524082515.9921880",
 *   "Error_Resolution":{
 *     "SAP_Transaction":"For backend administrators: use ADT feed reader SAP Gateway Error Log or run transaction
 *  /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries with the timestamp above for more details",
 *     "SAP_Note":"See SAP Note 1797736 for error analysis (https://service.sap.com/sap/support/notes/1797736)"
 *   },
 *   "errordetails":{
 *     "errordetail":[
 *       {
 *         "code":"/IWBEP/CM_TEA/002",
 *         "message":"'TEAM_012345678' is not a valid ID.",
 *         "propertyref":"Team/Team_Identifier",
 *         "severity":"error"
 *       }
 *     ]
 *   }
 * }
 */
public class InnerError {
  private final String transactionid;
  private final String timestamp;
  private final Application application;
  private final ErrorResolution Error_Resolution;
  private final InnerErrorDetail errordetails;

  public InnerError(String transactionid, String timestamp,
                    Application application,
                    ErrorResolution Error_Resolution,
                    InnerErrorDetail errordetails) {

    this.transactionid = transactionid;
    this.timestamp = timestamp;
    this.application = application;
    this.Error_Resolution = Error_Resolution;
    this.errordetails = errordetails;
  }

  public String getTransactionId() {
    return transactionid;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public Application getApplication() {
    return application;
  }

  public ErrorResolution getErrorResolution() {
    return Error_Resolution;
  }

  public InnerErrorDetail getInnerErrorDetails() {
    return errordetails;
  }
}
