package com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto;

import java.util.List;

/**
 * This {@code InnerErrorDetail} class contains SAP S4/Hana 1909 inner error details which is mapped by
 * 'innererror.errordetails'
 * Sample:
 * [
 *   {
 *     "code":"/IWBEP/CM_TEA/002",
 *     "message":"'TEAM_012345678' is not a valid ID.",
 *     "propertyref":"Team/Team_Identifier",
 *     "severity":"error"
 *   }
 * ]
 */
public class InnerErrorDetail {

  private final List<ErrorProperty> errordetail;

  public InnerErrorDetail(List<ErrorProperty> errordetail) {
    this.errordetail = errordetail;
  }

  public List<ErrorProperty> getErrorDetails() {
    return errordetail;
  }
}
