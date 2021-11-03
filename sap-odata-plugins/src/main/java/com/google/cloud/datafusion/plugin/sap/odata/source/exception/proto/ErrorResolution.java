package com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto;

/**
 * This {@code ErrorResolution} class contains SAP S4/Hana 1909 recommended error resolution details.
 * Sample:
 * {
 *   "SAP_Transaction":"For backend administrators: use ADT feed reader SAP Gateway Error Log or run transaction
 *  /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries with the timestamp above for more details",
 *   "SAP_Note":"See SAP Note 1797736 for error analysis (https://service.sap.com/sap/support/notes/1797736)"
 * }
 */
public class ErrorResolution {

  private String SAP_Transaction;
  private String SAP_Note;
  private String Additional_SAP_Note;

  public ErrorResolution(String SAP_Transaction, String SAP_Note, String Additional_SAP_Note) {
    this.SAP_Transaction = SAP_Transaction;
    this.SAP_Note = SAP_Note;
    this.Additional_SAP_Note = Additional_SAP_Note;
  }

  public String getSapTransaction() {
    return SAP_Transaction;
  }

  public String getSapNote() {
    return SAP_Note;
  }

  public String getAdditional_SAP_Note() {
    return Additional_SAP_Note;
  }
}

