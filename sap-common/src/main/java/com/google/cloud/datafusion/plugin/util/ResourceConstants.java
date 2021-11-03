/*
 * Copyright © 2021 Cask Data, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.datafusion.plugin.util;

import javax.annotation.Nullable;

/**
 * Contains constant keys for externalized strings, to be used as reference for
 * internationalization/localization of messages and text. The keys when passed
 * to any method of {@link ResourceText}, bring corresponding text message in a
 * language based on the specified or default locale from the ResourceBundle
 * files (i10n).
 * 
 * @author sankalpbapat
 *
 */
public enum ResourceConstants {

  // Common resource constants
  ERR_MISSING_PARAM_PREFIX(null, "err.missing.param.prefix"),
  ERR_MISSING_PARAM_ACTION(null, "err.missing.param.action"),

  ERR_MISSING_PARAM_FOR_CONN_PREFIX(null, "err.missing.param.for.conn.prefix"),
  ERR_MISSING_PARAM_OR_MACRO_ACTION(null, "err.missing.param.or.macro.action"),

  ERR_NEGATIVE_PARAM_PREFIX(null, "err.negative.param.prefix"),
  ERR_NEGATIVE_PARAM_ACTION(null, "err.negative.param.action"),
  ERR_INVALID_REFERENCE_PARAM_ACTION(null, "err.invalid.reference.param.action"),

  ERR_JCOLIB_MISSING("CDF_SAP_01412", "err.jcolib.missing"),
  ERR_JCO_MISSING_ACTION(null, "err.jcolib.missing.action"),

  ERR_SCHEMA_FIELD_COUNT_MISMATCH(null, "err.schema.field.count.mismatch"),

  ERR_SCHEMA_FIELD_INVALID(null, "err.schema.field.invalid"),
  ERR_SCHEMA_FIELD_INVALID_ACTION(null, "err.schema.field.invalid.action"),

  ERR_SCHEMA_FIELD_NON_NULLABLE(null, "err.schema.field.non.nullable"),
  ERR_SCHEMA_FIELD_TYPE_INVALID(null, "err.schema.field.type.invalid"),

  ROOT_CAUSE_LOG(null, "root.cause.log"),

  ERR_GET_DEST_FROM_MGR("CDF_SAP_01500", "err.get.dest.from.mgr"),
  ERR_SAP_PING("CDF_SAP_01404", "err.sap.ping"),
  ERR_GET_REPO_FROM_DEST("CDF_SAP_01512", "err.get.repo.from.dest"),
  ERR_GET_FUNC_FROM_REPO("CDF_SAP_01513", "err.get.func.from.repo"),
  ERR_FUNC_MISSING("CDF_SAP_01501", "err.func.missing"),
  ERR_EXEC_FUNC("CDF_SAP_01406", "err.exec.func"),

  ERR_UNAVAILABLE_WP("CDF_SAP_01545", "err.unavailable.wp"),
  ERR_UNAVAILABLE_MAX_MEMORY_FOR_WP("CDF_SAP_01546,", "err.unavailable.max.memory.for.wp"),

  INFO_FOUND_MAX_MEMORY_FOR_WP(null, "info.found.max.memory.for.wp"),
  INFO_FOUND_AVAILABLE_WP(null, "info.found.available.wp"),
  INFO_NUM_SPLITS(null, "info.num.splits"),

  ERR_FIELD_VAL_CONVERT("CDF_SAP_01550", "err.field.val.convert"),

  // Table reader constants
  ERR_TABLE_VIEW_MISSING(null, "err.table.view.missing"),
  ERR_TABLE_VIEW_INVALID(null, "err.table.view.invalid"),

  ERR_OPTION_NOT_VALID("CDF_SAP_TABLE_01532", "err.option.not.valid"),
  ERR_DATA_BUFFER_EXCEEDED("CDF_SAP_TABLE_01534", "err.data.buffer.exceeded"),
  ERR_NOT_AUTHORIZED_FOR_TABLE("CDF_SAP_TABLE_01403", "err.not.authorized.for.table"),
  ERR_SQL_FAILURE("CDF_SAP_TABLE_01535", "err.sql.failure"),
  ERR_DB_FAILURE("CDF_SAP_TABLE_01536", "err.db.failure"),

  INFO_NO_RECORDS(null, "info.no.records"),
  INFO_FOUND_NUM_RECORDS(null, "info.found.num.records"),
  INFO_NUM_RECORDS_PACKAGE(null, "info.num.records.package"),
  INFO_EXTRACT_NUM_RECORDS(null, "info.extract.num.records"),
  WARN_RETRY_PACKAGE_EXTRACT(null, "warn.retry.package.extract"),
  ERR_FAILED_PACKAGE_EXTRACT("CDF_SAP_TABLE_01520", "err.failed.package.extract"),

  // ODP constants
  ERR_INVALID_EXTRACT_TYPE(null, "err.invalid.extract.type"),
  ERR_INVALID_SUBSCRIBER_NAME(null, "err.invalid.subscriber.name"),
  ERR_SOURCE_OBJ_MISSING(null, "err.source.obj.missing"),
  ERR_SOURCE_OBJ_NOT_EXPOSED(null, "err.source.obj.not.exposed"),
  ERR_STRUCT_FOR_DATA_SOURCE_MISSING(null, "err.struct.for.data.source.missing"),
  ERR_FILTER_EQ_FIELD(null, "err.filter.eq.field"),
  ERR_FILTER_RANGE_FIELD(null, "err.filter.range.field"),
  ERR_FILTER_RANGE_VALUE(null, "err.filter.range.value"),

  ERR_PREVIEW_NOT_SUPPORTED("CDF_SAP_ODP_01503", "err.preview.not.supported"),
  ERR_NOT_AUTHORIZED_FOR_DATASOURCE("CDF_SAP_ODP_01403", "err.not.authorized.for.datasource"),

  INFO_SYNC_LAST_EXTRACT_NOT_FOUND(null, "info.sync.last.extract.not.found"),
  INFO_SYNC_LAST_EXTRACT(null, "info.sync.last.extract"),
  INFO_FOUND_NUM_PACKAGES(null, "info.found.num.packages"),
  INFO_SIZE_PACKAGE(null, "info.size.package"),
  INFO_EXTRACT_NUM_PACKAGES(null, "info.extract.num.packages"),

  // OData constants
  ERR_ODATA_SERVICE_CALL("CDF_SAP_ODATA_01532", "err.odata.service.call"),
  ERR_ODATA_SERVICE_FAILURE("CDF_SAP_ODATA_01533", "err.odata.service.failure"),
  ERR_INVALID_BASE_URL(null, "err.invalid.base.url"),
  ERR_FEATURE_NOT_SUPPORTED("CDF_SAP_ODATA_01500", "err.feature.not.supported"),
  ERR_INVALID_CREDENTIAL(null, "err.invalid.credential"),
  ERR_UNSUPPORTED_VERSION("CDF_SAP_ODATA_01501", "err.unsupported.version"),
  ERR_MISSING_DATASERVICE_VERSION("CDF_SAP_ODATA_01502", "err.missing.dataservice.version"),
  ERR_INVALID_SERVICE_NAME("CDF_SAP_ODATA_01503", "err.invalid.service.name"),

  ERR_CALL_SERVICE_FAILURE(null, "err.call.service.failure"),
  INFO_RETRY_ON_FAILURE(null, "info.retry.on.failure"),
  ERR_FAILED_SERVICE_VALIDATION(null, "err.failed.service.validation"),
  ERR_CHECK_ADVANCED_PARAM(null, "err.check.advanced.parameter"),
  ERR_NOT_FOUND(null, "err.resource.not.found"),

  DEBUG_TEST_ENDPOINT(null, "debug.test.endpoint"),
  DEBUG_METADATA_ENDPOINT(null, "debug.metadata.endpoint"),
  DEBUG_DATA_COUNT_ENDPOINT(null, "debug.data.count.endpoint"),
  DEBUG_DATA_ENDPOINT(null, "debug.data.endpoint"),
  DEBUG_CALL_SERVICE_START(null, "debug.call.service.start"),
  DEBUG_CALL_SERVICE_END(null, "debug.call.service.end"),

  ERR_METADATA_CALL(null, "err.metadata.call"),
  ERR_READING_METADATA(null, "err.reading.metadata"),

  ERR_ENTITY_DATA_CALL(null, "err.entity.data.call"),
  ERR_FAILED_SSL_CONFIGURATION("CDF_ODATA_01503", "err.failed.ssl.configuration"),

  DEBUG_NOT_FOUND(null, "debug.not.found"),
  DEBUG_NAVIGATION_NOT_FOUND(null, "debug.navigation.not.found"),
  DEBUG_ENTITY_NOT_FOUND(null, "debug.entity.not.found"),
  ERR_NO_COLUMN_FOUND(null, "err.no.column.found"),
  ERR_NAV_PATH_NOT_FOUND(null, "err.nav.path.not.found"),
  ERR_BUILDING_COLUMNS(null, "err.building.columns"),
  ERR_BUILDING_SCHEMA(null, "err.building.schema"),

  WARN_ENTITY_NOT_FOUND(null, "warn.entity.not.found"),

  ERR_MACRO_INPUT("CDF_SAP_ODATA_01534", "err.macro.input"),
  ERR_NO_RECORD_FOUND("CDF_SAP_ODATA_01535", "err.no.record.found"),
  ERR_RECORD_PULL("CDF_SAP_ODATA_01536", "err.record.pull")

  ;

  private final String code;
  private final String key;

  ResourceConstants(String code, String key) {
    this.code = code;
    this.key = key;
  }

  @Nullable
  public String getCode() {
    return code;
  }

  public String getKey() {
    return key;
  }

  public String getMsgForKeyWithCode() {
    return getMsgForKey(code);
  }

  public String getMsgForKeyWithCode(Object... params) {
    Object[] destArr = new Object[params.length + 1];
    destArr[0] = code;
    System.arraycopy(params, 0, destArr, 1, params.length);

    return getMsgForKey(destArr);
  }

  public String getMsgForKey() {
    return ResourceText.getString(key);
  }

  public String getMsgForKey(Object... params) {
    return ResourceText.getString(key, params);
  }
}
