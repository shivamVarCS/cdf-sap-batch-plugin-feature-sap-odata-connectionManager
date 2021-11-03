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

package com.google.cloud.datafusion.plugin.sap.odp.source;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.source.AbstractStructuredSchemaTransformer;

/**
 * @author sankalpbapat
 */
public class SapOdpToStructuredSchemaTransformer extends AbstractStructuredSchemaTransformer {

  /*
   * (non-Javadoc)
   *
   * @see com.google.cloud.datafusion.plugin.sap.source.AbstractStructuredSchemaTransformer#
   * getFieldNativeValue(com.google.cloud.datafusion.plugin.sap.metadata.model.
   * SapObjectMetadata, java.lang.String, int)
   */
  @Override
  public String getFieldNativeValue(SapObjectMetadata runtimeMetadata, String rawRecord, int fieldIdx) {
    return null;
  }
}