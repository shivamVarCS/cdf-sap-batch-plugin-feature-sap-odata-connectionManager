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

package com.google.cloud.datafusion.plugin.sap.table.metadata.model;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectRuntimeOutput;
import com.sap.conn.jco.JCoTable;

import javax.annotation.Nullable;

/**
 * @author sankalpbapat
 *
 */
public class SapTableRuntimeOutput implements SapObjectRuntimeOutput {

  // Contains the object metadata at actual data extraction time
  private final SapObjectMetadata objectMetadata;

  // Actual data records are contained in this at runtime
  private final JCoTable outputDataTable;

  public SapTableRuntimeOutput(@Nullable SapObjectMetadata objectMetadata, @Nullable JCoTable outputDataTable) {
    this.objectMetadata = objectMetadata;
    this.outputDataTable = outputDataTable;
  }

  @Override
  @Nullable
  public SapObjectMetadata getObjectMetadata() {
    return objectMetadata;
  }

  @Nullable
  public JCoTable getOutputDataTable() {
    return outputDataTable;
  }
}
