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

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapFieldMetadata;

import javax.annotation.Nullable;

/**
 * Model class to hold Column level metadata corresponding to a specific table
 * in SAP.
 * 
 * @author sankalpbapat
 *
 */
public class SapColumn extends SapFieldMetadata {
  private final int position;
  private final int offset;

  public SapColumn(String name, @Nullable String label, @Nullable String desc, int position, int offset, int length,
                   int decimals, String dataType, String abapType, boolean isKey) {

    super(name, label, desc, length, decimals, dataType, abapType, isKey);

    this.position = position;
    this.offset = offset;
  }

  @Override
  public int getPosition() {
    return position;
  }

  @Override
  public int getOffset() {
    return offset;
  }
}
