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

package com.google.cloud.datafusion.plugin.sap.metadata.model;

import javax.annotation.Nullable;

/**
 * Model class to hold Field level metadata corresponding to a specific object
 * in SAP.<br/>
 * <br/>
 * Explicitly non-abstract to facilitate direct use if no special information
 * about the field is needed from metadata
 * 
 * @author sankalpbapat
 *
 */
public class SapFieldMetadata {
  private final String name;
  private final String label;
  private final String desc;
  private final int length;
  private final int decimals;
  private final String dataType;
  private final String abapType;
  private final boolean isKey;

  public SapFieldMetadata(String name, @Nullable String label, @Nullable String desc, int length, int decimals,
                          String dataType, String abapType, boolean isKey) {

    this.name = name;
    this.label = label;
    this.desc = desc;
    this.length = length;
    this.decimals = decimals;
    this.dataType = dataType;
    this.abapType = abapType;
    this.isKey = isKey;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getLabel() {
    return label;
  }

  @Nullable
  public String getDesc() {
    return desc;
  }

  public int getLength() {
    return length;
  }

  public int getDecimals() {
    return decimals;
  }

  public String getDataType() {
    return dataType;
  }

  public String getAbapType() {
    return abapType;
  }

  public boolean isKey() {
    return isKey;
  }

  // These methods are overridden in child classes and are here only to enable
  // access from parent without a down cast
  public int getPosition() {
    return -1;
  }

  public int getOffset() {
    return -1;
  }

  public boolean isEqualFilterAllowed() {
    return false;
  }

  public boolean isBetweenFilterAllowed() {
    return false;
  }
}
