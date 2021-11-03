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

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapRuntimeConfigInfo;

/**
 * @author sankalpbapat
 *
 */
public class SapTableRuntimeConfigInfo extends SapRuntimeConfigInfo {
  private final long runtimeTableRecCount;
  private final int recordSize;

  private SapTableRuntimeConfigInfo(long tableRecCount, int recordSize, int totalWorkProcCount,
                                    int availableWorkProcCount, long wpMaxMemory) {

    super(totalWorkProcCount, availableWorkProcCount, wpMaxMemory);

    this.runtimeTableRecCount = tableRecCount;
    this.recordSize = recordSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public long getRuntimeTableRecCount() {
    return runtimeTableRecCount;
  }

  public int getRecordSize() {
    return recordSize;
  }

  /**
   * Helps to build an immutable {@code SapTableRuntimeConfigInfo}
   */
  public static class Builder
    extends SapRuntimeConfigInfo.Builder<SapTableRuntimeConfigInfo, SapTableRuntimeConfigInfo.Builder> {

    private long runtimeTableRecCount;
    private int recordSize;

    private Builder() {
    }

    protected SapTableRuntimeConfigInfo createRuntimeConfig(int totalWorkProcCount, int availableWorkProcCount,
                                                            long wpMaxMemory) {

      return new SapTableRuntimeConfigInfo(runtimeTableRecCount, recordSize, totalWorkProcCount, availableWorkProcCount,
        wpMaxMemory);
    }

    protected Builder getThis() {
      return this;
    }

    public Builder setRuntimeTableRecCount(long runtimeTableRecCount) {
      this.runtimeTableRecCount = runtimeTableRecCount;
      return this;
    }

    public Builder setRecordSize(int recordSize) {
      this.recordSize = recordSize;
      return this;
    }
  }
}
