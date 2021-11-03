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

/**
 * @author sankalpbapat
 *
 */
public abstract class SapRuntimeConfigInfo {
  private final int totalWorkProcCount;
  private final int availableWorkProcCount;
  private final long wpMaxMemory;

  protected SapRuntimeConfigInfo(int totalWorkProcCount, int workProcessCount, long wpMaxMemory) {
    this.totalWorkProcCount = totalWorkProcCount;
    this.availableWorkProcCount = workProcessCount;
    this.wpMaxMemory = wpMaxMemory;
  }

  public int getTotalWorkProcCount() {
    return totalWorkProcCount;
  }

  public int getAvailableWorkProcCount() {
    return availableWorkProcCount;
  }

  public long getWpMaxMemory() {
    return wpMaxMemory;
  }

  /**
   * Helps to build an immutable {@code SapRuntimeTableInfo}
   *
   * @param <T> Concrete implementation of {@code SapRuntimeConfigInfo}
   * @param <B> Concrete implementation of this {@code Builder}
   */
  public abstract static class Builder<T extends SapRuntimeConfigInfo, B extends Builder<T, B>> {
    private int totalWorkProcCount;
    private int availableWorkProcCount;
    private long wpMaxMemory;

    protected Builder() {
    }

    @SuppressWarnings("unchecked")
    public B setTotalWorkProcCount(int totalWorkProcCount) {
      this.totalWorkProcCount = totalWorkProcCount;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B setAvailableWorkProcCount(int availableWorkProcCount) {
      this.availableWorkProcCount = availableWorkProcCount;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B setWpMaxMemory(long wpMaxMemory) {
      this.wpMaxMemory = wpMaxMemory;
      return (B) this;
    }

    protected abstract T createRuntimeConfig(int totalWorkProcCount, int availableWorkProcCount, long wpMaxMemory);

    public T build() {
      return createRuntimeConfig(totalWorkProcCount, availableWorkProcCount, wpMaxMemory);
    }
  }
}
