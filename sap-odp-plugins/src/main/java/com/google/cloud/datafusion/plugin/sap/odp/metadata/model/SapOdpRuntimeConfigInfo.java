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

package com.google.cloud.datafusion.plugin.sap.odp.metadata.model;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapRuntimeConfigInfo;

import javax.annotation.Nullable;

/**
 * @author sankalpbapat
 */
public class SapOdpRuntimeConfigInfo extends SapRuntimeConfigInfo {
  private final long runtimePackageCount;
  private final String replicationPointer;
  private final boolean isHierarchical;
  private final String previousExtractType;
  private final String previousExtractStatus;
  private final boolean isDeltaSupported;
  private final String extractJobId;
  private final String extractJobName;
  private final boolean isExtractJobStatusFinished;

  private SapOdpRuntimeConfigInfo(@Nullable String previousExtractType, @Nullable String previousExtractStatus,
                                  boolean isDeltaSupported, long runtimePackageCount,
                                  @Nullable String replicationPointer, boolean isHierarchical,
                                  @Nullable String extractJobId, @Nullable String extractJobName,
                                  boolean isExtractJobStatusFinished, int totalWorkProcCount,
                                  int availableWorkProcCount, long wpMaxMemory) {

    super(totalWorkProcCount, availableWorkProcCount, wpMaxMemory);

    this.previousExtractType = previousExtractType;
    this.previousExtractStatus = previousExtractStatus;
    this.isDeltaSupported = isDeltaSupported;
    this.runtimePackageCount = runtimePackageCount;
    this.replicationPointer = replicationPointer;
    this.isHierarchical = isHierarchical;
    this.extractJobId = extractJobId;
    this.extractJobName = extractJobName;
    this.isExtractJobStatusFinished = isExtractJobStatusFinished;
  }

  public static Builder builder() {
    return new Builder();
  }

  public long getRuntimePackageCount() {
    return runtimePackageCount;
  }

  @Nullable
  public String getReplicationPointer() {
    return replicationPointer;
  }

  @Nullable
  public String getPreviousExtractType() {
    return previousExtractType;
  }

  @Nullable
  public String getPreviousExtractStatus() {
    return previousExtractStatus;
  }

  public boolean isHierarchical() {
    return isHierarchical;
  }

  public boolean isDeltaSupported() {
    return isDeltaSupported;
  }

  @Nullable
  public String getExtractJobId() {
    return extractJobId;
  }

  @Nullable
  public String getExtractJobName() {
    return extractJobName;
  }

  public boolean isExtractJobStatusFinished() {
    return isExtractJobStatusFinished;
  }


  /**
   * Helps to build an immutable {@code SapOdpRuntimeConfigInfo}
   */
  public static class Builder
    extends SapRuntimeConfigInfo.Builder<SapOdpRuntimeConfigInfo, SapOdpRuntimeConfigInfo.Builder> {

    private String previousExtractType;
    private String previousExtractStatus;
    private boolean isDeltaSupported;
    private long runtimePackageCount;
    private String replicationPointer;
    private boolean isHierarchical;
    private String extractJobId;
    private String extractJobName;
    private boolean isExtractJobStatusFinished;

    private Builder() {
    }

    protected SapOdpRuntimeConfigInfo createRuntimeConfig(int totalWorkProcCount, int availableWorkProcCount,
                                                          long wpMaxMemory) {

      return new SapOdpRuntimeConfigInfo(previousExtractType, previousExtractStatus, isDeltaSupported,
        runtimePackageCount, replicationPointer, isHierarchical, extractJobId, extractJobName,
        isExtractJobStatusFinished, totalWorkProcCount, availableWorkProcCount, wpMaxMemory);
    }

    public Builder setRuntimePackageCount(long runtimeTableRecCount) {
      this.runtimePackageCount = runtimeTableRecCount;
      return this;
    }

    public Builder setReplicationPointer(@Nullable String replicationPointer) {
      this.replicationPointer = replicationPointer;
      return this;
    }

    public Builder setPreviousExtractType(@Nullable String previousExtractType) {
      this.previousExtractType = previousExtractType;
      return this;
    }

    public Builder setPreviousExtractStatus(@Nullable String previousExtractStatus) {
      this.previousExtractStatus = previousExtractStatus;
      return this;
    }

    public Builder setHierarchical(boolean hierarchical) {
      isHierarchical = hierarchical;
      return this;
    }

    public Builder setDeltaSupported(boolean deltaSupported) {
      isDeltaSupported = deltaSupported;
      return this;
    }

    public Builder setExtractJobId(@Nullable String extractJobId) {
      this.extractJobId = extractJobId;
      return this;
    }

    public Builder setExtractJobName(@Nullable String extractJobName) {
      this.extractJobName = extractJobName;
      return this;
    }

    public Builder setExtractJobStatusFinished(boolean isExtractJobStatusFinished) {
      this.isExtractJobStatusFinished = isExtractJobStatusFinished;
      return this;
    }
  }
}
