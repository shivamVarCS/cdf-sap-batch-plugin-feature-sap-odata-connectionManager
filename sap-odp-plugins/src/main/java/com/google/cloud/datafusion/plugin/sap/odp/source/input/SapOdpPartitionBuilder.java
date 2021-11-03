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

package com.google.cloud.datafusion.plugin.sap.odp.source.input;

import com.google.cloud.datafusion.plugin.sap.exception.ConnectorException;
import com.google.cloud.datafusion.plugin.sap.odp.metadata.model.SapOdpRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sankalpbapat
 *
 */
public class SapOdpPartitionBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapOdpPartitionBuilder.class);

  private static final double MAX_WORK_PROCESS_USAGE_FACTOR = 0.5d;
  private static final double MAX_MEMORY_USAGE_FACTOR = 0.7d;
  // SAP considers 50 MB as default package size in absence of user input
  private static final long DEFAULT_PACKAGE_SIZE_BYTES = 52428800L;

  // These will save the updated/optimized values
  private long calculatedPackagesToFetch;
  private long optimizedNumSplits;
  private long optimizedPackageSizeBytes;

  public List<SapOdpInputSplit> build(SapOdpRuntimeConfigInfo runtimeConfig, long numSplits, long packageSizeBytes)
    throws ConnectorException {

    calculatedPackagesToFetch = runtimeConfig.getRuntimePackageCount();
    optimizedNumSplits = numSplits;
    optimizedPackageSizeBytes = packageSizeBytes;

    long netCallCount = calcNetCallCount(runtimeConfig);
    if (netCallCount < 1) {
      // Create 1 split even when no package count
      optimizedNumSplits = 1;
    }

    LOGGER.info(ResourceConstants.INFO_EXTRACT_NUM_PACKAGES.getMsgForKey(calculatedPackagesToFetch, netCallCount));
    LOGGER.info(ResourceConstants.INFO_FOUND_AVAILABLE_WP.getMsgForKey(runtimeConfig.getAvailableWorkProcCount()));
    LOGGER.info(ResourceConstants.INFO_NUM_SPLITS.getMsgForKey(optimizedNumSplits));

    // Number of network calls (with full packageSize), each split has to make. E.g:
    // splits = 10 and netCallCount = 34, then each split must make at least 34 / 10
    // = 3 network calls having data size = packageSize.
    long fullPackagePerSplit = calculatedPackagesToFetch / optimizedNumSplits;
    // Number of splits which must make these additional network calls. E.g: 34 % 10
    // = 4 splits will have their last network call with full packageSize
    long splitCountWLastFullPackage = calculatedPackagesToFetch % optimizedNumSplits;
    // Creates most equitable distribution of network calls to splits. E.g: [4, 4,
    // 4, 4, 3, 3, 3, 3, 3, 3]. Along the way, start and end indices of records in
    // the splits are created by multiplying exact network calls with packageSize
    return createSplits(fullPackagePerSplit, splitCountWLastFullPackage);
  }

  /**
   * Calculates the exact number of network calls required to extract {@code n}
   * data. As part of the process, also determines the optimum/maximum values for
   * parameters like number of splits and package size.
   *
   * @param runtimeConfig
   * @return
   */
  private long calcNetCallCount(SapOdpRuntimeConfigInfo runtimeConfig) throws ConnectorException {

    if (calculatedPackagesToFetch < 1) {
      return 0L;
    }

    // Available Work Process count in SAP will never be > Integer.MAX_VALUE, but
    // result is cast to long for synchronous comparisons with other params
    long maxNumSplits = (long) (runtimeConfig.getAvailableWorkProcCount() * MAX_WORK_PROCESS_USAGE_FACTOR);
    // No need to calculate further if max maxNumSplits is 0
    if (maxNumSplits < 1) {
      throw new ConnectorException(ResourceConstants.ERR_UNAVAILABLE_WP.getCode(),
        ResourceConstants.ERR_UNAVAILABLE_WP.getMsgForKeyWithCode());
    }

    // If user entered the split count
    if (optimizedNumSplits > 0) {
      // If user entered splitCount < packagesToFetch, use split count otherwise use
      // packagesToFetch
      optimizedNumSplits = Math.min(optimizedNumSplits, calculatedPackagesToFetch);
      // Do not allow more than MAX_WORK_PROCESS_USAGE_FACTOR of available SAP work
      // processes from being used
      optimizedNumSplits = Math.min(optimizedNumSplits, maxNumSplits);
    }

    long maxPackageSize = (long) (runtimeConfig.getWpMaxMemory() * MAX_MEMORY_USAGE_FACTOR);

    // No need to calculate further if max packageSize is 0
    if (maxPackageSize < 1) {
      throw new ConnectorException(ResourceConstants.ERR_UNAVAILABLE_MAX_MEMORY_FOR_WP.getCode(),
        ResourceConstants.ERR_UNAVAILABLE_MAX_MEMORY_FOR_WP.getMsgForKeyWithCode());
    }
    // Package Size NOT specified by user
    if (optimizedPackageSizeBytes < 1) {
      optimizedPackageSizeBytes = Math.min(DEFAULT_PACKAGE_SIZE_BYTES, maxPackageSize);
    } else {
      optimizedPackageSizeBytes = Math.min(optimizedPackageSizeBytes, maxPackageSize);
    }

    // Exact network call count needed to extract data
    long exactNetworkCallCount = calculatedPackagesToFetch;

    // Splits NOT specified by user
    if (optimizedNumSplits < 1) {
      optimizedNumSplits = Math.min(exactNetworkCallCount, maxNumSplits);
    }

    LOGGER.info(ResourceConstants.INFO_SIZE_PACKAGE.getMsgForKey(optimizedPackageSizeBytes));

    return exactNetworkCallCount;
  }

  /**
   * Creates array of size {@code splitCount}, each index of which refers to the
   * exact number of network calls that must be made by that split to have the
   * most equitable distribution of calls.
   *
   * @param fullPackagePerSplit
   * @param splitCountWLastFullPackage
   * @return
   */
  private List<SapOdpInputSplit> createSplits(long fullPackagePerSplit, long splitCountWLastFullPackage) {
    long start;
    long end = 0L;
    List<SapOdpInputSplit> partitions = new ArrayList<>();

    for (int i = 0; i < optimizedNumSplits; i++) {
      long exactPackageCount = fullPackagePerSplit;
      if (i < splitCountWLastFullPackage) {
        exactPackageCount += 1;
      }

      start = end + 1;
      end += exactPackageCount;

      // 'start' is the index of fist package beginning at 1 and 'end' is index of
      // last package for this split
      SapOdpInputSplit partition = new SapOdpInputSplit(start, end);

      partitions.add(partition);
    }

    return partitions;
  }
}
