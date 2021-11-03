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

package com.google.cloud.datafusion.plugin.sap.table.source.input;

import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapTableRuntimeConfigInfo;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sankalpbapat
 *
 */
public class SapTablePartitionBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapTablePartitionBuilder.class);

  private static final double MAX_WORK_PROCESS_USAGE_FACTOR = 0.5d;
  private static final double MAX_MEMORY_USAGE_FACTOR = 0.7d;
  private static final long DEFAULT_PACKAGE_SIZE = 70000L;

  // These will save the updated/optimized values
  private long calculatedRowsToFetch;
  private int optimizedNumSplits;
  private long optimizedPackageSize;

  public List<SapTableInputSplit> build(SapTableRuntimeConfigInfo runtimeConfig, long extractableRowCount,
                                        long rowsToFetch, int numSplits, long packageSize) {

    calculatedRowsToFetch = rowsToFetch;
    optimizedNumSplits = numSplits;
    optimizedPackageSize = packageSize;

    double netCallCount = calcNetCallCount(runtimeConfig, extractableRowCount);
    int totalNwCalls = (int) Math.ceil(netCallCount);
    if (totalNwCalls < 1) {
      // Create 1 split even when no records can be extracted
      optimizedNumSplits = 1;
      optimizedPackageSize = 0L;
    }
    LOGGER.info(ResourceConstants.INFO_EXTRACT_NUM_RECORDS.getMsgForKey(calculatedRowsToFetch, totalNwCalls));

    LOGGER.info(ResourceConstants.INFO_FOUND_AVAILABLE_WP.getMsgForKey(runtimeConfig.getAvailableWorkProcCount()));
    LOGGER.info(ResourceConstants.INFO_NUM_SPLITS.getMsgForKey(optimizedNumSplits));

    // Number of network calls (with full packageSize), each split has to make. E.g:
    // splits = 10 and netCallCount = 34.6, then each split must make at least 34 /
    // 10 = 3 network calls having recordCount = packageSize.
    int fullCallsPerSplit = ((int) netCallCount) / optimizedNumSplits;
    // Number of splits which must make these additional network calls. E.g: 34 % 10
    // = 4 splits will have their last network call with full packageSize
    int splitCountWLastFullCall = ((int) netCallCount) % optimizedNumSplits;
    // Creates most equitable distribution of network calls to splits.
    // E.g: [4, 4, 4, 4, 3.6, 3, 3, 3, 3, 3]. Along the way, start and end indices
    // of records in the splits are created by multiplying exact network calls with
    // packagSize
    return createSplits(fullCallsPerSplit, splitCountWLastFullCall, netCallCount);
  }

  /**
   * Calculates the exact number of network calls (in decimal) required to extract
   * {@code n} rows. As part of the process, also determines the optimum/maximum
   * values for parameters like number of splits and package size.
   * 
   * @param runtimeConfig
   * @return
   */
  private double calcNetCallCount(SapTableRuntimeConfigInfo runtimeConfig, long extractableRowCount) {
    // Update number of rows to fetch based on extractable row count and user input
    // rows to fetch
    calculatedRowsToFetch =
      calculatedRowsToFetch < 1 ? extractableRowCount : Math.min(extractableRowCount, calculatedRowsToFetch);

    // No need to calculate further if 0 records match the condition for extraction
    if (calculatedRowsToFetch < 1) {
      return 0d;
    }

    int maxNumSplits = (int) (runtimeConfig.getAvailableWorkProcCount() * MAX_WORK_PROCESS_USAGE_FACTOR);
    // No need to calculate further if max maxNumSplits is 0
    if (maxNumSplits < 1) {
      return 0d;
    }

    long rowsPerSplit = calculatedRowsToFetch;
    // If user entered the split count, and it is <= rowsToFetch
    if (optimizedNumSplits > 0 && optimizedNumSplits <= calculatedRowsToFetch) {
      // Do not allow more than MAX_WORK_PROCESS_USAGE_FACTOR of available SAP work
      // processes from being used
      optimizedNumSplits = Math.min(optimizedNumSplits, maxNumSplits);
      rowsPerSplit = 1 + (calculatedRowsToFetch - 1) / optimizedNumSplits;
    } else if (optimizedNumSplits > 0) {
      // If user entered the split count, but it is > rowsToFetch
      optimizedNumSplits = 1;
    }

    long maxPackageSize =
      (long) (runtimeConfig.getWpMaxMemory() * MAX_MEMORY_USAGE_FACTOR / runtimeConfig.getRecordSize());

    // No need to calculate further if max packageSize is 0
    if (maxPackageSize < 1) {
      return 0d;
    }

    // Package Size NOT specified by user
    if (optimizedPackageSize < 1) {
      optimizedPackageSize = Math.min(DEFAULT_PACKAGE_SIZE, maxPackageSize);
    } else {
      optimizedPackageSize = Math.min(optimizedPackageSize, maxPackageSize);
    }
    optimizedPackageSize = Math.min(optimizedPackageSize, rowsPerSplit);

    // Exact network call count needed to extract 'rowsToFetch' records, with all
    // calls fetching 'packageSize' records, except may be 1 call (if division
    // result is not a whole number)
    double exactNetworkCallCount = ((double) calculatedRowsToFetch) / optimizedPackageSize;

    // Splits NOT specified by user
    if (optimizedNumSplits < 1) {
      optimizedNumSplits = (int) Math.min(Math.ceil(exactNetworkCallCount), maxNumSplits);
    }

    LOGGER.info(ResourceConstants.INFO_NUM_RECORDS_PACKAGE.getMsgForKey(optimizedPackageSize));

    return exactNetworkCallCount;
  }

  /**
   * Creates array of size {@code splitCount}, each index of which refers to the
   * exact number of network calls that must be made by that split to have the
   * most equitable distribution of calls.
   * 
   * @param packageSize
   * @param fullCallsPerSplit
   * @param splitCountWLastFullCall
   * @param netCallCount
   * @param splitCount
   * @return
   */
  private List<SapTableInputSplit> createSplits(int fullCallsPerSplit, int splitCountWLastFullCall,
                                                double netCallCount) {

    long start;
    long end = 0L;
    List<SapTableInputSplit> partitions = new ArrayList<>(optimizedNumSplits);

    for (int i = 0; i < optimizedNumSplits; i++) {
      double exactNetworkCalls = fullCallsPerSplit;
      if (i < splitCountWLastFullCall) {
        exactNetworkCalls += 1;
      } else if (i == splitCountWLastFullCall) {
        exactNetworkCalls += netCallCount - ((int) netCallCount);
      }

      start = end + 1;
      end +=
        BigDecimal.valueOf(exactNetworkCalls * optimizedPackageSize).setScale(0, BigDecimal.ROUND_HALF_UP).longValue();

      // 'start' is the index of fist row beginning at 1 and 'end' is index of last
      // row for this split
      SapTableInputSplit partition = new SapTableInputSplit(start, end, optimizedPackageSize);

      partitions.add(partition);
    }

    return partitions;
  }
}
