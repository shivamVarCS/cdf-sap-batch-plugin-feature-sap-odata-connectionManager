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

package com.google.cloud.datafusion.plugin.sap.odata.source.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This {@code SapODataPartitionBuilder} will prepare the list of splits containing the starting & ending indexes
 * for each split including the optimized package size.
 * <p>
 * Splits are given more priority compared to package size i.e. if the given package size is greater then the
 * calculated package size then actual package size is reduced to the calculated one
 * e.g.
 * total available row count = 123
 * number of rows to extract = 100
 * number of rows to skip = 19
 * total splits = 7
 * package size = 30
 * <p>
 * so the calculation will be as follows:
 * Total number to available record: 123
 * Total number to skipped record: 19
 * Total number of record to extract: 100
 * Record extraction to begin at index: 20
 * Record extraction to end at index: 119
 * Total number of splits: 7
 * Optimal record count to extract on the each splits: 14
 * Left over record count: 2
 * Optimal package size in each splits: 14
 * explanation: here the original package size is (30) but optimization process will override it to (14) for uniform
 * load distributions
 */
public class SapODataPartitionBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataPartitionBuilder.class);

  //TODO: identify the max allowed parallel splits by SAP gateway after the performance test
  private static final int MAX_ALLOWED_SPLITS = 50;
  //TODO: identify the optimal package size after the performance test
  private static final long DEFAULT_PACKAGE_SIZE = 1000L;
  //TODO: identify the max batch size after the performance test
  private static final long MAX_ALLOWED_BATCH_SIZE = 5000L;

  /**
   * builds the list of {@code SapODataInputSplit}
   *
   * @param availableRecordCount available row count
   * @param fetchRowCount        plugin property, number of rows to extract
   * @param skipRowCount         plugin property, number of rows to skip
   * @param splitCount           plugin property, number of splits required
   * @param packageSize          plugin property, size of the package in each splits
   * @return list of {@code SapODataInputSplit}
   */
  public List<SapODataInputSplit> buildSplit(long availableRecordCount, long fetchRowCount,
                                             long skipRowCount,
                                             int splitCount, long packageSize) {
    List<SapODataInputSplit> list = new ArrayList<>();

    long recordReadStartIndex = (skipRowCount == 0 ? 1 : (skipRowCount + 1));
    long actualRecordToExtract = (fetchRowCount == 0 ? availableRecordCount - skipRowCount : fetchRowCount);
    long totalCount = actualRecordToExtract + skipRowCount;
    long recordReadEndIndex = Math.min(totalCount, availableRecordCount);
    if (totalCount > availableRecordCount) {
      actualRecordToExtract = availableRecordCount - skipRowCount;
      if (actualRecordToExtract <= 0) {
        throw new IllegalArgumentException("Found no record to extract. Please check the 'Advanced " +
          "properties' i.e. 'Number of rows to skip' and 'Number of rows to fetch'.");
      }
    }

    if (packageSize == 0) {
      // defaulting the package size
      packageSize = DEFAULT_PACKAGE_SIZE;
    }

    // setting up the optimal values based on max allowed values
    packageSize = Math.min(packageSize, actualRecordToExtract);
    packageSize = Math.min(packageSize, MAX_ALLOWED_BATCH_SIZE);

    if (splitCount == 0) {
        // defaulting the initial split count based on the total records to extract and the package size to extract in
        // each split.
        splitCount = Math.toIntExact(actualRecordToExtract / packageSize);
    }

    //    splitCount = Math.min(splitCount, MAX_ALLOWED_SPLITS);

    long optimalLoadOnSplit =
      (splitCount == 1 ? actualRecordToExtract : Math.floorDiv(actualRecordToExtract, splitCount));
    long optimalPackageSize = Math.min(optimalLoadOnSplit, packageSize);

    long leftoverLoadCount = actualRecordToExtract % splitCount;

    LOGGER.info("Total number to available record: {}", availableRecordCount);
    LOGGER.info("Total number to skipped record: {}", skipRowCount);
    LOGGER.info("Total number of record to extract: {}", actualRecordToExtract);
    LOGGER.info("Record extraction to begin at index: {}", recordReadStartIndex);
    LOGGER.info("Record extraction to end at index: {}", recordReadEndIndex);
    LOGGER.info("Calculated number of splits: {}", splitCount);
    LOGGER.info("Optimal record count to extract on the each splits: {}", optimalLoadOnSplit);
    LOGGER.info("Optimal package size in each splits: {}", optimalPackageSize);

//    System.out.println("Total number to available record: " + availableRecordCount);
//    System.out.println("Total number to skipped record: " + skipRowCount);
//    System.out.println("Total number of record to extract: " + actualRecordToExtract);
//    System.out.println("Record extraction to begin at index: " + recordReadStartIndex);
//    System.out.println("Record extraction to end at index: " + recordReadEndIndex);
//    System.out.println("Calculated number of splits: " + (splitCount == 0 ? 1 : splitCount));
//    System.out.println("Optimal record count to extract on the each splits: " + optimalLoadOnSplit);
//    System.out.println("Left over record count: " + leftoverLoadCount);
//    System.out.println("Optimal package size in each splits: " + optimalPackageSize);


    long start = skipRowCount;
    long end = skipRowCount;
    long extraLoad = (leftoverLoadCount >= 1 ? 1 : 0);

    for (int i = 1; i < splitCount; i++) {
      start += 1;
      end += (optimalLoadOnSplit + extraLoad);
      //prepare splits
      list.add(new SapODataInputSplit(start, end, (optimalPackageSize + extraLoad)));
      start = end;
      if (--leftoverLoadCount <= 0) {
        extraLoad = 0;
      }
    }
    start += 1;
    end = recordReadEndIndex;
    //prepare splits
    list.add(new SapODataInputSplit(start, end, (packageSize == 0 ? end - start + 1 : optimalPackageSize)));

    list.forEach(s -> LOGGER.info(
      "start: " + s.getStart() + "\nend: " + s.getEnd() + "\ndiff: " + (s.getEnd() - s.getStart()) + "\nbatch: " +
        s.getPackageSize() + "\n\n"));

    list.forEach(s -> System.out.println(
      "start: " + s.getStart() + "\nend: " + s.getEnd() + "\ndiff: " + (s.getEnd() - s.getStart()) + "\nbatch: " +
        s.getPackageSize() + "\n\n"));

    return list;
  }


}
