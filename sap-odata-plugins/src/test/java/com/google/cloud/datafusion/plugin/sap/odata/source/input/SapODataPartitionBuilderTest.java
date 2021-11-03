package com.google.cloud.datafusion.plugin.sap.odata.source.input;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class SapODataPartitionBuilderTest {

  private long defaultPackageSize = 1000L;

  private SapODataPartitionBuilder partitionBuilder;

  @Before
  public void setUp() {
    partitionBuilder = new SapODataPartitionBuilder();
  }

  @Test
  public void testWithDefaultValue() {
    long availableRowCount = 100;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 0;
    long packageSize = 0;

    List<SapODataInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount, skipRowCount,
      splitCount,
      packageSize);

    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", 100, partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", 100, partitionList.get(0).getPackageSize());
  }

  @Test
  public void testWithSkipRowCount() {
    long availableRowCount = 100;
    long fetchRowCount = 0;
    long skipRowCount = 10;
    int splitCount = 0;
    long packageSize = 0;

    List<SapODataInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
      fetchRowCount, skipRowCount, splitCount, packageSize);

    Assert.assertEquals("Start is not same", 11, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", 100, partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", 90, partitionList.get(0).getPackageSize());
  }

  @Test
  public void testWithExtraLoadOnSplitWithDefaultPackageSize() {
    long availableRowCount = 378403;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 43;
    long packageSize = 0;

    List<SapODataInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
      fetchRowCount, skipRowCount, splitCount, packageSize);

    long expectedExtraLoadCount = availableRowCount % splitCount;
    long optimalLoad = Math.floorDiv(availableRowCount, splitCount);

    Assert.assertEquals("Split size is not same", 43, partitionList.size());
    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", (optimalLoad + 1), partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", (defaultPackageSize + 1),
      partitionList.get(0).getPackageSize());

    long distributedExtraLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getPackageSize() == (defaultPackageSize + 1))
        .count();
    Assert.assertEquals("extra load distribution count is not same",
      expectedExtraLoadCount, distributedExtraLoadCount);

    long distributedLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getPackageSize() == defaultPackageSize).count();
    Assert.assertEquals("Optimal load distribution count is not same",
      (splitCount - expectedExtraLoadCount), distributedLoadCount);
  }

  @Test
  public void testWithExtraLoadOnSplit() {
    long availableRowCount = 378403;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 43;
    long packageSize = 4538;

    List<SapODataInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
      fetchRowCount, skipRowCount, splitCount, packageSize);

    long expectedExtraLoadCount = availableRowCount % splitCount;
    long optimalLoad = Math.floorDiv(availableRowCount, splitCount);

    Assert.assertEquals("Split size is not same", 43, partitionList.size());
    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", (optimalLoad + 1), partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", (packageSize + 1), partitionList.get(0).getPackageSize());

    long distributedExtraLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getPackageSize() == (packageSize + 1)).count();
    Assert.assertEquals("extra load distribution count is not same",
      expectedExtraLoadCount, distributedExtraLoadCount);

    long distributedLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getPackageSize() == packageSize).count();
    Assert.assertEquals("Optimal load distribution count is not same",
      (splitCount - expectedExtraLoadCount), distributedLoadCount);
  }

  @Test
  public void testUpdatedFetchRowCount() {
    long availableRowCount = 123;
    long fetchRowCount = 100;
    long skipRowCount = 40;
    int splitCount = 9;
    long packageSize = 0;

    List<SapODataInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
      fetchRowCount, skipRowCount, splitCount, packageSize);

    long expectedFetchSize = availableRowCount - skipRowCount;
    long actualFetchSize =
      partitionList.stream().collect(Collectors.summarizingLong(SapODataInputSplit::getPackageSize)).getSum();
    Assert.assertEquals("Total record extraction count is not same", expectedFetchSize, actualFetchSize);
  }

  @Test
  public void testPackageSizeOptimizationBasedOnSplitCount() {
    long availableRowCount = 123;
    long fetchRowCount = 100;
    long skipRowCount = 19;
    int splitCount = 7;
    long packageSize = 30;

    List<SapODataInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
      fetchRowCount, skipRowCount, splitCount, packageSize);

    long expectedPackageSize = Math.min(packageSize, (fetchRowCount / splitCount));
    Assert.assertEquals("Package size is not optimized", expectedPackageSize,
      partitionList.get(partitionList.size() - 1).getPackageSize());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoRecordFoundForExtraction() {
    long availableRowCount = 123;
    long fetchRowCount = 1000;
    long skipRowCount = 190;
    int splitCount = 3;
    long packageSize = 30;

    partitionBuilder.buildSplit(availableRowCount, fetchRowCount, skipRowCount, splitCount, packageSize);
  }


  @Test
  public void testNoRecordFoundForExtraction2() {
    long availableRowCount = 10079;
    long fetchRowCount = 2000;
    long skipRowCount = 0;
    int splitCount = 3;
    long packageSize = 3000;

    partitionBuilder.buildSplit(availableRowCount, fetchRowCount, skipRowCount, splitCount, packageSize);
  }
}
