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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author sankalpbapat
 *
 */
public class SapTablePartitionBuilderTest {

  private SapTableRuntimeConfigInfo sapTableRuntimeConfigInfo;

  private SapTablePartitionBuilder sapTablePartitionBuilder;

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    sapTablePartitionBuilder = new SapTablePartitionBuilder();
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWORowSplitPkgSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 1000L;
    long rowsToFetch = 0;
    int numSplits = 0;
    long packageSize = 0;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(300000L).setRecordSize(10000).setTotalWorkProcCount(100)
      .setAvailableWorkProcCount(50).setWpMaxMemory(4000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for std. config does not match", 4, splits.size());

    SapTableInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 280L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 280L, split1.getLength());

    SapTableInputSplit split2 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 561L, split2.getStart());
    Assert.assertEquals("End does not match for split 3", 840L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 3", 280L, split2.getLength());

    SapTableInputSplit split3 = splits.get(3);
    Assert.assertEquals("Start does not match for split 4", 841L, split3.getStart());
    Assert.assertEquals("End does not match for split 4", 1000L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 4", 160L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWORowSplitSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 200000L;
    long rowsToFetch = 0;
    int numSplits = 0;
    long packageSize = 63000L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(300000L).setRecordSize(10000).setTotalWorkProcCount(70)
      .setAvailableWorkProcCount(35).setWpMaxMemory(40000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set packageSize does not match", 17, splits.size());

    SapTableInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 14000L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 14000L, split1.getLength());

    SapTableInputSplit split2 = splits.get(1);
    Assert.assertEquals("Start does not match for split 2", 14001L, split2.getStart());
    Assert.assertEquals("End does not match for split 2", 28000L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 2", 14000L, split2.getLength());

    SapTableInputSplit split8 = splits.get(7);
    Assert.assertEquals("Start does not match for split 8", 88001L, split8.getStart());
    Assert.assertEquals("End does not match for split 8", 99200L, split8.getEnd());
    Assert.assertEquals("Length does not match for split 8", 11200L, split8.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWORowPkgSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 200000L;
    long rowsToFetch = 0;
    int numSplits = 11;
    long packageSize = 0;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(300000L).setRecordSize(10000).setTotalWorkProcCount(70)
      .setAvailableWorkProcCount(35).setWpMaxMemory(40000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set numSplits does not match", 11, splits.size());

    SapTableInputSplit split5 = splits.get(4);
    Assert.assertEquals("Start does not match for split 5", 78401L, split5.getStart());
    Assert.assertEquals("End does not match for split 5", 98000L, split5.getEnd());
    Assert.assertEquals("Length does not match for split 5", 19600L, split5.getLength());

    SapTableInputSplit split9 = splits.get(8);
    Assert.assertEquals("Start does not match for split 9", 149601L, split9.getStart());
    Assert.assertEquals("End does not match for split 9", 166400L, split9.getEnd());
    Assert.assertEquals("Length does not match for split 9", 16800L, split9.getLength());

    SapTableInputSplit split10 = splits.get(9);
    Assert.assertEquals("Start does not match for split 10", 166401L, split10.getStart());
    Assert.assertEquals("End does not match for split 10", 183200L, split10.getEnd());
    Assert.assertEquals("Length does not match for split 10", 16800L, split10.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWORowSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 200000L;
    long rowsToFetch = 0;
    int numSplits = 12;
    long packageSize = 43500L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(300000L).setRecordSize(10000).setTotalWorkProcCount(100)
      .setAvailableWorkProcCount(50).setWpMaxMemory(40000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set numSplits & packageSize does not match", 12, splits.size());

    SapTableInputSplit split1 = (SapTableInputSplit) splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 16800L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 16800L, split1.getLength());

    SapTableInputSplit split4 = (SapTableInputSplit) splits.get(3);
    Assert.assertEquals("Start does not match for split 4", 50401L, split4.getStart());
    Assert.assertEquals("End does not match for split 4", 67200L, split4.getEnd());
    Assert.assertEquals("Length does not match for split 4", 16800L, split4.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWOSplitPkgSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 200000L;
    long rowsToFetch = 1200000L;
    int numSplits = 0;
    long packageSize = 0L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000000L).setRecordSize(10000).setTotalWorkProcCount(50)
      .setAvailableWorkProcCount(25).setWpMaxMemory(40000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set rowsToFetch does not match", 12, splits.size());

    SapTableInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 16800L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 16800L, split1.getLength());

    SapTableInputSplit split2 = splits.get(1);
    Assert.assertEquals("Start does not match for split 2", 16801L, split2.getStart());
    Assert.assertEquals("End does not match for split 2", 33600L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 2", 16800L, split2.getLength());

    SapTableInputSplit split8 = splits.get(7);
    Assert.assertEquals("Start does not match for split 8", 117601L, split8.getStart());
    Assert.assertEquals("End does not match for split 8", 134400L, split8.getEnd());
    Assert.assertEquals("Length does not match for split 8", 16800L, split8.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWOSplitSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 30000L;
    long rowsToFetch = 43600L;
    int numSplits = 0;
    long packageSize = 21500L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000000L).setRecordSize(20000).setTotalWorkProcCount(50)
      .setAvailableWorkProcCount(25).setWpMaxMemory(40000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set rowsToFetch & packageSize does not match", 12, splits.size());

    SapTableInputSplit split2 = (SapTableInputSplit) splits.get(1);
    Assert.assertEquals("Start does not match for split 2", 2801L, split2.getStart());
    Assert.assertEquals("End does not match for split 2", 5600L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 2", 2800L, split2.getLength());

    SapTableInputSplit split3 = (SapTableInputSplit) splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 5601L, split3.getStart());
    Assert.assertEquals("End does not match for split 3", 8400L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 3", 2800L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWOPkgSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 300L;
    long rowsToFetch = 470L;
    int numSplits = 3;
    long packageSize = 0;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000000L).setRecordSize(50000).setTotalWorkProcCount(50)
      .setAvailableWorkProcCount(25).setWpMaxMemory(20000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set rowsToFetch & numSplits does not match", 3, splits.size());

    SapTableInputSplit split2 = splits.get(1);
    Assert.assertEquals("Start does not match for split 2", 101L, split2.getStart());
    Assert.assertEquals("End does not match for split 2", 200L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 2", 100L, split2.getLength());

    SapTableInputSplit split3 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 201L, split3.getStart());
    Assert.assertEquals("End does not match for split 3", 300L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 3", 100L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWRowSplitPkgSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 10000L;
    long rowsToFetch = 13570L;
    int numSplits = 6;
    long packageSize = 5000;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000000L).setRecordSize(50000).setTotalWorkProcCount(60)
      .setAvailableWorkProcCount(30).setWpMaxMemory(20000000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set rowsToFetch, numSplits & packageSize does not match", 6,
      splits.size());

    SapTableInputSplit split3 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 3361L, split3.getStart());
    Assert.assertEquals("End does not match for split 3", 5040L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 3", 1680L, split3.getLength());

    SapTableInputSplit split6 = splits.get(5);
    Assert.assertEquals("Start does not match for split 6", 8401L, split6.getStart());
    Assert.assertEquals("End does not match for split 6", 10000L, split6.getEnd());
    Assert.assertEquals("Length does not match for split 6", 1600L, split6.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWRowLessThanSplitSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 100L;
    long rowsToFetch = 15L;
    int numSplits = 16;
    long packageSize = 5L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000L).setRecordSize(50).setTotalWorkProcCount(10)
      .setAvailableWorkProcCount(5).setWpMaxMemory(200000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for set rowsToFetch < numSplits does not match", 1, splits.size());

    SapTableInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 15L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 15L, split1.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWNoActualRecordsSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 0L;
    long rowsToFetch = 1L;
    int numSplits = 8;
    long packageSize = 25000L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000L).setRecordSize(50).setTotalWorkProcCount(10)
      .setAvailableWorkProcCount(5).setWpMaxMemory(200000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for actual row count = 0, does not match", 1, splits.size());

    SapTableInputSplit split1 = (SapTableInputSplit) splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 0L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 0L, split1.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWNoWorkProcessSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 100L;
    long rowsToFetch = 50L;
    int numSplits = 1;
    long packageSize = 25L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000L).setRecordSize(50).setTotalWorkProcCount(40)
      .setAvailableWorkProcCount(0).setWpMaxMemory(200000L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for actual row count = 0, does not match", 1, splits.size());

    SapTableInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 0L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 0L, split1.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.table.source.input.SapTablePartitionBuilder#build()}.
   */
  @Test
  public void testBuildWNoWorkProcessMemSuccessful() throws IOException, InterruptedException {
    long extractTableRowCount = 100L;
    long rowsToFetch = 50L;
    int numSplits = 1;
    long packageSize = 25L;

    SapTableRuntimeConfigInfo.Builder tableConfigInfoBuilder = SapTableRuntimeConfigInfo.builder();
    tableConfigInfoBuilder.setRuntimeTableRecCount(4000L).setRecordSize(50).setTotalWorkProcCount(70)
      .setAvailableWorkProcCount(35).setWpMaxMemory(0L);

    sapTableRuntimeConfigInfo = tableConfigInfoBuilder.build();

    List<SapTableInputSplit> splits = sapTablePartitionBuilder.build(sapTableRuntimeConfigInfo, extractTableRowCount,
      rowsToFetch, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for actual row count = 0, does not match", 1, splits.size());

    SapTableInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 0L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 0L, split1.getLength());
  }
}
