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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author sankalpbapat
 *
 */
public class SapOdpPartitionBuilderTest {
  private SapOdpRuntimeConfigInfo sapOdpConfigInfo;

  private SapOdpPartitionBuilder sapOdpPartitionBuilder;

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    sapOdpPartitionBuilder = new SapOdpPartitionBuilder();
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder#build}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBuildWOSplitPkgSuccessful() throws IOException, InterruptedException, ConnectorException {
    int numSplits = 0;
    long packageSize = 0;

    SapOdpRuntimeConfigInfo.Builder odpConfigInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigInfoBuilder.setRuntimePackageCount(16L).setTotalWorkProcCount(100).setAvailableWorkProcCount(50)
      .setWpMaxMemory(40000000L);

    sapOdpConfigInfo = odpConfigInfoBuilder.build();

    List<SapOdpInputSplit> splits = sapOdpPartitionBuilder.build(sapOdpConfigInfo, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for std. config does not match", 16, splits.size());

    SapOdpInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 1L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 1L, split1.getLength());

    SapOdpInputSplit split2 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 3L, split2.getStart());
    Assert.assertEquals("End does not match for split 3", 3L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 3", 1L, split2.getLength());

    SapOdpInputSplit split3 = splits.get(3);
    Assert.assertEquals("Start does not match for split 4", 4L, split3.getStart());
    Assert.assertEquals("End does not match for split 4", 4L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 4", 1L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder#build}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBuildWOSplitSuccessful() throws IOException, InterruptedException, ConnectorException {
    int numSplits = 0;
    long packageSize = 5000;

    SapOdpRuntimeConfigInfo.Builder odpConfigInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigInfoBuilder.setRuntimePackageCount(7L).setTotalWorkProcCount(100).setAvailableWorkProcCount(50)
      .setWpMaxMemory(40000000L);

    sapOdpConfigInfo = odpConfigInfoBuilder.build();

    List<SapOdpInputSplit> splits = sapOdpPartitionBuilder.build(sapOdpConfigInfo, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for std. config does not match", 7, splits.size());

    SapOdpInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 1L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 1L, split1.getLength());

    SapOdpInputSplit split2 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 3L, split2.getStart());
    Assert.assertEquals("End does not match for split 3", 3L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 3", 1L, split2.getLength());

    SapOdpInputSplit split3 = splits.get(3);
    Assert.assertEquals("Start does not match for split 4", 4L, split3.getStart());
    Assert.assertEquals("End does not match for split 4", 4L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 4", 1L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder#build}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBuildWSplitPkgSuccessful() throws IOException, InterruptedException, ConnectorException {
    int numSplits = 5;
    long packageSize = 5000;

    SapOdpRuntimeConfigInfo.Builder odpConfigInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigInfoBuilder.setRuntimePackageCount(15L).setTotalWorkProcCount(100).setAvailableWorkProcCount(50)
      .setWpMaxMemory(40000000L);

    sapOdpConfigInfo = odpConfigInfoBuilder.build();

    List<SapOdpInputSplit> splits = sapOdpPartitionBuilder.build(sapOdpConfigInfo, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for std. config does not match", 5, splits.size());

    SapOdpInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 3L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 3L, split1.getLength());

    SapOdpInputSplit split2 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 7L, split2.getStart());
    Assert.assertEquals("End does not match for split 3", 9L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 3", 3L, split2.getLength());

    SapOdpInputSplit split3 = splits.get(3);
    Assert.assertEquals("Start does not match for split 4", 10L, split3.getStart());
    Assert.assertEquals("End does not match for split 4", 12L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 4", 3L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder#build}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBuildWSplitsSuccessful() throws IOException, InterruptedException, ConnectorException {
    int numSplits = 5;
    long packageSize = 0L;

    SapOdpRuntimeConfigInfo.Builder odpConfigInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigInfoBuilder.setRuntimePackageCount(12L).setTotalWorkProcCount(100).setAvailableWorkProcCount(50)
      .setWpMaxMemory(40000000L);

    sapOdpConfigInfo = odpConfigInfoBuilder.build();

    List<SapOdpInputSplit> splits = sapOdpPartitionBuilder.build(sapOdpConfigInfo, numSplits, packageSize);

    Assert.assertEquals("InputSplit count for std. config does not match", 5, splits.size());

    SapOdpInputSplit split1 = splits.get(0);
    Assert.assertEquals("Start does not match for split 1", 1L, split1.getStart());
    Assert.assertEquals("End does not match for split 1", 3L, split1.getEnd());
    Assert.assertEquals("Length does not match for split 1", 3L, split1.getLength());

    SapOdpInputSplit split2 = splits.get(2);
    Assert.assertEquals("Start does not match for split 3", 7L, split2.getStart());
    Assert.assertEquals("End does not match for split 3", 8L, split2.getEnd());
    Assert.assertEquals("Length does not match for split 3", 2L, split2.getLength());

    SapOdpInputSplit split3 = splits.get(3);
    Assert.assertEquals("Start does not match for split 4", 9L, split3.getStart());
    Assert.assertEquals("End does not match for split 4", 10L, split3.getEnd());
    Assert.assertEquals("Length does not match for split 4", 2L, split3.getLength());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder#build}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBuildWNoWorkProcessSuccessful() throws IOException, InterruptedException {
    int numSplits = 1;
    long packageSize = 25L;

    SapOdpRuntimeConfigInfo.Builder odpConfigInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigInfoBuilder.setRuntimePackageCount(12L).setTotalWorkProcCount(100).setAvailableWorkProcCount(0)
      .setWpMaxMemory(40000000L);

    sapOdpConfigInfo = odpConfigInfoBuilder.build();
    try {
      sapOdpPartitionBuilder.build(sapOdpConfigInfo, numSplits, packageSize);

    } catch (ConnectorException e) {
      String errMsg = ResourceConstants.ERR_UNAVAILABLE_WP.getMsgForKeyWithCode();
      Assert.assertEquals(errMsg, e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.odp.source.input.SapOdpPartitionBuilder#build}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBuildWNoWorkProcessMemSuccessful() {
    int numSplits = 1;
    long packageSize = 25L;

    SapOdpRuntimeConfigInfo.Builder odpConfigInfoBuilder = SapOdpRuntimeConfigInfo.builder();
    odpConfigInfoBuilder.setRuntimePackageCount(12L).setTotalWorkProcCount(100).setAvailableWorkProcCount(50)
      .setWpMaxMemory(0);

    sapOdpConfigInfo = odpConfigInfoBuilder.build();
    try {
      sapOdpPartitionBuilder.build(sapOdpConfigInfo, numSplits, packageSize);
    } catch (ConnectorException e) {
      String errMsg = ResourceConstants.ERR_UNAVAILABLE_MAX_MEMORY_FOR_WP.getMsgForKeyWithCode();
      Assert.assertEquals(errMsg, e.getMessage());
    }
  }
}
