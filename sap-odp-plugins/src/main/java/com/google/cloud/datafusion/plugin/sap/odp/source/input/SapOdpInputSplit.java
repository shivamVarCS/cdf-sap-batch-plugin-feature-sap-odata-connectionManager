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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Pojo to hold a wrapper for partition details like start and end indices of
 * the packages to be extracted in each split.
 * 
 * @author sankalpbapat
 */
public class SapOdpInputSplit extends InputSplit implements Writable {
  private long start;
  private long end;

  public SapOdpInputSplit() {
  }

  public SapOdpInputSplit(long start, long end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return end - start + 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[] {};
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(start);
    out.writeLong(end);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.start = in.readLong();
    this.end = in.readLong();
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }
}
