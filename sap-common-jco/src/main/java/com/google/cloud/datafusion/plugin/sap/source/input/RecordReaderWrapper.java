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

package com.google.cloud.datafusion.plugin.sap.source.input;

import com.google.cloud.datafusion.plugin.util.JcoLibrariesManager;

import io.cdap.cdap.api.data.format.StructuredRecord;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author sankalpbapat
 *
 */
public class RecordReaderWrapper extends RecordReader<LongWritable, StructuredRecord> {

  private final RecordReader<LongWritable, StructuredRecord> delegateReader;
  private final Path cleanupPath;

  public RecordReaderWrapper(RecordReader<LongWritable, StructuredRecord> delegateReader, Path cleanupPath) {
    this.delegateReader = delegateReader;
    this.cleanupPath = cleanupPath;
  }

  @Override
  public void initialize(InputSplit paramInputSplit, TaskAttemptContext taContext)
    throws IOException, InterruptedException {

    delegateReader.initialize(paramInputSplit, taContext);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegateReader.nextKeyValue();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return delegateReader.getCurrentKey();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
    return delegateReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegateReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    JcoLibrariesManager.cleanUpResources(delegateReader, cleanupPath);
  }
}
