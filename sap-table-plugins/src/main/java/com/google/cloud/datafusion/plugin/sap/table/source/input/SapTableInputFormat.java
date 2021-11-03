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

import com.google.cloud.datafusion.plugin.sap.source.input.SapJcoInputFormat;
import com.google.cloud.datafusion.plugin.sap.table.source.SapTableBatchSource;
import com.google.cloud.datafusion.plugin.sap.table.source.config.SapTablePluginConfigWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import io.cdap.cdap.api.data.format.StructuredRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Prepares and configures splits (start, end and length) along with the
 * {@code RecordReader}
 * 
 * @author sankalpbapat
 */
public class SapTableInputFormat extends SapJcoInputFormat {
  private static final Type INPUT_SPLIT_TYPE = new TypeToken<List<SapTableInputSplit>>() {

  }.getType();

  @Override
  public List<InputSplit> getSplits(JobContext jContext) throws IOException, InterruptedException {
    Configuration conf = jContext.getConfiguration();
    List<InputSplit> splits = new ArrayList<>();

    // Deserialize partitions from Hadoop Configuration
    Gson gson = new GsonBuilder().create();
    List<SapTableInputSplit> partitions =
      gson.fromJson(conf.get(SapTableBatchSource.PARTITIONS_PROPERTY), INPUT_SPLIT_TYPE);

    for (SapTableInputSplit partition : partitions) {
      splits.add(partition);
    }

    return splits;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected RecordReader<LongWritable, StructuredRecord>
    loadInitRecordReader(ClassLoader jcoClassLoader, Configuration conf, InputSplit split) throws IOException {

    try {
      Class<?> recordReaderClass = jcoClassLoader.loadClass(SapTableRecordReader.class.getName());

      // Organize properties to set in SapRecordReader
      Map<String, String> jcoConnProps = getJcoPropsFromConf(conf);
      String tableName = conf.get(SapTablePluginConfigWrapper.EXTRACT_TABLE_VALUE);
      Gson gson = new GsonBuilder().create();
      List<String> filterOptions = gson.fromJson(conf.get(SapTablePluginConfigWrapper.FILTER_OPTIONS), List.class);
      String schemaStr = conf.get(SapTableBatchSource.OUTPUT_SCHEMA_NAME);
      String totalWorkProcessCount = conf.get(SapTableBatchSource.TOTAL_WORK_PROCESS);

      return (RecordReader<LongWritable, StructuredRecord>) recordReaderClass.getConstructor(Map.class, String.class,
        List.class, String.class, long.class, long.class, long.class, String.class).newInstance(jcoConnProps, tableName,
          filterOptions, schemaStr, ((SapTableInputSplit) split).getStart(), ((SapTableInputSplit) split).getEnd(),
          ((SapTableInputSplit) split).getPackageSize(), totalWorkProcessCount);
    } catch (Exception e) {
      // should not happen
      throw new IllegalStateException("Unable to instantiate RecordReader with modified classloader.", e);
    }
  }
}
