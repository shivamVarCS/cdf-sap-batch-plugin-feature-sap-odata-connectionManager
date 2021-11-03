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

import com.google.cloud.datafusion.plugin.sap.source.SapJcoBatchSourceWrapper;
import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.JcoLibrariesManager;

import io.cdap.cdap.api.data.format.StructuredRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sankalpbapat
 *
 */
public abstract class SapJcoInputFormat extends InputFormat<LongWritable, StructuredRecord> {

  private JcoLibrariesManager jcoLibMgr;

  @Override
  public RecordReader<LongWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext taContext)
    throws IOException, InterruptedException {

    RecordReader<LongWritable, StructuredRecord> delegateReader =
      createDelegateReader(split, taContext.getConfiguration());

    return new RecordReaderWrapper(delegateReader, jcoLibMgr.getLocalTempDirPath());
  }

  private RecordReader<LongWritable, StructuredRecord> createDelegateReader(InputSplit split, Configuration conf)
    throws IOException {

    jcoLibMgr = new JcoLibrariesManager();
    File jcoJarFile = jcoLibMgr.downloadAndGetLocalJcoPath(conf.get(SapJcoPluginConfigWrapper.GCP_PROJECT_ID),
      conf.get(SapJcoPluginConfigWrapper.GCS_PATH));

    ClassLoader jcoClassLoader = jcoLibMgr.createJcoClassLoader(jcoJarFile);

    return loadInitRecordReader(jcoClassLoader, conf, split);
  }

  protected abstract RecordReader<LongWritable, StructuredRecord>
    loadInitRecordReader(ClassLoader jcoClassLoader, Configuration conf, InputSplit split) throws IOException;

  /**
   * Reconstruct map of JCo properties from Hadoop job's configuration
   * 
   * @param conf Hadoop configuration
   * @return Map of JCo property name and value
   */
  protected Map<String, String> getJcoPropsFromConf(Configuration conf) {
    String[] props = conf.getStrings(SapJcoBatchSourceWrapper.PLUGIN_SOURCE_JCO_PROPERTY_NAMES);
    Map<String, String> pluginProps = new HashMap<>();
    for (int i = 0; i < props.length; i++) {
      String propName = props[i];
      if (propName.startsWith("jco.")) {
        pluginProps.put(propName, conf.get(propName));
      }
    }

    return pluginProps;
  }
}
