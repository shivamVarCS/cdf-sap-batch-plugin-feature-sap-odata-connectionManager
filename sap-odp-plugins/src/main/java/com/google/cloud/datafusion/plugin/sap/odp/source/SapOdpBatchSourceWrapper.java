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

package com.google.cloud.datafusion.plugin.sap.odp.source;

import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfig;
import com.google.cloud.datafusion.plugin.sap.odp.source.config.SapOdpPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.sap.source.SapJcoBatchSourceWrapper;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.BatchSource;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Performs class loading and system library setup before delegating all actual
 * work to {@link SapOdpBatchSource}.
 * 
 * @author sankalpbapat
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapOdpBatchSourceWrapper.NAME)
@Description("Reads SAP ECC or S/4 HANA On-Premise system's native data (full & delta) from Extractors via ODP")
public class SapOdpBatchSourceWrapper extends SapJcoBatchSourceWrapper {

  public static final String NAME = "SapOdp";

  private final SapOdpPluginConfigWrapper config;

  public SapOdpBatchSourceWrapper(SapOdpPluginConfigWrapper config) {
    super(config);
    this.config = config;
  }

  @SuppressWarnings("unchecked")
  protected BatchSource<LongWritable, StructuredRecord, StructuredRecord>
    loadInitPluginAndBatchConfig(ClassLoader jcoClassLoader) throws IOException {

    try {
      // can't pass in the config directly, since it is defined by the plugin
      // classloader already
      // need to create one defined by this new modified classloader
      Class<?> configClass = jcoClassLoader.loadClass(SapOdpPluginConfig.class.getName());
      Class<?> batchSourceClass = jcoClassLoader.loadClass(SapOdpBatchSource.class.getName());

      return (BatchSource<LongWritable, StructuredRecord, StructuredRecord>) batchSourceClass
        .getConstructor(configClass).newInstance(configClass.getConstructor(PluginConfig.class).newInstance(config));
    } catch (Exception e) {
      // should not happen
      throw new IllegalStateException("Unable to instantiate batch source with modified classloader.", e);
    }
  }
}
