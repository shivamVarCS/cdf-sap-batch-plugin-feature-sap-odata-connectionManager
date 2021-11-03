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

package com.google.cloud.datafusion.plugin.sap.source;

import com.google.cloud.datafusion.plugin.sap.source.config.SapJcoPluginConfigWrapper;
import com.google.cloud.datafusion.plugin.util.JcoLibrariesManager;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;

import org.apache.hadoop.io.LongWritable;

import java.io.File;
import java.io.IOException;

/**
 * Performs class loading and system library setup before delegating all actual
 * work to specific BatchSource implementations.
 * 
 * @author sankalpbapat
 */
public abstract class SapJcoBatchSourceWrapper extends BatchSource<LongWritable, StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_SOURCE_JCO_PROPERTY_NAMES = "sap.batch.source.jco.props";

  private final SapJcoPluginConfigWrapper config;
  private final JcoLibrariesManager jcoLibMgr;

  private BatchSource<LongWritable, StructuredRecord, StructuredRecord> delegate;

  public SapJcoBatchSourceWrapper(SapJcoPluginConfigWrapper config) {
    this.config = config;
    this.jcoLibMgr = new JcoLibrariesManager();
  }

  // configurePipeline is called exactly once when the pipeline is being created.
  // Any static configuration should be performed here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validateGcpParams(collector);
    config.validateConnProps(collector);
    config.validateOptionalProps(collector);
    collector.getOrThrowException();

    if (config.isClassLoadingReqd()) {
      try {
        delegate = createDelegate();
        delegate.configurePipeline(pipelineConfigurer);
      } catch (IOException e) { // Catch needed as super method does NOT have any throws clause
        throw new RuntimeException("Unable to access GCS or download JCo libraries from GCS", e);
      } finally {
        jcoLibMgr.cleanUpResources(delegate);
      }
    }
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {
    FailureCollector collector = batchSourceContext.getFailureCollector();
    config.validateGcpParams(collector);
    config.validateConnProps(collector);
    config.validateOptionalProps(collector);
    collector.getOrThrowException();

    try {
      delegate = createDelegate();
      delegate.prepareRun(batchSourceContext);
    } finally {
      jcoLibMgr.cleanUpResources(delegate);
    }
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    jcoLibMgr.cleanUpResources(delegate);
  }

  protected abstract BatchSource<LongWritable, StructuredRecord, StructuredRecord>
    loadInitPluginAndBatchConfig(ClassLoader jcoClassLoader) throws IOException;

  private BatchSource<LongWritable, StructuredRecord, StructuredRecord> createDelegate() throws IOException {
    File jcoJarFile = jcoLibMgr.downloadAndGetLocalJcoPath(config.getProject(), config.getGcsPathString());

    ClassLoader jcoClassLoader = jcoLibMgr.createJcoClassLoader(jcoJarFile);

    return loadInitPluginAndBatchConfig(jcoClassLoader);
  }
}
