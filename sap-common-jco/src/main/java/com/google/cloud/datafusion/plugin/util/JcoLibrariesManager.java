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

package com.google.cloud.datafusion.plugin.util;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nullable;

/**
 * @author sankalpbapat
 *
 */
public class JcoLibrariesManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JcoLibrariesManager.class);

  public static final String JCO_OBJECT_NAME = "sapjco3.jar";
  public static final String SO_OBJECT_NAME = "libsapjco3.so";

  // Deliberately non-final, as it is needed only when basic validations are
  // successful
  private Path localTempDirPath;

  @Nullable
  public Path getLocalTempDirPath() {
    return localTempDirPath;
  }

  public ClassLoader createJcoClassLoader(File jcoJarFile) {
    try {
      ClassLoader pluginClassLoader = this.getClass().getClassLoader();
      if (!(pluginClassLoader instanceof URLClassLoader)) {
        // won't happen unless something in CDAP changes
        throw new IllegalStateException("Unable to create modified classloader with SAP JCo jar due to CDAP changes."
          + "Please use the plugin with a compatible CDAP version.");
      }

      URL[] pluginURLs = ((URLClassLoader) pluginClassLoader).getURLs();
      URL[] urls = new URL[pluginURLs.length + 1];
      System.arraycopy(pluginURLs, 0, urls, 0, pluginURLs.length);
      urls[urls.length - 1] = jcoJarFile.toURI().toURL();

      // create a classloader that has all the urls from the current plugin
      // classloader except with the jco jar added. Has the plugin's parent
      // classloader as its parent. This is equivalent to the classloader that would
      // have been created if the jco jar could've been packaged with this plugin jar.
      return new URLClassLoader(urls, pluginClassLoader.getParent());
    } catch (Exception e) {
      // should not happen
      throw new IllegalStateException("Unable to instantiate batch source with modified classloader.", e);
    }
  }

  public File downloadAndGetLocalJcoPath(String gcpProjectId, String gcsPathString) throws IOException {
    GCSPath gcsPathObj = GCSPath.from(gcsPathString);
    String bucketName = gcsPathObj.getBucket();
    String bucketFilePath = gcsPathObj.getName();
    // For GCS path of the form gs://cdf-sap-jco-libs/unix/v3_0_20 ; bucketName =
    // 'cdf-sap-jco-libs' and bucketFilePath = 'unix/v3_0_20'; a '/' is needed at
    // the end so that JCo jar file and .so file name could be appended to it
    if (bucketFilePath != null && !bucketFilePath.isEmpty()) {
      bucketFilePath += bucketFilePath.endsWith(GCSPath.ROOT_DIR) ? "" : GCSPath.ROOT_DIR;
    }

    localTempDirPath = Files.createTempDirectory("jco_");
    Path jarFilePath = Paths.get(localTempDirPath.toString(), JCO_OBJECT_NAME);
    Path soFilePath = Paths.get(localTempDirPath.toString(), SO_OBJECT_NAME);

    StorageOptions options = StorageOptions.newBuilder().setProjectId(gcpProjectId)
      .setCredentials(GoogleCredentials.getApplicationDefault()).build();

    Storage storage = options.getService();
    Blob jarBlob = storage.get(bucketName, bucketFilePath + JCO_OBJECT_NAME);
    Blob soBlob = storage.get(bucketName, bucketFilePath + SO_OBJECT_NAME);

    if (jarBlob == null || soBlob == null) {
      throw new IllegalArgumentException(
        "CDF_SAP_01412 - One or more SAP JCo library files are missing. Please make sure the required"
          + " JCo library (sapjco3.jar) and its associated OS dependent shared library (libsapjco3.so)"
          + " were uploaded to your specified Google Cloud Storage bucket '" + gcsPathString + "'.");
    }

    jarBlob.downloadTo(jarFilePath);
    soBlob.downloadTo(soFilePath);

    return new File(jarFilePath.toString());
  }

  public void cleanUpResources(@Nullable Object delegate) {
    cleanUpResources(delegate, localTempDirPath);
  }

  public static void cleanUpResources(@Nullable Object delegate, @Nullable Path localPathToDelete) {
    if (delegate != null) {
      try {
        ((URLClassLoader) delegate.getClass().getClassLoader()).close();
      } catch (IOException e) {
        LOGGER.warn("Unable to close custom JCo classloader", e);
      }
    }

    if (localPathToDelete == null) {
      return;
    }

    File folderToDelete = localPathToDelete.toFile();
    // Folder can't be deleted unless its contents are removed
    File[] contents = folderToDelete.listFiles();
    if (contents != null) {
      for (File f : contents) {
        try {
          Files.deleteIfExists(f.toPath());
        } catch (IOException e) {
          LOGGER.warn("Unable to delete contents at path '{}'", f.getPath(), e);
        }
      }
    }
  }
}
