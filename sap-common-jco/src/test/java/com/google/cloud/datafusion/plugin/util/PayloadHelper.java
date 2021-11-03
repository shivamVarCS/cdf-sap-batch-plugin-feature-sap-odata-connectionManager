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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Helper class to create payload (Java objects) from data in text files, for
 * use in Junit test cases.
 * 
 * @author sankalpbapat
 */
public class PayloadHelper {

  private PayloadHelper() {
  }

  public static Map<String, String> loadProperties(String propFileName) {
    Map<String, String> map = new HashMap<>();
    try (InputStream propInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(propFileName)) {
      Properties props = new Properties();
      props.load(propInStr);
      for (Entry<Object, Object> propEntry : props.entrySet()) {
        map.put((String) propEntry.getKey(), (String) propEntry.getValue());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return map;
  }

  public static String createPayload(String testFileName) {
    InputStream payloadInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(testFileName);

    return new BufferedReader(new InputStreamReader(payloadInStr)).lines().map(String::trim)
      .collect(Collectors.joining(""));
  }

  public static String createPrettyPayload(String testFileName) {
    InputStream payloadInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(testFileName);

    return new BufferedReader(new InputStreamReader(payloadInStr)).lines().collect(Collectors.joining("\n"));
  }
}
