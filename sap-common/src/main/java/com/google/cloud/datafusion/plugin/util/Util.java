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

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class for common/repetitive logic and operations.
 * 
 * @author sankalpbapat
 */
public class Util {

  private static final Pattern datasetIdPattern = Pattern.compile("[$\\.a-zA-Z0-9_-]+");

  private Util() {
  }

  /**
   * Checks a {@code CharSequence} instance for {@code NOT null && NOT empty}.
   * 
   * @param charSeq which needs to be checked
   * @return the boolean result of
   *         {@code (charSeq != null && !charSeq.toString().isEmpty())}
   */
  public static boolean isNotNullOrEmpty(@Nullable CharSequence charSeq) {
    return charSeq != null && !charSeq.toString().isEmpty();
  }

  /**
   * Checks a {@code CharSequence} instance for {@code null || empty}.
   * 
   * @param charSeq which needs to be checked
   * @return the boolean result of
   *         {@code (charSeq == null || charSeq.toString().isEmpty())}
   */
  public static boolean isNullOrEmpty(@Nullable CharSequence charSeq) {
    return !isNotNullOrEmpty(charSeq);
  }

  public static boolean isValidDatasetId(String datasetId) {
    return datasetIdPattern.matcher(datasetId).matches();
  }
}
