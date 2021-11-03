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

import org.junit.Assert;
import org.junit.Test;

/**
 * Junit test class for {@link Util} class methods
 * 
 * @author sankalpbapat
 */
public class UtilTest {

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.util.Util#isNotNullOrEmpty(java.lang.CharSequence)}.
   */
  @Test
  public void isNotNullOrEmptyString() {
    String testStr = null;
    Assert.assertFalse("String null or empty false", Util.isNotNullOrEmpty(testStr));

    testStr = "";
    Assert.assertFalse("String null or empty false", Util.isNotNullOrEmpty(testStr));

    testStr = "    ";
    Assert.assertTrue("String null or empty true", Util.isNotNullOrEmpty(testStr));

    testStr = "    test";
    Assert.assertTrue("String null or empty true", Util.isNotNullOrEmpty(testStr));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.util.Util#isNotNullOrEmpty(java.lang.CharSequence)}.
   */
  @Test
  public void isNotNullOrEmptyStringBuilder() {
    StringBuilder testStrBuilder = null;
    Assert.assertFalse("StringBuilder null or empty false", Util.isNotNullOrEmpty(testStrBuilder));

    testStrBuilder = new StringBuilder();
    Assert.assertFalse("StringBuilder null or empty false", Util.isNotNullOrEmpty(testStrBuilder));

    testStrBuilder.append("    ");
    Assert.assertTrue("StringBuilder null or empty true", Util.isNotNullOrEmpty(testStrBuilder));

    testStrBuilder.append("    Test String in builder   ");
    Assert.assertTrue("StringBuilder null or empty true", Util.isNotNullOrEmpty(testStrBuilder));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.util.Util#isNotNullOrEmpty(java.lang.CharSequence)}.
   */
  @Test
  public void isNotNullOrEmptyStringBuffer() {
    StringBuffer testStrBuffer = null;
    Assert.assertFalse("StringBuffer null or empty false", Util.isNotNullOrEmpty(testStrBuffer));

    testStrBuffer = new StringBuffer();
    Assert.assertFalse("StringBuffer null or empty false", Util.isNotNullOrEmpty(testStrBuffer));

    testStrBuffer.append("  ");
    Assert.assertTrue("StringBuffer null or empty true", Util.isNotNullOrEmpty(testStrBuffer));

    testStrBuffer.append("Test String in buffer   ");
    Assert.assertTrue("StringBuffer null or empty true", Util.isNotNullOrEmpty(testStrBuffer));
  }
}
