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

package com.google.cloud.datafusion.plugin.sap.odp.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility methods to shorten a string via Base62 encoding by converting MD5
 * hash number of the long string to ASCII strings (0-9, a-z and A-Z) and vice
 * resulting in comparatively short strings.
 * 
 * @author sankalpbapat
 */
public class Base62Encoder {
  private static final char[] ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

  private static final int BASE = ALPHABET.length;

  private Base62Encoder() {
  }

  /**
   * Generates the encoded value from unique integer MD5 hash value
   * 
   * @param longStr String that needs to be shortened
   * @return the unique encoded string.
   */
  public static String getEncodedShortString(String longStr) {
    BigInteger md5Hash = getUniqueInteger(longStr);
    final StringBuilder sb = new StringBuilder(1);
    do {
      BigInteger[] result = md5Hash.divideAndRemainder(BigInteger.valueOf(BASE));
      sb.append(ALPHABET[result[1].intValue()]);
      md5Hash = result[0];
    } while (md5Hash.signum() > 0);

    return sb.reverse().toString();
  }

  /**
   * Generates the unique integer MD5 hash value
   *
   * @param longStr string to be parsed
   * @return the unique integer MD5 hash value.
   */
  private static BigInteger getUniqueInteger(String longStr) {
    int hash = longStr.hashCode();
    BigInteger hashBigInt;
    if (hash < 0) {
      hashBigInt = BigInteger.valueOf(Integer.MAX_VALUE);
      int diff = hash - Integer.MIN_VALUE + 1;
      hashBigInt = hashBigInt.add(new BigInteger(String.valueOf(diff)));
    } else {
      hashBigInt = new BigInteger(String.valueOf(hash));
    }

    try {
      MessageDigest m = MessageDigest.getInstance("MD5");
      m.reset();
      m.update(longStr.getBytes());
      byte[] digest = m.digest();
      BigInteger bigInt = new BigInteger(1, digest);
      String hashtext = bigInt.toString(10);
      long temp = 0;
      for (int i = 0; i < hashtext.length(); i++) {
        char c = hashtext.charAt(i);
        temp += c;
      }

      return hashBigInt.add(BigInteger.valueOf(temp));
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return hashBigInt;
  }
}
