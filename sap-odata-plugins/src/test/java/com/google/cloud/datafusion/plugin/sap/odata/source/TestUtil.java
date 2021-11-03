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

package com.google.cloud.datafusion.plugin.sap.odata.source;

import com.google.cloud.datafusion.plugin.sap.odata.source.exception.proto.ODataError;
import com.google.gson.Gson;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * This {@code TestUtil} contains supportive methods for testing.
 */
public class TestUtil {

  public static InputStream readResource(String resourceName) {
    return Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
  }

  @Nullable
  public static String convertInputStreamToString(InputStream responseStream) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining(""));
    } catch (IOException ioe) {
      //op-op
    }
    return null;
  }


  @Test
  public void testJson() {
    String str = "{" +
      "   \"error\":{" +
      "      \"code\":\"005056A509B11EE1B9A8FEC11C21D78E\"," +
      "      \"message\":{" +
      "         \"lang\":\"en\"," +
      "         \"value\":\"Resource not found for the segment 'Address2'.\"" +
      "      }," +
      "      \"innererror\":{" +
      "         \"transactionid\":\"C83CB3D2A1420000E00609D31E196BD4\"," +
      "         \"timestamp\":\"20210524082515.9921880\"," +
      "         \"Error_Resolution\":{" +
      "            \"SAP_Transaction\":\"For backend administrators: use ADT feed reader SAP Gateway Error " +
      "Log or run transaction /IWFND/ERROR_LOG on SAP Gateway hub system and search for entries with the timestamp " +
      "above for more details\"," +
      "            \"SAP_Note\":\"See SAP Note 1797736 for error analysis " +
      "(https://service.sap.com/sap/support/notes/1797736)\"" +
      "         }," +
      "\"application\": {\n" +
      "     \"component_id\": \"\",\n" +
      "      \"service_namespace\": \"/SAP/\",\n" +
      "      \"service_id\": \"ZPURCHASEORDER_SRV_X\",\n" +
      "      \"service_version\": \"0001\"\n" +
      "    }," +
      "         \"errordetails\":{" +
      "            \"errordetail\":[" +
      "               {" +
      "                  \"code\":\"/IWBEP/CM_TEA/002\"," +
      "                  \"message\":\"'TEAM_012345678' is not a valid ID.\"," +
      "                  \"propertyref\":\"Team/Team_Identifier\"," +
      "                  \"severity\":\"error\"" +
      "               }," +
      "               {" +
      "                  \"code\":\"/IWBEP/CM_TEA/004\"," +
      "                  \"message\":\"Team ID 'TEAM_012345678' is not in the defined range.\"," +
      "                  \"propertyref\":\"Team/Team_Identifier\"," +
      "                  \"severity\":\"error\"" +
      "               }," +
      "               {" +
      "                  \"code\":\"/IWBEP/CX_TEA_BUSINESS\"," +
      "                  \"message\":\"'TEAM_012345678' is not a valid ID.\"," +
      "                  \"propertyref\":\"\"," +
      "                  \"severity\":\"error\"" +
      "               }" +
      "            ]" +
      "         }" +
      "      }" +
      "   }" +
      "}";

    Gson gson = new Gson();
    ODataError oDataError = gson.fromJson(str, ODataError.class);

    System.out.println(oDataError);

    System.out.println(gson.toJson(oDataError));
  }

  @Test
  public void t1() {
    int precision = 15;
    int scale = 6;
    BigDecimal bd = new BigDecimal("23222.34");
    bd = bd.setScale(scale);
    System.out.println(bd.precision());
    System.out.println(bd.scale());
  }
}
