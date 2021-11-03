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

package com.google.cloud.datafusion.plugin.sap.connection;

import com.google.cloud.datafusion.plugin.util.Util;
import com.sap.conn.jco.ext.DestinationDataProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Collection of SAP connection parameters specific to every connection.
 * 
 * @author sankalpbapat
 *
 */
public class SapDefinition {
  public static final char COLON = ':';

  public static final String CONN_REPO_NAME = "sap.conn.repo.name";

  /**
   * <ul>
   * <li><b>(\S+)</b> matches at least one and up to unlimited consecutive
   * non-whitespace characters.
   * <li><b>(,|$)</b> checks for a literal , (comma) or end of line.
   * </ul>
   */
  public static final String PATTERN_JCO_PASWD = "(jco\\.client\\.passwd=)(?:\\S+)(,|$)";
  public static final String REPLACEMENT_JCO_PASWD = "<secret>";

  private final Properties defProps;

  private SapDefinition(Map<String, String> defMap) {
    defProps = new Properties();
    defProps.putAll(defMap);
  }

  /**
   * Builder to create {@link SapDefinition}
   * 
   * @param defMap Map of JCo properties
   * @return Builder instance
   */
  public static Builder builder(Map<String, String> defMap) {
    return new Builder(defMap);
  }

  /**
   * Creates a destination identification key
   *
   * @param isServerProvider
   * @return the destination or server key based on {@code isServerProvider}
   */
  public String getProviderKey(boolean isServerProvider) {
    StringBuilder key = new StringBuilder(60);
    /*
     * JCo by default only uses GWHost, GWServ, ProgId for Inbound Server key, so
     * adding other params, fails check for existing server and JCo tries to create
     * new server with same credentials, throwing an exception
     */
    if (!isServerProvider) {
      if (Util.isNotNullOrEmpty(this.getDestJcoMshost())) {
        key.append(this.getDestJcoMshost()).append(COLON).append(this.getDestJcoMsserv()).append(COLON)
          .append(this.getDestJcoGroup()).append(COLON).append(this.getDestJcoR3name());
      } else {
        key.append(this.getDestJcoAshost()).append(COLON).append(this.getDestJcoSysnr());
      }
      key.append(COLON).append(this.getDestJcoClient());

      if (Util.isNotNullOrEmpty(this.getDestJcoSncPartnername())) {
        key.append(COLON).append(this.getDestJcoSncPartnername());
      } else {
        key.append(COLON).append(this.getDestJcoUser());
      }
    }

    return key.toString();
  }

  /**
   * Get all connection parameters as {@code Properties}
   * @return
   */
  public Properties getProperties() {
    Properties jcoDefinition = new Properties();
    jcoDefinition.putAll(defProps);
    return jcoDefinition;
  }

  public String getRepositoryName() {
    return defProps.getProperty(CONN_REPO_NAME);
  }

  public String getDestJcoClient() {
    return (String) defProps.get(DestinationDataProvider.JCO_CLIENT);
  }

  public String getDestJcoLang() {
    return (String) defProps.get(DestinationDataProvider.JCO_LANG);
  }

  public String getDestJcoSaprouter() {
    return (String) defProps.get(DestinationDataProvider.JCO_SAPROUTER);
  }

  public String getDestJcoAshost() {
    return (String) defProps.get(DestinationDataProvider.JCO_ASHOST);
  }

  public String getDestJcoSysnr() {
    return (String) defProps.get(DestinationDataProvider.JCO_SYSNR);
  }

  public String getDestJcoMshost() {
    return (String) defProps.get(DestinationDataProvider.JCO_MSHOST);
  }

  public String getDestJcoMsserv() {
    return (String) defProps.get(DestinationDataProvider.JCO_MSSERV);
  }

  public String getDestJcoGroup() {
    return (String) defProps.get(DestinationDataProvider.JCO_GROUP);
  }

  public String getDestJcoR3name() {
    return (String) defProps.get(DestinationDataProvider.JCO_R3NAME);
  }

  public String getDestJcoUser() {
    return (String) defProps.get(DestinationDataProvider.JCO_USER);
  }

  public String getDestJcoPasswd() {
    return (String) defProps.get(DestinationDataProvider.JCO_PASSWD);
  }

  public String getDestJcoSncMode() {
    return (String) defProps.get(DestinationDataProvider.JCO_SNC_MODE);
  }

  public String getDestJcoSncPartnername() {
    return (String) defProps.get(DestinationDataProvider.JCO_SNC_PARTNERNAME);
  }

  public String getDestJcoSncLibrary() {
    return (String) defProps.get(DestinationDataProvider.JCO_SNC_LIBRARY);
  }

  public String getDestJcoSncQop() {
    return (String) defProps.get(DestinationDataProvider.JCO_SNC_QOP);
  }

  public String getDestJcoSncMyname() {
    return (String) defProps.get(DestinationDataProvider.JCO_SNC_MYNAME);
  }

  public String getDestJcoPoolCapacity() {
    return (String) defProps.get(DestinationDataProvider.JCO_POOL_CAPACITY);
  }

  public String getDestJcoPeakLimit() {
    return (String) defProps.get(DestinationDataProvider.JCO_PEAK_LIMIT);
  }

  public String getDestJcoExpirationTime() {
    return (String) defProps.get(DestinationDataProvider.JCO_EXPIRATION_TIME);
  }

  public String getDestJcoExpirationPeriod() {
    return (String) defProps.get(DestinationDataProvider.JCO_EXPIRATION_PERIOD);
  }

  public String getDestJcoMaxGetTime() {
    return (String) defProps.get(DestinationDataProvider.JCO_MAX_GET_TIME);
  }

  public String getDestJcoCpicTrace() {
    return (String) defProps.get(DestinationDataProvider.JCO_CPIC_TRACE);
  }

  public String getDestJcoTrace() {
    return (String) defProps.get(DestinationDataProvider.JCO_TRACE);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public synchronized String toString() {
    return this.defProps.toString().replaceAll(PATTERN_JCO_PASWD, "$1" + REPLACEMENT_JCO_PASWD + "$2");
  }


  /**
   * Helps to build an immutable {@code SapDefinition}
   */
  public static class Builder {
    private final Map<String, String> properties;

    private Builder(Map<String, String> properties) {
      this.properties = new HashMap<>();
      this.properties.putAll(properties);
    }

    public void putAll(Map<String, String> map) {
      properties.putAll(map);
    }

    public SapDefinition build() {
      return new SapDefinition(properties);
    }

    public Builder setRepositoryName(String name) {
      properties.put(CONN_REPO_NAME, name);
      return this;
    }

    public Builder setDestJcoClient(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_CLIENT, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_CLIENT);
      }
      return this;
    }

    public Builder setDestJcoLang(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_LANG, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_LANG);
      }
      return this;
    }

    public Builder setDestJcoSaprouter(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SAPROUTER, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SAPROUTER);
      }
      return this;
    }

    public Builder setDestJcoAshost(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_ASHOST, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_ASHOST);
      }
      return this;
    }

    public Builder setDestJcoSysnr(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SYSNR, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SYSNR);
      }
      return this;
    }

    public Builder setDestJcoMshost(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_MSHOST, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_MSHOST);
      }
      return this;
    }

    public Builder setDestJcoMsserv(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_MSSERV, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_MSSERV);
      }
      return this;
    }

    public Builder setDestJcoGroup(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_GROUP, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_GROUP);
      }
      return this;
    }

    public Builder setDestJcoR3name(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_R3NAME, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_R3NAME);
      }
      return this;
    }

    public Builder setDestJcoUser(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_USER, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_USER);
      }
      return this;
    }

    public Builder setDestJcoPasswd(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_PASSWD, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_PASSWD);
      }
      return this;
    }

    public Builder setDestJcoSncMode(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SNC_MODE, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SNC_MODE);
      }
      return this;
    }

    public Builder setDestJcoSncPartnername(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SNC_PARTNERNAME, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SNC_PARTNERNAME);
      }
      return this;
    }

    public Builder setDestJcoSncLibrary(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SNC_LIBRARY, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SNC_LIBRARY);
      }
      return this;
    }

    public Builder setDestJcoSncQop(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SNC_QOP, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SNC_QOP);
      }
      return this;
    }

    public Builder setDestJcoSncMyname(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_SNC_MYNAME, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_SNC_MYNAME);
      }
      return this;
    }

    public Builder setDestJcoPoolCapacity(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_POOL_CAPACITY, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_POOL_CAPACITY);
      }
      return this;
    }

    public Builder setDestJcoPeakLimit(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_PEAK_LIMIT, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_PEAK_LIMIT);
      }
      return this;
    }

    public Builder setDestJcoExpirationTime(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_EXPIRATION_TIME, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_EXPIRATION_TIME);
      }
      return this;
    }

    public Builder setDestJcoExpirationPeriod(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_EXPIRATION_PERIOD, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_EXPIRATION_PERIOD);
      }
      return this;
    }

    public Builder setDestJcoMaxGetTime(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_MAX_GET_TIME, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_MAX_GET_TIME);
      }
      return this;
    }

    public Builder setDestJcoCpicTrace(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_CPIC_TRACE, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_CPIC_TRACE);
      }
      return this;
    }

    public Builder setDestJcoTrace(String val) {
      if (val != null && !val.isEmpty()) {
        properties.put(DestinationDataProvider.JCO_TRACE, val);
      } else {
        properties.remove(DestinationDataProvider.JCO_TRACE);
      }
      return this;
    }
  }
}
