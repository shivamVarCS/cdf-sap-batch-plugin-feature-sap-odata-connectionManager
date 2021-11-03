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

package com.google.cloud.datafusion.plugin.sap.odata.source.transport;

import java.io.InputStream;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * This {@code SapODataResponseContainer} container class is used to contains request body of type {@code InputStream},
 * along with the following:
 * - HTTP STATUS CODE,
 * - HTTP STATUS MESSAGE &
 * - SAP OData service version number
 */

public class SapODataResponseContainer {

  private final int httpStatusCode;
  private final String httpStatusMsg;

  @Nullable
  private final String dataServiceVersion;
  private final Supplier<InputStream> responseStream;

  public SapODataResponseContainer(int httpStatusCode, String httpStatusMsg,
                                   @Nullable String dataServiceVersion,
                                   Supplier<InputStream> responseStream) {

    this.httpStatusCode = httpStatusCode;
    this.httpStatusMsg = httpStatusMsg;
    this.dataServiceVersion = dataServiceVersion;
    this.responseStream = responseStream;
  }

  public int getHttpStatusCode() {
    return this.httpStatusCode;
  }

  public String getHttpStatusMsg() {
    return this.httpStatusMsg;
  }

  @Nullable
  public String getDataServiceVersion() {
    return this.dataServiceVersion;
  }

  @Nullable
  public InputStream getResponseStream() {
    return responseStream.get();
  }

  public static SapODataResponseContainer.Builder builder() {
    return new Builder();
  }

  /**
   * Helper class to simplify {@link SapODataResponseContainer} class creation.
   */
  public static class Builder {
    private int httpStatusCode;
    private String httpStatusMsg;
    @Nullable
    private String dataServiceVersion;
    private Supplier<InputStream> responseStream;

    public Builder httpStatusCode(int httpStatusCode) {
      this.httpStatusCode = httpStatusCode;
      return this;
    }

    public Builder httpStatusMsg(String httpStatusMsg) {
      this.httpStatusMsg = httpStatusMsg;
      return this;
    }

    public Builder dataServiceVersion(@Nullable String dataServiceVersion) {
      this.dataServiceVersion = dataServiceVersion;
      return this;
    }

    public Builder responseStream(Supplier<InputStream> responseStream) {
      this.responseStream = responseStream;
      return this;
    }

    public SapODataResponseContainer build() {
      return new SapODataResponseContainer(this.httpStatusCode, this.httpStatusMsg, this.dataServiceVersion,
        this.responseStream);
    }
  }
}
