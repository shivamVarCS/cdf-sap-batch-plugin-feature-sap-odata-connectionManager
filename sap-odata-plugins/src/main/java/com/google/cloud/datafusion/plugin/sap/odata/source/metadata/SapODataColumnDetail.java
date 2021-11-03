/*
 *  Copyright (c) 2021 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package com.google.cloud.datafusion.plugin.sap.odata.source.metadata;

import java.util.List;

/**
 *
 */
public class SapODataColumnDetail {
  private String name;
  private String type;
  private String collation;
  private String concurrencyModeName;
  private String defaultValue;
  private Integer maxLength;
  private Integer precision;
  private Integer scale;
  private Boolean isNullable;
  private Boolean isFixedLength;
  private Boolean isUnicode;
  private String kindName;
  private Integer multiplicityOrdinal;

  //SAP specific attributes
  private String displayFormat;
  private String filterRestrictions;
  private Boolean requiredInFilter;
  private String label;

  private List<SapODataColumnDetail> childs;


  public SapODataColumnDetail(String name, String type, String collation, String concurrencyModeName,
                              String defaultValue, Integer maxLength, Integer precision, Integer scale,
                              Boolean isNullable, Boolean isFixedLength, Boolean isUnicode, String kindName,
                              Integer multiplicityOrdinal, String displayFormat, String filterRestrictions,
                              Boolean requiredInFilter, String label,
                              List<SapODataColumnDetail> childs) {
    this.name = name;
    this.type = type;
    this.collation = collation;
    this.concurrencyModeName = concurrencyModeName;
    this.defaultValue = defaultValue;
    this.maxLength = maxLength;
    this.precision = precision;
    this.scale = scale;
    this.isNullable = isNullable;
    this.isFixedLength = isFixedLength;
    this.isUnicode = isUnicode;
    this.kindName = kindName;
    this.multiplicityOrdinal = multiplicityOrdinal;
    this.displayFormat = displayFormat;
    this.filterRestrictions = filterRestrictions;
    this.requiredInFilter = requiredInFilter;
    this.label = label;
    this.childs = childs;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getCollation() {
    return collation;
  }

  public String getConcurrencyModeName() {
    return concurrencyModeName;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public Integer getMaxLength() {
    return maxLength;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  public boolean isNullable() {
    //this ia s added as there are cases where SAP entity properties doesn't holds this 'nullable' attribute in
    // metadata
    return isNullable == null ? Boolean.TRUE : isNullable.booleanValue();
  }

  public boolean isFixedLength() {
    return Boolean.TRUE.equals(isFixedLength);
  }

  public boolean isUnicode() {
    return Boolean.TRUE.equals(isUnicode);
  }

  public String getKindName() {
    return kindName;
  }

  public Integer getMultiplicityOrdinal() {
    return multiplicityOrdinal;
  }

  public String getDisplayFormat() {
    return displayFormat;
  }

  public String getFilterRestrictions() {
    return filterRestrictions;
  }

  public Boolean getRequiredInFilter() {
    return Boolean.TRUE.equals(requiredInFilter);
  }

  public String getLabel() {
    return label;
  }

  public List<SapODataColumnDetail> getChilds() {
    return childs;
  }

  public void setChilds(List<SapODataColumnDetail> childs) {
    this.childs = childs;
  }

  public boolean containsChild() {
    return !(childs == null || childs.isEmpty());
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   *
   */
  public static class Builder {
    private String name;
    private String type;
    private String collation;
    private String concurrencyModeName;
    private String defaultValue;
    private Integer maxLength;
    private Integer precision;
    private Integer scale;
    private Boolean isNullable;
    private Boolean isFixedLength;
    private Boolean isUnicode;
    private String kindName;
    private Integer multiplicityOrdinal;
    private String displayFormat;
    private String filterRestrictions;
    private Boolean requiredInFilter;
    private String label;
    private List<SapODataColumnDetail> childs;

    Builder() {
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder collation(String collation) {
      this.collation = collation;
      return this;
    }

    public Builder concurrencyModeName(String concurrencyModeName) {
      this.concurrencyModeName = concurrencyModeName;
      return this;
    }

    public Builder defaultValue(String defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder maxLength(Integer maxLength) {
      this.maxLength = maxLength;
      return this;
    }

    public Builder precision(Integer precision) {
      this.precision = precision;
      return this;
    }

    public Builder scale(Integer scale) {
      this.scale = scale;
      return this;
    }

    public Builder isNullable(Boolean isNullable) {
      this.isNullable = isNullable;
      return this;
    }

    public Builder isFixedLength(Boolean isFixedLength) {
      this.isFixedLength = isFixedLength;
      return this;
    }

    public Builder isUnicode(Boolean isUnicode) {
      this.isUnicode = isUnicode;
      return this;
    }

    public Builder kindName(String kindName) {
      this.kindName = kindName;
      return this;
    }

    public Builder multiplicityOrdinal(Integer multiplicityOrdinal) {
      this.multiplicityOrdinal = multiplicityOrdinal;
      return this;
    }

    public Builder displayFormat(String displayFormat) {
      this.displayFormat = displayFormat;
      return this;
    }

    public Builder filterRestrictions(String filterRestrictions) {
      this.filterRestrictions = filterRestrictions;
      return this;
    }

    public Builder requiredInFilter(Boolean requiredInFilter) {
      this.requiredInFilter = requiredInFilter;
      return this;
    }

    public Builder label(String label) {
      this.label = label;
      return this;
    }

    public Builder childs(List<SapODataColumnDetail> childs) {
      this.childs = childs;
      return this;
    }

    public SapODataColumnDetail build() {
      return new SapODataColumnDetail(this.name, this.type, this.collation, this.concurrencyModeName, this.defaultValue,
        this.maxLength, this.precision, this.scale, this.isNullable, this.isFixedLength, this.isUnicode, this.kindName,
        this.multiplicityOrdinal, this.displayFormat, this.filterRestrictions, this.requiredInFilter, this.label,
        this.childs);
    }
  }
}
