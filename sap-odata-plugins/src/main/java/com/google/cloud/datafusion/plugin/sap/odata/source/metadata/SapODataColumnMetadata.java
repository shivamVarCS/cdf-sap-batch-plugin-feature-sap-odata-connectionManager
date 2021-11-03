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

package com.google.cloud.datafusion.plugin.sap.odata.source.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This {@code SapODataColumnMetadata} contains all the available supported properties by any SAP OData service entity
 * property.
 */
public class SapODataColumnMetadata {

  private final String name;
  private final String type;
  private final String collation;
  private final String concurrencyModeName;
  private final String defaultValue;
  private final Integer maxLength;
  private final Integer precision;
  private final Integer scale;
  private final boolean isNullable;
  private final boolean isFixedLength;
  private final boolean isUnicode;
  private final String kindName;
  private final Integer multiplicityOrdinal;

  //SAP specific attributes
  private final String displayFormat;
  private final String filterRestrictions;
  private final Boolean requiredInFilter;
  private final String label;

  private boolean isChildListComplete = false;

  private List<SapODataColumnMetadata> childList;

  private SapODataColumnMetadata(String name,
                                 String type,
                                 @Nullable String collation,
                                 @Nullable String concurrencyModeName,
                                 @Nullable String defaultValue,
                                 @Nullable Integer maxLength,
                                 @Nullable Integer precision,
                                 @Nullable Integer scale,
                                 @Nullable Boolean isNullable,
                                 @Nullable Boolean isFixedLength,
                                 @Nullable Boolean isUnicode,
                                 @Nullable String kindName,
                                 @Nullable Integer multiplicityOrdinal,
                                 @Nullable String displayFormat,
                                 @Nullable String filterRestrictions,
                                 @Nullable Boolean requiredInFilter,
                                 @Nullable String label,
                                 @Nullable List<SapODataColumnMetadata> childList) {
    this.name = name;
    this.type = type;
    this.collation = collation;
    this.concurrencyModeName = concurrencyModeName;
    this.defaultValue = defaultValue;
    this.maxLength = maxLength;
    this.precision = precision;
    this.scale = scale;
    this.kindName = kindName;
    this.multiplicityOrdinal = multiplicityOrdinal;
    this.displayFormat = displayFormat;
    this.filterRestrictions = filterRestrictions;
    this.requiredInFilter = requiredInFilter;
    this.label = label;

    this.isNullable = isNullable == null || isNullable;
    this.isFixedLength = isFixedLength != null && isFixedLength;
    this.isUnicode = isUnicode != null && isUnicode;

    this.childList = childList == null ? Collections.emptyList() : childList;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Nullable
  public String getCollation() {
    return collation;
  }

  @Nullable
  public String getConcurrencyModeName() {
    return concurrencyModeName;
  }

  @Nullable
  public String getDefaultValue() {
    return defaultValue;
  }

  @Nullable
  public Integer getMaxLength() {
    return maxLength;
  }

  @Nullable
  public Integer getPrecision() {
    return precision;
  }

  @Nullable
  public Integer getScale() {
    return scale;
  }

  public boolean isNullable() {
    return isNullable;
  }

  public boolean isFixedLength() {
    return isFixedLength;
  }

  public boolean isUnicode() {
    return isUnicode;
  }

  @Nullable
  public String getKindName() {
    return kindName;
  }

  @Nullable
  public Integer getMultiplicityOrdinal() {
    return multiplicityOrdinal;
  }

  @Nullable
  public String getDisplayFormat() {
    return displayFormat;
  }

  @Nullable
  public String getFilterRestrictions() {
    return filterRestrictions;
  }

  @Nullable
  public Boolean getRequiredInFilter() {
    return Boolean.TRUE.equals(requiredInFilter);
  }

  @Nullable
  public String getLabel() {
    return label;
  }

  public List<SapODataColumnMetadata> getChildList() {
    return Collections.unmodifiableList(childList);
  }

  /**
   * add the incoming {@code SapODataColumnMetadata} as a new child in the existing child list
   *
   * @param child {@code SapODataColumnMetadata}
   */
  public void appendChild(SapODataColumnMetadata child) {
    if (!isChildListComplete) {
      if (child != null) {
        List<SapODataColumnMetadata> newChildList = new ArrayList<>(getChildList());
        newChildList.add(child);
        this.childList = newChildList;
      }
    } else {
      throw new IllegalStateException("No more children can be added for the current object, check if the " +
        "'finalizeChildren' is called before adding the children.");
    }
  }

  /**
   * returns a boolean flag about the existence of the child
   *
   * @return 'true' in case contains child otherwise 'false'.
   */
  public boolean containsChild() {
    return !(getChildList().isEmpty());
  }

  /**
   * marks the 'isChildListComplete' flag to 'true' to stop adding any further children in the current object.
   * This also finalizes all the nested children present in the current object.
   *
   * Note: this should be used only in case there is no further requirements to add any more children in the current
   * object.
   */
  public void finalizeChildren() {
    finalizeAllNestedChildren(this);
  }

  /**
   * marks the 'isChildListComplete' flag to 'true' and also do the same for all the nested children
   *
   * @param children
   */
  private static void finalizeAllNestedChildren(SapODataColumnMetadata children) {
    if (children.containsChild()) {
      children.getChildList().forEach(SapODataColumnMetadata::finalizeAllNestedChildren);
    }

    children.isChildListComplete = true;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Helper class to simplify {@link SapODataColumnMetadata} class creation.
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
    private List<SapODataColumnMetadata> childList;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder collation(@Nullable String collation) {
      this.collation = collation;
      return this;
    }

    public Builder concurrencyModeName(@Nullable String concurrencyModeName) {
      this.concurrencyModeName = concurrencyModeName;
      return this;
    }

    public Builder defaultValue(@Nullable String defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder maxLength(@Nullable Integer maxLength) {
      this.maxLength = maxLength;
      return this;
    }

    public Builder precision(@Nullable Integer precision) {
      this.precision = precision;
      return this;
    }

    public Builder scale(@Nullable Integer scale) {
      this.scale = scale;
      return this;
    }

    public Builder isNullable(@Nullable Boolean isNullable) {
      this.isNullable = isNullable;
      return this;
    }

    public Builder isFixedLength(@Nullable Boolean isFixedLength) {
      this.isFixedLength = isFixedLength;
      return this;
    }

    public Builder isUnicode(@Nullable Boolean isUnicode) {
      this.isUnicode = isUnicode;
      return this;
    }

    public Builder kindName(@Nullable String kindName) {
      this.kindName = kindName;
      return this;
    }

    public Builder multiplicityOrdinal(@Nullable Integer multiplicityOrdinal) {
      this.multiplicityOrdinal = multiplicityOrdinal;
      return this;
    }

    public Builder displayFormat(@Nullable String displayFormat) {
      this.displayFormat = displayFormat;
      return this;
    }

    public Builder filterRestrictions(@Nullable String filterRestrictions) {
      this.filterRestrictions = filterRestrictions;
      return this;
    }

    public Builder requiredInFilter(@Nullable Boolean requiredInFilter) {
      this.requiredInFilter = requiredInFilter;
      return this;
    }

    public Builder label(@Nullable String label) {
      this.label = label;
      return this;
    }

    public Builder childList(@Nullable List<SapODataColumnMetadata> childList) {
      this.childList = childList;
      return this;
    }

    public SapODataColumnMetadata build() {
      return new SapODataColumnMetadata(this.name, this.type, this.collation, this.concurrencyModeName,
        this.defaultValue, this.maxLength, this.precision, this.scale, this.isNullable, this.isFixedLength,
        this.isUnicode, this.kindName, this.multiplicityOrdinal, this.displayFormat, this.filterRestrictions,
        this.requiredInFilter, this.label, this.childList);
    }
  }
}
