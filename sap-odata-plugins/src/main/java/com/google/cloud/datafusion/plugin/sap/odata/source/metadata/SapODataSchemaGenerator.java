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

import com.google.cloud.datafusion.plugin.sap.odata.source.exception.ODataServiceException;
import com.google.cloud.datafusion.plugin.util.ResourceConstants;
import com.google.cloud.datafusion.plugin.util.Util;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmFacets;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmTypeKind;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.core.edm.provider.EdmNavigationPropertyImplProv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * This {@code SapODataSchemaGenerator} contains all the logic to generate the different set of schemas.
 * e.g.
 * - schema with default (non-navigation) properties
 * - schema with default and given expanded navigation properties
 * - schema with given selective properties
 *
 * Note:
 *  - Default Property: A statically declared Property on an Entity. The value of a default property is a primitive or
 *                      complex type.
 *  - Navigation Property: A property of an Entry that represents a Link from the Entry to one or more related Entries.
 *    e.g.:
 * Sample OData metadata:
 * <?xml version="1.0" encoding="UTF-8"?>
 *   <edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
 *     <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
 *              m:DataServiceVersion="2.0">
 *       <Schema xmlns="http://schemas.microsoft.com/ado/2007/05/edm"
 *              xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" Namespace="ODataDemo">
 *         <EntityType Name="Product">
 *           <Key>
 *             <PropertyRef Name="ID" />
 *           </Key>
 *           <Property Name="ID" Type="Edm.Int32" Nullable="false" />
 *             <Property Name="Name" Type="Edm.String" Nullable="true"/>
 *             <Property Name="Price" Type="Edm.Decimal" Nullable="false" />
 *             <NavigationProperty Name="Category" Relationship="ODataDemo.Product_Category_Category_Products"
 *                    FromRole="Product_Category" ToRole="Category_Products" />
 *         </EntityType>
 *         <EntityType Name="Category">
 *           <Key>
 *             <PropertyRef Name="ID" />
 *           </Key>
 *           <Property Name="ID" Type="Edm.Int32" Nullable="false" />
 *           <Property Name="Name" Type="Edm.String" Nullable="true"/>
 *           <NavigationProperty Name="Products" Relationship="ODataDemo.Product_Category_Category_Products"
 *                FromRole="Category_Products" ToRole="Product_Category" />
 *         </EntityType>
 *         <Association Name="Product_Category_Category_Products">
 *           <End Role="Product_Category" Type="ODataDemo.Product" Multiplicity="*" />
 *           <End Role="Category_Products" Type="ODataDemo.Category" Multiplicity="0..1" />
 *         </Association>
 *         <EntityContainer Name="DemoService" m:IsDefaultEntityContainer="true">
 *          <EntitySet Name="Products" EntityType="ODataDemo.Product" />
 *          <EntitySet Name="Categories" EntityType="ODataDemo.Category" />
 *          <AssociationSet Name="Products_Category_Categories"
 *              Association="ODataDemo.Product_Category_Category_Products">
 *            <End Role="Product_Category" EntitySet="Products" />
 *            <End Role="Category_Products" EntitySet="Categories" />
 *          </AssociationSet>
 *         </EntityContainer>
 *       </Schema>
 *     </edmx:DataServices>
 *   </edmx:Edmx>
 *
 *   Here, "Category" is an independent Entity however in the above example "Category" is referenced inside the
 *   "Product" entity and contains a "Relationship" attribute which points to "Category" entity via "AssociationSet",
 *   this led to term the "Category" as "navigation property" inside the "Products" entity.
 *
 *   Sample OData data:
 *    {
 *      "ID":0,
 *      "Name":"Bread",
 *      "Price":"2.5",
 *      "Category":{
 *        "ID":0,
 *        "Name":"Food"
 *      }
 *    }
 */
public class SapODataSchemaGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataSchemaGenerator.class);

  // Mapping of SAP OData type as key and its corresponding Schema type as value
  private static final Map<String, Schema> SCHEMA_TYPE_MAPPING;

  static {
    Map<String, Schema> dataTypeMap = new HashMap<>();
    dataTypeMap.put("SByte", Schema.of(Schema.Type.INT));
    dataTypeMap.put("Byte", Schema.of(Schema.Type.INT));
    dataTypeMap.put("Int16", Schema.of(Schema.Type.INT));
    dataTypeMap.put("Int32", Schema.of(Schema.Type.INT));
    dataTypeMap.put("Int64", Schema.of(Schema.Type.LONG));
    dataTypeMap.put("Single", Schema.of(Schema.Type.FLOAT));
    dataTypeMap.put("Double", Schema.of(Schema.Type.DOUBLE));
    // precision and scale are dummy here actual values are set while creating the 'Schema.Field'
    // from the 'SapODataColumnMetadata'
    dataTypeMap.put("Decimal", Schema.decimalOf(2, 1));

    dataTypeMap.put("Guid", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("String", Schema.of(Schema.Type.STRING));

    dataTypeMap.put("Binary", Schema.of(Schema.Type.BYTES));

    dataTypeMap.put("Boolean", Schema.of(Schema.Type.BOOLEAN));

    dataTypeMap.put("DateTime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    dataTypeMap.put("Time", Schema.of(Schema.LogicalType.TIME_MICROS));
    dataTypeMap.put("DateTimeOffset", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));

    SCHEMA_TYPE_MAPPING = Collections.unmodifiableMap(dataTypeMap);
  }

  private static final String DEFAULT_PROPERTY = "Default property";

  private static final String NAV_PROPERTY_SEPARATOR = "/";
  private static final String PROPERTY_SEPARATOR = ",";

  private final SapODataEntityProvider oDataServiceHelper;

  public SapODataSchemaGenerator(SapODataEntityProvider oDataServiceHelper) {
    this.oDataServiceHelper = oDataServiceHelper;
  }

  /**
   * Build schema with all the default (non-navigation) properties for the given entity name.
   *
   * @param entityName   service entity name
   * @return {@code Schema}
   * @throws ODataServiceException throws in following two cases
   *        1. if no default property were found in the given entity name,
   *        2. if fails at apache olingo processing.
   */
  public Schema buildDefaultOutputSchema(String entityName) throws ODataServiceException {

    try {
      List<SapODataColumnMetadata> columnDetailList = buildDefaultColumns(entityName);
      if (columnDetailList.isEmpty()) {
        throw new ODataServiceException(
          ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(DEFAULT_PROPERTY, entityName));
      }

      freezeSapColumnMetadata(columnDetailList);

      return buildSchema(columnDetailList);
    } catch (EdmException ee) {
      throw new ODataServiceException(
        ResourceConstants.ERR_BUILDING_COLUMNS.getMsgForKey(DEFAULT_PROPERTY, entityName), ee);
    }
  }

  /**
   * Builds all the non-navigational property for the given entity name.
   *
   * @param entityName service entity name
   * @return list of {@code SapODataColumnMetadata} or empty list in case of invalid entity name.
   * @throws EdmException any apache olingo processing exception
   */
  private List<SapODataColumnMetadata> buildDefaultColumns(String entityName) throws EdmException {

    EdmEntityType entityType = oDataServiceHelper.getEntityType(entityName);
    List<String> propList = oDataServiceHelper.getEntityPropertyList(entityType);

    if (propList != null) {
      return buildSapODataColumns(entityType, propList);
    }

    LOGGER.debug(ResourceConstants.DEBUG_ENTITY_NOT_FOUND.getMsgForKey(entityName));
    return Collections.emptyList();
  }

  /**
   * Prepares list of {@code SapODataColumnMetadata} for both navigation as well as non-navigation property from
   * the provided 'propList'.
   *
   * @param entityType service entity type
   * @param propList   list of required property name for the given entity type.
   * @return  list of {@code SapODataColumnMetadata} or empty list in case of invalid entity name.
   * @throws EdmException any apache olingo processing exception
   */
  private List<SapODataColumnMetadata> buildSapODataColumns(EdmEntityType entityType, List<String> propList)
    throws EdmException {

    List<SapODataColumnMetadata> oDataColumnDetailList = new ArrayList<>();
    for (String prop : propList) {
      String namespace = entityType.getNamespace();
      EdmTyped type = entityType.getProperty(prop);

      // There are many implementation of the 'EdmTyped' such as EdmProperty, EdmNavigationPropertyImplProv,
      // EdmComplexType, EdmEntityType, EdmParameterImplProv, EdmElementImplProv and so on however to
      // generate the plugin nested structure schema only three types are required EdmProperty,
      // EdmNavigationPropertyImplProv & EdmComplexType (used in other 'buildComplexTypes' method)

      // check for non-navigation property
      if (type instanceof EdmProperty) {
        EdmProperty edmProperty = (EdmProperty) type;
        oDataColumnDetailList.add(buildSapODataColumnMetadata(namespace, edmProperty));

      }

      // check for navigation property
      if (type instanceof EdmNavigationPropertyImplProv) {

        EdmNavigationPropertyImplProv navProperty = (EdmNavigationPropertyImplProv) type;
        EdmEntityType navEntityType = oDataServiceHelper.extractEntitySetFromNavigationProperty(navProperty);
        if (navEntityType != null) {

          List<SapODataColumnMetadata> navChild = buildSapODataColumns(navEntityType, navEntityType.getPropertyNames());

          SapODataColumnMetadata navigationColumn = SapODataColumnMetadata.builder()
            .name(prop)
            .type(navProperty.getType().getName())
            .multiplicityOrdinal(navProperty.getMultiplicity().ordinal())
            .childList(navChild)
            .build();

          oDataColumnDetailList.add(navigationColumn);
        }
      }
    }

    return oDataColumnDetailList;
  }

  /**
   * Builds the {@code SapODataColumnMetadata} from the given {@code EdmProperty}.
   * Also builds the COMPLEX property.
   *
   * @param namespace   SAP OData service entity namespace. Used to build the COMPLEX properties.
   * @param edmProperty {@code EdmProperty} of the SAP OData service entity.
   * @return {@code SapODataColumnMetadata}
   * @throws EdmException any apache olingo processing exception
   */
  private SapODataColumnMetadata buildSapODataColumnMetadata(String namespace, EdmProperty edmProperty)
    throws EdmException {

    SapODataColumnMetadata.Builder sapODataColumnDetailBuilder = SapODataColumnMetadata.builder()
      .name(edmProperty.getName())
      .kindName(edmProperty.getType().getKind().name())
      .type(edmProperty.getType().getName())
      .multiplicityOrdinal(edmProperty.getMultiplicity().ordinal());

    if (edmProperty.getFacets() != null) {
      EdmFacets facets = edmProperty.getFacets();
      sapODataColumnDetailBuilder
        .collation(facets.getCollation())
        .defaultValue(facets.getDefaultValue())
        .maxLength(facets.getMaxLength())
        .precision(facets.getPrecision())
        .scale(facets.getScale())
        .isFixedLength(facets.isFixedLength())
        .isUnicode(facets.isUnicode())
        .isNullable(facets.isNullable());
    }

    //setting SAP specific details.
    List<EdmAnnotationAttribute> edmAnnotationAttribute = edmProperty.getAnnotations().getAnnotationAttributes();
    if (edmAnnotationAttribute != null) {
      edmAnnotationAttribute.forEach(sapAttribute -> {
        switch (sapAttribute.getName()) {
          case "display-format":
            sapODataColumnDetailBuilder.displayFormat(sapAttribute.getText());
            break;
          case "filter-restriction":
            sapODataColumnDetailBuilder.filterRestrictions(sapAttribute.getText());
            break;
          case "required-in-filter":
            sapODataColumnDetailBuilder.requiredInFilter(Boolean.parseBoolean(sapAttribute.getText()));
            break;
          case "label":
            sapODataColumnDetailBuilder.label(sapAttribute.getText());
            break;

          default: //no-ops
        }
      });
    }

    if (!edmProperty.isSimple()) {
      List<SapODataColumnMetadata> complexChild = buildComplexTypes(namespace, edmProperty);
      if (!complexChild.isEmpty()) {
        sapODataColumnDetailBuilder.childList(complexChild);
      }
    }

    return sapODataColumnDetailBuilder.build();
  }


  /**
   * Build schema with the all the default (non-navigation) properties for given entity name along with
   * all the navigation property provided in the 'expandOption'.
   *
   * @param entityName   service entity name
   * @param expandOption all the selective expanded property names
   * @return {@code Schema}
   * @throws ODataServiceException throws in following two cases
   *        1. if neither default nor expanded property were found in the given entity name,
   *        2. if fails at apache olingo processing.
   */
  public Schema buildExpandOutputSchema(String entityName, String expandOption) throws ODataServiceException {
    try {
      List<SapODataColumnMetadata> columnDetailList = buildDefaultColumns(entityName);
      if (columnDetailList.isEmpty()) {
        throw new ODataServiceException(ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(DEFAULT_PROPERTY,
          entityName));
      }

      List<SapODataColumnMetadata> expandColumnDetailList = buildExpandedEntity(entityName, expandOption);
      if (expandColumnDetailList.isEmpty()) {
        throw new ODataServiceException(ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(expandOption, entityName));
      }

      columnDetailList.addAll(expandColumnDetailList);

      freezeSapColumnMetadata(columnDetailList);

      return buildSchema(columnDetailList);
    } catch (EdmException ee) {
      throw new ODataServiceException(
        ResourceConstants.ERR_BUILDING_COLUMNS.getMsgForKey(expandOption, entityName), ee);
    }
  }

  /**
   * Finds and builds all the child under the given expanded navigation path.
   * Example:
   * Let say the metadata is as follows:
   *  Root
   *    - C1
   *    - C2
   *    - N1
   *      - N1C1
   *      - N1C2
   *        - NN1C1
   *        - NN1Root -- navigation attribute to Root
   *    - N2
   *      - N2C1
   *      - N2C2
   *
   * Let say user provided following expanded path as an input: N1, N2, N1/N1C2, N1/N1C2/Root
   * So in this case the final output should be
   *
   *  - N1
   *    - N1C1
   *    - N1C2
   *      - NN1C1
   *      - Root
   *        - C1
   *        - C2
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * Iteration wise output
   * ---------------------
   * Iteration: 1
   * input: N1
   * output:
   *
   *  - N1
   *    - N1C1
   *
   * Iteration: 2
   * input: N2
   * output:
   *
   *  - N1
   *    - N1C1
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * Iteration: 3
   * input: N1/N1C2
   * output:
   *
   *  - N1
   *    - N1C1
   *    - N1C2
   *      - NN1C1
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * Iteration: 4
   * input: N1/N1C2/Root
   * output:
   *
   *  - N1
   *    - N1C1
   *    - N1C2
   *      - NN1C1
   *      - Root
   *        - C1
   *        - C2
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * will return the list of all the navigation attributes containing it's relevant child i.e. [N1, N2].
   *
   * @param entityName  service entity name
   * @param expandEntry all the selective expanded property names
   * @return list of {@code SapODataColumnMetadata} or empty list in case of invalid expanded property name.
   * @throws EdmException any apache olingo processing exception.
   */
  private List<SapODataColumnMetadata> buildExpandedEntity(String entityName, String expandEntry)
    throws EdmException {

    // create a root metadata container which will hold all provided nested navigation property details as child
    SapODataColumnMetadata root = SapODataColumnMetadata.builder().build();

    // breaks the comma separated expanded path and forms a array
    // e.g. "supplier,supplier/products/category" --> ["supplier","supplier/products/category"]
    String[] expandedPathList = expandEntry.split(PROPERTY_SEPARATOR);

    // traverse each expanded navigation path
    for (String expandPath : expandedPathList) {
      SapODataColumnMetadata parent = root;
      SapODataColumnMetadata current;

      // used to store the navigation property sequence path in progressive way
      List<String> childSequence = new ArrayList<>();

      // breaks the '/' separated expanded navigation path and form an array for build the navigation sequence
      // e.g. "supplier/products/category" --> ["supplier","products","category"]
      String[] expandSequenceList = expandPath.split(NAV_PROPERTY_SEPARATOR);

      // traverse each navigation property in sequence
      for (String expandSequence : expandSequenceList) {

        // adding each navigation property in the list to prepare the sequence path
        childSequence.add(expandSequence);

        // childSequence is joined back to form the interim navigation path, which will be used to build the
        // navigation property by "buildNavigationColumns" method
        String childPath = String.join(NAV_PROPERTY_SEPARATOR, childSequence);

        // checking if the parent contains the child with name (expandSequence)
        current = parent.getChildList()
          .stream()
          .filter(s -> Util.isNotNullOrEmpty(s.getName()) && s.getName().equals(expandSequence))
          .findFirst()
          .orElse(null);

        // if 'current' is not null then parent contains the required navigation property so no need to create that
        // property just assign the 'current' to 'parent' to check the existence of the next navigation property
        // present in the sequence
        // otherwise create the property and assign the 'new property' to 'parent' to check the existence of the
        // next navigation property present in the sequence
        // Note: as per the example above, for the 1st iteration in 'expandedPathList' it will be null as the 'supplier'
        // is not yet created but in the
        // 2nd iteration in 'expandPathList' as 'supplier' already exists so no need to create it again and it will
        // be used to create the next navigation property in the path i.e. "products" and so on.
        if (current != null) {
          parent = current;
        } else {
          SapODataColumnMetadata child = buildNavigationColumns(entityName, childPath);
          // appending child
          if (child != null) {
            parent.appendChild(child);
            parent = child;
          }
        }
      }
    }
    return root.getChildList();
  }

  /**
   * Builds the {@code SapODataColumnMetadata} with all the default properties of the last expanded property
   * present under the given navigation path.
   *
   * @param entityName service entity name
   * @param navPath    can have navigation path or navigation property name
   * @return {@code SapODataColumnMetadata} or null in case of invalid expanded property path.
   * @throws EdmException any apache olingo processing exception.
   */
  @Nullable
  private SapODataColumnMetadata buildNavigationColumns(String entityName, String navPath) throws EdmException {
    EdmNavigationPropertyImplProv association = oDataServiceHelper.getNavigationProperty(entityName, navPath);
    if (association == null) {
      LOGGER.debug(ResourceConstants.DEBUG_NAVIGATION_NOT_FOUND.getMsgForKey(navPath, entityName));
      return null;
    }

    EdmEntityType entitySet = oDataServiceHelper.extractEntitySetFromNavigationProperty(association);
    if (entitySet == null) {
      LOGGER.debug(ResourceConstants.DEBUG_ENTITY_NOT_FOUND.getMsgForKey(association.getName(), navPath));
      return null;
    }

    List<String> propList = entitySet.getPropertyNames();
    List<SapODataColumnMetadata> columns = buildSapODataColumns(entitySet, propList);

    return SapODataColumnMetadata.builder()
      .name(association.getName())
      .type(entitySet.getKind().name())
      .multiplicityOrdinal(association.getMultiplicity().ordinal())
      .childList(columns)
      .build();
  }


  /**
   * Build schema for all the given selective property under the 'selectOption'.
   *
   * @param entityName   service entity name
   * @param selectOption all the selective property names
   * @return {@code Schema}
   * @throws ODataServiceException throws in following two cases
   *        1. if no selective property were found in the given entity name,
   *        2. if fails at apache olingo processing.
   */
  public Schema buildSelectOutputSchema(String entityName, String selectOption) throws ODataServiceException {
    try {
      List<SapODataColumnMetadata> columnDetailList = buildSelectedColumns(entityName, selectOption);
      if (columnDetailList.isEmpty()) {
        throw new ODataServiceException(ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(selectOption, entityName));
      }

      freezeSapColumnMetadata(columnDetailList);

      return buildSchema(columnDetailList);
    } catch (EdmException ee) {
      throw new ODataServiceException(
        ResourceConstants.ERR_BUILDING_COLUMNS.getMsgForKey(selectOption, entityName), ee);
    }
  }

  /**
   * Finds and builds all the selective (navigation & non-navigation) property provided under 'selectEntity'
   *
   * @param entityName  service entity name
   * @param selectEntry selective property name
   * @return list of {@code SapODataColumnMetadata} or empty list in case of invalid selective property name.
   * @throws EdmException any apache olingo processing exception.
   */
  private List<SapODataColumnMetadata> buildSelectedColumns(String entityName, String selectEntry)
    throws EdmException {

    //collect non-navigation properties from the $select option
    List<String> selectList = Arrays.stream(selectEntry.split(PROPERTY_SEPARATOR))
      .filter(s -> !s.contains(NAV_PROPERTY_SEPARATOR))
      .collect(Collectors.toList());

    EdmEntityType entityType = oDataServiceHelper.getEntityType(entityName);
    if (entityType == null) {
      return Collections.emptyList();
    }
    List<SapODataColumnMetadata> columnList = buildSapODataColumns(entityType, selectList);

    if (columnList.isEmpty()) {
      LOGGER.debug(ResourceConstants.DEBUG_NOT_FOUND.getMsgForKey(selectList));
      return Collections.emptyList();
    }

    //collect navigation properties from the $select option
    List<String> expandSelectList = Arrays.stream(selectEntry.split(PROPERTY_SEPARATOR))
      .filter(s -> s.contains(NAV_PROPERTY_SEPARATOR))
      .collect(Collectors.toList());

    if (!expandSelectList.isEmpty()) {
      SapODataColumnMetadata expandColumns = buildSelectedNavigationColumns(entityName, expandSelectList);
      if (expandColumns.containsChild()) {
        columnList.addAll(expandColumns.getChildList());
      }
    }
    return columnList;
  }

  /**
   * Builds {@code SapODataColumnMetadata} for all the selected navigation path
   *
   * @param entityName     service entity name
   * @param expandPathList list of all the navigation path
   * @return {@code SapODataColumnMetadata}
   * @throws EdmException any apache olingo processing exception.
   */
  private SapODataColumnMetadata buildSelectedNavigationColumns(String entityName, List<String> expandPathList)
    throws EdmException {

    SapODataColumnMetadata root = SapODataColumnMetadata.builder().build();

    for (String expandPath : expandPathList) {
      SapODataColumnMetadata parent = root;
      SapODataColumnMetadata current;

      //used to store the expanded entity sequence path
      List<String> childSequence = new ArrayList<>();

      String[] expandSequenceList = expandPath.split(NAV_PROPERTY_SEPARATOR);
      for (String expandSequence : expandSequenceList) {

        childSequence.add(expandSequence);
        String childPath = String.join(NAV_PROPERTY_SEPARATOR, childSequence);

        //checking if the parent contains the child with name(expandSequence)
        current = parent.getChildList()
          .stream()
          .filter(s -> Util.isNotNullOrEmpty(s.getName()) && s.getName().equals(expandSequence))
          .findFirst()
          .orElse(null);

        if (current != null) {
          parent = current;
        } else {
          SapODataColumnMetadata child;
          if (childPath.equals(expandPath)) {
            childSequence.remove(childSequence.size() - 1);
            String parentPath = String.join(NAV_PROPERTY_SEPARATOR, childSequence);

            EdmEntityType entityType = oDataServiceHelper.getNavigationPropertyEntityType(entityName, parentPath);
            if (entityType == null) {
              LOGGER.debug(ResourceConstants.DEBUG_ENTITY_NOT_FOUND.getMsgForKey(entityName, parentPath));
              return null;
            }

            // the last child could be a property or can be a navigation property so in both the case there will be only
            // one item in the returned list, so fetching the first index value from the list.
            child = buildSapODataColumns(entityType, ImmutableList.of(expandSequence)).get(0);
          } else {
            //building the parents as per the given path(childPath)
            child = buildNavigationColumn(entityName, childPath);
          }

          //appending child
          if (child != null) {
            parent.appendChild(child);
            parent = child;
          }
        }
      }
    }
    return root;
  }

  /**
   * Build {@code SapODataColumnMetadata} for the provided navigation property.
   *
   * @param entityName service entity name
   * @param navName can have navigation path
   * @return {@code SapODataColumnMetadata} or null in case of invalid navigation property name
   * @throws EdmException any apache olingo processing exception.
   */
  @Nullable
  private SapODataColumnMetadata buildNavigationColumn(String entityName, String navName) throws EdmException {
    EdmNavigationPropertyImplProv association = oDataServiceHelper.getNavigationProperty(entityName, navName);
    if (association == null) {
      LOGGER.debug(ResourceConstants.DEBUG_NAVIGATION_NOT_FOUND.getMsgForKey(navName, entityName));
      return null;
    }

    EdmEntityType entitySet = oDataServiceHelper.extractEntitySetFromNavigationProperty(association);
    if (entitySet == null) {
      LOGGER.debug(ResourceConstants.DEBUG_ENTITY_NOT_FOUND.getMsgForKey(association.getName(), navName));
      return null;
    }

    return SapODataColumnMetadata.builder()
      .name(association.getName())
      .type(entitySet.getKind().name())
      .multiplicityOrdinal(association.getMultiplicity().ordinal())
      .build();
  }

  /**
   * Builds the COMPLEX properties into list of {@code SapODataColumnMetadata}.
   *
   * @param namespace   SAP OData service entity namespace.
   * @param edmProperty SAP OData service entity complex {@code EdmProperty}
   * @return list of {@code SapODataColumnMetadata}
   * @throws EdmException any apache olingo processing exception.
   */
  private List<SapODataColumnMetadata> buildComplexTypes(String namespace, EdmProperty edmProperty)
    throws EdmException {

    EdmComplexType complexType = oDataServiceHelper.getComplexType(namespace, edmProperty.getName());
    if (complexType == null) {
      return Collections.emptyList();
    }

    List<SapODataColumnMetadata> columns = new ArrayList<>();

    List<String> propList = complexType.getPropertyNames();
    for (String prop : propList) {
      EdmProperty property = ((EdmProperty) complexType.getProperty(prop));
      columns.add(buildSapODataColumnMetadata(namespace, property));
    }

    return columns;
  }

  /**
   * Builds schema from the given list of {@code SapODataColumnMetadata}
   *
   * @param columnDetailList {@code SapODataColumnMetadata}
   * @return {@code Schema}
   */
  private Schema buildSchema(List<SapODataColumnMetadata> columnDetailList) {
    List<Schema.Field> outputSchema = columnDetailList.stream()
      .map(this::buildSchemaField)
      .collect(Collectors.toList());

    return Schema.recordOf("ODataColumnMetadata", outputSchema);
  }

  /**
   * Builds Schema field from {@code SapODataColumnMetadata}
   *
   * @param oDataColumnDetail {@code SapODataColumnMetadata}
   * @return {@code Schema.Field}
   */
  private Schema.Field buildSchemaField(SapODataColumnMetadata oDataColumnDetail) {
    if (oDataColumnDetail.containsChild()) {
      List<Schema.Field> outputSchema = oDataColumnDetail.getChildList()
        .stream()
        .map(this::buildSchemaField)
        .collect(Collectors.toList());

      String typeName = buildNestedTypeUniqueName(oDataColumnDetail.getName());

      if (oDataColumnDetail.getType().equals(EdmTypeKind.ENTITY.name()) &&
        (oDataColumnDetail.getMultiplicityOrdinal() != null && oDataColumnDetail.getMultiplicityOrdinal() > 0)) {
        // adding 1 to * multiplicity record to ARRAY type
        return Schema.Field.of(oDataColumnDetail.getName(), Schema.arrayOf(Schema.recordOf(typeName, outputSchema)));
      }

      // adding 0 to 1 multiplicity record to NULLABLE
      return Schema.Field.of(oDataColumnDetail.getName(), Schema.nullableOf(Schema.recordOf(typeName, outputSchema)));
    }

    return Schema.Field.of(oDataColumnDetail.getName(), buildRequiredSchemaType(oDataColumnDetail));
  }

  /**
   * Build and returns the appropriate schema type.
   *
   * @param oDataColumnDetail {@code SapODataColumnMetadata}
   * @return {@code Schema}
   */
  private Schema buildRequiredSchemaType(SapODataColumnMetadata oDataColumnDetail) {
    Schema schemaType = SCHEMA_TYPE_MAPPING.get(oDataColumnDetail.getType());

    if (schemaType.getLogicalType() == Schema.LogicalType.DECIMAL
      && oDataColumnDetail.getPrecision() != null
      && oDataColumnDetail.getScale() != null) {

      schemaType = Schema.decimalOf(oDataColumnDetail.getPrecision(), oDataColumnDetail.getScale());
    }

    // this check ensure that any DATE or TIME related fields are always set to NULLABLE Schema types.
    // Reason: in SAP OData catalog service any DATE or TIME field which is mandatory can hold '00000000' in case
    // of null and SAP OData service returns 'null' on data extraction for such fields so, to accordance this behaviour
    // inside the plugin any DATE or TIME related Schema type are hardcoded to NULLABLE type.
    if (schemaType.getLogicalType() == Schema.LogicalType.TIMESTAMP_MICROS ||
      schemaType.getLogicalType() == Schema.LogicalType.TIME_MICROS) {

      return Schema.nullableOf(schemaType);
    }

    return oDataColumnDetail.isNullable() ? Schema.nullableOf(schemaType) : schemaType;
  }

  /**
   * Prepares a unique name for the provide name. This is required in case to avoid the same type name referencing
   * issue at the runtime. Name format: <actualname>_<random name>
   * e.g. Supplier_5810e28b_c38d_41fe_8dc1_e24150c515d9
   *
   * @param actualName nested property name
   * @return unique name
   */
  private String buildNestedTypeUniqueName(String actualName) {

    // schema name with '-', fails at runtime because '-' is not supported only character and '_' is supported
    String randomName = UUID.randomUUID().toString().replace("-", "_");
    return actualName.concat("_").concat(randomName);
  }

  /**
   * freezes all the {@code SapODataColumnMetadata} to stop accepting more childrens.
   *
   * @param oDataColumnMetadataList
   */
  private void freezeSapColumnMetadata(List<SapODataColumnMetadata> oDataColumnMetadataList) {
    oDataColumnMetadataList.forEach(SapODataColumnMetadata::finalizeChildren);
  }
}
