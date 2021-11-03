package com.google.cloud.datafusion.plugin.sap.odata.source.metadata;

import com.google.cloud.datafusion.plugin.util.Util;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.core.edm.provider.EdmNavigationPropertyImplProv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;


/**
 * This {@code SapODataEntityHelper} contains reusable SAP OData service metadata functions.
 */
public class SapODataEntityHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapODataEntityHelper.class);

  @NotNull
  private final Edm edmMetadata;

  public SapODataEntityHelper(@NotNull Edm edmMetadata) {
    this.edmMetadata = edmMetadata;
  }

  public Edm getEdmMetadata() {
    return edmMetadata;
  }

  /**
   * Find and return the EdmEntitySet instance from the metadata.
   *
   * @param entityName OData entity name
   * @return instance of EdmEntitySet class
   * @throws EdmException
   */
  @Nullable
  public EdmEntitySet getEntitySet(String entityName) throws EdmException {
    if (Util.isNotNullOrEmpty(entityName)) {
      for (EdmEntitySet edmEntitySet : edmMetadata.getEntitySets()) {
        if (edmEntitySet.getName().equals(entityName)) {
          return edmEntitySet;
        }
      }
    }

    String debugMsg = String.format("Entity name: '%s', could not find EntitySet for the given entity name. " +
      "Root cause: null / empty or invalid entity name is provided.", entityName);
    LOGGER.debug(debugMsg);

    return null;
  }

  @Nullable
  public EdmEntityType getEntityType(String entityName) throws EdmException {
    EdmEntitySet entitySet = getEntitySet(entityName);
    if (entitySet != null) {
      return entitySet.getEntityType();
    }

    String debugMsg = String.format("Entity name: '%s', could not find EntityType for the given entity name. " +
      "Root cause: null / empty or invalid entity name is provided.", entityName);
    LOGGER.debug(debugMsg);

    return null;
  }

  /**
   * Get list of all the default property name associated with the give 'entityName'
   *
   * @param entityType service entity type
   * @return list of default property name
   * @throws EdmException
   */
  @Nullable
  public List<String> getEntityPropertyList(EdmEntityType entityType) throws EdmException {
    if (entityType != null) {
      return entityType.getPropertyNames();
    }
    return null;
  }

  /**
   * Returns the list of default entity from the metadata.
   *
   * @return list of default entity from the metadata
   * @throws EdmException
   */
  public List<EdmEntitySet> getDefaultEntitySet() throws EdmException {
    return edmMetadata.getDefaultEntityContainer().getEntitySets();
  }

  /**
   * Find and return the last navigation property from the given navigation path.
   *
   * @param entityName service entity name
   * @param navPath    can have navigation path or navigation property name
   * @return
   * @throws EdmException
   */
  @Nullable
  public EdmNavigationPropertyImplProv getNavigationProperty(String entityName, String navPath)
    throws EdmException {

    if (Util.isNotNullOrEmpty(entityName) && Util.isNotNullOrEmpty(navPath)) {
      EdmEntitySet entitySet = getEntitySet(entityName);
      if (entitySet != null) {
        EdmEntityType entityType = entitySet.getEntityType();
        EdmNavigationPropertyImplProv association = null;

        String[] navNames = navPath.split("/");
        for (String name : navNames) {
          if (entityType.getNavigationPropertyNames().contains(name)) {
            EdmNavigationPropertyImplProv navProperty = (EdmNavigationPropertyImplProv) (entityType.getProperty(name));
            entityType = navProperty.getRelationship().getEnd(navProperty.getToRole()).getEntityType();
            association = navProperty;
          }
        }
        return association;
      }
    }

    String debugMsg = String.format("Entity name: '%s' and Expand path: '%s', navigation property is not found in the" +
      " given expand path. " +
      "Root cause: null / empty or invalid entity name or expand path was provided.", entityName, navPath);
    LOGGER.info(debugMsg);

    return null;
  }

  /**
   * Find and return the EdmEntityType for the given navigation property.
   *
   * @param navProperty navigation property
   * @return
   * @throws EdmException
   */
  @Nullable
  public EdmEntityType extractEntitySetFromNavigationProperty(EdmNavigationPropertyImplProv navProperty)
    throws EdmException {

    if (navProperty != null) {
      String toRole = navProperty.getToRole();
      return navProperty.getRelationship().getEnd(toRole).getEntityType();
    }

    LOGGER.debug("Could not find the Entity type extraction from the given navigation property. " +
      "Root cause: null object passed in the parameter.");

    return null;
  }

  /**
   * Find and returns the EdmComplexType for the given parameters.
   *
   * @param namespace    of the parent entity
   * @param propertyName complex property name
   * @return
   * @throws EdmException
   */
  @Nullable
  public EdmComplexType getComplexType(String namespace, String propertyName) throws EdmException {
    if (Util.isNotNullOrEmpty(namespace) && Util.isNotNullOrEmpty(propertyName)) {
      return edmMetadata.getComplexType(namespace, propertyName);
    }

    String debugMsg = String.format("Namespace: '%s' and Complex property name: '%s', " +
      "no complex type property found in the given namespace. " +
      "Root cause: null / empty or invalid Namespace or Complex property name provided.", namespace, propertyName);
    LOGGER.debug(debugMsg);

    return null;
  }

  @Nullable
  public EdmEntityType getNavigationPropertyEntityType(String entityName, String navPath) throws EdmException {
    EdmNavigationPropertyImplProv navProp = getNavigationProperty(entityName, navPath);
    return extractEntitySetFromNavigationProperty(navProp);
  }
}
