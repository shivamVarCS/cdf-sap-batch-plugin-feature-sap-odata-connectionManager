package com.google.cloud.datafusion.plugin.sap.odata.source.metadata;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

public class SapODataColumnMetadataTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private static final String FIELD_NAME = "fieldName";

  private SapODataColumnMetadata sapODataColumnMetadata;

  private enum SapODataColMetadata {
    SBYTE(FIELD_NAME + 1, "SByte", false, null),
    BYTE(FIELD_NAME + 2, "Byte", false, null),
    INT16(FIELD_NAME + 3, "Int16", false, null),
    INT32(FIELD_NAME + 4, "Int32", false, null),
    INT64(FIELD_NAME + 5, "Int64", false, null),
    SINGLE(FIELD_NAME + 6, "Single", false, null),
    DOUBLE(FIELD_NAME + 7, "Double", false, null),
    DECIMAL(FIELD_NAME + 8, "Decimal", false, null),
    GUID(FIELD_NAME + 9, "Guid", false, null),
    STRING(FIELD_NAME + 10, "String", false, null),
    BINARY(FIELD_NAME + 11, "Binary", false, null),
    BOOLEAN(FIELD_NAME + 12, "Boolean", false, null),
    DATETIME(FIELD_NAME + 13, "DateTime", false, null),
    TIME(FIELD_NAME + 14, "Time", false, null),
    DATETIMEOFFSET(FIELD_NAME + 15, "DateTimeOffset", false, null);

    private String name;
    private String type;
    private boolean isNullable;
    private List<SapODataColumnMetadata> childList;

    SapODataColMetadata(String name, String type, boolean isNullable,
                        List<SapODataColumnMetadata> childList) {
      this.name = name;
      this.type = type;
      this.isNullable = isNullable;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public boolean isNullable() {
      return isNullable;
    }
  }

  @Test
  public void checkBuildForDefaultInitialization() {
    sapODataColumnMetadata = SapODataColumnMetadata.builder().build();

    Assert.assertTrue(sapODataColumnMetadata.isNullable());
    Assert.assertFalse(sapODataColumnMetadata.containsChild());
  }

  @Test
  public void checkBasicInitialization() {
    sapODataColumnMetadata = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.STRING.getName())
      .type(SapODataColMetadata.STRING.getType())
      .isNullable(SapODataColMetadata.STRING.isNullable())
      .build();

    Assert.assertEquals(SapODataColMetadata.STRING.getName(), sapODataColumnMetadata.getName());
    Assert.assertEquals(SapODataColMetadata.STRING.getType(), sapODataColumnMetadata.getType());
    Assert.assertFalse(sapODataColumnMetadata.isNullable());
    Assert.assertFalse(sapODataColumnMetadata.containsChild());
  }

  @Test
  public void checkBuildForChildInitialization() {
    SapODataColumnMetadata child1 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.GUID.getName())
      .type(SapODataColMetadata.GUID.getType())
      .build();

    SapODataColumnMetadata child2 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.DATETIME.getName())
      .type(SapODataColMetadata.DATETIME.getType())
      .build();

    sapODataColumnMetadata = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.STRING.getName())
      .type(SapODataColMetadata.STRING.getType())
      .childList(Arrays.asList(child1, child2))
      .build();

    Assert.assertTrue(sapODataColumnMetadata.isNullable());
    Assert.assertTrue(sapODataColumnMetadata.containsChild());

    Assert.assertEquals(2, sapODataColumnMetadata.getChildList().size());
  }

  @Test
  public void checkAppendChildren() {
    sapODataColumnMetadata = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.INT16.getName())
      .type(SapODataColMetadata.INT16.getType())
      .build();

    SapODataColumnMetadata child1 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.BYTE.getName())
      .type(SapODataColMetadata.BYTE.getType())
      .build();

    sapODataColumnMetadata.appendChild(child1);

    Assert.assertTrue(sapODataColumnMetadata.containsChild());

    Assert.assertEquals(1, sapODataColumnMetadata.getChildList().size());
  }

  @Test
  public void checkAppendChildrenAfterFinalize() {
    sapODataColumnMetadata = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.INT16.getName())
      .type(SapODataColMetadata.INT16.getType())
      .build();

    sapODataColumnMetadata.finalizeChildren();

    SapODataColumnMetadata child1 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.BYTE.getName())
      .type(SapODataColMetadata.BYTE.getType())
      .build();

    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("No more children can be added for the current object, check if the " +
      "'finalizeChildren' is called before adding the children.");

    sapODataColumnMetadata.appendChild(child1);
  }

  @Test
  public void checkAppendingNestedChildrenAfterParentFinalize() {
    SapODataColumnMetadata child1 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.GUID.getName())
      .type(SapODataColMetadata.GUID.getType())
      .build();

    SapODataColumnMetadata child2 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.DATETIME.getName())
      .type(SapODataColMetadata.DATETIME.getType())
      .build();

    sapODataColumnMetadata = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.STRING.getName())
      .type(SapODataColMetadata.STRING.getType())
      .childList(Arrays.asList(child1, child2))
      .build();

    SapODataColumnMetadata child3 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.DATETIME.getName())
      .type(SapODataColMetadata.DATETIME.getType())
      .build();

    sapODataColumnMetadata.getChildList().get(1).appendChild(child3);

    Assert.assertTrue(sapODataColumnMetadata.containsChild());

    Assert.assertEquals(1, sapODataColumnMetadata.getChildList().get(1).getChildList().size());

    sapODataColumnMetadata.finalizeChildren();

    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("No more children can be added for the current object, check if the " +
      "'finalizeChildren' is called before adding the children.");

    SapODataColumnMetadata child4 = SapODataColumnMetadata.builder()
      .name(SapODataColMetadata.DATETIME.getName())
      .type(SapODataColMetadata.DATETIME.getType())
      .build();

    // must throw IllegalStateException as finalizeChildren is already called
    sapODataColumnMetadata.getChildList().get(1).appendChild(child4);
  }
}
