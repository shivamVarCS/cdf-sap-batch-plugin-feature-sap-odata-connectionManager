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

package com.google.cloud.datafusion.plugin.sap.table.source;

import com.google.cloud.datafusion.plugin.sap.metadata.model.SapObjectMetadata;
import com.google.cloud.datafusion.plugin.sap.source.AbstractStructuredSchemaTransformer;
import com.google.cloud.datafusion.plugin.sap.table.metadata.model.SapColumn;

/**
 * @author sankalpbapat
 *
 */
public class SapTableToStructuredSchemaTransformer extends AbstractStructuredSchemaTransformer {

  /**
   * Retrieves the native value for the SAP Object's field from the raw SAP record
   * and column metadata corresponding to SAP table.<br/>
   * <br/>
   * 
   * For a sample raw record from SAP<br/>
   * 
   * <pre>
   * 10045000000011710FNB    920171008BPINST      20171008232445.3245740
   * 0000100009USSU-VSF04E0002 14  30   0  2.000  0.000 1710002USD     1.00000  
   * 20171008000000000000000000000000000000000000000000000000         00000000
   * </pre>
   * 
   * All column values are just a running string (including spaces for empty
   * columns or data length less than size of the column) without any marker to
   * identify individual columns and {@code runtimeMetadata} is used to get the
   * offset and length of every column value.
   * 
   * @param rawRecord       SAP response record for the table
   * @param runtimeMetadata column metadata helps to get individual column values
   *                        from {@code rawRecord}
   * @param fieldIdx        Field index based on it's position in metadata
   *                        acquired by plugin
   */
  @Override
  public String getFieldNativeValue(SapObjectMetadata runtimeMetadata, String rawRecord, int fieldIdx) {
    SapColumn sapColumn = (SapColumn) runtimeMetadata.getFieldMetadata().get(fieldIdx);
    int offset = sapColumn.getOffset();
    int colLength = sapColumn.getLength();
    int recordLength = rawRecord.length();
    String colValStr = null;
    // actual length of data in record may be less than metadata offset of column,
    // if later columns do not have a value
    if (offset < recordLength) {
      colValStr = (offset + colLength) <= recordLength ? rawRecord.substring(offset, offset + colLength)
        : rawRecord.substring(offset, recordLength);
    }

    return colValStr;
  }
}
