**Description**  
The SAP Table Batch Source plugin enables bulk data integration from SAP
applications with the Cloud Data Fusion platform. You can configure and execute
bulk data transfers from SAP tables and views without any coding.  

**Properties**  
You can configure the following properties for the SAP Table.  

**Note**: The following indicators are used to define the fields:  
**M** - Indicates Macros are supported for the respective field  
**O** - Optional field  

**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.  
**SAP Client (M)**: The SAP client to use (e.g. 100).  
**SAP Language (M)**: SAP logon language (e.g. EN).  
**Connection Type**: SAP Connection type (Direct or Load Balanced).   
For Direct connection:  
**SAP Application Server Host (M)**: The SAP server name or IP address.  
**SAP System Number (M)**: The SAP system number (e.g. 00).  
**SAP Router (M, O)**: The router string.  
For Load Balanced connection:   
**SAP Message Server Host (M)**: The SAP Message Host name or IP address.  
**SAP Message Server Service or Port Number (M)**: The SAP Message Server
Service or Port Number (e.g. sapms02).  
**SAP System ID (SID) (M)**: The SAP System ID (e.g. N75).  
**SAP Logon Group Name (M)**: The SAP logon group name (e.g. PUBLIC).  

**SAP Table/View Name (M)**: The SAP table/view name (e.g. MARA).  

**Get Schema** button: The plugin generates a schema based on the metadata from
SAP, with automatic mapping of SAP data types to corresponding Cloud Data Fusion
data types (same functionality as the Validate button).  

**SAP Logon Username (M)**: SAP User name. **Recommended**: If the SAP Logon
Username changes periodically, use a [macro](
    https://cdap.atlassian.net/wiki/spaces/DOCS/pages/382043060/Using+macros+to+create+a+dynamic+data+pipeline).  
**SAP Logon Password (M)**: SAP User password. **Recommended**: Use
[**secure macros**](
https://cdap.atlassian.net/wiki/spaces/DOCS/pages/801767425/Using+Secure+Keys)
for sensitive values like **User password**.  

**GCP Project ID (M)**: Google Cloud Project ID, which uniquely identifies a
project. It can be found on the Dashboard in the Google Cloud Platform Console.  
**SAP JCo Library GCS Path (M)**: The Google Cloud Storage path which
contains the user uploaded SAP JCo library files.  

**Filter Options (M, O)**: Conditions specified in OpenSQL syntax which will be
used as filtering conditions in the SQL WHERE clause (e.g. KEY6 LT ‘25').
Records can be extracted based on conditions like certain columns having a
defined set of values or a range of values.  
**Number of Rows to Fetch (M, O)**: Use to limit the number of extracted records.
Enter a positive whole number. If 0 or left blank, extracts all records from the
specified table. If a positive value is provided, which happens to be greater
than the actual number of records available based on Filter Options, then only
the available records are extracted.

**Number of Splits to Generate (M, O)**: Use to create partitions to extract
table records in parallel. Enter a positive whole number. The runtime engine
creates the specified number of partitions (and SAP connections) while
extracting the table records. Use caution when setting this property to a number
greater than 16, since higher parallelism increases simultaneous connections
with SAP. Values between 8-16 are recommended. If the value is 0 or left blank,
then the system automatically chooses an appropriate value based on the number
of Executors available, records to be extracted, and the package size.  

**Package Size (M, O)**: Number of records to be extracted in a single SAP
network call. This is the number of records SAP stores in memory during every
network extract call. Multiple data pipelines extracting data can peak the
memory usage and may result in failures due to ‘Out of Memory' errors. Use
caution when setting this property.   
Enter a positive whole number. If 0 or left blank, the plugin uses a standard
value of 70000 or an appropriately calculated value if the number of records to
be extracted is less than 70000.   
If the data pipeline fails with ‘Out of Memory' errors, either decrease the
package size or increase the memory available for your SAP work processes.  

**Data Type Mappings from SAP to CDAP**  
The following table lists out different SAP data types, as well as the
corresponding CDAP data type for each SAP type.  

<table>
<thead>
<tr>
<th>ABAP Type</th>
<th>Detail</th>
<th>Data Fusion Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Numeric</strong></td>
<td></td>
<td></td>
</tr>
<tr>
<td>b</td>
<td>1-byte Integer (INT1)</td>
<td><strong>INT</strong></td>
</tr>
<tr>
<td>s</td>
<td>2-byte Integer (INT2)</td>
<td><strong>INT</strong></td>
</tr>
<tr>
<td>i</td>
<td>4-byte Integer (INT4)</td>
<td><strong>INT</strong></td>
</tr>
<tr>
<td>int8<br>
8</td>
<td>8-byte Integer (INT8)</td>
<td><strong>LONG</strong></td>
</tr>
<tr>
<td>p</td>
<td>Packed number in BCD format (DEC)</td>
<td><strong>DECIMAL</strong></td>
</tr>
<tr>
<td>(decfloat16)<br>
a</td>
<td>Decimal floating point 8 bytes IEEE 754r (DF16_DEC, DF16_RAW)</td>
<td><strong>DECIMAL</strong></td>
</tr>
<tr>
<td>(decfloat34)<br>
e</td>
<td>Decimal floating point 16 bytes IEEE 754r (DF34_DEC, DF34_RAW)</td>
<td><strong>DECIMAL</strong></td>
</tr>
<tr>
<td>f</td>
<td>Binary floating-point number (FLTP)</td>
<td><strong>DOUBLE</strong></td>
</tr>
<tr>
<td><strong>Character</strong></td>
<td></td>
<td></td>
</tr>
<tr>
<td>c</td>
<td>Character string (CHAR/LCHR)</td>
<td><strong>STRING</strong></td>
</tr>
<tr>
<td>string</td>
<td>Character string (SSTRING,<a
href="https://help.sap.com/doc/abapdocu_cp_index_htm/CLOUD/en-US/abenlength_functions.htm">
<u>GEOM_EWKB</a>)</td>
<td><strong>STRING</strong></td>
</tr>
<tr>
<td>string</td>
<td>Character string CLOB (STRING)</td>
<td><strong>BYTES</strong></td>
</tr>
<tr>
<td>n</td>
<td>Numeric Text (NUMC/ACCP)</td>
<td><strong>STRING</strong></td>
</tr>
<tr>
<td><strong>Byte</strong></td>
<td></td>
<td></td>
</tr>
<tr>
<td>x</td>
<td>Binary Data (RAW/LRAW)</td>
<td><strong>BYTES</strong></td>
</tr>
<tr>
<td>xstring</td>
<td>Byte string BLOB (RAWSTRING)</td>
<td><strong>BYTES</strong></td>
</tr>
<tr>
<td><strong>Date/Time</strong></td>
<td></td>
<td></td>
</tr>
<tr>
<td>d</td>
<td>Date (DATS)</td>
<td><strong>DATE</strong></td>
</tr>
<tr>
<td>t</td>
<td>Time (TIMS)</td>
<td><strong>TIME</strong></td>
</tr>
<tr>
<td>utclong/utcl</td>
<td>UTC Timestamp</td>
<td><strong>TIMESTAMP</strong></td>
</tr>
</tbody>
</table>
