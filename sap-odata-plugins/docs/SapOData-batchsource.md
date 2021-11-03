# SAP OData Batch Source  
## Description  
The SAP OData Batch Source plugin enables bulk data integration from SAP
applications with the Cloud Data Fusion platform. You can configure and execute
bulk data transfers from SAP Entities and views without any coding.

## Properties  
You can configure the following properties for the SAP OData.  

**Note**: The following indicators are used to define the fields:  
**M** - Indicates Macros are supported for the respective field  
**O** - Optional field  

**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.  
**SAP Hostname (M)**: SAP Gateway hostname or IP address.  
**OData Version (M)**: Option to change the OData version to v2.  
**Service Name (M)**: Name of the SAP OData service from which the user wants to extract an Entity.   
**Entity Name (M)**: Name of the Entity which is being extracted.   

## For Basic (via Username and Password):  

**SAP Logon Username (M)**: SAP Logon user ID.  
**SAP Logon Password (M)**: SAP Logon password for user authentication.  

## SAP X.509 Client Certificate  

**GCP Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.  
**GCS Path**: Google Cloud Storage path which contains the user uploaded X.509
certificate corresponding to the SAP application server for secure calls.  
**Passphrase**: Passphrase corresponding to the provided X.509 certificate.    

## Advance Option:  

**Filter Options (M, O)**: Filter condition to restrict the output data volume e.g. Price gt 200
<table border="1" cellspacing="0" cellpadding="0" aria-label="Filter Query Option Operators">
<tbody>
<tr>
<th>Operator</th>
<th>Description</th>
<th>Example</th>
</tr>
<tr>
<td colspan="3"><b>Logical Operators</b></td>
</tr>
<tr>
<td>Eq</td>
<td>Equal</td>
<td>/Suppliers?$filter=Address/City eq 'Redmond'</td>
</tr>
<tr>
<td>Ne</td>
<td>Not equal</td>
<td>/Suppliers?$filter=Address/City ne 'London'</td>
</tr>
<tr>
<td>Gt</td>
<td>Greater than</td>
<td>/Products?$filter=Price gt 20</td>
</tr>
<tr>
<td>Ge</td>
<td>Greater than or equal</td>
<td>/Products?$filter=Price ge 10</td>
</tr>
<tr>
<td>Lt</td>
<td>Less than</td>
<td>/Products?$filter=Price lt 20</td>
</tr>
<tr>
<td>Le</td>
<td>Less than or equal</td>
<td>/Products?$filter=Price le 100</td>
</tr>
<tr>
<td>And</td>
<td>Logical and</td>
<td>/Products?$filter=Price le 200 and Price gt 3.5</td>
</tr>
<tr>
<td>Or</td>
<td>Logical or</td>
<td>/Products?$filter=Price le 3.5 or Price gt 200</td>
</tr>
<tr>
<td>Not</td>
<td>Logical negation</td>
<td>/Products?$filter=not endswith(Description,'milk')</td>
</tr>
<tr>
<td colspan="3"><b>Arithmetic Operators</b></td>
</tr>
<tr>
<td>Add</td>
<td>Addition</td>
<td>/Products?$filter=Price add 5 gt 10</td>
</tr>
<tr>
<td>Sub</td>
<td>Subtraction</td>
<td>/Products?$filter=Price sub 5 gt 10</td>
</tr>
<tr>
<td>Mul</td>
<td>Multiplication</td>
<td>/Products?$filter=Price mul 2 gt 2000</td>
</tr>
<tr>
<td>Div</td>
<td>Division</td>
<td>/Products?$filter=Price div 2 gt 4</td>
</tr>
<tr>
<td>Mod</td>
<td>Modulo</td>
<td>/Products?$filter=Price mod 2 eq 0</td>
</tr>
<tr>
<td colspan="3"><b>Grouping Operators</b></td>
</tr>
<tr>
<td>( )</td>
<td>Precedence grouping</td>
<td>/Products?$filter=(Price sub 5) gt 10</td>
</tr>
</tbody>
</table>   

**Select Fields (M, O)**: Fields to be preserved in the extracted data. e.g.: Category,Price,Name,
Supplier/Address. In case of empty all the non-navigation fields will be preserved in the extracted data.  
**Expand Fields (M, O)**: List of complex fields to be expanded in the extracted output data
e.g.: Products,Products/Suppliers  
**Number of Rows to Skip (M, O)**: Rows to skip e.g.: 10.  
**Number of Rows to Fetch (M, O)**: Total number of rows to be extracted (accounts for conditions specified
in Filter Options).  

**Number of Splits to Generate (M, O)**: The number of splits used to partition the input data.
More partitions will increase the level of parallelism, but will require more resources and overhead.  
**Batch Size (M, O)**: Number of rows to fetch in each network call to SAP.
Smaller size will cause frequent network calls repeating the associated overhead.
A large size may slow down data retrieval & cause excessive resource usage in SAP.    
