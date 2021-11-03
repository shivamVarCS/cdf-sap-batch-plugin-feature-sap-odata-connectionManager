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

punch hole

**Note**: Since macros are supported for the connection parameters, they can be
used to manage the SAP connections in a centralized way (e.g. by setting the
values at runtime using the Argument Setter plugin).

**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.  
**SAP Hostname (M)**: SAP Gateway hostname or IP address.  
**OData Version (M)**: Option to change the OData version to v2.  
**Service Name (M)**: Name of the SAP OData service from which the user wants to extract an Entity.     
**Entity Name (M)**: Name of the Entity which is being extracted.  
**Security Type**: Option to change authentication/authorization scheme between Username-Password or OAuth2.0
Client Credentials.  

## For Basic (via Username and Password):

**SAP Logon Username (M)**: SAP Logon user ID.  
**SAP Logon Password (M)**: SAP Logon password for user authentication.   
**X.509 Certificate GCS Path (M)**: Google Cloud Storage path which contains the user uploaded X.509
certificate corresponding to the SAP application server for secure calls.  

## For OAuth2.0 Client Credentials:

**Authorization Server URL (M)**: SAP Identity provider authorization server URL.  
**Client ID (M)**: SAP registered client id.  
**Client Secret (M)**: Secret passphrase corresponding to the registered client id.   
**X.509 Certificate GCS Path (M)**: Google Cloud Storage path which contains the user uploaded X.509
certificate corresponding to the SAP application server for secure calls.  

## Advance Option:

**Filter Options ($filter) (M, O)**: Filter condition to restrict the output data volume e.g. Price gt 200  
**Select Fields ($select) (M, O)**: Fields to be preserved in the extracted data e.g.: Category,Price,Name,
Supplier/Address  
**Expand Fields ($expand) (M, O)**: List of complex fields to be expanded in the extracted output data
e.g.: Products,Products/Suppliers  

**Number of Rows to Skip ($skip) (M, O)**: Rows to skip e.g.: 10.  

**Number of Rows to Fetch (M, O)**: Total number of rows to be extracted (accounts for conditions specified
in Filter Options ($filter)).  

**Number of Splits to Generate (M, O)**: The number of splits used to partition the input data.
More partitions will increase the level of parallelism, but will require more resources and overhead.  

**Batch Size (M, O)**: Number of rows to fetch in each network call to SAP.
Smaller size will cause frequent network calls repeating the associated overhead.
A large size may slow down data retrieval & cause excessive resource usage in SAP.  
