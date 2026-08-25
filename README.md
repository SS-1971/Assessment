ORDERS INCREMENTAL ETL PIPELINE
================================

1. OVERVIEW
-----------

This project implements an incremental ETL pipeline using Azure Data Factory (ADF).

The pipeline:

1. Reads the previous watermark from the metadata table.
2. Gets an OAuth access token from the API.
3. Extracts order data from the REST API.
4. Stores the raw JSON file in Azure Data Lake Storage.
5. Transforms the JSON using an ADF Mapping Data Flow.
6. Stores the transformed data in the sales schema.
7. Loads the sales data into the curated schema using a SQL stored procedure.
8. Performs INSERT/UPDATE operations using MERGE.
9. Updates the ETL watermark after successful processing.


2. HIGH-LEVEL ARCHITECTURE
--------------------------

REST API
   |
   v
Lookup Watermark
   |
   v
Get OAuth Token
   |
   v
Copy Raw JSON
   |
   v
Azure Data Lake Storage
   |
   v
Mapping Data Flow
   |
   v
sales.Orders
sales.Products
sales.Order_Products
   |
   v
Stored Procedure
curated.Load_Sales_To_Curated
   |
   v
curated.Orders
curated.Products
curated.Order_Products
   |
   v
meta.ETL_Watermark


3. PIPELINE NAME
----------------

pl_ingest_orders_incremental


4. PIPELINE ACTIVITIES
----------------------

4.1 LK-GetWaterMark
-------------------

This Lookup activity reads the current watermark values from:

meta.ETL_Watermark

SQL query:

SELECT EntityName, WatermarkValue
FROM meta.ETL_Watermark
WHERE SourceSystem = 'SourceAPI'
  AND EntityName IN ('Orders', 'Products');


4.2 setproductWaterMark
-----------------------

This Set Variable activity stores the Products watermark.

Variable:

productWaterMark

Current expression:

@activity('LK-GetWaterMark').output.value[0].WatermarkValue


4.3 setOrderWaterMark
---------------------

This Set Variable activity stores the Orders watermark.

Variable:

orderWaterMark

Current expression:

@activity('LK-GetWaterMark').output.value[1].WatermarkValue


NOTE:
The current implementation depends on the order of rows returned by
the Lookup activity. A safer implementation is to select the row using
EntityName instead of relying on [0] and [1].


4.4 WB-GetToken
---------------

This Web activity calls the OAuth token endpoint.

Authentication flow:

OAuth 2.0 Client Credentials

The response contains an access token.

The access token is used when calling the REST API.


4.5 fileName
------------

This Set Variable activity generates the JSON file name.

Format:

orders_yyyyMMdd_HHmmss.json

Example:

orders_20260825_101530.json


4.6 CD-CopyRawData
------------------

This Copy activity calls the REST API and stores the response as a
JSON file in Azure Data Lake Storage.

The Authorization header is:

Bearer <access_token>

The file name is generated dynamically using the fileName variable.


4.7 TransformRawData
--------------------

This activity executes the Mapping Data Flow:

TransformRawData

The following parameters are passed to the Data Flow:

order_id   = orderWaterMark
product_id = productWaterMark

The Data Flow performs the required transformations and filtering.

The transformed data is stored in:

sales.Orders
sales.Products
sales.Order_Products


4.8 Stored procedure1
---------------------

This activity executes:

[curated].[Load_Sales_To_Curated]

The stored procedure loads data from the sales schema into the
curated schema.

It performs UPSERT operations using MERGE.


5. DATABASE SCHEMAS
-------------------

The database is divided into three logical areas:

sales
curated
meta


5.1 SALES SCHEMA
----------------

The sales schema contains the transformed/staging data produced by
the ADF Mapping Data Flow.

Tables:

sales.Orders
sales.Products
sales.Order_Products


5.2 CURATED SCHEMA
------------------

The curated schema contains the cleaned and upserted data used for
downstream processing and analytics.

Tables:

curated.Orders
curated.Products
curated.Order_Products


5.3 META SCHEMA
---------------

The meta schema contains ETL metadata.

Table:

meta.ETL_Watermark


6. ORDERS TABLE
---------------

Source:

sales.Orders

Target:

curated.Orders

Primary key:

order_id

MERGE condition:

target.order_id = source.order_id

If the order already exists:

UPDATE

If the order does not exist:

INSERT


7. PRODUCTS TABLE
-----------------

Source:

sales.Products

Target:

curated.Products

Primary key:

product_id

MERGE condition:

target.product_id = source.product_id

If the product already exists:

UPDATE

If the product does not exist:

INSERT


8. ORDER_PRODUCTS TABLE
-----------------------

Source:

sales.Order_Products

Target:

curated.Order_Products

Primary key:

(order_id, product_id)

MERGE condition:

target.order_id = source.order_id
AND target.product_id = source.product_id

If the order-product combination already exists:

UPDATE

If the order-product combination does not exist:

INSERT


9. CURATED TABLE RELATIONSHIPS
------------------------------

curated.Order_Products references:

curated.Orders(order_id)

curated.Products(product_id)


Relationship:

curated.Orders
       |
       | order_id
       |
       v
curated.Order_Products
       ^
       |
       | product_id
       |
curated.Products


10. WATERMARK TABLE
-------------------

Table:

meta.ETL_Watermark

Columns:

SourceSystem
EntityName
WatermarkValue
LastRunUtc
RowsExtracted
Status

Primary key:

(SourceSystem, EntityName)


11. WATERMARK PURPOSE
---------------------

The watermark is used to support incremental processing.

The pipeline first reads the previous watermark.

ADF then uses the watermark to filter the incoming data.

Only the required/new records are passed through the transformation
process.

After the curated load succeeds, the watermark is updated.


12. WATERMARK EXAMPLE
---------------------

Example metadata:

SourceSystem | EntityName       | WatermarkValue | RowsExtracted | Status
-------------|------------------|----------------|---------------|--------
SourceAPI    | Orders           | 10             | 5             | Success
SourceAPI    | Products         | 86             | 8             | Success


WatermarkValue:

Stores the latest processed ID.

RowsExtracted:

Stores the number of rows processed during the run.

Status:

Stores whether the ETL operation succeeded or failed.


13. ETL PROCESS
---------------

Step 1:
Read the current watermark from meta.ETL_Watermark.

Step 2:
Store the Orders watermark in the orderWaterMark variable.

Step 3:
Store the Products watermark in the productWaterMark variable.

Step 4:
Call the OAuth API and obtain an access token.

Step 5:
Call the REST API using the access token.

Step 6:
Store the raw JSON response in Azure Data Lake Storage.

Step 7:
Execute the TransformRawData Mapping Data Flow.

Step 8:
Use the watermark values to filter the required records.

Step 9:
Store transformed records in:

sales.Orders
sales.Products
sales.Order_Products

Step 10:
Execute:

curated.Load_Sales_To_Curated

Step 11:
MERGE the sales tables into the curated tables.

Step 12:
Update meta.ETL_Watermark.


14. STORED PROCEDURE
--------------------

Stored procedure:

curated.Load_Sales_To_Curated

The procedure performs:

1. MERGE sales.Orders into curated.Orders.
2. MERGE sales.Products into curated.Products.
3. MERGE sales.Order_Products into curated.Order_Products.
4. Calculate MAX(order_id).
5. Calculate MAX(product_id).
6. Calculate the number of rows processed.
7. Update the Orders watermark.
8. Update the Products watermark.
9. Update the Order_Products watermark.
10. Set the ETL status to Success.
11. Roll back the transaction if an error occurs.
12. Set the ETL status to Failed when an error occurs.


15. TRANSACTION HANDLING
-----------------------

The stored procedure uses a transaction.

Successful execution:

BEGIN TRANSACTION
       |
       v
MERGE Orders
       |
       v
MERGE Products
       |
       v
MERGE Order_Products
       |
       v
Update Watermarks
       |
       v
COMMIT TRANSACTION


If an error occurs:

BEGIN TRANSACTION
       |
       v
ETL operation fails
       |
       v
ROLLBACK TRANSACTION
       |
       v
Status = Failed


16. ADF VARIABLES
-----------------

The pipeline contains the following variables:

fileName
Type: String

Purpose:
Stores the generated JSON file name.


productWaterMark
Type: Integer

Purpose:
Stores the Products watermark.


orderWaterMark
Type: Integer

Purpose:
Stores the Orders watermark.


17. DATA FLOW PARAMETERS
------------------------

The Mapping Data Flow receives:

order_id

and

product_id

The values are passed from the ADF pipeline variables:

orderWaterMark
productWaterMark


18. DATA FLOW OUTPUTS
---------------------

The Mapping Data Flow writes to:

sales.Orders

sales.Products

sales.Order_Products


19. CURATED LOAD
----------------

The curated load uses SQL MERGE.

Orders:

sales.Orders
     |
     | MERGE
     v
curated.Orders


Products:

sales.Products
     |
     | MERGE
     v
curated.Products


Order Products:

sales.Order_Products
     |
     | MERGE
     v
curated.Order_Products


20. INCREMENTAL LOAD
--------------------

First run:

Watermark = 0

The pipeline processes the available records.

Example:

Orders:
1, 2, 3, 4, 5

Products:
10, 20, 30, 40


After successful processing:

Orders watermark = 5

Products watermark = 40


Next run:

ADF reads:

Orders watermark = 5

Products watermark = 40

Only records after those watermark values are processed.


21. IMPORTANT DESIGN POINTS
---------------------------

1. Raw API data is stored separately from transformed data.

2. The sales schema contains transformed/staging data.

3. The curated schema contains the final cleaned data.

4. The meta schema contains ETL metadata.

5. MERGE provides INSERT and UPDATE behavior.

6. Orders use order_id as the key.

7. Products use product_id as the key.

8. Order_Products uses order_id + product_id as the composite key.

9. Watermark values are used by ADF before the transformation.

10. The stored procedure does not receive watermark parameters because
    the filtering has already been performed in the ADF Data Flow.

11. The stored procedure updates the watermark after the curated load
    succeeds.

12. The stored procedure uses a transaction so that a failed curated
    load does not leave a partially committed transaction.


22. TECHNOLOGIES USED
---------------------

Azure Data Factory
Azure SQL Database
Azure Data Lake Storage
REST API
OAuth 2.0
Mapping Data Flow
SQL Server Stored Procedures
T-SQL
Incremental ETL
Watermark-based processing
MERGE / UPSERT


23. FINAL ARCHITECTURE
----------------------

                    REST API
                       |
                       v
                OAuth Token
                       |
                       v
               Copy Raw JSON
                       |
                       v
             Azure Data Lake
                       |
                       v
              TransformRawData
                Mapping Data Flow
                       |
          +------------+------------+
          |            |            |
          v            v            v
     sales.Orders  sales.Products  sales.Order_Products
          |            |            |
          +------------+------------+
                       |
                       v
       curated.Load_Sales_To_Curated
                       |
          +------------+------------+
          |            |            |
          v            v            v
   curated.Orders curated.Products curated.Order_Products
                       |
                       v
             meta.ETL_Watermark


24. EXECUTION
-------------

The curated stored procedure can be executed using:

EXEC curated.Load_Sales_To_Curated;


25. MAINTENANCE
---------------

When changing the pipeline:

1. Verify the watermark logic.
2. Verify Data Flow filtering.
3. Verify sales table mappings.
4. Verify curated table mappings.
5. Verify MERGE keys.
6. Verify foreign key relationships.
7. Verify watermark updates.
8. Test both first-run and subsequent-run scenarios.
9. Test duplicate records.
10. Test failure and retry scenarios.
"""

print(readme)
print("\n---\nCopy the text above into README.md")
print("---")
print(readme[:500])
