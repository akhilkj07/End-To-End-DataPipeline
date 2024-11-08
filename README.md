# End-to-End Data Pipeline Using Azure and Databricks
This project demonstrates a complete end-to-end data pipeline utilizing Microsoft Azure services and Databricks, built to process and transform data from an on-premise AdventureWorks sample database to Azure Data Lake Storage Gen2.

![image](https://github.com/user-attachments/assets/9f9f1cf9-d8b4-4666-8d2d-047ca0f6304d)


## Project Overview
Using a medallion architecture (Bronze, Silver, and Gold), this pipeline performs secure data transfers, transformations, and query operations, making the data ready for insightful reporting.

Steps
### Step 1: Loading Data into Azure Data Lake Storage (ADLS) Gen2
Creating Storage Containers: Three containers were created in the ADLS storage account — Bronze, Silver, and Gold — to represent each stage of data processing.

Azure Data Factory Pipeline: An Azure Data Factory pipeline was designed to securely copy tables from the on-premise AdventureWorks database to the Bronze container in Parquet format:

Lookup Activity: Retrieves the list of tables.
ForEach Activity: Copies each table to the ADLS container.

![image](https://github.com/user-attachments/assets/303e855e-5c66-4da6-a8e7-7746f80eb1e6)

Secure Credentials: Linked Services in Azure Data Factory use credentials stored in Azure Key Vault to securely access resources.

### Step 2: Data Transformation with Azure Databricks
Mounting Storage: The ADLS storage was mounted to Databricks by registering an app in Azure, granting it Storage Blob Data Contributor permissions, and using its credentials (Client ID, Tenant ID, Secret Key) in the Databricks notebook (ADLS_mount) to create a connection.

Data Transformation:

Bronze to Silver: Converted date columns from Timestamp to Date data type.
Silver to Gold: Renamed columns to follow snake_case naming conventions.
Databricks Pipeline: The transformation notebooks are integrated into the ADF pipeline, allowing data to be processed through each layer, resulting in clean, structured Delta tables in the Gold container.

![image](https://github.com/user-attachments/assets/22a19f85-fc19-4e64-b199-5a9779dfbfb7)

### Step 3: Querying and Creating Views in Azure Synapse Analytics
Stored Procedure for View Creation: In Azure Synapse, a serverless pool database (gold_db) was created, and a parameterized stored procedure (CreateGoldViews_SP) was developed.
The SP Takes each table name as a parameter, retrieved via a Get Metadata activity linked to the Gold container.
Creates views for each Delta table in the Gold layer.
Synapse Pipeline: The Synapse pipeline orchestrates the view creation process, ensuring each Gold-layer Delta table has a corresponding view.

![image](https://github.com/user-attachments/assets/3b46e395-3994-441e-949d-e4a95696c27b)

## Final Output
Upon successful pipeline execution, data is seamlessly moved from the on-premise database into Azure’s structured storage, transformed in Databricks, and available as views in Synapse for further analytics and reporting. The views in Synapse can serve as a data source for Power BI, enabling rich visualizations and insights.

![image](https://github.com/user-attachments/assets/80e17896-f3ea-44e8-8ed2-380d9c4b7bc1)



