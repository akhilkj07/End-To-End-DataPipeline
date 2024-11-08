# End-To-End-DataPipeline
This is a personal project to build an end-to-end data pipeline using Azure services and Databricks

For this project, I used the AdventureWorks sample database provided by Microsoft.

Step 1: Loading the database tables into Azure Data Lake Storage Gen2. For this, I created containers in my storage account named Bronze, Silver and Gold (Medallion Architecture).

To start with, I created an Azure Data Factory pipeline to copy all the tables from the on-premise database to bronze container in parquet format.
Here is how the pipeline looked at this stage:
![image](https://github.com/user-attachments/assets/303e855e-5c66-4da6-a8e7-7746f80eb1e6)

The Lookup activity gets the list of tables from the database and using a ForEach activity, each table is being copied to ADLS container as Parquet tables.

Note: The credentials used in the Linked Services are taken from Azure Key Vault where they are stored securely as secrets.

Step 2: Now that we have all the raw data in the Bronze table, next step is to perform transformations/enforce schema on this raw data.

To do this, I used Azure Databricks. Starting with mounting the storage account to the databricks workspace. 
For this I registered an App in Azure and provided Storage blob Data Contributor to the App and then used the App's Client Id, Tenant Id and Secret Key to create connection to the container.

The code for this can be found in the notebook named 'ADLS mount'

Once mounting was done, next is to perform transformations on the data. Since this is a sample project I'm doing only small transformations:
1. Bronze to Siver -> The columns with date is converted from Timestamp type to Date type.
2. Silver to gold -> Column names are renamed to avoid pascal case and use snake case.

For this, I use 2 other notebooks. After adding mounting and transformation notebooks to the pipeline, it looks like this:

![image](https://github.com/user-attachments/assets/22a19f85-fc19-4e64-b199-5a9779dfbfb7)

Upon successful completion of the pipeline, the data is successfully moved from on-prem database to storage account, which is then accessed in databricks notebooks
for transformation and the clean data reaches gold layer in delta table format.

Step 3: Once the data is ready, I used Azure Synapse Analytics to query the data and create a Stored Procedure to create views for each table.
The Stored Procedure is created in a database (gold_db) I created using serverless pool. The script for the SP can be found in CreateGoldViews_SP file. 
It uses parameterized script which takes value of each table name fetched using Get Metadata activity. Here is how the pipeline in Synapse looks like:

![image](https://github.com/user-attachments/assets/3b46e395-3994-441e-949d-e4a95696c27b)

The Getmetadata uses a binary dataset connected to the gold container.

Upon successful completion of this pipeline, views are created for each delta table in gold layer.

![image](https://github.com/user-attachments/assets/d33605e8-fe92-4b96-b3cb-bbe6464258ad)

Thereby, an end-to-end pipeline has been successfully created. The views in the synapse database can be used as source for Power BI reports to create insightful dashboards.

