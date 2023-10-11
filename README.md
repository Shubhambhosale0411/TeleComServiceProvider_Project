# TeleComServiceProvider_Project
Case Study For Data Engineering  with PySpark and Azure Service
## Architecture Diagram
![image](https://github.com/Shubhambhosale0411/TeleComServiceProvider_Project/assets/90494573/e0f62cfa-4bd9-4327-a633-7d362605a706)
## Flow of the Project:
<B>
ZIP files --> Blob Storage --> ADLS(Bronze) --> Databricks & Dataflow(ADF) -- > ADLS(Gold) -- > PowerBi


Xyz Company is a Streaming telecom service provider . 
Developed an end-to-end data engineering pipeline for xyz company to analyze data for september month billing amount and perform customer segmentation. 
This will help the company to better understand their customers' needs.

Services used: Azure Blob Storage, Azure Data Factory, Azure Datalake storage (ADLS), Pyspark, Azure Databricks, PowerBi

## DATA INGESTION :

The dataset includes several types of data related to a telecom services provider.
Generate a monthly billing report for the current month.
Create visualizations and dashboards using network metrics and caller complaints data.
Data cleaning, data integration, report generation, and visualization using azure services.

## DATA TRANSFORMATIONS USING DATABRICKS & ADF(Dataflow):

Create adlsfiles container with three directory called Bronze,Silver,Gold in ADLS.
 using Azure Active Directory (Azure AD) and OAuth2 for authentication, which is a more secure and modern approach for mounting Azure Data Lake Storage to Databricks.i.e Mounting through Secret Scope.
Do cleaning, Removing Nulls, Outliers, validation checks and other basic transformations is been done using Azure Data Factory via Dataflow Activity. 
Doing some complex transformations using Databricks, finding some trends and insights on the data based on the business requirement(present in problemstatement.pdf).
Then, all files get written into transformedData container in ADLS and also saved in parquet format because of its own benefits.

## DATA VISUALIZATION USING POWERBI:

Power BI is a business intelligence tool by Microsoft for data visualization and interactive reports, 
helping businesses make data-driven decisions. 

## AZURE KEY VAULT:

Here, we have used Azure Key vault to store secret keys for more secured connection. 

## Trigger:

commonly used in Azure services to automate processes and enable event-driven architectures.
Here in our project we used Storage Trigger as we insert those zip files in blob storage Trigger starts its execution and 
completes the entire workflow
