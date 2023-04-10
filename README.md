Capstone in Data Engineering camp provided by DataTalks.
# Topic of Project - Does temperature affected crime incidents in the big city?

## Problem description
Does the temperature affect the crime rate? Last summer break 30year temperature record in Austin, TX. Did this scorching weather affect crime in the city overall? 
This question could be answered after conducting data analysis and vizualization based on Austin police department and historical weather data of 2022.

## Overview

1. Infra setup in Terraform
2. Data ingestion - download the dataset via Airflow and place it in a GCP bucket
3. Data warehouse - Host db tables on BigQuery, setup BQ using terraform
4. Transformations - Use dbt to transform the data to a suitable schema and store in BigQuery efficiently (partitioned and clustered)
5. Dashboard - Build a dashboard in Google Data studio to visualize the results
 
## Set up Infrastructure using Terraform
- Set up Google Cloud Platform project following these instructions [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md#initial-setup)
- The [terraform file](terraform/main.tf) sets up Airflow using Google Cloud Composer, as well as BigQuery.
Add your GCP Project ID to the commands below or add a default value in the project variable in `variables.tf`.
```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate) (navigate to the terraform directory first)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```

```shell
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```

- Upload the DAGs from `airflow/dags` into the dags-folder Composer created in GCS. 
Go to https://console.cloud.google.com/storage/browser?project=[YOUR_PROJECT_NAME]. Choose created bucket (e.g. my bucket us-central1-crime-de-capsto-8ccf2137-bucket) and copy the DAGs from airflow/dags into to folder /dags. Optionally one could automate previous manual uploading by syncing /dags folder with a repository. Instructions [here](https://engineering.adwerx.com/sync-a-github-repo-to-your-gcp-composer-airflow-dags-folder-2b87eb065915).
- Follow https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md
instructions to set up a dbt project. Then deploy a dbt jobs to transform the data in BigQuery.
- To visualise the data, Data Studio was used, utilizing the two views created in the dbt core model. 
The final Data Studio dashboard can be found here https://lookerstudio.google.com/reporting/39712dcc-5d57-4e2b-9d66-ca9623e7f59b or here https://github.com/ISazhyna/capstone_data_engineering/blob/main/DE_capstone.pdf.

## Project Description
### 1. Data Ingestion

**Data Source:**
Crime reports updated once a week from https://data.austintexas.gov/Public-Safety/Crime-Reports/fdj4-gpfu . Weather dataset uploaded one time from https://www.visualcrossing.com/weather/weather-data-services.
**Airflow DAGs for data ingestion:**
There are two different kinds of dags.
1. The first type of dags are with bash operator, which downloads the dataset from url directly into the Composer's data folder. Afterwards it is accessible by other dags.
2. The second type of dags use GoogleCloudStorageToBigQueryOperator, which takes the csv file from a source folder and bring it to the tables in BigQuery, using a specified schema. 
### 2. Data Warehouse
We're using BigQuery as a Data Warehouse in Google Cloud Platform. The terraform scripts run in the 
setup step of this project already created the necessary dataset, called `austin_crime`.

In this database both raw and transformed data are stored, which then could be used for analytics and visualisations.

### 3. Transformations using dbt

The dbt repository for the data transformations can be found https://github.com/ISazhyna/dbt_capstone_data_engineering.

### 4. Dashboard

A pdf of the final dashboard can be found https://github.com/ISazhyna/capstone_data_engineering/blob/main/DE_capstone.pdf. As we can see there is no significant affect from temperature to the crime rates (even for the hottest days during long time of Texas summer).

