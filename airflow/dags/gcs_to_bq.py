import os
import pendulum

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

DATA_BUCKET = os.environ["GCS_BUCKET"] + "/data"

CRIME_GCS_FILES = "crime_austin.csv"
DATASET = 'austin_crime_data'

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').add(days=-1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="crime_gcs_to_bq_dag",
    schedule_interval="0 6 30 * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['crime_gcs_bq'],
) as crime_dag:

    CRIME_GCS_to_BQ = GoogleCloudStorageToBigQueryOperator(
        task_id='crime_gcs_to_bq',
        bucket=DATA_BUCKET,
        source_objects=[CRIME_GCS_FILES],
        destination_project_dataset_table=f'{DATASET}.austin_crime_data',
        schema_fields=[
            {'name': 'incident_report_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'crime_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ucr_code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'family_violence', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'occ_date_time', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'occ_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'occ_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'rep_date_time', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'rep_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'rep_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'location_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'zip_code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'council_district', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'sector', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'district', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pra', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'census_tract', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'clearance_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'clearance_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ucr_category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'category_description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'x_coordinate', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'y_coordinate', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'latitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],



        skip_leading_rows=1,
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
    )

    CRIME_GCS_to_BQ
