import os
import pendulum

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

DATA_BUCKET = os.environ["GCS_BUCKET"] + "/data"

CRIME_GCS_FILES = "crime_austin.csv"
TEMP_22_GCS_FILES = "temp_austin_2022.csv"
DATASET = 'austin_crime'

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
        destination_project_dataset_table=f'{DATASET}.raw_data',
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


with DAG(
    dag_id="temp_gcs_to_bq_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['temp_gcs_bq'],
) as temp_dag:

    TEMP_22_GCS_to_BQ = GoogleCloudStorageToBigQueryOperator(
        task_id='temp_gcs_to_bq_dag',
        bucket=DATA_BUCKET,
        source_objects=[TEMP_22_GCS_FILES],
        destination_project_dataset_table=f'{DATASET}.raw_temp',
        schema_fields=[
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'datetime', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'tempmax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'tempmin', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'temp', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'feelslikemax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'feelslikemin', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'feelslike', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'dew', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'humidity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'precip', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'precipprob', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'precipcover', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'preciptype', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'snow', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'snowdepth', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'windgust', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'windspeed', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'winddir', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'sealevelpressure', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'cloudcover', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'visibility', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'solarradiation', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'solarenergy', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'uvindex', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'severerisk', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'sunrise', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'sunset', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'moonphase', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'conditions', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'icon', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'stations', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],


        skip_leading_rows=1,
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
    )

    TEMP_22_GCS_to_BQ