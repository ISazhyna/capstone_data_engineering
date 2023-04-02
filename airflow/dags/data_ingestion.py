import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').add(days=-1),
    "depends_on_past": False,
    "retries": 1,
}

def download_data_to_gcs(
    dag,
    dataset,
    download_url,
    csv_file,
):
    """
    task definition:
    download the csv file to Google Cloud Storage (data folder inside Composer bucket)
    using a bash curl command
    """
    with dag:
        download_dataset_task = BashOperator(
            task_id=f"download_{dataset}_task",
            bash_command=f"curl -sSLf {download_url} > /home/airflow/gcs/data/{csv_file}"
        )

        download_dataset_task

"""
New data is added every week, so we overwrite the existed file accordingly.
"""
CRIME_URL = 'https://data.austintexas.gov/api/views/fdj4-gpfu/rows.csv?accessType=DOWNLOAD  '
CRIME_CSV_FILE = 'crime_austin.csv'

crime_data_dag = DAG(
    dag_id="crime_data_dag",
    schedule_interval="0 23 * * 1",
    start_date=pendulum.today('UTC').add(days=-1),
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['main_crime'],
)

download_data_to_gcs(
    dag=crime_data_dag,
    dataset='crime',
    download_url=CRIME_URL,
    csv_file=CRIME_CSV_FILE,
)
