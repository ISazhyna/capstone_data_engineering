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
CRIME_URL = 'https://data.austintexas.gov/api/views/fdj4-gpfu/rows.csv?accessType=DOWNLOAD'
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


"""
Historical measured data of temperature in Austin 2022
"""

TEMP_22_URL='\"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=LS846U5QUC4XUPFSUSK44UR3T&taskId=eda09bd6d60dce86d4c043eb83e24ee4&zip=false\"'
TEMP_22_CSV_FILE='temp_austin_2022.csv'

temp_22_data_dag = DAG(
    dag_id="temp_22_data_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['temp_data'],
)

download_data_to_gcs(
    dag=temp_22_data_dag,
    dataset='crime',
    download_url=TEMP_22_URL,
    csv_file=TEMP_22_CSV_FILE,
)


"""
Historical measured data of temperature in Austin 2021
"""

TEMP_21_URL='\"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=LS846U5QUC4XUPFSUSK44UR3T&taskId=9ba67a57065456d5a95987d32495fb48&zip=false\"'
TEMP_21_CSV_FILE='temp_austin_2021.csv'

temp_21_data_dag = DAG(
    dag_id="temp_21_data_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['temp_data'],
)

download_data_to_gcs(
    dag=temp_21_data_dag,
    dataset='crime',
    download_url=TEMP_21_URL,
    csv_file=TEMP_21_CSV_FILE,
)