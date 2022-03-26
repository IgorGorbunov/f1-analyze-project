import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

zip_file = "f1db_csv.zip"
dataset_url = f"http://ergast.com/downloads/f1db_csv.zip"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
csv_folder_name = "csv_data"
parquet_file = zip_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    head_table = pd.read_csv(src_file, header=None, nrows=1)
    table = pd.read_csv(src_file, header=None, skiprows=1)
    if len(head_table.columns) != len(table.columns):
        logging.warning("Header columns number doesn`t equal table column number!")
        table = pd.read_csv(src_file, names=head_table.columns)
        table.columns = table.columns.astype(str)
        table.to_parquet(dest_file)
    else:
        table = pv.read_csv(src_file)
        pq.write_table(table, dest_file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{zip_file}"
    )

    unzip_files = BashOperator(
        task_id="unzip_files",
        bash_command=f"unzip {path_to_local_home}/{zip_file} -d {path_to_local_home}/{csv_folder_name}"
    )

    format_to_parquet_circuits = PythonOperator(
        task_id="format_to_parquet_circuits",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/circuits.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/circuits.parquet",
        },
    )
    
    format_to_parquet_constructor_results = PythonOperator(
        task_id="format_to_parquet_constructor_results",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/constructor_results.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/constructor_results.parquet",
        },
    )
    
    format_to_parquet_constructor_standings = PythonOperator(
        task_id="format_to_parquet_constructor_standings",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/constructor_standings.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/constructor_standings.parquet",
        },
    )
    
    format_to_parquet_constructors = PythonOperator(
        task_id="format_to_parquet_constructors",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/constructors.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/constructors.parquet",
        },
    )
    
    format_to_parquet_driver_standings = PythonOperator(
        task_id="format_to_parquet_driver_standings",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/driver_standings.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/driver_standings.parquet",
        },
    )
    
    format_to_parquet_drivers = PythonOperator(
        task_id="format_to_parquet_drivers",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/drivers.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/drivers.parquet",
        },
    )
    
    format_to_parquet_lap_times = PythonOperator(
        task_id="format_to_parquet_lap_times",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/lap_times.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/lap_times.parquet",
        },
    )
    
    format_to_parquet_pit_stops = PythonOperator(
        task_id="format_to_parquet_pit_stops",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/pit_stops.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/pit_stops.parquet",
        },
    )
    
    format_to_parquet_qualifying = PythonOperator(
        task_id="format_to_parquet_qualifying",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/qualifying.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/qualifying.parquet",
        },
    )
    
    format_to_parquet_races = PythonOperator(
        task_id="format_to_parquet_races",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/races.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/races.parquet",
        },
    )
    
    format_to_parquet_results = PythonOperator(
        task_id="format_to_parquet_results",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/results.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/results.parquet",
        },
    )
    
    format_to_parquet_seasons = PythonOperator(
        task_id="format_to_parquet_seasons",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/seasons.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/seasons.parquet",
        },
    )
    
    format_to_parquet_sprint_results = PythonOperator(
        task_id="format_to_parquet_sprint_results",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/sprint_results.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/sprint_results.parquet",
        },
    )
    
    format_to_parquet_status = PythonOperator(
        task_id="format_to_parquet_status",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_folder_name}/status.csv",
            "dest_file": f"{path_to_local_home}/{csv_folder_name}/status.parquet",
        },
    )

    local_to_gcs_circuits = PythonOperator(
        task_id="local_to_gcs_circuits",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/circuits.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/circuits.parquet",
        },
    )

    local_to_gcs_constructor_results = PythonOperator(
        task_id="local_to_gcs_constructor_results",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/constructor_results.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/constructor_results.parquet",
        },
    )

    local_to_gcs_constructor_standings = PythonOperator(
        task_id="local_to_gcs_constructor_standings",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/constructor_standings.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/constructor_standings.parquet",
        },
    )

    local_to_gcs_constructors = PythonOperator(
        task_id="local_to_gcs_constructors",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/constructors.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/constructors.parquet",
        },
    )

    local_to_gcs_driver_standings = PythonOperator(
        task_id="local_to_gcs_driver_standings",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/driver_standings.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/driver_standings.parquet",
        },
    )

    local_to_gcs_drivers = PythonOperator(
        task_id="local_to_gcs_drivers",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/drivers.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/drivers.parquet",
        },
    )

    local_to_gcs_lap_times = PythonOperator(
        task_id="local_to_gcs_lap_times",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/lap_times.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/lap_times.parquet",
        },
    )

    local_to_gcs_pit_stops = PythonOperator(
        task_id="local_to_gcs_pit_stops",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/pit_stops.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/pit_stops.parquet",
        },
    )

    local_to_gcs_qualifying = PythonOperator(
        task_id="local_to_gcs_qualifying",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/qualifying.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/qualifying.parquet",
        },
    )

    local_to_gcs_races = PythonOperator(
        task_id="local_to_gcs_races",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/races.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/races.parquet",
        },
    )

    local_to_gcs_results = PythonOperator(
        task_id="local_to_gcs_results",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/results.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/results.parquet",
        },
    )

    local_to_gcs_seasons = PythonOperator(
        task_id="local_to_gcs_seasons",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/seasons.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/seasons.parquet",
        },
    )

    local_to_gcs_sprint_results = PythonOperator(
        task_id="local_to_gcs_sprint_results",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/sprint_results.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/sprint_results.parquet",
        },
    )

    local_to_gcs_status = PythonOperator(
        task_id="local_to_gcs_status",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/status.parquet",
            "local_file": f"{path_to_local_home}/{csv_folder_name}/status.parquet",
        },
    )

    bigquery_external_table_circuits = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_circuits",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/circuits.parquet"],
            },
        },
    )

    bigquery_external_table_constructor_results = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_constructor_results",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/constructor_results.parquet"],
            },
        },
    )

    bigquery_external_table_constructor_standings = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_constructor_standings",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/constructor_standings.parquet"],
            },
        },
    )

    bigquery_external_table_constructors = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_constructors",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/constructors.parquet"],
            },
        },
    )

    bigquery_external_table_driver_standings = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_driver_standings",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/driver_standings.parquet"],
            },
        },
    )

    bigquery_external_table_drivers = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_drivers",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/drivers.parquet"],
            },
        },
    )

    bigquery_external_table_lap_times = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_lap_times",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/lap_times.parquet"],
            },
        },
    )

    bigquery_external_table_pit_stops = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_pit_stops",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/pit_stops.parquet"],
            },
        },
    )

    bigquery_external_table_qualifying = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_qualifying",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/qualifying.parquet"],
            },
        },
    )

    bigquery_external_table_races = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_races",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/races.parquet"],
            },
        },
    )

    bigquery_external_table_results = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_results",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/results.parquet"],
            },
        },
    )

    bigquery_external_table_seasons = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_seasons",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/seasons.parquet"],
            },
        },
    )

    bigquery_external_table_sprint_results = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_sprint_results",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/sprint_results.parquet"],
            },
        },
    )

    bigquery_external_table_status = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_status",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/status.parquet"],
            },
        },
    )

    
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -r {path_to_local_home}/ {zip_file}"
    )


    download_dataset_task >> unzip_files
    unzip_files >> format_to_parquet_circuits >> local_to_gcs_circuits >> bigquery_external_table_circuits >> cleanup
    unzip_files >> format_to_parquet_constructor_results >> local_to_gcs_constructor_results >> bigquery_external_table_constructor_results >> cleanup
    unzip_files >> format_to_parquet_constructor_standings >> local_to_gcs_constructor_standings >> bigquery_external_table_constructor_standings >> cleanup
    unzip_files >> format_to_parquet_constructors >> local_to_gcs_constructors >> bigquery_external_table_constructors >> cleanup
    unzip_files >> format_to_parquet_driver_standings >> local_to_gcs_driver_standings >> bigquery_external_table_driver_standings >> cleanup
    unzip_files >> format_to_parquet_drivers >> local_to_gcs_drivers >> bigquery_external_table_drivers >> cleanup
    unzip_files >> format_to_parquet_lap_times >> local_to_gcs_lap_times >> bigquery_external_table_lap_times >> cleanup
    unzip_files >> format_to_parquet_pit_stops >> local_to_gcs_pit_stops >> bigquery_external_table_pit_stops >> cleanup
    unzip_files >> format_to_parquet_qualifying >> local_to_gcs_qualifying >> bigquery_external_table_qualifying >> cleanup
    unzip_files >> format_to_parquet_races >> local_to_gcs_races >> bigquery_external_table_races >> cleanup
    unzip_files >> format_to_parquet_results >> local_to_gcs_results >> bigquery_external_table_results >> cleanup
    unzip_files >> format_to_parquet_seasons >> local_to_gcs_seasons >> bigquery_external_table_seasons >> cleanup
    unzip_files >> format_to_parquet_sprint_results >> local_to_gcs_sprint_results >> bigquery_external_table_sprint_results >> cleanup
    unzip_files >> format_to_parquet_status >> local_to_gcs_status >> bigquery_external_table_status >> cleanup
    
   
