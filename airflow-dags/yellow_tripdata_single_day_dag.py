import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)

import datetime as dt
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "de-bootcamp-dipankar")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "test-buck-dip")
path_to_local_home = "/home/airflow/gcs/data/"
DATASET_NAME = os.environ.get("GCP_DATASET_NAME", "ny_trips_from_dag")
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", "trips_data_all")


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

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def csv_to_parquet(local_file, output_file):
    df = pv.read_csv(local_file)
    pq.write_table(df, output_file)


default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2019, 1, 1),
    "depends_on_past": True,
    "retries": 1,
    "catchup": False,
}

with DAG(
        dag_id="yellow_taxi_trip_data_one_run",
        schedule_interval="@once",
        default_args=default_args,
        catchup=True,
        max_active_runs=1,
        tags=["example"],
) as dag:
    URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"
    URL_TEMPLATE = (
            URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
    )
    OUTPUT_FILE_TEMPLATE = (
            path_to_local_home
            + "/output_yellow_taxi_tripdate{{ execution_date.strftime('%Y-%m') }}.csv"
    )
    OUTPUT_FILE_TEMPLATE_PARQUET = (
            path_to_local_home
            + "/output_yellow_taxi_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"
    )
    FILE_NAME = OUTPUT_FILE_TEMPLATE_PARQUET.split("/")[-1]
    TABLE_NAME_TEMPLATE = "yellow_taxi_trip_date_{{ execution_date.strftime('%Y_%m') }}"
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"""curl -sSL {URL_TEMPLATE} >{OUTPUT_FILE_TEMPLATE}""",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{FILE_NAME}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE_PARQUET}",
        },
    )

    csv_to_parquet_task = PythonOperator(
        task_id="csv_to_parquet",
        python_callable=csv_to_parquet,
        op_kwargs={
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
            "output_file": f"{OUTPUT_FILE_TEMPLATE_PARQUET}",
        },
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset", dataset_id=DATASET_NAME, location="europe-west6"
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=f"{BUCKET}",
        source_objects=[f"raw/{FILE_NAME}"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME_TEMPLATE}",
        source_format="parquet"
        # table_resource={
        #     "tableReference": {
        #         "projectId": PROJECT_ID,
        #         "datasetId": DATASET_NAME,
        #         "tableId": "external_table",
        #     },
        #
        #     "externalDataConfiguration": {
        #         "bucket": BUCKET,
        #         "sourceFormat": "parquet",
        #         "sourceUris": f"{BUCKET}/raw/{datasset_transformed}"
        #     },
        # },
    )

    remove_local_dataset_task = BashOperator(
        task_id="remove_local_dataset_task",
        bash_command=f"rm -r {path_to_local_home}/*",
    )

    (
            download_dataset_task
            >> csv_to_parquet_task
            >> local_to_gcs_task
            >> create_dataset
            >> create_external_table
            >> remove_local_dataset_task
    )
