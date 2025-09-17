import os
import pendulum
import logging
import pandas as pd
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.log.logging_mixin import LoggingMixin
from tempfile import NamedTemporaryFile
# --- Set Your Google Cloud and BigQuery Variables ---
# Make sure to replace these with your actual details

 # 1. Instantiate the BigQueryHook with your connection ID
hook = BigQueryHook(gcp_conn_id="gbq_npn")
        
# 2. Get the credentials object from the hook
credentials = hook.get_credentials()
#GCP_PROJECT_ID = hook.project_id         # Get project_id from the connection
GCP_PROJECT_ID = "ap-infotech-develop-iics" 
BQ_SOURCE_DATASET = "outbound"
BQ_SOURCE_TABLE = "vw_wd_philippines"
BQ_DESTINATION_DATASET = "outbound"
BQ_DESTINATION_TABLE = "vw_wd_philippines"


# --- Define Default Arguments for the DAG ---
default_args = {
    'owner': 'ETL',  # <-- Specify the author's name
    'email': ['sailesh.kumaryadav@anaplan.com'],  # <-- Add recipient(s) in a list
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="Philippines_DownloadEmp1",
    default_args=default_args,
    description="Reads data from a BigQuery table, transforms it, and appends to another table.",
    schedule='@once',  # This DAG is manually triggered
    start_date=pendulum.datetime(2025, 9, 15, tz="UTC"),
    catchup=False,
    tags=["bigquery", "etl", "pandas"],
)

def DownloadEmp():
    @task
    def extract_from_bigquery() -> pd.DataFrame:

        logger = LoggingMixin().log

        print(f"Reading from {BQ_SOURCE_DATASET}.{BQ_SOURCE_TABLE}...")
        
        # This is the query to select your data
        sql_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_SOURCE_DATASET}.{BQ_SOURCE_TABLE}`;"
        print(sql_query)
        # pandas-gbq handles the connection and data fetching
        df = pd.read_gbq(sql_query, project_id=GCP_PROJECT_ID)
        json_str = df.to_json(orient="records")
        logger.info("DF rows: %s", len(df))
        #logger.info("DF JSON length: %s", len(json_str))
        #logger.info("DF head:\n%s", df.head().to_string())
        #print(df.to_json(orient="records")) 
        #logger.info(df.to_json(orient="records"))
        #print(df.head(5))
        print(f"Sailesh :: Successfully extracted {len(df)} rows.")
        return df.encode('utf-8')
    @task
    def fetch_and_write_local(df):
        
        # atomic write: write to temp file then rename
        out_dir = "/home/airflowadmin/airflow/airflow_dags/Projects/WD_Philippines/"      # ensure this directory exists & is writable by worker
        #os.makedirs(out_dir, exist_ok=True)
        with NamedTemporaryFile("w", delete=False, dir=out_dir, suffix=".csv") as tmp:
            tmp_path = tmp.name
            df.to_csv(tmp, index=False)   # or df.head().to_csv(...) to limit
        final_path = os.path.join(out_dir, "DownloadEmp.csv")
        os.replace(tmp_path, final_path)  # atomic on most OSes

        return final_path
    # Set the task dependency
    extract_from_bigquery()
DownloadEmp()
