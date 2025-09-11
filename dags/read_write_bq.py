import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- Set Your Google Cloud and BigQuery Variables ---
# Make sure to replace these with your actual details

 # 1. Instantiate the BigQueryHook with your connection ID
hook = BigQueryHook(gcp_conn_id="gbq_npn")
        
# 2. Get the credentials object from the hook
credentials = hook.get_credentials()
GCP_PROJECT_ID = hook.project_id         # Get project_id from the connection
BQ_SOURCE_DATASET = "outbound"
BQ_SOURCE_TABLE = "okta_transaction_data"
BQ_DESTINATION_DATASET = "outbound"
BQ_DESTINATION_TABLE = "okta_dummy_test"

@dag(
    dag_id="read_write_bq",
    description="Reads data from a BigQuery table, transforms it, and appends to another table.",
    schedule=None,  # This DAG is manually triggered
    start_date=pendulum.datetime(2025, 9, 11, tz="UTC"),
    catchup=False,
    tags=["bigquery", "etl", "pandas"],
)
def bq_etl_dag():
    """
    ### BigQuery ETL DAG

    This DAG demonstrates a simple ETL process:
    1.  **Extract:** Reads data from a source table in BigQuery into a Pandas DataFrame.
    2.  **Transform:** Performs a simple transformation on the DataFrame.
    3.  **Load:** Writes the transformed data to a destination table. If the table
        does not exist, it is created. If it already exists, the new data is appended.
    """

    @task
    def extract_from_bigquery() -> pd.DataFrame:
        """
        Reads data from the source BigQuery table and returns it as a Pandas DataFrame.
        """


        print(f"Reading from {BQ_SOURCE_DATASET}.{BQ_SOURCE_TABLE}...")
        
        # This is the query to select your data
        sql_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_SOURCE_DATASET}.{BQ_SOURCE_TABLE}`;"
        
        # pandas-gbq handles the connection and data fetching
        df = pd.read_gbq(sql_query, project_id=GCP_PROJECT_ID)
        
        print(f"Successfully extracted {len(df)} rows.")
        return df

    @task
    def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Receives a DataFrame, performs transformations, and returns the modified DataFrame.
        """
        if df.empty:
            print("Received an empty DataFrame. Skipping transformation.")
            return df
            
        print("Transforming data...")
        
        # --- Your Transformation Logic Goes Here ---
        # Example 1: Add a new column with a static value or calculated value
        df['processing_timestamp'] = pendulum.now("UTC")

        # --- End of Transformation Logic ---
            
        print("Transformation complete.")
        return df

    @task
    def load_to_bigquery(df: pd.DataFrame):
        """
        Writes the transformed DataFrame to the destination BigQuery table.
        It will create the table if it doesn't exist or append if it does.
        """
        if df.empty:
            print("Received an empty DataFrame. Skipping load process.")
            return

        destination_table_id = f"{BQ_DESTINATION_DATASET}.{BQ_DESTINATION_TABLE}"
        print(f"Loading data to {destination_table_id}...")

        # The `to_gbq` function handles the "create or append" logic automatically.
        # if_exists='append' is the key parameter here.
        df.to_gbq(
            destination_table=destination_table_id,
            project_id=GCP_PROJECT_ID,
            if_exists='append',  # Creates table if it doesn't exist, appends if it does
            chunksize=10000,    # Optional: for large dataframes, write in chunks
            progress_bar=False  # Set to False to avoid excessive logging in Airflow
        )
        
        print(f"Successfully loaded {len(df)} rows to {destination_table_id}.")

    # --- Define the DAG's Task Flow ---
    extracted_df = extract_from_bigquery()
    transformed_df = transform_dataframe(extracted_df)
    load_to_bigquery(transformed_df)

# Instantiate the DAG
bq_etl_dag()
