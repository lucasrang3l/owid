import os
from google.cloud import bigquery
from pandas import DataFrame
from airflow.models import Variable

PROJECT_NAME = Variable.get("PROJECT_NAME")
DATASET_NAME = Variable.get("DATASET_NAME")
TABLE_NAME = Variable.get("TABLE_NAME")


class Load:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe

    def load(self) -> None:
        client = bigquery.Client()

        table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            self.dataframe, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)
        print(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
        )
