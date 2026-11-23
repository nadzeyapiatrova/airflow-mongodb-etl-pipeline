import os
import re
from datetime import datetime
import pandas as pd

from airflow import DAG, Dataset
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

RAW_FILE_PATH = "/opt/airflow/data/tiktok_google_play_reviews.csv"
PROCESSED_FILE_PATH = "/opt/airflow/data/processed_tiktok_reviews.csv"
PROCESSED_DATASET = Dataset(f"file://{PROCESSED_FILE_PATH}")

with DAG(
    dag_id = "process_data_tiktok", 
    start_date=datetime(2026, 1, 1), 
    schedule="@once", 
    catchup=False,
) as dag:
    
    wait_for_file = FileSensor(
        task_id="wait_for_file", 
        fs_conn_id="fs_default",
        filepath ="tiktok_google_play_reviews.csv",
        poke_interval=10,
        timeout=300,
        mode="poke",
    )

    @task.branch(task_id="check_if_file_empty")
    def check_file_empty() -> str:
        if os.path.getsize(RAW_FILE_PATH) == 0:
            return "log_empty_file"
        return "data_processing_group.replace_nulls"

    branch_task = check_file_empty()

    log_empty = BashOperator(
    task_id="log_empty_file",
    bash_command='echo "File is empty!"',
    )

    with TaskGroup(group_id="data_processing_group") as processing_group:

        @task(task_id="replace_nulls")
        def replace_null() -> str:
            df = pd.read_csv(RAW_FILE_PATH)
            df = df.fillna("-")
            step1_path = "/opt/airflow/data/step1_no_nulls.csv"
            df.to_csv(step1_path, index=False)
            return step1_path

        @task(task_id="sort_data")
        def sort_data(input_path: str) -> str:
            df = pd.read_csv(input_path)
            df["created_date"] = pd.to_datetime(df["at"], errors="coerce")
            df = df.sort_values(by="created_date", ascending=True)
            step2_path = "/opt/airflow/data/step2_sorted.csv"
            df.to_csv(step2_path, index=False)
            return step2_path

        @task(task_id="remove_characters", outlets=[PROCESSED_DATASET])
        def remove_characters(input_path: str) -> str:
            df = pd.read_csv(input_path)
            df["content"] = (
                df["content"]
                .astype(str)
                .apply(lambda text: re.sub(r"[^\w\s.,!?;:'\"()-]", "", text))
                .str.strip()
            )
            df.to_csv(PROCESSED_FILE_PATH, index=False)
            return PROCESSED_FILE_PATH

        s1 = replace_null()
        s2 = sort_data(s1)
        remove_characters(s2)

    wait_for_file >> branch_task
    branch_task >> Label("File is empty") >> log_empty
    branch_task >> Label("File has data") >> processing_group