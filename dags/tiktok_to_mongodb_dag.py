from datetime import datetime
import pandas as pd

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_FILE_PATH = "/opt/airflow/data/processed_tiktok_reviews.csv"
PROCESSED_DATASET = Dataset(f"file://{PROCESSED_FILE_PATH}")

with DAG(
    dag_id="load_tiktok_to_mongodb",
    start_date=datetime(2026, 1, 1),
    schedule=[PROCESSED_DATASET],
    catchup=False,
) as dag:

    @task(task_id="load_to_mongo")
    def type_casting():
        df = pd.read_csv(PROCESSED_FILE_PATH)
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
        df["created_date"] = df["created_date"].astype(object).where(df["created_date"].notna(), None)
        df["content"] = df["content"].fillna("").astype(str)

        hook = MongoHook(mongo_conn_id="mongo_default")
        collection = hook.get_collection(mongo_collection="tiktok_reviews", mongo_db="tiktok_db")
        collection.delete_many({})

        for start in range(0, len(df), 50000):
            batch = df.iloc[start : start + 50000].to_dict(orient="records")
            collection.insert_many(batch)
            print(f"Inserted {start + len(batch)} / {len(df)} records")

    type_casting()