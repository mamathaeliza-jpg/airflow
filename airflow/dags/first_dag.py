
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="first_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
)
def etl_pipeline():

    @task
    def extract():
        print("Extracting data")

    @task
    def transform():
        print("Transforming data")

    @task
    def load():
        print("Loading data")

    extract() >> transform() >> load()


etl_pipeline()
