
from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path
import pandas as pd
import os


@dag(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sales", "etl", "data-engineering"]
)
def sales_etl_pipeline():

    @task
    def extract():
        sales = [
            {"product": "Laptop", "quantity": 3, "price": 800},
            {"product": "Mobile", "quantity": 5, "price": 500},
            {"product": "Tablet", "quantity": 4, "price": 300},
            {"product": "Monitor", "quantity": 2, "price": 200}
        ]

        print("Sales data extracted successfully")
        return sales

    @task
    def transform(sales):
        df = pd.DataFrame(sales)

        df["total_revenue"] = df["quantity"] * df["price"]

        print(df)
        return df.to_dict(orient="records")

    @task
    def load(data):
        df = pd.DataFrame(data)

        output_dir = Path(os.environ["AIRFLOW_HOME"]) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / "sales_report.csv"

        df.to_csv(file_path, index=False)

        print(f"Data saved to {file_path}")
        return str(file_path)

    @task
    def validate(file_path):
        df = pd.read_csv(file_path)

        assert not df.empty, "Sales report is empty"
        assert df["total_revenue"].sum() > 0

        print("Data validation successful")
        print("Total revenue:", df["total_revenue"].sum())

    raw_data = extract()
    transformed_data = transform(raw_data)
    output_file = load(transformed_data)
    validate(output_file)


sales_etl_pipeline()
