
from airflow.sdk import dag, task
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import requests
import pandas as pd
import os


@dag(
    dag_id="bank_of_england_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["banking", "finance", "etl"]
)
def bank_scraping_pipeline():

    @task
    def extract():

        url = (
            "https://www.bankofengland.co.uk/boeapps/"
            "database/_iadb-fromshowcolumns.asp"
        )

        params = {
            "csv.x": "yes",
            "Datefrom": "01/Jan/2020",
            "Dateto": "now",
            "SeriesCodes": "IUDBEDR",
            "CSVF": "TN",
            "UsingCodes": "Y",
            "VPD": "Y",
            "VFD": "N"
        }

        headers = {
            "User-Agent": "Airflow-Banking-ETL/1.0",
            "Accept": "text/csv"
        }

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=60
        )

        if response.status_code == 403:
            raise AirflowFailException(
                "Access forbidden by data provider"
            )

        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))

        print(f"Extracted {len(df)} records")

        return df.to_json(orient="records")

    @task
    def transform(raw_data):

        df = pd.read_json(StringIO(raw_data))

        df.columns = df.columns.str.strip()

        df = df.rename(columns={
            "DATE": "date",
            "IUDBEDR": "interest_rate"
        })

        df["date"] = pd.to_datetime(
            df["date"],
            format="mixed",
            dayfirst=True,
            errors="coerce"
        )

        df["interest_rate"] = pd.to_numeric(
            df["interest_rate"],
            errors="coerce"
        )

        df = df.dropna()
        df = df.drop_duplicates()
        df = df.sort_values("date", ascending=False)

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["bank"] = "Bank of England"

        return df.to_dict(orient="records")

    @task
    def validate(records):

        df = pd.DataFrame(records)

        assert not df.empty, "No data found"
        assert df["date"].is_unique
        assert not df.isnull().any().any()

        print("Data validation successful")

        return records

    @task
    def load(records):

        df = pd.DataFrame(records)

        output_dir = Path(os.environ["AIRFLOW_HOME"]) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / "bank_interest_rates.csv"

        df.to_csv(file_path, index=False)

        print(f"Saved {len(df)} records to {file_path}")

    raw_data = extract()
    clean_data = transform(raw_data)
    validated_data = validate(clean_data)
    load(validated_data)


bank_scraping_pipeline()
