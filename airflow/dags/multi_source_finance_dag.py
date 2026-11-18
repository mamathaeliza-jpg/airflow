
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import os


@dag(
    dag_id="multi_source_finance_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["finance", "multi-source", "etl"]
)
def finance_pipeline():

    @task
    def start():
        print("Financial data pipeline started")

    # SOURCE 1: European Central Bank
    @task
    def extract_ecb():

        url = (
            "https://www.ecb.europa.eu/stats/"
            "eurofxref/eurofxref-daily.xml"
        )

        response = requests.get(url, timeout=60)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        records = []

        for item in root.iter():

            if "currency" in item.attrib:

                records.append({
                    "currency": item.attrib["currency"],
                    "exchange_rate": float(item.attrib["rate"]),
                    "base_currency": "EUR"
                })

        print(f"ECB records: {len(records)}")

        return records

    # SOURCE 2: Federal Reserve
    @task
    def extract_fred():

        url = (
            "https://fred.stlouisfed.org/graph/"
            "fredgraph.csv?id=FEDFUNDS&cosd=2020-01-01"
        )

        response = requests.get(url, timeout=60)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))

        df = df.dropna()

        print(f"FRED records: {len(df)}")

        return df.to_dict(orient="records")

    # SOURCE 3: World Bank
    @task
    def extract_world_bank():

        url = (
            "https://api.worldbank.org/v2/country/GBR/"
            "indicator/FP.CPI.TOTL.ZG"
        )

        response = requests.get(
            url,
            params={
                "format": "json",
                "per_page": 100
            },
            timeout=60
        )

        response.raise_for_status()

        data = response.json()

        records = []

        for item in data[1]:

            if item["value"] is not None:

                records.append({
                    "country": "United Kingdom",
                    "year": item["date"],
                    "inflation_rate": item["value"]
                })

        print(f"World Bank records: {len(records)}")

        return records

    # TRANSFORM
    @task
    def transform(ecb, fred, world_bank):

        datasets = {
            "ecb_exchange_rates": ecb,
            "fred_interest_rates": fred,
            "world_bank_inflation": world_bank
        }

        cleaned = {}

        for name, records in datasets.items():

            df = pd.DataFrame(records)

            df = df.dropna().drop_duplicates()

            cleaned[name] = df.to_dict(orient="records")

        print("All three datasets transformed")

        return cleaned

    # DATA QUALITY
    @task
    def validate(datasets):

        for name, records in datasets.items():

            if not records:
                raise ValueError(f"{name} is empty")

            print(f"{name}: Validation passed")

        return datasets

    # LOAD
    @task
    def load(datasets):

        output_dir = (
            Path(os.environ["AIRFLOW_HOME"])
            / "output"
            / "finance"
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        combined = []

        for name, records in datasets.items():

            df = pd.DataFrame(records)

            file_path = output_dir / f"{name}.csv"

            df.to_csv(file_path, index=False)

            df["source"] = name
            combined.append(df)

            print(f"Saved: {file_path}")

        combined_df = pd.concat(
            combined,
            ignore_index=True
        )

        combined_df.to_csv(
            output_dir / "combined_financial_data.csv",
            index=False
        )

    @task
    def finish():
        print("Financial data pipeline completed")

    # TASK DEPENDENCIES
    start_task = start()

    ecb = extract_ecb()
    fred = extract_fred()
    world_bank = extract_world_bank()

    start_task >> [ecb, fred, world_bank]

    cleaned = transform(ecb, fred, world_bank)

    validated = validate(cleaned)

    loaded = load(validated)

    loaded >> finish()


finance_pipeline()
