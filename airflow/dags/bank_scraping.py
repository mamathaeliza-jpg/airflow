
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
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
    tags=["banking", "web-scraping", "etl"]
)
def bank_scraping_pipeline():

    @task
    def extract():

        url = (
            "https://www.bankofengland.co.uk"
            "/boeapps/database/Bank-Rate.asp"
        )

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        records = []

        for row in soup.find_all("tr"):

            columns = row.find_all("td")

            if len(columns) == 2:

                date = columns[0].get_text(strip=True)
                rate = columns[1].get_text(strip=True)

                records.append({
                    "date": date,
                    "interest_rate": rate
                })

        if not records:
            raise ValueError("No banking data extracted")

        print(f"Extracted {len(records)} records")

        return records

    @task
    def transform(records):

        df = pd.DataFrame(records)

        df["date"] = pd.to_datetime(
            df["date"],
            format="%d %b %y",
            errors="coerce"
        )

        # Correct historical two-digit years
        future = df["date"] > pd.Timestamp.today()

        df.loc[future, "date"] = (
            df.loc[future, "date"]
            - pd.DateOffset(years=100)
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

        print(df.head())

        return df.to_dict(orient="records")

    @task
    def validate(records):

        df = pd.DataFrame(records)

        assert not df.empty, "No banking records"
        assert not df.isnull().any().any()
        assert df["date"].is_unique

        print("Data quality validation successful")

        return records

    @task
    def load(records):

        df = pd.DataFrame(records)

        output_dir = Path(
            os.environ["AIRFLOW_HOME"]
        ) / "output"

        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / "bank_interest_rates.csv"

        df.to_csv(file_path, index=False)

        print(f"Banking data saved to {file_path}")
        print(f"Total records: {len(df)}")

    raw_data = extract()
    clean_data = transform(raw_data)
    validated_data = validate(clean_data)
    load(validated_data)


bank_scraping_pipeline()
