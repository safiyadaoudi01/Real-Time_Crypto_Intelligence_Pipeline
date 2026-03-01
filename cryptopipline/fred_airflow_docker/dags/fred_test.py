import os
import json
import requests
import pandas as pd
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import loads

# =========================
# CONFIGURATION
# =========================

FRED_API_KEY = "704fdac2c75a836581cb74008d766882"


TOPIC = "fred_topic"

SERIES_LIST = {
    "DFF": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE": "Unemployment Rate",
    "GDP": "Gross Domestic Product",
    "PCE": "Personal Consumption Expenditures",
    "M2SL": "M2 Money Stock",
    "PAYEMS": "All Employees, Total Nonfarm",
    "INDPRO": "Industrial Production Index",
    "FEDFUNDS": "Effective Federal Funds Rate",
    "TB3MS": "3-Month Treasury Bill Rate"
}

START_DATE = "2024-01-01"

value_schema_str = """
{
  "type": "record",
  "name": "FredIndicator",
  "fields": [
    {"name": "series_id", "type": "string"},
    {"name": "series_name", "type": "string"},
    {"name": "date", "type": "string"},
    {"name": "value", "type": ["null", "double"], "default": null}
  ]
}
"""

# =========================
# KAFKA PRODUCER
# =========================

producer_config = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "VKJVOV3DSM5P66ZV",
    "sasl.password": "cfltWr+sHC2rn8L5DdEo004npOSTOjMt7VB/0zdV7WsSUKOfNp0z0hY5qTUkJh2Q",

    "schema.registry.url": "https://psrc-1ynovvj.us-east-2.aws.confluent.cloud",
    "schema.registry.basic.auth.credentials.source": "USER_INFO",
    "schema.registry.basic.auth.user.info": "IDHJYE5DR23ABO6Z:cfltKaQE0uYvshOKyVlBVRqKIrOR9FzLMjbG/ld5Rlls3zA6a45qqKnC8ND3xRvA"
}


value_schema = loads(value_schema_str)

producer = AvroProducer(
    producer_config,
    default_value_schema=value_schema
)

def delivery_report(err, msg):
    """Callback appelé par Kafka après tentative d'envoi."""
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} | partition {msg.partition()}")


# =========================
# FRED FETCH
# =========================

def fetch_fred_series(series_id, series_name, start_date):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date
    }

    response = requests.get(url, params=params)
    data = response.json()
    
    # Check if observations exist
    observations = data.get("observations", [])
    if not observations:
        print(f"⚠ No data returned for {series_id}")
        return pd.DataFrame(columns=["series_id", "series_name", "date", "value"])
    
    df = pd.DataFrame(observations)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series_id"] = series_id
    df["series_name"] = series_name
    return df[["series_id", "series_name", "date", "value"]]


# =========================
# MAIN PIPELINE
# =========================

def run_and_produce():
    print("Fetching FRED data...")

    frames = []
    for series_id, name in SERIES_LIST.items():
        df = fetch_fred_series(series_id, name, START_DATE)
        frames.append(df)

    all_data = pd.concat(frames, ignore_index=True)

    print("Rows fetched:", len(all_data))

    if all_data.empty:
        print("No data to send → stopping")
        return

    print("Sending data to Kafka topic:", TOPIC)

    for _, row in all_data.iterrows():
        record = {
            "series_id": row["series_id"],
            "series_name": row["series_name"],
            "date": str(row["date"]),
            "value": None if pd.isna(row["value"]) else float(row["value"])
        }

        producer.produce(topic=TOPIC, value=record)

    producer.flush()
    print("✅ Pipeline finished successfully")


# =========================
# EXECUTION DIRECTE
# =========================

if __name__ == "__main__":
    run_and_produce()