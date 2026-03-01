import requests
import json
from datetime import datetime, UTC

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer


# =========================
# 🔐 CONFIGURATION
# =========================

BOOTSTRAP = ".............."
KAFKA_KEY = ".............."
KAFKA_SECRET = "..................."

SR_URL = ".................."
SR_KEY = "................."
SR_SECRET = "............................."

NEWS_API_KEY = ".............................."

TOPIC = "news_avro_test"


# =========================
# 📦 Schema Registry Setup
# =========================

schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': f"{SR_KEY}:{SR_SECRET}"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


news_schema_str = """
{
  "type": "record",
  "name": "News",
  "namespace": "invistis.market",
  "fields": [
    { "name": "symbol", "type": "string" },
    { "name": "title", "type": "string" },
    { "name": "sentiment_score", "type": "double" },
    { "name": "event_time", "type": "string" }
  ]
}
"""


avro_serializer = AvroSerializer(
    schema_registry_client,
    news_schema_str
)


producer_conf = {
    'bootstrap.servers': BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_KEY,
    'sasl.password': KAFKA_SECRET,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)


# =========================
# 📰 Fetch Crypto News
# =========================

def fetch_news():
    url = "https://newsapi.org/v2/everything"

    params = {
        "q": '(bitcoin OR ethereum) AND (price OR trading OR market)',
        "sources": "reuters,bloomberg,cnbc,cointelegraph,financial-times",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 5,
        "apiKey": NEWS_API_KEY
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("HTTP Error:", response.status_code)
        print(response.text)
        return

    data = response.json()

    if data["status"] != "ok":
        print("API Error:", data)
        return

    print("\n===== Sending News to Kafka (AVRO) =====\n")

    for article in data["articles"]:

        title = article["title"] or ""
        description = article["description"] or ""

        # Very simple sentiment logic (demo)
        sentiment = 0.0
        text = (title + " " + description).lower()

        if "surge" in text or "rise" in text or "bull" in text:
            sentiment = 1.0
        elif "crash" in text or "fall" in text or "bear" in text:
            sentiment = -1.0

        # Detect symbol
        symbol = "BTCUSDT"
        if "ethereum" in text:
            symbol = "ETHUSDT"

        news_event = {
            "symbol": symbol,
            "title": title,
            "sentiment_score": float(sentiment),
            "event_time": datetime.now(UTC).isoformat()
        }

        producer.produce(
            topic=TOPIC,
            key=symbol,
            value=news_event
        )

        producer.poll(0)
        print("Sent AVRO news:", title)

    producer.flush()
    print("\nAll news sent successfully!\n")


# =========================
# ▶ MAIN
# =========================

if __name__ == "__main__":
    print("Connected AVRO News Producer...")
    fetch_news()