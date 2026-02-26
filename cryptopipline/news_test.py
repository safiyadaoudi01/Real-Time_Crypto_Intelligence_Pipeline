import requests
import json
from datetime import datetime, UTC
from confluent_kafka import Producer

# ======================
# Kafka Config
# ======================

conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'MWFTUSHV2IGWKFLI',
    'sasl.password': 'cfltfv6EbMjSYbxyJTgAnJgCYggtoKuI7SC0aMkyXtlqgmwPqAOlGkMBvzqOyWzg'
}

producer = Producer(conf)
topic = "news_topic"

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)

# ======================
# News API
# ======================

NEWS_API_KEY = "9f37441490e54e06ba597fd2ba8a4bfb"

def fetch_news():
    url = "https://newsapi.org/v2/everything"

    params = {
        "q": '(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)',
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 5,
        "apiKey": NEWS_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] != "ok":
        print("API Error:", data)
        return

    for article in data["articles"]:
        news_event = {
            "symbol": "BTCUSDT",  # simple mapping for now
            "title": article["title"],
            "sentiment_score": 0.0,  # placeholder
            "event_time": datetime.now(UTC).isoformat()
        }

        producer.produce(
            topic,
            key="BTCUSDT",
            value=json.dumps(news_event),
            callback=delivery_report
        )

        producer.poll(0)
        print("Sent news to Kafka:", article["title"])

    producer.flush()

if __name__ == "__main__":
    fetch_news()