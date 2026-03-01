import json
import websocket
from datetime import datetime, UTC

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# =========================
# 🔐 CONFIGURATION
# =========================

BOOTSTRAP = "......."
KAFKA_KEY = "..........."
KAFKA_SECRET = "................."

SR_URL = ".............."
SR_KEY = ".............."
SR_SECRET = "..................."

TOPIC = "trades_avro_test"


# =========================
# 📦 Schema Registry Client
# =========================

schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': f"{SR_KEY}:{SR_SECRET}"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


trade_schema_str = """
{
  "type": "record",
  "name": "Trade",
  "namespace": "invistis.market",
  "fields": [
    { "name": "symbol", "type": "string" },
    { "name": "price", "type": "double" },
    { "name": "quantity", "type": "double" },
    { "name": "event_time", "type": "string" }
  ]
}
"""


avro_serializer = AvroSerializer(
    schema_registry_client,
    trade_schema_str
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
# 📡 Binance WebSocket
# =========================

SYMBOLS = ["btcusdt", "ethusdt"]
stream = "/".join([f"{s}@trade" for s in SYMBOLS])
socket = f"wss://stream.binance.com:9443/stream?streams={stream}"

def on_open(ws):
    print("Connected AVRO producer...")

def on_message(ws, message):
    msg = json.loads(message)
    data = msg["data"]

    trade_event = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "event_time": datetime.now(UTC).isoformat()
    }

    producer.produce(
        topic=TOPIC,
        key=data["s"],
        value=trade_event
    )

    producer.poll(0)
    producer.flush()
    print("Sent AVRO trade:", trade_event["symbol"])


def on_error(ws, error):
    print("WebSocket Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed. Reconnecting...")

def run_ws():
    ws = websocket.WebSocketApp(
        socket,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()


while True:
    run_ws()