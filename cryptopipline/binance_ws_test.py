import json
import websocket
from confluent_kafka import Producer
from datetime import datetime

# ===== Kafka Config =====
conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'VKJVOV3DSM5P66ZV',
    'sasl.password': 'cfltWr+sHC2rn8L5DdEo004npOSTOjMt7VB/0zdV7WsSUKOfNp0z0hY5qTUkJh2Q'
}

producer = Producer(conf)
topic = "trades_topic"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

# ===== Binance =====
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "adausdt"]

stream = "/".join([f"{s}@trade" for s in SYMBOLS])
socket = f"wss://stream.binance.com:9443/stream?streams={stream}"

def on_message(ws, message):
    msg = json.loads(message)
    data = msg["data"]
    trade_data = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "event_time": datetime.utcnow().isoformat()
    }

    producer.produce(
        topic,
        key=data["s"],
        value=json.dumps(trade_data),
        callback=delivery_report
    )
    producer.poll(0)

    print(f"Sent to Kafka: {trade_data['symbol']}")

def on_open(ws):
    print("Connected and streaming prices...")

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

ws = websocket.WebSocketApp(
    socket,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

ws.run_forever()