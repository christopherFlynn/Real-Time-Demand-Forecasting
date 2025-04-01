# kafka_consumer.py
import json
import os
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

# Load DB credentials from .env
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# Kafka consumer config
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['orders'])

def insert_order(order):
    cursor.execute("""
        INSERT INTO orders (
            order_id, timestamp, product_id, user_id, region, quantity,
            unit_price, total_price, device_type, promo_applied
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        order["order_id"],
        order["timestamp"],
        order["product_id"],
        order["user_id"],
        order["region"],
        order["quantity"],
        order["unit_price"],
        order["total_price"],
        order["device_type"],
        order["promo_applied"]
    ))
    conn.commit()

print("📥 Kafka consumer started...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Consumer error:", msg.error())
            continue

        try:
            order = json.loads(msg.value().decode('utf-8'))
            insert_order(order)
            print(f"⬇️  Inserted order {order['order_id']}")
        except Exception as e:
            print("⚠️ Failed to insert order:", e)

except KeyboardInterrupt:
    print("🛑 Consumer stopped.")

finally:
    consumer.close()
    cursor.close()
    conn.close()
