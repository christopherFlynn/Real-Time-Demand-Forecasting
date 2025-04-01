# backfill_orders.py
from faker import Faker
import psycopg2
import uuid
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

REGIONS = ['Northeast', 'Midwest', 'South', 'West']
PRODUCT_IDS = ['SKU-001', 'SKU-002', 'SKU-003']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']

NUM_DAYS = 90
ORDERS_PER_DAY = 200  # You can increase this for more data

print(f"📦 Backfilling {NUM_DAYS} days x {ORDERS_PER_DAY} orders/day...")

for day_offset in range(NUM_DAYS):
    order_date = datetime.utcnow() - timedelta(days=day_offset)
    
    for _ in range(ORDERS_PER_DAY):
        order = {
            "timestamp": order_date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            ),
            "order_id": f"ORD-{uuid.uuid4().hex[:10]}",
            "product_id": random.choice(PRODUCT_IDS),
            "user_id": f"USER-{uuid.uuid4().hex[:8]}",
            "region": random.choice(REGIONS),
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(10, 100), 2),
            "device_type": random.choice(DEVICE_TYPES),
            "promo_applied": random.choice([True, False])
        }
        order["total_price"] = round(order["quantity"] * order["unit_price"], 2)

        cursor.execute("""
            INSERT INTO orders (
                order_id, timestamp, product_id, user_id, region, quantity,
                unit_price, total_price, device_type, promo_applied
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
cursor.close()
conn.close()
print("✅ Backfill complete.")
