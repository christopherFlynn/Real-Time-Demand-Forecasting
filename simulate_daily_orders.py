# simulate_daily_orders.py (upgraded)
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

today = datetime.utcnow()
weekday = today.weekday()  # 0 = Monday, 6 = Sunday

# Simulate holiday effect
HOLIDAYS = [
    datetime(today.year, 1, 1).date(),   # New Year's
    datetime(today.year, 7, 4).date(),   # Independence Day
    datetime(today.year, 12, 25).date(), # Christmas
]
is_holiday = today.date() in HOLIDAYS
is_promo_day = today.day == 10  # Every 10th of the month is a fake sale

# Volume logic
if is_holiday:
    ORDERS_TO_GENERATE = 100
elif is_promo_day:
    ORDERS_TO_GENERATE = 1000
elif weekday >= 5:  # Saturday/Sunday
    ORDERS_TO_GENERATE = 300
else:
    ORDERS_TO_GENERATE = 500

print(f"📦 Simulating {ORDERS_TO_GENERATE} orders for {today.date()}")

for _ in range(ORDERS_TO_GENERATE):
    random_time = today.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )

    order = {
        "timestamp": random_time,
        "order_id": f"ORD-{uuid.uuid4().hex[:10]}",
        "product_id": random.choice(PRODUCT_IDS),
        "user_id": f"USER-{uuid.uuid4().hex[:8]}",
        "region": random.choice(REGIONS),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 100), 2),
        "device_type": random.choice(DEVICE_TYPES),
        "promo_applied": is_promo_day  # True only on 10th
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
print("✅ Fake orders generated.")
