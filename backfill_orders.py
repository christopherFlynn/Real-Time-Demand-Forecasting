# backfill_orders.py (supercharged version)
from faker import Faker
import psycopg2
import uuid
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time

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

# --- Configs ---
REGIONS = ['Northeast', 'Midwest', 'South', 'West']
PRODUCT_IDS = ['SKU-001', 'SKU-002', 'SKU-003']
CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home Goods', 'Toys']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
USER_SEGMENTS = ['new_user', 'returning', 'VIP', 'guest']

NUM_DAYS = 90
ORDERS_PER_DAY = 400  # Final count: NUM_DAYS * ORDERS_PER_DAY

print(f"📦 Starting backfill of {NUM_DAYS * ORDERS_PER_DAY:,} orders...")
start_time = time.time()

for day_offset in range(NUM_DAYS):
    order_date = datetime.utcnow() - timedelta(days=day_offset)
    orders = []
    print(f"📅 Inserting {ORDERS_PER_DAY} orders for {order_date.date()}... ", end='', flush=True)

    for _ in range(ORDERS_PER_DAY):
        random_time = order_date.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        hour = random_time.hour
        region = random.choice(REGIONS)
        product_id = random.choice(PRODUCT_IDS)
        category = random.choice(CATEGORIES)
        user_segment = random.choice(USER_SEGMENTS)
        device_type = random.choice(DEVICE_TYPES)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 100), 2)
        total_price = round(quantity * unit_price, 2)
        promo_applied = random.choice([True, False])
        user_id = f"USER-{uuid.uuid4().hex[:8]}"
        order_id = f"ORD-{uuid.uuid4().hex[:10]}"

        orders.append((
            order_id, random_time, product_id, category, user_id, user_segment,
            region, quantity, unit_price, total_price, device_type, promo_applied, hour
        ))

    cursor.executemany("""
        INSERT INTO orders (
            order_id, timestamp, product_id, category, user_id, user_segment,
            region, quantity, unit_price, total_price,
            device_type, promo_applied, hour
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, orders)
    print("✅")


# Final commit after all inserts
conn.commit()
cursor.close()
conn.close()

elapsed = time.time() - start_time
print(f"✅ Backfill complete in {elapsed:.2f} seconds.")
