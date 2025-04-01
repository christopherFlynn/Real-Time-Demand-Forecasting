# aggregate_daily_metrics.py
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

today = datetime.utcnow().date()
is_promo = today.day == 10

# Aggregate daily metrics from orders
cursor.execute("""
    INSERT INTO daily_metrics (
        date, region, total_orders, total_revenue, avg_order_value, is_promo_day
    )
    SELECT
        DATE(timestamp) AS date,
        region,
        COUNT(*) AS total_orders,
        SUM(total_price) AS total_revenue,
        AVG(total_price) AS avg_order_value,
        %s AS is_promo_day
    FROM orders
    WHERE DATE(timestamp) = %s
    GROUP BY DATE(timestamp), region
    ON CONFLICT (date, region) DO UPDATE
    SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        avg_order_value = EXCLUDED.avg_order_value,
        is_promo_day = EXCLUDED.is_promo_day;
""", (is_promo, today))


conn.commit()
cursor.close()
conn.close()

print("✅ Daily metrics aggregated.")
