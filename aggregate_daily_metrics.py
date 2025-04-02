# aggregate_daily_metrics.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Connect to DB
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

print("📊 Aggregating daily metrics for all historical data...")

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
        CASE WHEN EXTRACT(DAY FROM DATE(timestamp)) = 10 THEN TRUE ELSE FALSE END AS is_promo_day
    FROM orders
    GROUP BY DATE(timestamp), region
    ON CONFLICT (date, region) DO UPDATE
    SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        avg_order_value = EXCLUDED.avg_order_value,
        is_promo_day = EXCLUDED.is_promo_day;
""")

conn.commit()
cursor.close()
conn.close()

print("✅ Daily metrics aggregation complete.")
