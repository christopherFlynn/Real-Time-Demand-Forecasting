# forecast_orders.py
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prophet import Prophet

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# Pull one region to start
query = """
SELECT date AS ds, total_orders AS y
FROM daily_metrics
WHERE region = 'Northeast'
ORDER BY date;
"""

df = pd.read_sql(query, conn)
conn.close()

# Train model
model = Prophet()
model.fit(df)

# Make future dataframe
future = model.make_future_dataframe(periods=7)
forecast = model.predict(future)

# Show forecast
model.plot(forecast)
