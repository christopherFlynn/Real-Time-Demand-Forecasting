import os
import psycopg2
import pandas as pd
from prophet import Prophet
from datetime import datetime
from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage

load_dotenv()

# Email utility
def send_email_alert(subject, body):
    try:
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = os.getenv("EMAIL_USER")
        msg['To'] = os.getenv("EMAIL_RECEIVER")
        msg.set_content(body)

        with smtplib.SMTP(os.getenv("EMAIL_HOST"), int(os.getenv("EMAIL_PORT"))) as smtp:
            smtp.starttls()
            smtp.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
            smtp.send_message(msg)
        print("📧 Email alert sent.")
    except Exception as e:
        print(f"⚠️ Failed to send email alert: {e}")

# Connect to DB
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

metrics_to_forecast = ['total_orders', 'total_revenue']
regions = ['Northeast', 'Midwest', 'South', 'West']
forecast_horizon = 7
forecast_summary = []

print("🧹 Cleaning up old avg_order_value forecasts...")
cursor.execute("DELETE FROM forecast_metrics WHERE metric = 'avg_order_value'")
conn.commit()

print("📈 Starting forecasting process...")
for region in regions:
    for metric_col in metrics_to_forecast:
        print(f"📍 Region: {region} | Metric: {metric_col}")

        cursor.execute(f"""
            SELECT date, {metric_col} FROM daily_metrics
            WHERE region = %s AND {metric_col} IS NOT NULL
            ORDER BY date
        """, (region,))
        
        rows = cursor.fetchall()
        if len(rows) < 7:
            print(f"⚠️ Not enough data to forecast {metric_col} for {region}. Skipping.")
            continue

        df = pd.DataFrame(rows, columns=["ds", "y"])

        try:
            model = Prophet()
            model.fit(df)

            future = model.make_future_dataframe(periods=forecast_horizon)
            forecast = model.predict(future)
            forecast_trimmed = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(forecast_horizon)
        except Exception as e:
            print(f"❌ Forecast failed for {region} - {metric_col}: {e}")
            continue

        summary_lines = [f"📍 {region} – {metric_col}:"]
        for _, row in forecast_trimmed.iterrows():
            forecast_date = row['ds'].date()
            yhat = row['yhat']
            yhat_lower = row['yhat_lower']
            yhat_upper = row['yhat_upper']

            cursor.execute("""
                INSERT INTO forecast_metrics (
                    region, metric, forecast_date, forecast_value,
                    lower_bound, upper_bound, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (region, metric, forecast_date) DO UPDATE
                SET
                    forecast_value = EXCLUDED.forecast_value,
                    lower_bound = EXCLUDED.lower_bound,
                    upper_bound = EXCLUDED.upper_bound,
                    created_at = EXCLUDED.created_at
            """, (
                region, metric_col, forecast_date,
                round(yhat, 2),
                round(yhat_lower, 2),
                round(yhat_upper, 2),
                datetime.utcnow()
            ))

            summary_lines.append(
                f"{forecast_date} → {yhat:.2f} (CI: {yhat_lower:.2f} – {yhat_upper:.2f})"
            )

        forecast_summary.append("\n".join(summary_lines))

conn.commit()
cursor.close()
conn.close()

# Send daily email summary
if forecast_summary:
    full_summary = "\n\n".join(forecast_summary)
    send_email_alert(
        subject=f"✅ Forecast Summary – {datetime.utcnow().date()}",
        body=full_summary
    )

print("🏁 All forecasts complete. Email sent.")
