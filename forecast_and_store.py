# forecast_and_store.py
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prophet import Prophet
import smtplib
from email.message import EmailMessage
from datetime import datetime

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


load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()
forecast_summary = []

def forecast_and_store(region, metric_col):
    print(f"🔮 Forecasting {metric_col} for {region}")
    
    query = f"""
        SELECT date AS ds, {metric_col} AS y
        FROM daily_metrics
        WHERE region = %s AND {metric_col} IS NOT NULL
        ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(region,))
    if len(df) < 10:
        print(f"⚠️ Not enough data for {region} - {metric_col}")
        return

    try:
        model = Prophet()
        model.fit(df)
    except Exception as e:
        send_email_alert(
            subject=f"❌ Forecast Failed for {region} - {metric_col}",
            body=f"Exception:\n{str(e)}"
        )
        return


    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)

    for _, row in forecast.tail(7).iterrows():
        cursor.execute("""
            INSERT INTO forecast_metrics (region, metric, forecast_date, forecast_value, lower_bound, upper_bound)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (region, metric, forecast_date) DO UPDATE
            SET forecast_value = EXCLUDED.forecast_value,
                lower_bound = EXCLUDED.lower_bound,
                upper_bound = EXCLUDED.upper_bound,
                created_at = CURRENT_TIMESTAMP
        """, (
            region,
            metric_col,
            row['ds'].date(),
            row['yhat'],
            row['yhat_lower'],
            row['yhat_upper']
        ))
        # Check for Anomalies; send email
        yhat = row['yhat']
        if metric_col == 'total_orders' and (yhat < 10 or yhat > 5000):
            send_email_alert(
                subject=f"⚠️ Anomaly Detected: {metric_col} forecast for {region}",
                body=f"{yhat:.2f} predicted on {row['ds'].date()}"
            )
        # Store each summary
        forecast_summary.append(
            f"{row['ds'].date()} → {row['yhat']:.2f} (CI: {row['yhat_lower']:.2f} – {row['yhat_upper']:.2f})"
        )


    conn.commit()
    print(f"✅ Forecast saved for {region} - {metric_col}")

    if forecast_summary:
        summary_text = "\n".join(forecast_summary)
        send_email_alert(
            subject=f"✅ Forecast Complete – {datetime.utcnow().date()}",
            body=f"Forecast summary for {region} - {metric_col}:\n\n{summary_text}"
    )

# Forecast for each region + metric
regions = ['Northeast', 'Midwest', 'South', 'West']
metrics = ['total_orders', 'total_revenue']

for region in regions:
    for metric in metrics:
        forecast_and_store(region, metric)

cursor.close()
conn.close()