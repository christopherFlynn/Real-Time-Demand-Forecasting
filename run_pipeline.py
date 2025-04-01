import subprocess

print("📦 Generating today's orders")
subprocess.run(["python", "simulate_daily_orders.py"])

print("📊 Aggregating daily metrics")
subprocess.run(["python", "aggregate_daily_metrics.py"])

print("🔮 Forecasting next 7 days")
subprocess.run(["python", "forecast_and_store.py"])

print("✅ Daily pipeline complete.")
