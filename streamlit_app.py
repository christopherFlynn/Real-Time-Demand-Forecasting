# streamlit_app.py
import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import date
import altair as alt

import streamlit as st

st.set_page_config(page_title="Demand Forecasting Dashboard", layout="wide")
st.title("📈 Real-Time Demand Forecasting")

# DB connection
conn = psycopg2.connect(
    dbname=st.secrets("DB_NAME"),
    user=st.secrets("DB_USER"),
    password=st.secrets("DB_PASSWORD"),
    host=st.secrets("DB_HOST"),
    port=st.secrets("DB_PORT")
)

# UI Controls
regions = ['Northeast', 'Midwest', 'South', 'West']
region = st.selectbox("Select Region", regions)

metrics = ['total_orders', 'total_revenue', 'avg_order_value']
metric = st.radio("Select Metric", metrics)

# Query forecast data (only for supported metrics)
if metric in ['total_orders', 'total_revenue']:
    query_forecast = """
    SELECT forecast_date, forecast_value, lower_bound, upper_bound
    FROM forecast_metrics
    WHERE region = %s AND metric = %s
    ORDER BY forecast_date
    """
    df_forecast = pd.read_sql(query_forecast, conn, params=(region, metric))
    df_forecast['type'] = 'Forecast'
else:
    df_forecast = pd.DataFrame()

# Query historical data (includes promo flag)
query_history = f"""
SELECT date AS forecast_date, {metric} AS forecast_value, is_promo_day
FROM daily_metrics
WHERE region = %s AND {metric} IS NOT NULL
ORDER BY date
"""
df_history = pd.read_sql(query_history, conn, params=(region,))
df_history['type'] = 'Historical'

# Highlight promo days on the chart
promo_dates = df_history[df_history['is_promo_day'] == True]['forecast_date'].tolist()


conn.close()

# Combine and chart
df_combined = pd.concat([df_history, df_forecast], ignore_index=True)

st.subheader(f"{metric.replace('_', ' ').title()} - {region}")


if df_combined.empty:
    st.warning("Not enough data to show chart.")
else:
    base = alt.Chart(df_combined).mark_line().encode(
        x='forecast_date:T',
        y='forecast_value:Q',
        color='type:N'
    )

    promo_overlay = alt.Chart(pd.DataFrame({'promo_date': promo_dates})).mark_rule(
        color='orange',
        strokeDash=[5, 5]
    ).encode(
        x='promo_date:T'
    )

    st.altair_chart(base + promo_overlay, use_container_width=True)


# Optional forecast details
if not df_forecast.empty:
    with st.expander("View Forecast Table with Confidence Intervals"):
        st.dataframe(df_forecast)

