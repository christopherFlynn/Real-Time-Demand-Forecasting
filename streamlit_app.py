import os
import pandas as pd
import altair as alt
import streamlit as st
from supabase import create_client, Client
import datetime

# Initialize Supabase connection
@st.cache_resource
def init_connection():
    url: str = st.secrets["connections"]["supabase"]["SUPABASE_URL"]
    key: str = st.secrets["connections"]["supabase"]["SUPABASE_KEY"]
    return create_client(url, key)

supabase: Client = init_connection()

# Fetch forecasted metrics for a selected region and metric
def get_forecast(region, metric):
    if metric == "avg_order_value":
        response = supabase.table("forecast_metrics").select("region, metric, forecast_date, forecast_value").eq("region", region).in_("metric", ["total_orders", "total_revenue"]).execute()
        df_raw = pd.DataFrame(response.data)
        df_pivot = df_raw.pivot(index="forecast_date", columns="metric", values="forecast_value")
        df_pivot = df_pivot.dropna()
        df_pivot["forecast_value"] = df_pivot["total_revenue"] / df_pivot["total_orders"]
        df_pivot = df_pivot.reset_index()
        df_pivot["type"] = "Forecast"
        df_pivot["forecast_date"] = pd.to_datetime(df_pivot["forecast_date"])
        return df_pivot[["forecast_date", "forecast_value", "type"]]
    else:
        response = supabase.table("forecast_metrics").select("forecast_date, forecast_value").eq("region", region).eq("metric", metric).order("forecast_date").execute()
        df = pd.DataFrame(response.data)
        df["type"] = "Forecast"
        df["forecast_date"] = pd.to_datetime(df["forecast_date"])
        return df

# Fetch historical data based on selected filters
def get_history(region, metric, category, segment, hour, lookback_days):
    rows = []
    page_size = 1000
    start = 0
    while True:
        batch = supabase.table("orders")\
            .select("timestamp, region, category, user_segment, total_price")\
            .eq("region", region)\
            .range(start, start + page_size - 1)\
            .execute()
        rows.extend(batch.data)
        if len(batch.data) < page_size:
            break
        start += page_size

    response = type('obj', (object,), {'data': rows})
    df_orders = pd.DataFrame(response.data)

    if df_orders.empty:
        return pd.DataFrame()

    df_orders["timestamp"] = pd.to_datetime(df_orders["timestamp"])
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
    df_orders = df_orders[df_orders["timestamp"] >= cutoff_date]

    if category != "All":
        df_orders = df_orders[df_orders["category"] == category]
    if segment != "All":
        df_orders = df_orders[df_orders["user_segment"] == segment]
    if hour != "All":
        df_orders = df_orders[df_orders["timestamp"].dt.hour == int(hour)]

    df_orders["forecast_date"] = df_orders["timestamp"].dt.floor("D")

    if metric == "total_revenue":
        agg = df_orders.groupby("forecast_date")["total_price"].sum()
    elif metric == "total_orders":
        agg = df_orders.groupby("forecast_date")["total_price"].count()
    elif metric == "avg_order_value":
        agg = df_orders.groupby("forecast_date")["total_price"].mean()
    else:
        st.error(f"Unsupported metric: {metric}")
        st.stop()

    df_metrics = agg.reset_index().rename(columns={"total_price": "forecast_value"})
    df_metrics["type"] = "Historical"

    promo_resp = supabase.table("daily_metrics").select("date, region, is_promo_day").eq("region", region).execute()
    df_promo = pd.DataFrame(promo_resp.data)
    df_promo["date"] = pd.to_datetime(df_promo["date"]).dt.floor("D")

    df = df_metrics.merge(df_promo, left_on=["forecast_date"], right_on=["date"], how="left")
    return df.drop(columns="date")


# Sidebar filter controls
st.sidebar.header("📊 Filters")
region = st.sidebar.selectbox("Select Region", ['Northeast', 'Midwest', 'South', 'West'])
metric = st.sidebar.radio("Metric", ['total_orders', 'total_revenue', 'avg_order_value'])

lookback_days = st.sidebar.selectbox("Lookback Period", [7, 14, 30, 60, 90], index=2)

category_resp = supabase.table("orders").select("category").execute()
segment_resp = supabase.table("orders").select("user_segment").execute()
hour_resp = supabase.table("orders").select("hour").order("hour").execute()

categories = sorted({row["category"] for row in category_resp.data if row["category"]})
segments = sorted({row["user_segment"] for row in segment_resp.data if row["user_segment"]})
hours = sorted({row["hour"] for row in hour_resp.data if row["hour"] is not None})

selected_category = st.sidebar.selectbox("Product Category", ["All"] + categories)
selected_segment = st.sidebar.selectbox("User Segment", ["All"] + segments)
selected_hour = st.sidebar.selectbox("Hour of Day", ["All"] + [str(h) for h in hours])

# Pull and combine forecast and history data
df_forecast = get_forecast(region, metric)
df_history = get_history(region, metric, selected_category, selected_segment, selected_hour, lookback_days)
df_combined = pd.concat([df_forecast, df_history]) if not df_history.empty else df_forecast

promo_dates = df_history[df_history['is_promo_day'] == True]['forecast_date'].tolist() if not df_history.empty else []

# Format y-axis based on selected metric
if metric in ["total_revenue", "avg_order_value"]:
    y_axis = alt.Y('forecast_value:Q', title=metric.replace("_", " ").title(), axis=alt.Axis(format='$,.2f'))
else:
    y_axis = alt.Y('forecast_value:Q', title=metric.replace("_", " ").title())

# Build main forecast chart
base = alt.Chart(df_combined).mark_line().encode(
    x=alt.X('forecast_date:T', title='Date'),
    y=y_axis,
    color=alt.Color('type:N', title='Data Type')
).properties(width=700, height=400).interactive()

promo_overlay = alt.Chart(pd.DataFrame({'promo_date': pd.to_datetime(promo_dates)})).mark_rule(
    color='orange',
    strokeDash=[5, 5]
).encode(x='promo_date:T')

chart = base + promo_overlay

# Display title and chart
st.title("📈 Forecast Dashboard")
st.altair_chart(chart, use_container_width=True)

# Show forecast table
st.subheader("📋 Forecast Data Table")
st.dataframe(df_forecast, use_container_width=True)

# Download CSV of combined data
st.download_button(
    label="Download CSV",
    data=df_combined.to_csv(index=False),
    file_name=f"forecast_{region}_{metric}.csv",
    mime='text/csv'
)
