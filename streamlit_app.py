import os
import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import text

# Connect to Supabase using Streamlit's native SQL connection
conn = st.connection("supabase_db", type="sql")

# Fetch forecasted metrics for a selected region and metric
# If AOV is selected, derive it from forecasted revenue and orders

def get_forecast(region, metric):
    if metric == "avg_order_value":
        query = """
        SELECT region, metric, forecast_date, forecast_value
        FROM forecast_metrics
        WHERE region = :region AND metric IN ('total_orders', 'total_revenue')
        """
        df_raw = pd.read_sql(text(query), conn.session, params={"region": region})
        df_pivot = df_raw.pivot(index="forecast_date", columns="metric", values="forecast_value")
        df_pivot = df_pivot.dropna()
        df_pivot["forecast_value"] = df_pivot["total_revenue"] / df_pivot["total_orders"]
        df_pivot = df_pivot.reset_index()
        df_pivot["type"] = "Forecast"
        return df_pivot[["forecast_date", "forecast_value", "type"]]
    else:
        query = """
        SELECT forecast_date, forecast_value, 'Forecast' AS type
        FROM forecast_metrics
        WHERE region = :region AND metric = :metric
        ORDER BY forecast_date
        """
        return pd.read_sql(text(query), conn.session, params={"region": region, "metric": metric})

# Fetch historical data based on selected filters

def get_history(region, metric, category, segment, hour):
    if metric == "total_revenue":
        value_expr = "SUM(o.total_price)"
    elif metric == "total_orders":
        value_expr = "COUNT(*)"
    elif metric == "avg_order_value":
        value_expr = "AVG(o.total_price)"
    else:
        st.error(f"Unsupported metric: {metric}")
        st.stop()

    query = f"""
    SELECT DATE(o.timestamp) AS forecast_date,
           {value_expr} AS forecast_value,
           dm.is_promo_day
    FROM orders o
    JOIN daily_metrics dm ON DATE(o.timestamp) = dm.date AND o.region = dm.region
    WHERE o.region = :region
    """
    params = {"region": region}

    if category != "All":
        query += " AND o.category = :category"
        params["category"] = category

    if segment != "All":
        query += " AND o.user_segment = :segment"
        params["segment"] = segment

    if hour != "All":
        query += " AND EXTRACT(HOUR FROM o.timestamp) = :hour"
        params["hour"] = int(hour)

    query += " GROUP BY DATE(o.timestamp), dm.is_promo_day ORDER BY DATE(o.timestamp)"
    df = pd.read_sql(text(query), conn.session, params=params)
    df["type"] = "Historical"
    return df

# Sidebar filter controls
st.sidebar.header("📊 Filters")
region = st.sidebar.selectbox("Select Region", ['Northeast', 'Midwest', 'South', 'West'])
metric = st.sidebar.radio("Metric", ['total_orders', 'total_revenue', 'avg_order_value'])

categories = conn.query("SELECT DISTINCT category FROM orders", ttl=0)["category"].tolist()
segments = conn.query("SELECT DISTINCT user_segment FROM orders", ttl=0)["user_segment"].tolist()
hours = conn.query("SELECT DISTINCT hour FROM orders ORDER BY hour", ttl=0)["hour"].tolist()

selected_category = st.sidebar.selectbox("Product Category", ["All"] + categories)
selected_segment = st.sidebar.selectbox("User Segment", ["All"] + segments)
selected_hour = st.sidebar.selectbox("Hour of Day", ["All"] + [str(h) for h in hours])

# Pull and combine forecast and history data
df_forecast = get_forecast(region, metric)
df_history = get_history(region, metric, selected_category, selected_segment, selected_hour)
df_combined = pd.concat([df_forecast, df_history])

# Extract promo dates for visual overlay
promo_dates = df_history[df_history['is_promo_day'] == True]['forecast_date'].tolist()

# Format y-axis based on selected metric
if metric in ["total_revenue", "avg_order_value"]:
    y_axis = alt.Y('forecast_value:Q', title=metric.replace("_", " ").title(), axis=alt.Axis(format='$,.2f'))
else:
    y_axis = alt.Y('forecast_value:Q', title=metric.replace("_", " ").title())

# Build main forecast chart
chart = alt.Chart(df_combined).mark_line().encode(
    x='forecast_date:T',
    y=y_axis,
    color='type:N'
)

# Add vertical line for promo days
promo_overlay = alt.Chart(pd.DataFrame({'promo_date': promo_dates})).mark_rule(
    color='orange',
    strokeDash=[5, 5]
).encode(x='promo_date:T')

# Display title and chart
st.title("📈 Forecast Dashboard")
st.altair_chart(chart + promo_overlay, use_container_width=True)

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