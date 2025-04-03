# Real-Time-Demand-Forecasting

**Project Type**: Real-Time Pipeline | Forecasting | Time Series | Dashboard  
**Tools Used**: Python, Pandas, Prophet, PostgreSQL, Supabase, Kafka, Docker, Streamlit, Cron  
**Dataset**: Simulated Order Data Streamed in Real-Time

[Link to Live Dashboard](https://real-time-demand-forecasting-vytxizqfzi2schv547waar.streamlit.app/)

### **Objective**

Build a full-stack real-time demand forecasting system capable of simulating orders, aggregating and storing metrics, generating daily forecasts, and visualizing results in an interactive, cloud-deployed dashboard.

### **Key Features**

- **Real-Time Event Streaming**:

  - Used Kafka to simulate a continuous stream of customer orders with custom payloads (region, category, user segment, price).
  - Kafka consumer stores incoming events into a PostgreSQL database.

- **Data Aggregation and Forecasting**:

  - Daily metrics (total revenue, total orders, average order value) aggregated and inserted into a dedicated `daily_metrics` table.
  - Forecasts generated using Prophet and stored in `forecast_metrics` with uncertainty intervals.

- **Simulated Promotions and Segments**:

  - Injected realistic behavior by simulating weekends, holidays, and promotional days.
  - Orders contain metadata like hour, category, and user segment to allow detailed filtering.

- **Automation & Scheduling**:

  - Full pipeline automated with a cron job to run daily at 10:30 AM.
  - Includes a shell script to activate the virtual environment and sequentially trigger each pipeline step.

- **Streamlit Dashboard**:
  - Filter by region, category, user segment, hour of day, and lookback window.
  - Visualize historical and forecasted metrics with clear promo overlays.
  - Download filtered CSVs and explore tabular forecast data.

### **Deployment Highlights**

- Streamlit Cloud used for dashboard deployment.
- Supabase hosted PostgreSQL used for fast and secure data storage.
- Dockerized Kafka/Zookeeper/Postgres stack for local simulation.

🔗 **View Project Page on my Website:** [Link](https://christopherflynn.dev/real-time-demand-forecasting/)
