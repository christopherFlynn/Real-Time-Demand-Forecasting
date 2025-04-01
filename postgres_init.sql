-- orders table
CREATE TABLE IF NOT EXISTS orders (
  order_id VARCHAR PRIMARY KEY,
  timestamp TIMESTAMP,
  product_id VARCHAR,
  user_id VARCHAR,
  region VARCHAR,
  quantity INT,
  unit_price NUMERIC,
  total_price NUMERIC,
  device_type VARCHAR,
  promo_applied BOOLEAN
);

-- daily metrics
CREATE TABLE IF NOT EXISTS daily_metrics (
  date DATE,
  region VARCHAR,
  total_orders INT,
  total_revenue NUMERIC,
  avg_order_value NUMERIC,
  is_promo_day BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (date, region)
);

-- forecasted metrics
CREATE TABLE IF NOT EXISTS forecast_metrics (
  region VARCHAR,
  metric VARCHAR,
  forecast_date DATE,
  forecast_value NUMERIC,
  lower_bound NUMERIC,
  upper_bound NUMERIC,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (region, metric, forecast_date)
);
