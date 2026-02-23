-- ============================================================
-- CommercePulse BigQuery Warehouse Schema
-- ============================================================
-- How to use:
-- 1) Replace `your_project.your_dataset` with your real values
-- 2) Run this file in BigQuery SQL workspace
-- 3) Then run the load script to populate tables
--
-- Example:
--   `apt-perception-478516-g1.Commerce_pulse`
-- ============================================================

CREATE SCHEMA IF NOT EXISTS `your_project.your_dataset`;

-- ----------------------------
-- Dimension tables
-- ----------------------------

-- Latest known customer profile by customer_id.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_customer` (
  customer_id STRING NOT NULL,
  email STRING,
  phone_number STRING,
  source_vendor STRING,
  last_event_time TIMESTAMP,
  last_ingested_at TIMESTAMP
);

-- Product-level rollups from event payloads.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_product` (
  product_id STRING NOT NULL,
  first_event_time TIMESTAMP,
  last_event_time TIMESTAMP,
  event_count INT64,
  order_count INT64
);

-- Date dimension used for daily-level analysis.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_date` (
  date_key DATE NOT NULL,
  event_count INT64
);

-- ----------------------------
-- Fact tables
-- ----------------------------
-- Partitioning + clustering are set for query performance and cost control.

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.facts_order` (
  event_id STRING NOT NULL,
  event_type STRING,
  event_time TIMESTAMP,
  vendor STRING,
  order_id STRING,
  transaction_id STRING,
  tracking_id STRING,
  amount NUMERIC,
  currency STRING,
  status STRING,
  region STRING,
  ingested_at TIMESTAMP,
  quality_bucket STRING,
  is_late_arrival BOOL,
  is_warning BOOL,
  is_critical BOOL,
  is_anomaly BOOL
)
PARTITION BY DATE(event_time)
CLUSTER BY order_id, vendor, event_type;

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.facts_payment` (
  event_id STRING NOT NULL,
  event_type STRING,
  event_time TIMESTAMP,
  vendor STRING,
  order_id STRING,
  transaction_id STRING,
  tracking_id STRING,
  amount NUMERIC,
  currency STRING,
  status STRING,
  region STRING,
  ingested_at TIMESTAMP,
  quality_bucket STRING,
  is_late_arrival BOOL,
  is_warning BOOL,
  is_critical BOOL,
  is_anomaly BOOL
)
PARTITION BY DATE(event_time)
CLUSTER BY order_id, transaction_id, vendor;

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.facts_refunds` (
  event_id STRING NOT NULL,
  event_type STRING,
  event_time TIMESTAMP,
  vendor STRING,
  order_id STRING,
  transaction_id STRING,
  tracking_id STRING,
  amount NUMERIC,
  currency STRING,
  status STRING,
  region STRING,
  ingested_at TIMESTAMP,
  quality_bucket STRING,
  is_late_arrival BOOL,
  is_warning BOOL,
  is_critical BOOL,
  is_anomaly BOOL
)
PARTITION BY DATE(event_time)
CLUSTER BY order_id, vendor;

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.facts_shipment` (
  event_id STRING NOT NULL,
  event_type STRING,
  event_time TIMESTAMP,
  vendor STRING,
  order_id STRING,
  transaction_id STRING,
  tracking_id STRING,
  amount NUMERIC,
  currency STRING,
  status STRING,
  region STRING,
  ingested_at TIMESTAMP,
  quality_bucket STRING,
  is_late_arrival BOOL,
  is_warning BOOL,
  is_critical BOOL,
  is_anomaly BOOL
)
PARTITION BY DATE(event_time)
CLUSTER BY order_id, tracking_id, vendor;

-- Daily order grain table used for trend dashboards.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.facts_order_daily` (
  event_date DATE NOT NULL,
  order_id STRING NOT NULL,
  event_count INT64,
  total_amount NUMERIC,
  payment_events INT64,
  refund_events INT64,
  shipment_events INT64,
  warning_count INT64,
  critical_count INT64
)
PARTITION BY event_date
CLUSTER BY order_id;
