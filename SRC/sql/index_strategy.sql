-- ============================================================
-- CommercePulse BigQuery Index / Performance Strategy
-- ============================================================
-- Note for reviewers:
-- BigQuery is not optimized with traditional relational indexes (B-tree).
-- The main optimization pattern is:
--   1) PARTITION BY (usually date/timestamp)
--   2) CLUSTER BY frequently filtered columns
--
-- In this project, those are already defined in `bigquery_schema.sql`.
-- The statements below are optional SEARCH INDEXes for point lookups.
-- Replace `your_project.your_dataset` before running.
-- ============================================================

-- Fast lookup by order id in order fact table.
CREATE SEARCH INDEX IF NOT EXISTS idx_facts_order_order_id
ON `your_project.your_dataset.facts_order`(order_id);

-- Fast lookup by transaction id in payments.
CREATE SEARCH INDEX IF NOT EXISTS idx_facts_payment_txn
ON `your_project.your_dataset.facts_payment`(transaction_id);

-- Fast lookup by tracking id in shipments.
CREATE SEARCH INDEX IF NOT EXISTS idx_facts_shipment_tracking
ON `your_project.your_dataset.facts_shipment`(tracking_id);

-- Fast lookup by customer email.
CREATE SEARCH INDEX IF NOT EXISTS idx_dim_customer_email
ON `your_project.your_dataset.dim_customer`(email);
