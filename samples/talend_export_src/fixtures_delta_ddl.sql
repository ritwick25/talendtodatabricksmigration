-- fixtures_delta_ddl.sql
-- Synthetic test-bed fixtures expressed as Delta tables instead of flat files.
-- NOT executed by this generator -- someone else runs this against the real workspace.
-- Target catalog: talend_migration.samples

CREATE CATALOG IF NOT EXISTS talend_migration;
CREATE SCHEMA IF NOT EXISTS talend_migration.samples;

-- Backs fixtures/customers.csv (job_01, job_02) as a Delta table.
CREATE TABLE IF NOT EXISTS talend_migration.samples.customers (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  signup_date DATE,
  country_code STRING,
  loyalty_points INT
) USING DELTA;

INSERT INTO talend_migration.samples.customers VALUES
  (1001, 'jane', 'doe', 'jane.doe@example.com', DATE'2022-03-14', 'US', 1250),
  (1002, 'marco', 'rossi', 'marco.rossi@example.it', DATE'2021-11-02', 'IT', 480),
  (1003, 'li', 'wei', 'li.wei@example.cn', DATE'2023-07-19', 'CN', 75),
  (1004, NULL, 'garcia', 'bad-email-missing-first-name', DATE'2020-01-05', 'MX', 0),
  (1005, 'amara', 'okafor', 'amara.okafor@example.ng', DATE'2019-05-30', 'NG', 999),
  (1006, 'erik', 'larsson', 'erik.larsson@example.se', DATE'2024-02-29', 'SE', 1500);

-- Backs fixtures/regions.csv (job_01) as a Delta table.
CREATE TABLE IF NOT EXISTS talend_migration.samples.regions (
  country_code STRING,
  region_name STRING
) USING DELTA;

INSERT INTO talend_migration.samples.regions VALUES
  ('US', 'North America'), ('MX', 'North America'), ('IT', 'Europe'),
  ('SE', 'Europe'), ('CN', 'Asia Pacific'), ('NG', 'Africa');

-- Backs fixtures/loyalty_tiers.csv (job_01) as a Delta table.
CREATE TABLE IF NOT EXISTS talend_migration.samples.loyalty_tiers (
  tier_code STRING,
  min_points INT,
  max_points INT,
  tier_name STRING
) USING DELTA;

INSERT INTO talend_migration.samples.loyalty_tiers VALUES
  ('T0', 0, 99, 'Bronze'), ('T1', 100, 499, 'Silver'),
  ('T2', 500, 999, 'Gold'), ('T3', 1000, 999999, 'Platinum');

-- Backs fixtures/ledger_control_totals.csv (job_15) as a Delta table.
CREATE TABLE IF NOT EXISTS talend_migration.samples.ledger_control_totals (
  account_id STRING,
  control_total DECIMAL(12,2)
) USING DELTA;

INSERT INTO talend_migration.samples.ledger_control_totals VALUES
  ('4111111111111111', 15000.00),
  ('1234567812345670', -250.50);

-- Backs fixtures/vendor_directory.csv (job_14) as a Delta table.
CREATE TABLE IF NOT EXISTS talend_migration.samples.vendor_directory (
  vendor_id STRING,
  vendor_name STRING,
  active BOOLEAN
) USING DELTA;

INSERT INTO talend_migration.samples.vendor_directory VALUES
  ('V100', 'Acme Supply', true),
  ('V200', 'Globex Logistics', true);

-- Backs fixtures/inventory_raw.csv (job_12) as a Delta table (tDBInput custom-SQL source).
CREATE TABLE IF NOT EXISTS talend_migration.samples.inventory_raw (
  sku STRING,
  warehouse_id STRING,
  qty_on_hand INT,
  last_updated TIMESTAMP
) USING DELTA;

INSERT INTO talend_migration.samples.inventory_raw VALUES
  ('SKU-2024-00123-A', 'W1', 120, TIMESTAMP'2024-06-02 10:00:00'),
  ('SKU-2023-00042-B', 'W2', 0, TIMESTAMP'2024-06-02 11:00:00');

