# Automated Dormitory Data Engineering Pipeline (Medallion Architecture)

An end-to-end Data Engineering pipeline built on **Databricks** using **PySpark** and **Delta Lake**. This project automates the ETL/ELT workflow for dormitory utility billing and revenue analytics by ingesting operational transactional data from a **Neon PostgreSQL** database and structuring it through a **Medallion Architecture (Bronze, Silver, and Gold layers)**.

---

## Tech Stack & Key Tools

* **Compute & Processing:** PySpark, Databricks (Serverless / Jobs)
* **Storage & Catalog:** Delta Lake, Unity Catalog
* **Source Database:** PostgreSQL (Neon DB) via JDBC
* **Secrets Management:** Databricks Secrets Scope (`dbutils.secrets`)
* **Version Control:** Databricks Git Folders & GitHub Integration

---

## Pipeline Layers & Data Flow

### 1. Bronze Layer (`bronze_nb`)
* **Objective:** Ingest raw transactional data directly from PostgreSQL tables (`dorm_bills`, `dorm_rates`) into Delta Lake tables without modifying historical payload formats.
* **Key Operations:**
  * Secure JDBC connectivity using Databricks Secrets.
  * Schema casting (`DecimalType` precision adjustments for meter metrics).
  * System metadata enrichment by attaching `_processing_time` timestamps.
  * **Upserting Strategy:** Idempotent merges via `merge_into_table()` using unique keys (`bill_id`, `rate_id`).

### 2. Silver Layer (`silver_nb`)
* **Objective:** Calculate utility usages, and Perform point-in-time rate joins.
* **Key Operations:**
  * **Meter Difference Calculation:** Computes `elec_unit_used` and `water_unit_used` by evaluating previous readings via PySpark Window functions (`lag()`) partitioned by `room_id` and ordered by `bill_date`.
  * **Null Handling:** Encapsulated within `coalesce()` to handle boundary conditions (e.g., initial month readings default to 0).
  * **Rate Validation Join:** Joins active bill dates with effective rate validity periods (`valid_from`) from `dorm_rate` using Windowing/Filtering to prevent duplicate join matches.
  * **Cost Computation:** Multiplies consumption units with active rates to derive final charges (`elec_charge`, `water_charge`, `total_charge`).
  * **Upserting Strategy:** Merge into `silver_layer` partitioned by `bill_id`.

### 3. Gold Layer (`gold_nb`)
* **Objective:** Aggregate business-level metrics for financial dashboards and management analytics.
* **Key Operations:**
  * **Revenue Aggregation:** Summarizes revenue figures (`monthly_revenue`, `avg_revenue_per_room`) grouped by billing period (`bill_date`).
  * **Latest Status Tracking:** Uses `row_number()` over room partitions to extract the most recent status per tenant.
  * **Upserting Strategy:** Merge into `gold_layer` / aggregated analytical tables.

---

## 🔒 Security & Best Practices

* **Zero Hardcoded Credentials:** Database credentials and JDBC connection strings are retrieved dynamically via `dbutils.secrets.get(scope="project_key", ...)`.
* **Idempotent Data Pipelines:** All writes to Delta tables rely on `MERGE INTO` (Upsert) logic, ensuring pipelines can be safely re-run without creating duplicate rows.
* **Modular Codebase:** Common table maintenance functions and Delta merge operations are centralized within `project_tools.py` for maintainability.
```
