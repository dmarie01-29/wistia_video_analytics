# Wistia Cross-Channel Video Analytics Pipeline 🧠

Automated enterprise data engineering pipeline built to ingest, transform, and report cross-channel video engagement and visitor-level analytics from Wistia.

## 🏗 System Architecture Diagram

[Wistia REST API]|(Token Auth via AWS Secrets Manager)|v[AWS Glue Python Shell]  <-- Scheduled Daily via EventBridge / Glue Workflows|(Lands Raw JSON Paginated Multi-Line Data)|v[Amazon S3 Datalake: Raw landing/]|v[AWS Glue PySpark ETL Node]|(Medallion Architecture Transformations)├── Bronze: Raw Append + Lineage Logging├── Silver: Deduplication via Surrogate Hashing (MD5) & Cast Sanitization└── Gold: Star-Schema Dimensional Modelling (dim_media, dim_visitor, fact_media_engagement)|v[Amazon S3 Datalake: Curated delta/] <-- Powered by Delta Lake ACID Transaction Logs|v[AWS Glue Data Catalog & Amazon Athena] <-- Serverless Pay-Per-Query SQL Layer|v[Streamlit Community Cloud Application] <-- Secure Live Multi-Tab Interactive Dashboard


## 🛠 Tech Stack & Rationale
* **Ingestion:** AWS Glue Python Shell (0.0625 DPU) - Serverless, cost-efficient, handles Wistia REST pagination loops beyond 15-minute Lambda boundaries.
* **Processing Framework:** PySpark via AWS Glue ETL 4.0 - Serverless distributed processing engine providing robust scaling and native Delta Lake framework extensions.
* **Storage Layer:** Amazon S3 + Delta Lake - Guarantees ACID compliance, prevents analytical table corruption via structural transaction logs, and enables historical audit time-travel.
* **Query Warehouse:** AWS Glue Data Catalog & Amazon Athena - Decoupled serverless SQL infrastructure costing strictly $5.00/TB scanned, completely eliminating idle cluster maintenance overhead.
* **Visualization UI:** Streamlit Cloud - Open-source custom Python frontend linked natively to GitHub for rapid CI/CD deployment pipelines.
* **D DBT Rationale:** Complied strictly with technical constraints; DBT was explicitly excluded from the transformation stack.

## 📊 Relational Data Warehousing Models (FR10)

### `dim_media` (FR3)
* `media_id` (VARCHAR, Primary Key) - Unique Wistia asset hash identifier.
* `title` (VARCHAR) - Video title.
* `channel` (VARCHAR) - Tracking vector (On-Site Embedded Channel).
* `created_at` (TIMESTAMP) - Ingestion timestamp milestone.

### `dim_visitor` (FR5)
* `visitor_id` (VARCHAR, Primary Key) - Unique anonymous user session key.
* `ip_address` (VARCHAR) - Masked viewer IP footprint identifier.
* `country` (VARCHAR) - ISO location country code.

### `fact_media_engagement` (FR4)
* `media_id` (VARCHAR, Foreign Key) - Associated video asset key.
* `visitor_id` (VARCHAR, Foreign Key) - Associated visitor identifier.
* `date` (TIMESTAMP) - Exact interaction window execution timestamp.
* `play_count` (BIGINT) - Quantity of distinct streams initialized.
* `play_rate` (DOUBLE) - Stream validation quotient score.
* `total_watch_time` (BIGINT) - Performance watch metrics duration (seconds).
* `watched_percent` (DOUBLE) - Segment engagement progression score.

## 🚀 Setup, Ingestion, & Execution Flow
1. **Credentials Management (FR2):** Populate Wistia bearer keys inside AWS Secrets Manager mapping `wistia/api/credentials`.
2. **Infrastructure Assembly:** Provision an IAM Execution Role containing S3, Secrets, and Glue read/write policies.
3. **Ingestion & Pagination (FR6 & FR7):** Deploy the Python script into a Glue Python Shell container. Script downloads checkpoints from S3, reads the delta boundary, queries the API with pagination loops, writes multi-line JSON payloads, and updates states.
4. **Processing Orchestration:** Execute the PySpark Medallion routine using `.option("multiLine", "true")` and robust `_corrupt_record` validation blocks to form the transactional Delta layers across S3.
5. **SQL Mapping:** Execute DDL table registration queries via Amazon Athena against the S3 Gold targets.
6. **Automation Scheduling (FR8):** Configured automated daily pipeline tracking loops via chained Glue Workflows to govern your active 7-day production validation run window, with failure alerts bound to an Amazon SNS topic.
7. **Dashboard Activation (FR11):** Connect the Streamlit workspace repository

