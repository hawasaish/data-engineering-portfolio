# Project Overview:

This project implements a modern local ELT platform for an e-commerce business using PostgreSQL, MinIO, Apache Airflow, DuckDB, dbt, and Apache Superset.

The platform integrates transactional order data, marketing funnel data, and incremental synthetic daily orders into a centralized analytics environment. Airflow orchestrates ingestion and transformation, MinIO provides S3-compatible raw storage, DuckDB provides analytical processing, and dbt creates analytics-ready marts with automated data-quality testing.

The final Superset dashboard provides visibility into revenue trends, customer lifetime value, marketing performance, and pipeline health.


## Business problem:

```Transactional data
↓
PostgreSQL
Marketing data
↓
CSV files
User/order updates
↓
Daily incremental data```

### Problem:

Data is fragmented
↓
Difficult reporting
↓
No centralized quality checks
↓
No automated pipeline
```



## Tech stack:

| Component        | Technology      | Purpose               |
| ---------------- | --------------- | --------------------- |
| Source DB        | PostgreSQL      | Transactional data    |
| Object Storage   | MinIO           | Raw data lake         |
| Orchestration    | Apache Airflow  | Scheduling/workflows  |
| Processing       | DuckDB          | Analytical processing |
| Storage Format   | Parquet         | Columnar storage      |
| Transformation   | dbt             | Analytics engineering |
| Data Quality     | dbt tests       | Validation            |
| BI               | Apache Superset | Dashboard             |
| Language         | Python          | Ingestion             |
| Containerization | Docker          | Local infrastructure  |


## Data sources:

### Olist dataset:

- Orders
- Customers
- Products
- Order Items
- Payments
- Sellers

### Marketing:

- CSV
- utm_source
- campaign
- landing_page_clicks
- coupon_codes_used

### Daily synthetic data:

- Faker
- 100 daily orders

The synthetic daily data simulates incremental transactional activity so the pipeline can demonstrate daily ingestion and incremental processing without requiring a continuously changing public dataset.



## Pipeline workflow:

```PostgreSQL
│
▼
Airflow
│
├── Extract
├── Generate Daily Data
└── Load Marketing
│
▼
MinIO
│
▼
Parquet
│
▼
DuckDB
│
▼
dbt
│
┌─────┴─────┐
▼           ▼
Transform      Tests
│
▼
Analytics Marts
│
▼
Superset
```


## Data quality:

✓ Primary keys cannot be NULL
✓ Dimension keys are unique
✓ Composite fact keys are unique
✓ Payment values must be positive
✓ Customer foreign keys must exist
✓ Product foreign keys must exist
✓ Source/target row counts are monitored
✓ Data ingestion timestamps are tracked

dbt test
↓
PASS → pipeline continues
FAIL → Airflow DAG fails


## Service URLs:

| Service  | URL                     |
| -------- | ----------------------- |
| Airflow  | `http://localhost:8080` |
| MinIO    | `http://localhost:9001` |
| Superset | `http://localhost:8088` |


## Engineering decisions:

**MinIO**

Used as a local S3-compatible object store to simulate cloud object storage.

**Parquet**

Used as a columnar storage format because it is efficient for analytical workloads and integrates naturally with DuckDB.

**DuckDB**

Used as a lightweight analytical engine that can query Parquet efficiently without requiring a large database cluster.

**Airflow**

Used to demonstrate scheduled workflows, dependencies, retries, logging, and pipeline orchestration.

**dbt**

Used to separate data transformation and analytics modeling from ingestion logic.


