# MIA — Enterprise Sales Data Lakehouse

> End-to-end Azure Databricks lakehouse built on the **Medallion Architecture** (Bronze → Silver → Gold) with a **Kimball Star Schema** at the gold layer. Powered by Delta Lake, Unity Catalog, PySpark, and ADLS Gen2.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Medallion Pipeline Flow](#medallion-pipeline-flow)
- [Star Schema](#star-schema)
- [Notebook Pipeline Details](#notebook-pipeline-details)
- [Folder Structure](#folder-structure)

---

## Project Overview

MIA (Medallion Intelligence Architecture) is a production-style data lakehouse that ingests raw transactional sales data, cleans and conforms it through layered transformations, and exposes a Kimball star schema in the gold zone for BI reporting and analytics via Power BI.

The project covers:
- Parameterized bronze ingestion pipeline driven by a parameter notebook
- Silver cleansing and deduplication layer
- Gold dimensional modelling with SCD Type 1 and SCD Type 2 dimensions
- A central `FactOrders` table resolved via point-in-time surrogate key lookups against SCD2 dimensions
- Full Delta Lake MERGE patterns for incremental loads across all layers
- Unity Catalog (`databricks_cata`) for governance across all three layers

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Compute | Azure Databricks (PySpark) |
| Table Format | Delta Lake |
| Catalog | Databricks Unity Catalog |
| Orchestration | Databricks Workflows / Parameter Notebooks |
| BI Layer | Power BI |
| Version Control | GitHub |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MIA — Medallion Architecture                         │
└─────────────────────────────────────────────────────────────────────────────┘

   SOURCE SYSTEM
   ┌──────────────┐
   │  Raw Tables  │  Orders, Customers, Products, Regions
   │  (CSV / DB)  │
   └──────┬───────┘
          │  Parameter Notebook feeds table names dynamically
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER   abfss://bronze@customersprojectete.dfs.core.windows.net/   │
│                                                                             │
│  • Raw data landed as-is from source                                        │
│  • No transformations — full fidelity copy                                  │
│  • Parameterized ingestion — table names passed via parameter notebook      │
│  • Managed in Unity Catalog: databricks_cata.bronze.*                       │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  PySpark cleansing notebooks
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER   abfss://silver@customersprojectete.dfs.core.windows.net/   │
│                                                                             │
│  • Deduplication, null handling, type casting                               │
│  • Conformed column names and standardised schemas                          │
│  • Source of truth for all gold transformations                             │
│  • Managed in Unity Catalog: databricks_cata.silver.*                       │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  Dimensional modelling notebooks
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER     abfss://gold@customersprojectete.dfs.core.windows.net/     │
│                                                                             │
│  Kimball Star Schema                                                        │
│                                                                             │
│  Dimensions:                                                                │
│  • DimCustomersSCD2   — SCD Type 2 (tracks email, city, state changes)     │
│  • DimProductsscd2    — SCD Type 2 (tracks name, brand, category, price)   │
│  • DimDate            — Static generated date dimension (2020–2030)         │
│                                                                             │
│  Fact:                                                                      │
│  • FactOrders         — One row per order, FK to all three dimensions       │
│                                                                             │
│  Managed in Unity Catalog: databricks_cata.gold.*                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Pipeline Flow

```
Parameter Notebook
       │
       │  table_name, source_path
       ▼
Bronze Notebook  ──────────────►  databricks_cata.bronze.<table>
       │
       │  read bronze, cleanse, deduplicate
       ▼
Silver Notebook  ──────────────►  databricks_cata.silver.<table>_silver
       │
       ├──► DimCustomers_SCD2.ipynb  ──►  databricks_cata.gold.DimCustomersSCD2
       │
       ├──► DimProducts_SCD2.ipynb   ──►  databricks_cata.gold.DimProductsscd2
       │
       ├──► DimDate.ipynb            ──►  databricks_cata.gold.DimDate
       │
       └──► FactOrders.ipynb         ──►  databricks_cata.gold.FactOrders
```

All gold notebooks accept an `init_load_flag` widget:

| Flag | Behaviour |
|---|---|
| `1` | Initial load — writes full dataset, creates table |
| `0` | Incremental load — runs Delta MERGE, upserts changes only |

---

## Star Schema

```
                        ┌─────────────────────────┐
                        │        DimDate           │
                        │─────────────────────────│
                        │ 🔑 DimDateKey (YYYYMMDD) │
                        │  full_date               │
                        │  year, quarter, month    │
                        │  month_name, week        │
                        │  day_name, is_weekend    │
                        └───────────┬─────────────┘
                                    │ DimDateKey
                                    │
┌─────────────────────┐             │            ┌──────────────────────────┐
│  DimCustomersSCD2   │             │            │    DimProductsscd2       │
│─────────────────────│             │            │──────────────────────────│
│ 🔑 DimCustomerKey   │             │            │ 🔑 DimProductKey         │
│  customer_id (BK)   │             │            │  product_id (BK)         │
│  email, city, state │             │            │  product_name            │
│  effective_start    ├─────────────┤────────────┤  brand, category, price  │
│  effective_end      │  CustKey    │  ProdKey   │  effective_start         │
│  is_current         │             │            │  effective_end           │
└─────────────────────┘             │            │  is_current              │
                                    │            └──────────────────────────┘
                                    │
                        ┌───────────┴─────────────┐
                        │       FactOrders         │
                        │─────────────────────────│
                        │ 🔑 order_id  (DD)        │
                        │  DimCustomerKey  (FK)    │
                        │  DimProductKey   (FK)    │
                        │  DimDateKey      (FK)    │
                        │  quantity                │
                        │  total_amount            │
                        │  row_hash                │
                        │  load_date               │
                        └─────────────────────────┘

  🔑 = surrogate key    BK = business key    DD = degenerate dimension
  FK = foreign key      SCD2 dimensions use point-in-time surrogate key resolution
```

---

## Notebook Pipeline Details

### `init_load_flag` Widget

Every gold notebook accepts a widget parameter `init_load_flag`:
- **`1` — Initial Load**: Assumes no gold table exists. Writes the full dataset using `mode("overwrite")` and registers the table in Unity Catalog.
- **`0` — Incremental Load**: Reads the existing gold table, computes changes, and runs a Delta `MERGE` to upsert only new or changed records.

---

### SCD Type 2 — Row Hash Change Detection

Dimensions `DimCustomersSCD2` and `DimProductsscd2` use MD5 row hashing to detect attribute changes:

```python
df = df.withColumn(
    "row_hash",
    md5(concat_ws("||", col("product_name"), col("brand"), col("category"), col("price")))
)
```

If the incoming hash differs from the stored hash, the existing row is **expired** (`is_current = 0`, `effective_end_date` stamped) and a **new version** is inserted (`is_current = 1`, `effective_start_date = now()`).

---

### Surrogate Key Generation

Surrogate keys are generated using `row_number()` over a window, offset by the current maximum key in the gold table to ensure continuity across incremental loads:

```python
w = Window.partitionBy(lit(1)).orderBy("product_id")
df_inserts = df_inserts.withColumn("DimProductKey", row_number().over(w))

# offset by max existing key
df_inserts = df_inserts.withColumn("DimProductKey", lit(max_surrogate_key) + col("DimProductKey"))
```

---

### Point-in-Time Surrogate Key Resolution (FactOrders)

`FactOrders` resolves surrogate keys from SCD2 dimensions using a date-range join — ensuring each order gets the dimension version that was **true at the time of the order**, not today's version:

```python
df_joined = df_orders.join(
    df_cust,
    (col("ord.customer_id") == col("cust.customer_id")) &
    (col("ord.order_date") >= col("cust.effective_start_date")) &
    (
        col("cust.effective_end_date").isNull() |
        (col("ord.order_date") <= col("cust.effective_end_date"))
    ),
    "left"
)
```

Unresolved lookups fall back to surrogate key `-1` (unknown member pattern) to preserve all fact rows.

---

## Folder Structure

```
MIA-Enterprise-Sales-Lakehouse/
│
├── README.md
│
├── notebooks/
│   ├── bronze/
│   │   └── bronze_ingestion.ipynb
│   │
│   ├── silver/
│   │   └── silver_cleansing.ipynb
│   │
│   └── gold/
│       ├── DimCustomers_SCD2.ipynb
│       ├── DimProducts_SCD2.ipynb
│       ├── DimDate.ipynb
│       └── FactOrders.ipynb
│
├── data/
│   └── sample/
│       └── (sample CSVs for local testing)
│
└── docs/
    └── (architecture diagrams, screenshots)
```

---

## Author

**Sagar kamble** — Data Engineer  
Azure Databricks | PySpark | Delta Lake | Medallion Architecture  
[LinkedIn](https://github.com/sagarkamble7) • [GitHub](https://github.com/sagarkamble7)


