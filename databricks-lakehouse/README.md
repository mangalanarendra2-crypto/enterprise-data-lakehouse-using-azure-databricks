# Enterprise Data Lakehouse Using Azure Databricks

A simple base project for a **Delta Lake lakehouse** on Azure Databricks with Unity Catalog,
using the medallion pattern and the lakehouse features that set it apart from a plain data lake.

```
 landing volume (CSV drops)
        │  Auto Loader (incremental, exactly-once)
        ▼
 BRONZE  raw Delta tables, append-only, + _ingested_at / _source_file
        │  clean · validate · de-duplicate
        ▼
 SILVER  orders        ← MERGE upsert on order_id (late corrections update, never duplicate)
         dim_customer  ← SCD Type 2 history (city / segment changes)
        │  aggregate · point-in-time join
        ▼
 GOLD    daily_revenue · revenue_by_segment · customer_360   → Power BI / Databricks SQL
```

## Lakehouse features demonstrated

| Feature | Where |
|---|---|
| Unity Catalog (catalog / schemas / volume / GRANTs) | `sql/00_setup_unity_catalog.sql` |
| Auto Loader incremental ingestion (`cloudFiles`, `availableNow`) | `notebooks/01_bronze_autoloader.py` |
| Delta `MERGE` upsert (idempotent, ignores stale re-sends) | `src/lakehouse/delta_ops.py::upsert_orders` |
| SCD Type 2 dimension via MERGE | `transforms.stage_scd2_updates` + `delta_ops.apply_scd2` |
| Delta `CHECK` constraints | `notebooks/02_silver_merge.py` |
| Point-in-time join (segment *as of the order date*) | `transforms.revenue_by_segment` |
| `OPTIMIZE … ZORDER`, `VACUUM`, `DESCRIBE HISTORY`, time travel, `RESTORE` | `notebooks/04_maintenance_time_travel.py` |
| Workflow job + dev/prod targets (Asset Bundle) | `databricks.yml` |
| Unit-testable PySpark logic kept out of notebooks | `src/lakehouse/transforms.py`, `tests/` |

## Project structure

```
databricks-lakehouse/
├── databricks.yml                     # Asset Bundle: 4-task workflow, dev/prod targets
├── sql/00_setup_unity_catalog.sql
├── notebooks/
│   ├── 01_bronze_autoloader.py
│   ├── 02_silver_merge.py
│   ├── 03_gold_marts.py
│   └── 04_maintenance_time_travel.py
├── src/lakehouse/
│   ├── transforms.py                  # pure PySpark (tested locally)
│   └── delta_ops.py                   # Delta MERGE operations (run on Databricks)
├── data/
│   ├── generate_sample_data.py
│   └── landing/{customers,orders}/    # ready-made sample CSVs
└── tests/test_transforms.py
```

## Run the unit tests locally (needs Java 17+)

```bash
pip install -r requirements.txt
pytest -q
```

These cover the cleaning rules, SCD2 staging logic and Gold marts. They do **not** exercise
the Delta `MERGE` calls in `delta_ops.py`, which need a Delta-enabled Spark (i.e. Databricks) —
run the workflow once and check the results (see step 5).

## Deploy to Azure Databricks

Prerequisites: a **Premium** Azure Databricks workspace with Unity Catalog enabled, and the
[Databricks CLI](https://docs.databricks.com/dev-tools/cli/) (`databricks auth login --host https://<workspace>.azuredatabricks.net`).

1. **Set up governance** – run `sql/00_setup_unity_catalog.sql` in the SQL editor
   (adjust the group names in the `GRANT` lines, or delete them).
2. **Configure the bundle** – put your workspace URL in `databricks.yml`, and check that
   `spark_version` is a current LTS runtime in your workspace.
3. **Upload the sample files** into the landing volume:
   ```bash
   databricks fs cp -r data/landing dbfs:/Volumes/lakehouse_dev/bronze/landing/
   ```
4. **Deploy and run**:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run lakehouse_medallion -t dev
   ```
5. **Check the results** in a SQL editor:
   ```sql
   SELECT * FROM lakehouse_dev.silver.dim_customer
   WHERE customer_id IN (SELECT customer_id FROM lakehouse_dev.silver.dim_customer GROUP BY 1 HAVING count(*) > 1)
   ORDER BY customer_id, start_date;                    -- SCD2 history (12 customers changed)
   SELECT count(*), count(DISTINCT order_id) FROM lakehouse_dev.silver.orders;   -- equal => no duplicates
   SELECT * FROM lakehouse_dev.gold.revenue_by_segment ORDER BY order_month, segment;
   ```
6. **Schedule** – the job is created `PAUSED`; un-pause it (daily 02:00 IST) once the first run succeeds.

## Sample data (deliberately imperfect)

* `customers_2026-01-01.csv` – 60 customers. `customers_2026-03-01.csv` – 12 changes, 5 new, 8 unchanged.
* `orders_part1.csv` / `orders_part2.csv` – ~550 orders with duplicates, null customer IDs, invalid
  quantities and messy text; part 2 also re-sends 10 part-1 orders with a changed status
  (these must **update** existing rows, not add new ones).

## Known limitations of this base project

* Silver re-reads all of Bronze each run – fine for small data; switch to a Bronze change feed
  (`delta.enableChangeDataFeed`) or Lakeflow Declarative Pipelines for large volumes.
* SCD2 uses a global watermark on `effective_date`: change records dated on or before the
  latest applied date are skipped, so late-arriving history needs a backfill run.
* Gold tables are fully rebuilt each run.

## Ideas to extend

Lakeflow Declarative Pipelines (DLT) with expectations, streaming ingestion from Event Hubs,
Delta Sharing, row filters / column masks in Unity Catalog, Databricks SQL dashboards,
CI with GitHub Actions (`pytest` + `databricks bundle validate`).
