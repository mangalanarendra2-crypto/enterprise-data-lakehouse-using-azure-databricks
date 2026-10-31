import sys
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from lakehouse import transforms as t  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]").appName("lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


BRONZE_SCHEMA = ("order_id string, customer_id string, product string, category string, quantity string, "
                 "unit_price string, order_date string, country string, status string, "
                 "_ingested_at timestamp, _source_file string")


def bronze(spark, rows):
    from datetime import datetime
    ts = datetime(2026, 5, 1, 10, 0, 0)
    full = [(*r[:9], r[9] if len(r) > 9 else ts, r[10] if len(r) > 10 else "f1.csv") for r in rows]
    return spark.createDataFrame(full, BRONZE_SCHEMA)


GOOD = ("O1", "C1", "Laptop", "Electronics", "2", "100.00", "2026-01-05", " india ", "COMPLETED")


# ---------------------------------------------------------------- clean_orders
def test_clean_orders_standardises_and_computes_total(spark):
    out = t.clean_orders(bronze(spark, [GOOD])).collect()[0]
    assert (out.country, out.status, out.total_amount) == ("INDIA", "completed", 200.0)
    assert out.order_date == date(2026, 1, 5)


def test_clean_orders_drops_invalid_rows_without_failing(spark):
    rows = [
        GOOD,
        ("O2", "", "Desk", "Furniture", "1", "250", "2026-01-06", "UK", "shipped"),       # null customer
        ("O3", "C2", "Desk", "Furniture", "-1", "250", "2026-01-06", "UK", "shipped"),    # bad quantity
        ("O4", "C3", "Desk", "Furniture", "abc", "250", "2026-01-06", "UK", "shipped"),   # non-numeric (ANSI-safe)
        ("O5", "C4", "Desk", "Furniture", "1", "250", "not-a-date", "UK", "shipped"),     # bad date
    ]
    assert [r.order_id for r in t.clean_orders(bronze(spark, rows)).collect()] == ["O1"]


def test_clean_orders_keeps_latest_version_of_duplicate_order(spark):
    from datetime import datetime
    old = (*GOOD[:8], "shipped", datetime(2026, 5, 1, 9), "orders_part1.csv")
    new = (*GOOD[:8], "cancelled", datetime(2026, 5, 1, 10), "orders_part2.csv")
    out = t.clean_orders(bronze(spark, [old, new])).collect()
    assert len(out) == 1 and out[0].status == "cancelled"


# ---------------------------------------------------------------- SCD2 staging
DIM_SCHEMA = ("customer_id string, name string, email string, city string, segment string, "
              "row_hash string, start_date date, end_date date, is_current boolean")
INC_SCHEMA = "customer_id string, name string, email string, city string, segment string, effective_date date"


def dim_row(spark_rows, spark):
    return spark.createDataFrame(spark_rows, DIM_SCHEMA)


def test_scd2_staging_new_changed_unchanged(spark):
    base = t.add_row_hash(
        spark.createDataFrame(
            [("C1", "A", "a@x.com", "Hyderabad", "SMB"), ("C3", "C", "c@x.com", "Berlin", "Consumer")],
            "customer_id string, name string, email string, city string, segment string"),
        t.CUSTOMER_TRACKED_COLS).collect()
    current = dim_row([
        (r.customer_id, r.name, r.email, r.city, r.segment, r.row_hash, date(2026, 1, 1), None, True)
        for r in base], spark)

    incoming = spark.createDataFrame([
        ("C1", "A", "a@x.com", "Mumbai", "SMB", date(2026, 3, 1)),        # city changed
        ("C2", "B", "b@x.com", "London", "Enterprise", date(2026, 3, 1)),  # brand new
        ("C3", "C", "c@x.com", "Berlin", "Consumer", date(2026, 3, 1)),   # unchanged
    ], INC_SCHEMA)

    staged = t.stage_scd2_updates(incoming, current, "customer_id").collect()
    summary = sorted(((r.customer_id, r.merge_key) for r in staged), key=lambda x: (x[0], x[1] or ""))
    # C1: one row to expire (key) + one to insert (null); C2: insert only; C3: nothing
    assert summary == [("C1", None), ("C1", "C1"), ("C2", None)]


# ---------------------------------------------------------------- Gold
ORDERS_SCHEMA = "order_id string, customer_id string, order_date date, status string, total_amount double"


def orders_df(spark, rows):
    return spark.createDataFrame(rows, ORDERS_SCHEMA)


def versioned_dim(spark):
    # C1 was SMB until 2026-03-01, then Enterprise
    return spark.createDataFrame([
        ("C1", "A", "a@x.com", "Hyderabad", "SMB", "h1", date(2026, 1, 1), date(2026, 3, 1), False),
        ("C1", "A", "a@x.com", "Hyderabad", "Enterprise", "h2", date(2026, 3, 1), None, True),
    ], DIM_SCHEMA)


def test_daily_revenue_excludes_cancelled(spark):
    o = orders_df(spark, [("O1", "C1", date(2026, 1, 5), "completed", 100.0),
                          ("O2", "C1", date(2026, 1, 5), "shipped", 50.0),
                          ("O3", "C1", date(2026, 1, 5), "cancelled", 999.0)])
    r = t.daily_revenue(o).collect()
    assert len(r) == 1 and r[0].total_orders == 2 and r[0].total_revenue == 150.0


def test_revenue_by_segment_is_point_in_time(spark):
    o = orders_df(spark, [("O1", "C1", date(2026, 2, 10), "completed", 100.0),   # SMB at the time
                          ("O2", "C1", date(2026, 3, 10), "completed", 300.0),   # Enterprise at the time
                          ("O3", "C9", date(2026, 3, 11), "completed", 7.0)])    # not in dimension
    got = {(r.order_month, r.segment): r.revenue for r in t.revenue_by_segment(o, versioned_dim(spark)).collect()}
    assert got == {(date(2026, 2, 1), "SMB"): 100.0,
                   (date(2026, 3, 1), "Enterprise"): 300.0,
                   (date(2026, 3, 1), "UNKNOWN"): 7.0}


def test_customer_360_one_row_per_current_customer(spark):
    o = orders_df(spark, [("O1", "C1", date(2026, 2, 10), "completed", 100.0),
                          ("O2", "C1", date(2026, 3, 10), "completed", 300.0)])
    r = t.customer_360(o, versioned_dim(spark)).collect()
    assert len(r) == 1
    assert (r[0].segment, r[0].total_orders, r[0].lifetime_value) == ("Enterprise", 2, 400.0)
    assert r[0].first_order_date == date(2026, 2, 10) and r[0].last_order_date == date(2026, 3, 10)
