"""
silver_orders.py — Phase 2 silver-layer curation job.

Reads raw demo.orders from the Iceberg REST catalog (seeded by iceberg-init),
applies minimal cleaning (non-null status, date cast), and writes the result
to demo.orders_silver as a managed Iceberg table.

Submit via:
    spark-ctrl job submit scripts/silver_orders.py --curate --wait

The --curate flag pre-wires all spark.sql.catalog.demo.* and
spark.hadoop.fs.s3a.* conf keys so no manual --conf flags are needed.
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("silver_orders")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. Read raw layer ──────────────────────────────────────────────────────────
raw = spark.table("demo.orders")
raw_count = raw.count()
print(f"[silver_orders] raw row count: {raw_count}")

# ── 2. Apply silver transforms ────────────────────────────────────────────────
#   • Drop rows with null status (shouldn't exist in seed data, but guard anyway)
#   • Cast created_at → order_date (DATE) for easier partitioning downstream
from pyspark.sql.functions import to_date, col

silver = (
    raw
    .filter(col("status").isNotNull())
    .select(
        "id",
        "user_id",
        "status",
        "total_amount",
        to_date("created_at").alias("order_date"),
    )
)

silver_count = silver.count()
print(f"[silver_orders] silver row count: {silver_count}")

assert silver_count > 0, "silver layer produced 0 rows — check iceberg-init seeding"
assert silver_count == raw_count, (
    f"row count mismatch: raw={raw_count} silver={silver_count}. "
    "All seed rows have a non-null status so counts should be equal."
)

# ── 3. Write to demo.orders_silver ────────────────────────────────────────────
(
    silver.writeTo("demo.orders_silver")
    .tableProperty("write.format.default", "parquet")
    .partitionedBy("order_date")
    .createOrReplace()
)

snap_count = spark.sql(
    "SELECT COUNT(*) AS n FROM demo.orders_silver.snapshots"
).collect()[0]["n"]

print(f"[silver_orders] demo.orders_silver written — {snap_count} snapshot(s)")
print("[silver_orders] done ✓")

spark.stop()
