"""
gold_daily_revenue.py — Phase 2 gold-layer curation job.

Aggregates demo.orders into a daily_revenue gold table using MERGE INTO
(idempotent upsert), then optionally compacts small files produced by
repeated incremental runs.

Submit via:
    spark-ctrl job submit scripts/gold_daily_revenue.py --curate --wait

Pass --compact as a Spark app argument to also run rewrite_data_files:
    spark-ctrl job submit scripts/gold_daily_revenue.py --curate --wait -- --compact
"""

import sys
from pyspark.sql import SparkSession

compact = "--compact" in sys.argv[1:]

spark = (
    SparkSession.builder
    .appName("gold_daily_revenue")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. Ensure gold table exists ────────────────────────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.daily_revenue (
        order_date   DATE,
        order_count  BIGINT,
        revenue      DOUBLE
    )
    USING iceberg
    PARTITIONED BY (order_date)
""")

snap_before = spark.sql(
    "SELECT COUNT(*) AS n FROM demo.daily_revenue.snapshots"
).collect()[0]["n"]
print(f"[gold_daily_revenue] snapshots before merge: {snap_before}")

# ── 2. Build the source aggregation ───────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW new_daily AS
    SELECT
        TO_DATE(created_at)  AS order_date,
        COUNT(*)             AS order_count,
        COALESCE(SUM(total_amount), 0.0) AS revenue
    FROM demo.orders
    WHERE status = 'shipped'
    GROUP BY TO_DATE(created_at)
""")

src_rows = spark.sql("SELECT COUNT(*) AS n FROM new_daily").collect()[0]["n"]
print(f"[gold_daily_revenue] source rows to merge: {src_rows}")

# ── 3. MERGE INTO (idempotent upsert) ─────────────────────────────────────────
spark.sql("""
    MERGE INTO demo.daily_revenue t
    USING new_daily s
    ON t.order_date = s.order_date
    WHEN MATCHED THEN UPDATE SET
        t.order_count = s.order_count,
        t.revenue     = s.revenue
    WHEN NOT MATCHED THEN INSERT *
""")

gold_count = spark.sql(
    "SELECT COUNT(*) AS n FROM demo.daily_revenue"
).collect()[0]["n"]
snap_after = spark.sql(
    "SELECT COUNT(*) AS n FROM demo.daily_revenue.snapshots"
).collect()[0]["n"]

print(f"[gold_daily_revenue] gold rows: {gold_count}  snapshots after: {snap_after}")
assert gold_count > 0, "gold table has 0 rows after MERGE INTO"
assert snap_after > snap_before, (
    f"snapshot count must increase after MERGE (before={snap_before} after={snap_after})"
)

# ── 4. Optional compaction ─────────────────────────────────────────────────────
if compact:
    print("[gold_daily_revenue] running rewrite_data_files...")
    result = spark.sql("""
        CALL demo.system.rewrite_data_files(
            table  => 'demo.daily_revenue',
            options => map('target-file-size-bytes', '134217728')
        )
    """)
    result.show()
    print("[gold_daily_revenue] compaction complete ✓")

print("[gold_daily_revenue] done ✓")
spark.stop()
