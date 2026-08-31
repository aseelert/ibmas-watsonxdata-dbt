-- -----------------------------------------------------------------------------
--  time_spine_daily.sql — daily calendar spine for the dbt Semantic Layer
--
--  Location  : models/silver/time_spine_daily.sql
--  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
--  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
--  Author    : Alexander Seelert — IBM Customer Success Engineer
--  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
--
--  Changelog :
--    v1.0 (2026-06-26) — Initial version. One row per calendar day for 2026.
--    v1.1 (2026-06-26) — Correctness fix: widened the spine to 2025-01-01 ..
--                        2026-12-31. The seed data spans BOTH years — customer
--                        signups start in Oct-2025 (raw_customers.csv) and orders
--                        run through 2026 (raw_orders.csv). A 2026-only spine
--                        silently dropped the 2025 signup dates from any
--                        MetricFlow time-based join, so the spine must cover the
--                        full range actually present in the seeds.
-- -----------------------------------------------------------------------------

-- WHAT: a gap-free list of calendar days, one row per day, used by the dbt
-- Semantic Layer / MetricFlow as the canonical time dimension to join metrics
-- onto (so days with no activity still appear in trend reports).
-- WHY this range: it must be a SUPERSET of every date in the seeds. Signups
-- begin 2025-10 and orders end 2026-06, so 2025-01-01 .. 2026-12-31 comfortably
-- covers both years with headroom. Widen these bounds if the seeds ever change.
select date_day
from unnest(
  sequence(date '2025-01-01', date '2026-12-31', interval '1' day)
) as t(date_day)
