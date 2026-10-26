-- Scale-test copies of the fact table for the clustering and sizing benchmarks.
--
-- 846K claims fit in a handful of micro-partitions, too few for pruning to show.
-- These tables repeat every claim 20 times (about 17M rows; `scale_copy` tells
-- the copies apart) so the effect of clustering is measurable. They are for
-- benchmarking only, not reporting.

USE SCHEMA CLAIMS_DB.GOLD;

-- Same rows in random order: dates are spread across all micro-partitions.
CREATE OR REPLACE TABLE FACT_CLAIMS_X20_UNCLUSTERED AS
SELECT f.*, g.scale_copy
  FROM FACT_CLAIMS f
 CROSS JOIN (SELECT SEQ4() AS scale_copy FROM TABLE(GENERATOR(ROWCOUNT => 20))) g
 ORDER BY RANDOM();

-- Same rows, clustered on the claim date. Loading in key order gives
-- well-clustered micro-partitions immediately; automatic clustering keeps
-- them that way as new data arrives.
CREATE OR REPLACE TABLE FACT_CLAIMS_X20
    CLUSTER BY (from_date_key)
AS
SELECT * FROM FACT_CLAIMS_X20_UNCLUSTERED ORDER BY from_date_key;
