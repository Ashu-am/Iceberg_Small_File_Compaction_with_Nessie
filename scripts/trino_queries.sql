-- ═══════════════════════════════════════════════════════════════
--  trino_queries.sql
--
--  All Trino SQL queries used during this project.
--  Use these for verification, stats, and debugging.
--
--  HOW TO RUN:
--  docker exec -it trino trino
--  Then paste queries one by one.
--
--  NOTE: Trino is NOT used for compaction (OPTIMIZE fails
--  on live tables — see README for full explanation).
--  Trino is used for everything else.
-- ═══════════════════════════════════════════════════════════════


-- ── CONNECTION VERIFICATION ───────────────────────────────────────

-- Check available catalogs (should show 'iceberg' and 'system')
SHOW CATALOGS;

-- Check available schemas in Nessie (confirms Nessie connection)
SHOW SCHEMAS IN iceberg;

-- Check tables in your schema
SHOW TABLES IN iceberg.<your-schema>;

-- Check table structure and column types
DESCRIBE iceberg.<your-schema>.<your-table>;

-- Check table creation SQL including partition spec
SHOW CREATE TABLE iceberg.<your-schema>.<your-table>;


-- ── DAILY STATS QUERIES ───────────────────────────────────────────

-- Get event count and time range for a specific full day
-- Replace <your-schema>, <your-table>, and date as needed
SELECT
    COUNT(*)          AS total_events,
    MIN(ig_timestamp) AS earliest_event,
    MAX(ig_timestamp) AS latest_event
FROM iceberg.<your-schema>.<your-table>
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-20';

-- Get event count for a specific time range (hours)
-- Useful for checking data after a specific event (e.g. property change)
SELECT
    COUNT(*)          AS total_events,
    MIN(ig_timestamp) AS earliest_event,
    MAX(ig_timestamp) AS latest_event
FROM iceberg.<your-schema>.<your-table>
WHERE ig_timestamp >= TIMESTAMP '2026-05-19 14:00:00'
AND ig_timestamp <  TIMESTAMP '2026-05-19 16:00:00';

-- Get today's event count
SELECT
    COUNT(*)          AS total_events,
    MIN(ig_timestamp) AS earliest_event,
    MAX(ig_timestamp) AS latest_event
FROM iceberg.<your-schema>.<your-table>
WHERE CAST("ig_timestamp" AS DATE) = CURRENT_DATE;


-- ── FILE STATE QUERIES (small file problem diagnosis) ─────────────

-- Overall file count across entire table
-- WARNING: On large tables (80k+ files) this takes 20-30 minutes
-- because Nessie must scan all file metadata before filtering
SELECT
    COUNT(*)                            AS file_count,
    SUM(file_size_in_bytes)/1048576     AS total_size_mb,
    AVG(file_size_in_bytes)/1024        AS avg_size_kb,
    SUM(record_count)                   AS total_events
FROM iceberg.<your-schema>."<your-table>$files";

-- File count for a specific date partition
-- Still slow on large tables (WHERE filters AFTER full metadata scan)
SELECT
    COUNT(*)                            AS file_count,
    SUM(file_size_in_bytes)/1048576     AS total_size_mb,
    AVG(file_size_in_bytes)/1024        AS avg_size_kb,
    SUM(record_count)                   AS total_events
FROM iceberg.<your-schema>."<your-table>$files"
WHERE file_path LIKE '%2026-05-20%';

-- Individual file listing with sizes
-- Use LIMIT to avoid overwhelming output
SELECT
    file_path,
    file_size_in_bytes/1024     AS size_kb,
    record_count,
    file_format
FROM iceberg.<your-schema>."<your-table>$files"
ORDER BY file_size_in_bytes DESC
LIMIT 20;


-- ── SNAPSHOT QUERIES ──────────────────────────────────────────────

-- Check current snapshot state
-- parent_id = NULL means broken snapshot chain (root cause of compaction failure)
SELECT
    snapshot_id,
    committed_at,
    operation,
    element_at(summary, 'total-data-files')  AS total_files,
    element_at(summary, 'total-records')     AS total_records,
    element_at(summary, 'added-data-files')  AS added_files,
    element_at(summary, 'deleted-data-files') AS deleted_files
FROM iceberg.<your-schema>."<your-table>$snapshots"
ORDER BY committed_at DESC;

-- Count total snapshots
SELECT COUNT(*) AS snapshot_count
FROM iceberg.<your-schema>."<your-table>$snapshots";

-- Check snapshot history chain
-- is_current_ancestor = true means that snapshot is in the active chain
-- parent_id = NULL means chain is broken (no parent to commit against)
SELECT
    snapshot_id,
    parent_id,
    made_current_at,
    is_current_ancestor
FROM iceberg.<your-schema>."<your-table>$history"
ORDER BY made_current_at DESC;


-- ── TABLE PROPERTIES ──────────────────────────────────────────────

-- Check all table properties
-- Look for gc.enabled and write.metadata.delete-after-commit.enabled
-- Both being 'true' causes snapshot history to be deleted automatically
SELECT * FROM iceberg.<your-schema>."<your-table>$properties";

-- Check partition spec
SELECT * FROM iceberg.<your-schema>."<your-table>$partitions" LIMIT 5;


-- ── MANIFEST FILES ────────────────────────────────────────────────

-- Check manifest file state
SELECT
    added_snapshot_id,
    added_data_files_count      AS files_added,
    existing_data_files_count   AS files_existing,
    deleted_data_files_count    AS files_deleted
FROM iceberg.<your-schema>."<your-table>$manifests"
ORDER BY added_snapshot_id DESC
LIMIT 10;


-- ── POST-COMPACTION VERIFICATION ─────────────────────────────────

-- Run AFTER compaction to confirm:
-- 1. File count dropped significantly
-- 2. Total events unchanged (zero data loss)
-- 3. Snapshot count reduced

-- File state after compaction
SELECT
    COUNT(*)                            AS file_count,
    SUM(file_size_in_bytes)/1048576     AS total_size_mb,
    AVG(file_size_in_bytes)/1048576     AS avg_size_mb,
    SUM(record_count)                   AS total_events
FROM iceberg.<your-schema>."<your-table>$files";

-- Data integrity check
SELECT COUNT(*) AS total_events
FROM iceberg.<your-schema>.<your-table>
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-20';

-- Snapshot count after expire
SELECT COUNT(*) AS snapshot_count
FROM iceberg.<your-schema>."<your-table>$snapshots";


-- ── EXPIRE SNAPSHOTS (run after compaction) ───────────────────────
-- Removes old Nessie metadata entries older than 1 day
-- Works fine in Trino — no concurrent write conflict for this operation

ALTER TABLE iceberg.<your-schema>.<your-table>
EXECUTE expire_snapshots(retention_threshold => '1d');


-- ── REMOVE ORPHAN FILES (run after expire_snapshots) ─────────────
-- Physically deletes old unreferenced Parquet files from S3
-- Works fine in Trino — no concurrent write conflict for this operation

ALTER TABLE iceberg.<your-schema>.<your-table>
EXECUTE remove_orphan_files(retention_threshold => '1d');


-- ── TRINO SYSTEM QUERIES ──────────────────────────────────────────

-- Check running/recent queries and their status
-- Useful for debugging failed queries
SELECT
    query_id,
    state,
    error_type,
    error_code,
    created,
    "end"
FROM system.runtime.queries
WHERE query_id = '<your-query-id>'
ORDER BY created DESC;

-- Check available columns in system.runtime.queries
DESCRIBE system.runtime.queries;
