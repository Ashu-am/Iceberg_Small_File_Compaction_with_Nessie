-- ═══════════════════════════════════════════════════════════════
--  compaction_spark.sql — REFERENCE ONLY
--
--  ⚠️  DO NOT RUN THIS WITH: spark-sql -f compaction_spark.sql
--
--  WHY THIS FILE EXISTS:
--  This was the original approach — running compaction via Spark SQL
--  CLI using a .sql file. It failed due to a known spark-sql CLI bug:
--  the CLI strips single-quote escaping ('') from string literals
--  inside procedure where clauses before passing to the expression
--  parser. No amount of escaping in a .sql file fixes this.
--
--  WHAT TO USE INSTEAD:
--  Use compaction.py with spark-submit. Python handles its own
--  string escaping and passes the exact string to spark.sql()
--  without any stripping.
--
--  This file is kept for reference and documentation purposes only.
-- ═══════════════════════════════════════════════════════════════


-- ── STEP 1: REWRITE DATA FILES (compaction) ──────────────────────
-- Spark equivalent of Trino's ALTER TABLE EXECUTE optimize
-- partial-progress.enabled = true → breaks into 10 commit groups,
--   retries each group independently on concurrent write conflict
-- use-starting-sequence-number = false → allows commits alongside
--   concurrent ingestion writes
-- target-file-size-bytes = 134217728 → 128 MB per file
--
-- ⚠️  WHERE CLAUSE KNOWN ISSUES WITH spark-sql -f:
--   CAST(ig_timestamp AS DATE) = DATE '2026-05-20'  → fails (quotes stripped)
--   ig_timestamp >= TIMESTAMP '2026-05-20 00:00:00' → fails (quotes stripped)
--   ig_timestamp >= 2026-05-20T00:00:00             → fails (: not valid)
--   make_timestamp(2026, 5, 20, 0, 0, 0)            → fails (CAST injected)
--   All fail because spark-sql -f strips quote escaping before
--   the expression reaches Iceberg's filter parser.
--   Solution: use compaction.py instead.

-- CALL nessie.system.rewrite_data_files(
--     table => 'nessie.<your-schema>.<your-table>',
--     options => map(
--         'partial-progress.enabled',      'true',
--         'partial-progress.max-commits',  '10',
--         'use-starting-sequence-number',  'false',
--         'target-file-size-bytes',        '134217728'
--     ),
--     where => 'ig_timestamp >= TIMESTAMP ''2026-05-20 00:00:00''
--               AND ig_timestamp < TIMESTAMP ''2026-05-21 00:00:00'''
-- );


-- ── STEP 2: EXPIRE SNAPSHOTS ──────────────────────────────────────
-- NOTE: Use Trino for this instead — it works reliably

-- CALL nessie.system.expire_snapshots(
--     table => 'nessie.<your-schema>.<your-table>',
--     older_than => TIMESTAMP '2026-05-19 00:00:00',
--     retain_last => 1
-- );


-- ── STEP 3: REMOVE ORPHAN FILES ───────────────────────────────────
-- NOTE: Use Trino for this instead — it works reliably

-- CALL nessie.system.remove_orphan_files(
--     table => 'nessie.<your-schema>.<your-table>',
--     older_than => TIMESTAMP '2026-05-19 00:00:00'
-- );
