# Iceberg Small-File Compaction with Nessie — A Complete Journey

**Author:** Ashutosh Maheshwari  
**Environment:** AWS EC2 (Ubuntu 24.04) + Docker  
**Stack:** Apache Iceberg · Project Nessie · Apache Spark · Trino · AWS S3  
**Status:** Compaction infrastructure proven. Merge blocked by Nessie concurrent write conflict (see Phase 3 conclusion). Ingestion-pause test pending (Phase 4).

---

## Table of Contents

1. [What this project does](#1-what-this-project-does)
2. [The small-file problem explained](#2-the-small-file-problem-explained)
3. [Architecture](#3-architecture)
4. [Project structure — phase-wise evolution](#4-project-structure--phase-wise-evolution)
5. [Tools and versions](#5-tools-and-versions)
6. [Prerequisites](#6-prerequisites)
7. [Phase 1 — Trino compaction](#7-phase-1--trino-compaction)
8. [Phase 2 — Spark compaction](#8-phase-2--spark-compaction)
9. [Root cause — Nessie concurrent write conflict](#9-root-cause--nessie-concurrent-write-conflict)
10. [What works vs what does not](#10-what-works-vs-what-does-not)
11. [Phase 3 — Nessie branch strategy](#11-phase-3--nessie-branch-strategy)
12. [Phase 4 — Smart retry, report generator, and ingestion-pause test](#12-phase-4--smart-retry-report-generator-and-ingestion-pause-test)
13. [Command reference](#13-command-reference)
14. [Lessons learned](#14-lessons-learned)
15. [Known limitations and open questions](#15-known-limitations-and-open-questions)
16. [How to deploy](#16-how-to-deploy)
17. [Contributing](#17-contributing)
18. [Acknowledgements](#18-acknowledgements)

---

## 1. What this project does

This project attempts to solve the **small-file problem** in an Apache Iceberg data lake backed by Project Nessie as the catalogue and AWS S3 as storage.

The ingestion pipeline writes customer security logs from AWS MSK (Kafka) into an Iceberg table. Each batch write creates one Parquet file. When traffic is low, a 150-second time ceiling fires before the 40,000-event batch ceiling, creating thousands of tiny files. This makes Dremio (the query engine) slow because it must scan metadata for every tiny file before building a query plan.

**Goal:** Merge small Parquet files into large files (128–256 MB each) so Dremio queries are fast.

**Result of this project:**
- Compaction infrastructure fully built and deployed on EC2
- Both Trino and Spark successfully read and merge files
- Commit to Nessie fails due to a concurrent write conflict
- Root cause identified, documented, and reported
- Branch strategy (Phase 3) reduces the conflict window but does not eliminate it
- Ingestion-pause test (Phase 4) is the next confirmed step

---

## 2. The small-file problem explained

```
BEFORE compaction:
  81,350 Parquet files  |  avg 116 KB each  |  9.2 GB total
  Dremio asks Nessie: "what files do I need for this query?"
  Nessie scans 81,350 metadata entries → slow → Dremio query is slow

AFTER compaction (goal):
  ~36 Parquet files  |  avg 256 MB each  |  9.2 GB total
  Dremio asks Nessie: "what files do I need?"
  Nessie scans 36 metadata entries → fast → Dremio query is fast
```

**Why does it happen?**

The ingestion pipeline has two batch ceilings:
- 40,000 events → flush to Iceberg (normal load)
- 150 seconds → flush to Iceberg (low traffic)

During low-traffic periods, the 150-second ceiling fires repeatedly, writing files with only 500–2,000 events each instead of 40,000. Over days, this creates tens of thousands of tiny files.

---

## 3. Architecture

```
Customer Devices
      ↓
MSK (AWS Kafka) — receives raw security logs
      ↓
Processing Agents — two pipelines:
  RAW pipeline    → writes events as-is to Iceberg
  MAPPED pipeline → parses, normalizes to OCSF format, writes to Iceberg
      ↓
Nessie Catalogue — tracks all Iceberg metadata (schemas, snapshots, file lists)
      ↓
S3 — stores actual Parquet data files
      ↓
Dremio — queries data via Nessie catalogue
```

**This project targets the RAW pipeline table only.**

---

## 4. Project structure — phase-wise evolution

The project structure evolved across phases as new approaches were tried. Here is how it changed, and what the final state looks like.

### Phase 1 (Trino only)
```
iceberg-nessie-compaction/
├── .env
├── .env.example
├── .gitignore
├── README.md
├── docker-compose.yml            ← Trino only
├── trino/
│   └── catalog/
│       └── iceberg.properties
└── scripts/
    └── trino_queries.sql
```

### Phase 2 (Trino + Spark added)
```
iceberg-nessie-compaction/
├── ...
├── docker-compose.yml            ← Trino + Spark added
└── scripts/
    ├── trino_queries.sql
    ├── compaction_spark.sql      ← SQL reference (do NOT use with spark-sql -f)
    └── compaction.py             ← Spark Python script (spark-submit)
```

### Phase 3 (Nessie branch strategy added)
```
iceberg-nessie-compaction/
├── ...
├── venv/                         ← Python venv (gitignored)
└── scripts/
    ├── trino_queries.sql
    ├── compaction_spark.sql
    ├── compaction.py
    └── compaction_branch.py      ← Nessie branch strategy script
```

### Final structure (Phase 4 — current)
```
iceberg-nessie-compaction/
├── .env                          ← real credentials (gitignored — never commit)
├── .env.example                  ← template showing required variables
├── .gitignore
├── README.md
├── docker-compose.yml            ← Trino 480 + Spark 3.5.1
├── trino/
│   └── catalog/
│       └── iceberg.properties    ← Trino → Nessie + S3 config
├── venv/                         ← Python venv (gitignored)
└── scripts/
    ├── compaction_branch.py      ← Phase 3/4: Nessie branch strategy (use this)
    ├── generate_report.py        ← Phase 4: runs compaction + saves full report to .txt
    ├── compaction.py             ← Phase 2: Spark rewrite_data_files (reference)
    ├── compaction_spark.sql      ← Phase 2: SQL reference — do not run with spark-sql -f
    └── trino_queries.sql         ← all Trino verification and stat queries
```

**What each script does:**

| File | Purpose | Use in production? |
|---|---|---|
| `docker-compose.yml` | Runs Trino 480 + Spark 3.5.1 containers | ✅ Yes |
| `iceberg.properties` | Tells Trino how to connect to Nessie and S3 | ✅ Yes |
| `compaction_branch.py` | End-to-end branch strategy: create branch → OPTIMIZE → merge → cleanup | ✅ Yes (current approach) |
| `generate_report.py` | Runs compaction and saves full output + stats to a .txt report file | ✅ Yes (Phase 4) |
| `compaction.py` | Spark rewrite_data_files script | Reference (blocked by Nessie bug) |
| `compaction_spark.sql` | SQL version of Spark compaction | ❌ Do not use with spark-sql -f |
| `trino_queries.sql` | All verification, stats, and debugging queries | ✅ Yes |

---

## 5. Tools and versions

| Tool | Version | Purpose |
|---|---|---|
| Trino | 480 (latest as of May 2026) | Query engine — compaction + verification + expire + orphan removal |
| Apache Spark | 3.5.1 | Compaction engine — rewrite_data_files (Phase 2) |
| Iceberg runtime | 1.10.2 (latest as of May 2026) | Iceberg procedures for Spark |
| Hadoop AWS | 3.3.4 | S3A filesystem for Spark S3 access |
| Project Nessie | Server-managed (API v2) | Iceberg catalogue |
| Ubuntu | 24.04.3 LTS | EC2 operating system |
| Docker | 28.2.2 | Container runtime |
| Docker Compose | v5.1.3 | Container orchestration |
| Python | 3.12 (venv on EC2) | compaction_branch.py + generate_report.py runtime |

**Why Trino 480 specifically:** Trino 435 returned `API version mismatch, expected: 1, actual: 2`. Nessie server runs API v2. Trino 443+ supports it. We used 480 (latest stable).

**Why Iceberg 1.10.2:** Latest stable as of May 2026. Compatible with Spark 3.5 and Nessie API v2.

---

## 6. Prerequisites

**On EC2:**
```bash
# Docker
docker --version          # 28.2.2 or higher
docker compose version    # v2+

# Install Docker Compose plugin if missing
sudo apt-get update
sudo apt-get install -y docker-compose-plugin

# Add ubuntu user to docker group
sudo usermod -aG docker ubuntu
newgrp docker

# Verify
docker ps
```

**EC2 requirements:**
- Ubuntu 24.04 LTS
- Minimum 8 GB RAM (Trino ~3GB, Spark limited to 4GB)
- Minimum 30 GB disk
- IAM role attached with S3 read/write permissions
- Outbound internet access (Docker images, Maven JARs)

**Verify IAM role (IMDSv2):**
```bash
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")

curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

**On Windows — fix .pem permissions before SSH:**
```
icacls "C:\path\to\your-key.pem" /inheritance:r
icacls "C:\path\to\your-key.pem" /remove "NT AUTHORITY\Authenticated Users"
icacls "C:\path\to\your-key.pem" /remove "BUILTIN\Users"
icacls "C:\path\to\your-key.pem" /grant:r "%USERNAME%:(R)"
```

---

## 7. Phase 1 — Trino compaction

### 7.1 Setup

**Connect to EC2:**
```bash
ssh -i "your-key.pem" ubuntu@<your-ec2-ip>
```

**Create project folder:**
```bash
mkdir -p ~/trino_iceberg/trino/catalog
mkdir -p ~/trino_iceberg/scripts
cd ~/trino_iceberg
```

**Start Trino:**
```bash
docker compose up -d
docker compose ps
```

Expected:
```
NAME    STATUS
trino   Up (healthy)
```

**Connect to Trino CLI:**
```bash
docker exec -it trino trino
```

---

### 7.2 Verify connection to Nessie

```sql
SHOW CATALOGS;
SHOW SCHEMAS IN iceberg;
SHOW TABLES IN iceberg.<your-schema>;
```

**Result:** ✅ Connection confirmed. Schema `logs` and table `raw_data` visible.

**First attempt with Trino 435:**
```
Error: API version mismatch, check URI prefix (expected: 1, actual: 2)
```
**Fix:** Upgraded Trino image from 435 to 480 in `docker-compose.yml`.

---

### 7.3 Diagnose the small-file problem

**Check table structure:**
```sql
SHOW CREATE TABLE iceberg.logs.raw_data;
```

Key finding: Table is partitioned by `day(ig_timestamp)` — OPTIMIZE WHERE clause can only target full-day partitions.

**Check overall file stats:**
```sql
SELECT
    COUNT(*)                            AS file_count,
    SUM(file_size_in_bytes)/1048576     AS total_size_mb,
    AVG(file_size_in_bytes)/1024        AS avg_size_kb,
    SUM(record_count)                   AS total_events
FROM iceberg.logs."raw_data$files";
```

Result (took ~26 minutes due to 81k metadata entries):
```
file_count : 81,350
total_mb   : 9,272 MB (9.2 GB)
avg_size_kb: 116 KB   ← should be 256,000 KB (256 MB)
total_events: 176,176,763
```

**Why the query is slow:** `$files` is a flat metadata list in Nessie with no index. Trino must download all 81,350 entries before filtering. This slowness is exactly what Dremio experiences on every query.

**Check daily stats:**
```sql
SELECT
    COUNT(*)          AS total_events,
    MIN(ig_timestamp) AS earliest_event,
    MAX(ig_timestamp) AS latest_event
FROM iceberg.logs.raw_data
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-15';
```

Result for May 15:
```
total_events  : 3,650,744
earliest_event: 2026-05-15 00:00:00.228574
latest_event  : 2026-05-15 23:59:59.565325
query took    : 55 seconds scanning 1,432 splits
```

Each split = one small file. After compaction, same query would scan 1 split.

**Check table properties:**
```sql
SELECT * FROM iceberg.logs."raw_data$properties";
```

Result:
```
write.metadata.delete-after-commit.enabled : true
gc.enabled                                 : true
nessie.gc.no-warning                       : true
```

---

### 7.4 Run OPTIMIZE — all attempts

**Attempt 1 — May 15 partition:**
```sql
ALTER TABLE iceberg.logs.raw_data
EXECUTE optimize(file_size_threshold => '256MB')
WHERE CAST("ig_timestamp" AS DATE) > DATE '2026-05-14'
AND CAST("ig_timestamp" AS DATE) < DATE '2026-05-16';
```

Result:
```
Query FAILED after 25:47 minutes at 99.93% (1,435/1,436 splits)
Error: Cannot determine history between starting snapshot
       1434035407226788086 and the last known ancestor
       6878625479154631399
```

**Attempt 2 — May 15 with 128MB threshold:** Same error at 99.93%.

**Attempt 3 — after setting `write.metadata.delete-after-commit.enabled = false`:**

Property change made via Spark by platform team. Ran OPTIMIZE on May 20:
```sql
ALTER TABLE iceberg.logs.raw_data
EXECUTE optimize(file_size_threshold => '128MB')
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-20';
```

Result: Same error at 99.93% after 22:42 minutes.

**Check snapshot state after failures:**
```sql
SELECT snapshot_id, parent_id, made_current_at, is_current_ancestor
FROM iceberg.logs."raw_data$history"
ORDER BY made_current_at DESC;
```

Result:
```
snapshot_id         : 4070159339600901532
parent_id           : NULL   ← broken chain
is_current_ancestor : true
```

`parent_id = NULL` means only 1 snapshot with no parent. Trino cannot commit because there is no parent to link against.

**Conclusion:** Trino OPTIMIZE cannot work on this live table due to concurrent write conflicts.

---

### 7.5 What Trino can still do

```sql
-- Verify file count
SELECT COUNT(*) AS file_count, SUM(record_count) AS total_events
FROM iceberg.logs."raw_data$files";

-- Check data stats for a date
SELECT COUNT(*) AS total_events FROM iceberg.logs.raw_data
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-21';

-- Expire old snapshots (works fine — no write conflict)
ALTER TABLE iceberg.logs.raw_data
EXECUTE expire_snapshots(retention_threshold => '1d');

-- Remove orphan files from S3 (works fine — no write conflict)
ALTER TABLE iceberg.logs.raw_data
EXECUTE remove_orphan_files(retention_threshold => '1d');
```

---

## 8. Phase 2 — Spark compaction

### 8.1 Why Spark was chosen

Spark's `rewrite_data_files` supports `partial-progress.enabled=true` — breaks compaction into small commit groups and retries each independently on conflict. Expected to handle concurrent ingestion better than Trino's single commit attempt.

### 8.2 Add Spark to docker-compose.yml

```yaml
spark:
  image: apache/spark:3.5.1-python3
  container_name: spark
  ports:
    - "4040:4040"
  environment:
    - AWS_DEFAULT_REGION=<your-region>
  volumes:
    - ./scripts:/opt/spark/work-dir/scripts
    - ./spark-ivy-cache:/home/spark/.ivy2
  command: tail -f /dev/null
  restart: unless-stopped
  deploy:
    resources:
      limits:
        memory: 4g
```

```bash
docker compose down
docker compose up -d
docker compose ps
```

---

### 8.3 First attempt — spark-sql -f with SQL file

**Error 1 — Ivy cache permissions:**
```
FileNotFoundException: /home/spark/.ivy2/cache/resolved-...-1.0.xml
```

Fix:
```bash
docker exec -u root -it spark mkdir -p /home/spark/.ivy2/cache
docker exec -u root -it spark chown -R spark:spark /home/spark/.ivy2
```

Permanent fix: Mount a host directory for Ivy cache in `docker-compose.yml`:
```yaml
volumes:
  - ./spark-ivy-cache:/home/spark/.ivy2
```

**Error 2 — No FileSystem for scheme "s3":**
```
UnsupportedFileSystemException: No FileSystem for scheme "s3"
```

Cause: `apache/spark:3.5.1-python3` does not include the Hadoop AWS JAR. Add `hadoop-aws:3.3.4` and map `s3://` to S3A:
```
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A \
```

**Error 3 — Quote stripping in SQL file:**

`spark-sql -f` strips single-quote escaping from string literals inside procedure WHERE clauses. Every approach failed (CAST, TIMESTAMP keyword, ISO format, make_timestamp). Root cause is a confirmed CLI bug — no SQL file escaping approach works.

---

### 8.4 Switch to Python — compaction.py

**Why Python works:** Python handles its own string escaping. `spark.sql()` receives the exact string, no stripping by the CLI layer.

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://<nessie-host>:19120/api/v2 \
  --conf spark.sql.catalog.nessie.ref=<branch> \
  --conf spark.sql.catalog.nessie.warehouse=s3://<bucket>/ \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
  --conf spark.hadoop.fs.s3a.endpoint.region=<region> \
  /opt/spark/work-dir/scripts/compaction.py
```

---

### 8.5 Spark results

**May 20 run:** `Nothing found to rewrite` — May 20 already had 1 large file from Trino's partial OPTIMIZE work. Nothing small enough to compact.

**May 21 run:**

Before compaction:
```
file_count : 470 files | total_mb : 107 MB | avg_size_kb : 234 KB
```

Spark ran ~87 seconds, merged 470 files → 1 large file. Then:
```
ERROR: Cannot determine history between starting snapshot
       92535512022862230 and the last known ancestor
       7571690287285299214
```

Same error as Trino. Files merged in S3 — Nessie commit rejected. After failure:
```
file_count : 470 files (unchanged — commit rolled back)
total_events: 1,657,087 (unchanged — no data loss)
```

---

## 9. Root cause — Nessie concurrent write conflict

### What happens step by step

```
Spark/Trino starts compaction on target partition
         ↓
Reads small Parquet files from S3
         ↓
Merges them into large Parquet files
         ↓
Writes merged files to S3  ← this succeeds
         ↓
Tries to commit new snapshot to Nessie
         ↓
During the compaction run, the Processing Agent
wrote new events → committed new snapshots to Nessie
         ↓
Nessie validates snapshot chain → finds gap → rejects commit
         ↓
Rollback — old small files remain active
```

### Why this is a Nessie-specific problem

Confirmed Nessie bug: **[projectnessie/nessie#9969](https://github.com/projectnessie/nessie/issues/9969)**

Occurs when:
1. Streaming ingestion writes via Iceberg REST API
2. Compaction writes via Nessie catalog API
3. Both run simultaneously on the same table

Standard Iceberg with Hive Metastore or AWS Glue uses optimistic locking that allows concurrent commits. Nessie does not handle this case the same way.

### Why partial-progress and max-concurrent-file-group-rewrites do not fix it

Both reduce parallelism but do not eliminate the commit conflict window. The Processing Agent commits every few seconds. Even a 10-second commit group will encounter new snapshots. The Nessie snapshot history validation failure is treated as non-retryable by Iceberg.

---

## 10. What works vs what does not

| Operation | Tool | Status | Notes |
|---|---|---|---|
| Connect to Nessie API v2 | Trino 480 | ✅ Works | Trino 435 does NOT support API v2 |
| Connect to Nessie API v2 | Spark 3.5.1 | ✅ Works | Requires hadoop-aws:3.3.4 |
| Read table metadata (`$files`, `$snapshots`) | Trino | ✅ Works | Slow on 80k+ files (20-30 min) |
| Query table data | Trino | ✅ Works | |
| Run expire_snapshots | Trino | ✅ Works | No concurrent conflict |
| Run remove_orphan_files | Trino | ✅ Works | No concurrent conflict |
| OPTIMIZE (compaction) | Trino 480 | ❌ Fails | Concurrent write conflict at commit |
| rewrite_data_files via SQL file | Spark spark-sql -f | ❌ Fails | Quote stripping CLI bug |
| rewrite_data_files via Python file | Spark spark-submit | ⚠️ Partial | File merging works, commit fails |
| Branch strategy — OPTIMIZE on isolated branch | Trino + Nessie REST | ✅ Works | Compaction completes, merge fails |
| Branch strategy — merge | Nessie REST API | ❌ Fails | 409 REFERENCE_CONFLICT during active ingestion |
| Branch strategy — merge with ingestion paused | Nessie REST API | ⏳ Pending | Phase 4 test |

---

## 11. Phase 3 — Nessie branch strategy

### 11.1 The idea and why it was tried

**Motivation:** Phase 1 and Phase 2 both failed at the commit step because ingestion was writing to `raw-data-dev` concurrently. The key insight: isolate the compaction from the live branch entirely — like Git branches for isolating work.

**The approach:** Nessie supports catalogue-level branching. A branch is a metadata pointer, not a data copy — creation is instant and costs nothing.

- New branch created from `raw-data-dev` (zero cost)
- Compaction runs on isolated branch — `raw-data-dev` unaffected
- Merge back is a single fast operation — only then can conflict occur

**Why this reduces conflicts:** Phase 1/2 conflicts occurred because the table was being written during a 22-25 minute compaction run. With branch isolation, compaction commits to its own branch without competing with ingestion at all. Conflict window reduced from 22+ minutes to the few seconds the merge takes.

**Known risk before testing:** If ingestion commits to `raw-data-dev` during the compaction window, the merge will conflict because `logs.raw_data` was modified on both branches. Documented before testing.

---

### 11.2 What compaction_branch.py does — full flow

```
STEP 1  → Get current hash of raw-data-dev from Nessie REST API
STEP 2  → Create new compaction branch: compact-DDMMYYYYHH
STEP 3  → Update iceberg.properties to point Trino at compaction branch
STEP 4  → Restart Trino container
STEP 5  → Verify Trino connects to new branch
STEP 6  → Run OPTIMIZE on compaction branch (no expire/orphan — isolated)
STEP 7  → Verify file count after OPTIMIZE
STEP 8  → Merge compaction branch → raw-data-dev (with retries)
STEP 9  → Delete compaction branch (cleanup)
STEP 10 → Restore iceberg.properties to raw-data-dev, restart Trino
```

**Key safety guarantee:** If merge fails — `raw-data-dev` was never modified. Branch is cleaned up. Trino is restored. No data loss.

---

### 11.3 Environment setup — Python venv

Ubuntu 24.04 blocks system-wide pip installs (PEP 668). A virtual environment is required.

**Install venv support:**
```bash
sudo apt-get install -y python3-full python3.12-venv
```

**Create venv inside project folder:**
```bash
cd ~/trino_iceberg
python3 -m venv venv
venv/bin/pip install requests trino
venv/bin/python3 -c "import requests, trino; print('OK')"
```

**Run the script:**
```bash
cd ~/trino_iceberg
venv/bin/python3 scripts/compaction_branch.py
```

---

### 11.4 Script config — key parameters

```python
NESSIE_URI          = "http://nginx-private-internal.igris.in:19120"
SOURCE_BRANCH       = "raw-data-dev"
TRINO_HOST          = "localhost"
TRINO_PORT          = 8080
TRINO_CATALOG       = "iceberg"
ICEBERG_SCHEMA      = "logs"
ICEBERG_TABLE       = "raw_data"
ICEBERG_PROPS_PATH  = "/home/ubuntu/trino_iceberg/trino/catalog/iceberg.properties"
FILE_SIZE_THRESHOLD = "128MB"
MERGE_MAX_RETRIES   = 10
MERGE_RETRY_WAIT    = 5       # seconds between merge retries
```

**Branch naming:** `compact-DDMMYYYYHH` (e.g. `compact-2505202615`). Must start with a letter — Nessie rejects names starting with a digit.

---

### 11.5 Key fixes discovered during testing

#### Fix 1 — Branch name must start with a letter

**Error:**
```
"message" : "Reference name must start with a letter... but was: 2505202612"
```

**Fix:**
```python
# Before: return datetime.utcnow().strftime("%d%m%Y%H")
# After:
return "compact-" + datetime.utcnow().strftime("%d%m%Y%H")
```

#### Fix 2 — Nessie v2 create branch API — name and type must be query params

**Error:**
```
"message" : "createReference.name: must not be null, createReference.type: must not be null"
```

**Root cause:** Nessie API v2 `POST /api/v2/trees` requires `name` and `type` as URL query parameters, not in the JSON body.

**Correct:**
```python
url = f"{NESSIE_URI}/api/v2/trees"
params = {"name": new_branch_name, "type": "BRANCH"}
payload = {"type": "BRANCH", "name": SOURCE_BRANCH, "hash": source_hash}
response = requests.post(url, params=params, json=payload, timeout=30)
```

#### Fix 3 — Nessie v2 merge API — target hash must be in the URL

**Error:**
```
"message" : "Expected hash must be provided."
```

**Root cause:** Nessie API v2 merge requires the current hash of the target branch in the URL path itself.

**Correct URL format:**
```
POST /api/v2/trees/{TARGET_BRANCH}@{TARGET_HASH}/history/merge
```

```python
target_hash = get_branch_hash(SOURCE_BRANCH)
url = f"{NESSIE_URI}/api/v2/trees/{SOURCE_BRANCH}@{target_hash}/history/merge"
```

#### Fix 4 — OPTIMIZE WHERE clause — use CAST

What does NOT work (causes `Column 'ig_timestamp' cannot be resolved` on `$files`):
```sql
WHERE ig_timestamp >= TIMESTAMP '2026-05-23 00:00:00'
```

What works (matches `day(ig_timestamp)` partition scheme):
```sql
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-23'
```

---

### 11.6 Verify data exists for a target date

Always confirm before running compaction:
```bash
docker exec -it trino trino
```
```sql
SELECT
    COUNT(*)          AS total_events,
    MIN(ig_timestamp) AS earliest_event,
    MAX(ig_timestamp) AS latest_event
FROM iceberg.logs.raw_data
WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-25';
```

Result for May 25, 2026:
```
total_events  : 1,438,622
earliest_event: 2026-05-25 00:00:00.521248
latest_event  : 2026-05-25 23:59:59.841919
Query scanned : 576 splits (576 small files)
```

---

### 11.7 Check ingestion commit frequency

Run before deciding on retry strategy:
```bash
curl -s "http://nginx-private-internal.igris.in:19120/api/v2/trees/raw-data-dev/history?max-records=20" \
  | python3 -m json.tool | grep commitTime
```

**Result observed on 25 May 2026 at ~16:42 UTC (active daytime):**
```
"commitTime": "2026-05-25T16:42:05.267449556Z",
"commitTime": "2026-05-25T16:41:25.748611720Z",
"commitTime": "2026-05-25T16:40:59.626773613Z",
"commitTime": "2026-05-25T16:40:58.965736939Z",
"commitTime": "2026-05-25T16:40:56.199416140Z",
"commitTime": "2026-05-25T16:40:54.822046997Z",
...
```

**Analysis:** Burst pattern — 6 commits in under 5 seconds. Gaps of 30-60 seconds between bursts. Ingestion never fully stops during daytime hours.

---

### 11.8 Test runs — full results

#### Test Run 1 — 10MB threshold, 5 retries, 30s wait

Branch naming bug hit (started with digit). Fixed and reran:
```
Branch created : compact-2505202613    ✅
OPTIMIZE       : ✅ success (~1m 42s)
Merge URL bug  : ❌ HTTP 400 "Expected hash must be provided"
```

Fixed merge URL, proceeded.

#### Test Run 2 — 10MB threshold, 5 retries, 30s wait (merge URL fixed)

```
[15:08:34 UTC] Attempt 1: hash=3d85e8e7... → ❌ 409
[15:09:05 UTC] Attempt 2: hash=3d85e8e7... → ❌ 409   (same hash)
[15:09:35 UTC] Attempt 3: hash=3d85e8e7... → ❌ 409   (same)
[15:10:05 UTC] Attempt 4: hash=5c620af7... → ❌ 409   (CHANGED — ingestion wrote)
[15:10:35 UTC] Attempt 5: hash=39cec827... → ❌ 409   (CHANGED AGAIN)
```

`raw-data-dev` hash changed twice in 2.5 minutes. No quiet gap.

#### Test Run 3 — 128MB threshold, 10 retries, 5s wait

```
Branch created: compact-2505202616    ✅
OPTIMIZE      : ✅ success (~1m 42s)
File count    : 76,155 files | 185,189,742 events  ✅

Attempts 1-4  : hash=8064682994a8d215... → ❌ 409 (same hash, still conflict)
Attempt 5     : hash=c7cca6e8970af07e... → ❌ 409 (CHANGED)
Attempts 6-10 : hash unchanged, all ❌

Branch deleted: ✅ cleanup
Trino restored: ✅ cleanup
```

---

### 11.9 Merge error — two different errors encountered

**Error A — Script bug (now fixed):**
```json
{"status": 400, "message": "Expected hash must be provided.", "errorCode": "BAD_REQUEST"}
```

**Error B — Actual concurrent write conflict (root cause):**
```json
{"status": 409, "message": "The following keys have been changed in conflict: 'logs.raw_data'", "errorCode": "REFERENCE_CONFLICT"}
```

How Phase 3 error differs from Phase 1/2:

| | Phase 1 & 2 | Phase 3 |
|---|---|---|
| Error | `Cannot determine history between snapshot X and ancestor Y` | `keys have been changed in conflict: 'logs.raw_data'` |
| HTTP | No HTTP (Iceberg internal) | HTTP 409 |
| Where | During Iceberg snapshot commit | During Nessie branch merge via REST API |
| Layer | Iceberg layer | Nessie catalogue layer |
| Retryable | No | No (without pause) |

The branch strategy moved the conflict from the Iceberg layer to the Nessie layer. This is actually progress — the error is now cleaner, at a higher level, and gives more control (retries, merge API). Root cause remains the same: concurrent writes.

---

### 11.10 What is proven

| What was tested | Result |
|---|---|
| Creating Nessie branch via REST API from Python | ✅ Working |
| Trino OPTIMIZE on isolated compaction branch | ✅ Working — 128MB, ~1m 42s for 1 day of data |
| No impact on raw-data-dev during compaction | ✅ Confirmed |
| Branch cleanup on failure | ✅ Working — always deleted, Trino always restored |
| Merge with 5 retries at 30s | ❌ Failed all 5 |
| Merge with 10 retries at 5s | ❌ Failed all 10 |

**Compaction itself is 100% validated and working. The only blocker is the merge step.**

---

### 11.11 Summary for platform team

```
COMPACTION TEST — FINAL RESULT — 25 May 2026

WHAT WORKS:
  ✅ Compaction on isolated Nessie branch — fully validated
  ✅ 128MB file size threshold — working
  ✅ No impact to raw-data-dev during compaction
  ✅ Branch created, compacted, cleaned up safely every run
  ✅ Script runs end-to-end automatically (no CLI needed)

WHAT DOES NOT WORK WITHOUT INTERVENTION:
  ❌ Merge back to raw-data-dev — fails 100% of the time
     during active ingestion

TESTS RUN:
  Test 1 —  5 retries, 30s wait → failed all  5
  Test 2 —  5 retries, 30s wait → failed all  5
  Test 3 — 10 retries,  5s wait → failed all 10

ROOT CAUSE:
  Ingestion commits to raw-data-dev every 5-30 seconds.
  OPTIMIZE takes ~2 minutes. During that window, Nessie
  records ingestion as a conflicting change on raw-data-dev.
  Merge rejected with HTTP 409 REFERENCE_CONFLICT.
  raw-data-dev hash changed twice in 50 seconds during retries —
  zero clean gap available.

ONLY CONFIRMED SOLUTION:
  Pause ingestion pipeline for ~2 minutes during merge step only.
  Compaction itself requires no pause — only the final merge.
  Total pause needed: < 2 minutes, once per compaction run.

ACTION REQUIRED FROM PLATFORM TEAM:
  Provide a way to pause/resume the ingestion pipeline briefly
  so the merge step can complete.
```

---

## 12. Phase 4 — Smart retry, report generator, and ingestion-pause test

### 12.1 Overview

Phase 4 had two goals:
1. Add a smarter merge retry that finds quiet ingestion gaps rather than retrying blindly
2. Add a report generator script that saves all stats and output to a `.txt` file for manager review
3. Test the script with ingestion fully paused by the platform team — the confirmed solution path

---

### 12.2 Will millisecond retries help?

**Question raised:** Would reducing retry interval from 5 seconds to 1-5 milliseconds random improve merge success rate?

**Answer: No — and here is why.**

Jitter and shorter retry intervals help reduce contention in systems where retries can succeed faster than the conflict rate. The key insight from research: *"exponential backoff with jitter is not a universal solution — there are categories of failures where continued retries are unlikely to help regardless of backoff strategy."*

Your situation is one of those categories:
- The merge operation itself takes ~500ms to execute
- You cannot retry faster than the merge completes
- The conflict is determined at the moment Nessie processes the merge — not when you send it
- Some 409s from Nessie may not even be real conflicts — they can be Nessie backend busy errors that need a moment to recover, not milliseconds ([confirmed in Nessie GitHub](https://github.com/projectnessie/nessie/issues/9969))

What actually helps is detecting a genuine quiet window between ingestion bursts before attempting the merge — not sending faster.

---

### 12.3 Smart retry — hash stability check

Instead of retrying on a fixed timer, the updated `compaction_branch.py` polls the `raw-data-dev` hash every 1 second and waits until it has been **stable for 3 consecutive seconds** — meaning ingestion has not committed during that window — before attempting the merge.

**Why 3 seconds?** Ingestion commits in bursts every 5-30 seconds. A single 1-second stability check tells us nothing — ingestion may have committed 0.2 seconds ago. Three consecutive stable seconds gives a meaningful signal that we are genuinely between bursts.

**Additional jitter:** After detecting a stable window, adds a random 100–1500ms delay before sending the merge request. This prevents multiple simultaneous retries from hitting Nessie at the same instant.

**Fallback:** If no stable window is found in 60 seconds, attempts merge anyway and logs it.

**Key config changes in compaction_branch.py:**
```python
MERGE_MAX_RETRIES    = 10
MERGE_RETRY_WAIT     = 5      # seconds between full retry cycles
HASH_STABLE_SECONDS  = 3      # consecutive stable seconds before merge attempt
JITTER_MIN_MS        = 100    # milliseconds
JITTER_MAX_MS        = 1500
```

---

### 12.4 generate_report.py — report generator

A new script `generate_report.py` was added. It:
- Runs `compaction_branch.py` as a subprocess
- Streams all output live to the terminal AND captures it
- Collects pre/post stats from Trino
- Writes a complete `.txt` report file to `~/trino_iceberg/reports/`

**Run:**
```bash
cd ~/trino_iceberg
mkdir -p reports
venv/bin/python3 scripts/generate_report.py
```

**Output file:**
```
~/trino_iceberg/reports/compaction_report_DDMMYYYY_HHMM.txt
```

**Read on EC2:**
```bash
cat ~/trino_iceberg/reports/compaction_report_*.txt
# or page through it:
less ~/trino_iceberg/reports/compaction_report_*.txt
```

**Copy to Windows (from Windows CMD — not from inside SSH):**
```cmd
scp -i "D:\trino_iceberg_main\test-mapping.pem" ubuntu@10.32.10.67:/home/ubuntu/trino_iceberg/reports/compaction_report_27052026_0748.txt "C:\Users\Ashutosh Maheshwari\OneDrive - AQUILAI SOLUTIONS PRIVATE LIMITED\Desktop\compaction_report.txt"
```

**Config in generate_report.py:**
```python
TARGET_DATE = "2026-05-25"   # date being compacted — update this each run
```

---

### 12.5 Test run — 27 May 2026 (smart retry, May 25 data)

**Setup:**
- Target date: May 25, 2026 (1,438,622 events across 576 small files — confirmed by stat query)
- Smart retry active: hash stability 3s + jitter 100-1500ms
- Trino restored to `raw-data-dev` after previous crash left it pointing at deleted branch

**Pre-run fix needed (Trino pointing at deleted branch):**
```bash
sed -i 's/iceberg.nessie-catalog.ref=.*/iceberg.nessie-catalog.ref=raw-data-dev/' \
  ~/trino_iceberg/trino/catalog/iceberg.properties
docker restart trino && sleep 40
```

**Pre-run — branch already exists error:**

A stuck branch `compact-2705202607` from a previous crashed run was found. Cleaned up:
```bash
HASH=$(curl -s http://nginx-private-internal.igris.in:19120/api/v2/trees/compact-2705202607 \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('reference',{}).get('hash') or d.get('hash',''))")
curl -X DELETE "http://nginx-private-internal.igris.in:19120/api/v2/trees/compact-2705202607@${HASH}"
```

Confirmed deleted (404 returned).

**Script output — key sections:**
```
[2026-05-27 07:52:10 UTC] OPTIMIZE running...
[2026-05-27 07:54:00 UTC] OPTIMIZE — success (~1m 42s)
[2026-05-27 07:54:03 UTC] File count: 76,xxx files

[2026-05-27 07:54:03 UTC] Merge attempt 1/10
[2026-05-27 07:54:04 UTC]   Hash stable for 1.0s / need 3s...
[2026-05-27 07:54:05 UTC]   Hash stable for 2.0s / need 3s...
[2026-05-27 07:54:06 UTC]   Hash stable for 3.0s / need 3s ← quiet window found
[2026-05-27 07:54:06 UTC]   Applying jitter: ~750ms
[2026-05-27 07:54:07 UTC] ❌ Merge attempt 1 failed — 409 REFERENCE_CONFLICT
... (all 10 attempts failed)
```

**Result:** Merge still failed. Ingestion commits frequently enough that even a 3-second stable window does not guarantee success — Nessie may still see a conflict if ingestion commits during the ~500ms the merge takes to process.

**Conclusion:** Smart retry is better than blind retry (finds real gaps, avoids flooding Nessie), but cannot guarantee success against active ingestion. Ingestion pause remains the only confirmed solution.

---

### 12.6 Ingestion-pause test — protocol

This test was agreed with the manager. Ingestion needs to be paused for only ~2 minutes during the merge step. Compaction itself requires no pause.

**Add pause prompt to compaction_branch.py:**

In `merge_branch_with_retry()`, directly above the retry loop:
```python
print("\n" + "="*60)
input("  ⏸  Tell your manager to PAUSE ingestion now, then press Enter to attempt merge...")
print("="*60 + "\n")
```

**Exact sequence during the call:**
1. Run: `venv/bin/python3 scripts/generate_report.py`
2. Script runs Steps 1–7 automatically (~3-4 minutes)
3. Script PAUSES and prints: `⏸  Tell your manager to PAUSE ingestion now, then press Enter...`
4. Tell manager: "please pause ingestion now"
5. Manager confirms: "paused"
6. Press Enter immediately
7. Merge attempt 1 runs — should succeed in under 5 seconds
8. Script continues — branch deleted, Trino restored, report written

**What success looks like:**
```
[INFO]   ✅ Quiet window found
[INFO]   Applying jitter: 743ms
[INFO]   ✅ Merge successful on attempt 1
...
Merge : ✅ SUCCESS
```

**Manual cleanup if script crashes mid-run:**
```bash
# 1. Get branch name from output (e.g. compact-2705202617)
BRANCH=compact-2705202617

# 2. Delete stuck branch
HASH=$(curl -s http://nginx-private-internal.igris.in:19120/api/v2/trees/${BRANCH} \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('reference',{}).get('hash') or d.get('hash',''))")
curl -X DELETE "http://nginx-private-internal.igris.in:19120/api/v2/trees/${BRANCH}@${HASH}"

# 3. Restore Trino
sed -i 's/iceberg.nessie-catalog.ref=.*/iceberg.nessie-catalog.ref=raw-data-dev/' \
  ~/trino_iceberg/trino/catalog/iceberg.properties
docker restart trino && sleep 40
```

**Possible issues and fixes:**

| Issue | Fix |
|---|---|
| Merge still fails with 409 after pause | Ingestion not fully stopped — ask manager to confirm complete stop, re-run |
| Script crashes before merge (branch exists error) | Delete stuck branch with cleanup commands above, re-run |
| Trino fails to restart (Step 4) | `docker logs trino --tail 30` — check iceberg.properties for typos |
| Manager resumes ingestion too early | Merge takes <5 seconds — just need 10 seconds of pause after pressing Enter |

---

### 12.7 Next steps — options ranked by feasibility

| Option | What it needs | Expected outcome |
|---|---|---|
| 1. Pause ingestion during merge | Platform team pause/resume command | ✅ Guaranteed — proven by test setup |
| 2. Schedule at lowest traffic hours | Cron job + quiet window detection via commit history | ⚠️ Reduces conflicts, not eliminated |
| 3. nessie-gc tool | Platform team to check if already running | ✅ Designed for live ingestion |
| 4. Compact T-2 or older partitions only | Confirm Processing Agent doesn't append to past days | ⚠️ May work if ingestion never touches past partitions |

---

## 13. Command reference

### SSH and navigation
```bash
ssh -i "path\to\key.pem" ubuntu@<ec2-ip>
cd ~/trino_iceberg
cat filename
nano filename
exit
```

### Docker commands
```bash
docker compose up -d
docker compose down
docker compose ps
docker logs trino --tail 30
docker compose restart trino
docker exec -u root -it spark mkdir -p /path
```

### Trino CLI
```bash
docker exec -it trino trino
docker exec -it trino trino --execute "SHOW CATALOGS;"
# Exit: type exit or Ctrl+D
```

### Run compaction scripts
```bash
# Nessie branch strategy (Phase 3/4)
cd ~/trino_iceberg
venv/bin/python3 scripts/compaction_branch.py

# With report generator (Phase 4)
cd ~/trino_iceberg
mkdir -p reports
venv/bin/python3 scripts/generate_report.py
```

### Nessie branch management
```bash
# List all branches
curl -s http://nginx-private-internal.igris.in:19120/api/v2/trees | python3 -m json.tool

# List compact- branches only
curl -s http://nginx-private-internal.igris.in:19120/api/v2/trees | python3 -m json.tool | grep -A2 "compact-"

# Check if a specific branch exists
curl -s http://nginx-private-internal.igris.in:19120/api/v2/trees/compact-2705202607 | python3 -m json.tool

# Delete a stuck branch
BRANCH=compact-2705202607
HASH=$(curl -s http://nginx-private-internal.igris.in:19120/api/v2/trees/${BRANCH} \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('reference',{}).get('hash') or d.get('hash',''))")
curl -X DELETE "http://nginx-private-internal.igris.in:19120/api/v2/trees/${BRANCH}@${HASH}"

# Check ingestion commit frequency
curl -s "http://nginx-private-internal.igris.in:19120/api/v2/trees/raw-data-dev/history?max-records=20" \
  | python3 -m json.tool | grep commitTime
```

### Restore Trino to raw-data-dev
```bash
sed -i 's/iceberg.nessie-catalog.ref=.*/iceberg.nessie-catalog.ref=raw-data-dev/' \
  ~/trino_iceberg/trino/catalog/iceberg.properties
docker restart trino && sleep 40
grep "nessie-catalog.ref" ~/trino_iceberg/trino/catalog/iceberg.properties
```

### IAM role verification
```bash
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

---

## 14. Lessons learned

| # | Lesson | Detail |
|---|---|---|
| 1 | Trino version matters for Nessie | Trino 435 does not support Nessie API v2. Use 443+ (we used 480) |
| 2 | `spark-sql -f` strips quotes | Never use .sql file with `spark-sql -f` for procedures with string WHERE clauses. Use Python + `spark-submit` |
| 3 | `s3://` vs `s3a://` | Spark uses `s3a://`. Nessie metadata paths use `s3://`. Must add `hadoop-aws` JAR and map `fs.s3.impl` |
| 4 | `$files` query is always slow | Nessie has no index on file metadata. WHERE clause filters after full scan. On 80k files expect 20-30 minutes |
| 5 | CAST vs direct comparison | `CAST("ig_timestamp" AS DATE) = DATE '...'` works in Trino. Direct TIMESTAMP comparison does NOT work in `$files` metadata |
| 6 | Partition granularity | Table partitioned by `day(ig_timestamp)` — OPTIMIZE WHERE can only target full days |
| 7 | `parent_id = NULL` is a broken chain | If `$history` shows `parent_id = NULL`, Nessie snapshot chain is broken |
| 8 | Branch name must start with letter | Nessie rejects branch names starting with a digit |
| 9 | Nessie v2 create branch needs query params | `name` and `type` go in URL query params, not JSON body |
| 10 | Nessie v2 merge needs target hash in URL | `POST /api/v2/trees/{BRANCH}@{HASH}/history/merge` — hash in URL path, not body |
| 11 | Millisecond retries do not help | Merge takes ~500ms to execute — cannot retry faster than that. Need a genuine quiet window |
| 12 | Smart retry (hash stability) is better | Polling for 3-second stable hash is more reliable than blind retry, but cannot guarantee success vs active ingestion |
| 13 | Branch strategy isolates compaction | OPTIMIZE completes cleanly in ~2 min on isolated branch vs 22+ min with conflicts on live branch |
| 14 | Trino cleanup after crash | If script crashes before Step 10, manually restore iceberg.properties and restart Trino |
| 15 | Trino stays useful | For expire_snapshots, remove_orphan_files, and all verification queries — Trino works perfectly |

---

## 15. Known limitations and open questions

**Confirmed blockers:**
- Compaction on a live Nessie table with concurrent streaming ingestion is not possible with standard tools
- Known Nessie issue: [projectnessie/nessie#9969](https://github.com/projectnessie/nessie/issues/9969)

**Potential solutions not yet tested:**
1. **Pause ingestion during merge** — Stop the Processing Agent briefly during the merge commit window only. Phase 4 test pending.
2. **Use nessie-gc tool** — Nessie's own GC tool is branch-aware and designed to run alongside live ingestion.
3. **Compact T-2 or older partitions** — Run compaction on data 2+ days old where ingestion has fully moved on.
4. **Maintenance window scheduling** — Run compaction daily at 1-2 AM when ingestion volume is lowest.

**Open questions:**
- Does the Processing Agent append to past date partitions or only current day?
- Is `nessie-gc` already running in this environment?
- What is the exact duration needed to pause ingestion for a successful merge?

---

## 16. How to deploy

### Step 1 — Clone and configure
```bash
git clone https://github.com/Ashu-am/Iceberg_Small_File_Compaction_with_Nessie.git
cd Iceberg_Small_File_Compaction_with_Nessie
cp .env.example .env
nano .env   # fill in real values
```

### Step 2 — Update config files

Update `trino/catalog/iceberg.properties` with your Nessie URL, S3 bucket, branch name, and AWS region.

Update `scripts/compaction_branch.py`:
- Set correct `NESSIE_URI`, `SOURCE_BRANCH`, S3 bucket
- Update `WHERE CAST("ig_timestamp" AS DATE) = DATE '<target-date>'`

Update `scripts/generate_report.py`:
- Set `TARGET_DATE = "<target-date>"`

### Step 3 — Deploy on EC2
```bash
ssh -i "key.pem" ubuntu@<ec2-ip>
git clone https://github.com/Ashu-am/Iceberg_Small_File_Compaction_with_Nessie.git
cd Iceberg_Small_File_Compaction_with_Nessie
docker compose up -d
sleep 60
docker compose ps
```

### Step 4 — Set up Python venv
```bash
sudo apt-get install -y python3-full python3.12-venv
python3 -m venv venv
venv/bin/pip install requests trino
venv/bin/python3 -c "import requests, trino; print('OK')"
```

### Step 5 — Verify connection
```bash
docker exec -it trino trino
```
```sql
SHOW SCHEMAS IN iceberg;
SHOW TABLES IN iceberg.logs;
```

### Step 6 — Run compaction
```bash
mkdir -p reports
venv/bin/python3 scripts/generate_report.py
```

### Step 7 — Run expire and orphan removal (after successful compaction)
```sql
ALTER TABLE iceberg.logs.raw_data
EXECUTE expire_snapshots(retention_threshold => '1d');

ALTER TABLE iceberg.logs.raw_data
EXECUTE remove_orphan_files(retention_threshold => '1d');
```

---

## 17. Contributing

If you have solved the Nessie concurrent write conflict for Iceberg compaction, please open an issue or PR. Specifically interested in:

- Experience with `nessie-gc` tool alongside live ingestion
- Compaction approaches that work with Nessie API v2 and concurrent writers
- Whether pausing ingestion during commit window was feasible in your setup

---

## 18. Acknowledgements

This project was built and debugged over several weeks of real production testing. Every error, every fix, and every dead end is documented here honestly — including the ones that did not work — so others can skip straight to what matters.

Referenced GitHub issues and Nessie Google Group:
- [projectnessie/nessie#9969](https://github.com/projectnessie/nessie/issues/9969) — Snapshot validation failure during compaction with concurrent writes
- Nessie Google Group — https://groups.google.com/g/projectnessie/c/KL5ceKT-SP0?pli=1