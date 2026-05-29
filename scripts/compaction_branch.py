#!/usr/bin/env python3
"""
compaction_branch.py
─────────────────────────────────────────────────────────────────
Iceberg small-file compaction using a Nessie branch strategy.

WHAT THIS SCRIPT DOES:
  1. Gets current commit hash of source branch (raw-data-dev)
  2. Creates a new compaction branch named DDMMYYYYHH
  3. Updates iceberg.properties to point Trino at the new branch
  4. Restarts Trino to pick up the new branch config
  5. Runs OPTIMIZE via Trino on the compaction branch
  6. Merges compaction branch back into raw-data-dev
     (with configurable retries on conflict)
  7. Deletes the compaction branch (cleanup)
  8. Restores iceberg.properties to raw-data-dev and restarts Trino

NOTE — expire_snapshots and remove_orphan_files are NOT run here.
Run those separately via Trino CLI after confirming merge succeeded.

HOW TO RUN:
  pip install requests trino
  python3 compaction_branch.py

CONFIGURATION:
  Edit the CONFIG section below before running.
"""

import requests
import subprocess
import time
import trino
import sys
import random
from datetime import datetime

# ─────────────────────────────────────────────────────────────────
#  CONFIG — edit these before running
# ─────────────────────────────────────────────────────────────────

NESSIE_URI         = "http://nginx-private-internal.igris.in:19120"
SOURCE_BRANCH      = "raw-data-dev"
ICEBERG_SCHEMA     = "logs"
ICEBERG_TABLE      = "raw_data"
TRINO_HOST         = "localhost"
TRINO_PORT         = 8080
TRINO_USER         = "compaction-script"
TRINO_CATALOG      = "iceberg"
ICEBERG_PROPS_PATH = "/home/ubuntu/trino_iceberg/trino/catalog/iceberg.properties"
TRINO_CONTAINER    = "trino"

# ── Merge retry config ────────────────────────────────────────────
MERGE_MAX_RETRIES      = 10     # how many times to retry merge on conflict
MERGE_RETRY_WAIT       = 5      # seconds to wait between retries
FILE_SIZE_THRESHOLD    = "128MB"

# ── Smart retry (hash stability check) ───────────────────────────
# Before each merge attempt, wait until raw-data-dev hash has been
# stable for HASH_STABLE_SECONDS. This catches quiet gaps between
# ingestion commits instead of blindly retrying at fixed intervals.
# Jitter adds random milliseconds (JITTER_MIN_MS to JITTER_MAX_MS)
# to desynchronize retries if multiple processes run at once.
HASH_STABLE_SECONDS    = 3      # seconds raw-data-dev hash must stay unchanged
HASH_CHECK_INTERVAL    = 1      # how often (seconds) to re-poll hash during stability check
HASH_STABLE_TIMEOUT    = 60     # max seconds to wait for a stable window before giving up
JITTER_MIN_MS          = 100    # minimum random jitter in milliseconds
JITTER_MAX_MS          = 1500   # maximum random jitter in milliseconds

# ─────────────────────────────────────────────────────────────────


def log(msg, level="INFO"):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{level}] {msg}")


def generate_branch_name():
    """Generate branch name as compact-DDMMYYYYHH. Must start with a letter — Nessie rejects names starting with a digit."""
    return "compact-" + datetime.utcnow().strftime("%d%m%Y%H")


def get_branch_hash(branch_name):
    """
    Get current commit hash of a Nessie branch.
    GET /api/v2/trees/{branch}
    """
    log(f"Getting current hash of branch: {branch_name}")
    url = f"{NESSIE_URI}/api/v2/trees/{branch_name}"
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        log(f"Failed to get branch hash. Status: {response.status_code} — {response.text}", "ERROR")
        sys.exit(1)

    data = response.json()
    hash_value = data.get("reference", {}).get("hash") or data.get("hash")

    if not hash_value:
        log(f"Could not extract hash from response: {data}", "ERROR")
        sys.exit(1)

    log(f"  Hash of {branch_name}: {hash_value}")
    return hash_value


def create_branch(new_branch_name, source_hash):
    """
    Create a new Nessie branch from exact hash of source branch.
    POST /api/v2/trees?name=<branch>&type=BRANCH
    Nessie v2: name and type must be query params, not in body.
    No data is copied — branch is just a metadata pointer (like Git).
    """
    log(f"Creating compaction branch: {new_branch_name}")
    url = f"{NESSIE_URI}/api/v2/trees"
    params = {"name": new_branch_name, "type": "BRANCH"}
    payload = {
        "type": "BRANCH",
        "name": SOURCE_BRANCH,
        "hash": source_hash
    }
    response = requests.post(url, params=params, json=payload, timeout=30)

    if response.status_code not in (200, 201):
        log(f"Failed to create branch. Status: {response.status_code} — {response.text}", "ERROR")
        sys.exit(1)

    log(f"  Branch created: {new_branch_name}")


def update_trino_config(branch_name):
    """
    Update iceberg.nessie-catalog.ref in iceberg.properties.
    Trino reads this at startup — requires restart to take effect.
    """
    log(f"Updating iceberg.properties → branch: {branch_name}")
    try:
        with open(ICEBERG_PROPS_PATH, "r") as f:
            content = f.read()

        lines = content.split("\n")
        new_lines = []
        for line in lines:
            if line.startswith("iceberg.nessie-catalog.ref="):
                new_lines.append(f"iceberg.nessie-catalog.ref={branch_name}")
            else:
                new_lines.append(line)

        with open(ICEBERG_PROPS_PATH, "w") as f:
            f.write("\n".join(new_lines))

        log(f"  iceberg.properties updated to branch: {branch_name}")

    except Exception as e:
        log(f"Failed to update iceberg.properties: {e}", "ERROR")
        sys.exit(1)


def restart_trino():
    """
    Restart Trino container so it picks up the new branch config.
    Waits 35 seconds for Trino to become healthy.
    """
    log(f"Restarting Trino container: {TRINO_CONTAINER}")
    result = subprocess.run(
        ["docker", "restart", TRINO_CONTAINER],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log(f"Failed to restart Trino: {result.stderr}", "ERROR")
        sys.exit(1)

    log("  Waiting 35 seconds for Trino to become healthy...")
    time.sleep(35)
    log("  Trino ready")


def run_trino_query(query, description):
    """
    Run a SQL query against Trino via Python trino library.
    Returns (success: bool, result_rows: list)
    """
    log(f"Running: {description}")
    log(f"  SQL: {query.strip()[:120]}{'...' if len(query.strip()) > 120 else ''}")

    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=ICEBERG_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        log(f"  ✅ {description} — success")
        if rows:
            log(f"  Result: {rows}")
        return True, rows

    except Exception as e:
        log(f"  ❌ {description} — failed: {e}", "ERROR")
        return False, []


def wait_for_stable_hash(poll_interval=HASH_CHECK_INTERVAL,
                         stable_seconds=HASH_STABLE_SECONDS,
                         timeout=HASH_STABLE_TIMEOUT):
    """
    Poll raw-data-dev hash until it has been unchanged for stable_seconds.
    Returns (stable_hash, True) if a quiet window is found within timeout.
    Returns (last_hash, False) if timeout is reached without stability.

    WHY THIS MATTERS:
    Ingestion commits every 5-30s. Retrying blindly in ms is pointless
    because the conflict is determined at the moment Nessie processes
    the request — not when we send it. We must catch a genuine quiet
    gap between ingestion commits (hash unchanged for >= stable_seconds).
    """
    log(f"  Waiting for quiet window: hash stable for {stable_seconds}s "
        f"(timeout: {timeout}s, poll every {poll_interval}s)")

    start      = time.time()
    last_hash  = None
    stable_since = None

    while True:
        elapsed = time.time() - start
        if elapsed >= timeout:
            log(f"  ⚠ Stability timeout after {timeout}s — proceeding anyway", "WARN")
            return last_hash, False

        url = f"{NESSIE_URI}/api/v2/trees/{SOURCE_BRANCH}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                current_hash = (data.get("reference", {}).get("hash")
                                or data.get("hash", ""))
            else:
                current_hash = last_hash  # treat error as no-change, keep waiting
        except Exception:
            current_hash = last_hash

        now = time.time()
        if current_hash != last_hash:
            log(f"  Hash changed → {current_hash[:16]}... (reset stability timer)")
            last_hash    = current_hash
            stable_since = now
        else:
            stable_for = now - (stable_since or now)
            log(f"  Hash stable for {stable_for:.1f}s / need {stable_seconds}s ...")
            if stable_since and stable_for >= stable_seconds:
                log(f"  ✅ Quiet window found — hash stable for {stable_for:.1f}s")
                return last_hash, True

        time.sleep(poll_interval)


def merge_branch_with_retry(compaction_branch):
    """
    Merge compaction branch into source branch using smart retry.

    STRATEGY:
    1. Before each merge attempt, wait until raw-data-dev hash has been
       stable for HASH_STABLE_SECONDS — this catches genuine quiet gaps
       between ingestion commits.
    2. Add random jitter (JITTER_MIN_MS to JITTER_MAX_MS) before
       sending the merge request — desynchronizes retries if multiple
       processes run at once.
    3. On 409 conflict, re-enter the stability wait before next attempt.

    WHY NOT MILLISECOND RETRIES:
    Nessie merge takes ~500ms to process. Retrying faster than that
    just floods Nessie. The conflict window is ingestion-frequency-
    driven (5-30s gaps), not request-frequency-driven. Catching a
    stable hash window is the correct approach.

    MERGE REST API:
    POST /api/v2/trees/{target}@{target_hash}/history/merge
    Target hash must be in the URL — confirmed from Nessie v2 spec.
    """
    log(f"Merging {compaction_branch} → {SOURCE_BRANCH}")
    log(f"  Max retries       : {MERGE_MAX_RETRIES}")
    log(f"  Hash stable needed: {HASH_STABLE_SECONDS}s")
    log(f"  Stability timeout : {HASH_STABLE_TIMEOUT}s per attempt")
    log(f"  Jitter range      : {JITTER_MIN_MS}–{JITTER_MAX_MS}ms")


    print("\n" + "="*60)
    input("  ⏸ PAUSE INGESTION, then press Enter to attempt merge...")
    print("="*60 + "\n")

    for attempt in range(1, MERGE_MAX_RETRIES + 1):
        log(f"  ── Merge attempt {attempt}/{MERGE_MAX_RETRIES} ──")

        # Wait for a quiet window on raw-data-dev
        target_hash, is_stable = wait_for_stable_hash()

        if not is_stable:
            log(f"  ⚠ Could not find stable window — attempting merge anyway", "WARN")

        # Apply random jitter before sending merge request
        jitter_ms = random.randint(JITTER_MIN_MS, JITTER_MAX_MS)
        log(f"  Applying jitter: {jitter_ms}ms before merge request")
        time.sleep(jitter_ms / 1000.0)

        # Get fresh compaction branch hash
        compaction_hash = get_branch_hash(compaction_branch)

        # Re-fetch target hash right before merge (after jitter)
        target_hash = get_branch_hash(SOURCE_BRANCH)

        url = f"{NESSIE_URI}/api/v2/trees/{SOURCE_BRANCH}@{target_hash}/history/merge"
        payload = {
            "fromRefName": compaction_branch,
            "fromHash":    compaction_hash,
            "message":     f"Compaction merge from {compaction_branch} (attempt {attempt})",
            "isDryRun":    False,
            "returnConflictAsResult": True
        }

        try:
            response = requests.post(url, json=payload, timeout=60)

            if response.status_code in (200, 201, 204):
                log(f"  ✅ Merge successful on attempt {attempt}")
                return True

            log(f"  ❌ Merge attempt {attempt} failed. "
                f"Status: {response.status_code}", "WARN")
            log(f"  Response: {response.text[:400]}", "WARN")

            if attempt < MERGE_MAX_RETRIES:
                log(f"  Waiting {MERGE_RETRY_WAIT}s before next stability check...")
                time.sleep(MERGE_RETRY_WAIT)

        except Exception as e:
            log(f"  ❌ Merge attempt {attempt} exception: {e}", "WARN")
            if attempt < MERGE_MAX_RETRIES:
                time.sleep(MERGE_RETRY_WAIT)

    log(f"❌ All {MERGE_MAX_RETRIES} merge attempts failed.", "ERROR")
    log("  Ingestion commits too frequently — no stable window found.", "ERROR")
    log("  See: https://github.com/projectnessie/nessie/issues/9969", "ERROR")
    log("  Only confirmed fix: pause ingestion during merge step.", "ERROR")
    return False


def delete_branch(branch_name):
    """
    Delete a Nessie branch (cleanup after merge).
    DELETE /api/v2/trees/{branch}@{hash}
    """
    log(f"Deleting compaction branch: {branch_name}")
    try:
        current_hash = get_branch_hash(branch_name)
        url = f"{NESSIE_URI}/api/v2/trees/{branch_name}@{current_hash}"
        response = requests.delete(url, timeout=30)
        if response.status_code in (200, 204):
            log(f"  Branch deleted: {branch_name}")
        else:
            log(f"  Could not delete branch: {response.status_code} {response.text}", "WARN")
    except Exception as e:
        log(f"  Could not delete branch: {e}", "WARN")


def restore_and_restart():
    """Restore Trino to source branch and restart."""
    log(f"Restoring Trino to source branch: {SOURCE_BRANCH}")
    update_trino_config(SOURCE_BRANCH)
    restart_trino()


def main():
    print("\n" + "="*70)
    print("  Iceberg Compaction — Nessie Branch Strategy")
    print(f"  Source branch    : {SOURCE_BRANCH}")
    print(f"  Table            : {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}")
    print(f"  Nessie URI       : {NESSIE_URI}")
    print(f"  File threshold   : {FILE_SIZE_THRESHOLD}")
    print(f"  Merge retries    : {MERGE_MAX_RETRIES}")
    print(f"  Hash stable need : {HASH_STABLE_SECONDS}s (smart retry — waits for quiet window)")
    print(f"  Jitter range     : {JITTER_MIN_MS}–{JITTER_MAX_MS}ms")
    print("="*70 + "\n")

    branch_name = generate_branch_name()
    log(f"Compaction branch name: {branch_name}")

    # ── STEP 1: Get source branch hash ────────────────────────────
    log("─── STEP 1: Get source branch hash ───")
    source_hash = get_branch_hash(SOURCE_BRANCH)

    # ── STEP 2: Create compaction branch ─────────────────────────
    log("─── STEP 2: Create compaction branch ───")
    create_branch(branch_name, source_hash)

    # ── STEP 3: Point Trino at compaction branch ──────────────────
    log("─── STEP 3: Update Trino config → compaction branch ───")
    update_trino_config(branch_name)

    # ── STEP 4: Restart Trino ─────────────────────────────────────
    log("─── STEP 4: Restart Trino ───")
    restart_trino()

    # ── STEP 5: Verify Trino sees compaction branch ───────────────
    log("─── STEP 5: Verify Trino connection ───")
    ok, _ = run_trino_query("SHOW SCHEMAS IN iceberg", "Verify connection")
    if not ok:
        log("Cannot connect to Nessie via Trino. Cleaning up.", "ERROR")
        delete_branch(branch_name)
        restore_and_restart()
        sys.exit(1)

    # ── STEP 6: Run OPTIMIZE on compaction branch ─────────────────
    log("─── STEP 6: OPTIMIZE (compaction only) ───")
    log("  NOTE: expire_snapshots and remove_orphan_files are skipped.")
    log("  Run those manually via Trino CLI after merge succeeds.")

    optimize_sql = f"""
        ALTER TABLE {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}
        EXECUTE optimize(file_size_threshold => '{FILE_SIZE_THRESHOLD}')
        WHERE CAST("ig_timestamp" AS DATE) = DATE '2026-05-07'
    """
    ok, _ = run_trino_query(optimize_sql.strip(), "OPTIMIZE")
    if not ok:
        log("OPTIMIZE failed. Cleaning up.", "WARN")
        delete_branch(branch_name)
        restore_and_restart()
        sys.exit(1)

    # ── STEP 7: Check file count after OPTIMIZE ───────────────────
    log("─── STEP 7: Verify file count after OPTIMIZE ───")
    verify_sql = f"""
        SELECT COUNT(*) AS file_count, SUM(record_count) AS total_events
        FROM {TRINO_CATALOG}.{ICEBERG_SCHEMA}."{ICEBERG_TABLE}$files"
    """
    run_trino_query(verify_sql.strip(), "File count check")

    # ── STEP 8: Merge with retries ────────────────────────────────
    log("─── STEP 8: Merge compaction branch → source branch ───")
    merge_ok = merge_branch_with_retry(branch_name)

    # # ── STEP 9: Delete compaction branch ─────────────────────────
    # log("─── STEP 9: Delete compaction branch ───")
    # delete_branch(branch_name)

    # ── STEP 10: Restore Trino to source branch ───────────────────
    log("─── STEP 10: Restore Trino → source branch ───")
    restore_and_restart()

    # ── Final summary ─────────────────────────────────────────────
    print("\n" + "="*70)
    print("  Run Complete")
    print(f"  Branch        : {branch_name}")
    print(f"  OPTIMIZE      : ✅ ran on compaction branch")
    print(f"  Merge         : {'✅ SUCCESS' if merge_ok else '❌ FAILED after all retries'}")
    print(f"  Trino         : restored to {SOURCE_BRANCH}")
    if merge_ok:
        print("\n  Next steps:")
        print("  Run expire_snapshots and remove_orphan_files via Trino CLI:")
        print(f"    docker exec -it trino trino")
        print(f"    ALTER TABLE {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}")
        print(f"    EXECUTE expire_snapshots(retention_threshold => '1d');")
        print(f"    ALTER TABLE {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}")
        print(f"    EXECUTE remove_orphan_files(retention_threshold => '1d');")
    else:
        print("\n  Merge failed. Options:")
        print("  1. Pause ingestion pipeline and re-run this script")
        print("  2. Increase MERGE_MAX_RETRIES in config")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()