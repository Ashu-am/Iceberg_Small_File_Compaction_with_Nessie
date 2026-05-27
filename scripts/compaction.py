#!/usr/bin/env python3
"""
compaction_branch.py
─────────────────────────────────────────────────────────────────
Iceberg small-file compaction using a Nessie branch strategy.

WHAT THIS SCRIPT DOES:
  1. Gets current commit hash of source branch (e.g. raw-data-dev)
  2. Creates a new compaction branch named compact-DDMMYYYYHH
  3. Updates iceberg.properties to point Trino at the new branch
  4. Restarts Trino to pick up the new branch config
  5. Runs OPTIMIZE via Trino on the compaction branch (isolated —
     no impact on source branch during compaction)
  6. Merges compaction branch back into source branch
     (with configurable retries on conflict)
  7. Deletes the compaction branch (cleanup)
  8. Restores iceberg.properties to source branch and restarts Trino

NOTE — expire_snapshots and remove_orphan_files are NOT run here.
Run those separately via Trino CLI after confirming merge succeeded:
  ALTER TABLE iceberg.<schema>.<table>
  EXECUTE expire_snapshots(retention_threshold => '1d');
  ALTER TABLE iceberg.<schema>.<table>
  EXECUTE remove_orphan_files(retention_threshold => '1d');

HOW TO RUN:
  # Create and activate virtual environment
  python3 -m venv venv
  venv/bin/pip install requests trino
  venv/bin/python3 scripts/compaction_branch.py

CONFIGURATION:
  Edit the CONFIG section below before running.
  Replace all <placeholder> values with your real values.

KNOWN LIMITATION:
  The merge step (Step 6) will fail with HTTP 409 REFERENCE_CONFLICT
  if the ingestion pipeline commits new data to the source branch
  during the compaction window (~2 minutes). This is a known Nessie
  limitation documented at:
  https://github.com/projectnessie/nessie/issues/9969
  The script retries the merge up to MERGE_MAX_RETRIES times.
  If all retries fail, pause ingestion briefly and re-run.

WHAT WAS TESTED (25 May 2026):
  - Branch creation via Nessie REST API: ✅ Working
  - Trino OPTIMIZE on isolated branch: ✅ Working (~2 min for 1 day)
  - No impact to source branch during compaction: ✅ Confirmed
  - Merge with retries: ❌ Failed — ingestion commits every 5-30s,
    no clean gap available. Pausing ingestion is required.
"""

import requests
import subprocess
import time
import trino
import sys
from datetime import datetime, timedelta, timezone

# ─────────────────────────────────────────────────────────────────
#  CONFIG — replace all <placeholder> values before running
# ─────────────────────────────────────────────────────────────────

NESSIE_URI          = "http://<your-nessie-host>:19120"
SOURCE_BRANCH       = "<your-source-branch>"      # e.g. raw-data-dev
ICEBERG_SCHEMA      = "<your-schema>"             # e.g. logs
ICEBERG_TABLE       = "<your-table>"              # e.g. raw_data
TRINO_HOST          = "localhost"
TRINO_PORT          = 8080
TRINO_USER          = "compaction-script"
TRINO_CATALOG       = "iceberg"
ICEBERG_PROPS_PATH  = "/home/ubuntu/trino_iceberg/trino/catalog/iceberg.properties"
TRINO_CONTAINER     = "trino"
FILE_SIZE_THRESHOLD = "128MB"                     # target file size after compaction

# ── Merge retry config ────────────────────────────────────────────
# Total retry window = MERGE_MAX_RETRIES x MERGE_RETRY_WAIT seconds
# Example: 10 retries x 5s = 50 seconds of retry attempts
MERGE_MAX_RETRIES   = 10
MERGE_RETRY_WAIT    = 5     # seconds between merge attempts

# ─────────────────────────────────────────────────────────────────


def log(msg, level="INFO"):
    """Print timestamped log message."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{level}] {msg}")


def get_target_date():
    """
    Returns yesterday's date (T-1) in YYYY-MM-DD format.
    Compaction always targets the previous day's partition —
    ingestion has moved on to today so yesterday is frozen.
    """
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    return str(yesterday)


def generate_branch_name():
    """
    Generate branch name in format: compact-DDMMYYYYHH
    Example: compact-2505202615 for 25 May 2026 15:00 UTC
    """
    return "compact-" + datetime.now(timezone.utc).strftime("%d%m%Y%H")


def get_branch_hash(branch_name):
    """
    Get current commit hash of a Nessie branch.

    REST API: GET /api/v2/trees/{branch}

    WHY WE NEED THIS:
    Branch creation and merge both require the exact current hash
    of a branch to confirm we are operating on the right commit.
    This prevents accidental operations on stale state.
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

    REST API: POST /api/v2/trees
              name and type as query params, source reference in body

    WHY THIS IS SAFE:
    Branches in Nessie are metadata pointers — like Git branches.
    No data is copied. The new branch points to the same commit
    as source at this exact moment. Ingestion continues on source
    branch completely unaffected.
    """
    log(f"Creating compaction branch: {new_branch_name}")
    url = f"{NESSIE_URI}/api/v2/trees"
    params = {
        "name": new_branch_name,
        "type": "BRANCH"
    }
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

    WHY THIS IS NEEDED:
    Trino reads the Nessie branch from iceberg.properties at startup.
    To switch Trino to a different branch, update this file and
    restart Trino. This is the official approach per Nessie docs.
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
    Restart Trino container to pick up the updated branch config.
    Waits 35 seconds for Trino to become healthy before returning.
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

    WHY PYTHON LIBRARY INSTEAD OF CLI:
    This script runs fully automated without any CLI interaction.
    The trino library connects to Trino's HTTP API and submits
    queries programmatically — equivalent to the Trino CLI shell
    but usable from scripts and scheduled jobs.
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


def merge_branch_with_retry(compaction_branch):
    """
    Merge compaction branch into source branch with retries.

    REST API: POST /api/v2/trees/{target}@{target-hash}/history/merge

    WHY RETRIES:
    The ingestion pipeline commits new data to the source branch
    every 5-30 seconds. If a new commit arrives between when
    compaction started and when we attempt merge, Nessie returns
    HTTP 409 REFERENCE_CONFLICT. Retrying gives the merge multiple
    chances to find a quiet window between ingestion commits.

    OBSERVED IN TESTING (25 May 2026):
    Source branch hash changed twice in 50 seconds during testing.
    Ingestion frequency makes retry-only approach unreliable during
    active daytime hours. Pausing ingestion is the confirmed fix.
    """
    log(f"Merging {compaction_branch} → {SOURCE_BRANCH}")
    log(f"  Max retries: {MERGE_MAX_RETRIES}, wait between retries: {MERGE_RETRY_WAIT}s")

    for attempt in range(1, MERGE_MAX_RETRIES + 1):
        log(f"  Merge attempt {attempt}/{MERGE_MAX_RETRIES}")

        compaction_hash = get_branch_hash(compaction_branch)
        target_hash = get_branch_hash(SOURCE_BRANCH)

        url = f"{NESSIE_URI}/api/v2/trees/{SOURCE_BRANCH}@{target_hash}/history/merge"
        payload = {
            "fromRefName": compaction_branch,
            "fromHash": compaction_hash,
            "message": f"Compaction merge from branch {compaction_branch} (attempt {attempt})",
            "isDryRun": False,
            "returnConflictAsResult": True
        }

        try:
            response = requests.post(url, json=payload, timeout=60)

            if response.status_code in (200, 201, 204):
                log(f"  ✅ Merge successful on attempt {attempt}")
                return True
            else:
                log(f"  ❌ Merge attempt {attempt} failed. "
                    f"Status: {response.status_code}", "WARN")
                log(f"  Response: {response.text[:300]}", "WARN")

                if attempt < MERGE_MAX_RETRIES:
                    log(f"  Waiting {MERGE_RETRY_WAIT}s before retry {attempt + 1}...")
                    time.sleep(MERGE_RETRY_WAIT)

        except Exception as e:
            log(f"  ❌ Merge attempt {attempt} exception: {e}", "WARN")
            if attempt < MERGE_MAX_RETRIES:
                time.sleep(MERGE_RETRY_WAIT)

    log(f"❌ All {MERGE_MAX_RETRIES} merge attempts failed.", "ERROR")
    log("  Root cause: ingestion pipeline commits every 5-30 seconds.", "ERROR")
    log("  No clean gap available for merge during active ingestion.", "ERROR")
    log("  Reference: https://github.com/projectnessie/nessie/issues/9969", "ERROR")
    log("  Fix: Pause ingestion pipeline briefly and re-run this script.", "ERROR")
    return False


def delete_branch(branch_name):
    """
    Delete a Nessie branch after merge (cleanup).

    REST API: DELETE /api/v2/trees/{branch}@{hash}
    The hash must be included in the URL to confirm deletion
    of the correct commit — prevents accidental deletion.
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
    """Restore iceberg.properties to source branch and restart Trino."""
    log(f"Restoring Trino to source branch: {SOURCE_BRANCH}")
    update_trino_config(SOURCE_BRANCH)
    restart_trino()


def main():
    target_date = get_target_date()

    print("\n" + "="*70)
    print("  Iceberg Compaction — Nessie Branch Strategy")
    print(f"  Source branch  : {SOURCE_BRANCH}")
    print(f"  Table          : {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}")
    print(f"  Target date    : {target_date} (yesterday T-1, auto-calculated)")
    print(f"  Nessie URI     : {NESSIE_URI}")
    print(f"  Merge retries  : {MERGE_MAX_RETRIES} (every {MERGE_RETRY_WAIT}s)")
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
    log(f"  Targeting date: {target_date} (yesterday T-1, auto-calculated)")
    log("  expire_snapshots and remove_orphan_files run after merge.")

    optimize_sql = f"""
        ALTER TABLE {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}
        EXECUTE optimize(file_size_threshold => '{FILE_SIZE_THRESHOLD}')
        WHERE CAST("ig_timestamp" AS DATE) = DATE '{target_date}'
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

    # ── STEP 9: Delete compaction branch ─────────────────────────
    log("─── STEP 9: Delete compaction branch ───")
    delete_branch(branch_name)

    # ── STEP 10: Restore Trino to source branch ───────────────────
    log("─── STEP 10: Restore Trino → source branch ───")
    restore_and_restart()

    # ── Final summary ─────────────────────────────────────────────
    print("\n" + "="*70)
    print("  Run Complete")
    print(f"  Branch        : {branch_name}")
    print(f"  Target date   : {target_date}")
    print(f"  OPTIMIZE      : ✅ ran on compaction branch")
    print(f"  Merge         : {'✅ SUCCESS' if merge_ok else '❌ FAILED after all retries'}")
    print(f"  Trino         : restored to {SOURCE_BRANCH}")
    if merge_ok:
        print("\n  Next steps — run via Trino CLI:")
        print(f"    docker exec -it trino trino")
        print(f"    ALTER TABLE {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}")
        print(f"    EXECUTE expire_snapshots(retention_threshold => '1d');")
        print(f"    ALTER TABLE {TRINO_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}")
        print(f"    EXECUTE remove_orphan_files(retention_threshold => '1d');")
    else:
        print("\n  Merge failed. Options:")
        print("  1. Pause ingestion pipeline briefly and re-run")
        print("  2. Ask platform team about nessie-gc tool")
        print("  3. Try compacting T-2 or older partitions")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()