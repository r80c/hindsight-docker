#!/usr/bin/env python3
"""
Hindsight CDC Replicator — Near real-time push from local PG to Neon.

Strategy: Incremental copy using pg_dump --data-only per-table with
ON CONFLICT DO NOTHING. Polls every few seconds for new WAL activity
via the replication slot (no parsing — just uses it as a change signal).
Falls back to periodic full-table upsert for reliability.

Delete support: Detects rows deleted locally by comparing primary keys.
Deleted rows are archived to softdel_<table> tombstone tables on Neon
(preserving full row data + deletion timestamp), then removed from the
main tables. Local PG has full flexibility; central storage follows
deletes on the surface but retains all data.

If Neon is unreachable, retries with exponential backoff.
"""

import subprocess
import time
import signal
import logging
import os
import sys
import json
from datetime import datetime

LOCAL_DSN = os.environ.get("LOCAL_DSN", "postgresql://hindsight:hindsight@localhost:5432/hindsight")
NEON_DSN = os.environ.get("NEON_DSN", "postgresql://neondb_owner:npg_GxLETXlqM6R1@ep-little-term-aid29kjh.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require")
PG_BIN = os.environ.get("PG_BIN", "/home/hindsight/.pg0/installation/18.1.0/bin")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "3"))  # seconds
DELETE_CHECK_INTERVAL = int(os.environ.get("DELETE_CHECK_INTERVAL", "5"))  # every N syncs
SLOT_NAME = "neon_sync"
STATE_FILE = "/app/state.json"
LOG_FILE = os.environ.get("LOG_FILE", "/root/.openclaw/logs/hindsight-cdc.log")

# Tables in dependency order (parents first) — used for inserts
TABLES = [
    "banks",
    "documents",
    "chunks",
    "entities",
    "memory_units",
    "entity_cooccurrences",
    "memory_links",
    "unit_entities",
    "directives",
    "mental_models",
    "async_operations",
]

# Reverse dependency order — used for deletes (children first)
TABLES_DELETE_ORDER = list(reversed(TABLES))

# Primary key columns per table (used for delete detection)
TABLE_PKS = {
    "banks":                 ["bank_id"],
    "documents":             ["id", "bank_id"],
    "chunks":                ["chunk_id"],
    "entities":              ["id"],
    "memory_units":          ["id"],
    "entity_cooccurrences":  ["entity_id_1", "entity_id_2"],
    "memory_links":          ["from_unit_id", "to_unit_id", "link_type"],
    "unit_entities":         ["unit_id", "entity_id"],
    "directives":            ["id"],
    "mental_models":         ["bank_id", "id"],
    "async_operations":      ["operation_id"],
}

# Columns to EXCLUDE from tombstone tables (generated columns, etc.)
GENERATED_COLUMNS = {
    "memory_units": ["search_vector"],
    "mental_models": ["search_vector"],
}

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CDC] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

running = True
def signal_handler(sig, frame):
    global running
    log.info(f"Signal {sig}, shutting down...")
    running = False
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def psql(dsn, sql, capture=True, timeout=30):
    """Run psql command. Always checks exit code; raises on failure."""
    cmd = [f"{PG_BIN}/psql", dsn, "-t", "-A", "-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or f"psql exited {r.returncode}")
    if capture:
        return r.stdout.strip()
    return None


def psql_csv(dsn, sql, timeout=30):
    """Run psql command and return CSV output as list of rows."""
    cmd = [f"{PG_BIN}/psql", dsn, "-t", "-A", "--csv", "-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    lines = r.stdout.strip().split('\n') if r.stdout.strip() else []
    return lines


def has_wal_changes():
    """Check if there are pending WAL changes in the replication slot."""
    try:
        result = psql(LOCAL_DSN,
            f"SELECT count(*) FROM pg_logical_slot_peek_changes('{SLOT_NAME}', NULL, 1);")
        return int(result) > 0
    except:
        return True  # Assume changes if we can't check


def consume_wal():
    """Consume WAL changes from slot (so they don't accumulate)."""
    try:
        psql(LOCAL_DSN,
            f"SELECT count(*) FROM pg_logical_slot_get_changes('{SLOT_NAME}', NULL, NULL);")
    except:
        pass


# ---------------------------------------------------------------------------
# Tombstone table management
# ---------------------------------------------------------------------------

def get_table_columns(dsn, table):
    """Get column names for a table (excluding generated columns)."""
    sql = f"""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '{table}'
      AND is_generated = 'NEVER'
    ORDER BY ordinal_position;
    """
    result = psql(dsn, sql)
    if not result:
        return []
    return [c.strip() for c in result.split('\n') if c.strip()]


def ensure_tombstone_tables(neon_dsn):
    """Create softdel_<table> tombstone tables on Neon if they don't exist.
    
    Each tombstone table mirrors the source schema (minus generated columns)
    with an added _deleted_at timestamp. No constraints or indexes — pure archive.
    """
    log.info("Ensuring tombstone tables exist on Neon...")
    
    for table in TABLES:
        tombstone = f"softdel_{table}"
        
        # Check if tombstone already exists
        exists = psql(neon_dsn, f"""
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = '{tombstone}';
        """)
        
        if exists:
            continue
        
        # Get column definitions from the main table on Neon
        # Note: PG internal array type names start with '_' (e.g. _varchar)
        # but CREATE TABLE needs 'varchar[]' syntax, so we strip the prefix.
        col_defs = psql(neon_dsn, f"""
            SELECT string_agg(
                column_name || ' ' || 
                CASE 
                    WHEN data_type = 'ARRAY' THEN 
                        CASE 
                            WHEN udt_name LIKE '\\_%' THEN substring(udt_name from 2) || '[]'
                            ELSE udt_name || '[]'
                        END
                    WHEN data_type = 'USER-DEFINED' THEN udt_name
                    ELSE data_type 
                END,
                ', ' ORDER BY ordinal_position
            )
            FROM information_schema.columns
            WHERE table_schema = 'public' 
              AND table_name = '{table}'
              AND is_generated = 'NEVER';
        """, timeout=15)
        
        if not col_defs:
            log.warning(f"  Could not get column defs for {table}, skipping tombstone")
            continue
        
        create_sql = f"""
            SET search_path = public;
            CREATE TABLE IF NOT EXISTS {tombstone} (
                {col_defs},
                _deleted_at timestamptz NOT NULL DEFAULT now()
            );
        """
        
        try:
            psql(neon_dsn, create_sql, capture=False, timeout=15)
            log.info(f"  Created tombstone: {tombstone}")
        except Exception as e:
            log.error(f"  Failed to create {tombstone}: {e}")


# ---------------------------------------------------------------------------
# Delete detection and tombstoning
# ---------------------------------------------------------------------------

def get_pk_set(dsn, table, pk_cols, timeout=30):
    """Get the set of primary key tuples for a table."""
    pk_expr = " || '|' || ".join([f"COALESCE({c}::text, '')" for c in pk_cols])
    sql = f"SELECT {pk_expr} FROM public.{table};"
    result = psql(dsn, sql, timeout=timeout)
    if not result:
        return set()
    return set(result.split('\n'))


def tombstone_and_delete(neon_dsn, table, pk_cols, deleted_pks):
    """Archive deleted rows to tombstone table, then delete from main table.
    
    Args:
        neon_dsn: Neon connection string
        table: Table name
        pk_cols: List of primary key column names
        deleted_pks: Set of '|'-joined PK strings that were deleted locally
    """
    if not deleted_pks:
        return 0
    
    tombstone = f"softdel_{table}"
    columns = get_table_columns(neon_dsn, table)
    if not columns:
        log.error(f"  Could not get columns for {table}")
        return 0
    
    col_list = ", ".join(columns)
    deleted_count = 0
    
    # Process in batches to avoid huge queries
    batch_size = 100
    pk_list = list(deleted_pks)
    
    for i in range(0, len(pk_list), batch_size):
        batch = pk_list[i:i + batch_size]
        
        # Build WHERE clause for this batch
        if len(pk_cols) == 1:
            values = ", ".join([f"'{pk}'" for pk in batch])
            where = f"{pk_cols[0]}::text IN ({values})"
        else:
            # Composite PK — match on concatenated form
            pk_expr = " || '|' || ".join([f"COALESCE({c}::text, '')" for c in pk_cols])
            values = ", ".join([f"'{pk}'" for pk in batch])
            where = f"({pk_expr}) IN ({values})"
        
        # Step 1: Copy to tombstone
        insert_sql = f"""
            SET search_path = public;
            INSERT INTO {tombstone} ({col_list}, _deleted_at)
            SELECT {col_list}, now() FROM {table} WHERE {where};
        """
        
        # Step 2: Delete from main table
        delete_sql = f"SET search_path = public; DELETE FROM {table} WHERE {where};"
        
        try:
            psql(neon_dsn, insert_sql, capture=False, timeout=60)
            psql(neon_dsn, delete_sql, capture=False, timeout=60)
            deleted_count += len(batch)
        except Exception as e:
            log.error(f"  Tombstone/delete batch failed for {table}: {e}")
    
    return deleted_count


def detect_and_process_deletes():
    """Compare local and Neon PKs, tombstone+delete rows missing from local."""
    total_tombstoned = 0
    
    for table in TABLES_DELETE_ORDER:
        pk_cols = TABLE_PKS.get(table)
        if not pk_cols:
            continue
        
        try:
            local_pks = get_pk_set(LOCAL_DSN, table, pk_cols, timeout=30)
            neon_pks = get_pk_set(NEON_DSN, table, pk_cols, timeout=30)
            
            deleted_pks = neon_pks - local_pks
            
            if deleted_pks:
                log.info(f"  {table}: {len(deleted_pks)} deletions detected")
                count = tombstone_and_delete(NEON_DSN, table, pk_cols, deleted_pks)
                total_tombstoned += count
                
        except Exception as e:
            log.error(f"  Delete check failed for {table}: {e}")
    
    return total_tombstoned


# ---------------------------------------------------------------------------
# Sync (insert/upsert)
# ---------------------------------------------------------------------------

def sync_table(table):
    """Dump one table from local and upsert to Neon.
    
    For large tables (>500 rows), uses --rows-per-insert to batch multiple
    rows per INSERT statement, dramatically reducing round trips to Neon.
    """
    dump_file = f"/tmp/sync_{table}.sql"
    
    # Check row count to decide strategy
    try:
        row_count = int(psql(LOCAL_DSN, f"SELECT COUNT(*) FROM {table};"))
    except:
        row_count = 0
    
    # Large tables: batch 200 rows per INSERT to reduce round trips
    # Small tables: one row per INSERT (simpler, fast enough)
    rows_per_insert = 200 if row_count > 500 else 1
    apply_timeout = 600 if row_count > 5000 else (300 if row_count > 500 else 120)
    
    cmd = [
        f"{PG_BIN}/pg_dump", LOCAL_DSN,
        "--data-only", "--no-owner", "--no-privileges",
        "--inserts", "--on-conflict-do-nothing",
        f"--rows-per-insert={rows_per_insert}",
        "--table", f"public.{table}",
        "-f", dump_file
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        log.error(f"Dump {table} failed: {r.stderr[:200]}")
        return False
    
    if row_count > 500:
        log.info(f"  {table}: {row_count} rows, {rows_per_insert} rows/insert, timeout {apply_timeout}s")
    
    cmd = [f"{PG_BIN}/psql", NEON_DSN, "-f", dump_file]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=apply_timeout)
    
    errors = [l for l in r.stderr.split('\n') 
              if 'ERROR' in l and 'already exists' not in l and 'duplicate key' not in l]
    
    try:
        os.remove(dump_file)
    except:
        pass
    
    if errors:
        log.warning(f"  {table}: {len(errors)} errors")
        for e in errors[:3]:
            log.warning(f"    {e[:150]}")
    
    return True


def full_sync():
    """Full incremental sync of all tables."""
    start = time.time()
    success = True
    
    for table in TABLES:
        try:
            if not sync_table(table):
                success = False
        except Exception as e:
            log.error(f"  {table}: {e}")
            success = False
    
    elapsed = time.time() - start
    return success, elapsed


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify():
    """Compare row counts between local and Neon."""
    try:
        local_count = psql(LOCAL_DSN, "SELECT COUNT(*) FROM memory_units;")
        neon_count = psql(NEON_DSN, "SET search_path = public; SELECT COUNT(*) FROM memory_units;")
        return int(local_count), int(neon_count)
    except Exception as e:
        log.error(f"Verify failed: {e}")
        return -1, -1


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {"last_sync": None, "total_syncs": 0, "last_verified": None,
                "total_tombstoned": 0, "tombstones_initialized": False}


def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
    except:
        pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    log.info("Hindsight CDC Replicator starting...")
    log.info(f"Local: {LOCAL_DSN.split('@')[1] if '@' in LOCAL_DSN else '?'}")
    log.info(f"Neon: {NEON_DSN.split('@')[1].split('/')[0] if '@' in NEON_DSN else '?'}")
    log.info(f"Poll interval: {POLL_INTERVAL}s | Delete check every: {DELETE_CHECK_INTERVAL} syncs")
    
    # Ensure replication slot exists
    try:
        existing = psql(LOCAL_DSN, 
            f"SELECT plugin FROM pg_replication_slots WHERE slot_name = '{SLOT_NAME}';")
        if not existing:
            psql(LOCAL_DSN, 
                f"SELECT pg_create_logical_replication_slot('{SLOT_NAME}', 'test_decoding');")
            log.info(f"Created replication slot '{SLOT_NAME}'")
        else:
            log.info(f"Using existing slot '{SLOT_NAME}' (plugin: {existing})")
    except Exception as e:
        log.warning(f"Slot setup: {e}")
    
    state = load_state()
    
    # Ensure tombstone tables exist on Neon (once)
    if not state.get("tombstones_initialized"):
        try:
            ensure_tombstone_tables(NEON_DSN)
            state["tombstones_initialized"] = True
            save_state(state)
        except Exception as e:
            log.error(f"Tombstone init failed: {e}")
    
    backoff = 1
    idle_count = 0
    
    while running:
        try:
            changes = has_wal_changes()
            
            if changes or idle_count >= 20:  # Force sync every ~60s
                log.info(f"Syncing... (trigger: {'WAL change' if changes else 'periodic'})")
                
                success, elapsed = full_sync()
                
                if changes:
                    consume_wal()
                
                if success:
                    state["last_sync"] = datetime.now().isoformat()
                    state["total_syncs"] = state.get("total_syncs", 0) + 1
                    
                    # Run delete detection periodically
                    if state["total_syncs"] % DELETE_CHECK_INTERVAL == 0:
                        log.info("Running delete detection...")
                        try:
                            tombstoned = detect_and_process_deletes()
                            if tombstoned > 0:
                                state["total_tombstoned"] = state.get("total_tombstoned", 0) + tombstoned
                                log.info(f"Tombstoned {tombstoned} rows (total: {state['total_tombstoned']})")
                            else:
                                log.info("No deletions detected")
                        except Exception as e:
                            log.error(f"Delete detection failed: {e}")
                    
                    # Verify every 10 syncs
                    if state["total_syncs"] % 10 == 0:
                        local_n, neon_n = verify()
                        state["last_verified"] = f"local={local_n} neon={neon_n}"
                        log.info(f"Verified: local={local_n} neon={neon_n} ({elapsed:.1f}s)")
                    else:
                        log.info(f"Synced in {elapsed:.1f}s (#{state['total_syncs']})")
                    
                    save_state(state)
                    backoff = 1
                    idle_count = 0
                else:
                    backoff = min(backoff * 2, 60)
                    log.warning(f"Sync had errors, backoff {backoff}s")
                    time.sleep(backoff)
                    idle_count = 0
            else:
                idle_count += 1
                time.sleep(POLL_INTERVAL)
                
        except Exception as e:
            log.error(f"Main loop error: {e}")
            backoff = min(backoff * 2, 60)
            time.sleep(backoff)
    
    log.info(f"Shutdown. Total syncs: {state.get('total_syncs', 0)}, "
             f"total tombstoned: {state.get('total_tombstoned', 0)}")


if __name__ == "__main__":
    main()
