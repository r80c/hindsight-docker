# Hindsight CDC Replicator

Near real-time replication from a local Hindsight PostgreSQL instance to Neon (cloud PG), with soft-delete tombstone support.

## Architecture

```
Local PG (Hindsight) → CDC Replicator → Neon PG (cloud backup)
                                    ↘ softdel_* tombstone tables
```

### Sync (Inserts/Updates)
- Polls local PG's logical replication slot every 3s for change signals
- On change (or periodic fallback every ~60s), dumps each table with `pg_dump --inserts --on-conflict-do-nothing`
- Applies to Neon — new rows are inserted, existing rows are left untouched

### Delete Support (Tombstones)
- Every N sync cycles (default: 5), compares primary keys between local and Neon
- Rows that exist in Neon but not locally = deleted
- Deleted rows are copied to `softdel_<table>` tombstone tables with a `_deleted_at` timestamp
- Then deleted from the main tables on Neon

This gives local PG full flexibility to clean up (delete banks, consolidate memories, etc.) while central storage follows the deletes on the surface but **retains all data** in tombstone tables.

### Tombstone Tables
For each replicated table, a `softdel_<table>` mirror is auto-created on Neon:
- Same columns as the source (minus generated columns like `search_vector`)
- Added `_deleted_at timestamptz` column
- No constraints or indexes — pure append-only archive

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `LOCAL_DSN` | `postgresql://hindsight:hindsight@localhost:5432/hindsight` | Local PG connection |
| `NEON_DSN` | *(hardcoded)* | Neon PG connection |
| `PG_BIN` | `/usr/local/bin` | Path to `pg_dump`/`psql` binaries |
| `POLL_INTERVAL` | `3` | Seconds between WAL checks |
| `DELETE_CHECK_INTERVAL` | `5` | Run delete detection every N syncs |
| `LOG_FILE` | `/root/.openclaw/logs/hindsight-cdc.log` | Log file path |

## Docker

```bash
docker build -t hindsight-cdc .
docker run -d --name hindsight-cdc \
  --network container:hindsight \
  -e NEON_DSN="postgresql://..." \
  -e LOCAL_DSN="postgresql://hindsight:hindsight@localhost:5432/hindsight" \
  hindsight-cdc
```

## Replicated Tables

`banks`, `documents`, `chunks`, `entities`, `memory_units`, `entity_cooccurrences`, `memory_links`, `unit_entities`, `directives`, `mental_models`, `async_operations`

## Notes

- `memory_links` has no formal PK — uses `(from_unit_id, to_unit_id, link_type)` as composite key for delete detection
- Generated columns (`search_vector`) are excluded from tombstone tables
- Deletes are processed in reverse dependency order (children first) to avoid FK violations
- The replication slot is used as a change signal only — no WAL parsing
