# SDP
## Medallion Architecture — Streaming & Merge Patterns

### Bronze Layer
- Always **append-only**. Raw data is ingested as-is, no updates or deletes.

### Silver Layer
- Can be **SCD Type 1** (overwrite) or **SCD Type 2** (historical tracking).
- If Silver is SCD2, it uses a **merge** operation (not append-only).
  - SCD2 is inherently non-append-only, which makes pure streaming awkward.

### Gold Layer

The pattern depends on whether Gold needs **incremental updates** or can simply **append**.

#### Gold needs incremental updates (no full table overwrites)
- Use **SCD1 merge** with Change Data Feed:
  ```python
  spark.readStream.option("readChangeFeed", "true")
  ```
- Write pattern: `writeStream` or `write` → `foreachBatch` → `merge`

#### Gold can be append-only
- Use a **DLT Streaming Table** (`dlt.read_stream` + `dlt.table`)
    - dlt.view of silver using dlt.read_stream and create streaming table with SCD1
- Or a standard `dlt.table` with append semantics

