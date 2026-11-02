# Diagnostic JSONL files

This opt-in producer preserves diagnostic data independently of the live system
tables. Configure it at server startup:

```toml
[diagnostic-log]
types = ["deadlock", "index_usage"]
```

The default is an empty list; either type can be enabled alone. Files use the main
log directory, falling back to the slow-query log directory. Collectors must bind
that directory to the original node identity. This package does not upload files
or aggregate data across nodes.

Diagnostic files are created with owner-only permissions (0600). A file collector
must use the TiDB file owner's UID or an existing authorized node-level reader;
a shared group alone does not grant read access. Validate the deployed collector
identity and directory traversal permissions before enabling collection.

## Deadlock

The pessimistic-lock callback first applies the existing in-memory history policy,
then independently records an event. Retryable events can therefore be logged even
when they are excluded from `INFORMATION_SCHEMA.DEADLOCKS`.

The callback constructs a bounded JSON record and attempts queue admission. One
background worker batches writes to `tidb-deadlock.log` using the existing
lumberjack rotation implementation. No file I/O or fsync runs on the callback.
Admission never waits for queue capacity; a full queue drops the new event.

Initial limits, subject to workload evaluation:

| Limit | Value |
| --- | --- |
| Rate and burst, independently for each retryability class | 100 events/s, burst 100 |
| Event size / waiting relationships | 64 KiB / 64 |
| Outstanding records / encoded bytes, including in-flight writes | 1,024 / 8 MiB |
| Batch size / flush interval | 64 KiB / 100 ms |
| Shutdown drain wait | 5 seconds |
| Rotation size / backups / age | 100 MiB / 3 / 7 days |

Each JSONL event carries observation time, transaction ID, retryability and a
waiting chain. IDs are decimal strings. The chain retains complete raw keys in
uppercase hexadecimal and available SQL digests; absent values are null. Limits
remove whole relationships and record original/output lengths and truncation.

Admission is not a delivery guarantee. Write failures discard the affected batch;
process crashes can lose queued data. Shutdown stops admission and drains within
its wait budget. A blocked OS write cannot be cancelled; on timeout the existing
worker retains ownership until the write returns or the process exits. The
outcome counter `tidb_server_diagnostic_log_total` distinguishes rate limiting,
queue overflow, write failure and shutdown timeout. The write-duration histogram
includes rotation time.

## Index Usage

The Domain worker exports node-local cumulative counters approximately hourly,
without resetting them. It first copies index identities, then copies values in
batches of 2,048 under the collector lock and encodes outside the lock. The result
is a diagnostic sample, not a single-instant consistent view.

Files contain one detail per index and a final footer with collection-start UTC
time and row count. A valid empty snapshot consists only of the footer. Publication
flushes, syncs and closes a temporary file before atomically linking it to a unique
`index-usage-YYYYMMDDTHHMMSS.ffffffZ-<32 lowercase hex>.jsonl` name. Existing names
are never replaced. Temporary and formal paths must be on the same filesystem and
that filesystem must support hard links.

Only successful publication removes older formal snapshots. Failure keeps the
previous successful file; it is not relabeled as a new-hour snapshot. Retaining one
formal snapshot is best effort and does not guarantee a collector has uploaded it.
The next attempt is scheduled one hour after the previous attempt completes;
there is no overlapping export or catch-up loop.
