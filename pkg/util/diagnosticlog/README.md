# Diagnostic JSONL files

This opt-in producer preserves diagnostic data independently of the live system
tables. Configure it at server startup:

```toml
[diagnostic-log]
types = ["deadlock", "index_usage"]
```

An empty list disables both types. Either type can be enabled alone. TiDB writes
`diagnostic-log/tidb-deadlock.log` and `diagnostic-log/index-usage/` under the
server's working directory. Deployments can mount storage at the corresponding
absolute directory and must bind that storage to the original node identity
for collection.

Diagnostic files are created with owner-only permissions (0600). A file collector
must use the TiDB file owner's UID or an existing authorized node-level reader;
a shared group alone does not grant read access. Validate the deployed collector
identity and directory traversal permissions before enabling collection.

## Deadlock

The pessimistic-lock callback first applies the existing in-memory history policy,
then independently records an event. Retryable events can therefore be logged even
when they are excluded from `INFORMATION_SCHEMA.DEADLOCKS`.

With file logging enabled, the callback reserves queue space, then encodes each
waiting relationship once. A full queue drops the new event before encoding.
One background worker batches and rotates `tidb-deadlock.log` using lumberjack.
Before appending after restart, it terminates any partial final line so the next
event starts on its own line.

Initial limits, subject to workload evaluation:

| Limit | Value |
| --- | --- |
| Rate and burst, independently for each retryability class | 100 events/s, burst 100 |
| Event size / waiting relationships | 64 KiB / 64 |
| Outstanding records / bytes, including encoding reservations and in-flight writes | 1,024 / 8 MiB |
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

A dedicated Domain worker exports node-local cumulative counters approximately
hourly. The GC worker can run while the publisher waits for disk. The exporter
checks for at most 1,000,000 index identities before copying them, then copies
values in batches of 2,048 under the collector lock and encodes outside the lock.
If the identity count exceeds the limit, the round fails before allocating the
identity list.

Files contain one detail per index and a final footer with collection-start UTC
time and row count. A valid empty snapshot consists only of the footer. Publication
flushes, syncs and closes a temporary file before atomically linking it to a unique
`index-usage-YYYYMMDDTHHMMSS.ffffffZ-<32 lowercase hex>.jsonl` name. Existing names
are never replaced. Temporary and formal paths must be on the same filesystem and
that filesystem must support hard links.

Each attempt removes temporary files left by interrupted attempts. Successful
publication retains one formal snapshot on a best-effort basis. Failure keeps the
previous successful file with its original timestamp. The next round is scheduled
one hour after each attempt finishes. At shutdown, the Domain cancels the export
and waits up to five seconds for the worker. If a disk operation outlasts that wait,
the worker retains ownership and checks cancellation before exposing a formal file.
