# TTL Temporal Clustered Primary-Key Splitting

## Motivation

TTL normally divides a table by its primary-key Region distribution. A clustered
primary key beginning with the TTL column already stores expired rows in one
contiguous physical prefix, but `DATE`, `DATETIME`, and `TIMESTAMP` keys were not
previously decoded into task boundaries. Such tables therefore received one full
range task even when their records occupied many Regions.

## Design

When a common-handle clustered primary key starts with the TTL column and that
column is `DATE`, `DATETIME(fsp)`, or `TIMESTAMP(fsp)`, the scheduler locates only
the record-key range `[record_prefix, record_prefix + encode(expire_time))`.
Regions wholly after the expiration cutoff do not contribute tasks. The final
Region crossing the cutoff is retained because it can still contain expired rows;
the SQL predicate `ttl_column < expire_time` remains the exact upper bound.

Region boundaries are arbitrary byte strings. They may truncate the packed time
or contain an invalid calendar value, so the scheduler maps each boundary to the
greatest valid temporal value whose complete encoding does not exceed it. Adjacent
tasks reuse the same mapped boundary, preventing gaps and overlap. In a composite
key, multiple Regions that differ only in later primary-key columns can map to the
same TTL value and are collapsed. This may reduce balance but does not change
coverage.

The tasks remain PK scans: `split_by` is `NULL`, pagination orders by the complete
clustered primary key, and no secondary-index planner behavior is required. Range
boundaries retain the existing textual PK-task encoding. `TIMESTAMP` boundaries
are represented in UTC because TTL data sessions execute in UTC; `DATE` and
`DATETIME` retain the global-time-zone wall-clock value used by TTL expiration.

Other primary-key layouts retain the existing splitting behavior. In particular,
expire-based Region pruning is not applied when the TTL column is not the first
physical key column, because expired rows do not form a provably contiguous key
prefix in that case.

## Compatibility

No task column or task encoding version is added. An older worker can parse the
textual PK boundary, so this optimization does not use the secondary-index scan
version gate. During a rolling upgrade, an old worker can interpret a UTC
`TIMESTAMP` boundary in the former session time zone and incompletely cover that
task; the expiration predicate still prevents deletion of unexpired rows, and a
later TTL job retries omitted expired rows. The change depends on the UTC
TTL-session behavior introduced by #70767 so current workers preserve the instant
across DST transitions.
