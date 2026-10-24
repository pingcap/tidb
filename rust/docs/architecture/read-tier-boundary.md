# Read entry points

The Rust server has one SQL-to-storage path, as Go does: every statement
runs through `tidb-session::Session`, the shared planner and the executor
driver, with `cluster_storage` and `remote_scan` for TiKV access, and every
prepared statement through the session's general prepared path (Go
`ExecutePreparedStmt`).

The configured read/write tier that used to sit beside it (`--read-table` /
`--load-table`, `ReadOnlyScanPlan`, `real_tikv_dml`, the prepared point-read
and prepared-write templates) was removed on 2026-10-23 once nothing but its
own tests reached it. `tidb-exec::real_tikv_read` is now only the node's
process authority over its client-rust store (Go `tikvStore`): the
coprocessor transport factory, the transaction opener and the coprocessor
cache.

Go implementation and original Go tests define correctness. New read
capability belongs in the shared planner/executor path.
