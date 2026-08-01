# Chunk and statistics divergence inventory

Go source of truth: `pkg/util/chunk/{column,chunk,codec}.go`, `pkg/statistics/*`.
Rust under audit: `rust/crates/tidb-chunk`, `rust/crates/tidb-stats`,
plus the chunk-format wire decoder in `rust/crates/tidb-codec/src/column.rs`
and `rust/crates/tidb-distsql/src/chunk_decode.rs` (read-only for this audit).

Status: in progress.

## Part A -- chunk

### Verified equal

(pending)

### Ranked divergences

(pending)

## Part B -- statistics

(pending)
