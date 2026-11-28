# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-23; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-23.md](structural-audit-2026-11-23.md).

**1 unresolved: 1 open, 0 partial.**

A row states the remaining gap only.

## Unresolved


### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T25 | open | Go's unistore serves RPCs concurrently: `Server.KvCommit` (and every other handler) calls `MVCCStore` with no server-wide lock, and `MVCCStore.Commit` / `Prewrite` serialize only on the region's key-hash latches (`regCtx.AcquireLatches`), so client-go's 16 KB commit batches (`calcActionConcurrency`, up to `CommitterConcurrency` = 128) run in parallel. The Rust `KvHandler` takes `&mut self` and every RPC goes through one `Mutex<KvHandler>`, so all batches commit one after another. Measured 2026-11-28: a 50k-row insert's 100k secondary keys commit in 259 ms (40k keys in 120 ms; raising the client's commit concurrency from 16 to 128 changes nothing), while Go master's ANALYZE, started 25-65 ms after the same insert, always sees every row. Read-committed readers (ANALYZE) therefore miss rows Go's see: `tests_planner_core_integration_seek_cost`'s `analyze` re-runs ANALYZE until `stats_meta.count` matches, which Go's TestIndexScanSeekCost does not need. Rust: `tidb-unistore-tikv/src/kv_handler.rs` (`KvHandler`, `kv_commit(&mut self)`), `tidb-unistore/src/rpc.rs` and `mock.rs` (`Arc<Mutex<KvHandler>>`), `tidb-unistore-tikv/src/mvcc_store.rs` (`MvccStore::commit`); client side `third_party/tikv-client-rs/src/transaction/transaction.rs` (`commit_secondary_in_background` at `MULTI_REGION_CONCURRENCY`). | `pkg/store/mockstore/unistore/tikv/server.go` (`Server.KvCommit`, `newRequestCtx`), `tikv/mvcc.go` (`MVCCStore.Commit`, `Prewrite`, latches), client-go `txnkv/transaction/2pc.go` (`doActionOnBatches`, `calcActionConcurrency`) |

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Planner and bindings

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
