# config defaults parity audit: Go `pkg/config/config.go` `defaultConf` vs `DefaultConfig`

Audit date: 2026-09-05. Method: mechanical extraction of the 69
top-level `defaultConf` fields with literal resolution
(`Def*` constants, `.String()` forms, bool/number literals), compared
against the Rust `DefaultConfig` field tree.

## Results

- **Zero real divergences.** 38 fields matched on resolved value; the
  remaining 21 resolved as: 10 fields present under alternate spellings
  (`enable_32bits_connection_id`, `in_mem_slow_query_topn_num`,
  `tidb_max_reuse_chunk/column`, `tidb_edition/release_version`,
  `treat_old_version_utf8_as_utf8mb4`, `disaggregated_tiflash`,
  `is_tiflash_compute_fixed_pool`, `tidb_enable_exit_check`), 10
  non-scalar sections delegated to their own defaults
  (`TiKVClient`/`PDClient`/`PessimisticTxn`/`TrxSummary`/`RUV2`/
  `Labels`/`Experimental`/`HostedEmbedding`/`ExternalWorkload`/
  `RepairTableList`), and `lease` matched after resolving
  `DefSchemaLease.String()` (`"45s"`).
- Nested-section defaults (`tikvclient.rs`, memory limits) are their own
  table diffs and are not claimed here.

## Nested sections (2026-09-05, second pass)

- `PessimisticTxn` (`DefaultPessimisticTxn`): max_retry_count 256,
  deadlock_history_capacity 10, collect_retryable false,
  constraint_check_in_place_pessimistic true, and the auto-commit
  default's kernel-conditional (true only on next-gen) all match
  Rust's `PessimisticTxn::default_config`, which maps the conditional
  to `kerneltype::is_next_gen()`.
- `TrxSummary` (`DefaultTrxSummary`): capacity 500 and
  transaction_id_digest_min_duration 2147483647 match.
- `Performance`: every Go field matches the Rust default — including
  txn_entry_size_limit 6MB and txn_total_size_limit 100MB (Go
  `config.go:64-66`), stats leases, plan-replayer and stats-load
  settings, and the concurrency quotas.
- `TiKVClient`/`PDClient` defaults live in the client-go lineage
  (`tikvcfg.DefaultConfig()`), outside this tree; the Rust mirror is
  `third_party/tikv-client-rs` and follows the resync process rather
  than this audit.
