# TPC-DS Q64 parity receipt

This receipt records the local comparison for the Go planner source test
`pkg/planner/core/casetest/tpcds/tpcds_test.go::TestTPCDSQ64` and its
`testdata/tpcds_suite_in.json` query. The Rust server was built from the
latest `hparser-integration` tip used for this run.

## Servers and fixture

- Go: `tiup playground nightly`, TiDB `127.0.0.1:15000`.
- Rust: `rust/target/release/tidb-server`, TiDB `127.0.0.1:16000`.
- Both used the same one-PD/one-TiKV playground (`127.0.0.1:13379`) and the
  13-table schema from `pkg/planner/core/casetest/tpcds/main_test.go`.
- All 13 tables had one TiFlash replica available. The correctness fixture
  contained two dates, one customer/item/store, two catalog rows, and two
  store-sales rows; it was inserted through Go and queried through both
  servers.

## Plan surface

The focused Rust change now accepts Go's four-column `EXPLAIN FORMAT='plan_tree'`
format, normalizes Go-compatible `Column#N` labels, records `CTEFullScan` nodes,
and emits materialized CTE definitions as auxiliary `CTE_N` roots. It also
returns Go's error for unsupported `EXPLAIN ANALYZE FORMAT='plan_tree'`.

The Go source expected file contains 155 plan rows. A live Go run produced 156
rows (the only source-file difference was a runtime `stream_count: 8` detail).
With the source settings
(`tidb_enforce_mpp=ON`, MPP allowed, and `tikv,tiflash` engines), the live plans
were:

| server | plan rows | MPP rows |
| --- | ---: | ---: |
| Go | 156 | 148 |
| Rust | 77 | 0 |

Rust's plan is therefore not byte-for-byte aligned yet. The Rust planner has
structural MPP types, but the MPP exchange/task conversion and TiFlash
execution operators are not implemented; it falls back to a TiKV physical
plan. Exact source-plan parity requires transcreating the corresponding Go
planner/executor packages, rather than synthesizing an MPP-looking explain
output.

For a fair non-MPP control, both sessions were forced to TiKV with MPP off.
Go returned 84 rows and Rust 77 rows. Rust selected many nested index joins,
while Go selected mostly hash joins, so this control also remains plan
different.

## Correctness

Both servers returned the same one-row, 21-column result:

```text
Test Product | Test Store | 12345 | 2 | Main | Main | 12345 | 2 | Main |
Main | 12345 | 2000 | 1 | 10.00 | 36.00 | 1.00 | 12.00 | 36.00 |
2.00 | 2001 | 1
```

The normalized result SHA-256 (tab-delimited row, no trailing newline) was
`b896420999928ed7e3dd2f9ab23058679a7fccfc2fcf5ab09adb52e8c6017a0d` on both
servers.

## Single-concurrency latency

Twenty sequential executions after five warmups, one client per server, using
the source settings above:

| server | min | p50 | p95 | mean | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Go (MPP) | 110.93 ms | 122.52 ms | 130.93 ms | 123.67 ms | 135.18 ms |
| Rust (TiKV fallback) | 100.56 ms | 102.08 ms | 103.48 ms | 102.25 ms | 103.49 ms |

These numbers are not an apples-to-apples MPP comparison because Rust did not
execute MPP/TiFlash. In the fair TiKV-only control (MPP disabled and
`tidb_isolation_read_engines='tikv'`), Go measured p50 17.84 ms and Rust p50
67.51 ms. The requested no-regression criterion is therefore not established;
the remaining performance gap tracks the different physical join choices.

This is a minimal-fixture smoke test, not a TPC-DS SF1 throughput run. The Go
Q64 source test is the plan source of truth; full TPC-DS plan/result/performance
alignment remains pending until Rust has the required MPP and physical-join
package coverage.
