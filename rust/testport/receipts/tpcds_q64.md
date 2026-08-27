# TPC-DS Q64 parity receipt

This receipt records the local comparison for the Go planner source test
`pkg/planner/core/casetest/tpcds/tpcds_test.go::TestTPCDSQ64` and its
`testdata/tpcds_suite_in.json` query. The Rust server was built from the
latest `hparser-integration` tip used for this run.

## Servers and fixture

- Go: `tiup playground nightly`, TiDB `127.0.0.1:16000`.
- Rust: `rust/target/release/tidb-server`, TiDB `127.0.0.1:17000`.
- Both used the same one-PD/one-TiKV playground (`127.0.0.1:14379`) and the
  13-table schema from `pkg/planner/core/casetest/tpcds/main_test.go`.
- The schema marked all 13 tables with one TiFlash replica in planner metadata.
  The correctness fixture
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
With the source settings (`tidb_enforce_mpp=ON`, broadcast thresholds zero, and
the default `tikv,tiflash,tidb` engine list), the live plans were:

| server | plan rows | MPP rows |
| --- | ---: | ---: |
| Go | 156 | 147 |
| Rust | 76 | 0 |

Rust's plan is therefore not byte-for-byte aligned yet. The Rust planner has
structural MPP types, but the MPP exchange/task conversion and TiFlash
execution operators are not implemented; it falls back to a TiKV physical
plan. Exact source-plan parity requires transcreating the corresponding Go
planner/executor packages, rather than synthesizing an MPP-looking explain
output.

For a fair one-concurrency, TiKV-only control, both sessions set
`tidb_isolation_read_engines='tikv'` and all executor, join, lookup, scan, and
optimizer concurrency factors to `1`. Go returned 84 rows and Rust 77 rows.
Both selected two `MergeJoin` nodes, one `IndexHashJoin`, and one `HashJoin`
for the major Q64 joins; the remaining seven rows reflect different nested
join ordering and explain formatting, so the control is not byte-for-byte
identical yet.

## Correctness

Both servers returned the same one-row, 21-column result:

```text
Test Product | Test Store | 12345 | 1 | Old | Main | 12345 | 2 | Main |
Main | 12345 | 2000 | 1 | 10.00 | 36.00 | 1.00 | 12.00 | 36.00 |
2.00 | 2001 | 1
```

The normalized result SHA-256 (tab-delimited row, no trailing newline) was
`7acb112028cde67ae46294bd9171fc65dcf3674f5a32b429c6adf221056465f5` on both
servers.

## Single-concurrency latency

Twenty sequential executions after five warmups, one client per server. For
the fair TiKV-only control, MPP was disabled and
`tidb_isolation_read_engines='tikv'`; executor, hash-join, index-lookup-join,
index-lookup, and DistSQL scan concurrency were all set to `1`:

| server | min (ms) | p50 (ms) | p95 (ms) | mean (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Go | 17.977 | 18.999 | 22.751 | 19.581 | 26.333 |
| Rust | 118.648 | 131.615 | 134.826 | 135.684 | 218.184 |

The MPP timings are intentionally omitted: Rust did not execute MPP/TiFlash,
so comparing them with Go's MPP plan would not be apples-to-apples. Against the
immediately preceding remote `hparser-integration` binary (p50 142.292 ms
under the same 20-run protocol), this local Rust build is about 7.5% faster at
p50 (131.615 ms), so this change does not regress the measured minimal-fixture
workload. This is not a claim that Rust matches Go's absolute latency; the Rust
execution path remains slower on this fixture.

This is a minimal-fixture smoke test, not a TPC-DS SF1 throughput run. The Go
Q64 source test is the plan source of truth; full TPC-DS plan/result/performance
alignment remains pending until Rust has the required MPP and physical-join
package coverage.
