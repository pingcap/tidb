# TPC-DS Q64 parity receipt

This receipt records the local comparison for the Go planner source test
`pkg/planner/core/casetest/tpcds/tpcds_test.go::TestTPCDSQ64` and its
`testdata/tpcds_suite_in.json` query.

## Servers

- Go: `tiup playground nightly`, TiDB `127.0.0.1:14000`
- Rust: `rust/target/release/tidb-server` from `hparser-integration`, TiDB
  `127.0.0.1:15000`, using the same TiKV/PD (`127.0.0.1:12379`)
- Both servers used the 13-table schema from `pkg/planner/core/casetest/tpcds/main_test.go`.

## Plan surface

Before the change Rust returned `unknown EXPLAIN format name` for
`EXPLAIN FORMAT='plan_tree'` and rejected the `WITH` query. The Rust change
now accepts Go's four-column `plan_tree` format, records `CTEFullScan` nodes,
and emits materialized CTE definitions as auxiliary `CTE_N` roots.

For Q64 on the empty 13-table schema, Go returned 80 plan rows and Rust
returned 77. With the minimal correctness fixture below, Go returned 84 plan
rows and Rust returned 77. The remaining differences are physical-plan
choices in the Rust executor (notably its index-join shape and an elided outer
projection), so this receipt does not claim byte-for-byte plan parity.

## Correctness

A two-date, one-item/store/customer fixture was inserted through Go and read
through both servers. The query returned one identical 21-column row on each:

```text
Test Product | Test Store | 12345 | 1 | Main | City | 12345 | 2 | Main |
City | 12345 | 2000 | 1 | 10.00 | 36.00 | 1.00 | 12.00 | 36.00 |
2.00 | 2001 | 1
```

The normalized result SHA-256 (tab-delimited row, no trailing newline) was
`ef48145660e3bb681cff298e33e0d6e404ee52570f54bb45a53a630bcefbb26c` on both.
The empty-schema query also returned zero rows on both.

## Single-concurrency latency

Twenty sequential executions after three warmups, one client per server:

| server | p50 | p95 | mean |
| --- | ---: | ---: | ---: |
| Go | 16.41 ms | 18.40 ms | 16.92 ms |
| Rust | 151.90 ms | 154.10 ms | 152.01 ms |

This is a tiny correctness fixture, not an SF1 throughput run. Rust is
currently slower on this query, so the requested “no performance regression”
criterion is not met yet.
