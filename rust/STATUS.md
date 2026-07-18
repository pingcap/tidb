# TiDB Rust rewrite current status

This file is generated from the checked source/test ledgers and workstream
manifests. It is the dispatcher hot path; historical narrative belongs in
`HANDOFF.md`. Regenerate it with `python3 scripts/status-dashboard.py --write`.

These are ownership states, not product-parity percentages. `PARTIAL` means
the source or obligation still has explicit unported behavior.

## Queue

- Active claims: 4
- Active slices: 0
- Declared ready slices: 4
- Partial slices: 57
- Covered slices: 2
- Blocked slices: 1

## Campaigns

| Campaign | Status | Slices |
| --- | --- | --- |
| 2026-07-read-path-01 | integrated | 6 |
| 2026-07-read-path-02 | integrated | 6 |
| 2026-07-read-path-07 | integrated | 3 |
| 2026-07-read-path-08 | integrated | 2 |
| 2026-07-read-path-09 | integrated | 2 |
| 2026-07-read-path-10 | integrated | 2 |
| 2026-07-read-path-11 | integrated | 3 |
| 2026-07-read-path-12 | integrated | 3 |
| 2026-07-read-path-13 | integrated | 4 |
| 2026-07-read-path-14 | integrated | 4 |
| 2026-07-read-path-15 | active | 4 |
| 2026-07-runtime-closure-03 | integrated | 6 |
| 2026-07-runtime-closure-04 | integrated | 6 |
| 2026-07-runtime-closure-05 | integrated | 6 |
| 2026-07-runtime-closure-06 | integrated | 3 |

## Pinned external Go universes

| Module | Version | Production sources | Test files | AST declarations | Runner obligations |
| --- | --- | --- | --- | --- | --- |
| github.com/tikv/client-go/v2 | v2.0.8-0.20260708122311-01bd8f99f4da | 151 | 75 | 809 | 474 |
| github.com/tikv/pd/client | v0.0.0-20260708075407-4e05b9d2c2d3 | 58 | 28 | 319 | 170 |

### External ownership states

| Universe | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| client-go production sources | 129 | 22 | 0 | 0 |
| pd-client production sources | 53 | 5 | 0 | 0 |
| client-go runner obligations | 371 | 103 | 0 | 0 |
| pd-client runner obligations | 164 | 6 | 0 | 0 |
| All external production sources | 182 | 27 | 0 | 0 |
| All external runner obligations | 535 | 109 | 0 | 0 |

External module counts are pinned porting obligations and are not included in TiDB product-parity totals.

## Production source ledger

| State | Count |
| --- | --- |
| UNTRIAGED | 1907 |
| PARTIAL | 447 |
| COVERED | 36 |
| BLOCKED | 0 |

### By target crate

| Target | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| deferred-external | 390 | 0 | 0 | 0 |
| tidb-planner | 206 | 107 | 1 | 0 |
| tidb-server | 252 | 1 | 0 | 0 |
| tidb-exec | 179 | 62 | 0 | 0 |
| tidb-txnkv | 124 | 25 | 9 | 0 |
| tidb-catalog | 126 | 1 | 0 | 0 |
| tidb-expr | 96 | 26 | 0 | 0 |
| tidb-ddl | 119 | 2 | 0 | 0 |
| tidb-session | 44 | 45 | 0 | 0 |
| test-support | 84 | 0 | 0 | 0 |
| tooling | 83 | 0 | 0 | 0 |
| tidb-stats | 1 | 77 | 2 | 0 |
| tidb-parser | 23 | 44 | 8 | 0 |
| unassigned | 64 | 0 | 0 | 0 |
| tidb-datatype | 32 | 22 | 7 | 0 |
| tidb-protocol | 43 | 11 | 2 | 0 |
| tidb-chunk | 16 | 2 | 0 | 0 |
| eliminated-go-runtime | 14 | 0 | 0 | 0 |
| tidb-ast | 8 | 5 | 0 | 0 |
| tidb-distsql | 3 | 4 | 2 | 0 |
| tidb-lexer | 0 | 7 | 0 | 0 |
| tidb-codec | 0 | 6 | 5 | 0 |

## Original test/support ledger

| State | Count |
| --- | --- |
| UNTRIAGED | 15417 |
| PARTIAL | 990 |
| COVERED | 298 |
| BLOCKED | 14 |

### By differential ring

| Ring | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| unassigned | 4848 | 316 | 134 | 0 |
| result | 4295 | 437 | 29 | 3 |
| deferred-external | 3042 | 0 | 0 | 0 |
| plan | 1784 | 123 | 5 | 0 |
| transaction | 1135 | 81 | 54 | 6 |
| parser | 313 | 33 | 76 | 5 |

## Blocked slices

- `datatype-core-time-authority`: single CoreTime calendar, arithmetic, week, timezone, and DST authority; blocked until the distinct time.go CoreTime constructor/bit layout, Duration clock value, and error identities are owned without substituting codec PackedTime

## Retired slices

- `datatype-conversion-context`
- `datatype-datum-sentinel-order`
- `datatype-field-type-authority`
- `datatype-output-format-authority`
- `datatype-truncate-policy`
- `distsql-cop-paging-continuation`
- `distsql-cop-read-task-runtime`
- `distsql-copr-cache-key-admission`
- `distsql-copr-cache-live-runtime`
- `distsql-injected-query-runtime`
- `distsql-query-response-runtime`
- `distsql-read-bytes-ema`
- `distsql-region-location-coverage`
- `distsql-region-task-construction`
- `distsql-select-response-consumption`
- `distsql-tikv-unary-rpc-contract`
- `executor-lack-handles-wave`
- `executor-lead-lag-live-window-runtime`
- `executor-table-index-reader-runtime`
- `executor-window-ranking-live-runtime`
- `expression-aggregate-descriptor-authority`
- `expression-field-name-resolution`
- `mysql-error-catalog`
- `planner-cardinality-live-index-choice`
- `session-count-warning-wave133`
- `session-protocol-status-publication`
- `session-warning-handler-authority`
- `session-warning-live-publication`
- `shared-terror-error-identity`
- `txnkv-copr-key-ranges`
