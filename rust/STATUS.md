# TiDB Rust rewrite current status

This file is generated from the checked source/test ledgers and workstream
manifests. It is the dispatcher hot path; historical narrative belongs in
`HANDOFF.md`. Regenerate it with `python3 scripts/status-dashboard.py --write`.

These are ownership states, not product-parity percentages. `PARTIAL` means
the source or obligation still has explicit unported behavior.

## Queue

- Active claims: 0
- Active slices: 0
- Declared ready slices: 0
- Partial slices: 42
- Covered slices: 2
- Blocked slices: 1

## Campaigns

| Campaign | Status | Slices |
| --- | --- | --- |
| 2026-07-read-path-01 | integrated | 6 |
| 2026-07-read-path-02 | integrated | 6 |
| 2026-07-read-path-07 | integrated | 3 |
| 2026-07-runtime-closure-03 | integrated | 6 |
| 2026-07-runtime-closure-04 | integrated | 6 |
| 2026-07-runtime-closure-05 | integrated | 6 |
| 2026-07-runtime-closure-06 | integrated | 3 |

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
| UNTRIAGED | 15284 |
| PARTIAL | 976 |
| COVERED | 311 |
| BLOCKED | 14 |

### By differential ring

| Ring | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| unassigned | 4835 | 316 | 134 | 0 |
| result | 4339 | 436 | 29 | 3 |
| deferred-external | 2959 | 0 | 0 | 0 |
| plan | 1784 | 123 | 5 | 0 |
| transaction | 1054 | 68 | 67 | 6 |
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
- `distsql-read-bytes-ema`
- `distsql-region-location-coverage`
- `distsql-region-task-construction`
- `distsql-select-response-consumption`
- `executor-lack-handles-wave`
- `executor-lead-lag-live-window-runtime`
- `executor-window-ranking-live-runtime`
- `expression-aggregate-descriptor-authority`
- `expression-field-name-resolution`
- `mysql-error-catalog`
- `session-count-warning-wave133`
- `session-protocol-status-publication`
- `session-warning-handler-authority`
- `session-warning-live-publication`
- `shared-terror-error-identity`
- `txnkv-copr-key-ranges`
