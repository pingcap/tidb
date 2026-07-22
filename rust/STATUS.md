# TiDB Rust rewrite current status

This file is generated from the checked source/test ledgers and workstream
manifests. It is the dispatcher hot path; historical narrative belongs in
`HANDOFF.md`. Regenerate it with `python3 scripts/status-dashboard.py --write`.

These are ownership states, not product-parity percentages. `PARTIAL` means
the source or obligation still has explicit unported behavior.

## Queue

- Active package claims: 3
- Inventory packages: 0
- Declared ready packages: 3
- Active packages: 0
- Covered packages: 2
- Blocked packages: 1

## Package campaigns

| Campaign | Status | Packages |
| --- | --- | --- |
| 2026-07-package-frontier-01 | integrated | 2 |
| 2026-07-package-frontier-02 | planned | 3 |

## Legacy schema-1 evidence

Legacy records are retained as bounded evidence only. They do not
contribute to the package queue, package campaigns, or package completion.

- Legacy claims present: 0
- Legacy slice records: 164
- Legacy campaign records: 28

## Pinned external Go universes

| Module | Version | Production sources | Test files | AST declarations | Runner obligations |
| --- | --- | --- | --- | --- | --- |
| github.com/tikv/client-go/v2 | v2.0.8-0.20260708122311-01bd8f99f4da | 151 | 75 | 809 | 474 |
| github.com/tikv/pd/client | v0.0.0-20260708075407-4e05b9d2c2d3 | 58 | 28 | 319 | 170 |

### External ownership states

| Universe | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| client-go production sources | 120 | 31 | 0 | 0 |
| pd-client production sources | 50 | 8 | 0 | 0 |
| client-go runner obligations | 313 | 161 | 0 | 0 |
| pd-client runner obligations | 147 | 23 | 0 | 0 |
| All external production sources | 170 | 39 | 0 | 0 |
| All external runner obligations | 460 | 184 | 0 | 0 |

External module counts are pinned porting obligations and are not included in TiDB product-parity totals.

## Production source ledger

| State | Count |
| --- | --- |
| UNTRIAGED | 1864 |
| PARTIAL | 478 |
| COVERED | 48 |
| BLOCKED | 0 |

### By target crate

| Target | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| deferred-external | 390 | 0 | 0 | 0 |
| tidb-planner | 199 | 114 | 1 | 0 |
| tidb-server | 249 | 4 | 0 | 0 |
| tidb-exec | 167 | 74 | 0 | 0 |
| tidb-txnkv | 123 | 26 | 9 | 0 |
| tidb-catalog | 123 | 4 | 0 | 0 |
| tidb-expr | 95 | 27 | 0 | 0 |
| tidb-ddl | 119 | 2 | 0 | 0 |
| tidb-session | 44 | 45 | 0 | 0 |
| test-support | 82 | 2 | 0 | 0 |
| tooling | 83 | 0 | 0 | 0 |
| tidb-stats | 1 | 77 | 2 | 0 |
| unassigned | 64 | 0 | 0 | 0 |
| tidb-parser | 13 | 43 | 19 | 0 |
| tidb-datatype | 32 | 22 | 7 | 0 |
| tidb-protocol | 39 | 14 | 3 | 0 |
| tidb-chunk | 16 | 2 | 0 | 0 |
| eliminated-go-runtime | 14 | 0 | 0 | 0 |
| tidb-ast | 8 | 5 | 0 | 0 |
| tidb-distsql | 3 | 4 | 2 | 0 |
| tidb-lexer | 0 | 7 | 0 | 0 |
| tidb-codec | 0 | 6 | 5 | 0 |

## Original test/support ledger

| State | Count |
| --- | --- |
| UNTRIAGED | 15160 |
| PARTIAL | 1233 |
| COVERED | 313 |
| BLOCKED | 14 |

### By differential ring

| Ring | Untriaged | Partial | Covered | Blocked |
| --- | --- | --- | --- | --- |
| unassigned | 4781 | 394 | 124 | 0 |
| result | 4211 | 521 | 29 | 3 |
| deferred-external | 3042 | 0 | 0 | 0 |
| plan | 1730 | 177 | 5 | 0 |
| transaction | 1113 | 103 | 54 | 6 |
| parser | 283 | 38 | 101 | 5 |

## Blocked packages

- `server-internal-dump-package`: MySQL result, prepared-statement, column-metadata, and connection packets share one source-faithful authority for length-encoded values, little-endian integers, binary TIME, and binary DATE/DATETIME/TIMESTAMP values.
