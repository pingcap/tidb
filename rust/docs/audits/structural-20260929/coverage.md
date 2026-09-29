# Crate coverage ledger

Every row has a complete tracked-artifact inventory. “Selected boundaries” means only the concrete register boundaries were inspected; “Inventory only” means no semantic clearance. All rows remain not fully verified.

| Crate | Subsystem | Review depth | Findings | Server dependency | Artifacts |
| --- | --- | --- | --- | --- | --- |
| difftest | Test and support infrastructure | Inventory only | — | no | 749 |
| difftest-parser-tests | Test and support infrastructure | Inventory only | — | no | 5 |
| difftest-planner-tests | Test and support infrastructure | Inventory only | — | no | 81 |
| difftest-result-tests | Test and support infrastructure | Inventory only | — | no | 14 |
| difftest-transaction-tests | Test and support infrastructure | Inventory only | — | no | 43 |
| tidb-allocator-stats | Shared utility contracts | Inventory only | — | possible | 2 |
| tidb-ast | Parser and AST | Inventory only | — | possible | 97 |
| tidb-br | Background jobs and bulk operations | Inventory only | — | no | 15 |
| tidb-chunk | Expressions, values and encoding | Inventory only | — | possible | 31 |
| tidb-codec | Expressions, values and encoding | Inventory only | — | possible | 53 |
| tidb-config | Server, session and domain | Inventory only | — | possible | 28 |
| tidb-datatype | Expressions, values and encoding | Inventory only | — | possible | 108 |
| tidb-ddl-copr | DDL and schema lifecycle | Inventory only | — | no | 2 |
| tidb-ddl-logutil | DDL and schema lifecycle | Inventory only | — | possible | 2 |
| tidb-ddl-mock | Test and support infrastructure | Inventory only | — | no | 2 |
| tidb-ddl-notifier | DDL and schema lifecycle | Inventory only | — | possible | 4 |
| tidb-ddl-resourcegroup | DDL and schema lifecycle | Inventory only | — | no | 2 |
| tidb-ddl-serverstate | DDL and schema lifecycle | Inventory only | — | possible | 2 |
| tidb-ddl-session | DDL and schema lifecycle | Inventory only | — | possible | 4 |
| tidb-ddl-testargsv1 | Test and support infrastructure | Inventory only | — | no | 2 |
| tidb-distsql | Storage and protocols | Inventory only | — | possible | 81 |
| tidb-domain | Server, session and domain | Inventory only | — | possible | 18 |
| tidb-dxf | Background jobs and bulk operations | Inventory only | — | possible | 10 |
| tidb-dxf-operator | Background jobs and bulk operations | Inventory only | — | no | 7 |
| tidb-errmsg | Shared utility contracts | Inventory only | — | possible | 3 |
| tidb-error | Shared utility contracts | Inventory only | — | possible | 42 |
| tidb-exec | Planner and execution | Selected boundaries | [S01](README.md#s01), [S03](README.md#s03), [S07](README.md#s07), [S22](README.md#s22) | possible | 356 |
| tidb-executor | Planner and execution | Selected boundaries | [S07](README.md#s07) | possible | 339 |
| tidb-expr | Expressions, values and encoding | Selected boundaries | [S14](README.md#s14), [S17](README.md#s17), [S18](README.md#s18), [S19](README.md#s19), [S20](README.md#s20) | possible | 185 |
| tidb-funcdep | Planner and execution | Inventory only | — | possible | 5 |
| tidb-gcutil | Storage and protocols | Inventory only | — | possible | 2 |
| tidb-hack | Shared utility contracts | Inventory only | — | possible | 5 |
| tidb-hash | Shared utility contracts | Inventory only | — | possible | 4 |
| tidb-hint | Planner and execution | Inventory only | — | possible | 6 |
| tidb-kvcache | Shared utility contracts | Inventory only | — | possible | 5 |
| tidb-lexer | Parser and AST | Inventory only | — | possible | 24 |
| tidb-log | Shared utility contracts | Inventory only | — | possible | 10 |
| tidb-meta | DDL and schema lifecycle | Inventory only | — | possible | 21 |
| tidb-metadef | DDL and schema lifecycle | Inventory only | — | possible | 7 |
| tidb-model | DDL and schema lifecycle | Inventory only | — | possible | 45 |
| tidb-mysql | Shared utility contracts | Inventory only | — | possible | 11 |
| tidb-naming | Shared utility contracts | Inventory only | — | possible | 2 |
| tidb-owner | DDL and schema lifecycle | Inventory only | — | possible | 6 |
| tidb-parser | Parser and AST | Inventory only | — | possible | 219 |
| tidb-pd-client | Storage and protocols | Inventory only | — | possible | 27 |
| tidb-placement | DDL and schema lifecycle | Inventory only | — | possible | 12 |
| tidb-planner | Planner and execution | Selected boundaries | [S07](README.md#s07), [S08](README.md#s08) | possible | 364 |
| tidb-planner-coretestsdk | Test and support infrastructure | Inventory only | — | no | 2 |
| tidb-proto | Storage and protocols | Selected boundaries | [S13](README.md#s13) | possible | 25 |
| tidb-protocol | Storage and protocols | Inventory only | — | possible | 31 |
| tidb-resolve | Parser and AST | Inventory only | — | possible | 2 |
| tidb-resourcemanager | Server, session and domain | Inventory only | — | possible | 10 |
| tidb-schemacmp | DDL and schema lifecycle | Inventory only | — | no | 12 |
| tidb-schemaver | DDL and schema lifecycle | Inventory only | — | possible | 4 |
| tidb-server | Server, session and domain | Selected boundaries | [S01](README.md#s01), [S02](README.md#s02), [S04](README.md#s04), [S05](README.md#s05), [S06](README.md#s06), [S08](README.md#s08), [S09](README.md#s09), [S12](README.md#s12) | possible | 112 |
| tidb-session | Server, session and domain | Selected boundaries | [S04](README.md#s04), [S08](README.md#s08), [S09](README.md#s09), [S10](README.md#s10), [S11](README.md#s11), [S22](README.md#s22) | possible | 538 |
| tidb-sqlexec | Server, session and domain | Inventory only | — | possible | 2 |
| tidb-sqlexec-mock | Test and support infrastructure | Inventory only | — | possible | 2 |
| tidb-stats | Statistics and observability | Inventory only | — | possible | 79 |
| tidb-stats-handle-autoanalyze-exec | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-autoanalyze-priorityqueue | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-autoanalyze-refresher | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-cache | Statistics and observability | Inventory only | — | possible | 4 |
| tidb-stats-handle-cache-internal | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-cache-internal-lfu | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-cache-internal-mapcache | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-cache-internal-testutil | Test and support infrastructure | Inventory only | — | no | 2 |
| tidb-stats-handle-cache-metrics | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-initstats | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-internal | Statistics and observability | Inventory only | — | no | 2 |
| tidb-stats-handle-logutil | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-metrics | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-usage | Statistics and observability | Inventory only | — | possible | 2 |
| tidb-stats-handle-usage-collector | Statistics and observability | Inventory only | — | possible | 3 |
| tidb-stats-handle-usage-indexusage | Statistics and observability | Inventory only | — | possible | 4 |
| tidb-stats-handle-util | Statistics and observability | Inventory only | — | possible | 7 |
| tidb-stats-handle-util-test | Test and support infrastructure | Inventory only | — | no | 2 |
| tidb-stmtsummary | Statistics and observability | Inventory only | — | possible | 13 |
| tidb-syssession | Server, session and domain | Inventory only | — | possible | 2 |
| tidb-tablecodec | Expressions, values and encoding | Inventory only | — | possible | 9 |
| tidb-tikvutil | Storage and protocols | Inventory only | — | possible | 3 |
| tidb-timer | Background jobs and bulk operations | Inventory only | — | no | 30 |
| tidb-ttl | Background jobs and bulk operations | Selected boundaries | [S04](README.md#s04) | no | 13 |
| tidb-txnkv | Storage and protocols | Selected boundaries | [S05](README.md#s05), [S06](README.md#s06), [S12](README.md#s12) | possible | 203 |
| tidb-unistore | Storage and protocols | Selected boundaries | [S15](README.md#s15), [S16](README.md#s16), [S19](README.md#s19), [S21](README.md#s21), [S23](README.md#s23) | possible | 27 |
| tidb-util | Shared utility contracts | Selected boundaries | [S20](README.md#s20) | possible | 197 |
| tidb-vardef | Server, session and domain | Inventory only | — | possible | 11 |
| tidb-workloadrepo | Statistics and observability | Inventory only | — | possible | 3 |

The complete Go-package ledger, including artifacts and mapping hints, is `inventory.json.gz`. The 855 package units are all marked `not_fully_verified`; review remains open for their complete production variants, tests, fixtures, integration decisions and validation gates.
