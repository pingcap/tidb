# Complete structural-audit scope

Go reference: `e953a09d9d5e29e60c62f42d3aacebb819af49a5`. Regenerate with
`python3 rust/scripts/build-structural-coverage.py`.

This accounts for **all 83 Rust crates and all 856 inventoried TiDB Go package directories**.
Counts include test/support package directories. The grouping is a work queue, not a semantic mapping or acceptance receipt.
An entrypoint review can establish a finding but cannot clear the rest of its package.
Every row still requires complete production, generated/platform/build, original-test, fixture and integration validation.
The complete [globalconfigsync receipt](global-config-sync-repair.md) records one leaf package and its integration; it does not accept its whole parent crate.
The [restore-utils receipt](restore-utils-protocol-repair.md) records the complete package review and P04 protocol repair; live BRIE and other BR owners remain open.
The [range-tree receipt](rtree-protocol-repair.md) and [progress-ownership follow-up](rtree-progress-ownership-repair.md) record the complete package review and P05 protocol/retained-record repair; dependency and live backup/restore acceptance remain open.
The exact package/artifact list remains in [package-coverage.json](package-coverage.json); no copy replaces it.

## Subsystem queues

| Queue | Go package directories | Rust crates | Confirmed findings / review limits |
| --- | ---: | ---: | --- |
| Syntax and name resolution | 20 | 7 | A01, N01; grammar/AST variants still require full review |
| Values and expressions | 24 | 8 | X01, K03, N01 |
| Planning | 93 | 4 | Q01, C02, E02, M01–M02 |
| Execution | 60 | 4 | E02–E07, K03; E01 repaired |
| Session and authorization | 44 | 3 | A01–A04, B01–B02, S01–S04, I01–I03, N04; C01 repaired |
| Domain and shared services | 11 | 5 | O01–O11, O13, I04, C02; O12 repaired |
| Metadata and tables | 27 | 4 | K01–K03, I04, T01, D01–D11 |
| DDL | 32 | 8 | D01–D11, F01–F03 |
| Statistics | 40 | 19 | O07, C04; LFU lifetime repaired, dependency admission/metrics remain open |
| Storage and distributed reads | 34 | 5 | T01–T03, C03, M01–M04, O03, O13 |
| Protocols and external services | 0 | 3 | P03, T02; P01–P02, P04–P05 repaired; other helpers/variants unreviewed |
| Server and configuration | 31 | 2 | N01–N05, A02–A03, O01–O11, O13; O12 repaired |
| Background jobs and bulk data | 182 | 6 | O04–O06, O10, E05, E07; P04–P05 repaired; other bulk-data packages unreviewed |
| Utilities and errors | 137 | 5 | O11; other utility/error contracts unreviewed |
| Build, tools and test support | 93 | 0 | Original suites/build variants not accepted at current master |
| Other upstream product surfaces | 28 | 0 | Unreviewed: no inference of absence from missing crate names |

## Every Rust crate

The register describes the reviewed entrypoints; **none of these rows asserts full current-master package acceptance**.

| Crate | Primary queue |
| --- | --- |
| `tidb-allocator-stats` | Utilities and errors |
| `tidb-ast` | Syntax and name resolution |
| `tidb-br` | Background jobs and bulk data |
| `tidb-chunk` | Values and expressions |
| `tidb-codec` | Values and expressions |
| `tidb-config` | Server and configuration |
| `tidb-datatype` | Values and expressions |
| `tidb-ddl-copr` | DDL |
| `tidb-ddl-logutil` | DDL |
| `tidb-ddl-mock` | DDL |
| `tidb-ddl-notifier` | DDL |
| `tidb-ddl-resourcegroup` | DDL |
| `tidb-ddl-serverstate` | DDL |
| `tidb-ddl-session` | DDL |
| `tidb-ddl-testargsv1` | DDL |
| `tidb-distsql` | Storage and distributed reads |
| `tidb-domain` | Domain and shared services |
| `tidb-dxf` | Background jobs and bulk data |
| `tidb-dxf-operator` | Background jobs and bulk data |
| `tidb-errmsg` | Utilities and errors |
| `tidb-error` | Utilities and errors |
| `tidb-exec` | Execution |
| `tidb-executor` | Execution |
| `tidb-expr` | Values and expressions |
| `tidb-funcdep` | Planning |
| `tidb-gcutil` | Storage and distributed reads |
| `tidb-hack` | Values and expressions |
| `tidb-hash` | Values and expressions |
| `tidb-hint` | Syntax and name resolution |
| `tidb-kvcache` | Planning |
| `tidb-lexer` | Syntax and name resolution |
| `tidb-log` | Utilities and errors |
| `tidb-meta` | Metadata and tables |
| `tidb-metadef` | Metadata and tables |
| `tidb-model` | Metadata and tables |
| `tidb-mysql` | Syntax and name resolution |
| `tidb-naming` | Syntax and name resolution |
| `tidb-owner` | Domain and shared services |
| `tidb-parser` | Syntax and name resolution |
| `tidb-pd-client` | Protocols and external services |
| `tidb-placement` | Metadata and tables |
| `tidb-planner` | Planning |
| `tidb-planner-coretestsdk` | Planning |
| `tidb-proto` | Protocols and external services |
| `tidb-protocol` | Protocols and external services |
| `tidb-resolve` | Syntax and name resolution |
| `tidb-resourcemanager` | Background jobs and bulk data |
| `tidb-schemacmp` | Values and expressions |
| `tidb-schemaver` | Domain and shared services |
| `tidb-server` | Server and configuration |
| `tidb-session` | Session and authorization |
| `tidb-sqlexec` | Execution |
| `tidb-sqlexec-mock` | Execution |
| `tidb-stats` | Statistics |
| `tidb-stats-handle-autoanalyze-exec` | Statistics |
| `tidb-stats-handle-autoanalyze-priorityqueue` | Statistics |
| `tidb-stats-handle-autoanalyze-refresher` | Statistics |
| `tidb-stats-handle-cache` | Statistics |
| `tidb-stats-handle-cache-internal` | Statistics |
| `tidb-stats-handle-cache-internal-lfu` | Statistics |
| `tidb-stats-handle-cache-internal-mapcache` | Statistics |
| `tidb-stats-handle-cache-internal-testutil` | Statistics |
| `tidb-stats-handle-cache-metrics` | Statistics |
| `tidb-stats-handle-initstats` | Statistics |
| `tidb-stats-handle-internal` | Statistics |
| `tidb-stats-handle-logutil` | Statistics |
| `tidb-stats-handle-metrics` | Statistics |
| `tidb-stats-handle-usage` | Statistics |
| `tidb-stats-handle-usage-collector` | Statistics |
| `tidb-stats-handle-usage-indexusage` | Statistics |
| `tidb-stats-handle-util` | Statistics |
| `tidb-stats-handle-util-test` | Statistics |
| `tidb-stmtsummary` | Session and authorization |
| `tidb-syssession` | Domain and shared services |
| `tidb-tablecodec` | Values and expressions |
| `tidb-tikvutil` | Storage and distributed reads |
| `tidb-timer` | Background jobs and bulk data |
| `tidb-ttl` | Background jobs and bulk data |
| `tidb-txnkv` | Storage and distributed reads |
| `tidb-unistore` | Storage and distributed reads |
| `tidb-util` | Utilities and errors |
| `tidb-vardef` | Session and authorization |
| `tidb-workloadrepo` | Domain and shared services |

## External package obligations

| Inventory | Package directories | Revision |
| --- | ---: | --- |
| [client-go](client-go-package-coverage.json) | 41 | `v2.0.8-0.20260928031501-8edb23f6c7ee` |
| [kvproto](kvproto-package-coverage.json) | 41 | `v0.0.0-20260820070758-623e58e60fa9` |
| [pd-client](pd-client-package-coverage.json) | 24 | `v0.0.0-20260805103528-afa43111d149` |
| [etcd-api](etcd-api-package-coverage.json) | 7 | `v3.5.15` |

TiPB complete-source and descriptor receipts are separate. Other external modules still need full inventories; the four rows above are not all dependencies.
Native client-rust's local latches and region TTL have live production callers; their existence does not accept all 41 client-go packages.

## Upstream surfaces not yet assigned a runtime owner review

These are explicit outstanding scope, not automatically confirmed defects:

- `pkg/extension`
- `pkg/extension/_import`
- `pkg/extension/extensionimpl`
- `pkg/extworkload`
- `pkg/extworkload/client`
- `pkg/inference`
- `pkg/inference/domainadaptor`
- `pkg/inference/embedding/base`
- `pkg/inference/embedding/batcher`
- `pkg/inference/embedding/cohere`
- `pkg/inference/embedding/gemini`
- `pkg/inference/embedding/huggingface`
- `pkg/inference/embedding/internal/testutil`
- `pkg/inference/embedding/jina`
- `pkg/inference/embedding/mock`
- `pkg/inference/embedding/nvidia`
- `pkg/inference/embedding/openai`
- `pkg/inference/embedding/tidbcloud`
- `pkg/lock`
- `pkg/lock/context`
- `pkg/metaservice`
- `pkg/param`
- `pkg/plugin`
- `pkg/plugin/conn_ip_example`
- `pkg/standby`
- `pkg/telemetry`
- `pkg/tidbmanager`
- `pkg/workloadlearning`

NextGen/starter/standby and platform/build variants remain in the artifact inventory. Classic-only refusals must be compared with Go classic before becoming findings.
Benchmarks and distributed fault testing remain required; none was performed by this scope generator.
