# infoschema parity audit: Go `pkg/infoschema` vs the Rust catalog layers

Audit date: 2026-09-05. Opening inventory.

## Architectural mapping

Go concentrates table-metadata resolution in one package (15,398
lines): `infoschema.go`/`infoschema_v2.go` (the schema view),
`builder.go`/`builder_misc.go` (DDL-driven rebuilds), `cache.go` +
`sieve.go` (versioned cache), `cluster.go` (cluster-table plumbing),
`tables.go`/`metric_table_def.go`/`metrics_schema.go` (memory tables),
`bundle_builder.go` (placement bundles), `error.go`, and the
`issyncer`/`validatorapi`/`context` sub-packages.

The Rust port distributes the same responsibilities by function:

| Go responsibility | Rust home |
| --- | --- |
| schema view for the bounded node | `tidb-planner` `configured_catalog.rs` |
| metadata persistence + reload | `tidb-exec` `cluster_catalog.rs`, `catalog_reload.rs`, `catalog_watch.rs` |
| bundle/placement definitions | `tidb-exec` placement modules |
| validity checking | `tidb-domain` `schema_checker.rs` (VERIFIED slice) |
| typed errors | each module's typed error enums |
| memory tables (`tables.go`, metric/metrics schemas) | per-feature modules (bootstrap tables, stats tables); no generic memory-table engine |
| `infoschema_v2`/`sieve` cache machinery | intentionally unported — the bounded node keeps one authoritative image per registry |

## Scope decisions

1. The memory-table engines (`tables.go` 2,938 lines,
   `metric_table_def.go` 3,180 lines, `metrics_schema.go`) are a
   SHOW/INFORMATION_SCHEMA surface: audit per exposed table when that
   surface is wired, not as one batch.
2. `infoschema_v2`/`sieve` are a cache-strategy alternative; the port's
   single-image registry makes them inapplicable (by-design divergence).
3. Behavioral slices proceed in dependency order: (a) DDL reload version
   semantics (`catalog_reload`/`catalog_watch` vs `builder.go`), (b)
   cluster-table plumbing, (c) bundle builder.
