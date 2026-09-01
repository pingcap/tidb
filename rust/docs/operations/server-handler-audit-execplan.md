# `server/handler` package audit ExecPlan

## Objective

Certify the complete parent `pkg/server/handler` package against Go master and
record the Rust ownership boundary before auditing its child HTTP packages.

## Inventory and decision

Read all six package artifacts (`BUILD.bazel`, four production files, and the
existing auto-ID owner test): 773 lines total, with no package documentation,
fixtures, generated/platform variants, benchmarks, fuzz targets, or nested
package. Every artifact is byte-identical to fetched Go master
`c6054025ed4c32ab3672a2a24ea46892714d21ec`. Rust has only partial catalog,
model, storage, and server-route owners; the complete PD/MVCC/upgrade/domain
handler is not dependency-closed, so no facade or Rust-only behavior removal is
warranted.

## Validation

The package's failpoint-bearing production source was tested with the canonical
enable/disable wrapper. The full Go package suite passes. Receipt:
`rust/testport/receipts/server_handler.md`.
