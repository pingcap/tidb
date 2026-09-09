# `pkg/server/handler` Go-parity receipt

## Source and inventory

- Go comparison source: fetched `origin/master` at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- The parent package contains six artifacts and 773 lines: `BUILD.bazel` (49),
  `auto_id_owner_handler.go` (49), `auto_id_owner_handler_test.go` (57),
  `tikv_handler.go` (286), `upgrade_handler.go` (217), and `util.go` (115).
- All six production/build/test artifacts were read completely and are
  byte-identical to Go master. There is no `doc.go`, fixture/testdata input,
  generated/platform variant, benchmark/fuzz target, or nested package in this
  package; the `extractorhandler`, `optimizor`, `tests`, `tikvhandler`, and
  `ttlhandler` directories are separate packages with their own receipts.

## Boundary decision

This package is a Go HTTP and TiKV/domain adapter. The Rust tree has partial
owners for catalog/schema JSON, transaction storage, model metadata, and the
server's schema routes, but no dependency-closed Rust implementation of the
complete handler package (including PD region lookup, MVCC, cluster upgrade
state, and Go testkit lifecycle). No Rust-only behavior was found to remove and
no speculative facade was added. The package remains an explicit Go boundary.

## Validation

The package uses failpoints (`tikv_handler.go`), so the canonical wrapper was
used:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/server/handler -count=1
```

The full package test passes. No production fix was required in this batch;
the inventory and boundary are recorded for the rolling audit.
