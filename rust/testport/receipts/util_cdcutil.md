# `pkg/util/cdcutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains `BUILD.bazel`, production `cdc.go`, external
source test `cdc_test.go`, and test-only support `export_for_test.go`. It has
one top-level test with two subtests and no `doc.go`, generated source,
fixture file, benchmark, fuzz target, example, platform variant, or
build-tagged production variant. The checkout package is byte-identical to the
pin.

## Rust ownership and integration

`tidb-domain::cdcutil` implements the complete package against the existing
`EtcdOps` boundary, whose production server adapter wraps the PD etcd client.
It discovers both legacy and namespaced changefeed keys, rejects backup,
cluster-only, malformed-cluster, and unrelated keys, reads info and status
records, applies every pinned state, uses start TS when status is absent, and
selects checkpoints older than the supplied safe TS. Removed and finished
feeds use `u64::MAX`, matching Go's sentinel.

`CDCNameSet` retains Go's cluster/namespace grouping, legacy `<nil>` label,
emptiness check, and user-facing list spelling. Invalid states and
incompatible feeds retain Go's warning/info observability. The sole Rust test
identity covers both source subtests with an in-memory `EtcdOps`; Go's own
embedded-etcd suite validates the pinned package against real etcd. The
test-only name flattener maps `export_for_test.go`.

The pinned consumers are BR stream backup, Lightning precheck, and executor
import precheck. Those composition roots are not yet complete Rust packages;
the package is exposed through the same etcd boundary they use rather than a
consumer-specific workaround.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/cdcutil` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/cdcutil -count=1` — passed outside the sandbox; one top-level embedded-etcd test.
- `cargo check -p tidb-domain` — passed.
- `cargo test -p tidb-domain --lib cdcutil::tests::test_cdc_check_with_embed_etcd` — passed.
- `cargo fmt -p tidb-domain` — passed.
- `git diff --check` — passed.

The first Go test attempt was sandbox-blocked from opening the embedded-etcd
Unix listener; the same command passed with socket access allowed. No Go
source, Go test, Bazel metadata, or Go module file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: the formerly absent package now implements all pinned states,
  key formats, checkpoint rules, grouping, errors, and messages.
- Compatibility: both TiCDC key generations and the pinned safe-TS boundary
  use the same strict comparisons and sentinels as Go.
- Performance: one prefix scan discovers feeds and at most two exact reads are
  made per active feed, matching Go's etcd access shape.
- Not verified locally: a Rust production PD-etcd adapter connected to a live
  TiCDC deployment. The pinned Go embedded-etcd test and Rust boundary test
  cover the complete package logic and key/value protocol.
