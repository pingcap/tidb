# `pkg/util/cdcutil` — complete package transcreation

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The current source
is byte-for-byte unchanged from the pinned extraction.

## Complete inventory

The package contains exactly four artifacts, all read in full. It has one
top-level embedded-etcd test with two subtests and no `doc.go`, generated
source, fixture file, benchmark, fuzz target, example, platform variant, or
build-tagged production variant.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 30 | `805598f965c54edf3a03486aa6f583238a7e50d4` | `a73772cf8f054d1d9dac0bff5e44b9161b6e9cbdfc16ccf903b4e49fa44e82cb` | library/flaky embedded-etcd test targets and client dependencies inventoried |
| `cdc.go` | 265 | `d058d8e947c08be3473d2800b82dedc1590678f5` | `87e619a1f2c345e27f1f0e6227993d77abf53cd6349b4b69e259c7123d559b32` | legacy/namespaced key parsing, state/checkpoint filtering, grouping, and message API inventoried |
| `cdc_test.go` | 167 | `0048ca92af9784c87a105359edda6fb1db8e962b` | `b749b1adf1a53045b668237ad57d958a178fc2551a84e57637303d08514fd7eb` | real embedded-etcd key/state/checkpoint matrix inventoried |
| `export_for_test.go` | 27 | `5e13d85094ceaf5ce7c0848c6dd58c1754ade14e` | `2437697c04184af07638f6e8a0ff5661143d730666031ac62f60a77733941707` | test-only flattened name accessor inventoried |

Total: 489 textual lines. The checkout package is byte-identical to the pin.

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

- `git diff --exit-code origin/master -- pkg/util/cdcutil` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/cdcutil -run '^TestCDCCheckWithEmbedEtcd$' -count=1` — passed; one top-level embedded-etcd test.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-domain cdcutil::tests::test_cdc_check_with_embed_etcd --lib -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check` — passed.
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
