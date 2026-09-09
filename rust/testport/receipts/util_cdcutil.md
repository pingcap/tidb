# `pkg/util/cdcutil` — Go-master parity audit receipt

Original full-audit Go authority: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`. The corrective return-contract
follow-up below refreshes the authority to `c767f6fd8c01e9dcb459767611c0c1d4110d210d`.

## Complete inventory

The package contains exactly four artifacts, all read in full (489 lines
total):

- `cdc.go` (265 lines): legacy and namespaced key parsing, state/checkpoint
  filtering, grouping, and message API;
- `cdc_test.go` (167 lines): the embedded-etcd source matrix;
- `export_for_test.go` (27 lines): the test-only flattened-name accessor;
- `BUILD.bazel` (30 lines): production and flaky embedded-etcd test targets.

There is no `doc.go`, README, fixture, testdata, benchmark, fuzz target,
example, generated/platform variant, nested package, or other build input. All
four files are byte-identical to the Go-master checkout.

## Rust ownership and audit result

`rust/crates/tidb-domain/src/cdcutil.rs` owns the complete package against the
ordinary `EtcdOps` boundary. It preserves both legacy and namespaced key
formats, valid-cluster filtering, backup/noise rejection, all source states,
start/checkpoint fallback, safe-TS comparison, removed/finished `u64::MAX`
sentinel behavior, cluster/namespace grouping, legacy `<nil>` naming, and
user-facing message rendering. The source-derived Rust test uses the same
matrix as Go's embedded-etcd test; no alternate CDC facade or Rust-only
behavior remains.

No Go or Rust production delta was found in this rolling audit, so no new
package-local regression was warranted. The existing source-derived test is
the focused regression carrier.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/cdcutil -run '^TestCDCCheckWithEmbedEtcd$' -count=1` — passed (current checkout).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/cdcutil -run '^TestCDCCheckWithEmbedEtcd$' -count=1)` — passed (detached Go master).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-domain cdcutil::tests::test_cdc_check_with_embed_etcd --lib --offline --locked -- --test-threads=1` — passed (one source-derived test).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed for the current repository Ready gate.
- `git diff --check` — passed.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The Go test starts embedded etcd but does not use TiDB
failpoints, so the failpoint wrapper is not applicable.

## Risks and boundaries

- Correctness: the source matrix covers both key generations, invalid/noise
  keys, every accepted state, checkpoint/start fallback, and safe-TS edges.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; the existing etcd prefix/exact-read
  access shape is unchanged.
- Not verified locally: a live production PD-etcd adapter; the package-level
  source matrix and ordinary Rust boundary cover the implemented logic.

## 2026-09-07 corrective return-contract follow-up

Current Go master `c767f6fd8c01e9dcb459767611c0c1d4110d210d`
was read package-completely before the Rust edit. The inventory remains exactly
four artifacts and 489 lines: `cdc.go` (265), `cdc_test.go` (167),
`export_for_test.go` (27), and `BUILD.bazel` (30). The full production surface
includes both key versions, the three key constants, changefeed key builders,
the etcd client loader/checkpoint/filter pipeline, `CDCNameSet` save/query/
message methods, and the two exported changefeed queries. The test surface is
one top-level embedded-etcd test plus its two helpers and the test-only
flattened-name accessor. There is still no `doc.go`, `TestMain`, fixture,
testdata, benchmark/fuzz target, example, generated/platform/build-tag
variant, nested package, or other build input. All four files are byte-identical
to current Go master.

The complete 17-artifact, 11,721-line pre-edit `tidb-domain` crate and all
references to its self-contained `cdcutil.rs` owner were inventoried. Existing
behavior remains unchanged: legacy/namespaced key parsing, cluster validation,
state/checkpoint fallback, invalid-TS filtering, grouping, and user-message
formatting. This follow-up removes only the Rust-exclusive `#[must_use]`
diagnostics from `CDCNameSet::is_empty` and `CDCNameSet::message_to_user`, the
direct counterparts of Go's discardable `Empty` and `MessageToUser` methods.

The focused `#[deny(unused_must_use)]` regression failed before the fix with
exactly two unused-return diagnostics and passes after it. Ready evidence:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-domain --lib cdcutil::tests::cdc_name_set_returns_may_be_ignored_like_go -- --exact --nocapture` — failed before the fix with exactly two diagnostics, then passed 1/1.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-domain --lib -- --test-threads=1` — passed 160/160.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-domain --all-targets` — passed with pre-existing warnings.
- `rustfmt +nightly-2026-08-22 --edition 2021 --check rust/crates/tidb-domain/src/cdcutil.rs` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed.
- `git diff --check` — passed.

This is Rust/testport-only work. No Go source, Go import, Go test, Bazel file,
or module dependency changed, so `make bazel_prepare` is not required. The Go
embedded-etcd test has no TiDB failpoints, so the failpoint wrapper is not
applicable.
