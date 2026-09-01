# `pkg/util/cdcutil` — Go-master parity audit receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

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
