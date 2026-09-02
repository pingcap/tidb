# `pkg/meta` — starter bootstrap and next-gen version parity receipt

Status: complete direct-package inventory for this batch; restored the
Go-master starter-bootstrap API and the materialized-view next-gen boot-table
version, with matching Rust `tidb-meta` behavior. Nested packages under
`pkg/meta/autoid`, `pkg/meta/model`, and `pkg/meta/metadef` remain separate
package boundaries and are covered by their own receipts.

Comparison source: Go `origin/master` at
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02).

## Complete Go inventory

The direct package has seven tracked artifacts and 4,107 lines after the
focused regression additions. Every production, test, support, and build
artifact was read before editing. There is no `doc.go`, fixture/testdata
directory, generated production input/output, platform-specific variant, or
separate benchmark file; benchmark functions, when present, are in
`meta_test.go` and are included in the test inventory.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 70 | package and test target metadata |
| `OWNERS` | 5 | package ownership |
| `main_test.go` | 34 | package test setup and leak checks |
| `meta.go` | 2,234 | metadata keys, mutator behavior, IDs, bootstrap and schema operations |
| `meta_autoid.go` | 211 | metadata-backed auto-ID helpers |
| `meta_test.go` | 1,479 | metadata lifecycle, key, bootstrap, and benchmark coverage |
| `reader.go` | 74 | snapshot reader interface and constructor |

The direct production sources contain 171 function/method declarations and
the test/support sources contain 35 declarations. Nested package artifacts
were inventoried separately and are not silently folded into this receipt.

## Implemented behavior

- Go now exposes `mStarterBootstrapKey = []byte("StarterBootstrapKey")`,
  `GetStarterBootstrapVersion`, and `FinishStarterBootstrap`. Reads preserve
  the absent-key default of zero; writes use Go's decimal `int64` string
  representation and are surfaced through the `Reader` interface.
- Go now declares `MaterializedViewNextGenBootTableVersion = 3`.
- Rust `tidb-meta` owns the same logical key, encoded string key, mutator
  accessors, malformed-scalar behavior, and
  `NextGenBootTableVersion::MATERIALIZED_VIEW` value. No cache-only or
  Rust-only substitute was added.

## Regression and validation

Profile: Ready for this package batch. The regression was written first and
the pre-fix runs were recorded before production edits.

- Pre-fix Go focused run failed to compile with missing
  `GetStarterBootstrapVersion`, `FinishStarterBootstrap`, and
  `MaterializedViewNextGenBootTableVersion` symbols.
- Pre-fix Rust focused run failed to compile with missing starter-bootstrap
  methods and `NextGenBootTableVersion::MATERIALIZED_VIEW`.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/meta -run '^(TestMeta|TestBootTableVersion)$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/meta -count=1` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-meta --test all meta_starter_bootstrap_round_trip -- --nocapture` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `make bazel_prepare` — required for Go package source/test changes by repository policy; locally blocked because the `bazel` executable is unavailable.

## Risks and unverified surfaces

- The new reader method is an interface expansion; the package compile-time
  assertion and full package suite verify the in-tree `Mutator` implementation,
  while external implementations must add the corresponding accessor.
- Rust's transaction owner uses the existing Go-compatible integer codec for
  the starter key; storage-adapter integration beyond `MemoryTransaction` was
  not run locally.
- Session bootstrap and InfoSchemaV2 tests listed in the historical `b039`
  receipt remain separate dependency boundaries and are not claimed here.
