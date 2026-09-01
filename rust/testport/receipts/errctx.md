# `pkg/errctx` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 389 lines. Production error
context, its source test, and the public Bazel target were read line by line
before this receipt. There are no fixtures, generated sources, platform
variants, benchmarks, fuzz targets, or additional test support files.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `pkg/errctx/BUILD.bazel` | 29 | `6fe6f78f5ee1af294ef1639a17a8f4ae2d372c6f` | `f313aebd6b8b83a16a36bdb8f30835dec24eb8e8b4816b6594fb66a15a8c1834` | public library and flaky short unit target |
| `pkg/errctx/context.go` | 266 | `34fae1304463c0a1a442ce5573e70abefd421887` | `0c22410a2286ae9744177657c9afa0855bd248c8faa5e1c5921b7c3ad64bb064` | error groups, levels, warning sink, and handlers |
| `pkg/errctx/context_test.go` | 94 | `2a94d77e57fb54f81599bbd18cd76be9f4ad787c` | `a4ddec6f7ea09b105ee931f1e72a0242882e5120c75f2643e804cbcd2ff198e8` | context, alias, multi-error, and copy-on-write tests |

## Rust ownership and integration decision

`tidb-error::errctx` is the dependency-closed Rust owner and is integrated by
the executor/session error paths. It preserves all seven `ErrGroup` values,
the exact source error-code membership, strict/warn/ignore levels, immutable
context copies, warning/note publication, `ErrorGroup` first-error handling,
root-cause matching, the strict no-warning singleton, and
`ResolveErrLevel(ignore, warn)` precedence. The warning appender and error
group are native Rust seams for the corresponding Go interfaces; they do not
add uncalled behavior. The package is therefore a complete parity claim.

The source-derived `errctx_source` tests cover `TestContext` plus a full map
membership guard and pass without a production change in this batch.

## Validation and risk

Profile: **WIP** for this documentation-only authority refresh; the repository
package loop continues and no Go source, imports, Bazel metadata, or module
files changed. `make bazel_prepare` and a new Ready lint run are not required
for this no-code batch.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/errctx -count=1
# passed: package compiled and source TestContext passed

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-error --test all errctx -- --test-threads=1
# passed: 2 errctx source tests

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-error --all-targets
# owner crate check is covered by the workspace validation run
```

No behavior, compatibility, or performance risk was introduced. Full
workspace tests, Bazel execution, and downstream integration consumers are not
re-run for this documentation-only receipt.
