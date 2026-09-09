# `pkg/expression/exprstatic` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains five tracked artifacts and 2,036 lines. Every production
source, test source, and Bazel target was read in full before editing. There is
no `doc.go`, fixture directory, generated output, platform-specific variant,
or additional generator input in this package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 54 | `484660a6a4481020bff7525b5b91441bab4606a5` | `d3e98b58d3e46e47306f71c9a54bc4b7eb3293a720d9c742a94e761cab418017` | public library and thirteen-shard flaky test target |
| `evalctx.go` | 536 | `5a60bb5ff9f9ab23d2cf7729e11d4ff9d5628da7` | `cd67c99d79b4f19fc509bbafd3652b7d1a1ffe9410d984406b0b76eadec12ae4` | static evaluation state, options, warnings, time caching, params, system-variable loading, and static conversion |
| `evalctx_test.go` | 678 | `d2319d998666aa4e1f21b20d5a31613b6b4ccb8e` | `8ce294108753b3208456bbec7efe1d1b7fe829d07d906063470e4eaef05b5bf8` | eight tests for defaults/options, current time, warnings, optional properties, Apply, params, cloning, and system variables |
| `exprctx.go` | 384 | `0704d0bfe80d107eeaba11a0d6c417c25e381ec8` | `42053c440b0901c3509d10eb7a292cb3760ba0c3da49186a7cc338c67776cafd` | static expression state/options, all accessors, cloning, collation mode, and system-variable loading |
| `exprctx_test.go` | 384 | `80d33b046b72ee15e7ae243290991dfcc20188b5` | `08a35423738ec5e7369015327c60459317eb15bc742c2ed8401510116655343e` | five tests for construction, option application, allocation, cloning, and system-variable loading |

The two production sources declare 47 concrete functions/methods plus the
option and conversion contracts. The thirteen top-level tests exercise every
branch of the package-owned state transitions. The current master delta from
the earlier pinned source `e2788410d8d696605e8cb002585877a063ccc909` was read
in full: `exprctx.go` adds the `newCollationEnabled` field, option, default,
accessor, and static-copy wiring; the test state and deep-copy exclusions are
updated; and `BUILD.bazel` adds the `collate` dependency. `evalctx.go` and its
tests are unchanged by that delta.

## Rust ownership and parity status

The Rust owner is `rust/crates/tidb-expr/src/exprstatic/{evalctx,exprctx}.rs`
with public re-exports in `src/exprstatic/mod.rs`. The live-session counterpart
`src/sessionexpr.rs` supplies the same `NewCollationEnabled` read when the
static conversion trait is implemented. The Rust module carries all thirteen
Go test behaviors: default and option state, warning-handler routing, cached
current time and Apply location semantics, optional-property replacement,
parameter-list copying across reset, deep static snapshots, and every listed
system-variable update.

The master-added collation mode is now source-compatible: static construction
captures `tidb_datatype::new_collation_enabled()` by default, an explicit
option overrides it, `apply` preserves/overrides it, and
`make_expr_context_static` copies it. The focused regression
`new_collation_mode_is_captured_per_context` failed before the option/accessor
existed and passes now; the static deep-copy carrier and live-session build
carrier also assert the value.

One explicit boundary remains: Go's `newSessionVarsWithSystemVariables` asks the
full session sysvar catalog to reject unknown names, while this crate's
dependency-closed `StaticSessionVars` parses only variables consumed by these
two files and accepts/ignores other names. The umbrella `exprctx` interfaces
remain a separate boundary documented in `expression_exprctx.md`; no fabricated
session graph or Rust-only global replacement was introduced here.

## Validation and risk

Profile: **Ready** for the implementation batch that closed the collation
field. This receipt records the complete package inventory and the existing
Rust carriers; no Go files, imports, Bazel targets, or module files were edited
in the receipt update, so `make bazel_prepare` was not required.

```text
# Exact Go-master package (detached worktree at 5e8a1a229a):
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/expression/exprstatic -count=1                         # passed, 0.483s

# Rust package carriers:
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  'exprstatic::exprctx::tests::' --offline --locked                    # 6 passed
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  'sessionexpr::tests::' --offline --locked                            # 6 passed
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 fmt \
  --manifest-path rust/Cargo.toml --all -- --check
git diff --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
```

Not verified here: Bazel execution, Windows behavior, full workspace suites,
the full session sysvar catalog, and the deferred umbrella-interface wrapper.
The static/live collation split intentionally follows Go's snapshot-versus-
global semantics, so changing bootstrap order remains a compatibility risk.

This receipt certifies the bounded `pkg/expression/exprstatic` inventory and
its current carriers; it is not a repository-wide parity claim.
