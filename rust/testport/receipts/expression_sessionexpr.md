# `pkg/expression/sessionexpr` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 837 lines. Every production
source, test source, and Bazel target was read in full before editing. There is
no `doc.go`, fixture directory, generated output, platform-specific variant,
or additional generator input in this package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 56 | `e1d179ca49f3ab78f75ea35cf9afb0d59dd052b3` | `297527cb94d33c9c48e3d0eb1569fc2ea278b920ff0c26c8553a2bfd422f3067` | public live-session library and five-shard flaky test target |
| `sessionctx.go` | 448 | `ba58fb66e6db3af1b7f58c2e705fca1fd26657f1` | `e3e5543731a6003a74e2561e6bba145939be9eb75210be1888ddc313388c743f` | live Expr/Eval contexts, timestamp/privilege/property adapters, sequence operator, and static-conversion accessors |
| `sessionctx_test.go` | 333 | `db39bac3f5fd92dd613d8108106bf14f34d6a205` | `6041d74a16d78570dbbf127a41b922005b69dcdf6397d5f5bd3753b3bc786264` | five tests for live fields, timestamps, privilege checks, optional providers, and build context |

The production source declares the live expression/evaluation constructors and
accessors, warning/time/parameter paths, privilege checks, all ten optional
property registrations, sequence lookup/operator methods, and static snapshot
bridges. The test source defines five top-level tests and exercises every
package-owned branch. The current master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read in full: `BUILD.bazel` and
`sessionctx.go` add the `collate` dependency and `NewCollationEnabled` accessor,
and `NewEvalContext` installs the `SessionContext` optional provider. No other
artifact changed.

## Rust ownership and parity status

The Rust owner is `rust/crates/tidb-expr/src/sessionexpr.rs`; it reuses the ten
provider/reader pairs in `src/expropt`, the optional keys in `src/exprctx`, and
the static snapshot machinery in `src/exprstatic`. The owner carries all five
Go test behaviors: live session values and warning routing, stale-TSO and
`timestamp` precedence/caching, nil-versus-bound privilege decisions, all
optional providers (including SessionContext), and the build-context fields,
allocation, plan-cache, readonly-user-var, and static conversion paths.

The master additions are source-compatible. `EvalContext::new` installs the
session-context provider through a narrow adapter forwarding trace context,
session variables, and domain; `ExprContext::new_collation_enabled` reads the
same process-wide runtime mode as Go's `collate.NewCollationEnabled`; and both
live `EvalContext` types implement the shared `exprctx::ParamValues` contract.
The focused optional-provider and session-build regressions pass.

The Rust boundary intentionally narrows `sessionctx.Context`, SessionVars and
StatementContext, privilege manager, infoschema, sequence table, and Oracle TSO
conversion to the methods this package calls. Opaque `Any` handles preserve Go's
context/domain nil semantics without inventing a cross-crate session graph.

## Validation and risk

Profile: **Ready** for the implementation batch that added the SessionContext
provider, parameter contract, and collation accessor. This receipt records the
complete package inventory; no Go files, imports, Bazel targets, or module files
were edited in the receipt update, so `make bazel_prepare` was not required.

```text
# Exact Go-master package (detached worktree at 5e8a1a229a):
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/expression/sessionexpr -count=1                         # passed

# Rust live-session carriers:
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  'sessionexpr::tests::' --offline --locked                            # 6 passed
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  'expropt::tests::' --offline --locked                                 # 2 passed
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 fmt \
  --manifest-path rust/Cargo.toml --all -- --check
git diff --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
```

Not verified here: Bazel execution, Windows behavior, full workspace suites,
real privilege/session implementations, and live sequence/infoschema backends.
The stale-TSO and timestamp paths depend on the provided session boundary to
preserve Go's statement-time precedence and error behavior.

This receipt certifies the bounded `pkg/expression/sessionexpr` inventory and
its current carriers; it is not a repository-wide parity claim.
