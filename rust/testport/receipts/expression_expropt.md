# `pkg/expression/expropt` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains 13 tracked artifacts and 1,116 lines. Every production
source, test source, and Bazel target was read in full before editing. There is
no `doc.go`, fixture directory, generated output, platform-specific variant,
or additional generator input in this package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 50 | `5752c70ae82fe02a6030a615a7c3c065be70afdf` | `4fcae1b742b804de0cf559b1808b013faf8c6cf3e980cd576f53896a08f81275` | public library and flaky optional-provider test target |
| `advisory_lock.go` | 66 | `55bb4585b093265a469913e84ec763867ac3d9e0` | `02c2805c59c30e37ea006466de50b67699f8524ac22f3fc5e0125c37e053050d` | advisory-lock provider and reader |
| `current_user.go` | 63 | `7f4c684279fb8456a70811609aca13eb085194f9` | `4de5aca00146a0bbba097013f3a04f54b99539841587a184e28c05cdb5889da8` | current-user and active-role provider/reader |
| `ddlowner.go` | 48 | `05508ec30f1e963eeb9cc4da8496b17c1dbf194f` | `5e9c4e28bb879693492d5a5a82e270b330b1f921dc69085e893c075137c37f98` | DDL-owner provider and reader |
| `infoschema.go` | 54 | `cd103aa9912a07ccc853565f5da7f13c93693eae` | `7a30c016f75990e2ea4083b7ca6c210c00d1e9b0cef4921294512782d699e3f5` | session/latest information-schema provider and readers |
| `kvstore.go` | 47 | `18c7d3d11d1c716ebb746a5ce66bcefc09f4005f` | `e35da06ebf4c51b742763a673896963912d8a235e04b268bbaa6c1ba8f7523d3` | KV storage provider and reader |
| `optional.go` | 124 | `9cd8ae10576073cf0845f6fa645e6586f271f47d` | `307b81873a739e8421c410087b147a089e7dcc609436223174175efe87c0ec52` | provider array, type checks, key-set collection, generic lookup |
| `optional_test.go` | 319 | `35a69807f6de093780c5eb212759525bfe293701` | `2791e7691cb8839b66c08d53dadc5bdd0fe273d3dbcdcb97e1fe32f1204f2580` | all ten optional-provider registration/reader cases |
| `priv.go` | 55 | `2afed7089ca157868e8ea114de032ec689d8ef3d` | `e7a7e7c4bc5dce1f25d0e80c2f3ae180153a72b381a329438ba136486d3ff32d` | privilege-checker contract, provider, and reader |
| `sequence.go` | 57 | `b00357bcd1b3834a8f32c7bf183600f2d81111c4` | `886839a17622e0355075ecec6764d563039d229629b88e7f42dc3bea5d8358c1` | sequence operator contract, provider, and reader |
| `sessioncontext.go` | 68 | `9965f54e4c67bde2c6478420d394cc6858cd24e2` | `e5320c164c4fed5ff325b3851cff29c7138f16f3ff8eb0bea60fe85297650d05` | EMBED_TEXT session-context contract, provider, and reader |
| `sessionvars.go` | 62 | `8c0528baf9d0678d4f6d2779e9118d24710de058` | `47253f6ef7b45e309f24986cbdbde02b5b1dbbdc79224213f348db58e0cbb7b0` | session-variable provider and reader |
| `sqlexec.go` | 63 | `a0bf9a43777d95522b85eb11164fe26f2804aa08` | `b86b9281d1771948fbc71067c047dd1c3845ab35a5bcecea52299dd14096e5c2` | restricted SQL executor contract, provider, and reader |

The production sources define 41 concrete functions/methods, in addition to
the provider interfaces' method contracts. The single test source defines one
top-level test and exercises the provider array in key order, absent-provider
errors, concrete-type recovery, provider-owned errors, and all ten property
keys, including the nil session-context result.

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read in full: the BUILD target
adds `sessioncontext.go`, `OptionalEvalPropProviders.Add` admits
`OptPropSessionContext`, and the test loop covers the new provider/reader. No
other production, test, fixture, platform, generated, or build-input delta
exists in this package.

## Rust ownership and parity fix

The Rust owner is `rust/crates/tidb-expr/src/expropt/`, with the key/descriptor
table in `src/exprctx.rs` and live-session installation in
`src/sessionexpr.rs`. The production provider modules mirror all eleven Go
files, including the new `sessioncontext.rs`; `tests.rs` carries the complete
Go optional-provider test. `sessionexpr::EvalContext::new` installs the
session-context provider through a narrow adapter so `GetTraceCtx`,
`GetSessionVars`, and `GetDomain` remain available without introducing a
crate cycle.

Before this fix Rust had only nine keys/providers: the focused regression
failed with `OPT_PROPS_CNT == 9` while Go master requires ten. The fix adds the
tenth key/descriptor at its source position, preserves the source descriptor
string typo for `SequenceOperator`, implements nil-preserving and non-nil
session-context providers/readers, and wires the live evaluator to expose the
property. No Rust-only behavior was found or retained.

## Validation and risk

Profile: **Ready** for this code batch. Rust production and test sources
changed, so the focused regression, dependent session tests, exact Go-master
package test, formatting, diff checks, and Ready lint gate were run. Go source,
imports, Bazel targets, and module files were not changed; `make bazel_prepare`
was therefore not required.

```text
# Regression before implementation (expected failure):
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  expropt::tests::session_context_property_regression --offline --locked
# failed: left 9, right 10

# Rust focused tests after implementation:
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  expropt::tests --offline --locked                         # 2 passed
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  exprctx::tests --offline --locked                         # 10 passed
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  sessionexpr::tests --offline --locked                     # 6 passed

# Exact Go-master source (detached worktree at 5e8a1a229a):
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/expression/expropt -count=1                         # passed, 0.542s

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
make lint
```

The Ready lint gate is run from the repository root after the receipt and plan
updates are staged. Not verified here: EMBED_TEXT's live embedding service,
real domain/context implementations, Bazel execution, Windows behavior, or
full-workspace Go/Rust test suites. The Rust boundary uses opaque `Any` handles
for Go `context.Context`/`any`; this preserves ownership and nil semantics but
does not invent a cross-crate context implementation.

This receipt certifies the bounded `pkg/expression/expropt` inventory and
transcreation; it is not a repository-wide parity claim.
