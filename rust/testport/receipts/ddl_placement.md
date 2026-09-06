# `pkg/ddl/placement` → `tidb-placement`

Historical pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.
Current Go source rechecked at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Atomic inventory

| Go artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 16 | `1ca9a35ba475292d0ea64c9893d843b898a4d1d9` | workspace crate and package test target |
| `bundle.go` | 705 | `5be11cb097c7c14608cf29403d926aab300bdc99` | `bundle.rs`: bundle construction, normalization, serialization, key ranges, and policy expansion |
| `bundle_test.go` | 1481 | `4113bbaf6344fe628e1523ead9eab587814e9523` | inline `bundle` tests |
| `common.go` | 82 | `747c5a0923cbb81d6ee8ed46df581d372237e5ea` | `common.rs`: constants and group-id helper |
| `common_test.go` | 27 | `b22226f29c94862d6e9e99671a0b3960eff0b26d` | inline `common` test |
| `constraint.go` | 136 | `8d017c93523c3ddb2c68c701a0d4c358215f7cdf` | `constraint.rs`: YAML and label-constraint conversion |
| `constraint_test.go` | 273 | `8099e785e7c4fa6a9127cf5aeb9e88401af71969` | inline `constraint` tests |
| `constraints.go` | 169 | `adb830adb034d9b761edd8c751909c93f0758f54` | `constraints.rs`: parsing, fingerprints, add/restore, and compatibility |
| `constraints_test.go` | 168 | `a8d9899999d2c358fd2b462005f82efc8fd766b1` | inline `constraints` tests |
| `errors.go` | 52 | `99842ba9a3a342f3017061987935893017f6ba24` | `errors.rs`: Go sentinel identity and wrapping carrier |
| `meta_bundle_test.go` | 371 | `44639ba73a915c9a4ba26723e82e987d4c6b6b62` | `tests/meta_bundle_test.rs`: in-memory `PolicyGetter` carrier |
| `rule.go` | 180 | `ab15e5f582e91bcd9826f99e897c1a9c4d67c25c` | `rule.rs`: builder, rule construction, and YAML rule maps |
| `rule_test.go` | 184 | `89232e44fc759f123fbbdf2f9aec0448f7e468d3` | inline `rule` tests and the regression below |

The package contains 13 tracked Go artifacts and 3,844 Go lines. No package
doc, platform-specific source, generated output, fixture, benchmark, fuzz,
example, or extra build-input artifact exists in the current Go tree. The Go
package is byte-identical to the historical pin. The complete Rust owner is
`rust/crates/tidb-placement`: `Cargo.toml` (24 lines), `src/lib.rs` (89),
`bundle.rs` (2,285), `common.rs` (92), `constraint.rs` (407),
`constraints.rs` (356), `errors.rs` (151), `pd.rs` (173), `rule.rs` (504),
`yaml_lite.rs` (582), and `tests/meta_bundle_test.rs` (371). Every production
and private helper, inline test, aggregate-test registration, workspace/lock
entry, and direct builder caller was read before editing.

## Behavior mapping

The owner preserves Go's placement-policy bundle behavior: rule and constraint
parsing, placement settings and policy lookup, role/count defaults, rule merge
and tidy ordering, leader/DC selection, key-range encoding, JSON omission and
error behavior, and the table/partition/full-table bundle constructors. The
`pd` module is the local JSON DTO boundary for the external PD value types;
`yaml_lite` carries the narrow strict YAML behavior needed by this package.
The 25 inline owner tests plus four metadata integration tests cover the 26 Go
tests listed in the historical `b050` receipt, including the Go shared-policy
mutation quirk and the in-memory replacement for the `meta.Mutator` test
scaffold.

## Follow-up closure — Go pointer-builder semantics (2026-09-06)

Go's `NewRuleBuilder` returns a pointer. Each `Set*` method mutates that same
builder and returns the pointer only as fluent sugar; callers may ignore any
setter return. Rust previously consumed `self` in all four setters and marked
both the setters and constructors `#[must_use]`. An ignored setter therefore
moved the builder and could not apply the mutation, a Rust-only behavior.

The setters now take `&mut self` and return `&mut Self`, so both discarded and
chained calls retain Go's mutation semantics. The two long-lived bundle callers
were changed to use one mutable binding; the short-lived fluent call remains
valid. `#[must_use]` was removed from `RuleBuilder::new` and `new_rule`, whose
Go counterparts are also discardable. Rule construction, serialization, and
constraint semantics are otherwise unchanged.

`rule::tests::go_builder_mutators_apply_when_returns_are_ignored` first invokes
the discardable constructors and then ignores every setter return while
asserting that learner role and replica count were applied. On the pre-fix
owner, compilation failed with exactly four `E0382` moved-value diagnostics;
the focused test now passes.

## Ready validation

Rust-only validation was requested; no Go execution was performed. No Go,
Bazel, Cargo dependency, or module file changed, so `make bazel_prepare` was
not required.

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-placement --offline --locked go_builder_mutators_apply_when_returns_are_ignored -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-placement --offline --locked -- --test-threads=1
PASS; 25 unit tests and 4 metadata tests passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-placement --all-targets --offline --locked
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

The Rust crate emits only pre-existing warnings in dependencies and unrelated
workspace modules. Go-side execution remains intentionally unverified for
this Rust-only follow-up.

## Corrective `GroupID` return contract (2026-09-06)

Commit `8d42bcc7035` restored the Rust-only `#[must_use]` annotation on
`common::group_id` and deleted the regression and receipt evidence from
`9f2fbd93e02`. Under the requested Rust-only scope, the Go inventory above was
not re-read. The current Rust owner was read in full before this correction:
exactly 11 tracked artifacts and 5,066 lines after the focused test addition,
comprising the manifest, eight production modules, all inline tests, and the
`meta_bundle_test` integration target. There is no generated input/output,
platform variant, fixture, example, benchmark, build script, or crate-local
lockfile.

Go's `GroupID` string may be discarded, while the restored Rust annotation
made that source-shaped use a compile diagnostic. Removing the one annotation
does not change the `TiDB_DDL_<id>` encoding or any bundle construction,
key-range, policy, YAML, PD DTO, or error behavior.

`common::tests::go_group_id_return_may_be_ignored_like_go` discards the result
under `#[deny(unused_must_use)]`. Against the restored source, the focused
compile failed with exactly one diagnostic, captured in
`/tmp/tidb-ddl-placement-group-id-restored-prefix.log`; after the correction,
it passes. All 30 owner tests, all-target compilation, standalone rustfmt,
Ready `make lint`, and diff hygiene pass. Warnings emitted by dependency and
unrelated workspace modules remain visible and are outside this one-line
contract correction.

No Go, Bazel, Cargo manifest/dependency, module, or import graph changed, so
the Bazel prepare gate does not require `make bazel_prepare`.
