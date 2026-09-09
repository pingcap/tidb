# `pkg/planner/core/access` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 219 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 9 | public Go library target and tipb dependency |
| `access_obj.go` | 210 | scan, index, other, and dynamic partition access objects |

The declaration inventory was checked function by function:
`ScanAccessObject.NormalizedString`, `String`, and `SetIntoPB`;
`IndexAccess.ToPB`; `OtherAccessObject.String`, `NormalizedString`, and
`SetIntoPB`; `DynamicPartitionAccessObject.String`; and
`DynamicPartitionAccessObjects.String`, `NormalizedString`, and `SetIntoPB`.
The package has no Go test file, `doc.go`, `TestMain`, benchmark, fuzz target,
example, fixture/testdata tree, generated source, platform/build-tag variant,
nested package, or other support artifact. The BUILD file lists exactly the
one production source file above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner/src/access.rs`. Its
scan/index/other/dynamic object data, display and normalized rendering,
protobuf conversion, inline owner tests, explain consumer, and direct physical
plan consumers were re-read. Go methods return values that callers may discard;
Rust had imposed `#[must_use]` on five source-shaped results:
`ScanAccessObject::normalized_string`, `IndexAccess::to_pb`,
`OtherAccessObject::normalized_string`,
`DynamicPartitionAccessObjects::normalized_string`, and
`AccessObject::normalized_string`. Those annotations were removed. Display
formatting, normalized partition masking, nil/empty protobuf behavior,
dynamic error slots, and protobuf field mapping remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards all five
source-shaped results. In a temporary clean worktree at the pre-fix revision
it failed with exactly five `unused return value` diagnostics; after the edit
it passes 1/1.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib access::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly five diagnostics (captured in `/tmp/tidb-codex/access-prefix.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib access::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/access.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. Full workspace
tests, Bazel execution, and cross-platform planner builds were not run.

## Risks and boundaries

- Correctness: the fail-before/pass-after regression and planner library check
  pass; access-object formatting and protobuf conversion are unchanged.
- Compatibility: only Rust lint policy changed for source-shaped methods whose
  Go callers may discard results.
- Performance: no display, allocation, or protobuf path changed.
- Not verified locally: full workspace tests, Bazel execution, and
  cross-platform planner builds.
