# `pkg/planner/cascades/util` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 61 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 9 | public Go library target and `intest` dependency |
| `string_writer.go` | 52 | buffered string-writer interface and adapter |

The production declaration inventory was checked function by function:
`StrBufferWriter.WriteString`, `StrBufferWriter.Flush`,
`NewStrBuffer`, `StrBuffer.WriteString`, and `StrBuffer.Flush`. The package has
no Go test file, `doc.go`, `TestMain`, benchmark, fuzz target, example,
fixture/testdata tree, generated source, platform/build-tag variant, nested
package, or other support artifact. The BUILD file lists exactly the one
production source file above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner/src/string_writer.rs`.
Its `StrBufferWriter` trait, buffered adapter, source constructor, and direct
binder diagnostic consumer were re-read. `new_memory_buffer` and the
`StrBuffer::new` / `into_inner` helpers are Rust-native conveniences and have
no Go source counterpart; their ownership-oriented `#[must_use]` annotations
remain. Go's `NewStrBuffer` returns an interface and permits callers to ignore
it, so the source-shaped `new_str_buffer` annotation was removed. Write/flush
fail-fast behavior and the hidden byte count/error contract are unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards the direct
Go-shaped constructor result. Before the edit it failed with exactly one
diagnostic; after the edit it passes.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib string_writer::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly one `unused return value` diagnostic.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib string_writer::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/string_writer.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. Full workspace
tests, cross-platform writer builds, and Bazel execution were not run.

## Risks and boundaries

- Correctness: the fail-before/pass-after constructor regression and planner
  library check pass; buffered write and flush behavior is unchanged.
- Compatibility: only Rust lint policy changed for the one Go-shaped
  constructor; Rust-native convenience constructors remain strict.
- Performance: no buffering, allocation, or I/O path changed.
