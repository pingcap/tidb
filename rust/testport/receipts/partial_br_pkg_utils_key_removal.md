# Partial `br/pkg/utils/key.go` port removal

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Ownership audit

Pinned Go `br/pkg/utils` has 35 direct package artifacts plus three separately
owned subpackages. The removed `rust/crates/tidb-util/src/br_key_utils.rs`
represented only `key.go` and `key_test.go`, not the complete Go package.
Both pinned key files, the complete Rust file, and the complete direct package
inventory were read before deletion.

Repository-wide search found that only `KeyRange` and `CompareBytesExt` were
used, both exclusively by the complete `br/pkg/streamhelper/spans` owner. They
are dependency boundaries of that Go package: `KeyRange` comes from `pkg/kv`,
and the four-branch comparator comes from `br/pkg/utils`. Their native,
package-private representations now live with the spans owner. No consumer
used ParseKey, range intersection, date formatting, metadata predicates,
metadata encoding, or any other behavior from the partial module.

The audit deleted the 804-line partial implementation, its five translated Go
tests, three supplemental Rust tests, and its public `tidb-util` export. This
is not a claim that `br/pkg/utils` is transcreated; that package still requires
one complete atomic inventory and implementation decision.

## Validation

Profile: WIP; this is a parity cleanup within the continuing package audit,
not a repository-wide readiness claim.

- `cargo check -p tidb-util -p tidb-br --locked` — passed.
- `cargo test -q -p tidb-br --locked -- --test-threads=1` — passed: 31 tests
  passed and 2 were ignored.
- `cargo test -q -p tidb-util --locked -- --test-threads=1` — passed: 597 unit
  tests passed, 3 were ignored, and every integration and doc test passed.
- `cargo fmt --all` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: all consumers were moved to equivalent dependency boundaries
  and both affected crates pass their complete serial suites.
- Compatibility: the repository-unused public partial APIs are intentionally
  removed.
- Performance: the spans comparator remains allocation-free; all other
  deleted paths had no consumer.
