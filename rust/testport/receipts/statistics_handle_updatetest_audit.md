# `pkg/statistics/handle/updatetest` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 32 | `b12d429d050b92ba9c618b4ea9b03a17f07bce2a` | build metadata inventoried |
| `main_test.go` | 34 | `c10ec74b44ad1874020b9ade88c32f1692ed2f0f` | harness inventoried; not ported |
| `update_test.go` | 1,301 | `2e8c5cebf3b177896ac7b9b68fd9759ddd0fa07f` | 23 tests and one benchmark inventoried; not ported |

The package has no production, generated, platform-specific, fixture, or
other support artifacts.

## Package behavior and blockers

This is one integrated validation package for the ordinary statistics handle.
It checks committed, rolled-back, failed, and cross-session delta publication;
partition and out-of-order metadata updates; automatic table, partition, and
index analysis; histogram reload and range splitting; TopN merge; live stats
session variables; health-ratio policy; column-usage collection with and
without plan cache; stats locks; missing metadata repair; system-table
exclusion; and leak-free test lifecycle. Nearly every assertion drives a mock
store and domain through SQL sessions and then reads the live stats cache or
`mysql.*` storage.

Rust lacks the complete ordinary statistics handle/session/storage integration
needed to execute this package as a whole. `TestSplitRange` additionally needs
the statistics histogram and planner ranger/context seam; `TestMergeTopN` is
pure, but its production behavior is already covered in the owning Rust
CMSketch/TopN tests. Those isolated cases cannot substitute for the remaining
package or make an atomic package claim.

## Removed non-parity carriers

`statistics_part6_source.rs` contained 15 ignored functions with no executable
assertions. They represented only the first manifest batch of this 23-test
package, omitted the remaining eight tests and benchmark, and could never
detect behavioral drift. All 15 empty functions were removed. The package
remains explicitly unclaimed.

## Validation

WIP profile: removal of disconnected test carriers is checked through the
affected statistics owner gate.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/tests/statistics_part6_source.rs`
- `git diff --check`
