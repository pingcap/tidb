# `pkg/executor/join` — Go-master null build-key and anti-semi receipt

Status: complete direct-package inventory with focused current-master
regressions restored in both the Go fixture and the dependency-closed Rust
hash-join owner. The package's nested `joinversion`, `test/indexjoin`, and
`test/mergejoin` packages remain separate boundaries.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-04).

## Complete inventory

The direct package contains 47 tracked artifacts (45 Go files plus
`BUILD.bazel` and `OWNERS`) and 20,058 lines. The direct Go surface contains
711 function declarations; its tests contain 85 `Test*`, `Benchmark*`, or
`Example*` declarations. Every direct production file, test, benchmark,
harness, BUILD target, and ownership file was read before editing:

`anti_semi_join_probe.go`, `anti_semi_join_probe_test.go`,
`base_join_probe.go`, `base_semi_join.go`, `bench_test.go`, `concurrent_map.go`,
`concurrent_map_test.go`, `hash_join_base.go`, `hash_join_spill.go`,
`hash_join_spill_helper.go`, `hash_join_stats.go`, `hash_join_test_util.go`,
`hash_join_v1.go`, `hash_join_v2.go`, `hash_table_v1.go`,
`hash_table_v1_test.go`, `hash_table_v2.go`, `hash_table_v2_test.go`,
`index_lookup_hash_join.go`, `index_lookup_join.go`,
`index_lookup_merge_join.go`, `inner_join_probe.go`,
`inner_join_probe_test.go`, `inner_join_spill_test.go`, `join_row_table.go`,
`join_row_table_test.go`, `join_stats_test.go`, `join_table_meta.go`,
`join_table_meta_test.go`, `joiner.go`, `joiner_test.go`,
`left_outer_anti_semi_join_probe_test.go`, `left_outer_join_probe_test.go`,
`left_outer_semi_join_probe.go`, `left_outer_semi_join_probe_test.go`,
`merge_join.go`, `outer_join_probe.go`, `outer_join_spill_test.go`,
`right_outer_join_probe_test.go`, `row_table_builder.go`,
`row_table_builder_test.go`, `semi_join_probe.go`, `semi_join_probe_test.go`,
`tagged_ptr.go`, and `tagged_ptr_test.go`, together with `BUILD.bazel` and
`OWNERS`.

There is no `doc.go`, fixture/testdata tree, generated Go source, or
platform-specific variant in the direct package. The recursive inventory also
read `joinversion` (2 artifacts, 72 lines), `test/indexjoin` (3 artifacts, 466
lines), and `test/mergejoin` (2 artifacts, 373 lines); those nested packages
are not folded into this direct claim.

## Restored behavior

Go master adds `TestAntiSemiJoinTypeNullBuildKey` and the `exec` test helper
dependency. The test constructs a NULL build key and an empty BLOB probe key,
then asserts that the anti-semi join preserves the NULL row. Before the paired
`pkg/util/codec` fix, the test failed with an empty result because the NULL
key was serialized as an empty key and collided with the empty BLOB key. After
the codec pre-allocation pass marks TypeNull rows through `canSkip`, the test
passes. This package batch restores the source-shaped test and BUILD metadata
in one package commit; the codec production fix is kept in its own package
commit as requested.

The Rust `tidb-executor` owner has no dependency-closed Go `pkg/executor/join`
package boundary for the full testkit harness. The dependency-closed Rust
`tidb-exec` hash-join owner now supports the missing no-residual
left-build `AntiSemiJoinProbe` path: probe rows mark matching build addresses,
then the row-table scan emits every unmarked build row. The immutable Rust row
representation uses a sequential address set for the source's atomic used
flag; the existing v2 driver remains sequential and does not claim Go's
goroutine topology. A focused regression proves that a nullable build row is
not lost when the probe key is an empty BLOB.

The paired SQL fixture is `tests/integrationtest/t/executor/jointest/hash_join.test`
with its expected output in
`tests/integrationtest/r/executor/jointest/hash_join.result`. The fixture adds
the same optimized-hash-join `NOT IN` query for issue #70672 and is kept
minimal; it is part of this join behavior cluster but not counted in the
direct Go artifact inventory above.

## Validation

Profile: Ready for this package batch.

- Before the codec fix, `TestAntiSemiJoinTypeNullBuildKey` failed with
  `[]` instead of one row.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/executor/join -run '^TestAntiSemiJoinTypeNullBuildKey$' -count=1 -vet=off` — passed after the paired codec change.
- `git diff --check` — passed for the package and receipt edits.
- `cargo test --offline --locked -p tidb-exec --test all hash_join_v2_source` —
  passed (18 tests), including the left-build nullable anti-semi regression.
- `cargo test --offline --locked -p tidb-exec --test all test_join_table_meta_key_mode_source` — passed, including the `TypeNull` default key-property case.
- `make lint` — passed with the repository's configured Go toolchain.
- `cargo fmt --all -- --check` still reports pre-existing formatting drift in
  the query-cop-store-limiter files; the changed executor files are rustfmt
  clean and no unrelated formatting was rewritten.
- `make bazel_prepare` remains required by the restored BUILD/test import and
  is blocked locally because the `bazel` executable is unavailable.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./run-tests.sh -r executor/jointest/hash_join` was attempted from `tests/integrationtest`; recording was blocked while building the server by unrelated stale failpoint-generated `_curpkg_` references in `pkg/ddl/util`, `pkg/bindinfo`, and `pkg/expression/aggregation`. The generated markers were then disabled and verified absent; the committed result diff remains exactly the Go-master fixture delta.

## Risks and unverified surfaces

- Correctness risk is low for the focused path: NULL keys are retained for the
  left-build anti-semi row-table scan, while valid matching addresses are
  suppressed after probing.
- The complete join algorithm, spill paths, full outer joins, and nested test
  packages were inventoried but not re-run in this focused batch.
- Rust cross-crate join integration and distributed SQL behavior remain
  unverified locally.
