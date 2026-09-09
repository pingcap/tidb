# `pkg/bindinfo` — Go-master parity boundary receipt

## Follow-up batch — value-form binding admission

Comparison source: Go `origin/master` at
`1c1a334d2be1dce64888b6e1f054462c566b0734`.
The complete inventory below remains the package boundary: 25 artifacts,
7,917 lines, all production/test/support files, JSON fixtures, ownership
metadata, and Bazel targets were read before this edit. There are no generated
production files or platform variants.

Go master now gates `MatchSQLBindingWithCache` through `mayHaveSQLBinding`.
INSERT/REPLACE `VALUES` and `SET` forms are not binding-capable (bindings are
for the `... SELECT` shape), while EXPLAIN delegates the decision to its
wrapped statement. The Go helper and parser matrix regression were restored.
The Rust `tidb-session` matcher applies the same recursive filter to
`DmlStmt::With` and `InsertStmt::source`, so stored bindings remain available
to administrative operations but value-form DML and EXPLAIN do not publish a
false `last_plan_from_binding` hit. The Rust regression covers INSERT,
REPLACE, and EXPLAIN value forms and failed before the filter (`1` instead of
`0`).

This is a bounded behavior fix, not a package-complete transcreation claim.
Automatic-binding persistence, manager/session integration, and the remaining
Go plan-generation AST visitor migration stay at the explicit boundaries
described below.

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package owns 25 tracked artifacts and 7,917 lines in the comparison
revision. The inventory includes production sources, all package and nested
integration tests, JSON fixtures, ownership metadata, and both Bazel targets.
There is no generated Go source, benchmark, fuzz target, or platform-specific
variant. Every artifact below was read before editing. The working branch has
concurrent changes in seven Go files; the receipt deliberately hashes the
authoritative fetched `origin/master` tree rather than overwriting those
changes.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `pkg/bindinfo/BUILD.bazel` | 92 | `805fd5495f4ee3195bd74ffd4545101b5515a2a2` | `7fa0bdae96c802d51c10a019b17a9e2b61ad88f33bb88e7ae8ef9485f014b70b` | package library and sharded unit target |
| `pkg/bindinfo/OWNERS` | 12 | `42ea46e7aeac44773fc6d7a774a76dc65e14d1a6` | `c5d8571c464a6db4df82bdf1c9137a83dce6b6ed47fb16c002d13f45a8374f95` | ownership metadata |
| `pkg/bindinfo/binding.go` | 621 | `0ac7bc83fcbf540172923f819002cd586b677cf2` | `98502b5a1d0064d33bc56586724bf20352934da79daf2bab9750b418f179f00c` | binding model, normalization, matching, and status |
| `pkg/bindinfo/binding_auto.go` | 322 | `f1da3257725c2213af510e3bc8e5781d78c4746f` | `e9c8eb341e484a4db750116218b121240dc3f7e923090d34d333d55b4565f8b1` | automatic binding generation |
| `pkg/bindinfo/binding_auto_test.go` | 350 | `4fc616c268df9a1e0768b13e4f28c4145dece9a2` | `213e5cfb41964a7db4742cf44b3682c23cd7f8dfd292345d2f296eca9cd69d8f` | automatic binding tests |
| `pkg/bindinfo/binding_cache.go` | 435 | `a25aa115a536270c324f481ebf985f66d935c11c` | `8493fbeb4971e97fac2144914f285eaf04330d7bc42993cb80eabccd754df90a` | session/global binding cache |
| `pkg/bindinfo/binding_cache_test.go` | 295 | `12fff153b2c0bc6f91328254adbf44967142c78e` | `374dcb0d6779e8272d6cbb75f5b4c54d5fe453f3d20250783319e0c6fa71489a` | cache reload, eviction, and matching tests |
| `pkg/bindinfo/binding_handle.go` | 90 | `c7c03a66559190a8371420708defc31e21a94a5b` | `f8f5d88dda34d77a849193945c82a09a2802ae545d42054303c3131d888a6550` | session binding handle |
| `pkg/bindinfo/binding_operator.go` | 271 | `68aa13b0be04cec14eaf44d2b65c102afbe393ae` | `411099be78a9b7853d011c58983f9cd1849f992a79dba13463e6c4b62b9b1658` | bind/unbind/drop operators |
| `pkg/bindinfo/binding_operator_test.go` | 1364 | `c6e215284f36d059b2129caec7a912af07ba5c41` | `c86879ee0e13586c985d2f45a11f3780b140a949fbb0d45015d67bbb58f6a314` | operator and privilege/error tests |
| `pkg/bindinfo/binding_plan_evolution.go` | 109 | `6fd8f8c94c28e72cbedbd2a380c6429f8d04b8a4` | `3144d9c73b784427e1cbc7b0b4d8a183231edb1e05404f2e2b2bdf45426c544c` | plan-evolution metadata |
| `pkg/bindinfo/binding_plan_evolution_test.go` | 85 | `4ac2b232f0893727c785d210f69bbb25cbdea70f` | `3364ce9c9ef75b4c853ee3aee082c1d62c0fe495cc59dadc6c683a5ee66b0222` | plan-evolution tests |
| `pkg/bindinfo/binding_plan_generation.go` | 988 | `f5f15198145df4e459da833475dc6a560b72d5ad` | `746c42ccde174d29ab8db03a3888aa3e4701c5b71aaf3291b2911e19bb9fe674` | bound-plan generation and validation |
| `pkg/bindinfo/binding_plan_generation_test.go` | 115 | `283c4cc54f19062340ffe7f8d4a00d00db6e0d9a` | `70b3885008ab8f0dbceb5dee3efe9db8a66562e313872b84ff01b38ef4cceeab` | plan-generation tests |
| `pkg/bindinfo/main_test.go` | 52 | `c9df75b01cf9273331e661752dbb7af25761edb1` | `71d138ec89bc13be7c2b36756b89fd2fae4e3e876de7a68cb79e88325e0de3c6` | package test setup and leak harness |
| `pkg/bindinfo/session_handle.go` | 217 | `2379aa00fdf69e9030ad36dddda267c9a6c5334c` | `7b18775c438b32b6ffed4c93abe49dba05731f940d00551ed2f1804f11fd8012` | session/global binding lifecycle |
| `pkg/bindinfo/session_handle_test.go` | 371 | `0c6ecb1ade99a5a7385f5b4889cdef712ca33236` | `c1df8ebba7eeae65b54e58c22b32e0b7fa1f8b028d8ade46d9c4dc30254d00f4` | lifecycle and reload tests |
| `pkg/bindinfo/testdata/binding_auto_suite_in.json` | 30 | `7a527e127280b92baae8b81c5f80120a04030d17` | `78de5e4fa243f7ca344bc3d43d03807670d9752542491aa82b9cc4d363b9dbb0` | automatic binding input fixture |
| `pkg/bindinfo/testdata/binding_auto_suite_out.json` | 225 | `de37e7834bb032e953946dd01e3a5aacb461bbed` | `becac2407e8b1138103694d6ede98807c8334430e4bbee2e27696db50d1a3a4c` | automatic binding expected output |
| `pkg/bindinfo/tests/BUILD.bazel` | 32 | `a1a162f48d39b88a8bbfe0d399807d35b35194bb` | `c92a7844184ed3a27e52635795cc9379f99deda2779bcf6698dcc24c0319b11e` | integration-test target |
| `pkg/bindinfo/tests/bind_test.go` | 946 | `cbdcca24e2081a9d68608da4bc829a0df25a5c1e` | `d676bc9be23a63a8521383ac9d284b2ef0fb806def088a0bb08ab02637ac920c` | SQL bind behavior and plan-cache integration |
| `pkg/bindinfo/tests/bind_usage_info_test.go` | 137 | `03f77609c88eb453b8921e39cd8b23a5b33caae6` | `8bd03abec935e3da748992ed725a171643d5aacadbdad660eab73190c03580bd` | usage-information integration tests |
| `pkg/bindinfo/tests/cross_db_binding_test.go` | 375 | `11916467913efe2e8f6187ad91a37ae931df6874` | `2e711b977c879f1cc8613b241f32d74671333b9c4706b5787289613e51deb0b2` | cross-database binding integration tests |
| `pkg/bindinfo/tests/main_test.go` | 36 | `b75858ec2c335242e7277e473fc3f37ad1d73822` | `dc71f5363b75018225c9d12cf652f11aaad7242337ed2a5a9fbbb6f3bc702763` | integration test setup |
| `pkg/bindinfo/utils.go` | 347 | `4ae780121c093fe6f5372a24d7d7ddcd58f25c49` | `1e7fd62563f3a01dae74377e14cf9c007bfaac49a5ce0a5b589d78bf58c33e68` | SQL normalization and utility helpers |

## Rust ownership and measured fix

The dependency-closed owner is `tidb-session`'s binding registry/cache,
normalization and prepared-statement execution path, with plan generation in
the session/planner crates. It covers matching, binding selection, cache-key
construction, and publication of the `PrevFoundInBinding` session variable.
The Go package's automatic binding persistence, manager/session wiring, and
SQL integration harness remain outside this owner; they are recorded as
explicit boundaries rather than represented by uncalled Rust APIs.

The audit found a real execution-lifecycle mismatch. Rust correctly selected a
binding for `EXECUTE`, but the nested execution boundary consumed
`found_in_binding` before the outer prepared statement published it. As a
result, `PrevFoundInBinding` was `0` even though the bound plan ran (and the
prepared-plan-cache key could observe the wrong state). The execution path now
remembers whether binding matched and re-arms the current-statement flag after
cached-select, cached-DML, and fallback execution, including error returns.
The focused regression `a_prepared_binding_is_published_when_the_plan_cache_is_disabled`
failed before the change (`["0", "0"]`) and passes after it
(`["0", "1"]`).

## Follow-up validation

- Before the implementation, the Rust selector
  `tests_binding::insert_values_bindings_do_not_match` failed with
  `last_plan_from_binding = 1`; after the filter it passes.
- The Go failpoint-aware selector `TestMayHaveSQLBinding` failed to compile
  before restoring the helper and passes afterward. The full failpoint-aware
  package run was attempted but was killed with exit 137 after emitting
  excessive existing integration logs; no package assertion failure was
  reported.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib insert_values_bindings_do_not_match -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed for the Rust batch.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed as the Ready gate.
- `make bazel_prepare` is required because Go production/test files changed;
  it remains blocked locally because no `bazel` executable is installed.

## Validation and risk

Profile: **Ready** for this code batch. No Go/Bazel/import/go.mod source was
changed, so `make bazel_prepare` is not required.

- The two focused prepared-plan-cache regressions pass.
- The `tidb-session` binding owner selector passes: 49 passed, 0 failed,
  1 ignored.
- The Go unit package passes with `go test ./pkg/bindinfo -count=1`; the
  nested integration package compiles with `go test ./pkg/bindinfo/tests
  -run '^$' -count=1`.
- The complete prepared-plan-cache selector has one unrelated existing
  parallel HashAgg panic (`expression_and_aggregate_parameters_rebuild_on_cache_hits`);
  the remaining 24 tests pass and the focused regression is stable across
  reruns.
- `cargo fmt --all -- --check`, workspace `cargo check --offline --locked`,
  and `make lint` all pass in the Ready run (with pre-existing compiler
  warnings).

Risks are limited to statement-lifecycle bookkeeping: the flag is re-armed
only when a binding was selected, preserving ordinary and error behavior while
ensuring the outer `EXECUTE` boundary reports the match. Automatic-binding
storage and the Go integration suites remain unverified Rust boundaries.
