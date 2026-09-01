# `pkg/dxf/framework/proto` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 31 | `54dd9ff5cde0717cb218a977985547da9e89d8a5` | standalone `tidb-dxf` crate and one package test target containing exactly ten source test identities |
| `modify.go` | 61 | `87d79ad57075b4d82af727212ab3d504fa195739` | `modify.rs`: modification types, JSON fields, and source string forms |
| `node.go` | 94 | `92f4723d42b637797940a893abb3ca78c028c755` | `node.rs`: target-sized CPU fields, shared test resource, percentage limits, and slot-proportional memory/disk resources |
| `step.go` | 208 | `cad663754c76fd2cccee6c583df3a12f0bbaf7d8` | `step.rs`: framework and business steps, exact numeric values, validation, and strings |
| `step_test.go` | 70 | `52fce21d39635fbadbf605f0d444da70b23498fa` | `test_step` and `test_is_valid_step` |
| `subtask.go` | 177 | `0cd2116a1ffe3a03f6edb18a059c31ecf421aa9b` | `subtask.rs`: records, year-1 zero times, states, wrapping atomic allocation, and step resources |
| `subtask_test.go` | 73 | `987522ec4c13ae062e59c6bbba5bbd2cdffa1a81` | `test_subtask_is_done` and the exact 10 × 10,000 concurrent allocation workload |
| `task.go` | 267 | `982ed617f38cf707e678d020c035622b6a8b49be` | `task.rs`: records, states, prepare mode, target-sized concurrency, callable restore, offset-preserving times, shared errors, rank, runtime slots, and source strings |
| `task_test.go` | 149 | `c99faa0352d436c1c945bfa0facd690098f282f2` | five source tests for step/JSON, terminal states, concurrency bounds, rank, and runtime resource limits |
| `type.go` | 52 | `c8137da4db34d1c829f16bb6ba5d47b36d80fe24` | `task_type.rs`: the three task types and integer encoding |
| `type_test.go` | 40 | `5354e10e0d54f5b8826fa8800ead67484b6964ac` | `test_task_type` |

There is no package doc, fixture, benchmark, generated source, platform
variant, or other artifact in the pinned directory.

## Native integration decision

Rust target-sized `isize` represents Go `int`; persisted `Step` and atomic
resource counters remain `i64`. `DateTime<FixedOffset>` preserves Go's year-1
zero time, instant ordering, RFC3339Nano offset, and nanosecond formatting.
Task errors use shared trait objects rather than flattened strings, preserving
copy identity and error behavior for later scheduler consumers.

The package's sole external implementation dependency is pinned
`docker/go-units v0.5.0`. The private native byte formatter follows its
`getSizeAndUnit` loop and `%.4g%s` output; the decimal `GB` constant remains
private just as it belongs to that dependency, not the proto API.

Removed six source-absent Rust tests, the exported dependency helpers,
UTC/optional zero-time narrowing, the RAII-only restore type, checked-overflow
allocation, flattened error strings, and crate documentation that described
those divergences as completed behavior.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --quiet --offline -p tidb-dxf
cargo check --quiet --offline -p tidb-dxf
git diff --check
```

All commands passed; the package target ran exactly 10 tests. The Go Bazel
test's flaky sharding/race configuration, non-native targets, workspace-wide
tests, and Ready-profile `make lint` were not run during this WIP iteration.
