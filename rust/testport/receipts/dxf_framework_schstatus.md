# `pkg/dxf/framework/schstatus` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 20 | `8e3f2810a5b9ae87a67b603231b2bf309e85b0f7` | `tidb-dxf` package module and one source test identity; the Go test's flaky/two-shard metadata has no native Cargo analogue |
| `status.go` | 121 | `0306e7a19df723b85d776b29196409c8573c9e2e` | `schstatus.rs`: version, queue, node groups, flags, TTL values, exact JSON fields, and bounded status string |
| `status_test.go` | 36 | `b0e7a0c6b35048ece03b12c78ac0a30ab0942128` | `status_print`, preserving all assertions from `TestStatusPrint` |
| `tune.go` | 52 | `5489fce97f555ebdd8d11be94b9b8c8182f776c5` | `schstatus.rs`: amplification bounds/default, embedded TTL tuning values, and JSON string form |

There is no package doc, fixture, benchmark, generated source, platform
variant, or other artifact in the pinned directory.

## Native integration decision

The Go package remains a distinct public `schstatus` module rather than a set
of prefixed or crate-root aliases. Go `int` fields use target-sized `isize`,
`time.Duration` uses signed nanoseconds, and `time.Time` uses
`DateTime<FixedOffset>` with year-1 zero, offset retention, RFC3339Nano
fractions, and Go's JSON year validation. JSON rejects non-finite tuning
factors as Go does.

`Status.String` preserves the source's shallow-copy slice behavior: truncating
the printed TiDB worker list to five entries plus the marker overwrites element
five in the original backing list while retaining its original length. The
sole Go test checks that retained length and the six decoded output entries.

`pkg/meta` now consumes `schstatus::TtlTuneFactors`, as Go does, instead of
owning a flattened UTC-only duplicate. Its next-gen-only source test is gated
by Rust's existing `nextgen` feature; the storage-format test remains in its
own source package.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-dxf
cargo test --quiet --offline -p tidb-meta --features nextgen --test all set_get_dxf_schedule_tune_factors_round_trips
cargo test --quiet --offline -p tidb-meta --test all dxf_and_ru_stats_match_go_json_shapes_including_null
cargo check --quiet --offline -p tidb-dxf -p tidb-meta
git diff --check
```

All commands passed. The DXF crate ran exactly 11 tests: the ten previously
completed proto identities and the one schstatus identity. Non-native targets,
workspace-wide tests, the Go test target's race/shard configuration, and the
Ready-profile `make lint` were not run during this WIP iteration.
