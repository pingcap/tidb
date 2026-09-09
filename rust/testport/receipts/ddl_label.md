# `pkg/ddl/label` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`; re-pinned against
the repository Go tree tip 2026-09-06. The only drift since the original pin
is `rule_test.go`'s keyspace-codec construction picking up the protobuf
`KeyspaceMeta.Keyspace` wrapper — test-harness mechanics with no behavior
contract change; production `rule.go`/`attributes.go`/`errors.go` blobs are
unchanged.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 45 | `91f6bf7c1ee323e55641eced0d020b10c8fe1670` | `tidb-executor` production module and unit-test target; `tidb-exec` delivery/caller checks |
| `attributes.go` | 145 | `efd814747de128bb4f4e2e301ff833f2154ada3d` | `tidb-executor::ddl_label` label parsing, compatibility, add, and restore behavior |
| `attributes_test.go` | 248 | `059dff9462a8461e1c8633b45b3c825ddbba125d` | `ddl_label::tests` source-shaped parsing, deduplication, conflict, and kernel-specific restore cases |
| `errors.go` | 22 | `430a767b9a43c512822b664a1f41ba1fd5d202f9` | `LabelError::InvalidAttributesFormat` and its source message |
| `main_test.go` | 34 | `aa8bef08d69a68cc2f849c59d747445ef1734a7a` | Go goleak/test-suite harness only; Rust has no package-owned worker or harness behavior to reproduce |
| `rule.go` | 220 | `10904faa06d1059a600863579e9a5d80af299d6b` | `Rule`, `RegionLabel`, codec boundary, rule IDs, reset, JSON string/wire shape, clone, and patch construction |
| `rule_test.go` | 157 | `1a204cbd412ad9307cd936b9dfe04aa2a90057f6` | `ddl_label::tests`, plus the `tidb-exec` exchange-patch and PD HTTP delivery regressions |

There is no package doc, generated source/input, fixture, benchmark, fuzz target,
platform file, or build-tagged source in the pinned directory. Kernel branching
is inside ordinary source and is validated in both Rust kernel configurations.

## Behavior and integration decision

`rust/crates/tidb-executor/src/ddl_label.rs` is the package carrier. It wraps
the attribute text in `[...]` and decodes a YAML string sequence, matching
Go's `yaml.UnmarshalStrict` boundary instead of retaining the former manual
comma parser. `Rule` preserves PD's complete label expiry fields and arbitrary
JSON `Data`. Go-compatible shared carriers preserve `nil` versus allocated
empty labels and the shallow slice/interface aliases created by `Clone`.

`LabelCodec` is the named Rust boundary for the three `tikv.Codec` operations
the package consumes. `CodecV1` covers the classic/nil-metadata result, and the
real `tikv_client::request::ApiV2Codec` implementation covers NextGen rule IDs,
the reserved keyspace label, and `EncodeRegionRange`. The existing
`tidb-exec` exchange-partition planner consumes this package through that
boundary and its four-way rule patch is checked separately. Selecting the
running store's codec at every DDL caller belongs to those caller packages; it
does not create a second label implementation.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo check --locked -q -p tidb-executor -p tidb-exec
cargo test --locked -q -p tidb-executor --lib ddl_label::tests
cargo test --locked -q -p tidb-executor --lib --features tidb-config/nextgen ddl_label::tests
cargo test --locked -q -p tidb-exec --lib label_delivery::tests
cargo test --locked -q -p tidb-exec --test all exchange_partition_builds_gos_four_way_label_rule_patch
```

All scoped checks pass. The HTTP delivery test needs loopback bind permission
for its in-process mock PD server; it performs no external network access.
