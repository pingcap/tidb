# `pkg/util/plancodec` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly seven artifacts, all read in full. The local Go
package is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 42 | `ad0d18209a2fa2a949e83ce0fa683dc78060dd07` | `tidb-util::plancodec`; Cargo owns native dependency and test-process metadata |
| `binary_plan_decode.go` | 372 | `b93dd2fa60d093d4d67e12c2255ace885375ac1e` | protobuf binary decode, display, connection formats, operator recursion, labels, and access objects |
| `codec.go` | 449 | `78eb7b4b0e2f095a3cc52ff558e9baf292047556` | textual encode/decode, tree construction, task/store encoding, Snappy, and base64 |
| `codec_test.go` | 57 | `d9ea8ea69918a1a9c71df20853c3aff0d98b125f` | `encode_task_type_matches_go` and `decode_discard_plan` preserve the two source identities |
| `id.go` | 485 | `0549c4d18f2399c6a87631d2377107cf41a14d48` | all 63 stable IDs, type spellings, reverse mapping, and `Sequence` |
| `id_test.go` | 97 | `fe54dfcfff36e4e1538a007d5ad7ad2cc418bdc9` | `plan_id_changed` and `reverse` preserve the two source identities and exact source ranges |
| `main_test.go` | 33 | `22f4f0cb7cd8ef4cb00d7761ed1de8aa49806261` | Go test setup and goroutine-leak harness only; Cargo owns the native test process |

There is no package doc, fixture, benchmark, generated source, platform
variant, README, or ownership artifact. The Go test target is short and
flaky; those scheduler attributes have no Cargo behavior to port.

## Rust ownership and parity result

`rust/crates/tidb-util/src/plancodec.rs` owns the whole package. Go `int`
values are target-sized `isize`, including physical IDs, parsed IDs, and plan
depths. Indentation arithmetic wraps at machine width before slice allocation
or indexing, preserving Go's malformed-tree panic boundary. `DecodePlan`
alone catches such panics and returns `DecodePlan panicked`;
`DecodeNormalizedPlan` leaves them observable, as its Go counterpart does.
Textual fields remain byte-preserving because Go strings need not be UTF-8.

The former Rust-only `PlanStoreType` enum was removed. Task encoding now uses
the source `uint8` domain, while decoding parses a target-sized integer and
casts it to `u8` before naming the store, including Go's wrapping behavior.
Textual integer failures retain Go's `strconv.Atoi` category and surrounding
plan/depth/ID/task context.

Binary decoding uses the ordinary protobuf plan for both display functions.
The main operator is mandatory and therefore panics when absent, exactly like
Go's unconditional `decodeBinaryOperator(pb.Main, ...)`; the prior Rust empty
or CTE-only fallback is gone. Main/CTE/subquery selection, build/probe child
order, runtime columns, connection formats, signed `ActRows`, access objects,
driver labels, discarded plans, compression, and protobuf errors retain the
source behavior.

The audit also removed the public decoded-sentinel and result aliases, five
Rust-only `must_use` diagnostics, and nine source-absent tests. Exactly the
four functional Go tests remain. The two expression test consumers compare
the private decoded spelling as a value rather than depending on a public API
that Go does not expose.

## WIP validation

Profile: WIP. This completes one atomic package in an ongoing repository-wide
parity audit; it is not a Ready or repository-completion claim.

Passed from `rust/` unless noted:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/util/plancodec
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/plancodec
cargo fmt --all
cargo fmt --all -- --check
cargo test --quiet --offline -p tidb-util plancodec --lib -- --test-threads=1
cargo check --quiet --offline -p tidb-util
cargo check --quiet --offline -p tidb-expr
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
cargo test --quiet --offline -p tidb-expr --lib decode_plan_builtins_preserve_their_distinct_error_policies -- --test-threads=1
cargo test --quiet --offline -p tidb-expr --lib go_captured_edge_case_values -- --test-threads=1
git diff --check
```

The source package has no `failpoint.`, `testfailpoint.`, or Bazel failpoint
dependency match. Its targeted Go baseline command was attempted without
failpoint enablement:

```text
go test -run '^(TestPlanIDChanged|TestReverse|TestEncodeTaskType|TestDecodeDiscardPlan)$' -tags=intest,deadlock ./pkg/util/plancodec -count=1
```

The host's unsupported Go 1.27 toolchain could not build repository
prerequisites: `pkg/util/hack` has source variants only through Go 1.26, and
the cached gRPC transport dependency does not compile against the selected
HTTP/2 module. This failure occurs before `plancodec`; the unchanged pinned Go
sources and exact Rust source identities remain the WIP comparison evidence.

No Go, Bazel, module, or generated artifact changed, so `make bazel_prepare`
is not required. Cross-platform execution, workspace-wide tests, and the
Ready-profile `make lint` were not run in this WIP iteration.

## Risk

- Correctness: all seven artifacts and every production branch are mapped;
  the four source test identities and both ordinary expression consumers pass.
- Compatibility: public numeric inputs now use Go-width domains; the concrete
  Rust error type remains the native representation of Go's `error` return.
- Performance: tree and binary algorithms retain their source shapes; Go's
  garbage-collected decoder pool is not an observable Rust API contract.
