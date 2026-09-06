# `pkg/ddl/notifier` parity receipt

Comparison source: Go `origin/master` at commit
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-04).

## Complete inventory

The package contains exactly eight tracked artifacts and 1,999 lines: four
production files, three test files, and one Bazel build file. All artifacts were
read in full before editing. There is no package `doc.go`, fixture directory,
`testdata`, generated source/input, platform variant, benchmark, fuzz target,
or `OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 59 | `ca17f30d6a6015d0049e45bac11dd0591bfd3d08` | `773af2aac4840aa83566e513fb1af9e9fd41c19c2ccd29e234c53b7b798f8393` | public notifier library and 12-shard flaky test target |
| `events.go` | 530 | `96686b2e3f655092066531f93ef2341af1c18022` | `944c77659e4135d1ada46e3e69ec6b0183c728284d6b3aac9a3e3ec91b65c518` | schema-change event model, constructors, getters, and JSON wire representation |
| `events_test.go` | 70 | `fd412723a3aee25e7326ba3c0653743e4815d05a` | `40162ec77db5b5be18720edaafb6f4e2cbabfbcd3fd97529b590a9733c7b4d28` | event string rendering regression |
| `publish.go` | 51 | `88289c0be1cfd688fdb7844da67dac43f4ce4145` | `ca73a038b719a0a5b267a93681f78b861407a84a5b5eb6588c8e4e6eec4027a0` | transactional schema-change publication |
| `store.go` | 235 | `047919a23222fe4d6ea360cef99dc90d5e5a323f` | `934524d841b9be3d3073e2a3e3fdf3657d815815b16a64f8dbcc0732983fac53` | SQL-backed persistence, pagination, decoding, and deletion |
| `store_test.go` | 80 | `3aee8879ebc70c13b0a9cd0e337c0e3d0cd51483` | `b62e7caa42530177649f60b4ccee246b0af36019df8d03d557be05b50c5fc8fb` | reused-row JSON decode regression |
| `subscribe.go` | 357 | `6f5e76ff75e4b832eaaed9b34238a7a33fe98c6f` | `80f899a4765d7f7c6d5f8750751bdf77001c4518e55a459077c9a0150c6dc992` | owner listener, handler delivery order, retries, and cleanup |
| `testkit_test.go` | 617 | `2758d23f898afaa50528eda5699e95f02518f2ef` | `454de08be8d5ac7836679ef288f08c7748cd21311044445f7848343f7ff79358` | end-to-end publication/subscription, pagination, ownership, and transaction tests |

The production inventory contains 71 declarations; the test inventory has 12
top-level tests. The current package now matches Go master byte-for-byte: the
only delta was the cleanup eventual-wait timeout in `TestDeliverOrderAndCleanup`,
changed from one second to five seconds to match the upstream reliability
contract.

## Native integration decision

The notifier is Go-native infrastructure coupled to TiDB SQL sessions,
transaction semantics, owner election, persistent JSON schema-change rows,
failpoints, and handler registration. The current Rust branch now contains a
dependency-closed event/subscriber owner in
`rust/crates/tidb-ddl-notifier/{event,subscribe}.rs`; the SQL-backed store and
owner wiring remain in the server integration. This receipt therefore records
both the historical Go-native boundary and the current Rust owner inventory,
without claiming that the surrounding DDL producer integration is complete.

## Current Rust owner inventory (2026-09-06)

All four tracked crate artifacts were read before this return-contract fix;
the owner totals 1,615 lines and 10 inline tests. There are no Rust fixtures,
generated sources or inputs, platform variants, benchmarks, fuzz targets, or
additional build artifacts beyond the workspace `Cargo.toml`/`Cargo.lock`
entries.

| artifact | lines | SHA-256 | role |
| --- | ---: | --- | --- |
| `Cargo.toml` | 18 | `f1b900219408245db7b1c189c3015f03b86a6dcf3643de1cbb7de382057d388f` | crate metadata and dependency boundary |
| `src/lib.rs` | 28 | `b82b0bcfb4e41572be2ec0b265d3973e046a94aa7023a08ea1b83650d9a6cc48` | public event/subscriber exports |
| `src/event.rs` | 866 | `c79a4bc2fa1aed9ee701fec9c7896af4bbdb49bc6fa7c4dc4132548dc9843792` | JSON event model, constructors, getters, and event regressions |
| `src/subscribe.rs` | 703 | `5d77c98a70846b3787b673ec80ee4b70807aa43250f4c4f40e5ab70b8265a141` | durable delivery interfaces, owner listener, and subscriber tests |

The Go-shaped event API exposes 30 discardable constructors/getters/type
queries, and `DdlNotifier::new` is the 31st. None is a Rust ownership or error
contract: Go callers may ignore each return. The pre-fix deny-on-discard
regression emitted exactly 31 `unused return value` diagnostics; removing
those Rust-only `#[must_use]` annotations leaves the explicit `Result`
contract on `publish_schema_change_to_store` unchanged.

Focused regression and package evidence:

```text
OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-ddl-notifier --lib go_ --offline --locked
# PASS; 4 tests

cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml \
  -p tidb-ddl-notifier --offline --locked --no-fail-fast
# PASS; 10 tests
```

The complete Ready gates for this Rust-only batch are recorded in the living
ExecPlan and include all-target compilation, Rust formatting, `make lint`, and
`git diff --check`; `make bazel_prepare` is not required because no Go,
import, Bazel, or module file changed.

## Validation and risk

Profile: **Ready** for this test reliability fix. The focused regression passed
with failpoints enabled and disabled by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/notifier \
  -run '^TestDeliverOrderAndCleanup$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/notifier 2.746s
```

The complete failpoint-aware package suite also passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/notifier -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/notifier 14.269s
```

The shared Ready gates (`make lint`, Rust formatting, and `git diff --check`)
also passed for this batch. No import section, new Go file, Bazel target, or
module dependency changed, so `make bazel_prepare` is not required.

## Outcome

The notifier's Go-master timeout contract is restored with focused regression
coverage. The current Rust owner now also has Go-compatible discardable return
contracts, with focused pre-fix diagnostics and post-fix tests recorded here;
the rolling package audit continues for remaining producer integration gaps.
