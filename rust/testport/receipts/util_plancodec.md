# Complete `pkg/util/plancodec` package receipt

Status: Ready on the host target. The package inventory and Go-master
compatibility fix are complete. The Rust owner is
dependency-closed for the package's textual and binary codec behavior. The
live planner walkers remain consumers of other packages and are not folded
into this leaf claim.

## Pinned inventory

Comparison source: Go `origin/master` at commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-01), with the Rust
implementation from `origin/hparser-integration`
`5a005978dda57fbb3373a303660ea0a5f7990b38`.

The complete Go package tree contains exactly these seven artifacts:

    pkg/util/plancodec/BUILD.bazel                         (42 lines)
    pkg/util/plancodec/binary_plan_decode.go               (372 lines)
    pkg/util/plancodec/codec.go                            (449 lines)
    pkg/util/plancodec/codec_test.go                        (57 lines)
    pkg/util/plancodec/id.go                               (492 lines)
    pkg/util/plancodec/id_test.go                            (98 lines)
    pkg/util/plancodec/main_test.go                          (33 lines)

`BUILD.bazel` has one library and one short, flaky test target. There is no
`doc.go`, generated source, platform/build-tag variant, benchmark, fixture
directory, or `testdata` under this package. `main_test.go` is only the
source test setup/goleak harness. The Rust build artifacts are the workspace
member registration in `rust/Cargo.toml`, the `pub mod plancodec` export in
`rust/crates/tidb-util/src/lib.rs`, and the single owner
`rust/crates/tidb-util/src/plancodec.rs` (1,364 lines after this fix).

## Function and test inventory

The three Go production files contain every function below; all are covered
by the Rust owner, either directly or through the source-shaped helper path:

* `codec.go`: `DecodePlan`, `DecodeNormalizedPlan`, `planDecoder.decode`,
  `buildPlanTree`, `addPlanHeader`, `initPlanTreeIndents`,
  `findParentIndex`, `fillIndent`, `alignFields`, `getMaxFieldLength`,
  `getPlanFieldLen`, `decodePlanInfo`, `EncodePlanNode`, `escapeString`,
  `NormalizePlanNode`, `encodeID`, `EncodeTaskType`,
  `EncodeTaskTypeForNormalize`, `decodeTaskType`, `Compress`, and
  `Decompress`.
* `id.go`: `TypeStringToPhysicalID` and `PhysicalIDToTypeString`, plus the
  complete stable type/name table (now IDs 1 through 64).
* `binary_plan_decode.go`: `DecodeBinaryPlan`,
  `DecodeBinaryPlan4Connection`, `calculateMaxFieldLens`,
  `decodeBinaryOperator`, `printDriverSide`,
  `printDynamicPartitionObject`, and `printAccessObject`.

The five Go test identities are `TestEncodeTaskType`, `TestDecodeDiscardPlan`,
`TestPlanIDChanged`, `TestReverse`, and `TestMain`. The Rust owner retains
source-derived coverage for those contracts and its existing codec edge
vectors; this batch adds the focused `analyze_plan_id_matches_go_master`
regression. No Rust-only production behavior or alternate cache/benchmark
path was added. The planner integration tests that still mention unported
high-level `EncodePlan` walkers belong to `tidb-planner`, not this package,
and remain explicit integration gaps rather than false plancodec claims.

## Go-to-Rust mapping and fix

| Go contract | Rust owner | Result |
| --- | --- | --- |
| Textual sentinels, Snappy/base64, decode/normalize tree formatting, escaping, alignment, and task fields | `tidb_util::plancodec::{decode_plan,decode_normalized_plan,encode_plan_node,normalize_plan_node,compress,decompress}` | Complete; byte-oriented output preserves Go strings that are not UTF-8. |
| Stable plan type table and reverse lookup | `PLAN_TYPES`, `type_string_to_physical_id`, `physical_id_to_type_string` | Complete through Go master ID 64. |
| Binary ExplainData rendering, connection column selection, access objects, labels, runtime fields, and discard sentinel | `decode_binary_plan`, `decode_binary_plan_for_connection`, and private renderer helpers | Complete; the existing source-derived vectors cover the distinct CTE/subquery and runtime paths. |
| `BUILD.bazel` library/test targets and `TestMain` setup | Cargo workspace member, `lib.rs` export, and owner test module | Complete build mapping; no generated/platform artifact is omitted. |

Go master added (and this checkout now restores):

    TypeAnalyze = "Analyze"
    typeAnalyzeID int = 64

The Rust table previously stopped at `PhysicalCTESource` (63), so decoding a
new Go Analyze plan returned `UnknownPlanID64` and encoding it returned zero.
The Go checkout also lacked the new source constant and both switch cases;
those are restored alongside the focused two-way regression. Both tables now
include Analyze at position 64, and the stable round-trip test treats 65 as
the first unknown ID.

## Validation

Profile: Ready. Commands run from the repository root:

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      go test ./pkg/util/plancodec -count=1
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
      DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
      cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib plancodec
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
      DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
      cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked \
        -p tidb-util -p tidb-expr -p tidb-stmtsummary --lib
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

Observed on 2026-09-01: the Go package suite passed:

    ok  github.com/pingcap/tidb/pkg/util/plancodec  0.464s

The regression was verified to fail before the source mapping with
`undefined: typeAnalyzeID`; after the mapping, the focused and complete Go
suite pass. The Rust owner suite passed all 15 tests, the affected-crate
check passed, formatting and diff hygiene passed, and `make lint` exited 0.
Existing
workspace warnings in the vendored client, planner, executor, model, and
transaction crates were unchanged by this batch. The focused regression was
verified in the Rust owner to fail before the table change: with the pre-fix
63-entry table it returned `left: 0, right: 64` for
`type_string_to_physical_id("Analyze")`. `make bazel_prepare` is not required
because this batch changes no Go import, Bazel file, new Go file, or module
dependency.

## Risks and unverified targets

The stable ID is a wire-compatibility contract: inserting a type anywhere
other than the end would corrupt existing plan IDs, so the fix appends 64.
Go's live high-level planner/executor walkers and Windows/unsupported-target
execution are outside this dependency-leaf batch; their owning package
receipts remain responsible for those boundaries. The Rust command used the
host's bundled OpenSSL and Go 1.25.10 paths; no system toolchain assumptions
are part of the package claim.
