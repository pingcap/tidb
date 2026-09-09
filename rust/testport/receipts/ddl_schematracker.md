# `pkg/ddl/schematracker` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly six tracked artifacts and 2,979 lines: one Bazel
target, three production/support files, and two test files. All six artifacts
were read in full before this receipt. There is no `doc.go`, fixture directory,
`testdata`, generated source/input, platform variant, benchmark, fuzz target,
or `OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 68 | `b79409303bdb7e9e86c0a42def24943d9db98d49` | `579dcf6e7141395d24abb8aba7f2d18bb59a5dabb69cf27903480b8a7f8c02de` | schema-tracker library and test target |
| `checker.go` | 633 | `b8b340e2f66c994440ff71719f32c1cdccb356c0` | `e8cef9d4a1690541edcac8bb4d8098d55e4cd06c09b487e7fc8dd809ec30a4ff` | differential DDL/SchemaTracker checker and storage injector |
| `dm_tracker.go` | 1,216 | `970f306573008bc258640f8f68e9c6b25592e068` | `870e64c3557d2b0d9d3172239d23a08c391f3286204b82ceb51e19e9773f443f` | DDL-backed schema tracker and table mutation logic |
| `dm_tracker_test.go` | 701 | `6471c56bb95ff84b71d5bd7af3b986561d13742d` | `ec44f486631f2d6fe92e57dba15f050e646d692f98e4269ec1eb4f94d8da835f` | tracker DDL, index, partition, and immutability tests |
| `info_store.go` | 205 | `7c53abe56f415cef8a70576794285736bfb12e3e` | `1bd37cda68bcfefa503ff62c7f05da5c068e5d527bb3020fb3b645675838b353` | case-aware schema/table metadata store |
| `info_store_test.go` | 156 | `a6edb120e9ef7a6ec53c06c004582fe70d03d162` | `8e955e7ddd3684064b7b4f906f004db2ae54c9d9810f0b02d77050738e4ccf74` | lower-case and deletion behavior tests |

The production inventory has 134 top-level declarations. The test inventory
has 17 top-level test functions; current files are byte-identical to all
pinned Go-master artifacts.

## Native integration decision

This package is Go-native DDL infrastructure. `SchemaTracker` mutates
TiDB model metadata through parser ASTs, infoschema, session contexts,
restricted SQL execution, auto-ID allocators, placement/storage attributes,
and the live DDL executor. Rust has a deliberately partial
`tidb-exec::schematracker_info_store` seed and source-shaped tests, but no
dependency-closed tracker/checker/DDL owner. The Rust module documents its
missing `dm_tracker.go` and `checker.go` dependencies, so no Rust-only
behavior was removed and no speculative partial implementation was added.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete
failpoint-aware Go package suite passes:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/ddl/schematracker -count=1
    # PASS; ok github.com/pingcap/tidb/pkg/ddl/schematracker 1.375s

The Rust source-shaped InfoStore test file is not registered as a Cargo test
target (`cargo ... test --test schematracker_info_store_source` reports “no
test target”), so it was not claimed as a passing Rust gate. Rust formatting
and `git diff --check` pass; no Go/Bazel source changed, so
`make bazel_prepare` is not required. The main compatibility risk is the
existing explicit seed boundary: a future Rust tracker must first provide the
DDL executor/session/infoschema graph before package ownership can be claimed.

## Outcome

The complete schematracker inventory and explicit Go-only/SEED ownership
boundary are recorded. The rolling audit continues.
