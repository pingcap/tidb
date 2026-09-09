# `pkg/ingestor/globalsort` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02).

## Complete inventory

The global-sort package contains 17 tracked artifacts and 6,814 lines. Every
production source, test/benchmark source, Bazel target, and package support
artifact was read in full before this receipt was written. There are no fixture
directories, generated source files, platform-specific variants, fuzz inputs,
or additional build artifacts beyond the package BUILD target.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 100 | `625d4eb7aee6508803d40c4494b96aa00d65e304` | `c06b55736673a8fbbb776770d7e4437e5cc952ec5d8c3d52a961b621fd35032d` | global-sort library and 41-shard flaky test target |
| `bench_test.go` | 874 | `5475dfdb0c2c7cd4650f69048bd22266a72cdee6` | `593a2e82662fc6e6b9c79daed7785a796f2d27b79dab367b9c8bde293a141fc4` | storage writer/reader/merge performance harnesses and large-file tests |
| `engine.go` | 935 | `b2644c18b0ad8b0aee2509876112b85e8e12177c` | `a872d0aecf3498be145a03a2ce9b38048543ab2ed7fa9f39d4df266918b92295` | external engine, memory batches, ingest data, resources, and duplicates |
| `engine_test.go` | 617 | `174f0df054c47ed0459a521a5c84e5cffdb544de` | `88cb77b914120513906c099402bbffd40917a88b7226ed682c96493c647cdb57` | in-memory ingest data, retries, duplicate modes, batches, and resource changes |
| `kvgroup.go` | 37 | `792794d1cb5f944626af13c88d29b830e7db42a1` | `00c3a5f4bf5a2fbdc108b694be656ee945de713228e56e4e168a5f45331ff6be` | data/index KV-group naming conversions |
| `merge.go` | 366 | `4672ce26ef7da94b8d7180f440135044bd9466ca` | `e2dbdcca20eadd52260debf0aedb71becba00feae621a1ad55d3d2ab59ad3d37` | parallel overlapping-file merge operator and target-file planning |
| `merge_test.go` | 415 | `1f245752813e51cf7ab1b13f654a525126245d7b` | `f78637a6d17347ee87ee1d1fedc9f1e6f6d378991f4d40a2188defafa8c5201b` | split, failpoint, merge, duplicate, collector, and one-file regressions |
| `merge_v2.go` | 195 | `543ed15665050fb649cd76f9afcb11aa15a13b97` | `0b4c6490f9ff5392bbf7e4808a2b7de71f511a532877288e4dfc6e3bf50ff3d6` | range-splitter based single-reader/single-writer merge |
| `misc_bench_test.go` | 428 | `33a0e89fa5e1489a4c13a5e75ff1bf32d0a07c6d` | `2987b9e6ca27e5914456938f3fbb698a26229bcaa121128d170416807f3dfc1c` | key generators, storage setup, profilers, and benchmark support |
| `reader.go` | 279 | `a5f02bc064595b96f071524f20a1b561b19cadfd` | `b5b68440216c4b0765856d6b6883da55211adc3414a7f50c4777d36043808fec` | bounded asynchronous file readers and KV-channel reader |
| `reader_test.go` | 213 | `1e1af477c032bcd50aee598a7b087dd5e63a9921` | `f70a8c08bf1f46fd926efc9f70cd0673d54d80005615e5ee2f389d1fa5014ea6` | basic, one-file, large-file, and asynchronous-reader tests |
| `sort_test.go` | 356 | `99ef5a4115682b8ff393a5124b5f72b8d8e1cf7d` | `7e6e3887d069526c561226287ba2fc8993f9b309194295fa5833ba91731a73dc` | end-to-end local global-sort and merge-v2 tests |
| `split.go` | 326 | `1672c9e41cc8c87babd6d9346a2f788caa457ecc` | `22e2bfa1cfb0a71deb04831e7f27a2f0f5eb2eb72c4ac18173bdb4d0793b2a0b` | range-property heap, range/job/region split boundaries, and active files |
| `split_test.go` | 603 | `b8c9a78c437c24a7b780a892e7007b797958f7fd` | `b30d656e4440577ba59c9e57d5a15f7964b471c80f90e3324f4390773f4f5b40` | property invariants, strict overlap, 3K-file stress, and range sizing |
| `testutil.go` | 123 | `c10ea3a45e2d4f51108bb4aca405f09e248ec63a` | `eb0bd37561aa15b782a649359097b695d2ebfc4d19e98da665dec631fc63ab4c` | test storage metadata and read/compare helper |
| `util.go` | 385 | `604e0299ca7a97a105f7b48017722e6655ba34f5` | `d0b14ef7e09beb3201895d5b9949ebe3b51d45166a2ca451e17a107189b41001` | cleanup, JSON external metadata, path helpers, and file-group division |
| `util_test.go` | 562 | `84cf4d3d2cfef91d07f6ad6a39e6094c1b0ec97f` | `63397bb34ff68b7d01592c30d448e338ce022e6f15d3ea5c6e24cb5b3cf03f86` | cleanup, metadata marshal, path, and target-file-limit tests |

The current Go-master delta from the earlier pinned source is recorded here in
full: BUILD now depends explicitly on `pkg/ingestor/errdef`; merge planning
extracts `getTargetFileCount` and grouped target counting and returns `nil` for
an empty split; cleanup accepts multiple non-partitioned directories; and
`DivideMergeSortDataFiles` preserves complete rounds while enforcing the
adjusted target-file threshold, returning `GlobalSort:TooManyDataFiles` when
necessary. The accompanying tests cover these cases, including large node
counts, non-monotonic target counts, and multi-directory cleanup.

## Rust ownership and explicit boundary

Rust has no global-sort external engine, object-store SST merge reader/writer,
range-property splitter, duplicate-aware ingest data path, or global-sort
planner owner. `tidb-dxf` contains step labels and node-resource arithmetic
only; `tidb-util::extsort` is a local-disk sorter with a different protocol,
and neither crate can consume this package's metadata or feed a Rust ingest
engine. The Rust step documentation is metadata, not an implementation of
these Go behaviors.

No Rust-only behavior was found to remove. Implementing an isolated merge
stack or adding public planner APIs without the corresponding Rust ingest
engine, client, object storage, and TiKV region-job consumers would be
speculative and would not complete this Go package. This package therefore
remains an explicit parity boundary.

## Validation and risk

Profile: **Ready** for this package-level parity batch. The package uses
failpoints; the canonical wrapper enabled and disabled them around the complete
package suite. The pre-fix focused regression failed because nil input produced
an empty non-nil split slice; the post-fix focused regressions and full suite
pass.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor/globalsort -run '^(TestSplitDataFiles|TestCleanUpFiles|TestDivideMergeSortDataFilesBasic)$' -count=1 -vet=off
# passed: focused nil-split, multi-directory cleanup, and target-limit regressions

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ingestor/globalsort -count=1 -vet=off
# passed: all package tests in 10.554s; failpoints cleaned to refcount 0

make lint
# passed

git diff --check
# passed

make bazel_prepare
# blocked: bazel executable is not installed in the local environment
```

Not verified here: a live object-storage service, distributed DXF/global-sort
execution, Bazel, or full-workspace tests.
