# `pkg/ingestor/simplesst` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The simple-SST package contains 19 tracked artifacts and 6,545 lines. Every
production source, test, Bazel target, and package support artifact was read in
full before this receipt was written. There are no fixture directories,
generated source files, platform-specific variants, fuzz inputs, or additional
build artifacts beyond the package BUILD target.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 95 | `7c44b45d64384196cb51ddef0617b8d99fb64f72` | `8304174029106a52ebc6971ba947e332492e35728b09f9e31350c5e9b09bbab1` | library and test targets |
| `byte_reader.go` | 374 | `02b252c162013c971bf9fee63eacc493ad017a8b` | `1b96f1c5d2392a802c19bb09b6ab3e1b934b05a05b74cb396e8cb2f9ca452f68` | object-storage range reader and retry/concurrency switching |
| `byte_reader_test.go` | 342 | `8355fbc3001223e694d4c878733c8b752c9e7224` | `64cc1614cfe76a210f68baed80f10d7248b3dcc13675b80cc53e39319c7b2965` | fake-S3 reads, EOF, allocation, and hotspot tests |
| `codec.go` | 95 | `312905cb9ee28d25a45543f8d7c07c7cad4b3615` | `a2c18c650000306989d6c3c77f5c41e6b4dff8504bb1cc45848059443455b253` | length-prefixed key/value codec and range-property collector |
| `codec_test.go` | 53 | `3e5c3660f92e5133b92660c56ef6e76f69a710b6` | `9f2d643ed52792e51ea60cbb1f6ed9a8fca16fdd69f6eef13b411599aaa36827` | range-property codec regression |
| `concurrent_reader.go` | 101 | `22c8364cd833c0e64f58a47456abb6b5381be9b2` | `c84b956a4194bc6df6d16ee2658c68f1c47f08015011a12fcbe8b77ba931db0a` | bounded concurrent range reads |
| `concurrent_reader_test.go` | 85 | `6ad3b22b1291f8dc422417992ae8e33fd7366006` | `a761dd7d5c13d65fd5b3f2108ee00da6c08986608ca0fe59327ce257a02ed111` | random offset/concurrency reader test |
| `file.go` | 92 | `9caff48f0840ecd30e011c60d9be6b023ac5e4c5` | `e0ae674db03851146b9282b8be0ab16c788853f317bbb92709b5563420548f56` | SST key/value file abstraction |
| `file_test.go` | 179 | `a0de0d8b1fb009233420cb4386c9b5db8b52efee` | `eec4d5abde151541f2ab2dda2b32c89b5891cc21339ef1083ad9b6b4cf8ed615` | file writer and KV reader roundtrip tests |
| `iter.go` | 864 | `0fe3af07bb5c315ca0f6c233c1a9e5c039c641aa` | `0cbd660309a693b01c59a641905fb3320c87d8feabc672218aecce4e44f57289` | SST iterators, merge iterators, and weighted/limited variants |
| `iter_test.go` | 747 | `3f78fb2b6e214ec2ea738e5517117c85e97a1dd6` | `efb44abacb802cd7f5596bf5448992738b76fe29e82951469520f643fb630236` | merge, hotspot, corruption, close, and leak tests |
| `kv_reader.go` | 121 | `159707fe6c88c580ecef1ae79b1e3629abac50a0` | `a9049d88579753105ebb356bb8959fa71ec03d788a1b15274d72b9c0e15e27b1` | external-file KV reader |
| `onefile_writer.go` | 389 | `3ec4a111023cc8924e9f480a24d2b384e8d3f0d1` | `08245edffcb6c0982541d5f8e0111230b9e2b281af8d8e60ccde6eec5e92f569` | one-file writer and metadata/stat output |
| `onefile_writer_test.go` | 362 | `edaddaf66abd3b79357f7be232f5c1419068800d` | `5b90d1beacd13296df8bdd3e28385dbe79bd447062d070fee9553162d8202960` | one-file metadata, offsets, and duplicate-mode tests |
| `stat_reader.go` | 61 | `f4cf65a8aa8d177548571815f9c119bb2f39ddf2` | `88c80e315d0ae735e18ba6a94e3cbbdfbc3ad0c91c7115fd2c56f484add1df73` | statistics block reader |
| `util.go` | 311 | `99d65b4a720eddbdfdccc2236b9e02fd26bbebe8` | `f11b6a3092a33013192a0aaa37f1b383c2c14d5253eb9c7d0e6f21f184b9d990` | overlap, duplicate, stat-offset, and file-enumeration helpers |
| `util_test.go` | 398 | `8437cf83d80c2293493a22e1a402856bd4c35cbc` | `1bd220698111ca4927ad9f2374e5b11f7113e704a9e4cc825b1deda5540f3b20` | utility and bounded-read regressions |
| `writer.go` | 946 | `1fb1379f50d9478870a54197224c8333d421595c` | `8af1cdebfcce05c9c7ceec1b88933f037864cc24dac7fbc8cbf01e46fb8fe69d` | SST writer, engine adapter, duplicate handling, and partition output |
| `writer_test.go` | 930 | `850a69661c833b8397a85f30199dc57f818b33fa` | `704a5e679797554037febe7bdf544c8c9b5cca67826e8aafc7f6fa8866916842` | writer/engine, stats, partitions, retries, and duplicate tests |

The current Go-master delta from the earlier pinned source is behaviorally
small but recorded here: `GetAllFileNames` now accepts zero or more
`nonPartitionedDirs` and matches any supplied directory, while the writer
names its existing per-core connection limit `maxOpenedConnPerCore` instead of
using an inline `250` literal. This batch restores those exact Go-master
changes and adds a focused multi-directory regression for `GetAllFileNames`;
before the fix the test failed to compile because the function accepted only
one directory.

## Rust ownership and explicit boundary

Rust has no `simplesst` implementation, object-store SST byte-reader/codec,
TiDB range-property format, merge iterator, duplicate-aware writer, or
ingestor engine owner. `tidb-util::extsort` is a local-disk sorter with a
different file protocol, and `tidb-dxf` owns only task/step metadata; neither
is a behavioral owner for this package. No Rust-only behavior was found to
remove. Implementing a disconnected object-store SST stack without a Rust
ingest engine, client, and metadata consumers would invent an API rather than
close a Go package, so this package remains an explicit parity boundary.

## Validation and risk

Profile: **Ready** for this Go behavior batch. The package uses failpoints; the
canonical wrapper enabled and disabled them around the test run.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ingestor/simplesst -count=1
# passed: all package tests in 11.801s; failpoints cleaned up to refcount 0

Focused regression:
  go test ./pkg/ingestor/simplesst -run '^TestGetAllFileNamesMatchesMultipleNonPartitionedDirs$' -count=1
  # passed after the fix; pre-fix compile failed on the variadic call

`make lint` and `git diff --check` passed. `make bazel_prepare` was attempted
for the changed Go sources and is blocked locally (`bazel: No such file or directory`).
```

Not verified here: a live object-storage service, next-generation ingest
engine/client integration, Bazel, or full-workspace tests.
