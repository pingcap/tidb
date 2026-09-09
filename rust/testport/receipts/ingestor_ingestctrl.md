# `pkg/ingestor/ingestctrl` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The ingest-controller package contains 33 tracked artifacts and 16,709 lines.
Every production source, test/benchmark source, platform variant, and package
BUILD target was read in full before this receipt was written. There are no
fixture directories, generated source inputs/outputs, fuzz corpora, or other
build artifacts in this package beyond the BUILD target.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 223 | `b3d978d9a000496d898e4725ab6d9d02be438fd5` | `c50137d07f8cb628add95674731c29dadede9eb60160df7c4e237899032d0c00` | ingest-controller library and 50-shard flaky race-enabled test target |
| `checksum.go` | 570 | `9405100506a514d1d448815c0f69d4ca7592528a` | `2cdb4f53d0bbb601d6f1064f52f50528a5b52c107ca215fdb2dc63be17483c79` | SQL/TiKV checksum managers, safepoint lifetimes, retry and concurrency |
| `checksum_test.go` | 530 | `84b844a453f96a0928aa6e5cbeb0ca85ff696fe2` | `8b36a49ce9104516822207f8a342cca34bb4dfbd1fc5edeb7a0663add7945ba8` | checksum, GC lifetime, retry, and concurrency tests |
| `compress.go` | 64 | `dedd9c5a3772c999aec5276b44ee91f44e30565` | `fa45204ddbfd0def12a8238a10c0281da8baa3fa95120cc3ece15332d745c419` | pooled gzip compressor/decompressor |
| `compress_test.go` | 115 | `1a9cf768df31a8f1cbeaf4b3373de4cc626881df` | `8646e236c376489e333a82511cf5558abad1b30e9061fbfe39545cc71ec14eed` | compression round-trip and pool tests; four benchmarks |
| `disk_quota.go` | 64 | `b4ba5328fabc5f6a59aaeca87be4bc42ebbed065` | `e752b1dbeea736a4ce8c3425e51cedb5f90a3677dc80fd89ba73eb72804802c9` | importing-first engine ordering and over-quota selection |
| `disk_quota_test.go` | 99 | `670474b14f006041cbfa864664751924242b6024` | `fa57d678a76377e56f9b70fbcdbdcc2524b386370bc6f7469ff2dd600f7a48e0` | disk quota ordering and selection tests |
| `duplicate.go` | 1,288 | `67dc54e7b2f06453d21cc5be0d53e5c42f3b7383` | `dfe75d11119da3aebc686849afa49e2969b6098a470045be4e7f19b37c04df08` | duplicate ranges, local/remote streams, conflict decoding, retries |
| `duplicate_test.go` | 315 | `05dfbf3940ae32f1a562b1cba1a51aa824598172` | `8ae3ad3380d49121c34c1230247cd6344390581571516654afe0b70da482f86c` | duplicate range, stream, conflict, and keyspace metadata tests |
| `engine.go` | 1,750 | `ea4f0fb0e7001217707c03b013adb9e7e7901573` | `5e068b851e48bfa075174927e4a36dd4527071f741167260172c82a45614ae4a` | Pebble engine state, locks, range properties, ingest and SST merging |
| `engine_mgr.go` | 667 | `32ca5087d9b71242d0ba29a916a702f91db88e5f` | `5205e3f3fa08330a151db37d1b6719f66a09bfdb5e33fcf9b040d5c391e895e9` | local/external engine lifecycle, timestamps, cleanup, duplicate DB |
| `engine_mgr_test.go` | 150 | `55c78107c05ee0880ea71230579545f358bdc848` | `1c2479c9a4829adfef99fb61a33104a0db6fb7a518d3457748b52fbe88adc28d` | engine manager lifecycle and cleanup tests |
| `engine_test.go` | 277 | `32dceb4e797c5ba2c3fe93203dfc9ea070813ca5` | `58fd77f68769fc00b857199529e247b7032d6dc4ae7d467d88f8883cd1a21f85` | engine range, ingest, compaction, iterator, writer, and SST tests |
| `iterator.go` | 272 | `1fa8e150b6c5d5d443735c3f9552283e6a9a6804` | `cf8ffd54bfea5ab2993ea0756aeb2926c38a951f8863d8c3425a7b85301872a6` | Pebble, duplicate-aware, and duplicate-DB iterators |
| `iterator_test.go` | 243 | `09e57422fcf012339dfdf7bfba24adab10ffdf69` | `16264e22ba68e978f365ff32dbe56a6fd557146a08f33c8baca758a462c80e30` | iterator ordering, seeking, duplicate, and lifecycle tests; one benchmark |
| `job_worker.go` | 496 | `cb7f39f5629f3ab8a4abfc47c26736636f694764` | `2b8e9a1bd48e1e3765a478e9ea79841a14184fb9053a2915cc44b92c173ff500` | classic/object-storage workers, timeout/retry, recovery, ingest client |
| `job_worker_test.go` | 409 | `bf71623b63bb26d7a5481f085ecf2af17eb66fb8` | `fbd467f3634439c957f0734cb5fe542c236440c6dc899c711785bbb8eb69ba88` | worker success, failure, retry, and recovery tests |
| `local.go` | 2,035 | `9985fd4574e19b63fa9c3950f751cabbb89da459` | `8b63a4034d891f19cf1bb920108b373bbd1a87811b363bb54bf697ecba506868` | backend/client lifecycle, split/import pipeline, cancellation, writers |
| `local_check_test.go` | 117 | `6b5ad7fbe9304513037b95a44744dd4645e75cb2` | `6ba82737ed56905ab8b8472706376010e5a5d4d93d0dc3a84cf929ad9ad1cca9` | platform-independent local resource-limit checks |
| `local_freebsd.go` | 28 | `f238414b42294837a407163914a472ceec56bea2` | `123348afa26bf358dd1fc6885fc3fd36dcff5448fe01b5ebccddd7f221c3bcf4` | FreeBSD `RlimT` type and zap encoding |
| `local_test.go` | 3,404 | `59df566e4312337746f3e4c37c1903ebee29548f` | `77f2f0931cb0c8fcb0dfb3ec25d28ba76b6f52588a80355dd1f313258cbba01d` | local import, split, cancellation, backend, writer, and failpoint tests |
| `local_unix.go` | 92 | `da876e60245e9314ff31c4d94be6e2426b582a87` | `573ffffda064cb5385bc4966df4ef3360ee4908ad545e9ad0c60ae2e5ab10980` | Unix resource-limit get/set implementation |
| `local_unix_generic.go` | 26 | `abcf58d524ee794cdd68505daf2301c41926d27c` | `18eab60a4c7cfc14785c6798f4dbbdc34474dea635b384dad56df083f0a463a0` | non-FreeBSD Unix `RlimT` type |
| `local_windows.go` | 36 | `ef38e35c3ac4dab66ee1d71f6826414ebd23abd0` | `ca5f25d27438487eed8bf4de91394a81cee72b6f9d52ecc2b6a3374d2ca00092` | Windows resource-limit stubs and unsupported verification error |
| `localhelper.go` | 287 | `7791bd5c049b9827ef6007b9a7ef44c1f8747aff` | `7ef33706bdc8167ec01beac0b7fcb34cc84c7c3e9b246a7d4874dc932cced30f` | two-level split/scatter, store limiter, compaction threshold |
| `localhelper_test.go` | 470 | `f61bccd086f97e5315465e51fbe48961d7310cb1` | `c09e4961be10b6faf63fafc54b29e2ab50f9d4b17460acd64c1148bf2cdd09e1` | split/scatter, limiter, compaction, and resource helper tests |
| `main_test.go` | 36 | `d0892d6fb343d4108e9947f4622c17cb924475ab` | `75045586324bfcc9761ad4180cc440cdfdd0644b6a1cca4d1276b2e0d60c16f5` | package test initialization and failpoint setup |
| `rate_limiter.go` | 112 | `be8fdb5a969bab42f7da22e65f1815933a2fc633` | `d03a51b7c69037b50ef403028ff7bd9a687adbcbf4e0335ec6f7e286c69476f7` | per-store concurrency and rate gates |
| `rate_limiter_param.go` | 146 | `7db8b9b77012929ca8a539b02ce111b5a6766bee` | `d348cac5626e5faa7cca20205dac9bad1e40e05eca018dc47b5016aa3d5d2c0c` | meta-backed limiter parameters and defaults |
| `rate_limiter_test.go` | 162 | `287f1bde6c90601515f133af2e79b5dd7b9d4923` | `4bbc7016a0fbbca3312735fee3ef6a858a1db7e4f8a36b92a1b5b4ac40f533a4` | rate/concurrency limiter and parameter tests |
| `region_job.go` | 1,353 | `9e4edaea3e226018b0ddac2eb485e3e5dc47bc5c` | `2e761740bf0829ba778312036ec7829cf7fc1feced1962e516f18ce4c3c3cb58` | region job state machine, RPCs, retryer, dispatcher, store balancer |
| `region_job_test.go` | 796 | `dd2e2ce416e39d917058dfa9d123ee3cf36bfa72` | `636aa291864fef9241726c3b0620ce5936763d1241f1f8d862aeb4af45369e57` | region-job state, retry, dispatch, cancellation, and balancing tests |
| `tikv_mode.go` | 77 | `bc35f35b28ebd80705240b4ea8b622506cff6fa1` | `ef76af329aabbbdc97f4a4a5746b6518f5955cd95076af18419319dedda2c042` | periodic import/normal TiKV mode switching |

The package exposes 79 top-level tests (including `TestMain`) and five
benchmarks across its 14 test files. Coverage includes checksum and GC
safepoint lifetime management; compression; disk and rate limits; duplicate
range/stream handling; Pebble engine and iterator lifecycles; local and
external backends; split/scatter and compaction helpers; import workers,
recovery and retries; region-job dispatch/state transitions; cancellation and
failpoint recovery; and all Unix/FreeBSD/Windows resource-limit variants.
The manual `TestTotalMemoryConsume` calibration test is intentionally skipped
by its source guard.

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is limited to five files:
`duplicate_test.go`, `job_worker.go`, `local.go`, `local_test.go`, and
`region_job.go`. It updates keyspace metadata to the generated oneof form,
uses `RecoverArgs`, and fixes worker-context propagation, startup/release
ordering, cancellation error propagation, and dispatcher completion semantics.
`local_test.go` adds focused cancellation regressions covering worker-error
cancellation, parent cancellation, waiting for a running worker's `DecRef`,
dispatcher cancellation propagation, and closed-result-channel outcomes.
There is no BUILD or generated-file delta in this package.

## Rust ownership and explicit boundary

Rust contains generated TiKV protobuf/client vocabulary and adjacent helpers,
but no dependency-closed owner for this package's Pebble-backed local engines,
TiKV ImportSST write/ingest orchestration, duplicate detector/controller,
checksum/GC-TTL managers, split/scatter pipeline, worker/retry/dispatcher,
disk/rate gates, platform resource limits, or periodic TiKV mode switching.
`tidb-util::lightning_duplicate` belongs to the separate Go Lightning duplicate
package; `tidb-util::extsort` is a local sorter, not this import controller.
Generated protocol types and generic transaction GC barriers are vocabulary,
not implementations of these ingestctrl behaviors.

No Rust-only behavior was found to remove. Adding a cache-only controller,
partial ImportSST flow, or speculative backend API would not satisfy the
package-atomic Go contract without the storage, RPC, PD, and worker consumers.
This package therefore remains an explicit integration boundary rather than a
partial transcreation claim.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required for this batch. The package uses failpoints; the canonical wrapper
enabled and disabled them around both the branch-local and exact Go-master
package suites.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -count=1
# branch-local source: passed in 56.041s; failpoints cleaned to refcount 0

# same command from a detached worktree at Go origin/master
# exact master source: passed in 54.646s; failpoints cleaned to refcount 0
```

Not verified here: Bazel race/sharded execution, live TiKV/PD/TiFlash or
object-storage import services, Windows/FreeBSD execution, and full-workspace
tests. No Rust validation was applicable because no Rust source changed.
