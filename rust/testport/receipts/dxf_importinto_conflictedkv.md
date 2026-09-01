# `pkg/dxf/importinto/conflictedkv` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly 11 tracked artifacts and 2,667 lines. Every file
was read in full in a detached worktree at the pinned Go commit before this
receipt was written. The package has one package-level documentation source,
one public BUILD target, five production sources, and five test sources. There
are no fixtures, benchmarks, generated sources or generator inputs,
platform-specific variants, `OWNERS` metadata, or other checked-in artifacts.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 81 | `5b1e11d2659eddac4391fea16956c3d905bf61f7` | `ab987e491e87a0a4fba33d16931f71496045172f0f72de4e2502564c336b91bc` | public library and nine-shard flaky test target; dependency closure for DXF, global sort, SST, tablecodec, Lightning, object storage, and testkit owners |
| `collector.go` | 271 | `600b9c1ac23bdc27a2290ffbfaec61a2e2313a56` | `28bd03c966534b9c3a40f2a9bcaacc9f6793e30dda985c4267f96494d4aae7bb` | conflict-row collection result/checksum accounting, row-file rollover, shared hard size cap, object-store writes, and traffic/progress seams |
| `collector_test.go` | 380 | `0bee722dbdbee63eae9fa34c6dfca9d6350d4242` | `1473e818a4e18eb3d59eb3e2663ab4c8d8c96bc7bf6c9e670ec51c17e7d6a3aa` | result merging, codec errors, data/index row recording, file-size rollover, hard-cap behavior, writer-close errors, and shared collector limits |
| `deleter.go` | 254 | `8e079061e0d4e8cfb838df1a559a3bb7698e9f90` | `d071e0886fc6eb9485706a9f87d03100f6e6df4cccad4a6529d7ea0491d89599` | buffered TiKV key deletion, transaction commit/rollback, retry/backoff, snapshot reads, traffic accounting, and cancellation-safe key delivery |
| `deleter_internal_test.go` | 126 | `3bdaf678e3a290ce06a0cb22bef1be9850ac632e` | `f5d1210ef0473e034f15d7e4ad62ac253bb15f3c192916135ad70b1c0b58d0ad` | commit-error regression and retryable write-conflict regression for buffered deletion |
| `deleter_test.go` | 195 | `eebe6a527bccc1373e626a46194ccdfb3d772d62` | `4c9531bed94c4983a63ea530729eecd165522d64cd7ea78711152e3d38e4b2d2` | end-to-end data/index conflict deletion over clustered/non-clustered tables and V1/V2 codecs |
| `doc.go` | 165 | `85bc4c2bff14dee05b6ed4889c94bcf97efe66b5` | `d7813a4c4d8527828aad26c4d79b083135bd4fbb9f2ef15991713686ce05f940` | package contract for duplicate KV collection, conflict resolution, checksum correction, and the worked table example |
| `handler.go` | 403 | `6ce0b2b8be0c19b630aaf8bdf4ff5eba7ceb5b9b` | `45141dcb1d4c1511238954393d3b6546caabf3848fa4a5d933d88acc21c46fb0` | common/data/index KV handlers, row re-encoding, buffered index snapshots, keyspace codec handling, traffic recording, and handler lifecycle |
| `handler_test.go` | 527 | `cc28cc725bf1fae1555722dbba4956d0560f80b8` | `dfa61c8043577ce1b4cdad7cf4a13f38a2a10b917cc8e6cbc28643928e7350cd` | data/index handler lifecycle, clustered/non-clustered tables, functional and multi-valued indexes, local/global deduplication, and retry-after-handler-error cases |
| `row_handle.go` | 160 | `98f2c4294537dd41d7cc7bc20e6cddf324dcff80` | `893cfa869ad0aef6078b504d30799eed44bbe3b50ed46d550684b20186beefbd` | nil-safe global/local key filters and bounded row-key sets with shared atomic memory accounting |
| `row_handle_test.go` | 105 | `f85f9adc0aea76f1777611d8f61f8610546f46d4` | `f28ee96895d504b2bf338abb777cf7277c9f25c8fd437adae365cba739653399` | nil filters, physical partition row keys, size-bound admission, overflow, and set merge behavior |

The production inventory contains all 47 functions and methods: bounded-key
filter/set construction, admission, lookup, merge, and failpoint limits;
collector result merging, row encoding, file switching, hard-cap handling,
closing, and result access; deleter construction, lifecycle, retry/backoff,
snapshot gathering, transaction deletion, and buffered delivery; base/data/index
handler construction and lifecycle; row decode/re-encode; index-ID and
partition-aware row-key derivation; buffered index handling; lazy snapshot
refresh and traffic-counted `BatchGet`; and the row-file naming helper.

The complete test inventory is nine top-level tests. `collector_test.go`
covers `TestCollectResultMerge`, codec decode failures, data/index file
rollover at 90/300/800-byte limits, total-size caps, writer-close failures,
and shared-cap collectors. `deleter_internal_test.go` covers non-retryable
commit errors and retryable write conflicts; `deleter_test.go` covers data and
unique-index conflicts under both transaction codecs. `handler_test.go`
covers data handlers on clustered/non-clustered tables, functional-index NULL
re-encoding, index handlers and local/global filters, multi-valued-index
deduplication across batches, and marking a local key only after successful
handling. `row_handle_test.go` covers nil filters, physical partition keys,
bounded admission/overflow, and merges. The BUILD target also records nine
test shards and flaky execution; production failpoints cover the total-file
size and row-key memory limits.

## Rust ownership and parity decision

Rust's `tidb-dxf` crate owns only the ImportInto task/step vocabulary, including
the adjacent collect-conflicts and conflict-resolution labels. Generic Rust
DML conflict handling is a separate SQL row path. The workspace has no
dependency-closed owner for Lightning/global-sort duplicate KV collection,
conflict-row object-store writing, TiKV snapshot reads, row re-encoding through
table metadata, checksum subtraction, batched transactional deletion,
keyspace-aware codecs, traffic metering, or the ImportInto handler lifecycle.

No Rust-only conflicted-KV implementation, ignored test, or duplicate receipt
was found to remove. Porting a checksum type, key filter, or retry helper alone
would create a second conflict-resolution policy without the global-sort,
Lightning, object-store, and ImportInto scheduler consumers. No speculative
facade or partial implementation was added; the complete package remains an
explicit integration boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passed with failpoints enabled and disabled by the
repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/importinto/conflictedkv -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv 3.203s
```

Repository formatting, lint, and diff hygiene are run for this receipt batch
(`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all --
--check`, `make lint`, and `git diff --check`). No Go source, import section,
test, Bazel target, or module dependency changed, so `make bazel_prepare` is
not required. Rust tests and a full workspace build are not run because no
Rust source or owning target changed. Conflict KV ordering, keyspace codec
compatibility, object-store interoperability, checksum correctness, retry
behavior under real TiKV, memory caps, and traffic metering remain unverified
on the Rust side; this receipt records the boundary rather than claiming
transcreated parity.
