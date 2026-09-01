# `pkg/dxf/importinto` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The parent package contains exactly 26 direct tracked artifacts and 9,158
lines. Every direct production, test, and Bazel file was read in full in a
detached worktree at the pinned Go commit before this receipt was written.
The three nested packages (`conflictedkv`, `conflictrows`, `jobhistory`,
`mock`, and `taskkey`) are separate Go package units; their complete
inventories and ownership decisions are recorded in their own receipts. There
are no direct fixtures, testdata directories, platform-specific variants,
fuzz targets, benchmarks, generated sources, generator inputs, or `OWNERS`
files in this parent package. `job_doc.go` is checked-in design documentation,
not generated output.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 199 | `7f5e422fc98ef66791127eba4b12ce6d511809ca` | `0016f2dc89c73ecdf999a80b423b8ff5e0a3949434f37779784f5eef38adfeb3` | public ImportInto library and flaky 38-shard test target with DXF, Lightning, storage, parser, and testkit dependencies |
| `clean_up.go` | 250 | `89a142462e731c941e437f96e3d28917017d7ebd` | `c3f2f4a50d6957386727192ba4ae77c1fac6553ec21b037ab4af9496ae789ab8` | classic table-mode reset, batched global-sort object-store cleanup, and bounded parallel metering |
| `clean_up_test.go` | 220 | `a48022091b32459fcbf7244b7342e60621ca7008` | `b70cbbefe07654cc374083acc76e26a7a32323c5ed1d4123afa41df687c3289c` | S3 credential redaction/cleanup, metering success/cancellation, and panic recovery tests |
| `collect_conflicts.go` | 357 | `63700a137395819345f2dcbb48f4979e70479209` | `c068d4227cd1895627091335fecdc05870d598334f8302c60f8865cc10e63fa1` | collect-conflicts step executor, KV-group routing, MV-index dispatch, bounded row-key state, checksum metadata, and object-store metering |
| `collect_conflicts_internal_test.go` | 322 | `ebbf59345e7cf0f984bb7f4594e023a8dffea0a6` | `1d3b34551984b5869daf243ccbee966ce9b8fdc56915833bf40e181a7fcc0cd3` | KV-group/index lookup, MV-index channel routing across API V1/V2 codecs, decode errors, and cancellation tests |
| `collect_conflicts_test.go` | 104 | `3763cd20c2b4cee1166af101b52516ae145c9bb3` | `98af46b621983b865780adeeacba35be2675686e5ea9dc26a09eeb12e0f09128` | mock-store collect-conflicts executor, checksum, file truncation, and conflict-row cap tests |
| `conflict_resolution.go` | 206 | `8bb0c7b0f59ef5a3809859fd5d8c3fa1cc4c92e1` | `ef2cfab5d701f0baa990d12d79b7708862833e52363637e7976fc9c45777b3fb` | conflict-resolution executor, per-KV-group concurrent deleters, external metadata, and encoder lifecycle |
| `conflict_resolution_test.go` | 191 | `27e622fb2118d572ff94fa103fb679267025d1e3` | `9584a54a229b3fd16d55668dce09a9c3788cdb85ea271dfcd86f777398162` | end-to-end duplicate data/index KV generation, resolution, and final table contents |
| `encode_and_sort_operator.go` | 219 | `380671b000437cbc486216c9218a78df663af736` | `4ddd08189d700529f2480a7364fb4ef2dcc9556803beba959c3b517d77a3b02e` | asynchronous chunk worker/operator, global-sort writers, duplicate policy, writer close, and memory budgeting |
| `encode_and_sort_operator_test.go` | 235 | `02eac01bdbcdb794a0ace2bcd2c6ea9c0337f4ea` | `58fb65f034820fa52897d51b8f27b189d0b4cefeac34862a09bbbb3ca874779a` | worker error cancellation/logging and index-count-dependent writer memory limits |
| `job.go` | 425 | `0d999e74bf766a863490ccef800227a875ff3dbe` | `121adaaba6d7fdccaa155872722560acce59bfd851f705b9a50f86ed5bdea652` | job/task submission, async-prepare selection, runtime progress/ETA/formatting, history last-update query, and task keys |
| `job_doc.go` | 329 | `fc60819301cb15e328a13df073691af5866aac21` | `095c42d46ad7c47a362835e0221a65335da77761dbe3d9719c78305aa2b7561c` | transaction-boundary and cancellation interleaving design notes for import-job and DXF-task rows |
| `job_testkit_test.go` | 770 | `c88497ab6f43801850456489d124baa1d6c27805` | `22ec6a3004bfba590d638af6f93f479f1ad9a22f538bfa8b62d18dc32a56537a` | nextgen/system-vs-user keyspace submission and cancellation, runtime progress, history timestamp, SHOW IMPORT, groups, and time formatting |
| `metrics.go` | 81 | `482c9b2a02302a9c2d7e7d9aa50a675f048b6291` | `9305abd41b5dea608846fc1f4d81a9fa94ade0e32d83344b118386643ad1b3cc` | per-task Prometheus metric registration/reference counting and unregister lifecycle |
| `metrics_test.go` | 77 | `cec6f3f363f2e4bc8ae231cf1b1c644946e8c2d5` | `8076788b84e71cb9cb90280bc1bd9cf10655c1e058f419166cd51ec18c09b52d` | metric creation reuse, counter decrement, unregister, and registry cardinality |
| `planner.go` | 892 | `6619dcf9543caf8e452469a097e78e00f83fd16e` | `a7cdcf8b51fde6a53553dfec44e66299183e81220b0b94012fce4aa46cbb1ab9` | logical/physical ImportInto pipeline planning, external metadata, merge-sort/ingest range splitting, conflict-info aggregation, and overflow-safe totals |
| `planner_test.go` | 414 | `9065385bb24e2f28a17ebfd3a55978f97aaa2ef0` | `3dfd4b028741effcc8041d880b969e7413e405ca85d05c35730e5f7514c57eab` | logical/meta round trips, physical steps, prepared metadata preference, merge-sort grouping, and range-split behavior |
| `proto.go` | 292 | `837f4259a7a092d3d9492276a62266567a9865aa` | `2bdfb05932549993f63cf147c314391ac6e2c695d537efd54b36207660d8a5d5` | serializable task/step metadata, shared concurrent state, conflict-group aggregation, minimal-task recovery, and checksum conversion |
| `proto_test.go` | 53 | `19965dd7e7ca8bc64f908d30433e9384f44d745c` | `792ac809d979843571477ac16d4760a55e0a021c96009b0155b4bd21beb9627a` | zero-count filtering, data/index group creation, and conflict-info merging |
| `scheduler.go` | 978 | `15ac97620dd1ab7dc8a2b016927d671bbb4db93a` | `c9f27ea76d2c8f22541b7b03f427bac4e9a61a49e85e9877b3c040c79b8fd0ac` | task registration/TTL, TiKV mode switching, async prepare, step transitions, cancellation guards, job status updates, cleanup, and retry policy |
| `scheduler_test.go` | 240 | `a7e2d13c1eb180edd014e91f86bb97db10e59d31` | `ac27c034564023a88bf0428326ce14db6e99222530f67d7352e494da2a1cfdb3` | scheduler extension unit suite for keyspace, eligibility, step, retry, and TiKV-import predicates |
| `scheduler_testkit_test.go` | 765 | `df552838c699a0a68c9d7854ac2e9ddb0c1aea52` | `d5d08687affabb70bb6f27ea1ac18f4322c35862841d8ada467eebed7012082e` | local/global sort lifecycle, prepare-mode transitions, cancellation races, table-mode reset, and mock scheduler integration |
| `subtask_executor.go` | 174 | `8140c7da9c1ab998edc96895b60c77471d818249` | `66e9674f986268233f0baddbcfa4b32469a93d16d5aaf4e4eee36d677e4b811d` | minimal chunk execution, local/global encoding, checksum accumulation, allocator rebasing, and checksum verification |
| `task_executor.go` | 969 | `c1739e8f3428515a4b5190950aa1429bc3e2d463` | `702b5da6ba672925d3b3d19f832aece8144f6c7e7a0a656b58995d84936d80cc` | import/merge/ingest/post-process executors, parquet memory estimation, idempotence, duplicate policy, conflict normalization, and metering |
| `task_executor_test.go` | 183 | `867b087faa0821e81272653d2dd17053cb91b706` | `c6c60449cce4d0e8b7a0f89b6198d0d7bde984b1334b22c7040b29a9fdbfb584` | executor selection, task-runtime store use, duplicate policy, and duplicate-error normalization |
| `task_executor_testkit_test.go` | 213 | `9bc92ea9e7a5972d2225c2dbaf639405af8d7ece` | `8b3561834a796b9a92425ef801a1ba5246fa17f2f11c712b6b885b476a7df858` | post-process checksum levels and local-engine cleanup on retry |

The production inventory contains 170 function/method declarations across
the direct files. The test inventory contains 45 top-level test functions or
suite methods (including the `TestImportInto` suite runner), with nested cases
covering cancellation, codecs, keyspaces, global/local sort, duplicate modes,
checksum, and metadata serialization. The direct package's only build artifact
is `BUILD.bazel`; its `flaky` and `shard_count = 38` settings are scheduling
metadata, not runtime behavior.

## Rust ownership and parity decision

Rust's `tidb-dxf` crate owns the generic task, subtask, resource, task-type, and
step vocabulary. Its ImportInto constants and step labels mirror the names and
ordering, but it has no dependency-closed ImportInto planner, scheduler,
executor, Lightning encoder/SST writer, object-store conflict files, TiKV
ingest/deletion path, table-mode transaction integration, job-history SQL
queries, or checksum verification pipeline. Rust parser/session crates own
the SQL `IMPORT INTO` statement surface only; TiKV/DDL/SQL test mocks implement
unrelated traits. No Rust-only parent-package behavior or ignored runtime test
was found to remove.

The package is therefore one explicit Go-only integration boundary. Porting a
step enum, formatter, or mock in isolation would leave the task-key,
object-store, table metadata, scheduler state, and storage consumers
disconnected and would create speculative behavior. The nested conflict,
cleanup, history, task-key, and generated-mock packages remain separate
atomic receipts, not partial claims against this parent package.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passed with failpoints enabled and disabled by the
repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/importinto -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dxf/importinto 11.138s
```

Ready repository gates for this receipt batch passed:
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

Residual risks are intentionally recorded rather than hidden: the package
still depends on Go-only Lightning/tablecodec/object-store/session APIs; the
nextgen job/task rows are split across keyspaces with documented cancellation
windows in `job_doc.go`; and Rust has no executable parity path for those
semantics. Parent Go behavior is covered by the passing mock-store and
failpoint suite, but no Rust implementation parity is claimed.
