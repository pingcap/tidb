# Shared cache owner batch: B01, B02, C03 and C04

This batch repairs four existing findings together, starting at integration
`b38a25eb0e361739460d81263a17bc75455a3974`. Freshly fetched Go master remains
`93a01d31f6da205ae4bf376825293903a6899fdb`, selecting Ristretto v0.1.1.
Concurrent MDL-default commit `7b991676da79f044774caf6da4dfffe247160feb` is
preserved by a clean merge. Native client-rust remains unchanged at
`19a56ccda1e128218cd33c69709038219aced9bc`; no native synchronization is needed.
All source and build work ran in Codex Cloud.

The register moves from 65 unresolved/21 repaired to **61 unresolved
(52 open, nine partial), 25 repaired, 86 tracked**. The recorded source paths
of all 65 prior unresolved IDs were compared with the starting tree: 51 are
unchanged, fourteen share edited files. Four are repaired here. The remaining
ten still lack their separately recorded bootstrap, GC, TTL, resource-control,
RU-history, cross-keyspace, learning, inference, versioned InfoSchema or AZ
owners. The MDL merge changes three of those shared-file references only.
This comparison is not a new runtime reproduction of all unresolved findings.
The [validation JSON](shared-cache-batch-validation.json) retains per-ID evidence.

## One dependency, independent owners

`tidb-ristretto` implements the complete pinned root package. Its single
32,768-entry write channel defers publication of new entries until admission;
resident replacement is immediate. It retains the separate lossy read channel,
TinyLFU doorkeeper/sketch, sampled LFU budget, sharded store, TTL buckets,
ordered deletion/Wait, deferred costs, optional metrics and callback ordering.
Clear quiesces public operations and Close joins workers. Workers own the core,
not the public cache handle; callbacks can enqueue statistics eviction triggers.

The [native inventory](ristretto-native-inventory.json) maps all 91 original
module artifacts, all six production sources, all six test sources, 73 original
tests and five benchmarks. Every source SHA-256 matches the pinned module.
Rust Arc/Vec ownership replaces Go allocator/GC helpers; native randomized
hashing replaces runtime-specific primary hashing while preserving integer
identity, byte/string equivalence and xxhash64 conflicts. A dense native index
supplies bounded randomized policy samples in place of Go map iteration.
The source 56-byte internal item charge remains independent of Rust layout.
Root-required Bloom/histogram operations and licenses are retained; separate
`z`, `sim` and contribution packages are explicitly not accepted wholesale.
Concurrent Clear declines new operations rather than advertising a broader
contract than Go's externally quiesced Clear. Callback self-wait/panic is not
supported by the root; the LFU owner retains Go's callback recovery.

All three existing production consumers migrated before removing Stretto and
both private FIFO stores. They share implementation, not budgets or lifetimes.
Inference remains X02; the separate instance plan-cache contract remains C02.

| Finding | Production change and evidence |
| --- | --- |
| B01 | Binding cache uses deferred binding-size cost, source metrics/callbacks and Set/Wait. It retains digest indexing and shared binding references. The hot-binding regression fails on FIFO and passes on frequency admission. |
| B02 | One live cache survives reloads. Internal SELECT uses the update-time watermark with ten seconds of clock overlap and preserves equal/newer cached references. Independent reload/GC/usage deadlines use the binding owner, live usage switch and joined shutdown. Usage is written in bounded transactions; each committed batch is acknowledged even if a later batch fails. Real unistore tests verify persisted usage and old/young tombstones without a later binding write; owner-retirement tests stop GC. |
| C03 | Coprocessor construction uses effective TiKV settings and allows zero-capacity disabling. Requests, admission thresholds, timestamps, region versions, collision bytes, paging and shared payloads remain owned by coprocessor code; Ristretto owns storage. Process shutdown closes it. Hot-result, direct-unary replay, retained payload and enabled/disabled configuration tests pass. |
| C04 | LFU retains primary-first access, immediate fallback publication, nil trigger payloads, shared table identities, callback recovery/accounting and its public-operation lifetime guard. Both paused-admission and original concurrent-pressure failures pass without ignores. Original policy cost assertions are restored. |

The complete [five-artifact LFU package receipt](lfu-lifecycle-package.json)
maps its original ten tests, support and build obligations. Root Ristretto and
LFU are the atomic package claims. These four finding closures do not certify
all of `pkg/bindinfo`, `pkg/store/copr`, parent statistics packages or Domain.
Unrelated capture/evolution, planner digest, inference and runtime obligations
are not hidden by the cache repair.

## Tests that previously certified the wrong behavior

Removed the stale `negative_table_id_matches_go_shard_indexing` panic test:
negative eviction-trigger IDs already use the corrected native shard mapping.
The retained negative-ID test now covers -1 and -256 and checks evicted metadata.
Callback recovery injects an actual panic instead of assuming negative IDs panic.

Binding and coprocessor tests no longer demand the oldest FIFO victim or
synchronous rejection of a queued oversized write. They preserve Go's count,
budget, hot-key and Set/Wait assertions. Direct storage-update fixtures now
advance `update_time`, as Go's incremental loader requires. The binding snapshot
fixture pins individual references while the shared owner updates in place.
Tombstone tests explicitly invoke maintenance instead of expecting an unrelated
write to run GC. The two C04 ignores were removed; other ignored tests remain
explicit unrelated gaps.

The new usage failure-injection fixture initially counted the lock-row UPDATE
as a usage write. That invalid red result was discarded. The corrected fixture
counts only last-used writes; it fails against the old end-of-loop acknowledgement
helper and passes when each committed batch is acknowledged. The root allocator
TTL test also restored the original one-second TTL after a shortened 20ms fixture
expired before admission under pressure. No meaningful assertion was suppressed.

The final wire smoke exposed duplicate startup registration of the three
binding gauges. The server dashboard now re-exports the live session metrics
instead of defining a second owner. The retained startup/refresh regression
checks shared handles; the original startup panic is retained as red evidence.

## Validation and boundaries

Commands use `/workspace/.cloud-setup/env.sh`. Logs are under
`/workspace/.cloud-setup/cache-batch`; commands, counts and log hashes are in the
validation JSON. The final selected suite has 475 distinct Rust test/benchmark
smoke cases including the incoming MDL regression, with four unrelated ignores.
Repeated runs are not added to that total. Original Go race suites pass all
73 Ristretto root and ten LFU tests. All five original benchmark obligations ran
in each language; their different iteration units prohibit a speed comparison.
No workload speedup is claimed.

The seven affected Rust crates pass all-target checking. Root `make lint`
passes, including protocol/provenance checks; its full log has no hidden
module-download error. No Go imports/sources/Bazel files changed in this batch,
so `make bazel_prepare` is not triggered. The Ready profile requires the actual
precommit hook and a separate fresh locked server build immediately before
push; publication results are recorded after execution. The real-server smoke
uses the documented passwordless local development fixture and ephemeral
unistore database, followed by process shutdown.

Full bindinfo/copr Go suites, live multi-node TiKV/PD, platform/TLS matrices,
long-duration production timer observation and sysbench/TPC-C/TPC-H/YCSB
performance remain unverified. Timers are tested through the actual worker
with short injected intervals, and the real internal-session GC/usage paths
are invoked separately. Existing compiler warnings and unrelated ignored tests
are retained accurately.

The cloud disk is 32 GiB. Incremental outputs exhausted it during iteration;
disposable incremental data and seven obsolete test executables were removed
while Cargo was stopped. No compiled library dependencies were removed. Reusable
setup now exports `CARGO_INCREMENTAL=0` and `CARGO_BUILD_JOBS=2`. Sources, dependencies and logs remain.
The shared source queue is implemented once; no wrapper-specific pressure
compensation was added to conceal the old dependency mismatch.

## Publication and recovery

The authorized destinations remain `pingcap/tidb` `hparser-integration` and
`ngaut/client-rust` `master`; no fork or credentials are substituted. Earlier
GitHub push attempts returned `Permission to pingcap/tidb.git denied to ngaut.`
and HTTP 403. This establishes repository-scoped write denial but does not
identify account role versus GitHub-app installation scope. The user will
arrange access. The actual result for this batch is recorded in the validation
JSON after the mandatory fresh build. Preserve the verified recovery bundle
and exact local HEAD in the reusable environment draft; draft saving is not
publication or proof of fresh-task restoration.
