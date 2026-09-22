# `pkg/store/copr` transcreation inventory

This receipt is part of `rust/docs/go-physical-plan-parity-execplan.md`. The
claim unit is the complete tracked Go package at commit
`64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59` (`origin/master`, refreshed 2026-09-22). `Partial`
means that a native Rust owner exists but full contract/test parity has not yet
been demonstrated.

| Go artifact | Rust owner or disposition | Current status / receipt |
| --- | --- | --- |
| `BUILD.bazel` | Rust Cargo targets and this receipt; build/test inventory still needs comparison | Partial |
| `batch_coprocessor.go` | `tidb-distsql/src/cop_paging`, `tidb-txnkv/src/rpc/batch` | Partial |
| `batch_coprocessor_test.go` | Rust batch coprocessor suites | Partial |
| `batch_request_sender.go` | `tidb-distsql/src/cop_paging/direct_unary_query_transport.rs` | Partial |
| `copr_test/BUILD.bazel` | Rust integration targets | Missing receipt |
| `copr_test/coprocessor_test.go` | Rust TiKV/unistore cop suites | Partial — Go master’s same-store query/request limiter concurrency scenarios are represented by the direct-unary admission owner; live TiKV/unistore integration remains |
| `copr_test/main_test.go` | Rust test harness | Partial |
| `coprocessor.go` | `tidb-distsql/src/cop_paging.rs`, `tidb-distsql/src/cop_paging/direct_unary_query_transport.rs`, `tidb-exec/src/cop_scan.rs`, `tidb-txnkv/src/kv_contract.rs` | Partial — query-scoped/request-local per-store attempt limiting is now enforced around every TiKV dispatch, and `pagingResponseReadBytes` now follows the source compile-time kernel type; broader task, paging, retry, and metrics parity remains |
| `coprocessor_cache.go` | `tidb-distsql/src/copr_cache.rs` | Implemented behavior; all six Go tests ported plus branch regressions for the oversized start/end key and negative `Tp` paths (`copr_cache_source::cache_key_rejects_oversized_range_keys_in_source_order`, `negative_request_type_keeps_go_uint8_low_byte`) |
| `coprocessor_cache_test.go` | `tidb-distsql/tests/copr_cache_source.rs` | Implemented; 13 focused tests, including a deterministic stand-in for ristretto's asynchronous write buffer |
| `coprocessor_test.go` | Rust paging/transport/scan tests, including `direct_unary_paging_and_close`, `direct_unary_dispatch_contract` and the direct-unary transport unit tests | Partial — limiter admission/release and waiter cancellation are covered; complete response, failpoint, and live-store matrix remains |
| `ema.go` | `tidb-distsql/src/read_bytes_ema.rs`; `paging_response_read_bytes` in `tidb-distsql/src/cop_paging.rs` | Implemented; classic/NextGen read-byte selection and the time-decay EMA |
| `ema_test.go` | `tidb-distsql/tests/read_bytes_ema_source.rs`, `tidb-distsql/tests/cop_paging_source.rs` | Implemented; every Go case plus the zero-timestamp branch |
| `key_ranges.go` | `tidb-txnkv/src/key_ranges.rs` | Implemented; `first`/`mid`/`last` storage, `slice`, `split`, `reset`, Go `%q` display, and safe protobuf conversion |
| `key_ranges_test.go` | `tidb-txnkv/tests/key_ranges_source.rs` | Implemented; every Go split/slice case plus the unanchored methods |
| `main_test.go` | Rust package test harness | Partial |
| `metrics/BUILD.bazel` | `tidb-distsql` Cargo target and cache lifecycle tests | Partial — build inventory pending |
| `metrics/metrics.go` | `tidb-distsql/src/copr_cache_metrics.rs` | Implemented process-global `evict`/`hit`/`miss` counters; the Prometheus `/metrics` exporter is still unported in `tidb-server`, so Go's `tidb_distsql_copr_cache{type=…}` name/label is not published yet |
| `mpp.go` | `tidb-txnkv/src/mpp.rs`; TiFlash execution tier remains narrowed | Partial |
| `mpp_probe.go` | `tidb-txnkv/src/mpp_probe.rs` | Partial — failed-store prober TTL/recovery scan and server-info LRU implemented; full store integration receipt pending |
| `mpp_probe_test.go` | `tidb-txnkv::mpp_probe` focused tests | Partial |
| `range_diagnostics.go` | `tidb-txnkv/src/range_diagnostics.rs` | Implemented core monotonicity/overlap/gap diagnostics; focused unit tests |
| `region_cache.go` | `tidb-txnkv/src/region/**` | Partial |
| `region_cache_test.go` | Rust region-cache suites | Partial |
| `store.go` | distributed across `tidb-txnkv`, `tidb-distsql`, and SQL-node capabilities | Partial |

Current count: 25 tracked artifacts; 12,604 Go source/test/build lines at the
comparison commit; no complete-package claim. The previously absent MPP
probe, cache metrics, and range diagnostics now have concrete Rust owners and
focused tests. This batch closes the Go master query-scoped per-store limiter
execution boundary: metadata is admitted by store ID, a token is held through
RPC response classification, and retries release the old store token before
selecting another store. Integration, complete metrics, and the remaining
production/test rows stay explicit blockers.

Re-verified 2026-09-08 against `origin/master` `f5cf8f6337` (the package root
is unchanged in these artifacts): the four leaf owners above are now
`Implemented` rather than `Partial`. `coprocessor_cache.go`'s six Go tests,
`ema_test.go`'s five EMA cases plus `pagingResponseReadBytes`, and
`key_ranges_test.go`'s split/slice matrix all have direct Rust equivalents,
and the two Go production branches with no Go test — oversized range keys and
a negative `Tp` — now have focused Rust regressions. The package-level
blockers are the worker lifecycle, region-cache orchestration, MPP/TiFlash
tier, and the `/metrics` exporter, not these leaf owners.

## Query-scoped per-store request limiter alignment

Go `copIteratorWorker.setRequestAttemptLimiter` prefers
`QueryCopStoreLimiter.GetStoreLimiter(storeID)` over the request-local limiter,
fast-paths `TryAcquire`, waits with cancellation/deadline awareness, and
releases the token as the physical attempt completes. Rust previously carried
both limiter values through `KvRequestMetadata` but never consumed either one
in the direct-unary transport. `CoprRequestLimiter` now has a synchronous
condition-variable wait for the pull-based response owner; each prepared TiKV
dispatch owns an RAII token through synchronous, BatchCommands, and async
completion paths. A retry drops that permit before route recovery selects the
next store, and a query limiter does not fall back to the request limiter for
store ID zero, matching Go's precedence.

The complete package inventory above includes all 25 Go production, test,
fixture/harness, metrics, generated/protocol-facing, and Bazel artifacts. Rust
owners changed only in `tidb-distsql` transport/test support and
`tidb-txnkv::kv_contract`; no Go, Bazel, generated, fixture, or platform
artifact changed.

Focused fail-before/pass-after evidence is recorded in
`rust/testport/receipts/copr_query_limiter.md`.

## Current-master artifact refresh, 2026-09-22

The table below inventories every tracked artifact under pkg/store/copr at the
current pin, including the root package and its separately built copr_test and
metrics dependencies. Package boundaries remain atomic; the subtree count is
not a single-package completion claim. There is no package doc.go. The prior
source pin differs in BUILD.bazel, coprocessor.go and coprocessor_test.go. New
master contracts retain per-RPC lock hints during batched lock recovery and
scale serial store-batch deadlines; full Rust parity for those changes remains
an explicit open audit boundary. Admission/cancellation source contracts are
unchanged. Existing owner rows above retain their bounded status, not a fresh
whole-package acceptance.

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 137 | `be1569cae671dfbbd002e1144b1998589b9fb402` | `8f135399d50990eac80230f99845633b9090c393afc228ccd0f76930dde46788` |
| `batch_coprocessor.go` | 1739 | `2fe7defee64374fe313d00950f6ef45cd7919b43` | `d39b7cef54938935e6be3a90cf494ffa317b111f1689219aeefe1d61161a4790` |
| `batch_coprocessor_test.go` | 587 | `42fb6e46be7d4297b3fae36d2425236160d3b2c1` | `1f307ffbc8aa86a7b48357b72067b41cb114bef9208cd70bd018e051fdb62f96` |
| `batch_request_sender.go` | 119 | `5c6d9a6cbe1927395b5331b744c60899f2d93cdf` | `276eff2ff7c41a9fa66b1997a56b4f155d1137ff188969eebcd9a1c896e5b0c3` |
| `copr_test/BUILD.bazel` | 38 | `25957ae154caecf1e829618e8fb6edf58835bc49` | `932dd1ae79413485b84bda46f3a363d20b26e06a0dde9367b4821ed025501fe8` |
| `copr_test/coprocessor_test.go` | 787 | `f84c64b05e046fe401f1ce4ece1dca355f327e73` | `5e4cd96bb31a8272c05a25aaf0855d811c378e433a8a38cd66908b15347ecac8` |
| `copr_test/main_test.go` | 62 | `3ff792b5c1875b5677459cb2fbc847aebcfbe58a` | `de1789293adc2816e29f1ddf72b133ac2028bcca341b077cc69219addd6b67d0` |
| `coprocessor.go` | 3309 | `8537c16f2b5ee7e12214900efa7d8f1a80873ba6` | `bb6caccce699aa4f37c53707fc9869f2f5803edd2f21ccdf7f95f8a200fc05fb` |
| `coprocessor_cache.go` | 224 | `01da36c3c3df458901011c1be7ed129eeab4cce1` | `df329738eb83cea9db8fa0ec22d8f2b24a1c88771983e0419e3c0be10214e64a` |
| `coprocessor_cache_test.go` | 259 | `24929e858830557a396c59697babb72baf9dc129` | `c3613b44620349a5bba6247b5d244f62f5219453ba5774ca09c32d195bbc9787` |
| `coprocessor_test.go` | 1974 | `69b938239af66d24e797ad688b32bfde222f84e7` | `1e46506a533ee3bb3f92160afbd436adb8d60afbba6f2a51e43da81ff7b5464f` |
| `ema.go` | 64 | `dbeaf3ea16d9e9576cebf6cdea79a95397fd07ff` | `b554d37bb5d34a1480f89014eb424c810475aa0ace27eb178650dd6374183381` |
| `ema_test.go` | 183 | `f4619594355ec39c7fff2d233f416ef81d33d69c` | `83cc3c487376a893e4e816a6f45a927a9ce5dee515b70d321ca29571c3f50ab7` |
| `key_ranges.go` | 165 | `d1d27077152aef556b7dd538c2f94538106a6b38` | `da8f4cb8ac8bc1b20d588606de12d9b6266841386a790d3d34ed274fc905ec73` |
| `key_ranges_test.go` | 126 | `e2893a3fef1c69596a998633074a50ee54d44efc` | `04aea5f26af0fa9fc360b9bab7a9605bf842beb2d62289b4a70a124c161ab959` |
| `main_test.go` | 47 | `ad6f13003c5043b448ce7de2e5aa6b5a06894b7f` | `de6b5baa33392ec0ae1ee3b3fba9333cfa95a11ba14e4bccac8e6d76fa89a892` |
| `metrics/BUILD.bazel` | 12 | `1fb445ee388d04ee9b12f00f0c89065bfedc83d1` | `e7ba86badd83c24cda56eafd3a5aa773b53838bc11ca7262d2c72f5e0a2adab6` |
| `metrics/metrics.go` | 38 | `484d922466735370784f1ce0f5980e4d50ea07d7` | `d5be1725cb2acac2319037209a4de72bf77629b57a8b308400a083649e55f172` |
| `mpp.go` | 357 | `b692def3d29e83f7e7cfb797d7ce622fb3cebe18` | `8f60476304bcaad0315c30977abf75ce0878a0f1621ef519466154279689eb5c` |
| `mpp_probe.go` | 335 | `8043e9f316c2c19556167d3aa41d6863174a3e35` | `d9f3a923d3214cb457a6e5812a6e1dfd2499158097542169898b343621e96eb2` |
| `mpp_probe_test.go` | 229 | `387bc90ff3d73e6746b8d008de727bfbb6cbd12c` | `0d40af599f5fe002e045b40904a2168d863c84d4a2818400ce3399450a2f04b0` |
| `range_diagnostics.go` | 94 | `14c360df8286aae87c4dbdd9ce87993e3b176684` | `8109d5d8e14bf1dfef69f4c7d760bc7ef7b1d8138ed4cfe30f6ea95d83be27bf` |
| `region_cache.go` | 1024 | `6e7ec9edf5364110ae3d8127959190a6ac77fce4` | `8944998fa050a0f7641fb9cde9092ef49fafe7c6ea0c970f6584f8900d96b703` |
| `region_cache_test.go` | 539 | `337eedd2524644bd9bb7b9558e71d2aeda535eb9` | `8d463176a019c4ee02f4ffc587132ae036678280cba01be640b79fd1f756cde4` |
| `store.go` | 156 | `bfb635bcafaf8dee17d39a7f597d0db075d7da3c` | `f4040ede33e595043baf8efc57a6f6748072d67b40e7d46683863e55c41eff42` |


## Ignored request lock hints, 2026-09-22

The live direct-unary transport now checks the exact sent request's resolved
and committed transaction hints before lock resolution, charges one short
backoff per matching response, and preserves registered exhaustion error 9004
through raw/decoded responses and executor rows/chunks. The 64-case regression
covers resolved/committed hints, ordinary/shared locks, ordered/unordered reads,
synchronous/asynchronous completion, success, exhaustion, cancellation and
unhinted replies. This is bounded seed evidence only. Store-batched envelopes
remain rejected and their parent/child suppression and serial timeout contracts
remain open. Snapshot Get/BatchGet/Scan caller gaps also remain explicit.

See rust/docs/operations/store-copr-audit-execplan.md for the exact red/green,
original-master and dependent compilation/lint receipt. The complete pinned
client-go txnkv/txnlock dependency inventory is
rust/docs/parity/client-go-txnlock-package-inventory.md. Neither package is
accepted as fully transcreated by this change.
