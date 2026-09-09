# `pkg/timer` — Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package was
read and inventoried before the Rust changes below. It has no `doc.go`, no
platform-specific Go files, and no checked-in generated or fixture files
outside the listed test and Bazel artifacts.

## Complete Go inventory

The package contains 31 tracked artifacts and 10,028 logical lines. The
following is the complete production, test, documentation, and build-artifact
inventory; SHA-256 values are over the pinned Go-master files.

| Artifact | Lines | SHA-256 | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 30 | `af8b1967ee7410c3788cc524b6d18989194a75d9a07b7d7d947acd3cd547af28` | package/test Bazel targets |
| `README.md` | 290 | `4e17742481365675ba6967be5325560a5826feb1733b60b43281c9e6281fbb6e` | timer framework contract |
| `api/BUILD.bazel` | 49 | `ed274e40a37e0c198c126bb5e8c1078098092e08b0ada3c711cb593ff1cff3e1` | API Bazel targets |
| `api/client.go` | 272 | `f2dca5c63defd7421d77a2374bf841df103976280ffc41ab8bdc6d185a100286` | default client and retry behavior |
| `api/client_test.go` | 419 | `9c591ebeb8c0b1ecd7eb0eaacded4f03e3a70670c0636f16816e9b8efb5caa90` | client tests |
| `api/error.go` | 29 | `93b05d730e8e276a301fd3fcc7e624cd2f28aae76b1b9c89d14bcbf1e5935123` | error sentinels |
| `api/hook.go` | 59 | `f3530f3b7e8d05b8cf47145a97ac89563931bb898a09367735c40fcf91736405` | hook contract |
| `api/main_test.go` | 34 | `3316ba78cd8b739d0cbac2bc39a033a90029ba6deeb1d90b599ea556d6a617e3` | test setup |
| `api/mem_store.go` | 326 | `dc66ca2a62eea921ba616449f17a51dc8f6e41e0c0a725044afa78075926c21a` | memory store/notifier |
| `api/schedule_policy_test.go` | 187 | `e414661efef78cdac85844d69c531d02627e617609d418682e287624a002d749` | schedule policy tests |
| `api/store.go` | 448 | `ac8d37565f2f8c7c4d8e0803be83eb579f7e53992c92c09c969ac8dda44fe84d` | store/condition contracts |
| `api/store_test.go` | 345 | `42889f3e6f06d8b6dd13a34948041597523d1a97d90376b09ad924187895309b` | condition/store tests |
| `api/timer.go` | 285 | `b6a0c956edd7016b9117a7997db47f95608f0b6c0d9e3ca90b2b11af77593d5f` | timer records/policies |
| `api/timer_test.go` | 115 | `8951f29079c345de0ad650d89d21f189e33c03c9ee0af2431cb6030361ced2ec` | timer validation tests |
| `main_test.go` | 35 | `52b8f059bec20eaa1df3f91cdfdf31827c68bd0035aec0c2e1af62ea2e01a9e7` | package test setup |
| `metrics/BUILD.bazel` | 12 | `b9f5f9018b66b75455089b271c3daedec9f23bcf69cfe0f4abcb114f1882357f` | metrics Bazel target |
| `metrics/metrics.go` | 48 | `1c4c1103f68d55c1fd63a04bf4dea99dc640400dd051466bd95a2ff05ab00bd3` | timer metrics |
| `runtime/BUILD.bazel` | 53 | `038b10633d1adc2e27f344f4e96c0e13f01c6861b5014407523a39097ac295f1` | runtime Bazel targets |
| `runtime/cache.go` | 243 | `8442075b957fe71ee7644172469b80a673125063ce87451e203668567e300c1a` | runtime cache |
| `runtime/cache_test.go` | 477 | `43823a508079f9a5aacba6fc3ea44e8bf8aa4f88ab4282de91f49301ea65c9e4` | cache tests |
| `runtime/main_test.go` | 190 | `b473aaf38cecc45010fd1a0d3dce03e5e623894438dd9ad233e94dd84c2b9866` | runtime test setup |
| `runtime/runtime.go` | 555 | `56c4ca31550ab36cedf53f15cb820e53729b714d900064d245a8e20aea568a76` | scheduling runtime |
| `runtime/runtime_test.go` | 1002 | `bed1753f05131e7ac28c95a39cb745b1e7e9994c102ae16cf360ee013b82be22` | runtime tests |
| `runtime/worker.go` | 450 | `5637406ef57fa44c1a722d11ceda5503e0c66756e39eafa48d319eb6393f01c9` | hook worker |
| `runtime/worker_test.go` | 906 | `1483f88d837e5fe6d0342acccdedb95c1d93f00ca1a9096be36bafbdd63e1580` | worker tests |
| `store_intergartion_test.go` | 1030 | `5df7c6e757a9474e924e41dc0ca3b0dbab6b19f0632fcc0ec821e883620e45d8` | memory/table/etcd integration tests |
| `tablestore/BUILD.bazel` | 56 | `d454b7297bead5c8222a22d73248ccdb82872ae6c47476753b8bcd261f753eda` | table-store Bazel targets |
| `tablestore/notifier.go` | 321 | `8328098c384d827a73d6e00a09f768f2a255e58f5b76898787065a8868bb9ddf` | etcd watch/lease notifier |
| `tablestore/sql.go` | 456 | `c432f1ca415b96a8ee5761eea0ccbb177a35013aa95591fadbd5d7e300a2c2d2` | table SQL builders |
| `tablestore/sql_test.go` | 841 | `ad19b1e8bcd6219641eeb18d82a2ded277a055fae0e2c72e55ec3aa8a47c68a6` | SQL-builder tests |
| `tablestore/store.go` | 465 | `a0255c5d7edff49cd71c7c0cc5f312e7b896cd5c261c08c8f62956ce5b73a474` | table store CRUD/watch |

## Rust ownership and parity decision

The owner is `rust/crates/tidb-timer`. Its complete source and test inventory
was read before editing: `Cargo.toml`; `src/{client,cron,error,go_time,hook,
lib,mem_store,notifier,runtime/cache,runtime/mod,runtime/worker,store,
table_store/json,table_store/mod,table_store/sql,table_store/store,timer,
uuid}.rs`; and `tests/{all,client_test,common,runtime_cache_test,
runtime_runtime_test,runtime_worker_test,schedule_policy_test,
store_integration_test,store_test,table_store_sql_test,timer_test}.rs`.
The pre-change Rust owner had all API, memory-store, runtime, SQL, and test
surfaces but explicitly omitted `tablestore/notifier.go`, making the package a
SEED.

This batch removes that Rust-only omission. `src/notifier.rs` now implements
the Go key namespace, event JSON (including Go HTML escaping and field order),
PUT-only prefix watch filtering, per-watcher cancellation/closure, event
batching, one-second throttling, 60-second leased keys, keepalive, 20-second
PUT timeout, and idempotent shutdown. `new_table_timer_store` now accepts the
Go-equivalent `(cluster_id, ..., Option<Arc<EtcdClient>>)` and selects the etcd
or memory notifier. `tidb-pd-client::EtcdClient` gained the exact-timeout
leased PUT needed by the Go call site.

No Rust-only notifier behavior remains. The embedded-etcd integration half of
the upstream Go test remains environment-dependent and is not run locally;
codec and malformed-event regressions are deterministic unit coverage.

## Validation

Profile: **Ready** for this package batch; the repository-wide package loop
remains in progress.

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline -p tidb-timer --lib -- --test-threads=1
# 11 passed, 0 failed

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline -p tidb-timer --test all -- --test-threads=1
# 48 passed, 0 failed

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed
```

The pre-implementation regression command failed at compile time because the
notifier codec and owner did not exist; the focused codec tests pass after the
implementation. No Go or Bazel file changed, so `make bazel_prepare` was not
required. Full Go embedded-etcd/table-store integration and Bazel analysis
were not run locally.

## Risks and boundaries

- Correctness risk is concentrated in the unavailable live-etcd transport
  integration; wire compatibility is covered by exact JSON assertions.
- Compatibility risk is limited to Rust callers of the seed constructor,
  which now use the source-shaped five-argument signature; no in-repository
  Rust caller used the old three-argument form.
- Performance is bounded by the same one-second coalescing interval and
  leased-key write pattern as Go; each Rust watch has one reconnecting etcd
  worker and one cancellation monitor.
