# `pkg/store/copr` query-scoped coprocessor limiter receipt

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

The complete package inventory is
`rust/docs/parity/copr-package-inventory.md`: 25 tracked artifacts and 12,225
Go production/test/build lines, including the two Bazel targets, copr test
harness, metrics target, MPP/region-cache support, and all fixtures. This
bounded fix owns the Go `coprocessor.go` request-attempt admission behavior;
the Go source and Bazel files remain unchanged.

## Go contract and Rust gap

Go's `copIteratorWorker.setRequestAttemptLimiter` applies only to TiKV
attempts. It prefers the query-scoped `QueryCopStoreLimiter`'s limiter for the
selected store, otherwise uses the request-local limiter; it fast-paths
`TryAcquire`, waits against cancellation and the iterator finish signal, and
returns a release callback that runs when the physical attempt completes.
Retries therefore release the old store's token before selecting a new route.

Rust already carried both limiter values through `KvRequestMetadata` but the
direct-unary dispatcher never consumed them, allowing unlimited same-store
attempt overlap. The new `acquire_request_attempt_limiter` helper admits after
the selected endpoint is confirmed TiKV, and `RequestAttemptPermit` holds the
token through synchronous, BatchCommands, and asynchronous response
settlement. Its `Drop` releases on success, error, retry, cancellation, and
response teardown. A query limiter does not fall back to the request limiter
for store ID zero, matching Go's precedence.

## Focused regression

- Before the production helper was present, the focused source regression
  `direct_unary_async_region_runtime_source::every_tikv_attempt_honors_the_query_scoped_store_limiter`
  failed with `missing fn acquire_request_attempt_limiter(`.
- After the helper and permit wiring, the same test passed (`1 passed, 255
  filtered out`). It checks query/request-limiter selection, blocking
  admission, and permit lifetime through dispatch/settlement.
- `tidb_txnkv::kv_contract::tests::blocking_request_limiter_waits_for_release`
  passed (`1 passed, 157 filtered out`), proving that a second synchronous
  caller waits until the first token is released.

## Validation

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-distsql --test all \
  every_tikv_attempt_honors_the_query_scoped_store_limiter -- --nocapture

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-txnkv --lib blocking_request_limiter_waits_for_release -- --nocapture

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml \
  --package tidb-txnkv --package tidb-distsql -- --check
git diff --check
```

The complete `pkg/store/copr` Go test matrix, live TiKV/unistore copr
integration, limiter-wait metrics, MPP/region-cache paths, and full workspace
Ready lint gate are not part of this focused receipt yet. No Go, Bazel,
generated, fixture, or platform artifact changed, so `make bazel_prepare` is
not required for this Rust-only batch.
