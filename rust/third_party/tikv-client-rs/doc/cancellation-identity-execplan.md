# Preserve cancellation identity across native client packages

## Purpose and source

Replace display-text matching with a typed Rust equivalent of Go context.Canceled. Source: TiDB master 51a1a4abfc192a91f98fe968ad87eced9221f663, pinned client-go v2.0.8-0.20260928031501-8edb23f6c7ee, internal/locate/region_request.go:onSendFail and txnkv/transaction. Existing package inventories retain the broader package evidence; this is a repair, not a claim of complete package parity. Native baseline: 515e4aab0e4a98d891641ab726209748752eb50e.

## Progress

- [x] Reproduce text-based cancellation misclassification before production changes.
- [x] Introduce Error::ContextCanceled and migrate every native producer and consumer.
- [x] Preserve remote gRPC cancellation handling and recursive connection causes.
- [x] Verify regressions, full library tests, strict Clippy and formatting.
- [x] Commit and publish to master as 89926da, then refresh TiDB using its source sync script.

## Decisions and discoveries

Go compares the cancellation cause by identity. An ordinary error containing context canceled must not terminate request retries or become a transport failure. The new typed variant preserves the display spelling. Transaction-file retries explicitly reject local cancellation, including wrapped cancellation. Remote status cancellation still observes the request's actual context where required. Mock transport cancellation returns the same typed identity as production.

## Validation and outcome

The two new source_cancellation_uses_identity_not_error_text tests failed against the original classifier and pass after the change. Full library results: 1,401 passed, two ignored. Strict Clippy passes. Exact commands:

    cargo test --locked --lib source_cancellation_uses_identity_not_error_text
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    rustfmt --edition 2021 --check src/common/errors.rs src/request/plan.rs src/transaction/transaction.rs src/transaction/lock.rs src/mock/mocktikv/rpc.rs
    git diff --check

Logs: /private/tmp/native-cancellation-{red,green,lib,clippy}.log. No generated inputs change. Real TiKV cancellation faults, feature-matrix builds and benchmarks were not run. This change removes a string comparison; no throughput claim is made. The public Error enum gains a variant, so downstream exhaustive matches need compilation checks during dependency refresh.

## Recovery

Publish without force. The TiDB integration update is explicitly authorized. Keep native fixes upstream and regenerate the vendor copy rather than maintaining a native-algorithm patch.
