# `pkg/server/handler/tests` DXF history consumer ExecPlan

## Progress

- [x] (2026-09-02) Pinned Go master and inventoried all five package
  artifacts (3,630 lines), including the 46-shard BUILD target and complete
  HTTP test harness, before editing.
- [x] (2026-09-02) Updated the DXF history API regression to require stable
  error code/category fields and reject raw sensitive task-error text.
- [x] (2026-09-02) Ran the focused failpoint-aware success test.
- [ ] Complete the shared Ready gates, publish the combined batch, verify and
  pull `origin/hparser-integration`, then continue the rolling audit.

## Scope

This is test-only consumer coverage for the Go storage history API. Rust has
no dependency-closed HTTP handler server or session-backed DXF history owner;
the test therefore remains a Go integration boundary rather than a Rust
test-port claim.

## Validation

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/server/handler/tests \
  -run '^TestDXFAPI$/^task_history_api$/^success$' -count=1
```

`make bazel_prepare` is required for the changed Go test import and remains
blocked locally by the unavailable `bazel` executable. The complete inventory
and result are recorded in `rust/testport/receipts/server_handler_tests.md`.
