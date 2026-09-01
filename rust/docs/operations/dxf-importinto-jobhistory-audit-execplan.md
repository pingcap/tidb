# `pkg/dxf/importinto/jobhistory` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in `pkg/dxf/importinto/jobhistory` and
compare its SQL-backed ImportInto history query, aggregation, and formatting
contract with Rust owners without fabricating an API disconnected from task
keys, history-table schemas, and session execution.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all three tracked artifacts and all 408 lines: the
  BUILD target, complete production source, and complete end-to-end test.
  Verify no package documentation, owner file, fixture, benchmark, generated
  input/output, or platform variant exists.
- [x] (2026-09-01) Trace all five production functions and the full
  `TestGetFromHistory` contract: mode-sensitive key lookup, history-table SQL,
  large integer IDs, JSON plan/summary extraction, step/KV grouping, byte/rate
  and duration formatting, and missing-job error behavior. Search Rust table,
  DXF, metadata, session, and executor owners and confirm no dependency-closed
  history API exists.
- [x] (2026-09-01) Run the package compile check and the exact failpoint-enabled
  integration suite with the repository wrapper; both pass. Run the Ready
  documentation gates and capture the results in the parity receipt.
- [x] (2026-09-01) Publish one meaningful receipt batch to
  `origin/hparser-integration`, verify the remote SHA, pull the branch's latest
  state, and continue the rolling package audit.

## Scope and decision

This package is one atomic query-and-formatting unit. `GetFromHistory` depends
on `taskkey.ForJobInKeyspace`, `TaskManager` SQL sessions, global/background
history schemas, JSON plan and summary fields, and step/KV aggregation before
producing the user-visible `Info` JSON. A struct or formatter alone cannot
preserve those semantics. Keep the boundary explicit until Rust has the
dependency-closed metadata, session, and ImportInto history consumers.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/importinto/jobhistory -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The full mock-store test and compile probe pass. No Go/Bazel/module or Rust
source changed, so neither `make bazel_prepare` nor a Rust test target is
required for this receipt-only boundary batch.

## Outcome

The complete inventory, validation evidence, and native ownership decision are
recorded in `rust/testport/receipts/dxf_importinto_jobhistory.md`. The rolling
audit continues with the next unrecorded Go package; repository-wide parity is
not claimed.
