# `pkg/ddl/notifier` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every notifier production, test, and build artifact; align test
reliability with Go master; and verify that no Rust-only notifier behavior is
silently diverging.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all eight artifacts and
  1,999 lines, including event/store/subscribe tests and the 12-shard target.
- [x] (2026-09-02) Restored Go master's five-second cleanup eventual timeout
  in `TestDeliverOrderAndCleanup`; the focused failpoint-aware regression
  passed.
- [x] (2026-09-02) Complete failpoint-aware package suite passed in 14.269s;
  Rust formatting, diff hygiene, and `make lint` passed. No Bazel preparation
  was required because only an existing test body changed.
- [x] (2026-09-06) Re-read current Go `origin/master` and the complete Rust
  notifier owner. Removed 31 Rust-only `#[must_use]` annotations from all
  Go-shaped event constructors/getters/type queries and `DdlNotifier::new`.
  The deny-on-discard regression emitted exactly 31 diagnostics before the
  fix and passes afterward; the package owner suite and Ready gates are being
  run before the batch commit.
- [ ] Publish the return-contract batch commit, then continue the rolling
  package audit.

## Scope and decision

`pkg/ddl/notifier` owns persistent schema-change events, owner-listener
delivery, handler ordering/retry, and SQL transaction cleanup. Rust has no
dependency-closed owner for these TiDB-specific contracts, so the package stays
Go-native and no parallel Rust facade is added.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/notifier -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

No Go/Bazel/module dependency shape changed, so `make bazel_prepare` is not
required for this body-only test fix.

## Outcome

The exact Go-master timeout and complete package inventory are recorded in
`rust/testport/receipts/ddl_notifier.md`; the rolling audit remains in progress.
