# `pkg/lightning/config` parity audit ExecPlan

## Objective

Inventory every Go-master artifact in Lightning's configuration package,
including schema/default adjustment, flag/TOML loading, codecs, queue
behavior, tests, failpoint branches, and BUILD metadata, then determine
whether Rust has a dependency-closed owner.

## Completed

- Read all ten pinned Go-master artifacts in full: 4,005 lines across five
  production files, three test files, BUILD metadata, and OWNERS metadata.
- Counted 66 production declarations, 52 `TestXxx` functions, two test
  helpers, the single CPU failpoint branch, and the 50-shard flaky test target.
- Mapped endpoint/TLS and security adjustment, backend and concurrency
  defaults, checkpoint/post-restore policy, duplicate/compression codecs,
  CSV/charset handling, global/flag/TOML loading, redaction, and the
  context-aware task queue.
- Confirmed no fixtures, testdata, benchmarks, fuzz inputs,
  generated/platform variants, package docs, or extra build artifacts exist.
- Compared the hparser branch with Go master; production sources are
  unchanged and only the ownership-filter metadata differs.
- Reworked `TestRemoveAllowAllFiles` to assert parsed DSN semantics so the
  regression is stable across the branch's older Go/dependency toolchain.
- Searched Rust Lightning/config crates and confirmed no dependency-closed
  `pkg/lightning/config` owner or consumer; the existing Rust `ByteSize` owner
  belongs to the separate `pkg/config/configtypes` claim.

## Validation gate

- [x] Current-branch failpoint-aware package suite passes and cleanup returns
      refcount to zero.
- [x] Detached exact-Go-master failpoint-aware package suite passes and
      cleanup returns refcount to zero.
- [x] Ready formatting, lint, and diff checks pass for this receipt batch.
- [ ] Push the focused regression plus receipt/ExecPlan to
      `origin/hparser-integration`, verify local/tracking/advertised SHAs, and
      pull the explicit branch ref.

`make bazel_prepare` is not required for the final diff: it changes an
existing test body without adding a test function or changing the import
section, and no Go/Bazel/module metadata changed.

## Remaining boundary

An executable Rust port must move the Lightning command/server consumers,
common connection/TLS and table-filter types, concrete backend/checkpoint
drivers, and task-list lifecycle together with this schema. Keep the package
as an explicit boundary until those dependencies are closed; do not add an
unconnected configuration facade or ignored parity tests.
